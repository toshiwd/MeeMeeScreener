from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from pathlib import Path
import json
from typing import Any

from app.backend.services.toredex_config import load_toredex_config
from app.backend.services.toredex_execution import execute_live_decision
from app.backend.services.toredex_hash import hash_payload
from app.backend.services.toredex_paths import ensure_daily_paths, ensure_monthly_paths, resolve_runs_root
from app.backend.services.toredex_policy import build_decision
from app.backend.services.toredex_repository import ToredexRepository
from app.backend.services.toredex_snapshot_service import build_snapshot, snapshot_has_minimum_fields

_JST = timezone(timedelta(hours=9))


def _int_to_date(value: int) -> date:
    iv = int(value)
    raw = str(abs(iv))

    # YYYYMMDD
    if len(raw) == 8:
        return date(int(raw[:4]), int(raw[4:6]), int(raw[6:8]))

    # Some datasets store `date` as epoch seconds/millis/micros.
    if len(raw) >= 9:
        seconds = iv
        if len(raw) > 10:
            seconds = iv // (10 ** (len(raw) - 10))
        try:
            return datetime.fromtimestamp(seconds, tz=timezone.utc).astimezone(_JST).date()
        except Exception:
            pass

    # Last fallback: if prefix looks like YYYYMMDD, use first 8 chars.
    if len(raw) > 8:
        head = raw[:8]
        try:
            return date(int(head[:4]), int(head[4:6]), int(head[6:8]))
        except Exception:
            pass

    raise ValueError(f"invalid date value: {value}")


def _parse_as_of(value: str | None) -> date | None:
    if not value:
        return None
    text = str(value).strip()
    if not text:
        return None
    for fmt in ("%Y-%m-%d", "%Y%m%d", "%Y/%m/%d"):
        try:
            return datetime.strptime(text, fmt).date()
        except ValueError:
            continue
    return None


def _write_json(path: str, payload: dict[str, Any]) -> None:
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=False, indent=2, sort_keys=True)


def _json_ready(value: Any) -> Any:
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if isinstance(value, dict):
        return {str(k): _json_ready(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_json_ready(v) for v in value]
    return value


def _contains_forbidden_keys(value: Any, forbidden: set[str]) -> bool:
    if isinstance(value, dict):
        for key, item in value.items():
            if str(key) in forbidden:
                return True
            if _contains_forbidden_keys(item, forbidden):
                return True
        return False
    if isinstance(value, list):
        return any(_contains_forbidden_keys(item, forbidden) for item in value)
    return False


def _resolve_as_of(repo: ToredexRepository, requested: str | None) -> date:
    parsed = _parse_as_of(requested)
    if parsed is not None:
        return parsed
    today_jst = datetime.now(tz=_JST).date()
    latest = repo.get_latest_available_asof()
    if latest is not None:
        latest_date = _int_to_date(latest)
        return latest_date if latest_date <= today_jst else today_jst
    return today_jst


def run_live(
    *,
    season_id: str,
    as_of: str | None = None,
    dry_run: bool = False,
) -> dict[str, Any]:
    repo = ToredexRepository()
    cfg = load_toredex_config()

    as_of_date = _resolve_as_of(repo, as_of)
    as_of_iso = as_of_date.isoformat()

    cfg_json = json.dumps(cfg.data, ensure_ascii=False, sort_keys=True)
    repo.ensure_season(
        season_id=season_id,
        mode="LIVE",
        start_date=as_of_date,
        initial_cash=cfg.initial_cash,
        policy_version=cfg.policy_version,
        config_json=cfg_json,
        config_hash=cfg.config_hash,
    )

    positions = repo.get_positions(season_id)
    snapshot = build_snapshot(
        season_id=season_id,
        as_of=as_of_date,
        config=cfg,
        positions=positions,
    )

    if not snapshot_has_minimum_fields(snapshot):
        return {
            "ok": True,
            "status": "skipped",
            "reason": "K_NO_SNAPSHOT",
            "season_id": season_id,
            "asOf": as_of_iso,
            "dry_run": bool(dry_run),
        }

    snapshot_hash = hash_payload(snapshot)

    prev_metric = repo.get_latest_metrics(season_id, before_or_equal=as_of_date - timedelta(days=1))
    decision = build_decision(snapshot=snapshot, config=cfg, prev_metrics=prev_metric, mode="LIVE")
    checks = decision.get("checks") if isinstance(decision.get("checks"), dict) else {}
    if not (
        bool(checks.get("maxHoldingsOk"))
        and bool(checks.get("unitRuleOk"))
        and bool(checks.get("lossLimitOk"))
        and bool(checks.get("noFutureLeakOk"))
    ):
        raise RuntimeError("K_POLICY_INCONSISTENT: decision checks failed")
    if _contains_forbidden_keys(decision, {"path", "realPath", "runtime", "host", "createdAt"}):
        raise RuntimeError("K_POLICY_INCONSISTENT: environment-dependent fields in decision")
    decision_hash = hash_payload(decision, exclude_fields={"createdAt", "runtime", "path", "realPath", "host"})

    existing_snapshot = repo.get_snapshot_row(season_id, as_of_date)
    existing_decision = repo.get_decision_row(season_id, as_of_date)

    if existing_snapshot and str(existing_snapshot.get("snapshot_hash") or "") != str(snapshot_hash):
        raise RuntimeError("K_POLICY_INCONSISTENT: snapshot hash mismatch on rerun")

    if existing_decision:
        if str(existing_decision.get("decision_hash") or "") == str(decision_hash):
            return {
                "ok": True,
                "status": "noop",
                "reason": "idempotent",
                "season_id": season_id,
                "asOf": as_of_iso,
                "snapshot_hash": snapshot_hash,
                "decision_hash": decision_hash,
                "dry_run": bool(dry_run),
            }
        raise RuntimeError("K_POLICY_INCONSISTENT: decision hash mismatch on rerun")

    runs_root = resolve_runs_root(cfg.runs_dir)
    daily_paths = ensure_daily_paths(runs_root, season_id, as_of_iso)
    monthly_paths = ensure_monthly_paths(runs_root, season_id)

    logical_snapshot_path = f"{daily_paths['logical_dir']}/snapshot.json"
    logical_decision_path = f"{daily_paths['logical_dir']}/decision.json"

    if dry_run:
        return {
            "ok": True,
            "status": "dry_run",
            "season_id": season_id,
            "asOf": as_of_iso,
            "snapshot_hash": snapshot_hash,
            "decision_hash": decision_hash,
            "snapshot": snapshot,
            "decision": decision,
        }

    _write_json(daily_paths["snapshot"], snapshot)
    _write_json(daily_paths["decision"], decision)

    execution = execute_live_decision(
        repo=repo,
        season_id=season_id,
        as_of=as_of_date,
        snapshot=snapshot,
        decision=decision,
        config=cfg,
    )

    repo.save_snapshot(
        season_id=season_id,
        as_of=as_of_date,
        snapshot_path=logical_snapshot_path,
        snapshot_hash=snapshot_hash,
        payload_json=json.dumps(snapshot, ensure_ascii=False, sort_keys=True),
    )
    repo.save_decision(
        season_id=season_id,
        as_of=as_of_date,
        decision_path=logical_decision_path,
        decision_hash=decision_hash,
        payload_json=json.dumps(decision, ensure_ascii=False, sort_keys=True),
    )

    ledger_payload = {
        "asOf": as_of_iso,
        "seasonId": season_id,
        "positions": _json_ready(execution["positions"]),
        "metrics": _json_ready(execution["metrics"]),
    }
    _write_json(daily_paths["ledger_after"], ledger_payload)
    _write_json(daily_paths["metrics"], _json_ready(execution["metrics"]))

    month_kpi_payload = {
        "asOf": as_of_iso,
        "seasonId": season_id,
        "policyVersion": cfg.policy_version,
        "kpi": _json_ready(execution["metrics"]),
    }
    _write_json(monthly_paths["kpi"], month_kpi_payload)

    config_file_path = Path(runs_root) / season_id / "config.json"
    config_file_path.parent.mkdir(parents=True, exist_ok=True)
    _write_json(str(config_file_path), cfg.data)

    return {
        "ok": True,
        "status": "success",
        "season_id": season_id,
        "asOf": as_of_iso,
        "snapshot_hash": snapshot_hash,
        "decision_hash": decision_hash,
        "snapshot_path": logical_snapshot_path,
        "decision_path": logical_decision_path,
        "trade_count": len(execution.get("trades") or []),
        "metrics": _json_ready(execution.get("metrics")),
    }
