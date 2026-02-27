from __future__ import annotations

from collections import Counter
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
import json
from typing import Any

from app.backend.services.toredex_config import ToredexConfig, load_toredex_config
from app.backend.services.toredex_execution import execute_live_decision
from app.backend.services.toredex_hash import hash_payload
from app.backend.services.toredex_models import ALLOWED_UNIT_SET, REASON_ID_SET
from app.backend.services.toredex_paths import ensure_daily_paths, ensure_monthly_paths, resolve_runs_root
from app.backend.services.toredex_policy import build_decision
from app.backend.services.toredex_repository import ToredexRepository
from app.backend.services.toredex_snapshot_service import build_snapshot, snapshot_has_minimum_fields
from app.db.session import get_conn

_JST = timezone(timedelta(hours=9))
_FORBIDDEN_DECISION_KEYS = {"path", "realPath", "runtime", "host", "createdAt"}


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


def _parse_required_date(value: str, *, name: str) -> date:
    parsed = _parse_as_of(value)
    if parsed is None:
        raise ValueError(f"{name} must be YYYY-MM-DD or YYYYMMDD")
    return parsed


def _write_json(path: str, payload: dict[str, Any]) -> None:
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=False, indent=2, sort_keys=True)


def _write_text(path: str, text: str) -> None:
    with open(path, "w", encoding="utf-8") as handle:
        handle.write(text)


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


def _clear_rankings_caches_periodically(day_index: int) -> None:
    # Long backtests can accumulate large in-memory ranking caches and trigger OOM.
    if day_index <= 0 or (day_index % 20) != 0:
        return
    try:
        from app.backend.services import rankings_cache

        asof_cache = getattr(rankings_cache, "_ASOF_BASE_CACHE", None)
        if hasattr(asof_cache, "clear"):
            asof_cache.clear()
        prob_cache = getattr(rankings_cache, "_DAILY_PROB_CALIB_CACHE", None)
        if hasattr(prob_cache, "clear"):
            prob_cache.clear()
        base_cache = getattr(rankings_cache, "_CACHE", None)
        if hasattr(base_cache, "clear"):
            base_cache.clear()
    except Exception:
        return


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


def _iter_days(start: date, end: date) -> list[date]:
    out: list[date] = []
    cur = start
    while cur <= end:
        out.append(cur)
        cur = cur + timedelta(days=1)
    return out


def _validate_decision_actions(decision: dict[str, Any]) -> None:
    actions = decision.get("actions")
    if not isinstance(actions, list):
        raise RuntimeError("K_POLICY_INCONSISTENT: decision.actions is not a list")
    for idx, action in enumerate(actions):
        if not isinstance(action, dict):
            raise RuntimeError(f"K_POLICY_INCONSISTENT: action[{idx}] is not an object")
        delta = int(action.get("deltaUnits") or 0)
        if delta not in ALLOWED_UNIT_SET:
            raise RuntimeError(f"K_POLICY_INCONSISTENT: action[{idx}] has invalid deltaUnits={delta}")
        reason = str(action.get("reasonId") or "")
        if reason not in REASON_ID_SET:
            raise RuntimeError(f"K_POLICY_INCONSISTENT: action[{idx}] has unknown reasonId={reason}")
        ticker = str(action.get("ticker") or "").strip()
        side = str(action.get("side") or "").upper()
        if not ticker:
            raise RuntimeError(f"K_POLICY_INCONSISTENT: action[{idx}] has empty ticker")
        if side not in {"LONG", "SHORT"}:
            raise RuntimeError(f"K_POLICY_INCONSISTENT: action[{idx}] has invalid side={side}")


def _build_daily_narrative(
    *,
    snapshot: dict[str, Any],
    decision: dict[str, Any],
    metrics: dict[str, Any] | None,
) -> str:
    as_of = str(snapshot.get("asOf") or "")
    season_id = str(snapshot.get("seasonId") or "")
    mode = str(decision.get("mode") or "LIVE")
    policy_version = str(decision.get("policyVersion") or "")

    rankings = snapshot.get("rankings") if isinstance(snapshot.get("rankings"), dict) else {}
    buy_items = rankings.get("buy") if isinstance(rankings.get("buy"), list) else []
    sell_items = rankings.get("sell") if isinstance(rankings.get("sell"), list) else []
    actions = decision.get("actions") if isinstance(decision.get("actions"), list) else []

    lines: list[str] = []
    lines.append(f"# TOREDEX Narrative {as_of}")
    lines.append("")
    lines.append(f"- season_id: {season_id}")
    lines.append(f"- mode: {mode}")
    lines.append(f"- policy_version: {policy_version}")
    lines.append("")
    lines.append("## Buy Top 3")
    if buy_items:
        for idx, item in enumerate(buy_items[:3], start=1):
            if not isinstance(item, dict):
                continue
            gate = item.get("gate") if isinstance(item.get("gate"), dict) else {}
            lines.append(
                f"{idx}. {item.get('ticker')} ev={item.get('ev')} upProb={item.get('upProb')} "
                f"revRisk={item.get('revRisk')} gate={gate.get('ok')}:{gate.get('reason')}"
            )
    else:
        lines.append("- none")
    lines.append("")
    lines.append("## Sell Top 3")
    if sell_items:
        for idx, item in enumerate(sell_items[:3], start=1):
            if not isinstance(item, dict):
                continue
            gate = item.get("gate") if isinstance(item.get("gate"), dict) else {}
            lines.append(
                f"{idx}. {item.get('ticker')} ev={item.get('ev')} upProb={item.get('upProb')} "
                f"revRisk={item.get('revRisk')} gate={gate.get('ok')}:{gate.get('reason')}"
            )
    else:
        lines.append("- none")
    lines.append("")
    lines.append("## Actions")
    if actions:
        for idx, action in enumerate(actions, start=1):
            if not isinstance(action, dict):
                continue
            lines.append(
                f"{idx}. {action.get('ticker')} {action.get('side')} "
                f"delta={action.get('deltaUnits')} reason={action.get('reasonId')}"
            )
    else:
        lines.append("- no actions")

    if isinstance(metrics, dict) and metrics:
        lines.append("")
        lines.append("## Metrics")
        lines.append(f"- equity: {metrics.get('equity')}")
        lines.append(f"- cum_return_pct: {metrics.get('cum_return_pct')}")
        lines.append(f"- max_drawdown_pct: {metrics.get('max_drawdown_pct')}")
        lines.append(f"- holdings_count: {metrics.get('holdings_count')}")
        lines.append(f"- game_over: {metrics.get('game_over')}")

    return "\n".join(lines).strip() + "\n"


def _build_monthly_payload(
    *,
    repo: ToredexRepository,
    cfg: Any,
    season_id: str,
    as_of_date: date,
) -> dict[str, Any]:
    latest_metric = repo.get_latest_metrics(season_id, before_or_equal=as_of_date)
    reason_counts = repo.get_trade_reason_counts(season_id)
    top3 = reason_counts[:3]
    return {
        "asOf": as_of_date.isoformat(),
        "seasonId": season_id,
        "policyVersion": cfg.policy_version,
        "kpi": _json_ready(latest_metric) if isinstance(latest_metric, dict) else {},
        "reasonTop3": top3,
    }


def _build_monthly_summary_md(payload: dict[str, Any]) -> str:
    kpi = payload.get("kpi") if isinstance(payload.get("kpi"), dict) else {}
    reason_top3 = payload.get("reasonTop3") if isinstance(payload.get("reasonTop3"), list) else []
    lines: list[str] = []
    lines.append(f"# TOREDEX Monthly Summary ({payload.get('seasonId')})")
    lines.append("")
    lines.append(f"- asOf: {payload.get('asOf')}")
    lines.append(f"- policyVersion: {payload.get('policyVersion')}")
    lines.append(f"- equity: {kpi.get('equity')}")
    lines.append(f"- cum_return_pct: {kpi.get('cum_return_pct')}")
    lines.append(f"- max_drawdown_pct: {kpi.get('max_drawdown_pct')}")
    lines.append(f"- holdings_count: {kpi.get('holdings_count')}")
    lines.append(f"- goal20_reached: {kpi.get('goal20_reached')}")
    lines.append(f"- goal30_reached: {kpi.get('goal30_reached')}")
    lines.append(f"- game_over: {kpi.get('game_over')}")
    lines.append("")
    lines.append("## RuleId Top 3")
    if reason_top3:
        for idx, item in enumerate(reason_top3, start=1):
            if not isinstance(item, dict):
                continue
            lines.append(f"{idx}. {item.get('reason_id')} x {item.get('count')}")
    else:
        lines.append("- no trades")
    return "\n".join(lines).strip() + "\n"


def _build_performance_breakdown(metric: dict[str, Any] | None) -> dict[str, Any]:
    if not isinstance(metric, dict):
        return {}
    return {
        "gross": {
            "daily_pnl": metric.get("gross_daily_pnl"),
            "cum_pnl": metric.get("gross_cum_pnl"),
            "cum_return_pct": metric.get("gross_cum_return_pct"),
        },
        "net": {
            "daily_pnl": metric.get("net_daily_pnl", metric.get("daily_pnl")),
            "cum_pnl": metric.get("net_cum_pnl", metric.get("cum_pnl")),
            "cum_return_pct": metric.get("net_cum_return_pct", metric.get("cum_return_pct")),
        },
        "costs": {
            "fees_daily": metric.get("fees_cost_daily"),
            "slippage_daily": metric.get("slippage_cost_daily"),
            "borrow_daily": metric.get("borrow_cost_daily"),
            "fees_cum": metric.get("fees_cost_cum"),
            "slippage_cum": metric.get("slippage_cost_cum"),
            "borrow_cum": metric.get("borrow_cost_cum"),
        },
        "turnover": {
            "daily_notional": metric.get("turnover_notional_daily"),
            "cum_notional": metric.get("turnover_notional_cum"),
            "daily_pct": metric.get("turnover_pct_daily"),
        },
        "exposure": {
            "long_units": metric.get("long_units"),
            "short_units": metric.get("short_units"),
            "gross_units": metric.get("gross_units"),
            "net_units": metric.get("net_units"),
            "net_exposure_pct": metric.get("net_exposure_pct"),
        },
        "sensitivity": metric.get("cost_sensitivity") if isinstance(metric.get("cost_sensitivity"), list) else [],
    }


def _evaluate_risk_gate(
    *,
    repo: ToredexRepository,
    cfg: Any,
    season_id: str,
    as_of_date: date,
    metric: dict[str, Any],
) -> tuple[bool, str, dict[str, Any]]:
    mode = str(getattr(cfg, "operating_mode", "champion") or "champion").lower()
    gates = cfg.risk_gates if hasattr(cfg, "risk_gates") else {}
    rule = gates.get(mode) if isinstance(gates, dict) else None
    if not isinstance(rule, dict) or not bool(rule.get("enabled", True)):
        return True, "", {"mode": mode, "enabled": False}

    max_dd_threshold = float(rule.get("maxDrawdownPct", -8.0))
    worst_month_threshold = float(rule.get("worstMonthPct", -8.0))
    max_turnover_threshold = float(rule.get("maxTurnoverPctPerMonth", 300.0))
    max_net_units_threshold = float(rule.get("maxNetExposureUnits", 2.0))

    max_dd = float(metric.get("max_drawdown_pct") or 0.0)
    worst_month = repo.get_worst_month_return_pct(season_id, before_or_equal=as_of_date)
    month_turnover_max = repo.get_max_turnover_pct_per_month(season_id, before_or_equal=as_of_date)
    max_abs_net_units = repo.get_max_abs_net_units(season_id, before_or_equal=as_of_date)
    if max_abs_net_units is None:
        now_net_units = metric.get("net_units")
        max_abs_net_units = abs(float(now_net_units)) if now_net_units is not None else 0.0

    failures: list[str] = []
    if max_dd <= max_dd_threshold:
        failures.append(f"MAX_DD({max_dd:.4f}<= {max_dd_threshold:.4f})")
    if worst_month is not None and worst_month <= worst_month_threshold:
        failures.append(f"WORST_MONTH({worst_month:.4f}<= {worst_month_threshold:.4f})")
    if month_turnover_max is not None and month_turnover_max > max_turnover_threshold:
        failures.append(f"TURNOVER({month_turnover_max:.4f}> {max_turnover_threshold:.4f})")
    if max_abs_net_units is not None and max_abs_net_units > max_net_units_threshold:
        failures.append(f"NET_UNITS({max_abs_net_units:.4f}> {max_net_units_threshold:.4f})")

    passed = len(failures) == 0
    reason = ";".join(failures)
    details = {
        "mode": mode,
        "enabled": True,
        "thresholds": {
            "maxDrawdownPct": max_dd_threshold,
            "worstMonthPct": worst_month_threshold,
            "maxTurnoverPctPerMonth": max_turnover_threshold,
            "maxNetExposureUnits": max_net_units_threshold,
        },
        "observed": {
            "maxDrawdownPct": max_dd,
            "worstMonthPct": worst_month,
            "maxTurnoverPctPerMonth": month_turnover_max,
            "maxNetExposureUnits": max_abs_net_units,
        },
        "pass": passed,
        "reason": reason,
    }
    return passed, reason, details


def _run_for_as_of(
    *,
    repo: ToredexRepository,
    cfg: Any,
    season_id: str,
    as_of_date: date,
    mode: str,
    dry_run: bool,
) -> dict[str, Any]:
    as_of_iso = as_of_date.isoformat()

    if not repo.has_market_close_on_day(as_of_date):
        return {
            "ok": True,
            "status": "skipped",
            "reason": "K_MARKET_CLOSED",
            "season_id": season_id,
            "asOf": as_of_iso,
            "mode": mode,
            "dry_run": bool(dry_run),
        }

    cfg_json = json.dumps(cfg.data, ensure_ascii=False, sort_keys=True)
    repo.ensure_season(
        season_id=season_id,
        mode=mode,
        start_date=as_of_date,
        initial_cash=cfg.initial_cash,
        policy_version=cfg.policy_version,
        config_json=cfg_json,
        config_hash=cfg.config_hash,
    )

    existing_snapshot = repo.get_snapshot_row(season_id, as_of_date)
    existing_decision = repo.get_decision_row(season_id, as_of_date)
    if existing_decision:
        if not existing_snapshot:
            raise RuntimeError("K_POLICY_INCONSISTENT: decision exists without snapshot")
        return {
            "ok": True,
            "status": "noop",
            "reason": "idempotent",
            "season_id": season_id,
            "asOf": as_of_iso,
            "mode": mode,
            "snapshot_hash": str(existing_snapshot.get("snapshot_hash") or ""),
            "decision_hash": str(existing_decision.get("decision_hash") or ""),
            "trade_count": int(repo.count_trades_on_day(season_id, as_of_date)),
            "dry_run": bool(dry_run),
        }

    snapshot = repo.get_snapshot_payload(season_id, as_of_date) if existing_snapshot else None
    if not isinstance(snapshot, dict):
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
            "mode": mode,
            "dry_run": bool(dry_run),
        }

    snapshot_hash = hash_payload(snapshot)

    prev_metric = repo.get_latest_metrics(season_id, before_or_equal=as_of_date - timedelta(days=1))
    decision = build_decision(snapshot=snapshot, config=cfg, prev_metrics=prev_metric, mode=mode)
    checks = decision.get("checks") if isinstance(decision.get("checks"), dict) else {}
    if not (
        bool(checks.get("maxHoldingsOk"))
        and bool(checks.get("unitRuleOk"))
        and bool(checks.get("lossLimitOk"))
        and bool(checks.get("exposureOk", True))
        and bool(checks.get("noFutureLeakOk"))
    ):
        raise RuntimeError("K_POLICY_INCONSISTENT: decision checks failed")
    if _contains_forbidden_keys(decision, _FORBIDDEN_DECISION_KEYS):
        raise RuntimeError("K_POLICY_INCONSISTENT: environment-dependent fields in decision")
    _validate_decision_actions(decision)
    decision_hash = hash_payload(decision, exclude_fields={"createdAt", "runtime", "path", "realPath", "host"})

    if existing_snapshot and str(existing_snapshot.get("snapshot_hash") or "") != str(snapshot_hash):
        raise RuntimeError("K_POLICY_INCONSISTENT: snapshot hash mismatch on rerun")

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
            "mode": mode,
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
    metric_payload = execution.get("metrics") if isinstance(execution, dict) and isinstance(execution.get("metrics"), dict) else {}
    if isinstance(metric_payload, dict) and metric_payload:
        gate_pass, gate_reason, gate_details = _evaluate_risk_gate(
            repo=repo,
            cfg=cfg,
            season_id=season_id,
            as_of_date=as_of_date,
            metric=metric_payload,
        )
        metric_payload["risk_gate_pass"] = bool(gate_pass)
        metric_payload["risk_gate_reason"] = str(gate_reason)
        metric_payload["risk_gate_details"] = gate_details
        repo.save_daily_metrics(metric_payload)
        execution["metrics"] = metric_payload

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

    month_kpi_payload = _build_monthly_payload(
        repo=repo,
        cfg=cfg,
        season_id=season_id,
        as_of_date=as_of_date,
    )
    _write_json(monthly_paths["kpi"], month_kpi_payload)
    _write_text(monthly_paths["summary"], _build_monthly_summary_md(month_kpi_payload))

    daily_narrative = _build_daily_narrative(
        snapshot=snapshot,
        decision=decision,
        metrics=execution.get("metrics") if isinstance(execution, dict) else None,
    )
    _write_text(daily_paths["narrative"], daily_narrative)

    config_file_path = Path(runs_root) / season_id / "config.json"
    config_file_path.parent.mkdir(parents=True, exist_ok=True)
    _write_json(str(config_file_path), cfg.data)

    repo.save_log(season_id, as_of_date, f"{daily_paths['logical_dir']}/narrative.md", "NARRATIVE")
    repo.save_log(season_id, as_of_date, f"{monthly_paths['logical_dir']}/summary.md", "MONTHLY_SUMMARY")
    if isinstance(metric_payload, dict) and bool(metric_payload.get("game_over")):
        repo.set_season_end_date(season_id, as_of_date)

    return {
        "ok": True,
        "status": "success",
        "season_id": season_id,
        "asOf": as_of_iso,
        "mode": mode,
        "snapshot_hash": snapshot_hash,
        "decision_hash": decision_hash,
        "snapshot_path": logical_snapshot_path,
        "decision_path": logical_decision_path,
        "trade_count": len(execution.get("trades") or []),
        "metrics": _json_ready(execution.get("metrics")),
        "performance_breakdown": _build_performance_breakdown(metric_payload if isinstance(metric_payload, dict) else None),
        "risk_gate": {
            "pass": bool(metric_payload.get("risk_gate_pass")) if isinstance(metric_payload, dict) else True,
            "reason": str(metric_payload.get("risk_gate_reason") or "") if isinstance(metric_payload, dict) else "",
            "details": metric_payload.get("risk_gate_details") if isinstance(metric_payload, dict) else None,
        },
    }


def _load_runtime_config(
    *,
    repo: ToredexRepository,
    season_id: str,
    config_override: dict[str, Any] | None,
) -> ToredexConfig:
    cfg = load_toredex_config(override=config_override)
    if isinstance(config_override, dict) and config_override:
        return cfg
    season = repo.get_season(season_id)
    if not isinstance(season, dict):
        return cfg
    season_config_json = season.get("config_json")
    if not isinstance(season_config_json, str) or not season_config_json.strip():
        return cfg
    try:
        season_config = json.loads(season_config_json)
    except Exception:
        return cfg
    if not isinstance(season_config, dict):
        return cfg
    season_hash = str(season.get("config_hash") or "")
    if not season_hash:
        season_hash = hash_payload(season_config)
    return ToredexConfig(data=season_config, config_hash=season_hash)


def run_live(
    *,
    season_id: str,
    as_of: str | None = None,
    dry_run: bool = False,
    config_override: dict[str, Any] | None = None,
) -> dict[str, Any]:
    with get_conn() as conn:
        repo = ToredexRepository(conn=conn)
        cfg = _load_runtime_config(repo=repo, season_id=season_id, config_override=config_override)
        as_of_date = _resolve_as_of(repo, as_of)
        return _run_for_as_of(
            repo=repo,
            cfg=cfg,
            season_id=season_id,
            as_of_date=as_of_date,
            mode="LIVE",
            dry_run=dry_run,
        )


def run_backtest(
    *,
    season_id: str,
    start_date: str,
    end_date: str,
    dry_run: bool = False,
    config_override: dict[str, Any] | None = None,
) -> dict[str, Any]:
    start = _parse_required_date(start_date, name="start_date")
    end = _parse_required_date(end_date, name="end_date")
    if end < start:
        raise ValueError("end_date must be >= start_date")

    status_counts: Counter[str] = Counter()
    days_payload: list[dict[str, Any]] = []
    stopped_reason: str | None = None
    last_processed: date | None = None
    month_last_equity: dict[str, float] = {}
    month_turnover_pct: dict[str, float] = {}
    max_abs_net_units: float | None = None

    with get_conn() as conn:
        repo = ToredexRepository(conn=conn)
        cfg = _load_runtime_config(repo=repo, season_id=season_id, config_override=config_override)
        for day_idx, day in enumerate(_iter_days(start, end), start=1):
            _clear_rankings_caches_periodically(day_idx)
            result = _run_for_as_of(
                repo=repo,
                cfg=cfg,
                season_id=season_id,
                as_of_date=day,
                mode="BACKTEST",
                dry_run=dry_run,
            )
            status = str(result.get("status") or "unknown")
            status_counts[status] += 1
            days_payload.append(
                {
                    "asOf": str(result.get("asOf") or day.isoformat()),
                    "status": status,
                    "reason": result.get("reason"),
                    "trade_count": int(result.get("trade_count") or 0),
                }
            )
            last_processed = day
            if dry_run:
                continue
            metric = repo.get_latest_metrics(season_id, before_or_equal=day)
            metric_day_value = None
            if isinstance(metric, dict):
                metric_as_of = metric.get("asOf")
                if isinstance(metric_as_of, date):
                    metric_day_value = metric_as_of
                elif isinstance(metric_as_of, datetime):
                    metric_day_value = metric_as_of.date()
                elif isinstance(metric_as_of, str):
                    metric_day_value = _parse_as_of(metric_as_of)
            metric_is_current_day = bool(metric_day_value == day)

            if metric_is_current_day and isinstance(metric, dict):
                ym_key = day.strftime("%Y-%m")
                equity_value = metric.get("equity")
                if isinstance(equity_value, (int, float)):
                    month_last_equity[ym_key] = float(equity_value)
                turnover_daily = metric.get("turnover_pct_daily")
                if isinstance(turnover_daily, (int, float)):
                    month_turnover_pct[ym_key] = float(month_turnover_pct.get(ym_key, 0.0)) + float(turnover_daily)
                net_units_value = metric.get("net_units")
                if isinstance(net_units_value, (int, float)):
                    current_abs = abs(float(net_units_value))
                    if max_abs_net_units is None or current_abs > max_abs_net_units:
                        max_abs_net_units = current_abs
            if metric and bool(metric.get("game_over")):
                stopped_reason = "R_GAME_OVER"
                break
            if metric and metric.get("risk_gate_pass") is False:
                stopped_reason = "R_RISK_GATE_FAIL"
                break

        if not dry_run and last_processed is not None:
            repo.set_season_end_date(season_id, last_processed)

        final_metric = None
        if not dry_run and last_processed is not None:
            final_metric = repo.get_latest_metrics(season_id, before_or_equal=last_processed)
        reason_counts: dict[str, int] = {}
        if not dry_run:
            try:
                reason_rows = repo.get_trade_reason_counts(season_id)
            except Exception:
                reason_rows = []
            if isinstance(reason_rows, list):
                for row in reason_rows:
                    if not isinstance(row, dict):
                        continue
                    reason_id = str(row.get("reason_id") or "").strip()
                    if not reason_id:
                        continue
                    try:
                        count_value = int(row.get("count") or 0)
                    except Exception:
                        count_value = 0
                    if count_value <= 0:
                        continue
                    reason_counts[reason_id] = count_value

        worst_month_pct: float | None = None
        prev_month_equity: float | None = None
        for ym_key in sorted(month_last_equity.keys()):
            current_equity = float(month_last_equity[ym_key])
            if prev_month_equity is not None and prev_month_equity > 0:
                month_ret_pct = (current_equity / prev_month_equity - 1.0) * 100.0
                if worst_month_pct is None or month_ret_pct < worst_month_pct:
                    worst_month_pct = float(month_ret_pct)
            prev_month_equity = current_equity
        max_turnover_pct_per_month = max(month_turnover_pct.values()) if month_turnover_pct else None

        return {
            "ok": True,
            "status": "success",
            "mode": "BACKTEST",
            "season_id": season_id,
            "start_date": start.isoformat(),
            "end_date": end.isoformat(),
            "processed_days": int(len(days_payload)),
            "status_counts": dict(status_counts),
            "stopped_early": bool(stopped_reason),
            "stop_reason": stopped_reason,
            "final_metrics": _json_ready(final_metric) if isinstance(final_metric, dict) else None,
            "performance_breakdown": _build_performance_breakdown(final_metric if isinstance(final_metric, dict) else None),
            "risk_gate": {
                "pass": bool(final_metric.get("risk_gate_pass")) if isinstance(final_metric, dict) else True,
                "reason": str(final_metric.get("risk_gate_reason") or "") if isinstance(final_metric, dict) else "",
                "details": final_metric.get("risk_gate_details") if isinstance(final_metric, dict) else None,
            },
            "rollup": {
                "worst_month_pct": (round(float(worst_month_pct), 6) if worst_month_pct is not None else None),
                "max_turnover_pct_per_month": (
                    round(float(max_turnover_pct_per_month), 6) if max_turnover_pct_per_month is not None else None
                ),
                "max_abs_net_units": (round(float(max_abs_net_units), 6) if max_abs_net_units is not None else None),
            },
            "reason_counts": reason_counts,
            "days": days_payload,
        }
