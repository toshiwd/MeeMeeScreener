from __future__ import annotations

import argparse
import json
import math
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd


AXIS_ID = "current_buyable_invalidation_tracking_status_v1"
DEFAULT_INVALIDATION_ROOT = Path(
    r"G:\Tradex\current_buyable_invalidation_contract_v2_apply\20260526T014806Z-current-buyable-invalidation-contract-v2-apply"
)
DEFAULT_FORWARD_ROOT = Path(
    r"G:\Tradex\current_buyable_forward_paper_validation_v1\20260526T010838Z-current-buyable-forward-paper-validation-v1"
)
DEFAULT_SOURCE_DB = Path(r"C:\Users\enish\AppData\Local\MeeMeeScreener\data\stocks.duckdb")
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\current_buyable_invalidation_tracking_status_v1")
REQUIRED_ARTIFACTS = (
    "invalidation_tracking_summary.json",
    "invalidation_tracking_rows.csv",
    "candidate_invalidation_status.json",
    "no_lookahead_audit.json",
    "source_coverage.json",
    "research_decision.json",
    "_ARTIFACT_COMPLETE.json",
)


def _now_tag() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _json_ready(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(k): _json_ready(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_json_ready(v) for v in value]
    if isinstance(value, Path):
        return str(value)
    if hasattr(value, "item"):
        return _json_ready(value.item())
    if isinstance(value, float) and not math.isfinite(value):
        return None
    try:
        if pd.isna(value):
            return None
    except Exception:
        pass
    return value


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(_json_ready(payload), ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def load_contract(invalidation_root: Path) -> tuple[pd.DataFrame, dict[str, Any]]:
    rows_path = invalidation_root / "invalidation_contract_repair_rows.csv"
    if not rows_path.exists():
        rows_path = invalidation_root / "invalidation_contract_rows.csv"
    if not rows_path.exists():
        rows_path = invalidation_root / "invalidation_contract_v2_rows.csv"
    decision_path = invalidation_root / "research_decision.json"
    if not rows_path.exists():
        raise FileNotFoundError(rows_path)
    if not decision_path.exists():
        raise FileNotFoundError(decision_path)
    rows = pd.read_csv(rows_path, dtype={"code": str})
    rows["as_of_date"] = pd.to_numeric(rows["as_of_date"], errors="coerce").astype("Int64")
    rows["primary_invalidation_level"] = pd.to_numeric(rows["primary_invalidation_level"], errors="coerce")
    return rows, _load_json(decision_path)


def load_bars(source_db: Path, codes: list[str], as_of_date: int) -> pd.DataFrame:
    if not source_db.exists():
        raise FileNotFoundError(source_db)
    con = duckdb.connect(str(source_db), read_only=True)
    try:
        expr = "CAST(strftime(to_timestamp(date), '%Y%m%d') AS INTEGER)"
        query = f"""
            SELECT CAST(code AS VARCHAR) AS code, {expr} AS bar_date, o AS open, h AS high, l AS low, c AS close
            FROM daily_bars
            WHERE CAST(code AS VARCHAR) IN (SELECT * FROM UNNEST(?))
              AND {expr} >= ?
            ORDER BY code, bar_date
        """
        return con.execute(query, [codes, int(as_of_date)]).fetchdf()
    finally:
        con.close()


def build_tracking_rows(contract: pd.DataFrame, bars: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    by_code = {str(code): grp.sort_values("bar_date") for code, grp in bars.groupby("code")}
    for item in contract.itertuples(index=False):
        code = str(item.code)
        as_of = int(item.as_of_date)
        level = float(item.primary_invalidation_level)
        grp = by_code.get(code, pd.DataFrame(columns=["bar_date", "open", "high", "low", "close"]))
        future = grp[pd.to_numeric(grp["bar_date"], errors="coerce") > as_of].sort_values("bar_date")
        hit_rows = future[pd.to_numeric(future["close"], errors="coerce") < level]
        latest = future.iloc[-1] if not future.empty else None
        rows.append(
            {
                "as_of_date": as_of,
                "code": code,
                "primary_invalidation_level": level,
                "invalidation_reason": getattr(item, "invalidation_reason", ""),
                "future_confirmed_session_count": int(len(future)),
                "latest_confirmed_bar_date": int(latest["bar_date"]) if latest is not None else None,
                "latest_confirmed_close": float(latest["close"]) if latest is not None else None,
                "latest_close_vs_invalidation_level": float(latest["close"]) / level - 1.0 if latest is not None and level else None,
                "invalidation_hit": bool(not hit_rows.empty),
                "first_invalidation_hit_date": int(hit_rows.iloc[0]["bar_date"]) if not hit_rows.empty else None,
                "first_invalidation_hit_close": float(hit_rows.iloc[0]["close"]) if not hit_rows.empty else None,
                "tracking_status": "invalidated" if not hit_rows.empty else ("active_pending_ret5" if len(future) < 5 else "active_ret5_ready_or_later"),
            }
        )
    return pd.DataFrame(rows)


def no_lookahead_audit(contract_decision: dict[str, Any], forward_decision: dict[str, Any]) -> dict[str, Any]:
    contract_ok = contract_decision.get("research_decision") in {
        "invalidation_contract_repaired_full_levels_ready",
        "invalidation_contract_ready_for_forward_tracking",
        "invalidation_contract_v2_stop_atr2_ready_for_forward_tracking",
    }
    forward_ok = forward_decision.get("research_decision") == "forward_validation_pending_more_confirmed_bars"
    passed = bool(contract_ok and forward_ok)
    return {
        "audit_result": "pass" if passed else "blocked",
        "no_lookahead_pass": passed,
        "invalidation_contract_ready": contract_ok,
        "forward_validation_pending": forward_ok,
        "future_bars_used_only_for_tracking_after_freeze": True,
        "future_outcomes_used_for_selection": False,
        "runtime_db_write": False,
        "research_fallback_used": False,
    }


def decide(rows: pd.DataFrame, audit: dict[str, Any]) -> tuple[str, str, list[str]]:
    if not audit["no_lookahead_pass"]:
        return "blocked_no_lookahead_violation", "BLOCKED", ["tracking_inputs_failed_no_lookahead_contract"]
    if rows["invalidation_hit"].any():
        return "current_candidate_invalidation_hit_close_or_review", "DROP", ["one_or_more_candidates_hit_primary_invalidation_level"]
    return "current_candidates_active_no_invalidation_hit", "HOLD_UNDERPOWERED", ["no_primary_invalidation_hit_ret5_ret20_still_pending"]


def run(
    invalidation_root: Path = DEFAULT_INVALIDATION_ROOT,
    forward_root: Path = DEFAULT_FORWARD_ROOT,
    source_db: Path = DEFAULT_SOURCE_DB,
    output_root: Path = DEFAULT_OUTPUT_ROOT,
) -> Path:
    contract, contract_decision = load_contract(invalidation_root)
    forward_decision = _load_json(forward_root / "research_decision.json")
    as_of_date = int(contract["as_of_date"].min())
    codes = sorted(contract["code"].astype(str).unique().tolist())
    bars = load_bars(source_db, codes, as_of_date)
    tracking = build_tracking_rows(contract, bars)
    audit = no_lookahead_audit(contract_decision, forward_decision)
    decision, decision_class, reasons = decide(tracking, audit)

    out = output_root / f"{_now_tag()}-current-buyable-invalidation-tracking-status-v1"
    out.mkdir(parents=True, exist_ok=True)
    tracking.to_csv(out / "invalidation_tracking_rows.csv", index=False)
    _write_json(
        out / "invalidation_tracking_summary.json",
        {
            "axis_id": AXIS_ID,
            "decision": decision,
            "decision_class": decision_class,
            "reason_typed": reasons,
            "candidate_count": int(len(tracking)),
            "invalidation_hit_count": int(tracking["invalidation_hit"].sum()) if not tracking.empty else 0,
            "minimum_future_confirmed_sessions": int(tracking["future_confirmed_session_count"].min()) if not tracking.empty else 0,
            "validated_buy_count": 0,
            "forward_validation_remains_pending": True,
        },
    )
    _write_json(out / "candidate_invalidation_status.json", {"axis_id": AXIS_ID, "candidates": tracking.to_dict(orient="records")})
    _write_json(out / "no_lookahead_audit.json", audit)
    _write_json(
        out / "source_coverage.json",
        {
            "axis_id": AXIS_ID,
            "invalidation_root": str(invalidation_root),
            "forward_root": str(forward_root),
            "source_db": str(source_db),
            "candidate_count": int(len(contract)),
            "bar_row_count_for_candidate_codes": int(len(bars)),
            "latest_bar_date_for_candidate_codes": int(bars["bar_date"].max()) if not bars.empty else None,
            "research_fallback_used": False,
        },
    )
    _write_json(
        out / "research_decision.json",
        {
            "axis_id": AXIS_ID,
            "research_decision": decision,
            "decision_class": decision_class,
            "reason_typed": reasons,
            "research_watch_only": True,
            "buyable_selection_ready": False,
            "production_ready": False,
            "meemee_reflectable_candidate": False,
            "runtime_db_write": False,
            "production_ranking_changed": False,
            "production_candidate_generator_changed": False,
            "publish_allowed": False,
            "validated_buy_count": 0,
            "active_gate_created": False,
            "research_fallback_used": False,
        },
    )
    _write_json(out / "_ARTIFACT_COMPLETE.json", {"complete": True, "artifact_complete": True, "required_artifacts": list(REQUIRED_ARTIFACTS), "generated_at": _now_tag()})
    return out


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--invalidation-root", type=Path, default=DEFAULT_INVALIDATION_ROOT)
    parser.add_argument("--forward-root", type=Path, default=DEFAULT_FORWARD_ROOT)
    parser.add_argument("--source-db", type=Path, default=DEFAULT_SOURCE_DB)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    args = parser.parse_args(argv)
    out = run(args.invalidation_root, args.forward_root, args.source_db, args.output_root)
    print(json.dumps({"output_dir": str(out)}, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
