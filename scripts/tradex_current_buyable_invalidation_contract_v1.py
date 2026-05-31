from __future__ import annotations

import argparse
import json
import math
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd


AXIS_ID = "current_buyable_invalidation_contract_v1"
CONTRACT_VERSION = "current_buyable_invalidation_contract_v1"
DEFAULT_FORWARD_ROOT = Path(
    r"G:\Tradex\current_buyable_forward_paper_validation_v1\20260526T010838Z-current-buyable-forward-paper-validation-v1"
)
DEFAULT_SOURCE_DB = Path(r"C:\Users\enish\AppData\Local\MeeMeeScreener\data\stocks.duckdb")
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\current_buyable_invalidation_contract_v1")
REQUIRED_ARTIFACTS = (
    "invalidation_contract_summary.json",
    "invalidation_contract_rows.csv",
    "invalidation_level_contract.json",
    "candidate_freeze_reference.json",
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


def load_freeze(forward_root: Path) -> tuple[pd.DataFrame, dict[str, Any], dict[str, Any]]:
    rows_path = forward_root / "forward_paper_validation_rows.csv"
    contract_path = forward_root / "candidate_freeze_contract.json"
    decision_path = forward_root / "research_decision.json"
    if not rows_path.exists():
        raise FileNotFoundError(rows_path)
    if not contract_path.exists():
        raise FileNotFoundError(contract_path)
    if not decision_path.exists():
        raise FileNotFoundError(decision_path)
    rows = pd.read_csv(rows_path, dtype={"code": str})
    rows["as_of_date"] = pd.to_numeric(rows["as_of_date"], errors="coerce").astype("Int64")
    return rows, _load_json(contract_path), _load_json(decision_path)


def load_feature_snapshot(source_db: Path, codes: list[str], as_of_date: int) -> pd.DataFrame:
    if not source_db.exists():
        raise FileNotFoundError(source_db)
    con = duckdb.connect(str(source_db), read_only=True)
    try:
        return con.execute(
            """
            SELECT CAST(code AS VARCHAR) AS code, dt AS as_of_date, close, ma7, ma20, ma60, atr14
            FROM feature_snapshot_daily
            WHERE CAST(code AS VARCHAR) IN (SELECT * FROM UNNEST(?))
              AND dt = ?
            ORDER BY code
            """,
            [codes, int(as_of_date)],
        ).fetchdf()
    finally:
        con.close()


def load_recent_bars(source_db: Path, codes: list[str], as_of_date: int, lookback_rows: int = 20) -> pd.DataFrame:
    if not source_db.exists():
        raise FileNotFoundError(source_db)
    con = duckdb.connect(str(source_db), read_only=True)
    try:
        expr = "CAST(strftime(to_timestamp(date), '%Y%m%d') AS INTEGER)"
        query = f"""
            SELECT CAST(code AS VARCHAR) AS code, {expr} AS bar_date, l AS low
            FROM daily_bars
            WHERE CAST(code AS VARCHAR) IN (SELECT * FROM UNNEST(?))
              AND {expr} <= ?
            ORDER BY code, bar_date
        """
        bars = con.execute(query, [codes, int(as_of_date)]).fetchdf()
    finally:
        con.close()
    return bars.groupby("code", group_keys=False).tail(lookback_rows).copy()


def recent_swing_lows(bars: pd.DataFrame) -> pd.DataFrame:
    if bars.empty:
        return pd.DataFrame(columns=["code", "recent_swing_low"])
    return bars.groupby("code", as_index=False)["low"].min().rename(columns={"low": "recent_swing_low"})


def stricter_level(row: pd.Series) -> tuple[float | None, str]:
    candidates: list[tuple[str, float]] = []
    for name in ["invalidation_close_below_ma20_flag_level", "invalidation_close_below_recent_low_flag_level", "invalidation_atr_stop_level"]:
        value = row.get(name)
        if pd.notna(value):
            candidates.append((name, float(value)))
    if not candidates:
        return None, "no_point_in_time_level_available"
    # For a long candidate, the stricter stop is the highest valid invalidation level below/near entry.
    name, value = max(candidates, key=lambda item: item[1])
    return value, name


def build_invalidation_rows(candidates: pd.DataFrame, snapshot: pd.DataFrame, swing_lows: pd.DataFrame) -> pd.DataFrame:
    rows = candidates[["as_of_date", "code", "entry_close"]].copy()
    rows = rows.rename(columns={"entry_close": "entry_reference_close"})
    rows = rows.merge(snapshot, on=["as_of_date", "code"], how="left", validate="one_to_one")
    rows = rows.merge(swing_lows, on="code", how="left", validate="one_to_one")
    rows["invalidation_close_below_ma20_flag_level"] = rows["ma20"]
    rows["invalidation_close_below_recent_low_flag_level"] = rows["recent_swing_low"]
    rows["invalidation_atr_stop_level"] = rows["entry_reference_close"] - rows["atr14"]
    levels = rows.apply(stricter_level, axis=1, result_type="expand")
    rows["primary_invalidation_level"] = levels[0]
    rows["invalidation_reason"] = levels[1]
    rows["contract_version"] = CONTRACT_VERSION
    return rows[
        [
            "as_of_date",
            "code",
            "entry_reference_close",
            "ma7",
            "ma20",
            "ma60",
            "atr14",
            "recent_swing_low",
            "invalidation_close_below_ma20_flag_level",
            "invalidation_close_below_recent_low_flag_level",
            "invalidation_atr_stop_level",
            "primary_invalidation_level",
            "invalidation_reason",
            "contract_version",
        ]
    ]


def no_lookahead_audit(rows: pd.DataFrame, freeze_contract: dict[str, Any], forward_decision: dict[str, Any]) -> dict[str, Any]:
    freeze_ok = bool(freeze_contract.get("no_candidate_replacement") is True and freeze_contract.get("validated_buy_count_at_projection") == 0)
    forward_pending_ok = forward_decision.get("research_decision") == "forward_validation_pending_more_confirmed_bars"
    outcomes_present = sorted({"ret5", "ret20", "winner_ret20_gt_10pct", "bad_ret20_lt_minus_5pct", "severe_ret20_lt_minus_10pct"} & set(rows.columns))
    level_ready = bool(rows["primary_invalidation_level"].notna().all()) if not rows.empty else False
    passed = bool(freeze_ok and forward_pending_ok and not outcomes_present and level_ready)
    return {
        "audit_result": "pass" if passed else "blocked",
        "no_lookahead_pass": passed,
        "candidate_freeze_ok": freeze_ok,
        "forward_validation_pending": forward_pending_ok,
        "future_outcome_columns_present": outcomes_present,
        "levels_built_from_asof_snapshot_and_prior_confirmed_bars": True,
        "runtime_db_write": False,
        "research_fallback_used": False,
    }


def decide(rows: pd.DataFrame, audit: dict[str, Any]) -> tuple[str, str, list[str]]:
    if not audit["no_lookahead_pass"]:
        if not rows.empty and rows["primary_invalidation_level"].notna().any():
            return "invalidation_contract_created_with_partial_levels", "HOLD_UNDERPOWERED", ["some_invalidation_levels_available_but_contract_not_complete"]
        return "blocked_missing_point_in_time_features", "BLOCKED", ["required_ma_atr_or_recent_low_levels_missing"]
    return "invalidation_contract_ready_for_forward_tracking", "KEEP", ["point_in_time_invalidation_levels_ready_for_frozen_candidates"]


def run(
    forward_root: Path = DEFAULT_FORWARD_ROOT,
    source_db: Path = DEFAULT_SOURCE_DB,
    output_root: Path = DEFAULT_OUTPUT_ROOT,
) -> Path:
    candidates, freeze_contract, forward_decision = load_freeze(forward_root)
    as_of_date = int(freeze_contract["selected_as_of_date"])
    codes = [str(code) for code in freeze_contract["selected_codes"]]
    candidates = candidates[candidates["code"].astype(str).isin(codes)].copy()
    snapshot = load_feature_snapshot(source_db, codes, as_of_date)
    bars = load_recent_bars(source_db, codes, as_of_date)
    rows = build_invalidation_rows(candidates, snapshot, recent_swing_lows(bars))
    audit = no_lookahead_audit(rows, freeze_contract, forward_decision)
    decision, decision_class, reasons = decide(rows, audit)

    out = output_root / f"{_now_tag()}-current-buyable-invalidation-contract-v1"
    out.mkdir(parents=True, exist_ok=True)
    rows.to_csv(out / "invalidation_contract_rows.csv", index=False)
    _write_json(
        out / "invalidation_contract_summary.json",
        {
            "axis_id": AXIS_ID,
            "decision": decision,
            "decision_class": decision_class,
            "reason_typed": reasons,
            "selected_as_of_date": as_of_date,
            "selected_codes": codes,
            "candidate_count": int(len(rows)),
            "complete_level_count": int(rows["primary_invalidation_level"].notna().sum()) if not rows.empty else 0,
            "validated_buy_count": 0,
            "forward_validation_remains_pending": True,
        },
    )
    _write_json(
        out / "invalidation_level_contract.json",
        {
            "axis_id": AXIS_ID,
            "contract_version": CONTRACT_VERSION,
            "rule": "primary_invalidation_level_is_highest_available_long_stop_from_ma20_recent_swing_low_or_entry_minus_atr14",
            "required_inputs": ["entry_reference_close", "ma20", "recent_swing_low", "atr14"],
            "optional_inputs": ["ma7", "ma60"],
            "future_bars_used_to_set_level": False,
            "outcomes_used": False,
            "validated_buy_count": 0,
        },
    )
    _write_json(out / "candidate_freeze_reference.json", {"axis_id": AXIS_ID, "forward_root": str(forward_root), "candidate_freeze_contract": freeze_contract, "forward_decision": forward_decision})
    _write_json(out / "no_lookahead_audit.json", audit)
    _write_json(
        out / "source_coverage.json",
        {
            "axis_id": AXIS_ID,
            "source_db": str(source_db),
            "forward_root": str(forward_root),
            "candidate_count": int(len(candidates)),
            "feature_snapshot_row_count": int(len(snapshot)),
            "recent_bar_row_count": int(len(bars)),
            "feature_snapshot_complete": int(len(snapshot)) == len(codes),
            "recent_swing_low_complete": int(rows["recent_swing_low"].notna().sum()) == len(rows) if not rows.empty else False,
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
    parser.add_argument("--forward-root", type=Path, default=DEFAULT_FORWARD_ROOT)
    parser.add_argument("--source-db", type=Path, default=DEFAULT_SOURCE_DB)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    args = parser.parse_args(argv)
    out = run(args.forward_root, args.source_db, args.output_root)
    print(json.dumps({"output_dir": str(out)}, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
