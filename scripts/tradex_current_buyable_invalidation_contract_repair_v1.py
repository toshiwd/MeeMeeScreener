from __future__ import annotations

import argparse
import json
import math
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd


AXIS_ID = "current_buyable_invalidation_contract_repair_v1"
CONTRACT_VERSION = "current_buyable_invalidation_contract_repair_v1"
DEFAULT_FORWARD_ROOT = Path(
    r"G:\Tradex\current_buyable_forward_paper_validation_v1\20260526T010838Z-current-buyable-forward-paper-validation-v1"
)
DEFAULT_PRIOR_INVALIDATION_ROOT = Path(
    r"G:\Tradex\current_buyable_invalidation_contract_v1\20260526T011557Z-current-buyable-invalidation-contract-v1"
)
DEFAULT_SOURCE_DB = Path(r"C:\Users\enish\AppData\Local\MeeMeeScreener\data\stocks.duckdb")
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\current_buyable_invalidation_contract_repair_v1")
REQUIRED_ARTIFACTS = (
    "invalidation_contract_repair_summary.json",
    "invalidation_contract_repair_rows.csv",
    "asof_ma_atr_contract.json",
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


def load_confirmed_bars(source_db: Path, codes: list[str], as_of_date: int) -> pd.DataFrame:
    if not source_db.exists():
        raise FileNotFoundError(source_db)
    con = duckdb.connect(str(source_db), read_only=True)
    try:
        expr = "CAST(strftime(to_timestamp(date), '%Y%m%d') AS INTEGER)"
        query = f"""
            SELECT CAST(code AS VARCHAR) AS code, {expr} AS bar_date, o AS open, h AS high, l AS low, c AS close
            FROM daily_bars
            WHERE CAST(code AS VARCHAR) IN (SELECT * FROM UNNEST(?))
              AND {expr} <= ?
            ORDER BY code, bar_date
        """
        return con.execute(query, [codes, int(as_of_date)]).fetchdf()
    finally:
        con.close()


def build_asof_features(bars: pd.DataFrame, as_of_date: int) -> pd.DataFrame:
    out = bars.sort_values(["code", "bar_date"]).copy()
    g = out.groupby("code", sort=False)
    out["ma7"] = g["close"].transform(lambda s: s.rolling(7, min_periods=7).mean())
    out["ma20"] = g["close"].transform(lambda s: s.rolling(20, min_periods=20).mean())
    out["ma60"] = g["close"].transform(lambda s: s.rolling(60, min_periods=60).mean())
    prev_close = g["close"].shift(1)
    tr = pd.concat([(out["high"] - out["low"]), (out["high"] - prev_close).abs(), (out["low"] - prev_close).abs()], axis=1).max(axis=1)
    out["atr14"] = tr.groupby(out["code"]).transform(lambda s: s.rolling(14, min_periods=14).mean())
    out["recent_swing_low"] = g["low"].transform(lambda s: s.rolling(20, min_periods=20).min())
    current = out[out["bar_date"] == int(as_of_date)].copy()
    return current.rename(columns={"bar_date": "as_of_date", "close": "entry_reference_close"})[
        ["as_of_date", "code", "entry_reference_close", "ma7", "ma20", "ma60", "atr14", "recent_swing_low"]
    ]


def primary_level(row: pd.Series) -> tuple[float | None, str]:
    options: list[tuple[str, float]] = []
    if pd.notna(row.get("ma20")):
        options.append(("invalidation_close_below_ma20_flag_level", float(row["ma20"])))
    if pd.notna(row.get("recent_swing_low")):
        options.append(("invalidation_close_below_recent_low_flag_level", float(row["recent_swing_low"])))
    if pd.notna(row.get("atr14")) and pd.notna(row.get("entry_reference_close")):
        options.append(("invalidation_atr_stop_level", float(row["entry_reference_close"]) - float(row["atr14"])))
    if not options:
        return None, "no_point_in_time_level_available"
    return max(options, key=lambda item: item[1])


def build_repaired_rows(candidates: pd.DataFrame, features: pd.DataFrame) -> pd.DataFrame:
    keys = candidates[["as_of_date", "code"]].copy()
    rows = keys.merge(features, on=["as_of_date", "code"], how="left", validate="one_to_one")
    rows["invalidation_close_below_ma20_flag_level"] = rows["ma20"]
    rows["invalidation_close_below_recent_low_flag_level"] = rows["recent_swing_low"]
    rows["invalidation_atr_stop_level"] = rows["entry_reference_close"] - rows["atr14"]
    levels = rows.apply(primary_level, axis=1, result_type="expand")
    rows["primary_invalidation_level"] = levels[1]
    rows["invalidation_reason"] = levels[0]
    rows["contract_version"] = CONTRACT_VERSION
    return rows


def no_lookahead_audit(rows: pd.DataFrame, freeze_contract: dict[str, Any], forward_decision: dict[str, Any]) -> dict[str, Any]:
    freeze_ok = bool(freeze_contract.get("no_candidate_replacement") is True and freeze_contract.get("validated_buy_count_at_projection") == 0)
    forward_ok = forward_decision.get("research_decision") == "forward_validation_pending_more_confirmed_bars"
    complete = bool(rows[["ma20", "atr14", "recent_swing_low", "primary_invalidation_level"]].notna().all().all()) if not rows.empty else False
    passed = bool(freeze_ok and forward_ok and complete)
    return {
        "audit_result": "pass" if passed else "blocked",
        "no_lookahead_pass": passed,
        "candidate_freeze_ok": freeze_ok,
        "forward_validation_pending": forward_ok,
        "all_required_levels_complete": complete,
        "features_built_from_confirmed_bars_on_or_before_as_of_date": True,
        "future_outcomes_used": False,
        "runtime_db_write": False,
        "research_fallback_used": False,
    }


def decide(audit: dict[str, Any]) -> tuple[str, str, list[str]]:
    if audit["no_lookahead_pass"]:
        return "invalidation_contract_repaired_full_levels_ready", "KEEP", ["asof_ma_atr_recent_low_levels_complete_for_frozen_candidates"]
    return "blocked_missing_point_in_time_features", "BLOCKED", ["asof_ma_atr_recent_low_levels_incomplete"]


def run(
    forward_root: Path = DEFAULT_FORWARD_ROOT,
    prior_invalidation_root: Path = DEFAULT_PRIOR_INVALIDATION_ROOT,
    source_db: Path = DEFAULT_SOURCE_DB,
    output_root: Path = DEFAULT_OUTPUT_ROOT,
) -> Path:
    candidates, freeze_contract, forward_decision = load_freeze(forward_root)
    as_of_date = int(freeze_contract["selected_as_of_date"])
    codes = [str(code) for code in freeze_contract["selected_codes"]]
    candidates = candidates[candidates["code"].astype(str).isin(codes)].copy()
    bars = load_confirmed_bars(source_db, codes, as_of_date)
    features = build_asof_features(bars, as_of_date)
    repaired = build_repaired_rows(candidates, features)
    audit = no_lookahead_audit(repaired, freeze_contract, forward_decision)
    decision, decision_class, reasons = decide(audit)

    out = output_root / f"{_now_tag()}-current-buyable-invalidation-contract-repair-v1"
    out.mkdir(parents=True, exist_ok=True)
    repaired.to_csv(out / "invalidation_contract_repair_rows.csv", index=False)
    _write_json(
        out / "invalidation_contract_repair_summary.json",
        {
            "axis_id": AXIS_ID,
            "decision": decision,
            "decision_class": decision_class,
            "reason_typed": reasons,
            "candidate_count": int(len(repaired)),
            "complete_level_count": int(repaired["primary_invalidation_level"].notna().sum()),
            "ma20_complete": bool(repaired["ma20"].notna().all()) if not repaired.empty else False,
            "atr14_complete": bool(repaired["atr14"].notna().all()) if not repaired.empty else False,
            "recent_swing_low_complete": bool(repaired["recent_swing_low"].notna().all()) if not repaired.empty else False,
            "validated_buy_count": 0,
            "forward_validation_remains_pending": True,
        },
    )
    _write_json(
        out / "asof_ma_atr_contract.json",
        {
            "axis_id": AXIS_ID,
            "contract_version": CONTRACT_VERSION,
            "ma7": "rolling_mean_close_7_confirmed_bars_including_as_of_date",
            "ma20": "rolling_mean_close_20_confirmed_bars_including_as_of_date",
            "ma60": "rolling_mean_close_60_confirmed_bars_including_as_of_date",
            "atr14": "rolling_mean_true_range_14_confirmed_bars_including_as_of_date",
            "recent_swing_low": "rolling_min_low_20_confirmed_bars_including_as_of_date",
            "primary_invalidation_level": "highest_available_long_stop_from_ma20_recent_swing_low_entry_minus_atr14",
            "future_bars_used": False,
            "outcomes_used": False,
        },
    )
    _write_json(
        out / "candidate_freeze_reference.json",
        {
            "axis_id": AXIS_ID,
            "forward_root": str(forward_root),
            "prior_invalidation_root": str(prior_invalidation_root),
            "candidate_freeze_contract": freeze_contract,
            "forward_decision": forward_decision,
        },
    )
    _write_json(out / "no_lookahead_audit.json", audit)
    _write_json(
        out / "source_coverage.json",
        {
            "axis_id": AXIS_ID,
            "source_db": str(source_db),
            "forward_root": str(forward_root),
            "prior_invalidation_root": str(prior_invalidation_root),
            "candidate_count": int(len(candidates)),
            "bar_row_count_for_candidate_codes": int(len(bars)),
            "asof_feature_row_count": int(len(features)),
            "ma20_complete": bool(repaired["ma20"].notna().all()) if not repaired.empty else False,
            "atr14_complete": bool(repaired["atr14"].notna().all()) if not repaired.empty else False,
            "recent_swing_low_complete": bool(repaired["recent_swing_low"].notna().all()) if not repaired.empty else False,
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
    parser.add_argument("--prior-invalidation-root", type=Path, default=DEFAULT_PRIOR_INVALIDATION_ROOT)
    parser.add_argument("--source-db", type=Path, default=DEFAULT_SOURCE_DB)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    args = parser.parse_args(argv)
    out = run(args.forward_root, args.prior_invalidation_root, args.source_db, args.output_root)
    print(json.dumps({"output_dir": str(out)}, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
