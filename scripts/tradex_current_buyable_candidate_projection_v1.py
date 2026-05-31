from __future__ import annotations

import argparse
import json
import math
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb
import numpy as np
import pandas as pd


AXIS_ID = "current_buyable_candidate_projection_v1"
DEFAULT_FORWARD_ROOT = Path(
    r"G:\Tradex\intersection_family_forward_paper_validation_v1\20260526T004922Z-intersection-family-forward-paper-validation-v1"
)
DEFAULT_RISK_ROOT = Path(
    r"G:\Tradex\intersection_family_current_period_risk_containment_v1\20260526T010028Z-intersection-family-current-period-risk-containment-v1"
)
DEFAULT_SOURCE_DB = Path(r"C:\Users\enish\AppData\Local\MeeMeeScreener\data\stocks.duckdb")
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\current_buyable_candidate_projection_v1")
REQUIRED_ARTIFACTS = (
    "current_buyable_projection_summary.json",
    "current_buyable_candidate_rows.csv",
    "candidate_feature_rows.csv",
    "projection_contract.json",
    "source_coverage.json",
    "no_lookahead_audit.json",
    "lineage.json",
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


def load_forward_candidates(forward_root: Path) -> tuple[pd.DataFrame, dict[str, Any]]:
    rows_path = forward_root / "forward_paper_candidate_rows.csv"
    decision_path = forward_root / "research_decision.json"
    if not rows_path.exists():
        raise FileNotFoundError(rows_path)
    if not decision_path.exists():
        raise FileNotFoundError(decision_path)
    rows = pd.read_csv(rows_path, dtype={"code": str})
    rows["as_of_date"] = pd.to_numeric(rows["as_of_date"], errors="coerce").astype("Int64")
    return rows, _load_json(decision_path)


def load_bars(source_db: Path, codes: list[str], as_of_date: int) -> pd.DataFrame:
    if not source_db.exists():
        raise FileNotFoundError(source_db)
    con = duckdb.connect(str(source_db), read_only=True)
    try:
        expr = "CAST(strftime(to_timestamp(date), '%Y%m%d') AS INTEGER)"
        query = f"""
            SELECT CAST(code AS VARCHAR) AS code, {expr} AS as_of_date, o AS open, h AS high, l AS low, c AS close, v AS volume
            FROM daily_bars
            WHERE CAST(code AS VARCHAR) IN (SELECT * FROM UNNEST(?))
              AND {expr} <= ?
            ORDER BY code, as_of_date
        """
        return con.execute(query, [codes, int(as_of_date)]).fetchdf()
    finally:
        con.close()


def build_candle_features(bars: pd.DataFrame, as_of_date: int) -> pd.DataFrame:
    out = bars.sort_values(["code", "as_of_date"]).copy()
    g = out.groupby("code", sort=False)
    rng = (out["high"] - out["low"]).replace(0, np.nan)
    out["upper_wick_ratio"] = (out["high"] - out[["open", "close"]].max(axis=1)) / rng
    out["bearish_body_flag"] = out["close"] < out["open"]
    prior_high20 = g["high"].transform(lambda s: s.shift(1).rolling(20, min_periods=20).max())
    out["failed_high_flag"] = (out["high"] >= prior_high20) & (out["close"] < prior_high20)
    current = out[out["as_of_date"] == int(as_of_date)].copy()
    current["variant_a_candle_risk_clean"] = (
        ~current["failed_high_flag"].fillna(True).astype(bool)
        & ~current["bearish_body_flag"].fillna(True).astype(bool)
        & pd.to_numeric(current["upper_wick_ratio"], errors="coerce").le(0.45)
    )
    current["feature_available_flag"] = current[["failed_high_flag", "bearish_body_flag", "upper_wick_ratio"]].notna().all(axis=1)
    return current[["as_of_date", "code", "open", "high", "low", "close", "failed_high_flag", "bearish_body_flag", "upper_wick_ratio", "variant_a_candle_risk_clean", "feature_available_flag"]]


def project_candidates(forward_rows: pd.DataFrame, features: pd.DataFrame) -> pd.DataFrame:
    rows = forward_rows.merge(features, on=["as_of_date", "code"], how="left", validate="one_to_one")
    rows["feature_missing_reason"] = rows["feature_available_flag"].map(lambda ok: "" if bool(ok) else "confirmed_bar_feature_missing")
    rows["research_buyable_candidate"] = rows["variant_a_candle_risk_clean"].fillna(False).astype(bool)
    rows["buy_recommendation"] = False
    rows["validated_buy"] = False
    rows["active_gate_created"] = False
    selected = rows[rows["research_buyable_candidate"]].copy()
    selected = selected.sort_values(["forward_paper_rank", "code"])
    selected["research_buyable_rank"] = range(1, len(selected) + 1)
    rows = rows.merge(selected[["as_of_date", "code", "research_buyable_rank"]], on=["as_of_date", "code"], how="left")
    return rows


def no_lookahead_audit(rows: pd.DataFrame, forward_decision: dict[str, Any], risk_decision: dict[str, Any]) -> dict[str, Any]:
    source_ok = forward_decision.get("research_decision") == "intersection_family_forward_paper_candidates_frozen"
    risk_ok = risk_decision.get("research_decision") == "intersection_current_period_risk_containment_buyable_ready"
    forbidden = {"ret5", "ret10", "ret20", "winner_ret20_gt_10pct", "bad_ret20_lt_minus_5pct", "severe_ret20_lt_minus_10pct"}
    present = sorted(forbidden & set(rows.columns))
    passed = source_ok and risk_ok and not present and bool(rows["feature_available_flag"].all())
    return {
        "audit_result": "pass" if passed else "blocked",
        "no_lookahead_pass": passed,
        "forward_candidates_frozen": source_ok,
        "risk_containment_keep": risk_ok,
        "forbidden_outcome_columns_present": present,
        "features_built_from_confirmed_bars_on_or_before_as_of_date": True,
        "runtime_db_write": False,
        "research_fallback_used": False,
    }


def decide(rows: pd.DataFrame, audit: dict[str, Any]) -> tuple[str, str, list[str]]:
    selected_count = int(rows["research_buyable_candidate"].sum())
    if not audit["no_lookahead_pass"]:
        return "blocked_missing_current_feature_contract", "BLOCKED", ["current_candidate_projection_failed_no_lookahead_or_feature_contract"]
    if selected_count > 0:
        return "current_research_buyable_candidates_selected", "KEEP", ["current_candidates_passed_frozen_intersection_and_candle_risk_projection"]
    return "current_projection_no_candidate_after_risk_containment", "HOLD_UNDERPOWERED", ["risk_containment_removed_all_current_candidates"]


def run(
    forward_root: Path = DEFAULT_FORWARD_ROOT,
    risk_root: Path = DEFAULT_RISK_ROOT,
    source_db: Path = DEFAULT_SOURCE_DB,
    output_root: Path = DEFAULT_OUTPUT_ROOT,
) -> Path:
    forward_rows, forward_decision = load_forward_candidates(forward_root)
    risk_decision = _load_json(risk_root / "research_decision.json")
    as_of_date = int(pd.to_numeric(forward_rows["as_of_date"], errors="coerce").max())
    codes = sorted(forward_rows["code"].astype(str).unique().tolist())
    bars = load_bars(source_db, codes, as_of_date)
    features = build_candle_features(bars, as_of_date)
    projected = project_candidates(forward_rows, features)
    audit = no_lookahead_audit(projected, forward_decision, risk_decision)
    decision, decision_class, reasons = decide(projected, audit)
    selected = projected[projected["research_buyable_candidate"]].copy()

    out = output_root / f"{_now_tag()}-current-buyable-candidate-projection-v1"
    out.mkdir(parents=True, exist_ok=True)
    selected.to_csv(out / "current_buyable_candidate_rows.csv", index=False)
    projected.to_csv(out / "candidate_feature_rows.csv", index=False)
    _write_json(
        out / "current_buyable_projection_summary.json",
        {
            "axis_id": AXIS_ID,
            "decision": decision,
            "decision_class": decision_class,
            "reason_typed": reasons,
            "as_of_date": as_of_date,
            "input_candidate_count": int(len(projected)),
            "research_buyable_candidate_count": int(len(selected)),
            "research_buyable_candidate_codes": selected["code"].astype(str).tolist(),
            "buy_recommendation": False,
            "validated_buy_count": 0,
            "active_gate_created": False,
            "buyable_selection_ready": decision_class == "KEEP",
        },
    )
    _write_json(
        out / "projection_contract.json",
        {
            "axis_id": AXIS_ID,
            "selector_contract": "frozen_intersection_family_variant_b_entry_qualified_top50",
            "risk_contract": "variant_a_candle_risk_clean",
            "risk_conditions": {
                "failed_high_flag": False,
                "bearish_body_flag": False,
                "upper_wick_ratio_max": 0.45,
            },
            "outcomes_used_for_projection": False,
            "research_only": True,
            "production_mutation": False,
        },
    )
    _write_json(
        out / "source_coverage.json",
        {
            "axis_id": AXIS_ID,
            "source_db": str(source_db),
            "forward_root": str(forward_root),
            "risk_root": str(risk_root),
            "input_candidate_count": int(len(projected)),
            "feature_available_count": int(projected["feature_available_flag"].sum()),
            "feature_available_rate": float(projected["feature_available_flag"].mean()) if len(projected) else 0.0,
            "bar_row_count_for_candidate_codes": int(len(bars)),
            "latest_bar_date_used": as_of_date,
            "research_fallback_used": False,
        },
    )
    _write_json(out / "no_lookahead_audit.json", audit)
    _write_json(out / "lineage.json", {"axis_id": AXIS_ID, "forward_root": str(forward_root), "forward_decision": forward_decision, "risk_root": str(risk_root), "risk_decision": risk_decision})
    _write_json(
        out / "research_decision.json",
        {
            "axis_id": AXIS_ID,
            "research_decision": decision,
            "decision_class": decision_class,
            "reason_typed": reasons,
            "research_watch_only": True,
            "buyable_selection_ready": decision_class == "KEEP",
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
    parser.add_argument("--risk-root", type=Path, default=DEFAULT_RISK_ROOT)
    parser.add_argument("--source-db", type=Path, default=DEFAULT_SOURCE_DB)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    args = parser.parse_args(argv)
    out = run(args.forward_root, args.risk_root, args.source_db, args.output_root)
    print(json.dumps({"output_dir": str(out)}, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
