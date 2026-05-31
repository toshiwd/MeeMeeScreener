from __future__ import annotations

import argparse
import json
import math
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd


AXIS_ID = "forward_current_candidate_surface_v1"
DEFAULT_SOURCE_DB = Path(r"G:\Tradex\db_snapshots\stocks_20260426_022925.duckdb")
DEFAULT_READINESS_ROOT = Path(r"G:\Tradex\current_buyability_readiness_audit_v1\20260525T141200Z-current-buyability-readiness-audit-v1")
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\forward_current_candidate_surface_v1")
LIVE_FEATURE_COLUMNS = [
    "close",
    "ma7",
    "ma20",
    "ma60",
    "atr14_pct",
    "gap_pct",
    "vol_ratio5_20",
    "turnover20",
    "turnover_z20",
    "high20_dist",
    "low20_dist",
    "candle_body_ratio",
    "candle_upper_wick_ratio",
    "candle_lower_wick_ratio",
    "diff20_pct",
    "cnt_20_above",
    "cnt_7_above",
    "weekly_breakout_up_prob",
    "monthly_breakout_up_prob",
    "monthly_range_prob",
]
REQUIRED_ARTIFACTS = (
    "forward_current_candidate_surface_summary.json",
    "forward_current_candidate_surface_rows.parquet",
    "forward_current_candidate_surface_rows_sample.csv",
    "surface_contract.json",
    "feature_contract.json",
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


def load_latest_feature_frame(source_db: Path) -> tuple[pd.DataFrame, int | None]:
    if not source_db.exists():
        raise FileNotFoundError(source_db)
    con = duckdb.connect(str(source_db), read_only=True)
    try:
        latest = con.execute("SELECT max(CAST(strftime(to_timestamp(dt), '%Y%m%d') AS INTEGER)) FROM feature_frame_daily").fetchone()[0]
        if latest is None:
            return pd.DataFrame(), None
        query = """
            SELECT
                CAST(strftime(to_timestamp(dt), '%Y%m%d') AS INTEGER) AS as_of_date,
                CAST(code AS VARCHAR) AS code,
                available_at,
                source_presence_flag,
                close,
                ma7,
                ma20,
                ma60,
                atr14_pct,
                gap_pct,
                vol_ratio5_20,
                turnover20,
                turnover_z20,
                high20_dist,
                low20_dist,
                candle_body_ratio,
                candle_upper_wick_ratio,
                candle_lower_wick_ratio,
                diff20_pct,
                cnt_20_above,
                cnt_7_above,
                weekly_breakout_up_prob,
                monthly_breakout_up_prob,
                monthly_range_prob
            FROM feature_frame_daily
            WHERE CAST(strftime(to_timestamp(dt), '%Y%m%d') AS INTEGER) = ?
        """
        return con.execute(query, [int(latest)]).fetchdf(), int(latest)
    finally:
        con.close()


def build_surface(frame: pd.DataFrame) -> pd.DataFrame:
    out = frame.copy()
    out["forward_surface_live_feature_available_flag"] = out[LIVE_FEATURE_COLUMNS].notna().all(axis=1)
    out["forward_surface_missing_reason"] = out[LIVE_FEATURE_COLUMNS].isna().apply(lambda row: ",".join(row.index[row].tolist()[:8]), axis=1)
    # Diagnostic ordering only. This is not a validated positive-selection score.
    out["forward_research_watch_score"] = (
        0.20 * pd.to_numeric(out["diff20_pct"], errors="coerce").rank(pct=True)
        + 0.15 * pd.to_numeric(out["weekly_breakout_up_prob"], errors="coerce").rank(pct=True)
        + 0.15 * pd.to_numeric(out["monthly_breakout_up_prob"], errors="coerce").rank(pct=True)
        + 0.15 * pd.to_numeric(out["monthly_range_prob"], errors="coerce").rank(pct=True)
        + 0.10 * pd.to_numeric(out["turnover_z20"], errors="coerce").rank(pct=True)
        + 0.10 * pd.to_numeric(out["candle_body_ratio"], errors="coerce").rank(pct=True)
        - 0.10 * pd.to_numeric(out["candle_upper_wick_ratio"], errors="coerce").rank(pct=True)
        - 0.05 * pd.to_numeric(out["atr14_pct"], errors="coerce").rank(pct=True)
    )
    out["forward_research_watch_rank"] = out["forward_research_watch_score"].rank(method="first", ascending=False).astype(int)
    out["forward_research_watch_bucket"] = out["forward_research_watch_rank"].map(
        lambda rank: "watch_top10_unvalidated"
        if rank <= 10
        else ("watch_11_50_unvalidated" if rank <= 50 else ("watch_51_100_unvalidated" if rank <= 100 else "watch_remaining_unvalidated"))
    )
    return out


def no_lookahead(frame: pd.DataFrame) -> dict[str, Any]:
    available_dates = pd.to_datetime(frame["available_at"], errors="coerce").dt.strftime("%Y%m%d")
    available_ymd = pd.to_numeric(available_dates, errors="coerce")
    safe = bool((available_ymd <= frame["as_of_date"]).fillna(False).all()) if not frame.empty else False
    return {
        "audit_result": "pass" if safe else "blocked",
        "no_lookahead_pass": safe,
        "available_at_checked": True,
        "outcome_columns_present": False,
        "offline_outcomes_used": False,
        "runtime_db_write": False,
        "research_fallback_used": False,
    }


def feature_contract() -> dict[str, Any]:
    fields = {"as_of_date": {"classification": "identifier"}, "code": {"classification": "identifier"}}
    for col in LIVE_FEATURE_COLUMNS:
        fields[col] = {"classification": "point_in_time_feature"}
    for col in ["forward_research_watch_score", "forward_research_watch_rank", "forward_research_watch_bucket", "forward_surface_live_feature_available_flag", "forward_surface_missing_reason"]:
        fields[col] = {"classification": "point_in_time_feature"}
    for col in ["ret5", "ret10", "ret20", "winner_ret20_gt_10pct", "bad_ret20_lt_minus_5pct", "severe_ret20_lt_minus_10pct"]:
        fields[col] = {"classification": "forbidden_future_leak"}
    return {"axis_id": AXIS_ID, "fields": fields}


def decide(frame: pd.DataFrame, lookahead: dict[str, Any]) -> tuple[str, str, list[str]]:
    if not lookahead["no_lookahead_pass"]:
        return "blocked_no_lookahead_violation", "BLOCKED", ["available_at_after_as_of_date"]
    if frame.empty:
        return "blocked_missing_current_feature_frame", "BLOCKED", ["current_feature_frame_empty"]
    if float(frame["forward_surface_live_feature_available_flag"].mean()) < 0.95:
        return "forward_current_surface_created_but_feature_gaps", "HOLD_UNDERPOWERED", ["current_surface_has_missing_live_features"]
    return "forward_current_surface_ready_for_research_watch_pretest", "HOLD_UNDERPOWERED", ["current_no_outcome_surface_created_but_no_validated_buy_selector_yet"]


def run(source_db: Path = DEFAULT_SOURCE_DB, readiness_root: Path = DEFAULT_READINESS_ROOT, output_root: Path = DEFAULT_OUTPUT_ROOT) -> Path:
    frame, latest = load_latest_feature_frame(source_db)
    surface = build_surface(frame) if not frame.empty else frame
    lookahead = no_lookahead(surface) if not surface.empty else {"audit_result": "blocked", "no_lookahead_pass": False, "runtime_db_write": False, "research_fallback_used": False}
    decision, decision_class, reasons = decide(surface, lookahead)
    out = output_root / f"{_now_tag()}-forward-current-candidate-surface-v1"
    out.mkdir(parents=True, exist_ok=True)
    cols = ["as_of_date", "code", "forward_research_watch_score", "forward_research_watch_rank", "forward_research_watch_bucket", "forward_surface_live_feature_available_flag", "forward_surface_missing_reason", *LIVE_FEATURE_COLUMNS]
    surface[cols].to_parquet(out / "forward_current_candidate_surface_rows.parquet", index=False)
    surface[cols].sort_values("forward_research_watch_rank").head(250).to_csv(out / "forward_current_candidate_surface_rows_sample.csv", index=False)
    summary = {
        "axis_id": AXIS_ID,
        "decision": decision,
        "decision_class": decision_class,
        "reason_typed": reasons,
        "latest_feature_frame_date": latest,
        "row_count": int(len(surface)),
        "code_count": int(surface["code"].nunique()) if not surface.empty else 0,
        "live_feature_complete_rate": float(surface["forward_surface_live_feature_available_flag"].mean()) if not surface.empty else 0.0,
        "validated_buy_count": 0,
    }
    _write_json(out / "forward_current_candidate_surface_summary.json", summary)
    _write_json(out / "surface_contract.json", {"axis_id": AXIS_ID, "surface_id": "forward_current_candidate_surface_v1", "diagnostic_only": True, "contains_offline_outcomes": False, "buy_signal": False, "validated_buy_claim": False, "live_feature_columns": LIVE_FEATURE_COLUMNS})
    _write_json(out / "feature_contract.json", feature_contract())
    _write_json(out / "source_coverage.json", {"axis_id": AXIS_ID, "source_db": str(source_db), "latest_feature_frame_date": latest, "row_count": int(len(surface)), "code_count": int(surface["code"].nunique()) if not surface.empty else 0, "research_fallback_used": False})
    _write_json(out / "no_lookahead_audit.json", lookahead)
    _write_json(out / "lineage.json", {"readiness_root": str(readiness_root), "readiness_decision": _load_json(readiness_root / "research_decision.json"), "source_db": str(source_db)})
    _write_json(out / "research_decision.json", {"axis_id": AXIS_ID, "research_decision": decision, "decision_class": decision_class, "reason_typed": reasons, "buyable_selection_ready": False, "research_watch_only": True, "meemee_reflectable_candidate": False, "runtime_db_write": False, "production_ranking_changed": False, "production_candidate_generator_changed": False, "publish_allowed": False, "validated_buy_count": 0, "active_gate_created": False, "research_fallback_used": False})
    _write_json(out / "_ARTIFACT_COMPLETE.json", {"complete": True, "artifact_complete": True, "required_artifacts": list(REQUIRED_ARTIFACTS), "generated_at": _now_tag()})
    return out


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-db", type=Path, default=DEFAULT_SOURCE_DB)
    parser.add_argument("--readiness-root", type=Path, default=DEFAULT_READINESS_ROOT)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    args = parser.parse_args(argv)
    out = run(args.source_db, args.readiness_root, args.output_root)
    print(json.dumps({"output_dir": str(out)}, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
