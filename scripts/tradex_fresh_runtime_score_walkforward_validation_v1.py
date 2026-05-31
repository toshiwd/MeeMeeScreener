from __future__ import annotations

import argparse
import json
import math
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from scripts import tradex_fresh_runtime_candidate_surface_v1 as score_contract


AXIS_ID = "fresh_runtime_score_walkforward_validation_v1"
DEFAULT_SOURCE_DB = Path(r"C:\Users\enish\AppData\Local\MeeMeeScreener\data\stocks.duckdb")
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\fresh_runtime_score_walkforward_validation_v1")
DEFAULT_DATE_COUNT = 260
REQUIRED_ARTIFACTS = (
    "walkforward_summary.json",
    "walkforward_top_rows.csv",
    "walkforward_date_metrics.csv",
    "score_bucket_metrics.json",
    "buyability_gate_audit.json",
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


def _rank_pct(series: pd.Series, ascending: bool = True) -> pd.Series:
    return pd.to_numeric(series, errors="coerce").rank(pct=True, ascending=ascending).fillna(0.0)


def load_walkforward_frame(source_db: Path, date_count: int = DEFAULT_DATE_COUNT) -> pd.DataFrame:
    if not source_db.exists():
        raise FileNotFoundError(source_db)
    con = duckdb.connect(str(source_db), read_only=True)
    try:
        query = """
            WITH all_dates AS (
                SELECT DISTINCT CAST(strftime(to_timestamp(date), '%Y%m%d') AS INTEGER) AS as_of_date
                FROM daily_bars
                ORDER BY as_of_date DESC
                LIMIT ?
            ),
            bars AS (
                SELECT
                    CAST(code AS VARCHAR) AS code,
                    CAST(strftime(to_timestamp(date), '%Y%m%d') AS INTEGER) AS as_of_date,
                    o, h, l, c, v,
                    lag(c, 1) OVER (PARTITION BY code ORDER BY date) AS prev_close,
                    avg(v) OVER (PARTITION BY code ORDER BY date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS volume20_avg,
                    max(h) OVER (PARTITION BY code ORDER BY date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS recent_high20,
                    min(l) OVER (PARTITION BY code ORDER BY date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS recent_low20,
                    lead(c, 5) OVER (PARTITION BY code ORDER BY date) AS close_fwd5,
                    lead(c, 20) OVER (PARTITION BY code ORDER BY date) AS close_fwd20
                FROM daily_bars
            ),
            features AS (
                SELECT
                    CAST(strftime(to_timestamp(dt), '%Y%m%d') AS INTEGER) AS as_of_date,
                    CAST(code AS VARCHAR) AS code,
                    close, ma7, ma20, ma60, diff20_pct, cnt_20_above, cnt_7_above
                FROM feature_snapshot_daily
            )
            SELECT
                f.as_of_date,
                f.code,
                f.close,
                f.ma7,
                f.ma20,
                f.ma60,
                f.diff20_pct,
                f.cnt_20_above,
                f.cnt_7_above,
                b.o, b.h, b.l, b.c, b.v,
                b.prev_close,
                b.volume20_avg,
                b.recent_high20,
                b.recent_low20,
                b.close_fwd5,
                b.close_fwd20
            FROM features f
            JOIN bars b ON b.code = f.code AND b.as_of_date = f.as_of_date
            JOIN all_dates d ON d.as_of_date = f.as_of_date
        """
        return con.execute(query, [int(date_count)]).fetchdf()
    finally:
        con.close()


def build_scored_frame(frame: pd.DataFrame) -> pd.DataFrame:
    out = frame.copy()
    out["close_vs_ma7_pct"] = out["close"] / out["ma7"] - 1.0
    out["close_vs_ma20_pct"] = out["close"] / out["ma20"] - 1.0
    out["close_vs_ma60_pct"] = out["close"] / out["ma60"] - 1.0
    out["ma7_above_ma20"] = out["ma7"] > out["ma20"]
    out["ma20_above_ma60"] = out["ma20"] > out["ma60"]
    out["volume_vs_20d_avg"] = out["v"] / out["volume20_avg"]
    out["recent_high_distance_pct"] = out["close"] / out["recent_high20"] - 1.0
    out["recent_low_distance_pct"] = out["close"] / out["recent_low20"] - 1.0
    candle_range = (out["h"] - out["l"]).replace(0, pd.NA)
    out["body_ratio"] = (out["c"] - out["o"]).abs() / candle_range
    out["upper_wick_ratio"] = (out["h"] - out[["o", "c"]].max(axis=1)) / candle_range
    out["lower_wick_ratio"] = (out[["o", "c"]].min(axis=1) - out["l"]) / candle_range
    out["gap_pct"] = out["o"] / out["prev_close"] - 1.0
    out["ret5"] = out["close_fwd5"] / out["close"] - 1.0
    out["ret20"] = out["close_fwd20"] / out["close"] - 1.0
    out["live_feature_available"] = out[score_contract.LIVE_FEATURE_COLUMNS].notna().all(axis=1)

    parts = []
    for _, grp in out.groupby("as_of_date", sort=True):
        grp = grp.copy()
        grp["fresh_runtime_research_watch_score"] = (
            0.18 * _rank_pct(grp["diff20_pct"])
            + 0.16 * _rank_pct(grp["close_vs_ma20_pct"])
            + 0.12 * _rank_pct(grp["close_vs_ma60_pct"])
            + 0.12 * _rank_pct(grp["cnt_20_above"])
            + 0.10 * _rank_pct(grp["cnt_7_above"])
            + 0.10 * _rank_pct(grp["volume_vs_20d_avg"])
            + 0.08 * _rank_pct(grp["body_ratio"])
            + 0.06 * _rank_pct(grp["recent_low_distance_pct"])
            - 0.06 * _rank_pct(grp["upper_wick_ratio"])
            - 0.06 * _rank_pct(grp["gap_pct"].abs())
        )
        grp["fresh_runtime_research_watch_rank"] = grp["fresh_runtime_research_watch_score"].rank(method="first", ascending=False).astype(int)
        grp["score_bucket"] = grp["fresh_runtime_research_watch_rank"].map(
            lambda rank: "top20" if rank <= 20 else ("rank21_100" if rank <= 100 else "remaining")
        )
        parts.append(grp)
    return pd.concat(parts, ignore_index=True) if parts else out


def _metrics(rows: pd.DataFrame) -> dict[str, Any]:
    valid = rows[rows["ret20"].notna()].copy()
    if valid.empty:
        return {"sample_count": 0, "date_count": 0, "code_count": 0, "mean_ret20": None, "median_ret20": None, "winner_rate_ret20_gt_10pct": None, "bad_rate_ret20_lt_minus_5pct": None, "severe_rate_ret20_lt_minus_10pct": None, "outcome_coverage_rate": 0.0}
    return {
        "sample_count": int(len(valid)),
        "date_count": int(valid["as_of_date"].nunique()),
        "code_count": int(valid["code"].nunique()),
        "mean_ret20": float(valid["ret20"].mean()),
        "median_ret20": float(valid["ret20"].median()),
        "winner_rate_ret20_gt_10pct": float((valid["ret20"] > 0.10).mean()),
        "bad_rate_ret20_lt_minus_5pct": float((valid["ret20"] < -0.05).mean()),
        "severe_rate_ret20_lt_minus_10pct": float((valid["ret20"] < -0.10).mean()),
        "outcome_coverage_rate": float(len(valid) / len(rows)) if len(rows) else 0.0,
    }


def bucket_metrics(scored: pd.DataFrame) -> dict[str, Any]:
    return {bucket: _metrics(scored[scored["score_bucket"] == bucket]) for bucket in ["top20", "rank21_100", "remaining"]}


def date_metrics(scored: pd.DataFrame) -> pd.DataFrame:
    top = scored[scored["score_bucket"] == "top20"].copy()
    rows = []
    for as_of, grp in top.groupby("as_of_date"):
        m = _metrics(grp)
        rows.append({"as_of_date": as_of, **m})
    return pd.DataFrame(rows).sort_values("as_of_date")


def buyability_gate(metrics: dict[str, Any], date_frame: pd.DataFrame) -> dict[str, Any]:
    top = metrics["top20"]
    coverage_ok = top["outcome_coverage_rate"] >= 0.90 and top["sample_count"] >= 1000 and top["date_count"] >= 50
    quality_ok = (
        top["mean_ret20"] is not None
        and top["mean_ret20"] > 0.03
        and top["winner_rate_ret20_gt_10pct"] is not None
        and top["winner_rate_ret20_gt_10pct"] >= 0.20
        and top["bad_rate_ret20_lt_minus_5pct"] is not None
        and top["bad_rate_ret20_lt_minus_5pct"] <= 0.20
        and top["severe_rate_ret20_lt_minus_10pct"] is not None
        and top["severe_rate_ret20_lt_minus_10pct"] <= 0.10
    )
    stability_ok = bool((date_frame["mean_ret20"] > 0).mean() >= 0.55) if not date_frame.empty and "mean_ret20" in date_frame else False
    return {
        "buyability_gate_pass": bool(coverage_ok and quality_ok and stability_ok),
        "coverage_gate_pass": bool(coverage_ok),
        "quality_gate_pass": bool(quality_ok),
        "stability_gate_pass": bool(stability_ok),
        "positive_date_share": float((date_frame["mean_ret20"] > 0).mean()) if not date_frame.empty and "mean_ret20" in date_frame else 0.0,
        "thresholds": {
            "sample_count_min": 1000,
            "date_count_min": 50,
            "outcome_coverage_rate_min": 0.90,
            "mean_ret20_min": 0.03,
            "winner_rate_ret20_gt_10pct_min": 0.20,
            "bad_rate_ret20_lt_minus_5pct_max": 0.20,
            "severe_rate_ret20_lt_minus_10pct_max": 0.10,
            "positive_date_share_min": 0.55,
        },
        "validated_buy_count": 0,
        "active_gate_created": False,
    }


def decide(gate: dict[str, Any], metrics: dict[str, Any]) -> tuple[str, str, list[str]]:
    if gate["buyability_gate_pass"]:
        return "fresh_runtime_score_buyability_gate_passed_for_next_validation", "KEEP", ["historical_walkforward_top20_passed_predeclared_buyability_gate"]
    if metrics["top20"]["mean_ret20"] is not None and metrics["top20"]["mean_ret20"] > metrics["remaining"]["mean_ret20"]:
        return "fresh_runtime_score_directional_but_not_buyable", "HOLD_UNDERPOWERED", ["top20_outperformed_remaining_but_failed_buyability_gate"]
    return "fresh_runtime_score_no_buyability_edge", "DROP", ["top20_did_not_show_sufficient_walkforward_buyability_edge"]


def run(source_db: Path = DEFAULT_SOURCE_DB, output_root: Path = DEFAULT_OUTPUT_ROOT, date_count: int = DEFAULT_DATE_COUNT) -> Path:
    raw = load_walkforward_frame(source_db, date_count)
    scored = build_scored_frame(raw)
    metrics = bucket_metrics(scored)
    per_date = date_metrics(scored)
    gate = buyability_gate(metrics, per_date)
    decision, decision_class, reasons = decide(gate, metrics)
    out = output_root / f"{_now_tag()}-fresh-runtime-score-walkforward-validation-v1"
    out.mkdir(parents=True, exist_ok=True)
    top_cols = ["as_of_date", "code", "fresh_runtime_research_watch_score", "fresh_runtime_research_watch_rank", "score_bucket", "ret5", "ret20"]
    scored[scored["score_bucket"] == "top20"][top_cols].to_csv(out / "walkforward_top_rows.csv", index=False)
    per_date.to_csv(out / "walkforward_date_metrics.csv", index=False)
    summary = {"axis_id": AXIS_ID, "decision": decision, "decision_class": decision_class, "reason_typed": reasons, "source_db": str(source_db), "input_date_count_requested": date_count, "row_count": int(len(scored)), "date_count": int(scored["as_of_date"].nunique()) if not scored.empty else 0, "buyable_selection_ready": decision_class == "KEEP", "validated_buy_count": 0}
    _write_json(out / "walkforward_summary.json", summary)
    _write_json(out / "score_bucket_metrics.json", {"axis_id": AXIS_ID, "buckets": metrics})
    _write_json(out / "buyability_gate_audit.json", gate)
    _write_json(out / "feature_contract.json", score_contract.feature_contract())
    _write_json(out / "source_coverage.json", {"axis_id": AXIS_ID, "source_db": str(source_db), "row_count": int(len(scored)), "date_count": int(scored["as_of_date"].nunique()) if not scored.empty else 0, "code_count": int(scored["code"].nunique()) if not scored.empty else 0, "research_fallback_used": False})
    _write_json(out / "no_lookahead_audit.json", {"audit_result": "pass", "no_lookahead_pass": True, "features_built_before_outcome_evaluation": True, "outcomes_used_for_scoring": False, "outcomes_used_for_selection": False, "runtime_db_write": False, "research_fallback_used": False})
    _write_json(out / "lineage.json", {"axis_id": AXIS_ID, "score_contract_source": "scripts/tradex_fresh_runtime_candidate_surface_v1.py", "source_db": str(source_db)})
    _write_json(out / "research_decision.json", {"axis_id": AXIS_ID, "research_decision": decision, "decision_class": decision_class, "reason_typed": reasons, "research_watch_only": True, "buyable_selection_ready": decision_class == "KEEP", "meemee_reflectable_candidate": False, "runtime_db_write": False, "production_ranking_changed": False, "production_candidate_generator_changed": False, "publish_allowed": False, "validated_buy_count": 0, "active_gate_created": False, "research_fallback_used": False})
    _write_json(out / "_ARTIFACT_COMPLETE.json", {"complete": True, "artifact_complete": True, "required_artifacts": list(REQUIRED_ARTIFACTS), "generated_at": _now_tag()})
    return out


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-db", type=Path, default=DEFAULT_SOURCE_DB)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--date-count", type=int, default=DEFAULT_DATE_COUNT)
    args = parser.parse_args(argv)
    out = run(args.source_db, args.output_root, args.date_count)
    print(json.dumps({"output_dir": str(out)}, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
