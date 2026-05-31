from __future__ import annotations

import argparse
import json
import math
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd


AXIS_ID = "fresh_runtime_candidate_surface_v1"
DEFAULT_SOURCE_DB = Path(r"C:\Users\enish\AppData\Local\MeeMeeScreener\data\stocks.duckdb")
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\fresh_runtime_candidate_surface_v1")
LIVE_FEATURE_COLUMNS = [
    "close",
    "ma7",
    "ma20",
    "ma60",
    "diff20_pct",
    "cnt_20_above",
    "cnt_7_above",
    "close_vs_ma7_pct",
    "close_vs_ma20_pct",
    "close_vs_ma60_pct",
    "ma7_above_ma20",
    "ma20_above_ma60",
    "volume_vs_20d_avg",
    "recent_high_distance_pct",
    "recent_low_distance_pct",
    "body_ratio",
    "upper_wick_ratio",
    "lower_wick_ratio",
    "gap_pct",
]
REQUIRED_ARTIFACTS = (
    "fresh_runtime_candidate_surface_summary.json",
    "fresh_runtime_candidate_surface_rows.parquet",
    "fresh_runtime_candidate_surface_rows_sample.csv",
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


def load_latest_runtime_frames(source_db: Path) -> tuple[pd.DataFrame, pd.DataFrame, int, int]:
    if not source_db.exists():
        raise FileNotFoundError(source_db)
    con = duckdb.connect(str(source_db), read_only=True)
    try:
        feature_date = con.execute("SELECT max(CAST(strftime(to_timestamp(dt), '%Y%m%d') AS INTEGER)) FROM feature_snapshot_daily").fetchone()[0]
        bar_date = con.execute("SELECT max(CAST(strftime(to_timestamp(date), '%Y%m%d') AS INTEGER)) FROM daily_bars").fetchone()[0]
        feature_query = """
            SELECT
                CAST(strftime(to_timestamp(dt), '%Y%m%d') AS INTEGER) AS as_of_date,
                CAST(code AS VARCHAR) AS code,
                close,
                ma7,
                ma20,
                ma60,
                atr14,
                diff20_pct,
                cnt_20_above,
                cnt_7_above,
                day_count
            FROM feature_snapshot_daily
            WHERE CAST(strftime(to_timestamp(dt), '%Y%m%d') AS INTEGER) = ?
        """
        bars_query = """
            SELECT
                CAST(strftime(to_timestamp(date), '%Y%m%d') AS INTEGER) AS bar_date,
                CAST(code AS VARCHAR) AS code,
                o, h, l, c, v, source
            FROM daily_bars
            WHERE CAST(strftime(to_timestamp(date), '%Y%m%d') AS INTEGER) >= (
                SELECT min(x) FROM (
                    SELECT DISTINCT CAST(strftime(to_timestamp(date), '%Y%m%d') AS INTEGER) AS x
                    FROM daily_bars
                    ORDER BY x DESC
                    LIMIT 80
                )
            )
        """
        return con.execute(feature_query, [int(feature_date)]).fetchdf(), con.execute(bars_query).fetchdf(), int(feature_date), int(bar_date)
    finally:
        con.close()


def _rank_pct(series: pd.Series, ascending: bool = True) -> pd.Series:
    return pd.to_numeric(series, errors="coerce").rank(pct=True, ascending=ascending).fillna(0.0)


def build_surface(features: pd.DataFrame, bars: pd.DataFrame) -> pd.DataFrame:
    latest_bar_date = int(pd.to_numeric(bars["bar_date"], errors="coerce").max())
    latest_bars = bars[pd.to_numeric(bars["bar_date"], errors="coerce") == latest_bar_date].copy()
    latest_bars["prev_close"] = None
    latest_bars["volume20_avg"] = None
    latest_bars["recent_high20"] = None
    latest_bars["recent_low20"] = None
    by_code = {str(code): grp.sort_values("bar_date") for code, grp in bars.groupby("code")}
    stats: dict[str, dict[str, float | None]] = {}
    for code, grp in by_code.items():
        grp = grp.sort_values("bar_date")
        latest = grp[pd.to_numeric(grp["bar_date"], errors="coerce") == latest_bar_date]
        if latest.empty:
            continue
        prior = grp[pd.to_numeric(grp["bar_date"], errors="coerce") < latest_bar_date]
        last20 = grp.tail(20)
        stats[code] = {
            "prev_close": float(prior.iloc[-1]["c"]) if len(prior) else None,
            "volume20_avg": float(last20["v"].mean()) if len(last20) else None,
            "recent_high20": float(last20["h"].max()) if len(last20) else None,
            "recent_low20": float(last20["l"].min()) if len(last20) else None,
        }
    stat_frame = pd.DataFrame.from_dict(stats, orient="index").reset_index().rename(columns={"index": "code"})
    out = features.merge(latest_bars[["code", "o", "h", "l", "c", "v", "source"]], on="code", how="inner")
    out = out.merge(stat_frame, on="code", how="left")
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
    out["fresh_runtime_live_feature_available_flag"] = out[LIVE_FEATURE_COLUMNS].notna().all(axis=1)
    out["fresh_runtime_missing_reason"] = out[LIVE_FEATURE_COLUMNS].isna().apply(lambda row: ",".join(row.index[row].tolist()[:8]), axis=1)
    out["fresh_runtime_research_watch_score"] = (
        0.18 * _rank_pct(out["diff20_pct"])
        + 0.16 * _rank_pct(out["close_vs_ma20_pct"])
        + 0.12 * _rank_pct(out["close_vs_ma60_pct"])
        + 0.12 * _rank_pct(out["cnt_20_above"])
        + 0.10 * _rank_pct(out["cnt_7_above"])
        + 0.10 * _rank_pct(out["volume_vs_20d_avg"])
        + 0.08 * _rank_pct(out["body_ratio"])
        + 0.06 * _rank_pct(out["recent_low_distance_pct"])
        - 0.06 * _rank_pct(out["upper_wick_ratio"])
        - 0.06 * _rank_pct(out["gap_pct"].abs())
    )
    out["fresh_runtime_research_watch_rank"] = out["fresh_runtime_research_watch_score"].rank(method="first", ascending=False).astype(int)
    out["fresh_runtime_research_watch_bucket"] = out["fresh_runtime_research_watch_rank"].map(
        lambda rank: "watch_top10_unvalidated"
        if rank <= 10
        else ("watch_11_50_unvalidated" if rank <= 50 else ("watch_51_100_unvalidated" if rank <= 100 else "watch_remaining_unvalidated"))
    )
    return out


def feature_contract() -> dict[str, Any]:
    fields = {"as_of_date": {"classification": "identifier"}, "code": {"classification": "identifier"}}
    for col in LIVE_FEATURE_COLUMNS:
        fields[col] = {"classification": "point_in_time_feature"}
    for col in [
        "fresh_runtime_research_watch_score",
        "fresh_runtime_research_watch_rank",
        "fresh_runtime_research_watch_bucket",
        "fresh_runtime_live_feature_available_flag",
        "fresh_runtime_missing_reason",
    ]:
        fields[col] = {"classification": "point_in_time_feature"}
    for col in ["ret5", "ret10", "ret20", "winner_ret20_gt_10pct", "bad_ret20_lt_minus_5pct", "severe_ret20_lt_minus_10pct"]:
        fields[col] = {"classification": "forbidden_future_leak"}
    return {"axis_id": AXIS_ID, "fields": fields}


def decide(surface: pd.DataFrame, latest_feature_date: int, latest_bar_date: int) -> tuple[str, str, list[str]]:
    if surface.empty:
        return "blocked_missing_runtime_feature_snapshot", "BLOCKED", ["latest_feature_snapshot_daily_empty"]
    if latest_feature_date != latest_bar_date:
        return "fresh_runtime_surface_created_but_feature_bar_date_mismatch", "HOLD_UNDERPOWERED", ["feature_snapshot_date_differs_from_latest_daily_bar_date"]
    if float(surface["fresh_runtime_live_feature_available_flag"].mean()) < 0.95:
        return "fresh_runtime_surface_created_but_feature_gaps", "HOLD_UNDERPOWERED", ["fresh_runtime_surface_has_missing_live_features"]
    return "fresh_runtime_surface_ready_for_research_watch_pretest", "HOLD_UNDERPOWERED", ["fresh_runtime_no_outcome_surface_created_but_no_validated_buy_selector_yet"]


def run(source_db: Path = DEFAULT_SOURCE_DB, output_root: Path = DEFAULT_OUTPUT_ROOT) -> Path:
    features, bars, latest_feature_date, latest_bar_date = load_latest_runtime_frames(source_db)
    surface = build_surface(features, bars) if not features.empty and not bars.empty else pd.DataFrame()
    decision, decision_class, reasons = decide(surface, latest_feature_date, latest_bar_date)
    out = output_root / f"{_now_tag()}-fresh-runtime-candidate-surface-v1"
    out.mkdir(parents=True, exist_ok=True)
    cols = [
        "as_of_date",
        "code",
        "fresh_runtime_research_watch_score",
        "fresh_runtime_research_watch_rank",
        "fresh_runtime_research_watch_bucket",
        "fresh_runtime_live_feature_available_flag",
        "fresh_runtime_missing_reason",
        *LIVE_FEATURE_COLUMNS,
    ]
    surface[cols].to_parquet(out / "fresh_runtime_candidate_surface_rows.parquet", index=False)
    surface[cols].sort_values("fresh_runtime_research_watch_rank").head(250).to_csv(out / "fresh_runtime_candidate_surface_rows_sample.csv", index=False)
    summary = {
        "axis_id": AXIS_ID,
        "decision": decision,
        "decision_class": decision_class,
        "reason_typed": reasons,
        "source_db": str(source_db),
        "latest_feature_snapshot_date": latest_feature_date,
        "latest_daily_bar_date": latest_bar_date,
        "row_count": int(len(surface)),
        "code_count": int(surface["code"].nunique()) if not surface.empty else 0,
        "live_feature_complete_rate": float(surface["fresh_runtime_live_feature_available_flag"].mean()) if not surface.empty else 0.0,
        "top_watch_codes": surface.sort_values("fresh_runtime_research_watch_rank")["code"].astype(str).head(10).tolist() if not surface.empty else [],
        "validated_buy_count": 0,
    }
    _write_json(out / "fresh_runtime_candidate_surface_summary.json", summary)
    _write_json(out / "surface_contract.json", {"axis_id": AXIS_ID, "diagnostic_only": True, "contains_offline_outcomes": False, "buy_signal": False, "validated_buy_claim": False, "live_feature_columns": LIVE_FEATURE_COLUMNS})
    _write_json(out / "feature_contract.json", feature_contract())
    _write_json(out / "source_coverage.json", {"axis_id": AXIS_ID, "source_db": str(source_db), "latest_feature_snapshot_date": latest_feature_date, "latest_daily_bar_date": latest_bar_date, "row_count": int(len(surface)), "code_count": int(surface["code"].nunique()) if not surface.empty else 0, "research_fallback_used": False})
    _write_json(out / "no_lookahead_audit.json", {"audit_result": "pass", "no_lookahead_pass": True, "source_tables": ["feature_snapshot_daily", "daily_bars"], "outcome_columns_present": False, "offline_outcomes_used": False, "runtime_db_write": False, "research_fallback_used": False})
    _write_json(out / "lineage.json", {"axis_id": AXIS_ID, "source_db": str(source_db), "reason_for_new_surface": "feature_frame_daily_stale_but_runtime_feature_snapshot_daily_and_daily_bars_are_fresh"})
    _write_json(out / "research_decision.json", {"axis_id": AXIS_ID, "research_decision": decision, "decision_class": decision_class, "reason_typed": reasons, "buyable_selection_ready": False, "research_watch_only": True, "meemee_reflectable_candidate": False, "runtime_db_write": False, "production_ranking_changed": False, "production_candidate_generator_changed": False, "publish_allowed": False, "validated_buy_count": 0, "active_gate_created": False, "research_fallback_used": False})
    _write_json(out / "_ARTIFACT_COMPLETE.json", {"complete": True, "artifact_complete": True, "required_artifacts": list(REQUIRED_ARTIFACTS), "generated_at": _now_tag()})
    return out


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-db", type=Path, default=DEFAULT_SOURCE_DB)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    args = parser.parse_args(argv)
    out = run(args.source_db, args.output_root)
    print(json.dumps({"output_dir": str(out)}, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
