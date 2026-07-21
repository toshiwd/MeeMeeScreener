from __future__ import annotations

import argparse
import hashlib
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd

if __package__ in {None, ""}:
    sys.path.append(str(Path(__file__).resolve().parents[1]))

from scripts import tradex_decisive_trigger_full_universe_v1 as base


AXIS_ID = "decisive_trigger_pre_event_shape_v1"
SCHEMA_VERSION = "tradex_decisive_trigger_pre_event_shape_v1.compare.v1"
BUY_FAMILIES = {"BUY_DECISIVE_INITIAL", "BUY_DECISIVE_CONTINUATION"}
SELL_FAMILIES = {"SELL_DECISIVE_RETURN_SELL"}
SHAPE_COLUMNS = (
    "box_contraction_up_break",
    "prior_high_break_hold",
    "higher_pullback_low",
    "high_update_failure",
    "lower_rebound_high",
    "support_break_rebound_fail",
)


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _write_json(path: Path, payload: Any) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, allow_nan=False, default=str) + "\n", encoding="utf-8")


def _write_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    path.write_text("".join(json.dumps(row, ensure_ascii=False, allow_nan=False, default=str) + "\n" for row in rows), encoding="utf-8")


def load_bars(db_path: Path) -> pd.DataFrame:
    with duckdb.connect(str(db_path), read_only=True) as conn:
        bars = conn.execute(
            """
            SELECT code, strftime(to_timestamp(date), '%Y-%m-%d') AS trade_date,
                   o, h, l, c, v, source
            FROM daily_bars
            WHERE source='pan'
              AND date BETWEEN epoch(TIMESTAMP '2022-01-01') AND epoch(TIMESTAMP '2026-07-17')
            ORDER BY code, date
            """
        ).fetchdf()
    if bars.empty or bars.duplicated(["code", "trade_date"]).any():
        raise RuntimeError("confirmed PAN shape input is empty or duplicated")
    return bars


def add_shape_features(group: pd.DataFrame) -> pd.DataFrame:
    frame = group.sort_values("trade_date").copy()
    high, low, close, open_ = frame["h"], frame["l"], frame["c"], frame["o"]
    prior10_high = high.shift(1).rolling(10, min_periods=10).max()
    prior10_low = low.shift(1).rolling(10, min_periods=10).min()
    prior20_high = high.shift(1).rolling(20, min_periods=20).max()
    prior20_low = low.shift(1).rolling(20, min_periods=20).min()
    prior40_high = high.shift(1).rolling(40, min_periods=40).max()
    prior40_low = low.shift(1).rolling(40, min_periods=40).min()
    ma7 = close.rolling(7, min_periods=7).mean()
    ma20 = close.rolling(20, min_periods=20).mean()
    recent_low = low.shift(1).rolling(5, min_periods=5).min()
    earlier_low = low.shift(6).rolling(10, min_periods=10).min()
    recent_high = high.shift(1).rolling(5, min_periods=5).max()
    earlier_high = high.shift(6).rolling(10, min_periods=10).max()
    prior_break = close.shift(1) < prior20_low.shift(1)
    recent_support_break = prior_break.rolling(5, min_periods=1).max().fillna(0).astype(bool)

    range10 = (prior10_high - prior10_low) / close
    range40 = (prior40_high - prior40_low) / close
    frame["box_contraction_up_break"] = (range10 <= range40 * 0.60) & (close > prior10_high)
    frame["prior_high_break_hold"] = (close > prior20_high) & (low >= prior20_high * 0.985)
    frame["higher_pullback_low"] = (recent_low >= earlier_low * 1.01) & (close > ma20) & (close >= ma7 * 0.985)
    frame["high_update_failure"] = (high >= prior20_high * 0.995) & (close < prior20_high * 0.985) & (close < open_)
    frame["lower_rebound_high"] = (recent_high <= earlier_high * 0.98) & (close < ma7) & (ma7 < ma20)
    frame["support_break_rebound_fail"] = recent_support_break & (high < ma20 * 1.01) & (close < ma20) & (close < open_)
    return frame[["code", "trade_date", *SHAPE_COLUMNS]]


def build_shape_table(bars: pd.DataFrame) -> pd.DataFrame:
    return pd.concat([add_shape_features(group) for _, group in bars.groupby("code", sort=False)], ignore_index=True)


def enrich_events(events: pd.DataFrame, shapes: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, Any]]:
    if shapes.duplicated(["code", "trade_date"]).any():
        raise RuntimeError("shape join key is not unique")
    enriched = events.merge(shapes, on=["code", "trade_date"], how="left", validate="many_to_one")
    for column in SHAPE_COLUMNS:
        enriched[column] = enriched[column].fillna(False).astype(bool)
    enriched["buy_shape_any"] = enriched[list(SHAPE_COLUMNS[:3])].any(axis=1)
    enriched["sell_shape_any"] = enriched[list(SHAPE_COLUMNS[3:])].any(axis=1)
    return enriched, {
        "event_rows_before_join": int(len(events)),
        "event_rows_after_join": int(len(enriched)),
        "join_row_multiplier": float(len(enriched) / len(events)) if len(events) else None,
        "duplicate_event_keys_after_join": int(enriched.duplicated(["code", "trade_date", "event_type"]).sum()),
        "null_shape_rows": int(enriched[list(SHAPE_COLUMNS)].isna().any(axis=1).sum()),
    }


def _temporal_positive(events: pd.DataFrame) -> bool:
    complete = events[events["outcome_complete20"]]
    eligible = [group for _, group in complete.groupby("event_year") if len(group) >= 20]
    return bool(eligible) and all(group["directional_ret20"].mean() > 0 for group in eligible)


def _comparison(family: pd.DataFrame, selected: pd.DataFrame) -> dict[str, Any]:
    baseline = base.summarize(family)
    shaped = base.summarize(selected)
    retention = len(selected) / len(family) if len(family) else 0.0
    return {
        "baseline": baseline,
        "selected": shaped,
        "event_retention_rate": retention,
        "directional_ret20_mean_lift_pct_points": (shaped["directional_ret20_mean_pct"] or 0) - (baseline["directional_ret20_mean_pct"] or 0),
        "directional_ret20_win_rate_lift": (shaped["directional_ret20_win_rate"] or 0) - (baseline["directional_ret20_win_rate"] or 0),
        "completed_years_n_ge_20_all_positive_mean20": _temporal_positive(selected),
    }


def build_compare(events: pd.DataFrame, quality: dict[str, Any], source_compare: dict[str, Any]) -> dict[str, Any]:
    results: dict[str, Any] = {}
    decisions: dict[str, Any] = {}
    for event_type, family in events.groupby("event_type"):
        relevant = SHAPE_COLUMNS[:3] if event_type in BUY_FAMILIES else SHAPE_COLUMNS[3:]
        family_results = {shape: _comparison(family, family[family[shape]]) for shape in relevant}
        any_column = "buy_shape_any" if event_type in BUY_FAMILIES else "sell_shape_any"
        family_results[any_column] = _comparison(family, family[family[any_column]])
        results[event_type] = family_results
        candidate = family_results[any_column]
        selected = candidate["selected"]
        gates = {
            "complete20_count_ge_50": selected["complete20_count"] >= 50,
            "retention_between_0_15_and_0_80": 0.15 <= candidate["event_retention_rate"] <= 0.80,
            "mean20_lift_gt_0": candidate["directional_ret20_mean_lift_pct_points"] > 0,
            "win_rate_lift_gt_0": candidate["directional_ret20_win_rate_lift"] > 0,
            "median20_gt_0": (selected["directional_ret20_median_pct"] or -999) > 0,
            "trim5_mean20_gt_0": (selected["directional_ret20_trim5_mean_pct"] or -999) > 0,
            "symbol_equal_mean20_gt_0": (selected["directional_ret20_symbol_equal_mean_pct"] or -999) > 0,
            "completed_years_n_ge_20_all_positive_mean20": candidate["completed_years_n_ge_20_all_positive_mean20"],
        }
        decisions[event_type] = {"candidate": any_column, "gates": gates, "authoritative_decision": "keep_review_only" if all(gates.values()) else "drop"}
    kept = [key for key, row in decisions.items() if row["authoritative_decision"] == "keep_review_only"]
    return {
        "schema_version": SCHEMA_VERSION,
        "axis_id": AXIS_ID,
        "artifact_role": "authoritative_pre_event_shape_fixed_definition_validation",
        "review_only": True,
        "fixed_conditions": {
            "source_event_artifact": source_compare.get("axis_id"),
            "event_thresholds_changed": False,
            "shape_thresholds_tuned_on_outcomes": False,
            "shape_information_cutoff": "signal date close",
            "future_used_for_shape": False,
            "future_used_for_outcome_only": True,
            "costs_slippage_borrow": "ignored",
        },
        "shape_definitions": {
            "box_contraction_up_break": "prior 10-session range <= 60% of prior 40-session range and close above prior 10-session high",
            "prior_high_break_hold": "close above prior 20-session high and low no more than 1.5% below that high",
            "higher_pullback_low": "recent 5-session low >= 1% above preceding 10-session low, close above MA20 and not materially below MA7",
            "high_update_failure": "touch prior 20-session high, close at least 1.5% below it, bearish candle",
            "lower_rebound_high": "recent 5-session high >=2% below preceding 10-session high, close below MA7, MA7 below MA20",
            "support_break_rebound_fail": "support break observed in prior 5 sessions, rebound remains under MA20, bearish close",
        },
        "data_quality": quality,
        "authoritative_results": results,
        "family_decisions": decisions,
        "judgment": {
            "candidate_local_decision": "keep_selected_shape_families" if kept else "drop_axis",
            "kept_families": kept,
            "session_aggregate_decision": "hold_review_only_no_meemee_reflection",
            "authoritative_rollup_decision": "hold_review_only_pre_event_shape_axis_complete",
        },
        "not_changed": ["MeeMee", "ranking", "runtime DB", "production trading logic", "daily trigger thresholds", "position sizing", "hedge ratios"],
        "remaining_risks": [
            "manual blind MeeMee image review is a separate human judgment layer and is not completed by numeric tags",
            "current runtime universe retains survivorship bias",
            "fixed geometric thresholds require an independent future-period confirmation before promotion",
        ],
    }


def build_blind_sample(events: pd.DataFrame, per_class: int = 8) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    complete = events[events["outcome_complete20"]].copy()
    complete["outcome_class"] = complete["directional_ret20"].gt(0).map({True: "success", False: "failure"})
    chosen = []
    for (event_type, outcome_class), group in complete.groupby(["event_type", "outcome_class"]):
        ordered = group.assign(_key=group.apply(lambda row: hashlib.sha256(f"{row['code']}:{row['trade_date']}:{event_type}".encode()).hexdigest(), axis=1)).sort_values("_key")
        chosen.append(ordered.head(per_class))
    sample = pd.concat(chosen, ignore_index=True) if chosen else complete.head(0)
    review_rows: list[dict[str, Any]] = []
    sealed_rows: list[dict[str, Any]] = []
    for index, row in sample.sort_values(["event_type", "trade_date", "code"]).reset_index(drop=True).iterrows():
        review_id = f"shape-{index + 1:03d}"
        visible = {
            "review_id": review_id,
            "code": str(row["code"]),
            "as_of": str(row["trade_date"]),
            "event_type": row["event_type"],
            "review_decision": "unreviewed",
            "selected_shapes": [],
            "allowed_shapes": list(SHAPE_COLUMNS),
            "confidence": None,
            "notes": "",
        }
        review_rows.append(visible)
        sealed_rows.append({
            "review_id": review_id,
            "outcome_class": row["outcome_class"],
            "directional_ret20": float(row["directional_ret20"]),
            "directional_adverse20": float(row["directional_adverse20"]),
            "numeric_shapes": {column: bool(row[column]) for column in SHAPE_COLUMNS},
        })
    return review_rows, sealed_rows


def run(db_path: Path, source_run: Path, output: Path, per_class: int = 8) -> dict[str, Any]:
    output.mkdir(parents=True, exist_ok=False)
    events = pd.read_parquet(source_run / "higher_timeframe_event_ledger.parquet")
    source_compare = json.loads((source_run / "compare.json").read_text(encoding="utf-8"))
    enriched, quality = enrich_events(events, build_shape_table(load_bars(db_path)))
    compare = build_compare(enriched, quality, source_compare)
    review_rows, sealed_rows = build_blind_sample(enriched, per_class=per_class)
    ledger_path = output / "pre_event_shape_ledger.parquet"
    compare_path = output / "compare.json"
    review_path = output / "blind_review_template.jsonl"
    sealed_path = output / "sealed_outcomes.jsonl"
    audit_path = output / "audit.json"
    enriched.to_parquet(ledger_path, index=False)
    _write_json(compare_path, compare)
    _write_jsonl(review_path, review_rows)
    _write_jsonl(sealed_path, sealed_rows)
    samples_arg = ",".join(f"{row['code']}:{row['as_of']}" for row in review_rows)
    _write_json(output / "screenshot_command.json", {
        "boundary_owner": "MeeMee",
        "command": f"node scripts/meemee_detail_clean_screenshot_batch_v1.mjs --samples {samples_arg}",
        "outcomes_in_render_input": False,
        "future_bars_visible": False,
        "centered_capture_prohibited": True,
        "review_timeframes": ["monthly", "weekly", "daily"],
    })
    _write_json(audit_path, {
        "schema_version": f"{AXIS_ID}.audit.v1",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "db_path": str(db_path.resolve()),
        "db_read_only": True,
        "source_run": str(source_run.resolve()),
        "source_compare_sha256": _sha256(source_run / "compare.json"),
        "source_ledger_sha256": _sha256(source_run / "higher_timeframe_event_ledger.parquet"),
        "quality": quality,
        "blind_review_rows": len(review_rows),
        "outcomes_excluded_from_review_template": True,
        "review_only": True,
    })
    _write_json(output / "_ARTIFACT_COMPLETE.json", {
        "complete": True,
        "authoritative": "compare.json",
        "compare_sha256": _sha256(compare_path),
        "audit_sha256": _sha256(audit_path),
        "ledger_sha256": _sha256(ledger_path),
        "review_template_sha256": _sha256(review_path),
        "sealed_outcomes_sha256": _sha256(sealed_path),
    })
    return {"output": str(output.resolve()), "judgment": compare["judgment"], "blind_review_rows": len(review_rows)}


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", required=True, type=Path)
    parser.add_argument("--source-run", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--per-class", type=int, default=8)
    args = parser.parse_args()
    print(json.dumps(run(args.db, args.source_run, args.output, args.per_class), ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
