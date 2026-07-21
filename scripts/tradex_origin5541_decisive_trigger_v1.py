from __future__ import annotations

import argparse
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd


AXIS_ID = "origin5541_decisive_trigger_direction_v1"
SCHEMA_VERSION = "tradex_origin5541_decisive_trigger_v1.compare.v1"
CODE = "5541"
EXPECTED_BUY_DECISIVE_DATE = "2025-11-04"
EXPECTED_BUY_CONTINUATION_DATE = "2025-11-28"
EXPECTED_SELL_DECISIVE_DATE = "2025-12-11"


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def load_bars(db_path: Path) -> pd.DataFrame:
    with duckdb.connect(str(db_path), read_only=True) as conn:
        bars = conn.execute(
            """
            SELECT strftime(to_timestamp(date), '%Y-%m-%d') AS trade_date,
                   o, h, l, c, v, source
            FROM daily_bars
            WHERE code = ?
              AND date BETWEEN epoch(TIMESTAMP '2025-07-01') AND epoch(TIMESTAMP '2025-12-26')
            ORDER BY date
            """,
            [CODE],
        ).fetchdf()
    if bars.empty or bars["trade_date"].duplicated().any():
        raise RuntimeError("5541 decisive-trigger input is empty or duplicated")
    return bars


def build_features(bars: pd.DataFrame) -> pd.DataFrame:
    frame = bars.copy()
    frame["prev_close"] = frame["c"].shift(1)
    frame["ret1"] = frame["c"] / frame["prev_close"] - 1
    frame["ret5"] = frame["c"] / frame["c"].shift(5) - 1
    frame["gap"] = frame["o"] / frame["prev_close"] - 1
    frame["body_pct"] = (frame["c"] - frame["o"]) / frame["prev_close"]
    frame["range_pct"] = (frame["h"] - frame["l"]) / frame["prev_close"]
    frame["volume_ratio20"] = frame["v"] / frame["v"].rolling(20).mean()
    for window in (7, 20, 60):
        frame[f"ma{window}"] = frame["c"].rolling(window).mean()
    frame["prior20_high"] = frame["h"].shift(1).rolling(20).max()
    frame["prior20_low"] = frame["l"].shift(1).rolling(20).min()
    frame["breakout20_up"] = frame["c"] / frame["prior20_high"] - 1
    frame["breakout20_down"] = frame["c"] / frame["prior20_low"] - 1
    frame["below_ma7"] = frame["c"] < frame["ma7"]
    frame["below_ma20"] = frame["c"] < frame["ma20"]
    frame["below_ma7_count5"] = frame["below_ma7"].rolling(5).sum()
    frame["below_ma20_count3"] = frame["below_ma20"].rolling(3).sum()
    frame["bearish_shock"] = (frame["ret1"] <= -0.05) & (frame["volume_ratio20"] >= 2.0)
    frame["bearish_shock_in_prior8"] = frame["bearish_shock"].shift(1).rolling(8).max().eq(1.0)
    return frame


def classify(features: pd.DataFrame) -> pd.DataFrame:
    frame = features.copy()
    frame["buy_decisive_raw"] = (
        (frame["ret1"] >= 0.04)
        & (frame["volume_ratio20"] >= 2.0)
        & (frame["breakout20_up"] >= 0.0)
        & (frame["c"] > frame["ma7"])
        & (frame["ma7"] > frame["ma20"])
        & (frame["ma20"] > frame["ma60"])
    )
    frame["sell_decisive_raw"] = (
        frame["bearish_shock_in_prior8"]
        & (frame["below_ma7_count5"] >= 4)
        & (frame["below_ma20_count3"] >= 2)
        & (frame["ret5"] <= -0.03)
        & (frame["ma7"] < frame["ma20"])
        & (frame["c"] > frame["ma60"])
        & (frame["breakout20_down"] > 0.0)
    )
    # Only the first confirmation in a contiguous raw-signal episode is a decisive trigger.
    frame["buy_decisive"] = frame["buy_decisive_raw"] & ~frame["buy_decisive_raw"].shift(1, fill_value=False)
    frame["sell_decisive"] = frame["sell_decisive_raw"] & ~frame["sell_decisive_raw"].shift(1, fill_value=False)
    frame["prior_buy_decisive_count20"] = frame["buy_decisive"].shift(1, fill_value=False).rolling(20).sum()
    frame["decision"] = "WAIT"
    frame.loc[frame["buy_decisive"], "decision"] = "BUY_DECISIVE_INITIAL"
    frame.loc[frame["buy_decisive"] & (frame["prior_buy_decisive_count20"] >= 1), "decision"] = "BUY_DECISIVE_CONTINUATION"
    frame.loc[frame["sell_decisive"], "decision"] = "SELL_DECISIVE_RETURN_SELL"
    return frame


def build_compare(classified: pd.DataFrame) -> tuple[dict[str, Any], pd.DataFrame]:
    events = classified[classified["buy_decisive"] | classified["sell_decisive"]].copy()
    buy_dates = events.loc[events["buy_decisive"], "trade_date"].tolist()
    sell_dates = events.loc[events["sell_decisive"], "trade_date"].tolist()
    buy_initial_dates = events.loc[events["decision"] == "BUY_DECISIVE_INITIAL", "trade_date"].tolist()
    buy_continuation_dates = events.loc[events["decision"] == "BUY_DECISIVE_CONTINUATION", "trade_date"].tolist()
    buy_match = buy_initial_dates == [EXPECTED_BUY_DECISIVE_DATE]
    buy_continuation_match = buy_continuation_dates == [EXPECTED_BUY_CONTINUATION_DATE]
    sell_match = sell_dates == [EXPECTED_SELL_DECISIVE_DATE]
    local_keep = buy_match and buy_continuation_match and sell_match
    key_dates = classified[classified["trade_date"].isin(
        ["2025-11-04", "2025-12-01", "2025-12-02", "2025-12-09", "2025-12-10", "2025-12-11"]
    )]
    key_rows = []
    for row in key_dates.itertuples(index=False):
        key_rows.append(
            {
                "trade_date": row.trade_date,
                "decision": row.decision,
                "close": float(row.c),
                "ret1_pct": float(row.ret1 * 100),
                "ret5_pct": float(row.ret5 * 100),
                "volume_ratio20": float(row.volume_ratio20),
                "ma7": float(row.ma7),
                "ma20": float(row.ma20),
                "ma60": float(row.ma60),
                "breakout20_up_pct": float(row.breakout20_up * 100),
                "breakout20_down_pct": float(row.breakout20_down * 100),
                "below_ma7_count5": int(row.below_ma7_count5),
                "below_ma20_count3": int(row.below_ma20_count3),
                "bearish_shock_in_prior8": bool(row.bearish_shock_in_prior8),
            }
        )
    compare = {
        "schema_version": SCHEMA_VERSION,
        "axis_id": AXIS_ID,
        "artifact_role": "authoritative_single_ticker_two_side_decisive_trigger_replication",
        "review_only": True,
        "fixed_conditions": {
            "code": CODE,
            "feature_source": "MeeMee confirmed PAN daily_bars",
            "costs_slippage_borrow": "ignored",
            "same_state_machine": ["precursor", "probe_or_hedge_adjustment", "decisive_confirmation"],
            "buy_geometry": "base_breakout_expansion",
            "sell_geometry": "high_zone_failure_return_sell_sequence",
            "true_breakdown_required_for_sell": False,
            "future_used_for_trigger": False,
        },
        "trigger_contract": {
            "buy_decisive": {
                "ret1_gte": 0.04,
                "volume_ratio20_gte": 2.0,
                "close_breaks_prior20_high": True,
                "ma_order": "close > ma7 > ma20 > ma60",
            },
            "sell_decisive_return_sell": {
                "prior8_contains_ret_le_minus5pct_and_volume_ratio_gte2": True,
                "last5_closes_below_ma7_gte": 4,
                "last3_closes_below_ma20_gte": 2,
                "ret5_lte": -0.03,
                "ma_relation": "ma7 < ma20 while close remains above ma60",
                "prior20_low_not_broken": True,
            },
        },
        "authoritative_result": {
            "buy_decisive_dates": buy_dates,
            "buy_initial_decisive_dates": buy_initial_dates,
            "buy_continuation_decisive_dates": buy_continuation_dates,
            "sell_decisive_dates": sell_dates,
            "expected_buy_decisive_date": EXPECTED_BUY_DECISIVE_DATE,
            "expected_buy_continuation_date": EXPECTED_BUY_CONTINUATION_DATE,
            "expected_sell_decisive_date": EXPECTED_SELL_DECISIVE_DATE,
            "buy_exact_match": buy_match,
            "buy_continuation_exact_match": buy_continuation_match,
            "sell_exact_match": sell_match,
            "false_trigger_count_in_fixed_period": max(0, len(buy_dates) - 2) + max(0, len(sell_dates) - 1),
            "key_date_evidence": key_rows,
        },
        "observed_branching": {
            "changed_top5_members_count": None,
            "changed_top10_members_count": None,
            "changed_rank_count": None,
            "selection_divergence_reason": "single-ticker trigger replication only",
            "buy_event_count": len(buy_dates),
            "sell_event_count": len(sell_dates),
            "side_taxonomy_diverged": True,
            "side_taxonomy_reason": "buy is expansion breakout; sell is high-zone failed-return confirmation, not low breakdown",
        },
        "judgment": {
            "candidate_local_decision": "keep_for_full_universe_validation" if local_keep else "hold",
            "session_aggregate_decision": "hold_single_ticker_only",
            "authoritative_rollup_decision": "hold_review_only_pending_full_universe_recent_first_validation",
            "reason_type": "initial_continuation_and_sell_decisive_dates_replicated_without_false_trigger" if local_keep else "article_decisive_date_replication_failed",
        },
        "not_changed": ["MeeMee", "ranking", "runtime DB", "production trading logic", "position sizing", "hedge ratios"],
        "remaining_risks": [
            "thresholds were derived from one known successful episode",
            "sell branch is return-sell and must not be reported as true breakdown",
            "monthly and weekly permission are not yet encoded in this micro-replication",
            "full-universe false-positive rate and missed-decisive rate are unknown",
        ],
    }
    return compare, events


def run(db_path: Path, output: Path) -> dict[str, Any]:
    output.mkdir(parents=True, exist_ok=True)
    bars = load_bars(db_path)
    classified = classify(build_features(bars))
    compare, events = build_compare(classified)
    ledger_path = output / "decisive_event_ledger.csv"
    compare_path = output / "compare.json"
    audit_path = output / "audit.json"
    events.to_csv(ledger_path, index=False)
    _write_json(compare_path, compare)
    _write_json(
        audit_path,
        {
            "schema_version": "tradex_origin5541_decisive_trigger_v1.audit.v1",
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "db_path": str(db_path.resolve()),
            "db_read_only": True,
            "bar_rows": len(bars),
            "source_values": sorted(bars["source"].dropna().unique().tolist()),
            "future_used_for_trigger": False,
            "review_only": True,
        },
    )
    _write_json(
        output / "_ARTIFACT_COMPLETE.json",
        {
            "complete": True,
            "authoritative": "compare.json",
            "compare_sha256": _sha256(compare_path),
            "audit_sha256": _sha256(audit_path),
            "ledger_sha256": _sha256(ledger_path),
        },
    )
    return {"output": str(output.resolve()), "result": compare["authoritative_result"], "judgment": compare["judgment"]}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    print(json.dumps(run(args.db, args.output), ensure_ascii=False, indent=2))
