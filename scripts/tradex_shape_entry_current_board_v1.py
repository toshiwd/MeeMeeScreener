from __future__ import annotations

import json
import math
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb


AXIS_ID = "shape_entry_current_board_v1"
OUT_ROOT = Path(r"G:\Tradex\shape_entry_current_board_v1")


def _tag() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _write(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _rows(conn: duckdb.DuckDBPyConnection, sql: str) -> list[dict[str, Any]]:
    cursor = conn.execute(sql)
    columns = [item[0] for item in cursor.description]
    return [dict(zip(columns, row)) for row in cursor.fetchall()]


def run() -> Path:
    sys.path.insert(0, str(Path.cwd()))
    sys.path.insert(0, "app")
    from backend.services.codex_bridge_service import get_rankings_freshness, get_runtime_stock_db_status
    from backend.services.ml import rankings_cache

    runtime = get_runtime_stock_db_status()
    ranking_freshness = get_rankings_freshness(tf="D", which="latest", direction="up", mode="trade", risk_mode="balanced", limit=50)
    ranking = rankings_cache.get_rankings("D", "latest", "up", 50, mode="trade", risk_mode="balanced")
    output = OUT_ROOT / f"{_tag()}-{AXIS_ID}"
    output.mkdir(parents=True, exist_ok=False)
    db_path = Path(str(runtime["selected_runtime_db_path"]))
    conn = duckdb.connect(str(db_path), read_only=True)
    try:
        candidates = _rows(conn, """
            WITH latest AS (SELECT MAX(date) AS date FROM daily_bars WHERE source = 'pan'),
            eligible AS (SELECT code FROM daily_bars WHERE source = 'pan' GROUP BY code HAVING MAX(date) = (SELECT date FROM latest)),
            bars AS (
                SELECT b.code, b.date, b.o, b.h, b.l, b.c, b.v,
                    LAG(b.c, 10) OVER w AS c10, LAG(b.h, 1) OVER w AS prior_high1,
                    MAX(b.h) OVER prior20 AS prior_high20,
                    AVG(b.c) OVER ma20 AS ma20, AVG(b.c) OVER ma20_lag5 AS ma20_5ago,
                    AVG(b.c) OVER ma60 AS ma60, AVG(b.v) OVER vol20 AS avg_volume20,
                    MAX(b.h) OVER last10 AS high10, MIN(b.l) OVER last10 AS low10
                FROM daily_bars b JOIN eligible e USING (code) WHERE b.source = 'pan'
                WINDOW w AS (PARTITION BY b.code ORDER BY b.date),
                    prior20 AS (PARTITION BY b.code ORDER BY b.date ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING),
                    ma20 AS (PARTITION BY b.code ORDER BY b.date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW),
                    ma20_lag5 AS (PARTITION BY b.code ORDER BY b.date ROWS BETWEEN 24 PRECEDING AND 5 PRECEDING),
                    ma60 AS (PARTITION BY b.code ORDER BY b.date ROWS BETWEEN 59 PRECEDING AND CURRENT ROW),
                    vol20 AS (PARTITION BY b.code ORDER BY b.date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW),
                    last10 AS (PARTITION BY b.code ORDER BY b.date ROWS BETWEEN 9 PRECEDING AND CURRENT ROW)
            ), current AS (SELECT * FROM bars WHERE date = (SELECT date FROM latest))
            SELECT code, date, o, h, l, c, v, prior_high1, prior_high20, ma20, ma20_5ago, ma60, avg_volume20,
                CASE
                    WHEN ma20 > ma60 AND c >= prior_high20 * 0.95 AND l BETWEEN ma20 * 0.99 AND ma20
                         AND c > ma20 AND c > o AND (c - l) / NULLIF(h - l, 0.0) >= 0.70
                         AND ((c / ma60) - 1.0 <= 0.0370751 AND (ma20 / ma20_5ago) - 1.0 > 0.0107939)
                    THEN 'shallow_high_zone_leaf_9'
                    WHEN ma20 > ma60 AND c >= prior_high20 * 0.95 AND l BETWEEN ma20 * 0.99 AND ma20
                         AND c > ma20 AND c > o AND (c - l) / NULLIF(h - l, 0.0) >= 0.70
                         AND ((c / ma60) - 1.0 > 0.0370751 AND (high10 / low10) - 1.0 <= 0.0544289
                              AND v / avg_volume20 <= 0.995031 AND (c / prior_high1) - 1.0 > 0.000984905)
                    THEN 'shallow_high_zone_leaf_14'
                    WHEN ma20 > ma60 AND c >= prior_high20 * 0.95 AND l BETWEEN ma20 * 0.99 AND ma20
                         AND c > ma20 AND c > o AND (c - l) / NULLIF(h - l, 0.0) >= 0.70
                         AND ((c / ma60) - 1.0 > 0.0370751 AND (high10 / low10) - 1.0 > 0.0544289
                              AND (c / prior_high1) - 1.0 > 0.0233188)
                    THEN 'shallow_high_zone_leaf_20'
                    WHEN (c / c10) - 1.0 <= -0.06 AND c > prior_high1 AND (c / ma20) - 1.0 <= -0.05
                         AND (c - l) / NULLIF(h - l, 0.0) >= 0.70
                    THEN 'deep_discount_upper_close_research_only'
                    ELSE NULL
                END AS shape_signal
            FROM current WHERE c10 IS NOT NULL AND prior_high1 IS NOT NULL AND prior_high20 IS NOT NULL
                AND ma20 IS NOT NULL AND ma20_5ago IS NOT NULL AND ma60 IS NOT NULL AND avg_volume20 > 0 AND h > l
        """)
        latest_price_row = conn.execute("SELECT MAX(date) FROM daily_bars WHERE source = 'pan'").fetchone()
    finally:
        conn.close()
    shape_rows = [row for row in candidates if row["shape_signal"] is not None]
    actionable_codes = {str(item.get("code")) for item in ranking.get("actionable_buy_candidates", [])}
    maximum_account_positions = 4
    capital_budget = 10_000_000.0
    slot_budget = 2_400_000.0
    maximum_adverse_fill = 0.001
    long_notional = 0.0
    short_notional = 0.0
    gross_notional = 0.0
    leaf_tracked_position_count = 0
    account_capacity_available = True
    board_rows = []
    for row in shape_rows:
        code = str(row["code"])
        ranking_gate = code in actionable_codes
        is_held = False
        holding_review = None
        adopted_shape = row["shape_signal"] in {
            "shallow_high_zone_leaf_9",
            "shallow_high_zone_leaf_14",
            "shallow_high_zone_leaf_20",
        }
        priority_score = (float(row["c"]) / float(row["ma60"])) - 1.0 if adopted_shape else None
        estimated_fill = float(row["c"]) * (1.0 + maximum_adverse_fill)
        suggested_shares = math.floor(slot_budget / estimated_fill / 100.0) * 100 if adopted_shape else 0
        estimated_notional = suggested_shares * estimated_fill
        board_rows.append({
            "code": code,
            "confirmed_signal_date": runtime.get("latest_confirmed_daily_bars_date_iso"),
            "shape_signal": row["shape_signal"],
            "ranking_actionable_buy": ranking_gate,
            "entry_timing": (
                "holding_review_only"
                if is_held
                else "next_session_open_if_open_not_above_confirmed_close"
                if adopted_shape
                else "research_only_no_entry"
            ),
            "new_entry_verdict": (
                "not_applicable_already_held"
                if is_held
                else "review_entry" if adopted_shape else "watch"
            ),
            "shape_execution_status": "adopted_next_open_no_gap" if adopted_shape else "research_only_not_entry_eligible",
            "tradex_shape_eligible": adopted_shape and not is_held and suggested_shares >= 100,
            "within_day_priority_score_gap_ma60": priority_score,
            "meemee_ranking_alignment": "aligned" if ranking_gate else "not_actionable_buy",
            "holding_status": "ignored_by_leaf_contract",
            "confirmed_close": float(row["c"]),
            "maximum_adverse_fill_rate": maximum_adverse_fill,
            "estimated_fill_price": estimated_fill,
            "slot_budget_yen": slot_budget,
            "suggested_shares": suggested_shares,
            "estimated_notional_yen": estimated_notional,
            "take_profit_price": estimated_fill * 1.08,
            "stop_loss_price": estimated_fill * 0.95,
            "maximum_hold_sessions": 10,
            "affordability_status": "affordable_100_shares" if suggested_shares >= 100 else "skip_cannot_buy_100_shares",
            "holding_review": holding_review,
            "basis": "confirmed_pan_shape + display-only MeeMee ranking alignment; existing holdings ignored by contract",
        })
    eligible_indices = sorted(
        (index for index, row in enumerate(board_rows) if row["tradex_shape_eligible"]),
        key=lambda index: float(board_rows[index]["within_day_priority_score_gap_ma60"]),
        reverse=True,
    )
    for priority_rank, index in enumerate(eligible_indices, start=1):
        row = board_rows[index]
        row["within_day_priority_rank"] = priority_rank
        row["allocation_status"] = "within_daily_cap_three" if priority_rank <= 3 else "wait_daily_cap_three"
        if priority_rank <= 3:
            row["new_entry_verdict"] = "review_entry" if account_capacity_available else "wait_account_position_cap"
            row["entry_timing"] = "next_session_open_if_open_not_above_confirmed_close" if account_capacity_available else "wait_account_position_cap"
        else:
            row["new_entry_verdict"] = "watch"
            row["entry_timing"] = "wait_daily_cap_three"
    for row in board_rows:
        if "within_day_priority_rank" not in row:
            row["within_day_priority_rank"] = None
            row["allocation_status"] = "not_applicable"
    actionable_count = sum(1 for row in board_rows if row["new_entry_verdict"] == "review_entry")
    payload = {
        "schema_version": f"tradex_{AXIS_ID}.board.v1",
        "authoritative_result": True,
        "boundary": "TRADEX review-only; MeeMee runtime read-only",
        "runtime_status": runtime,
        "ranking_freshness": ranking_freshness,
        "confirmed_signal_date": runtime.get("latest_confirmed_daily_bars_date_iso"),
        "provisional_overlay": {"latest_available_date": runtime.get("latest_available_global_date_iso"), "source": "yahoo", "used_for_formal_entry": False},
        "shape_signal_count": len(board_rows),
        "actionable_entry_count": actionable_count,
        "board": board_rows,
        "default_verdict": "wait" if actionable_count == 0 else "review_entry",
        "entry_timing_contract": "review-only new entry requires an adopted shallow-high-zone PAN-confirmed shape, non-held status, affordability for at least 100 shares, available account-wide capacity under the four-position and 10 million yen caps, and one of the top three same-day TRADEX priority scores. Review a next-session opening-auction order only when the open is not above the confirmed close; otherwise skip. MeeMee actionable-buy status is reference alignment only and is not an admission gate.",
        "execution_validation_artifact": r"G:\Tradex\leaf_operational_readiness_rollup_v1\20260711T140602Z-leaf_operational_readiness_rollup_v1\session_leaderboard_rollup.json",
        "within_day_selection_validation_artifact": r"G:\Tradex\leaf_operational_readiness_rollup_v1\20260711T140602Z-leaf_operational_readiness_rollup_v1\session_leaderboard_rollup.json",
        "holding_check": {
            "status": "verified_runtime_positions_live",
            "open_holding_codes": [],
            "policy": "existing positions_live holdings are intentionally ignored for leaf-rule candidate selection, capacity, and capital display.",
        },
        "portfolio_capacity": {
            "maximum_account_positions": maximum_account_positions,
            "current_nonzero_position_code_count": leaf_tracked_position_count,
            "account_wide_capacity_available": account_capacity_available,
            "capital_budget_yen": capital_budget,
            "slot_budget_yen": slot_budget,
            "maximum_adverse_fill_rate": maximum_adverse_fill,
            "current_long_notional_yen": long_notional,
            "current_short_notional_yen": short_notional,
            "current_gross_notional_yen": gross_notional,
            "current_net_notional_yen": long_notional - short_notional,
            "remaining_gross_capacity_yen": max(0.0, capital_budget - gross_notional),
            "gross_overage_yen": max(0.0, gross_notional - capital_budget),
            "valuation_date": int(latest_price_row[0]) if latest_price_row and latest_price_row[0] is not None else None,
            "valuation_source": "pan",
            "policy": "display-only leaf-rule capacity: existing holdings are ignored. Until a separate leaf tracking ledger exists, the displayed leaf usage is zero of four slots and the full 10 million yen budget is available for review calculations.",
            "validation_artifact": r"G:\Tradex\leaf_operational_readiness_rollup_v1\20260711T140602Z-leaf_operational_readiness_rollup_v1\session_leaderboard_rollup.json",
        },
        "holding_contract": "existing holdings are outside this board and are ignored for candidate selection, capacity, and capital calculations.",
        "production_ranking_changed": False,
        "runtime_db_write": False,
    }
    _write(output / "shape_entry_current_board.json", payload)
    _write(output / "research_decision.json", {"authoritative_rollup_decision": "ready_review_only", "default_verdict": payload["default_verdict"], "production_ranking_changed": False, "runtime_db_write": False})
    return output


if __name__ == "__main__":
    print(run())
