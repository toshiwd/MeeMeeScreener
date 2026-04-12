from __future__ import annotations

import json
import math
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Any

from external_analysis.exporter.source_reader import connect_source_db, source_table_exists
from external_analysis.models.candidate_baseline import (
    _as_of_date_text,
    _build_reason_codes,
    _normalize_as_of_date,
    _safe_float,
    _score_frame,
    load_candidate_input_frame,
)
from external_analysis.models.forecast_surface_learning import load_or_train_forecast_surface_bundle, predict_current_surface
from external_analysis.results.result_schema import connect_result_db, ensure_result_schema

FORECAST_SURFACE_VERSION = "forecast_surface_v1"
FORECAST_LOOKBACK_DAYS = 20
FORECAST_LOOKAHEAD_DAYS = 60


@dataclass(frozen=True)
class _SourceContext:
    signal: dict[tuple[str, str], dict[str, Any]]
    trade: dict[str, dict[str, Any]]
    borrow: dict[str, dict[str, Any]]
    events: dict[str, dict[str, Any]]
    edinet: dict[str, dict[str, Any]]
    market: dict[str, Any]
    presence: dict[str, bool]


def _utcnow() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def _as_date(value: int) -> date:
    text = str(int(value))
    return datetime.strptime(text, "%Y%m%d").date()


def _json_dump(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True)


def _clamp01(value: float) -> float:
    return max(0.0, min(1.0, float(value)))


def _nonempty_tokens(values: list[Any]) -> list[str]:
    tokens: list[str] = []
    for value in values:
        text = str(value or "").strip()
        if text:
            tokens.append(text)
    return tokens


def _sigmoid(value: float) -> float:
    if value >= 0:
        z = math.exp(-float(value))
        return 1.0 / (1.0 + z)
    z = math.exp(float(value))
    return z / (1.0 + z)


def _table_exists(conn, table_name: str) -> bool:
    row = conn.execute(
        """
        SELECT 1
        FROM information_schema.tables
        WHERE table_schema = 'main' AND table_name = ?
        LIMIT 1
        """,
        [table_name],
    ).fetchone()
    return bool(row)


def _load_signal_context(conn, *, as_of_date: int) -> dict[tuple[str, str], dict[str, Any]]:
    if not source_table_exists(conn, "signal_decision_daily"):
        return {}
    rows = conn.execute(
        """
        SELECT
            code,
            side,
            entry_qualified,
            setup_type,
            reason_snapshot_json,
            score_snapshot_json,
            rank_snapshot_json,
            forward_return_5,
            forward_return_20,
            forward_return_30,
            forward_return_60,
            max_favorable_30,
            max_adverse_30
        FROM signal_decision_daily
        WHERE dt = ?
        """,
        [as_of_date],
    ).fetchall()
    payload: dict[tuple[str, str], dict[str, Any]] = {}
    for row in rows:
        code = str(row[0])
        side = str(row[1])
        payload[(code, side)] = {
            "entry_qualified": bool(row[2]),
            "setup_type": str(row[3] or ""),
            "reason_snapshot_json": row[4],
            "score_snapshot_json": row[5],
            "rank_snapshot_json": row[6],
            "forward_return_5": row[7],
            "forward_return_20": row[8],
            "forward_return_30": row[9],
            "forward_return_60": row[10],
            "max_favorable_30": row[11],
            "max_adverse_30": row[12],
        }
    return payload


def _load_trade_context(conn, *, as_of_date: int) -> dict[str, dict[str, Any]]:
    if not source_table_exists(conn, "trade_events"):
        return {}
    start = _as_date(as_of_date) - timedelta(days=FORECAST_LOOKBACK_DAYS)
    rows = conn.execute(
        """
        SELECT
            symbol,
            COUNT(*) AS trade_event_count,
            SUM(CASE WHEN COALESCE(side_type, '') IN ('BUY', 'OPEN_LONG', 'SPOT_BUY', 'LONG') THEN COALESCE(qty, 0) ELSE 0 END) AS buy_qty,
            SUM(CASE WHEN COALESCE(side_type, '') IN ('SELL', 'OPEN_SHORT', 'MARGIN_OPEN_SHORT', 'SHORT') THEN COALESCE(qty, 0) ELSE 0 END) AS sell_qty,
            SUM(CASE WHEN COALESCE(side_type, '') IN ('BUY', 'OPEN_LONG', 'SPOT_BUY', 'LONG') THEN COALESCE(qty, 0) * COALESCE(price, 0) ELSE 0 END) AS buy_notional,
            SUM(CASE WHEN COALESCE(side_type, '') IN ('SELL', 'OPEN_SHORT', 'MARGIN_OPEN_SHORT', 'SHORT') THEN COALESCE(qty, 0) * COALESCE(price, 0) ELSE 0 END) AS sell_notional,
            MAX(exec_dt) AS last_exec_dt
        FROM trade_events
        WHERE CAST(exec_dt AS DATE) BETWEEN ? AND ?
        GROUP BY symbol
        """,
        [start, _as_date(as_of_date)],
    ).fetchall()
    payload: dict[str, dict[str, Any]] = {}
    for row in rows:
        code = str(row[0])
        buy_qty = _safe_float(row[2], 0.0)
        sell_qty = _safe_float(row[3], 0.0)
        trade_count = int(row[1] or 0)
        bias = 0.0
        if buy_qty + sell_qty > 0:
            bias = (buy_qty - sell_qty) / (buy_qty + sell_qty)
        payload[code] = {
            "trade_event_count": trade_count,
            "buy_qty": buy_qty,
            "sell_qty": sell_qty,
            "buy_notional": _safe_float(row[4], 0.0),
            "sell_notional": _safe_float(row[5], 0.0),
            "last_exec_dt": row[6],
            "trade_bias": bias,
        }
    return payload


def _load_borrow_context(conn, *, as_of_date: int) -> dict[str, dict[str, Any]]:
    if not source_table_exists(conn, "taisyaku_balance_daily"):
        return {}
    rows = conn.execute(
        """
        WITH ranked AS (
            SELECT
                code,
                application_date,
                settlement_date,
                net_balance_shares,
                loan_ratio,
                fetched_at,
                ROW_NUMBER() OVER (PARTITION BY code ORDER BY application_date DESC, fetched_at DESC) AS rn
            FROM taisyaku_balance_daily
            WHERE application_date <= ?
        )
        SELECT code, application_date, settlement_date, net_balance_shares, loan_ratio, fetched_at
        FROM ranked
        WHERE rn = 1
        """,
        [as_of_date],
    ).fetchall()
    payload: dict[str, dict[str, Any]] = {}
    for row in rows:
        payload[str(row[0])] = {
            "application_date": int(row[1]) if row[1] is not None else None,
            "settlement_date": int(row[2]) if row[2] is not None else None,
            "net_balance_shares": _safe_float(row[3], 0.0),
            "loan_ratio": _safe_float(row[4], 0.0),
            "fetched_at": row[5],
        }
    return payload


def _load_event_context(conn, *, as_of_date: int) -> dict[str, dict[str, Any]]:
    payload: dict[str, dict[str, Any]] = {}
    as_of = _as_date(as_of_date)
    future_end = as_of + timedelta(days=FORECAST_LOOKAHEAD_DAYS)
    if source_table_exists(conn, "earnings_planned"):
        rows = conn.execute(
            """
            SELECT code, MIN(planned_date) AS next_planned_date, COUNT(*) AS planned_count
            FROM earnings_planned
            WHERE planned_date BETWEEN ? AND ?
            GROUP BY code
            """,
            [as_of, future_end],
        ).fetchall()
        for row in rows:
            code = str(row[0])
            next_planned = row[1]
            payload.setdefault(code, {})["earnings_planned"] = {
                "next_planned_date": next_planned,
                "planned_count": int(row[2] or 0),
            }
    if source_table_exists(conn, "ex_rights"):
        rows = conn.execute(
            """
            SELECT code, MIN(ex_date) AS next_ex_date, COUNT(*) AS ex_count
            FROM ex_rights
            WHERE ex_date BETWEEN ? AND ?
            GROUP BY code
            """,
            [as_of, future_end],
        ).fetchall()
        for row in rows:
            code = str(row[0])
            payload.setdefault(code, {})["ex_rights"] = {
                "next_ex_date": row[1],
                "ex_count": int(row[2] or 0),
            }
    return payload


def _load_edinet_context(conn) -> dict[str, dict[str, Any]]:
    if not source_table_exists(conn, "edinetdb_company_map"):
        return {}
    mappings = conn.execute(
        """
        SELECT sec_code, edinet_code, industry
        FROM edinetdb_company_map
        WHERE sec_code IS NOT NULL AND edinet_code IS NOT NULL
        """
    ).fetchall()
    sec_to_edinet = {str(row[0]): str(row[1]) for row in mappings}
    if not sec_to_edinet:
        return {}

    def _count_by_edinet(table_name: str) -> dict[str, dict[str, Any]]:
        if not source_table_exists(conn, table_name):
            return {}
        rows = conn.execute(
            f"""
            SELECT edinet_code, COUNT(*) AS row_count, MAX(fetched_at) AS last_fetched_at
            FROM {table_name}
            GROUP BY edinet_code
            """
        ).fetchall()
        return {
            str(row[0]): {
                "row_count": int(row[1] or 0),
                "last_fetched_at": row[2],
            }
            for row in rows
            if row and row[0] is not None
        }

    ratio_counts = _count_by_edinet("edinetdb_ratios")
    financial_counts = _count_by_edinet("edinetdb_financials")
    document_counts = _count_by_edinet("edinetdb_official_documents")
    text_counts = _count_by_edinet("edinetdb_text_blocks")

    payload: dict[str, dict[str, Any]] = {}
    for code, edinet_code in sec_to_edinet.items():
        ratio = ratio_counts.get(edinet_code, {})
        financial = financial_counts.get(edinet_code, {})
        document = document_counts.get(edinet_code, {})
        text = text_counts.get(edinet_code, {})
        payload[code] = {
            "edinet_code": edinet_code,
            "ratio_row_count": int(ratio.get("row_count") or 0),
            "ratio_last_fetched_at": ratio.get("last_fetched_at"),
            "financial_row_count": int(financial.get("row_count") or 0),
            "financial_last_fetched_at": financial.get("last_fetched_at"),
            "document_row_count": int(document.get("row_count") or 0),
            "document_last_fetched_at": document.get("last_fetched_at"),
            "text_block_row_count": int(text.get("row_count") or 0),
            "text_block_last_fetched_at": text.get("last_fetched_at"),
        }
    return payload


def _load_market_context(conn, *, as_of_date: int) -> dict[str, Any]:
    if not source_table_exists(conn, "market_regime_daily"):
        return {}
    row = conn.execute(
        """
        SELECT
            dt,
            regime_id,
            breadth_above_ma20,
            breadth_above_ma60,
            advancers_ratio,
            index_close_vs_ma20,
            index_close_vs_ma60,
            market_atr_pct,
            sector_dispersion,
            regime_score,
            label_version,
            created_at
        FROM market_regime_daily
        WHERE dt <= ?
        ORDER BY dt DESC
        LIMIT 1
        """,
        [as_of_date],
    ).fetchone()
    if not row:
        return {}
    return {
        "dt": int(row[0]) if row[0] is not None else None,
        "regime_id": row[1],
        "breadth_above_ma20": _safe_float(row[2], 0.0),
        "breadth_above_ma60": _safe_float(row[3], 0.0),
        "advancers_ratio": _safe_float(row[4], 0.0),
        "index_close_vs_ma20": _safe_float(row[5], 0.0),
        "index_close_vs_ma60": _safe_float(row[6], 0.0),
        "market_atr_pct": _safe_float(row[7], 0.0),
        "sector_dispersion": _safe_float(row[8], 0.0),
        "regime_score": _safe_float(row[9], 0.0),
        "label_version": row[10],
        "created_at": row[11],
    }


def _load_source_context(*, source_db_path: str | None, as_of_date: int) -> _SourceContext:
    if not source_db_path:
        return _SourceContext(signal={}, trade={}, borrow={}, events={}, edinet={}, market={}, presence={})
    conn = connect_source_db(source_db_path)
    try:
        presence = {
            "signal_decision_daily": source_table_exists(conn, "signal_decision_daily"),
            "trade_events": source_table_exists(conn, "trade_events"),
            "taisyaku_balance_daily": source_table_exists(conn, "taisyaku_balance_daily"),
            "earnings_planned": source_table_exists(conn, "earnings_planned"),
            "ex_rights": source_table_exists(conn, "ex_rights"),
            "tdnet_disclosures": source_table_exists(conn, "tdnet_disclosures"),
            "edinetdb_company_map": source_table_exists(conn, "edinetdb_company_map"),
            "edinetdb_ratios": source_table_exists(conn, "edinetdb_ratios"),
            "edinetdb_financials": source_table_exists(conn, "edinetdb_financials"),
            "edinetdb_official_documents": source_table_exists(conn, "edinetdb_official_documents"),
            "edinetdb_text_blocks": source_table_exists(conn, "edinetdb_text_blocks"),
            "market_regime_daily": source_table_exists(conn, "market_regime_daily"),
        }
        return _SourceContext(
            signal=_load_signal_context(conn, as_of_date=as_of_date),
            trade=_load_trade_context(conn, as_of_date=as_of_date),
            borrow=_load_borrow_context(conn, as_of_date=as_of_date),
            events=_load_event_context(conn, as_of_date=as_of_date),
            edinet=_load_edinet_context(conn),
            market=_load_market_context(conn, as_of_date=as_of_date),
            presence=presence,
        )
    finally:
        conn.close()


def _legacy_signal_boost(side: str, source_context: _SourceContext, code: str) -> tuple[float, list[str], list[str]]:
    signal_side = "buy" if side == "long" else "sell"
    context = source_context.signal.get((code, signal_side))
    if not context:
        return 0.0, [], []
    boost = 0.0
    tags: list[str] = []
    reasons: list[str] = []
    if bool(context.get("entry_qualified")):
        boost += 0.08
        tags.append(f"legacy_signal_{signal_side}_qualified")
        reasons.append(f"signal_{signal_side}_qualified")
    setup_type = str(context.get("setup_type") or "").strip()
    if setup_type:
        tags.append(f"legacy_{setup_type}")
        reasons.append(f"signal_setup_{setup_type}")
    forward_return_20 = context.get("forward_return_20")
    if forward_return_20 is not None:
        forward_return_20 = float(forward_return_20)
        if side == "long" and forward_return_20 > 0:
            boost += min(0.05, forward_return_20 * 0.5)
        if side == "short" and forward_return_20 < 0:
            boost += min(0.05, abs(forward_return_20) * 0.5)
    max_favorable_30 = context.get("max_favorable_30")
    max_adverse_30 = context.get("max_adverse_30")
    if max_favorable_30 is not None and max_adverse_30 is not None:
        favorable = abs(float(max_favorable_30))
        adverse = abs(float(max_adverse_30))
        if favorable > adverse:
            boost += 0.03
            reasons.append("signal_favorable_wider_than_adverse")
    return boost, tags, reasons


def _trade_event_signal(side: str, trade_context: dict[str, Any] | None) -> tuple[float, list[str], list[str], float]:
    if not trade_context:
        return 0.0, [], [], 0.0
    buy_qty = _safe_float(trade_context.get("buy_qty"), 0.0)
    sell_qty = _safe_float(trade_context.get("sell_qty"), 0.0)
    trade_bias = _safe_float(trade_context.get("trade_bias"), 0.0)
    boost = 0.0
    tags: list[str] = []
    reasons: list[str] = []
    if side == "long" and trade_bias > 0:
        boost += min(0.05, trade_bias * 0.08)
        tags.append("trade_bias_long")
        reasons.append("trade_flow_supports_long")
    if side == "short" and trade_bias < 0:
        boost += min(0.05, abs(trade_bias) * 0.08)
        tags.append("trade_bias_short")
        reasons.append("trade_flow_supports_short")
    if buy_qty + sell_qty > 0:
        tags.append("trade_events_recent")
    return boost, tags, reasons, abs(trade_bias)


def _event_risk_signal(side: str, event_context: dict[str, Any] | None) -> tuple[float, list[str], list[str], float]:
    if not event_context:
        return 0.0, [], [], 0.0
    boost = 0.0
    penalty = 0.0
    tags: list[str] = []
    reasons: list[str] = []
    for event_name, weight in (("earnings_planned", 0.04), ("ex_rights", 0.05)):
        event = event_context.get(event_name)
        if not event:
            continue
        next_date = event.get("next_planned_date") if event_name == "earnings_planned" else event.get("next_ex_date")
        if next_date is not None:
            tags.append(f"{event_name}_near")
            reasons.append(f"{event_name}_nearby")
            boost -= weight
            penalty += weight
    return boost, tags, reasons, penalty


def _borrow_signal(side: str, borrow_context: dict[str, Any] | None) -> tuple[float, list[str], list[str], float]:
    if not borrow_context:
        return 0.0, [], [], 0.0
    loan_ratio = _safe_float(borrow_context.get("loan_ratio"), 0.0)
    net_balance = _safe_float(borrow_context.get("net_balance_shares"), 0.0)
    boost = 0.0
    penalty = 0.0
    tags: list[str] = []
    reasons: list[str] = []
    if side == "short" and (loan_ratio >= 1.0 or net_balance > 0):
        boost += min(0.06, 0.02 + (loan_ratio - 1.0) * 0.03 + min(abs(net_balance) / 1_000_000.0, 0.02))
        tags.append("borrow_pressure_short")
        reasons.append("borrow_pressure_supports_short")
    elif side == "long" and (loan_ratio >= 1.0 or net_balance > 0):
        penalty += min(0.05, 0.02 + (loan_ratio - 1.0) * 0.02 + min(abs(net_balance) / 1_000_000.0, 0.02))
        tags.append("borrow_pressure_long")
        reasons.append("borrow_pressure_works_against_long")
    return boost - penalty, tags, reasons, penalty


def _edinet_signal(side: str, edinet_context: dict[str, Any] | None) -> tuple[float, list[str], list[str], float]:
    if not edinet_context:
        return 0.0, [], [], 0.0
    ratio_count = int(edinet_context.get("ratio_row_count") or 0)
    financial_count = int(edinet_context.get("financial_row_count") or 0)
    document_count = int(edinet_context.get("document_row_count") or 0)
    text_count = int(edinet_context.get("text_block_row_count") or 0)
    age_bonus = 0.0
    for key in ("ratio_last_fetched_at", "financial_last_fetched_at", "document_last_fetched_at", "text_block_last_fetched_at"):
        value = edinet_context.get(key)
        if value is not None:
            age_bonus += 0.01
    signal_strength = min(0.12, (ratio_count * 0.01) + (financial_count * 0.005) + (document_count * 0.004) + (text_count * 0.004) + age_bonus)
    tags = ["edinet_context"]
    reasons = ["edinet_context_present"]
    if side == "long":
        return signal_strength * 0.75, tags, reasons, 0.0
    return signal_strength * 0.45, tags, reasons, 0.0


def _market_signal(side: str, market_context: dict[str, Any] | None) -> tuple[float, list[str], list[str], float]:
    if not market_context:
        return 0.0, [], [], 0.0
    regime_score = _safe_float(market_context.get("regime_score"), 0.0)
    breadth = _safe_float(market_context.get("breadth_above_ma20"), 0.0) + _safe_float(market_context.get("breadth_above_ma60"), 0.0)
    market_atr_pct = _safe_float(market_context.get("market_atr_pct"), 0.0)
    boost = 0.0
    tags: list[str] = []
    reasons: list[str] = []
    if side == "long":
        boost += max(-0.05, min(0.05, regime_score * 0.04 + breadth * 0.02))
        tags.append("market_regime_long")
        if regime_score >= 0:
            reasons.append("market_regime_supports_long")
        else:
            reasons.append("market_regime_is_cautious")
    else:
        boost += max(-0.05, min(0.05, (-regime_score) * 0.04 + (1.0 - breadth) * 0.02))
        tags.append("market_regime_short")
        if regime_score <= 0:
            reasons.append("market_regime_supports_short")
        else:
            reasons.append("market_regime_is_cautious")
    penalty = min(0.05, market_atr_pct * 0.25)
    return boost - penalty, tags, reasons, penalty


def _build_surface_row(
    row: dict[str, Any],
    *,
    side: str,
    source_context: _SourceContext,
    market_context: dict[str, Any],
    publish_id: str,
    freshness_state: str,
    learned_prediction: dict[str, Any] | None = None,
) -> dict[str, Any]:
    side_sign = 1.0 if side == "long" else -1.0
    side_score = _safe_float(row["ranking_score_long"] if side == "long" else row["ranking_score_short"], 0.0)
    score_norm = math.tanh(side_score / 100.0)
    close_price = _safe_float(row["close_price"], 0.0)
    close_vs_ma20 = _safe_float(row["close_vs_ma20"], 0.0)
    ret_20_past = _safe_float(row["ret_20_past"], 0.0)
    atr_ratio = _safe_float(row["atr_ratio"], 0.0)
    volume_ratio = _safe_float(row["volume_ratio"], 1.0)
    side_close_vs_ma20 = side_sign * close_vs_ma20
    side_ret_20_past = side_sign * ret_20_past
    base_probability = 0.5 + (score_norm * 0.24) + (side_close_vs_ma20 * 0.9) + (side_ret_20_past * 0.18)
    source_boost = 0.0
    source_tags: list[str] = []
    reason_codes: list[str] = []
    context_notes: list[str] = []

    legacy_boost, legacy_tags, legacy_reasons = _legacy_signal_boost(side, source_context, str(row["code"]))
    source_boost += legacy_boost
    source_tags.extend(legacy_tags)
    reason_codes.extend(legacy_reasons)

    trade_boost, trade_tags, trade_reasons, trade_abs_bias = _trade_event_signal(side, source_context.trade.get(str(row["code"])))
    source_boost += trade_boost
    source_tags.extend(trade_tags)
    reason_codes.extend(trade_reasons)

    event_boost, event_tags, event_reasons, event_penalty = _event_risk_signal(side, source_context.events.get(str(row["code"])))
    source_boost += event_boost
    source_tags.extend(event_tags)
    reason_codes.extend(event_reasons)

    borrow_boost, borrow_tags, borrow_reasons, borrow_penalty = _borrow_signal(side, source_context.borrow.get(str(row["code"])))
    source_boost += borrow_boost
    source_tags.extend(borrow_tags)
    reason_codes.extend(borrow_reasons)

    edinet_boost, edinet_tags, edinet_reasons, _ = _edinet_signal(side, source_context.edinet.get(str(row["code"])))
    source_boost += edinet_boost
    source_tags.extend(edinet_tags)
    reason_codes.extend(edinet_reasons)

    market_boost, market_tags, market_reasons, market_penalty = _market_signal(side, market_context)
    source_boost += market_boost
    source_tags.extend(market_tags)
    reason_codes.extend(market_reasons)

    probability = _clamp01(base_probability + source_boost - (atr_ratio * 0.45) - (volume_ratio - 1.0) * 0.03)

    magnitude = max(
        0.006,
        abs(close_vs_ma20) * 0.45 + abs(ret_20_past) * 0.22 + atr_ratio * 1.15 + abs(trade_abs_bias) * 0.015,
    )
    if source_boost > 0:
        magnitude += min(0.02, source_boost * 0.12)

    learned_probability = None
    learned_expected_ret_5 = None
    learned_expected_ret_10 = None
    learned_expected_ret_20 = None
    learned_expected_mfe_20 = None
    learned_expected_mae_20 = None
    if learned_prediction:
        learned_probability = learned_prediction.get("direction_prob")
        learned_expected_ret_5 = learned_prediction.get("expected_ret_5")
        learned_expected_ret_10 = learned_prediction.get("expected_ret_10")
        learned_expected_ret_20 = learned_prediction.get("expected_ret_20")
        learned_expected_mfe_20 = learned_prediction.get("expected_mfe_20")
        learned_expected_mae_20 = learned_prediction.get("expected_mae_20")
        if learned_probability is not None:
            probability = _clamp01((probability * 0.35) + (float(learned_probability) * 0.65))
        heuristic_expected_ret_5 = side_sign * magnitude * 0.35
        heuristic_expected_ret_10 = side_sign * magnitude * 0.62
        heuristic_expected_ret_20 = side_sign * magnitude
        heuristic_expected_mfe_20 = side_sign * (magnitude * 1.22 + max(0.0, source_boost) * 0.08)
        heuristic_expected_mae_20 = -side_sign * (magnitude * 0.82 + atr_ratio * 0.35 + event_penalty + borrow_penalty + market_penalty)
        if learned_expected_ret_20 is not None:
            signed_learned_ret_20 = side_sign * float(learned_expected_ret_20)
            expected_ret_20 = (heuristic_expected_ret_20 * 0.35) + (signed_learned_ret_20 * 0.65)
        else:
            expected_ret_20 = heuristic_expected_ret_20
        if learned_expected_ret_5 is not None:
            expected_ret_5 = (heuristic_expected_ret_5 * 0.35) + (side_sign * float(learned_expected_ret_5) * 0.65)
        else:
            expected_ret_5 = heuristic_expected_ret_5
        if learned_expected_ret_10 is not None:
            expected_ret_10 = (heuristic_expected_ret_10 * 0.35) + (side_sign * float(learned_expected_ret_10) * 0.65)
        else:
            expected_ret_10 = None
        if learned_expected_mfe_20 is not None:
            expected_mfe_20 = (heuristic_expected_mfe_20 * 0.35) + (side_sign * float(learned_expected_mfe_20) * 0.65)
        else:
            expected_mfe_20 = heuristic_expected_mfe_20
        if learned_expected_mae_20 is not None:
            expected_mae_20 = (heuristic_expected_mae_20 * 0.35) + (-side_sign * float(learned_expected_mae_20) * 0.65)
        else:
            expected_mae_20 = heuristic_expected_mae_20
    else:
        expected_ret_5 = side_sign * magnitude * 0.35
        expected_ret_10 = side_sign * magnitude * 0.62
        expected_ret_20 = side_sign * magnitude
        expected_mfe_20 = side_sign * (magnitude * 1.22 + max(0.0, source_boost) * 0.08)
        expected_mae_20 = -side_sign * (magnitude * 0.82 + atr_ratio * 0.35 + event_penalty + borrow_penalty + market_penalty)

    invalidation_price = close_price * (1.0 + expected_mae_20) if close_price > 0 else None

    executionability_penalty = _clamp01(0.12 + atr_ratio * 3.0 + event_penalty + borrow_penalty + market_penalty)
    realization_confidence = _clamp01(0.28 + (probability * 0.48) + (1.0 - executionability_penalty) * 0.24)
    expected_directional_move = expected_ret_20 if side == "long" else -expected_ret_20
    market_opportunity_score = max(0.0, expected_directional_move * 100.0 * probability * (1.0 - executionability_penalty))
    personal_fit_score = _clamp01(0.5 + (trade_boost * 4.0) - (event_penalty * 2.0))
    opportunity_score = max(0.0, market_opportunity_score * realization_confidence * (0.75 + 0.5 * personal_fit_score))

    if probability >= 0.64 and opportunity_score >= 0.6:
        action_state = "enter"
    elif probability >= 0.52 or opportunity_score >= 0.35:
        action_state = "wait"
    else:
        action_state = "skip"

    if side == "long":
        if "market_regime_supports_long" in reason_codes:
            context_notes.append("market_support_long")
        if "legacy_signal_buy_qualified" in source_tags:
            context_notes.append("legacy_signal_buy")
    else:
        if "market_regime_supports_short" in reason_codes:
            context_notes.append("market_support_short")
        if "legacy_signal_sell_qualified" in source_tags:
            context_notes.append("legacy_signal_sell")

    setup_tags = sorted(
        {
            tag
            for tag in _nonempty_tokens([row["box_state"], row["ppp_state"], row["abc_state"]]) + source_tags + context_notes
            if str(tag).strip() and str(tag).strip().lower() != "none"
        }
    )
    primary_reason_codes = sorted(
        set(
            [
                "FORECAST_SURFACE",
                "SIDE_LONG" if side == "long" else "SIDE_SHORT",
                "MA20_ABOVE" if float(row["close_vs_ma20"]) >= 0 else "MA20_BELOW",
                "RET20_POS" if float(row["ret_20_past"]) >= 0 else "RET20_NEG",
            ]
            + reason_codes
        )
    )

    return {
        "publish_id": publish_id,
        "as_of_date": _as_date(_normalize_as_of_date(row["as_of_date"])),
        "code": str(row["code"]),
        "side": side,
        "action_state": action_state,
        "direction_prob": probability,
        "expected_ret_5": expected_ret_5,
        "expected_ret_10": expected_ret_10,
        "expected_ret_20": expected_ret_20,
        "expected_mfe_20": expected_mfe_20,
        "expected_mae_20": expected_mae_20,
        "invalidation_price": invalidation_price,
        "setup_tags": _json_dump(setup_tags),
        "reason_codes": _build_reason_codes(*primary_reason_codes),
        "market_opportunity_score": market_opportunity_score,
        "personal_fit_score": personal_fit_score,
        "opportunity_score": opportunity_score,
        "freshness_state": freshness_state,
        "created_at": _utcnow(),
    }


def build_forecast_surface_rows(
    *,
    export_db_path: str | None = None,
    source_db_path: str | None = None,
    label_db_path: str | None = None,
    as_of_date: str | int | None,
    publish_id: str | None = None,
    freshness_state: str = "fresh",
    codes: list[str] | None = None,
) -> dict[str, Any]:
    if as_of_date is None:
        raise ValueError("as_of_date is required")
    as_of_date_int = _normalize_as_of_date(as_of_date)
    actual_publish_id = publish_id or f"forecast_{_as_of_date_text(as_of_date_int)}"
    frame = load_candidate_input_frame(export_db_path=export_db_path, as_of_date=as_of_date_int, codes=codes)
    scored, regime = _score_frame(frame)
    source_context = _load_source_context(source_db_path=source_db_path, as_of_date=as_of_date_int)
    market_context = dict(source_context.market or {})
    learned_predictions: dict[str, dict[str, Any]] = {}
    learning_meta: dict[str, Any] = {}
    learning_alerts: list[str] = []
    if source_db_path and label_db_path:
        source_conn = connect_source_db(source_db_path)
        try:
            if source_table_exists(source_conn, "ml_feature_daily") or source_table_exists(source_conn, "feature_frame_daily"):
                try:
                    bundle = load_or_train_forecast_surface_bundle(
                        source_db_path=source_db_path,
                        label_db_path=label_db_path,
                        as_of_date=as_of_date_int,
                    )
                    if bundle:
                        learning_meta = dict(bundle.get("meta") or {})
                        learned_predictions = predict_current_surface(
                            bundle=bundle,
                            source_db_path=source_db_path,
                            as_of_date=as_of_date_int,
                        )
                    else:
                        learning_alerts.append("learning_bundle_unavailable")
                except Exception:
                    learned_predictions = {}
                    learning_alerts.append("learning_predictions_unavailable")
        finally:
            source_conn.close()
    rows: list[dict[str, Any]] = []
    side_counts = {"long": 0, "short": 0}
    action_counts = {"enter": 0, "wait": 0, "skip": 0}
    universe_code_count = len({str(row["code"]) for row in scored})
    for row in scored:
        for side in ("long", "short"):
            learned_prediction = (learned_predictions.get(str(row["code"])) or {}).get(side)
            surface_row = _build_surface_row(
                row,
                side=side,
                source_context=source_context,
                market_context=market_context,
                publish_id=actual_publish_id,
                freshness_state=freshness_state,
                learned_prediction=learned_prediction,
            )
            rows.append(surface_row)
            side_counts[side] += 1
            action_counts[str(surface_row["action_state"])] += 1
    expected_row_count = int(universe_code_count * 2)
    actual_row_count = int(len(rows))
    missing_row_count = max(int(expected_row_count - actual_row_count), 0)
    coverage_ratio = float(actual_row_count / max(expected_row_count, 1))
    alerts: list[str] = []
    if coverage_ratio < 1.0:
        alerts.append("coverage_incomplete")
    if int(side_counts["long"]) < universe_code_count:
        alerts.append("long_side_incomplete")
    if int(side_counts["short"]) < universe_code_count:
        alerts.append("short_side_incomplete")
    for source_name, present in sorted(source_context.presence.items()):
        if not bool(present):
            alerts.append(f"source_absent:{source_name}")
    for alert in learning_alerts:
        if alert not in alerts:
            alerts.append(alert)
    return {
        "ok": True,
        "publish_id": actual_publish_id,
        "as_of_date": _as_of_date_text(as_of_date_int),
        "source_context_presence": source_context.presence,
        "learning_meta": learning_meta,
        "regime": regime,
        "rows": rows,
        "row_count": len(rows),
        "universe_code_count": universe_code_count,
        "expected_row_count": expected_row_count,
        "actual_row_count": actual_row_count,
        "missing_row_count": missing_row_count,
        "coverage_ratio": coverage_ratio,
        "alerts": alerts,
        "side_counts": side_counts,
        "action_counts": action_counts,
    }


def persist_forecast_surface_daily(
    *,
    result_db_path: str | None = None,
    export_db_path: str | None = None,
    source_db_path: str | None = None,
    label_db_path: str | None = None,
    as_of_date: str | int | None,
    publish_id: str | None = None,
    freshness_state: str = "fresh",
    codes: list[str] | None = None,
) -> dict[str, Any]:
    payload = build_forecast_surface_rows(
        export_db_path=export_db_path,
        source_db_path=source_db_path,
        label_db_path=label_db_path,
        as_of_date=as_of_date,
        publish_id=publish_id,
        freshness_state=freshness_state,
        codes=codes,
    )
    conn = connect_result_db(result_db_path, read_only=False)
    try:
        ensure_result_schema(conn)
        conn.execute("BEGIN TRANSACTION")
        try:
            conn.execute("DELETE FROM forecast_surface_daily WHERE publish_id = ?", [payload["publish_id"]])
            conn.execute("DELETE FROM forecast_surface_runs WHERE publish_id = ?", [payload["publish_id"]])
            rows = list(payload["rows"])
            if rows:
                columns = list(rows[0].keys())
                conn.executemany(
                    f"INSERT INTO forecast_surface_daily ({', '.join(columns)}) VALUES ({', '.join(['?'] * len(columns))})",
                    [[row[column] for column in columns] for row in rows],
                )
            conn.execute(
                """
                INSERT INTO forecast_surface_runs (
                    publish_id,
                    as_of_date,
                    model_version,
                    universe_code_count,
                    expected_row_count,
                    actual_row_count,
                    missing_row_count,
                    coverage_ratio,
                    feature_frame_version,
                    market_opportunity_score_enabled,
                    personal_fit_score_enabled,
                    side_counts_json,
                    action_counts_json,
                    source_context_presence_json,
                    alerts_json,
                    created_at
                ) VALUES (?, CAST(? AS DATE), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    payload["publish_id"],
                    payload["as_of_date"],
                    str((payload.get("learning_meta") or {}).get("model_version") or FORECAST_SURFACE_VERSION),
                    int(payload["universe_code_count"]),
                    int(payload["expected_row_count"]),
                    int(payload["actual_row_count"]),
                    int(payload["missing_row_count"]),
                    float(payload["coverage_ratio"]),
                    None if (payload.get("learning_meta") or {}).get("feature_frame_version") is None else str((payload.get("learning_meta") or {}).get("feature_frame_version")),
                    True,
                    True,
                    _json_dump(payload["side_counts"]),
                    _json_dump(payload["action_counts"]),
                    _json_dump(payload["source_context_presence"]),
                    _json_dump(payload["alerts"]),
                    _utcnow(),
                ],
            )
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            raise
        conn.execute("CHECKPOINT")
    finally:
        conn.close()
    return {
        "saved": True,
        "publish_id": payload["publish_id"],
        "as_of_date": payload["as_of_date"],
        "row_count": payload["row_count"],
        "universe_code_count": payload["universe_code_count"],
        "expected_row_count": payload["expected_row_count"],
        "actual_row_count": payload["actual_row_count"],
        "missing_row_count": payload["missing_row_count"],
        "coverage_ratio": payload["coverage_ratio"],
        "alerts": payload["alerts"],
        "side_counts": payload["side_counts"],
        "action_counts": payload["action_counts"],
        "source_context_presence": payload["source_context_presence"],
        "feature_frame_version": (payload.get("learning_meta") or {}).get("feature_frame_version"),
        "market_opportunity_score_enabled": True,
        "personal_fit_score_enabled": True,
    }
