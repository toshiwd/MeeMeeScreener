from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Any

from external_analysis.exporter.export_schema import connect_export_db
from external_analysis.labels.store import connect_label_db
from external_analysis.models.state_eval_baseline import build_state_eval_rows, persist_state_eval_shadow
from external_analysis.results.publish_candidates import build_publish_candidate_bundle
from external_analysis.results.publish import publish_result
from external_analysis.results.result_schema import connect_result_db, ensure_result_schema

MODEL_KEY = "candidate_baseline_v1"
BASELINE_VERSION = "score_formula_v1"
FEATURE_VERSION = "slice-d-v1"
EXPECTED_HORIZON_DAYS = 20
MAX_CANDIDATES_PER_SIDE = 20
METRICS_MAX_ATTEMPTS = 3
logger = logging.getLogger(__name__)


def _utcnow() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def _normalize_as_of_date(value: str | int) -> int:
    text = str(value).strip().replace("-", "")
    if len(text) != 8 or not text.isdigit():
        raise ValueError(f"unsupported as_of_date: {value}")
    return int(text)


def _as_of_date_text(value: int) -> str:
    text = str(int(value))
    return f"{text[0:4]}-{text[4:6]}-{text[6:8]}"


def _default_publish_id(as_of_date: int) -> str:
    return f"pub_{_as_of_date_text(as_of_date)}"


def _safe_float(value: Any, default: float = 0.0) -> float:
    if value is None:
        return float(default)
    return float(value)


def _build_reason_codes(*codes: str) -> str:
    return json.dumps([code for code in codes if code], ensure_ascii=False)


def load_candidate_input_frame(
    export_db_path: str | None = None,
    as_of_date: str | int | None = None,
    *,
    codes: list[str] | None = None,
) -> list[dict[str, Any]]:
    if as_of_date is None:
        raise ValueError("as_of_date is required")
    as_of_date_int = _normalize_as_of_date(as_of_date)
    conn = connect_export_db(export_db_path)
    try:
        code_filter_sql = ""
        params: list[Any] = [as_of_date_int]
        if codes:
            code_filter_sql = f" AND code IN ({', '.join(['?'] * len(codes))})"
            params.extend([str(code) for code in codes])
        rows = conn.execute(
            f"""
            WITH enriched AS (
                SELECT
                    b.code,
                    b.trade_date AS as_of_date,
                    CAST(b.o AS DOUBLE) AS open_price,
                    CAST(b.h AS DOUBLE) AS high_price,
                    CAST(b.l AS DOUBLE) AS low_price,
                    CAST(b.c AS DOUBLE) AS close_price,
                    CAST(b.v AS DOUBLE) AS volume_value,
                    CAST(i.ma20 AS DOUBLE) AS ma20,
                    CAST(i.atr14 AS DOUBLE) AS atr14,
                    CAST(i.diff20_pct AS DOUBLE) AS diff20_pct,
                    CAST(i.diff20_atr AS DOUBLE) AS diff20_atr,
                    CAST(i.cnt_20_above AS DOUBLE) AS cnt_20_above,
                    CAST(i.cnt_7_above AS DOUBLE) AS cnt_7_above,
                    CAST(i.day_count AS DOUBLE) AS day_count,
                    COALESCE(i.candle_flags, '') AS candle_flags,
                    COALESCE(LAG(i.candle_flags, 1) OVER (PARTITION BY b.code ORDER BY b.trade_date), '') AS prev_candle_flags,
                    COALESCE(LAG(i.candle_flags, 2) OVER (PARTITION BY b.code ORDER BY b.trade_date), '') AS prev2_candle_flags,
                    CAST(LAG(b.c, 5) OVER (PARTITION BY b.code ORDER BY b.trade_date) AS DOUBLE) AS close_5d_ago,
                    CAST(LAG(b.c, 20) OVER (PARTITION BY b.code ORDER BY b.trade_date) AS DOUBLE) AS close_20d_ago,
                    CAST(AVG(b.v) OVER (
                        PARTITION BY b.code
                        ORDER BY b.trade_date
                        ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
                    ) AS DOUBLE) AS avg_volume_prev_5,
                    COALESCE(p.box_state, '') AS box_state,
                    COALESCE(p.ppp_state, '') AS ppp_state,
                    COALESCE(p.abc_state, '') AS abc_state
                FROM bars_daily_export b
                LEFT JOIN indicator_daily_export i
                  ON i.code = b.code AND i.trade_date = b.trade_date
                LEFT JOIN pattern_state_export p
                  ON p.code = b.code AND p.trade_date = b.trade_date
            )
            SELECT *
            FROM enriched
            WHERE as_of_date = ?{code_filter_sql}
            ORDER BY code
            """,
            params,
        ).fetchall()
        columns = [
            "code",
            "as_of_date",
            "open_price",
            "high_price",
            "low_price",
            "close_price",
            "volume_value",
            "ma20",
            "atr14",
            "diff20_pct",
            "diff20_atr",
            "cnt_20_above",
            "cnt_7_above",
            "day_count",
            "candle_flags",
            "prev_candle_flags",
            "prev2_candle_flags",
            "close_5d_ago",
            "close_20d_ago",
            "avg_volume_prev_5",
            "box_state",
            "ppp_state",
            "abc_state",
        ]
        return [dict(zip(columns, row, strict=True)) for row in rows]
    finally:
        conn.close()


def _score_frame(frame: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    scored: list[dict[str, Any]] = []
    breadth_inputs: list[float] = []
    momentum_inputs: list[float] = []
    volatility_inputs: list[float] = []
    for row in frame:
        close_price = _safe_float(row["close_price"])
        ma20 = _safe_float(row["ma20"], close_price)
        close_5d_ago = row["close_5d_ago"]
        close_20d_ago = row["close_20d_ago"]
        if close_price <= 0 or ma20 <= 0 or close_5d_ago in (None, 0) or close_20d_ago in (None, 0):
            continue
        ret_5 = (close_price / float(close_5d_ago)) - 1.0
        ret_20 = (close_price / float(close_20d_ago)) - 1.0
        close_vs_ma20 = (close_price / ma20) - 1.0
        avg_volume_prev_5 = _safe_float(row["avg_volume_prev_5"], 0.0)
        volume_ratio = 1.0 if avg_volume_prev_5 <= 0 else (_safe_float(row["volume_value"]) / avg_volume_prev_5)
        atr_ratio = 0.0
        if _safe_float(row["atr14"], 0.0) > 0 and close_price > 0:
            atr_ratio = _safe_float(row["atr14"]) / close_price
        trend_bias = 0.0
        day_count = max(1.0, _safe_float(row["day_count"], 1.0))
        trend_bias += _safe_float(row["cnt_20_above"]) / day_count
        trend_bias += _safe_float(row["cnt_7_above"]) / day_count
        if "break" in str(row["box_state"]).lower():
            trend_bias += 0.5
        if "up" in str(row["ppp_state"]).lower():
            trend_bias += 0.25
        if "up" in str(row["abc_state"]).lower():
            trend_bias += 0.25
        long_score = (ret_20 * 100.0) + (ret_5 * 40.0) + (close_vs_ma20 * 60.0) + ((volume_ratio - 1.0) * 6.0) + trend_bias
        short_score = ((-ret_20) * 100.0) + ((-ret_5) * 40.0) + ((-close_vs_ma20) * 60.0) + ((volume_ratio - 1.0) * 4.0) + max(0.0, 1.0 - trend_bias)
        risk_penalty = max(0.0, atr_ratio * 25.0)
        scored.append(
            {
                **row,
                "ret_5_past": ret_5,
                "ret_20_past": ret_20,
                "close_vs_ma20": close_vs_ma20,
                "volume_ratio": volume_ratio,
                "atr_ratio": atr_ratio,
                "retrieval_score_long": long_score,
                "retrieval_score_short": short_score,
                "ranking_score_long": long_score - risk_penalty,
                "ranking_score_short": short_score - risk_penalty,
                "risk_penalty": risk_penalty,
            }
        )
        breadth_inputs.append(1.0 if close_vs_ma20 > 0 else -1.0)
        momentum_inputs.append(ret_20)
        volatility_inputs.append(atr_ratio)
    if not scored:
        regime = {
            "regime_tag": "unknown",
            "regime_score": 0.0,
            "breadth_score": 0.0,
            "volatility_state": "unknown",
        }
        return [], regime
    breadth_score = sum(breadth_inputs) / len(breadth_inputs)
    momentum_score = sum(momentum_inputs) / len(momentum_inputs)
    volatility_score = sum(volatility_inputs) / len(volatility_inputs)
    regime_score = (breadth_score * 0.6) + (momentum_score * 3.0) - (volatility_score * 0.5)
    if regime_score >= 0.2:
        regime_tag = "risk_on"
    elif regime_score <= -0.2:
        regime_tag = "risk_off"
    else:
        regime_tag = "neutral"
    volatility_state = "high" if volatility_score >= 0.05 else "normal"
    regime = {
        "regime_tag": regime_tag,
        "regime_score": regime_score,
        "breadth_score": breadth_score,
        "volatility_state": volatility_state,
    }
    return scored, regime


def _build_candidate_rows(
    scored: list[dict[str, Any]],
    regime: dict[str, Any],
    publish_id: str,
    as_of_date: int,
    freshness_state: str,
    *,
    candidate_limit_per_side: int = MAX_CANDIDATES_PER_SIDE,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    component_rows: list[dict[str, Any]] = []
    candidate_rows: list[dict[str, Any]] = []
    effective_limit = max(1, int(candidate_limit_per_side))
    long_ranked = sorted(scored, key=lambda item: (float(item["ranking_score_long"]), str(item["code"])), reverse=True)[:effective_limit]
    short_ranked = sorted(scored, key=lambda item: (float(item["ranking_score_short"]), str(item["code"])), reverse=True)[:effective_limit]
    for rank_position, row in enumerate(long_ranked, start=1):
        component_rows.append(
            {
                "publish_id": publish_id,
                "as_of_date": _as_of_date_text(as_of_date),
                "code": row["code"],
                "side": "long",
                "retrieval_score": float(row["retrieval_score_long"]),
                "ranking_score": float(row["ranking_score_long"]),
                "risk_penalty": float(row["risk_penalty"]),
                "regime_adjustment": float(regime["regime_score"]),
                "reason_codes": _build_reason_codes("RET20_POS", "MA20_ABOVE", "LONG_BASELINE"),
            }
        )
        candidate_rows.append(
            {
                "publish_id": publish_id,
                "as_of_date": _as_of_date_text(as_of_date),
                "code": row["code"],
                "side": "long",
                "rank_position": rank_position,
                "candidate_score": float(row["ranking_score_long"]),
                "expected_horizon_days": EXPECTED_HORIZON_DAYS,
                "primary_reason_codes": _build_reason_codes("RET20_POS", "MA20_ABOVE", "LONG_BASELINE"),
                "regime_tag": regime["regime_tag"],
                "freshness_state": freshness_state,
            }
        )
    for rank_position, row in enumerate(short_ranked, start=1):
        component_rows.append(
            {
                "publish_id": publish_id,
                "as_of_date": _as_of_date_text(as_of_date),
                "code": row["code"],
                "side": "short",
                "retrieval_score": float(row["retrieval_score_short"]),
                "ranking_score": float(row["ranking_score_short"]),
                "risk_penalty": float(row["risk_penalty"]),
                "regime_adjustment": float(-regime["regime_score"]),
                "reason_codes": _build_reason_codes("RET20_NEG", "MA20_BELOW", "SHORT_BASELINE"),
            }
        )
        candidate_rows.append(
            {
                "publish_id": publish_id,
                "as_of_date": _as_of_date_text(as_of_date),
                "code": row["code"],
                "side": "short",
                "rank_position": rank_position,
                "candidate_score": float(row["ranking_score_short"]),
                "expected_horizon_days": EXPECTED_HORIZON_DAYS,
                "primary_reason_codes": _build_reason_codes("RET20_NEG", "MA20_BELOW", "SHORT_BASELINE"),
                "regime_tag": regime["regime_tag"],
                "freshness_state": freshness_state,
            }
        )
    return component_rows, candidate_rows


def _evaluate_nightly_metrics(
    *,
    label_db_path: str | None,
    as_of_date: int,
    publish_id: str,
    regime: dict[str, Any],
    candidate_rows: list[dict[str, Any]],
) -> dict[str, Any]:
    long_codes = [row["code"] for row in candidate_rows if row["side"] == "long"]
    short_codes = [row["code"] for row in candidate_rows if row["side"] == "short"]
    metrics = {
        "run_id": _utcnow().strftime("candidate_metrics_%Y%m%dT%H%M%S%fZ"),
        "publish_id": publish_id,
        "as_of_date": _as_of_date_text(as_of_date),
        "model_key": MODEL_KEY,
        "baseline_version": BASELINE_VERSION,
        "label_policy_version": None,
        "feature_version": FEATURE_VERSION,
        "universe_count": len({row["code"] for row in candidate_rows}),
        "candidate_count_long": len(long_codes),
        "candidate_count_short": len(short_codes),
        "recall_at_20": None,
        "recall_at_10": None,
        "monthly_top5_capture": None,
        "avg_ret_20_top20": None,
        "avg_mfe_20_top20": None,
        "avg_mae_20_top20": None,
        "max_drawdown_proxy": None,
        "turnover_proxy": float(len(candidate_rows)),
        "regime_breakdown_json": json.dumps(regime, ensure_ascii=False, sort_keys=True),
        "created_at": _utcnow(),
    }
    if not long_codes:
        return metrics
    conn = connect_label_db(label_db_path)
    try:
        placeholders = ", ".join(["?"] * len(long_codes))
        rows = conn.execute(
            f"""
            SELECT code, ret_h, mfe_h, mae_h, rank_ret_h, top_5pct_h, policy_version
            FROM label_daily_h20
            WHERE as_of_date = ? AND code IN ({placeholders})
            """,
            [as_of_date, *long_codes],
        ).fetchall()
    finally:
        conn.close()
    if not rows:
        return metrics
    returns = [float(row[1]) for row in rows if row[1] is not None]
    mfes = [float(row[2]) for row in rows if row[2] is not None]
    maes = [float(row[3]) for row in rows if row[3] is not None]
    ranks = [int(row[4]) for row in rows if row[4] is not None]
    top5_hits = [1.0 for row in rows if bool(row[5])]
    metrics["label_policy_version"] = str(rows[0][6]) if rows[0][6] is not None else None
    if returns:
        metrics["avg_ret_20_top20"] = sum(returns) / len(returns)
    if mfes:
        metrics["avg_mfe_20_top20"] = sum(mfes) / len(mfes)
    if maes:
        metrics["avg_mae_20_top20"] = sum(maes) / len(maes)
        metrics["max_drawdown_proxy"] = min(maes)
    if ranks:
        metrics["recall_at_20"] = sum(1.0 for value in ranks if value <= 20) / len(long_codes)
        metrics["recall_at_10"] = sum(1.0 for value in ranks if value <= 10) / len(long_codes)
    if top5_hits:
        metrics["monthly_top5_capture"] = sum(top5_hits) / len(long_codes)
    return metrics


def _persist_nightly_metrics(
    *,
    result_db_path: str | None,
    metrics_row: dict[str, Any],
) -> dict[str, Any]:
    conn = connect_result_db(result_db_path, read_only=False)
    try:
        ensure_result_schema(conn)
        conn.execute("BEGIN TRANSACTION")
        try:
            conn.execute("DELETE FROM nightly_candidate_metrics WHERE publish_id = ?", [metrics_row["publish_id"]])
            metrics_columns = list(metrics_row.keys())
            conn.execute(
                f"INSERT INTO nightly_candidate_metrics ({', '.join(metrics_columns)}) VALUES ({', '.join(['?'] * len(metrics_columns))})",
                [metrics_row[column] for column in metrics_columns],
            )
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            raise
        conn.execute("CHECKPOINT")
        return {"saved": True, "run_id": metrics_row["run_id"], "attempts": 1}
    finally:
        conn.close()


def persist_nightly_metrics_with_retry(
    *,
    result_db_path: str | None,
    metrics_row: dict[str, Any],
    max_attempts: int = METRICS_MAX_ATTEMPTS,
) -> dict[str, Any]:
    last_error: Exception | None = None
    for attempt in range(1, int(max_attempts) + 1):
        try:
            payload = _persist_nightly_metrics(result_db_path=result_db_path, metrics_row=metrics_row)
            payload["attempts"] = attempt
            return payload
        except Exception as exc:
            last_error = exc
            logger.warning(
                "nightly_candidate_metrics persist failed attempt=%s publish_id=%s error=%s",
                attempt,
                metrics_row.get("publish_id"),
                exc,
            )
    return {
        "saved": False,
        "run_id": metrics_row["run_id"],
        "attempts": int(max_attempts),
        "error_class": type(last_error).__name__ if last_error else "RuntimeError",
        "error_message": str(last_error) if last_error else "metrics_persist_failed",
    }


def run_candidate_baseline(
    *,
    export_db_path: str | None = None,
    label_db_path: str | None = None,
    result_db_path: str | None = None,
    as_of_date: str | int | None = None,
    publish_id: str | None = None,
    freshness_state: str = "fresh",
    publish_public: bool = True,
    codes: list[str] | None = None,
    ops_db_path: str | None = None,
    candidate_limit_per_side: int = MAX_CANDIDATES_PER_SIDE,
    similarity_db_path: str | None = None,
) -> dict[str, Any]:
    frame = load_candidate_input_frame(export_db_path=export_db_path, as_of_date=as_of_date, codes=codes)
    as_of_date_int = _normalize_as_of_date(as_of_date if as_of_date is not None else "")
    actual_publish_id = publish_id or _default_publish_id(as_of_date_int)
    scored, regime = _score_frame(frame)
    component_rows, candidate_rows = _build_candidate_rows(
        scored=scored,
        regime=regime,
        publish_id=actual_publish_id,
        as_of_date=as_of_date_int,
        freshness_state=freshness_state,
        candidate_limit_per_side=candidate_limit_per_side,
    )
    regime_rows = [
        {
            "publish_id": actual_publish_id,
            "as_of_date": _as_of_date_text(as_of_date_int),
            "regime_tag": regime["regime_tag"],
            "regime_score": float(regime["regime_score"]),
            "breadth_score": float(regime["breadth_score"]),
            "volatility_state": regime["volatility_state"],
        }
    ]
    metrics_row = _evaluate_nightly_metrics(
        label_db_path=label_db_path,
        as_of_date=as_of_date_int,
        publish_id=actual_publish_id,
        regime=regime,
        candidate_rows=candidate_rows,
    )
    state_eval_payload = build_state_eval_rows(
        scored=scored,
        candidate_rows=candidate_rows,
        publish_id=actual_publish_id,
        as_of_date=as_of_date_int,
        freshness_state=freshness_state,
        export_db_path=export_db_path,
        label_db_path=label_db_path,
        similarity_db_path=similarity_db_path,
        ops_db_path=ops_db_path,
    )
    state_eval_rows = state_eval_payload["rows"]
    conn = connect_result_db(result_db_path, read_only=False)
    try:
        ensure_result_schema(conn)
        conn.execute("BEGIN TRANSACTION")
        try:
            for table_name in ("candidate_daily", "candidate_component_scores", "regime_daily", "state_eval_daily"):
                conn.execute(f"DELETE FROM {table_name} WHERE publish_id = ?", [actual_publish_id])
            if component_rows:
                component_columns = list(component_rows[0].keys())
                conn.executemany(
                    f"INSERT INTO candidate_component_scores ({', '.join(component_columns)}) VALUES ({', '.join(['?'] * len(component_columns))})",
                    [[row[column] for column in component_columns] for row in component_rows],
                )
            if candidate_rows:
                candidate_columns = list(candidate_rows[0].keys())
                conn.executemany(
                    f"INSERT INTO candidate_daily ({', '.join(candidate_columns)}) VALUES ({', '.join(['?'] * len(candidate_columns))})",
                    [[row[column] for column in candidate_columns] for row in candidate_rows],
                )
            regime_columns = list(regime_rows[0].keys())
            conn.executemany(
                f"INSERT INTO regime_daily ({', '.join(regime_columns)}) VALUES ({', '.join(['?'] * len(regime_columns))})",
                [[row[column] for column in regime_columns] for row in regime_rows],
            )
            if state_eval_rows:
                state_eval_columns = list(state_eval_rows[0].keys())
                conn.executemany(
                    f"INSERT INTO state_eval_daily ({', '.join(state_eval_columns)}) VALUES ({', '.join(['?'] * len(state_eval_columns))})",
                    [[row[column] for column in state_eval_columns] for row in state_eval_rows],
                )
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            raise
        conn.execute("CHECKPOINT")
    finally:
        conn.close()
    publish_payload = None
    if publish_public:
        publish_payload = publish_result(
            db_path=result_db_path,
            publish_id=actual_publish_id,
            as_of_date=_as_of_date_text(as_of_date_int),
            freshness_state=freshness_state,
            table_row_counts={
                "candidate_daily": len(candidate_rows),
                "regime_daily": len(regime_rows),
                "state_eval_daily": len(state_eval_rows),
                "similar_cases_daily": 0,
                "similar_case_paths": 0,
            },
            degrade_ready=True,
        )
    metrics_result = persist_nightly_metrics_with_retry(
        result_db_path=result_db_path,
        metrics_row=metrics_row,
    )
    shadow_result = persist_state_eval_shadow(
        ops_db_path=ops_db_path,
        publish_id=actual_publish_id,
        as_of_date=as_of_date_int,
        champion_rows=state_eval_rows,
        challenger_rows=state_eval_payload["challenger_rows"],
        teacher_profile=state_eval_payload["teacher_profile"],
        similarity_support=state_eval_payload["similarity_support"],
        tag_prior_support=state_eval_payload["tag_prior_support"],
        label_db_path=label_db_path,
    )
    candidate_bundle_result = build_publish_candidate_bundle(
        db_path=result_db_path,
        ops_db_path=ops_db_path,
        publish_id=actual_publish_id,
        readiness=shadow_result,
    )
    return {
        "ok": True,
        "publish": publish_payload,
        "publish_id": actual_publish_id,
        "as_of_date": _as_of_date_text(as_of_date_int),
        "model_key": MODEL_KEY,
        "baseline_version": BASELINE_VERSION,
        "feature_version": FEATURE_VERSION,
        "candidate_limit_per_side": int(max(1, candidate_limit_per_side)),
        "candidate_count_long": len([row for row in candidate_rows if row["side"] == "long"]),
        "candidate_count_short": len([row for row in candidate_rows if row["side"] == "short"]),
        "state_eval_count": len(state_eval_rows),
        "regime_tag": regime["regime_tag"],
        "nightly_metrics_run_id": metrics_row["run_id"],
        "metrics_saved": bool(metrics_result["saved"]),
        "metrics_attempts": int(metrics_result["attempts"]),
        "metrics_error_class": metrics_result.get("error_class"),
        "state_eval_baseline_version": state_eval_payload["baseline_version"],
        "state_eval_challenger_version": state_eval_payload["challenger_version"],
        "state_eval_shadow_saved": bool(shadow_result.get("saved")),
        "state_eval_readiness_pass": bool(shadow_result.get("readiness_pass")),
        "state_eval_shadow_summary": shadow_result.get("summary"),
        "candidate_bundle_saved": bool(candidate_bundle_result.get("ok")),
        "candidate_bundle": candidate_bundle_result.get("bundle"),
    }
