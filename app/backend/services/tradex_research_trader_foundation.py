from __future__ import annotations

import asyncio
import json
import os
from datetime import datetime, timezone
from typing import Any

import httpx

from app.backend.api import dependencies
from app.backend.services.ai_explain_service import OpenAICompatibleProvider
from app.backend.services import tradex_research_os_contracts as os_contracts


TRADEX_OBSERVATION_CONTRACT_VERSION = "tradex_close_based_buy_judgement_v1"
TRADEX_DEFAULT_PRIMARY_ADAPTER_ID = "numeric_baseline_v1"
TRADEX_DEFAULT_ADAPTER_IDS = ("numeric_baseline_v1",)
TRADEX_SUPPORTED_ADAPTER_IDS = ("numeric_baseline_v1", "structured_reasoner_v1")
TRADEX_TRADER_LLM_ENDPOINT_ENV = "TRADEX_TRADER_LLM_ENDPOINT_URL"
TRADEX_TRADER_LLM_MODEL_ENV = "TRADEX_TRADER_LLM_MODEL"
TRADEX_TRADER_LLM_API_KEY_ENV = "TRADEX_TRADER_LLM_API_KEY"
TRADEX_TRADER_LLM_TIMEOUT_ENV = "TRADEX_TRADER_LLM_TIMEOUT_SEC"


def _text(value: Any, fallback: str = "") -> str:
    if value is None:
        return fallback
    if isinstance(value, str):
        text = value.strip()
        return text or fallback
    text = str(value).strip()
    return text or fallback


def _clamp(value: float, low: float = 0.0, high: float = 1.0) -> float:
    return max(low, min(high, float(value)))


def _mean(values: list[float]) -> float:
    if not values:
        return 0.0
    return float(sum(values)) / float(len(values))


def _as_yyyymmdd(value: Any) -> int:
    if isinstance(value, int):
        if value >= 1_000_000_000:
            return int(datetime.fromtimestamp(value, tz=timezone.utc).strftime("%Y%m%d"))
        return value
    text = _text(value)
    if not text:
        raise ValueError("target.as_of_date is required")
    digits = "".join(ch for ch in text if ch.isdigit())
    if len(digits) == 8:
        return int(digits)
    raise ValueError(f"unsupported market date format: {value}")


def _normalize_bar(row: Any) -> dict[str, Any]:
    if isinstance(row, dict):
        return {
            "market_date": _as_yyyymmdd(row.get("market_date", row.get("date"))),
            "open": float(row.get("open", row.get("o")) or 0.0),
            "high": float(row.get("high", row.get("h")) or 0.0),
            "low": float(row.get("low", row.get("l")) or 0.0),
            "close": float(row.get("close", row.get("c")) or 0.0),
            "volume": float(row.get("volume", row.get("v")) or 0.0),
        }
    if isinstance(row, (list, tuple)) and len(row) >= 6:
        return {
            "market_date": _as_yyyymmdd(row[0]),
            "open": float(row[1] or 0.0),
            "high": float(row[2] or 0.0),
            "low": float(row[3] or 0.0),
            "close": float(row[4] or 0.0),
            "volume": float(row[5] or 0.0),
        }
    raise ValueError(f"unsupported daily bar shape: {type(row).__name__}")


def resolve_strategy_foundation_config(hypothesis: dict[str, Any]) -> dict[str, Any] | None:
    strategy_target = hypothesis.get("strategy_target")
    if not isinstance(strategy_target, dict):
        return None
    strategy_judgement = hypothesis.get("strategy_judgement")
    strategy_judgement = dict(strategy_judgement) if isinstance(strategy_judgement, dict) else {}
    adapter_ids = strategy_judgement.get("adapter_ids")
    if isinstance(adapter_ids, list):
        normalized_adapter_ids = [_text(item) for item in adapter_ids if _text(item)]
    else:
        normalized_adapter_ids = list(TRADEX_DEFAULT_ADAPTER_IDS)
    if not normalized_adapter_ids:
        raise ValueError("strategy_judgement.adapter_ids must be non-empty when strategy_target is present")
    unsupported = [adapter_id for adapter_id in normalized_adapter_ids if adapter_id not in TRADEX_SUPPORTED_ADAPTER_IDS]
    if unsupported:
        raise ValueError(f"unsupported strategy_judgement.adapter_ids: {', '.join(unsupported)}")
    primary_adapter_id = _text(strategy_judgement.get("primary_adapter_id"), fallback=normalized_adapter_ids[0])
    if primary_adapter_id not in normalized_adapter_ids:
        raise ValueError("strategy_judgement.primary_adapter_id must be included in adapter_ids")
    return {
        "target": {
            "code": _text(strategy_target.get("code")),
            "as_of_date": _as_yyyymmdd(strategy_target.get("as_of_date")),
            "side": _text(strategy_target.get("side")),
            "judgement_type": _text(strategy_target.get("judgement_type")),
        },
        "primary_adapter_id": primary_adapter_id,
        "adapter_ids": normalized_adapter_ids,
        "observation_lookback_bars": int(strategy_judgement.get("observation_lookback_bars") or 120),
        "teacher_horizon_bars": int(strategy_judgement.get("teacher_horizon_bars") or 20),
        "observation_contract_version": TRADEX_OBSERVATION_CONTRACT_VERSION,
    }


def _moving_average(closes: list[float], length: int) -> float:
    if len(closes) < length or length <= 0:
        return 0.0
    return _mean(closes[-length:])


def _consecutive_up_closes(closes: list[float]) -> int:
    count = 0
    for idx in range(len(closes) - 1, 0, -1):
        if closes[idx] > closes[idx - 1]:
            count += 1
            continue
        break
    return count


def _rolling_high(bars: list[dict[str, Any]]) -> float:
    if not bars:
        return 0.0
    return max(float(bar["high"]) for bar in bars)


def _rolling_low(bars: list[dict[str, Any]]) -> float:
    if not bars:
        return 0.0
    return min(float(bar["low"]) for bar in bars)


def _build_derived_features(history_bars: list[dict[str, Any]]) -> dict[str, Any]:
    confirmed_bar = history_bars[-1]
    closes = [float(bar["close"]) for bar in history_bars]
    volumes = [float(bar["volume"]) for bar in history_bars]
    ma7 = _moving_average(closes, 7)
    ma20 = _moving_average(closes, 20)
    ma60 = _moving_average(closes, 60)
    ma100 = _moving_average(closes, 100)
    ma200 = _moving_average(closes, 200)
    prev_closes = closes[:-1]
    ma20_prev = _moving_average(prev_closes, 20)
    ma60_prev = _moving_average(prev_closes, 60)
    ma100_prev = _moving_average(prev_closes, 100)
    previous_bar = history_bars[-2] if len(history_bars) >= 2 else None
    previous_close = float(previous_bar["close"]) if previous_bar else float(confirmed_bar["close"])
    candle_range = max(0.0, float(confirmed_bar["high"]) - float(confirmed_bar["low"]))
    body = abs(float(confirmed_bar["close"]) - float(confirmed_bar["open"]))
    upper_wick = max(0.0, float(confirmed_bar["high"]) - max(float(confirmed_bar["open"]), float(confirmed_bar["close"])))
    lower_wick = max(0.0, min(float(confirmed_bar["open"]), float(confirmed_bar["close"])) - float(confirmed_bar["low"]))
    prior_window_5 = history_bars[-6:-1]
    prior_window_10 = history_bars[-11:-1]
    prior_window_20 = history_bars[-21:-1]
    window_20 = history_bars[-20:]
    prior_high_20 = _rolling_high(prior_window_20)
    prior_low_20 = _rolling_low(prior_window_20)
    window_high_20 = _rolling_high(window_20)
    window_low_20 = _rolling_low(window_20)
    range_span_20 = max(0.0, window_high_20 - window_low_20)
    close_position_20 = 0.0 if range_span_20 <= 0 else (float(confirmed_bar["close"]) - window_low_20) / range_span_20
    volume_ratio_5 = 1.0
    if len(volumes) >= 6:
        volume_ratio_5 = float(confirmed_bar["volume"]) / max(1.0, _mean(volumes[-6:-1]))
    volume_ratio_20 = 1.0
    if len(volumes) >= 21:
        volume_ratio_20 = float(confirmed_bar["volume"]) / max(1.0, _mean(volumes[-21:-1]))
    recent_hilo = history_bars[-4:]
    higher_highs = len(recent_hilo) >= 3 and all(float(curr["high"]) > float(prev["high"]) for prev, curr in zip(recent_hilo, recent_hilo[1:]))
    higher_lows = len(recent_hilo) >= 3 and all(float(curr["low"]) > float(prev["low"]) for prev, curr in zip(recent_hilo, recent_hilo[1:]))
    close_above_prior_high_20 = bool(prior_window_20) and float(confirmed_bar["close"]) > prior_high_20
    failed_breakout_20 = bool(prior_window_20) and float(confirmed_bar["high"]) > prior_high_20 and float(confirmed_bar["close"]) <= prior_high_20
    gap_value = float(confirmed_bar["open"]) - previous_close
    gap_pct = 0.0 if previous_close == 0 else gap_value / previous_close
    extension_from_ma20_pct = 0.0 if ma20 <= 0 else (float(confirmed_bar["close"]) - ma20) / ma20
    return {
        "moving_averages": {
            "ma7": ma7,
            "ma20": ma20,
            "ma60": ma60,
            "ma100": ma100,
            "ma200": ma200,
        },
        "ma_alignment": {
            "close_above_ma20": float(confirmed_bar["close"]) >= ma20 > 0,
            "close_above_ma60": float(confirmed_bar["close"]) >= ma60 > 0,
            "ma20_above_ma60": ma20 >= ma60 > 0,
            "ma60_above_ma100": ma60 >= ma100 > 0,
            "ma20_slope_positive": ma20 > ma20_prev if ma20_prev > 0 else ma20 > 0,
            "ma60_slope_positive": ma60 > ma60_prev if ma60_prev > 0 else ma60 > 0,
            "ma100_slope_positive": ma100 > ma100_prev if ma100_prev > 0 else ma100 > 0,
        },
        "candle_structure": {
            "range": candle_range,
            "body": body,
            "upper_wick": upper_wick,
            "lower_wick": lower_wick,
            "body_to_range_ratio": 0.0 if candle_range <= 0 else body / candle_range,
            "upper_wick_ratio": 0.0 if candle_range <= 0 else upper_wick / candle_range,
            "lower_wick_ratio": 0.0 if candle_range <= 0 else lower_wick / candle_range,
            "bullish_close": float(confirmed_bar["close"]) >= float(confirmed_bar["open"]),
        },
        "gap_context": {
            "gap_value": gap_value,
            "gap_pct": gap_pct,
            "gap_up": gap_value > 0,
            "gap_down": gap_value < 0,
        },
        "range_context": {
            "prior_high_5": _rolling_high(prior_window_5),
            "prior_low_5": _rolling_low(prior_window_5),
            "prior_high_10": _rolling_high(prior_window_10),
            "prior_low_10": _rolling_low(prior_window_10),
            "prior_high_20": prior_high_20,
            "prior_low_20": prior_low_20,
            "window_high_20": window_high_20,
            "window_low_20": window_low_20,
            "close_position_20": close_position_20,
            "extension_from_ma20_pct": extension_from_ma20_pct,
        },
        "breakout_context": {
            "close_above_prior_high_20": close_above_prior_high_20,
            "failed_breakout_20": failed_breakout_20,
        },
        "volume_context": {
            "volume_ratio_5": volume_ratio_5,
            "volume_ratio_20": volume_ratio_20,
        },
        "sequence_context": {
            "higher_highs_recent": higher_highs,
            "higher_lows_recent": higher_lows,
            "consecutive_up_closes": _consecutive_up_closes(closes),
        },
    }


def _reason_codes_from_features(features: dict[str, Any]) -> list[str]:
    reason_codes: list[str] = []
    ma_alignment = dict(features.get("ma_alignment") or {})
    breakout_context = dict(features.get("breakout_context") or {})
    volume_context = dict(features.get("volume_context") or {})
    candle_structure = dict(features.get("candle_structure") or {})
    range_context = dict(features.get("range_context") or {})
    sequence_context = dict(features.get("sequence_context") or {})
    if bool(breakout_context.get("close_above_prior_high_20")):
        reason_codes.append("close_breakout_20")
    if bool(breakout_context.get("failed_breakout_20")):
        reason_codes.append("breakout_failure")
    if bool(ma_alignment.get("ma20_above_ma60")) and bool(ma_alignment.get("close_above_ma20")):
        reason_codes.append("ma_trend_aligned")
    if float(volume_context.get("volume_ratio_20") or 0.0) >= 1.15:
        reason_codes.append("volume_expansion")
    if float(candle_structure.get("lower_wick_ratio") or 0.0) >= 0.25:
        reason_codes.append("support_rejection")
    if float(candle_structure.get("upper_wick_ratio") or 0.0) >= 0.45:
        reason_codes.append("upper_wick_supply")
    if float(range_context.get("extension_from_ma20_pct") or 0.0) >= 0.08:
        reason_codes.append("extended_from_ma20")
    if not bool(sequence_context.get("higher_highs_recent")):
        reason_codes.append("trend_sequence_not_confirmed")
    return list(dict.fromkeys(reason_codes))


def _numeric_baseline_adapter(observation_snapshot: dict[str, Any]) -> dict[str, Any]:
    features = dict(observation_snapshot.get("derived_features") or {})
    confirmed_bar = dict(observation_snapshot.get("confirmed_bar") or {})
    ma_alignment = dict(features.get("ma_alignment") or {})
    breakout_context = dict(features.get("breakout_context") or {})
    range_context = dict(features.get("range_context") or {})
    volume_context = dict(features.get("volume_context") or {})
    candle_structure = dict(features.get("candle_structure") or {})
    sequence_context = dict(features.get("sequence_context") or {})

    environment_score = 0.45
    if bool(ma_alignment.get("close_above_ma60")):
        environment_score += 0.15
    if bool(ma_alignment.get("ma20_above_ma60")):
        environment_score += 0.15
    if bool(ma_alignment.get("ma60_above_ma100")):
        environment_score += 0.1
    if not bool(ma_alignment.get("close_above_ma20")):
        environment_score -= 0.2
    environment_score = _clamp(environment_score)

    trend_score = 0.4
    if bool(sequence_context.get("higher_highs_recent")):
        trend_score += 0.15
    if bool(sequence_context.get("higher_lows_recent")):
        trend_score += 0.15
    if bool(ma_alignment.get("ma20_slope_positive")):
        trend_score += 0.1
    if int(sequence_context.get("consecutive_up_closes") or 0) >= 2:
        trend_score += 0.1
    if not bool(ma_alignment.get("close_above_ma20")):
        trend_score -= 0.2
    trend_score = _clamp(trend_score)

    trigger_score = 0.3
    if bool(breakout_context.get("close_above_prior_high_20")):
        trigger_score += 0.3
    if float(volume_context.get("volume_ratio_20") or 0.0) >= 1.1:
        trigger_score += 0.15
    if float(candle_structure.get("body_to_range_ratio") or 0.0) >= 0.55:
        trigger_score += 0.1
    if bool(candle_structure.get("bullish_close")):
        trigger_score += 0.05
    if bool(breakout_context.get("failed_breakout_20")):
        trigger_score -= 0.35
    if float(candle_structure.get("upper_wick_ratio") or 0.0) >= 0.45:
        trigger_score -= 0.1
    trigger_score = _clamp(trigger_score)

    risk_score = 0.55
    if bool(breakout_context.get("failed_breakout_20")):
        risk_score -= 0.3
    if float(range_context.get("extension_from_ma20_pct") or 0.0) >= 0.08:
        risk_score -= 0.15
    if float(range_context.get("close_position_20") or 0.0) >= 0.95:
        risk_score -= 0.1
    if float(candle_structure.get("lower_wick_ratio") or 0.0) >= 0.2:
        risk_score += 0.1
    if bool(ma_alignment.get("close_above_ma20")):
        risk_score += 0.05
    risk_score = _clamp(risk_score)

    buy_score = _clamp((environment_score * 0.2) + (trend_score * 0.3) + (trigger_score * 0.3) + (risk_score * 0.2))
    invalidation_price = min(
        float(range_context.get("prior_low_5") or float(confirmed_bar.get("low") or 0.0)),
        float(confirmed_bar.get("low") or 0.0),
    )
    reason_codes = _reason_codes_from_features(features)
    if bool(breakout_context.get("failed_breakout_20")) or (not bool(ma_alignment.get("close_above_ma20")) and not bool(ma_alignment.get("close_above_ma60"))):
        machine_action_state = "skip"
        human_readable_judgement = "reject"
        invalidation_reason_code = "breakout_failure" if bool(breakout_context.get("failed_breakout_20")) else "ma_support_lost"
    elif buy_score >= 0.65 and trigger_score >= 0.6 and risk_score >= 0.4:
        machine_action_state = "enter"
        human_readable_judgement = "buy"
        invalidation_reason_code = "daily_swing_low_break"
    elif buy_score < 0.4 or risk_score < 0.25:
        machine_action_state = "skip"
        human_readable_judgement = "reject"
        invalidation_reason_code = "risk_too_high"
    else:
        machine_action_state = "wait"
        human_readable_judgement = "hold"
        invalidation_reason_code = "trigger_not_confirmed"
    return {
        "adapter_id": "numeric_baseline_v1",
        "adapter_kind": "classical",
        "machine_action_state": machine_action_state,
        "human_readable_judgement": human_readable_judgement,
        "buy_score": buy_score,
        "environment_score": environment_score,
        "trend_score": trend_score,
        "trigger_score": trigger_score,
        "risk_score": risk_score,
        "invalidation_price": float(invalidation_price),
        "invalidation_reason_code": invalidation_reason_code,
        "reason_codes": reason_codes,
        "explanation": "confirmed candle, moving-average alignment, breakout context, and volume expansion baseline",
        "confidence": _clamp(0.45 + (buy_score * 0.4)),
    }


def _llm_reasoner_settings_from_env() -> dict[str, Any]:
    endpoint_url = _text(os.getenv(TRADEX_TRADER_LLM_ENDPOINT_ENV))
    model = _text(os.getenv(TRADEX_TRADER_LLM_MODEL_ENV))
    api_key = _text(os.getenv(TRADEX_TRADER_LLM_API_KEY_ENV))
    if not endpoint_url:
        raise ValueError(f"structured_reasoner_v1 requires {TRADEX_TRADER_LLM_ENDPOINT_ENV}")
    if not model:
        raise ValueError(f"structured_reasoner_v1 requires {TRADEX_TRADER_LLM_MODEL_ENV}")
    if not api_key:
        raise ValueError(f"structured_reasoner_v1 requires {TRADEX_TRADER_LLM_API_KEY_ENV}")
    timeout_raw = _text(os.getenv(TRADEX_TRADER_LLM_TIMEOUT_ENV), fallback="30")
    try:
        timeout_sec = float(timeout_raw)
    except ValueError as exc:
        raise ValueError(f"{TRADEX_TRADER_LLM_TIMEOUT_ENV} must be numeric") from exc
    return {
        "endpoint_url": endpoint_url,
        "model": model,
        "api_key": api_key,
        "timeout_sec": max(1.0, timeout_sec),
    }


def _llm_system_prompt() -> str:
    return (
        "あなたは日本株の日足 close-based buy judgement を行う研究用アダプタです。"
        "入力は observation snapshot のみです。"
        "出力は JSON object のみで返してください。"
        "説明や前置き、Markdown、コードフェンスは禁止です。"
        "machine_action_state は enter|wait|skip、"
        "human_readable_judgement は buy|hold|reject を使ってください。"
        "score は 0.0 から 1.0、invalidation_price は数値、reason_codes は文字列配列にしてください。"
    )


def _llm_user_prompt(observation_snapshot: dict[str, Any]) -> str:
    payload = {
        "target": observation_snapshot.get("target") or {},
        "confirmed_bar": observation_snapshot.get("confirmed_bar") or {},
        "recent_bars": observation_snapshot.get("recent_bars") or [],
        "derived_features": observation_snapshot.get("derived_features") or {},
        "market_context": observation_snapshot.get("market_context") or {},
    }
    required_shape = {
        "machine_action_state": "enter|wait|skip",
        "human_readable_judgement": "buy|hold|reject",
        "buy_score": "0.0-1.0",
        "environment_score": "0.0-1.0",
        "trend_score": "0.0-1.0",
        "trigger_score": "0.0-1.0",
        "risk_score": "0.0-1.0",
        "invalidation_price": "number",
        "invalidation_reason_code": "string",
        "reason_codes": ["string"],
        "explanation": "string",
        "confidence": "0.0-1.0",
    }
    return (
        "次の observation snapshot を読んで close-based daily buy judgement を行ってください。\n"
        "JSON object だけを返してください。\n"
        f"required_output_shape={json.dumps(required_shape, ensure_ascii=False)}\n"
        f"observation_snapshot={json.dumps(payload, ensure_ascii=False, sort_keys=True)}"
    )


def _extract_json_object(text: str) -> dict[str, Any]:
    candidate = text.strip()
    if candidate.startswith("```"):
        lines = candidate.splitlines()
        if len(lines) >= 3:
            candidate = "\n".join(lines[1:-1]).strip()
    if candidate.startswith("{") and candidate.endswith("}"):
        payload = json.loads(candidate)
        if isinstance(payload, dict):
            return payload
    start = candidate.find("{")
    end = candidate.rfind("}")
    if start >= 0 and end > start:
        payload = json.loads(candidate[start:end + 1])
        if isinstance(payload, dict):
            return payload
    raise ValueError("structured_reasoner_v1_invalid_output_json")


def _normalize_reason_codes(value: Any) -> list[str]:
    if not isinstance(value, list):
        raise ValueError("structured_reasoner_v1_invalid_reason_codes")
    return [_text(item) for item in value if _text(item)]


def _normalize_llm_adapter_output(payload: dict[str, Any]) -> dict[str, Any]:
    machine_action_state = _text(payload.get("machine_action_state"))
    if machine_action_state not in os_contracts.TRADEX_TRADER_MACHINE_ACTION_STATES:
        raise ValueError("structured_reasoner_v1_invalid_machine_action_state")
    human_readable_judgement = _text(payload.get("human_readable_judgement"))
    if human_readable_judgement not in os_contracts.TRADEX_TRADER_HUMAN_JUDGEMENTS:
        raise ValueError("structured_reasoner_v1_invalid_human_readable_judgement")
    invalidation_reason_code = _text(payload.get("invalidation_reason_code"))
    if not invalidation_reason_code:
        raise ValueError("structured_reasoner_v1_invalid_invalidation_reason_code")
    explanation = _text(payload.get("explanation"))
    if not explanation:
        raise ValueError("structured_reasoner_v1_invalid_explanation")
    try:
        invalidation_price = float(payload.get("invalidation_price"))
        buy_score = _clamp(float(payload.get("buy_score")))
        environment_score = _clamp(float(payload.get("environment_score")))
        trend_score = _clamp(float(payload.get("trend_score")))
        trigger_score = _clamp(float(payload.get("trigger_score")))
        risk_score = _clamp(float(payload.get("risk_score")))
        confidence = _clamp(float(payload.get("confidence")))
    except (TypeError, ValueError) as exc:
        raise ValueError("structured_reasoner_v1_invalid_numeric_fields") from exc
    return {
        "adapter_id": "structured_reasoner_v1",
        "adapter_kind": "llm_structured",
        "machine_action_state": machine_action_state,
        "human_readable_judgement": human_readable_judgement,
        "buy_score": buy_score,
        "environment_score": environment_score,
        "trend_score": trend_score,
        "trigger_score": trigger_score,
        "risk_score": risk_score,
        "invalidation_price": invalidation_price,
        "invalidation_reason_code": invalidation_reason_code,
        "reason_codes": _normalize_reason_codes(payload.get("reason_codes")),
        "explanation": explanation,
        "confidence": confidence,
    }


async def _generate_llm_adapter_output(observation_snapshot: dict[str, Any]) -> dict[str, Any]:
    settings = _llm_reasoner_settings_from_env()
    provider = OpenAICompatibleProvider(
        endpoint_url=settings["endpoint_url"],
        api_key=settings["api_key"],
        model=settings["model"],
        timeout_sec=float(settings["timeout_sec"]),
    )
    answer, _usage = await provider.generate(
        system_prompt=_llm_system_prompt(),
        user_prompt=_llm_user_prompt(observation_snapshot),
        images=[],
        max_tokens=700,
        temperature=0.1,
    )
    return _normalize_llm_adapter_output(_extract_json_object(answer))


def _structured_reasoner_adapter(observation_snapshot: dict[str, Any]) -> dict[str, Any]:
    try:
        return asyncio.run(_generate_llm_adapter_output(observation_snapshot))
    except ValueError:
        raise
    except httpx.TimeoutException as exc:
        raise RuntimeError("structured_reasoner_v1_timeout") from exc
    except httpx.HTTPError as exc:
        raise RuntimeError(f"structured_reasoner_v1_provider_http_error: {exc}") from exc
    except RuntimeError as exc:
        if "asyncio.run() cannot be called" in str(exc):
            raise RuntimeError("structured_reasoner_v1_requires_sync_context") from exc
        raise


def _teacher_outcome_window(
    *,
    confirmed_bar: dict[str, Any],
    future_bars: list[dict[str, Any]],
    teacher_horizon_bars: int,
) -> dict[str, Any]:
    anchor_close_price = float(confirmed_bar.get("close") or 0.0)
    next_open_price = float(future_bars[0]["open"]) if future_bars else None
    future_closes = [float(bar["close"]) for bar in future_bars]
    future_lows = [float(bar["low"]) for bar in future_bars]
    final_close_price = future_closes[-1] if future_closes else None
    max_close = max(future_closes) if future_closes else None
    min_low = min(future_lows) if future_lows else None
    return {
        "teacher_horizon_bars": int(teacher_horizon_bars),
        "future_bar_count": len(future_bars),
        "complete_horizon": len(future_bars) >= int(teacher_horizon_bars),
        "anchor_close_price": anchor_close_price,
        "next_open_price": next_open_price,
        "final_close_price": final_close_price,
        "return_close_basis": None if final_close_price is None or anchor_close_price == 0 else (final_close_price / anchor_close_price) - 1.0,
        "return_next_open_basis": None if final_close_price is None or next_open_price in (None, 0.0) else (final_close_price / float(next_open_price)) - 1.0,
        "max_favorable_excursion_close_basis": None if max_close is None or anchor_close_price == 0 else (max_close / anchor_close_price) - 1.0,
        "max_adverse_excursion_close_basis": None if min_low is None or anchor_close_price == 0 else (min_low / anchor_close_price) - 1.0,
        "future_dates": [int(bar["market_date"]) for bar in future_bars],
    }


def _adapter_outputs_for_snapshot(
    observation_snapshot: dict[str, Any],
    *,
    config: dict[str, Any],
) -> list[dict[str, Any]]:
    outputs: list[dict[str, Any]] = []
    for adapter_id in list(config.get("adapter_ids") or []):
        if adapter_id == "numeric_baseline_v1":
            outputs.append(_numeric_baseline_adapter(observation_snapshot))
            continue
        if adapter_id == "structured_reasoner_v1":
            outputs.append(_structured_reasoner_adapter(observation_snapshot))
            continue
        raise ValueError(f"unsupported strategy adapter: {adapter_id}")
    return outputs


def build_strategy_foundation_artifacts(
    *,
    experiment_id: str,
    hypothesis: dict[str, Any],
    stock_repo: Any | None = None,
) -> dict[str, Any] | None:
    config = resolve_strategy_foundation_config(hypothesis)
    if config is None:
        return None
    repo = stock_repo or dependencies.get_stock_repo()
    target = dict(config["target"])
    code = _text(target.get("code"))
    as_of_date = int(target.get("as_of_date"))
    requested_limit = max(
        int(config["observation_lookback_bars"]) + int(config["teacher_horizon_bars"]) + 30,
        260,
    )
    bars = repo.get_daily_bars(code, limit=requested_limit, asof_dt=None)
    normalized_bars = [_normalize_bar(row) for row in bars]
    matching_index = -1
    for idx, bar in enumerate(normalized_bars):
        if int(bar["market_date"]) == as_of_date:
            matching_index = idx
            break
    if matching_index < 0:
        raise ValueError(f"strategy_target.as_of_date not found in daily_bars: code={code} as_of_date={as_of_date}")
    anchor_end = matching_index + 1
    history_bars = normalized_bars[max(0, anchor_end - int(config["observation_lookback_bars"])):anchor_end]
    if not history_bars:
        raise ValueError(f"no observation history available for strategy_target: code={code} as_of_date={as_of_date}")
    confirmed_bar = dict(history_bars[-1])
    recent_bars = [dict(item) for item in history_bars[-min(20, len(history_bars)):]]
    future_bars = normalized_bars[anchor_end:anchor_end + int(config["teacher_horizon_bars"])]
    derived_features = _build_derived_features(history_bars)
    generated_at = os_contracts.now_utc_iso()
    observation_snapshot = os_contracts.build_observation_snapshot(
        experiment_id=experiment_id,
        hypothesis_id=_text(hypothesis.get("hypothesis_id")),
        target=target,
        observation_contract_version=_text(config.get("observation_contract_version")),
        confirmed_bar=confirmed_bar,
        recent_bars=recent_bars,
        derived_features=derived_features,
        market_context={
            "price_source": "daily_bars",
            "requested_lookback_bars": int(config["observation_lookback_bars"]),
            "available_lookback_bars": len(history_bars),
            "teacher_horizon_bars": int(config["teacher_horizon_bars"]),
            "future_bar_count": len(future_bars),
            "complete_teacher_horizon": len(future_bars) >= int(config["teacher_horizon_bars"]),
            "regime_context_available": False,
        },
        lineage={
            "source_method": "StockRepository.get_daily_bars",
            "source_code": code,
            "requested_limit": requested_limit,
            "anchor_market_date": as_of_date,
            "bar_count_loaded": len(normalized_bars),
            "hypothesis_hash": _text(hypothesis.get("hypothesis_hash")),
        },
        generated_at=generated_at,
    )
    adapter_outputs = _adapter_outputs_for_snapshot(observation_snapshot, config=config)
    adapter_output_by_id = {row["adapter_id"]: row for row in adapter_outputs}
    primary_output = adapter_output_by_id[_text(config["primary_adapter_id"])]
    distinct_decisions = {
        (_text(row.get("machine_action_state")), _text(row.get("human_readable_judgement")))
        for row in adapter_outputs
    }
    strategy_judgement = os_contracts.build_strategy_judgement(
        experiment_id=experiment_id,
        hypothesis_id=_text(hypothesis.get("hypothesis_id")),
        target=target,
        primary_adapter_id=_text(primary_output.get("adapter_id")),
        machine_action_state=_text(primary_output.get("machine_action_state")),
        human_readable_judgement=_text(primary_output.get("human_readable_judgement")),
        buy_score=float(primary_output.get("buy_score") or 0.0),
        environment_score=float(primary_output.get("environment_score") or 0.0),
        trend_score=float(primary_output.get("trend_score") or 0.0),
        trigger_score=float(primary_output.get("trigger_score") or 0.0),
        risk_score=float(primary_output.get("risk_score") or 0.0),
        invalidation_price=float(primary_output.get("invalidation_price") or 0.0),
        invalidation_reason_code=_text(primary_output.get("invalidation_reason_code")),
        reason_codes=[_text(item) for item in primary_output.get("reason_codes") or [] if _text(item)],
        adapter_outputs=adapter_outputs,
        observation_snapshot_hash=_text(observation_snapshot.get("observation_snapshot_hash")),
        generated_at=generated_at,
        explanation=_text(primary_output.get("explanation")),
        adapter_agreement=len(distinct_decisions) == 1 if len(adapter_outputs) > 1 else None,
    )
    teacher_evaluation_row = os_contracts.build_teacher_evaluation_row(
        experiment_id=experiment_id,
        hypothesis_id=_text(hypothesis.get("hypothesis_id")),
        target=target,
        observation_snapshot_hash=_text(observation_snapshot.get("observation_snapshot_hash")),
        strategy_judgement_hash=_text(strategy_judgement.get("strategy_judgement_hash")),
        realized_outcome_window=_teacher_outcome_window(
            confirmed_bar=confirmed_bar,
            future_bars=future_bars,
            teacher_horizon_bars=int(config["teacher_horizon_bars"]),
        ),
        lineage={
            "observation_contract_version": _text(config.get("observation_contract_version")),
            "primary_adapter_id": _text(config.get("primary_adapter_id")),
            "adapter_ids": list(config.get("adapter_ids") or []),
        },
        generated_at=generated_at,
    )
    return {
        "config": config,
        "observation_snapshot": observation_snapshot,
        "strategy_judgement": strategy_judgement,
        "teacher_evaluation_row": teacher_evaluation_row,
    }
