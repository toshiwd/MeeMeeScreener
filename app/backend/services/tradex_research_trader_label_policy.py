from __future__ import annotations

from pathlib import Path
from typing import Any, Final

from app.backend.services import tradex_research_os_contracts as os_contracts
from app.backend.services import tradex_research_os_store as os_store


TRADEX_TRADER_LABEL_POLICY_SCHEMA_VERSION: Final[str] = "tradex_trader_label_policy_v1"
TRADEX_TRADER_LABEL_POLICY_VERSION: Final[str] = "v1"
TRADEX_TRADER_LABEL_POLICY_FILE_NAME: Final[str] = "trader_label_policy_v1.json"


def _text(value: Any, fallback: str = "") -> str:
    if value is None:
        return fallback
    if isinstance(value, str):
        text = value.strip()
        return text or fallback
    text = str(value).strip()
    return text or fallback


def _float_or_none(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def trader_label_policy_path() -> Path:
    root = Path(__file__).resolve().parents[3]
    return root / "config" / "tradex" / TRADEX_TRADER_LABEL_POLICY_FILE_NAME


def load_trader_label_policy() -> dict[str, Any]:
    path = trader_label_policy_path()
    payload = os_store.read_json_object_strict(path, artifact_name="trader label policy")
    if _text(payload.get("schema_version")) != TRADEX_TRADER_LABEL_POLICY_SCHEMA_VERSION:
        raise ValueError("trader label policy schema_version mismatch")
    if _text(payload.get("label_policy_version")) != TRADEX_TRADER_LABEL_POLICY_VERSION:
        raise ValueError("trader label policy version mismatch")
    if not isinstance(payload.get("horizon"), dict):
        raise ValueError("trader label policy horizon must be an object")
    if not isinstance(payload.get("thresholds"), dict):
        raise ValueError("trader label policy thresholds must be an object")
    if not isinstance(payload.get("outcome_classes"), dict):
        raise ValueError("trader label policy outcome_classes must be an object")
    return payload


def apply_trader_label_policy(row: dict[str, Any], *, policy: dict[str, Any] | None = None) -> dict[str, Any]:
    loaded_policy = policy or load_trader_label_policy()
    horizon = loaded_policy.get("horizon") if isinstance(loaded_policy.get("horizon"), dict) else {}
    thresholds = loaded_policy.get("thresholds") if isinstance(loaded_policy.get("thresholds"), dict) else {}
    expected_horizon_bars = int(horizon.get("teacher_horizon_bars") or 0)
    required_future_bar_count = int(horizon.get("required_future_bar_count") or expected_horizon_bars)
    complete_horizon = bool(row.get("complete_horizon"))
    teacher_horizon_bars = int(row.get("teacher_horizon_bars") or 0)
    future_bar_count = int(row.get("future_bar_count") or 0)

    if not complete_horizon:
        return {
            "close_positive_20": None,
            "next_open_positive_20": None,
            "mfe_ge_10pct_20": None,
            "mae_worse_than_7pct_20": None,
            "judgement_outcome_class": "incomplete",
            "label_policy_version": TRADEX_TRADER_LABEL_POLICY_VERSION,
        }

    if teacher_horizon_bars != expected_horizon_bars:
        raise ValueError(f"teacher_horizon_bars mismatch: expected {expected_horizon_bars}, got {teacher_horizon_bars}")
    if future_bar_count < required_future_bar_count:
        raise ValueError(f"future_bar_count below required horizon: expected >= {required_future_bar_count}, got {future_bar_count}")

    return_close_basis = _float_or_none(row.get("return_close_basis"))
    return_next_open_basis = _float_or_none(row.get("return_next_open_basis"))
    mfe = _float_or_none(row.get("max_favorable_excursion_close_basis"))
    mae = _float_or_none(row.get("max_adverse_excursion_close_basis"))
    missing_fields = [
        field_name
        for field_name, value in (
            ("return_close_basis", return_close_basis),
            ("return_next_open_basis", return_next_open_basis),
            ("max_favorable_excursion_close_basis", mfe),
            ("max_adverse_excursion_close_basis", mae),
        )
        if value is None
    ]
    if missing_fields:
        raise ValueError(f"label inputs incomplete: {', '.join(missing_fields)}")

    close_positive = return_close_basis > float(thresholds.get("close_positive_min_return") or 0.0)
    next_open_positive = return_next_open_basis > float(thresholds.get("next_open_positive_min_return") or 0.0)
    mfe_ge_10pct = mfe >= float(thresholds.get("mfe_ge_threshold") or 0.10)
    mae_worse_than_7pct = mae <= float(thresholds.get("mae_worse_than_threshold") or -0.07)

    if close_positive and not mae_worse_than_7pct:
        outcome_class = _text((loaded_policy.get("outcome_classes") or {}).get("good"), fallback="good")
    elif (not close_positive) or mae_worse_than_7pct:
        outcome_class = _text((loaded_policy.get("outcome_classes") or {}).get("bad"), fallback="bad")
    else:
        outcome_class = _text((loaded_policy.get("outcome_classes") or {}).get("mixed"), fallback="mixed")

    if outcome_class not in os_contracts.TRADEX_TRADER_JUDGEMENT_OUTCOME_CLASSES:
        raise ValueError("judgement_outcome_class is invalid")

    return {
        "close_positive_20": close_positive,
        "next_open_positive_20": next_open_positive,
        "mfe_ge_10pct_20": mfe_ge_10pct,
        "mae_worse_than_7pct_20": mae_worse_than_7pct,
        "judgement_outcome_class": outcome_class,
        "label_policy_version": TRADEX_TRADER_LABEL_POLICY_VERSION,
    }
