from __future__ import annotations

from typing import Any, Final

_CONFIRMED_CONFIRMATION_STATES: Final[frozenset[str]] = frozenset(
    {
        "confirmed",
        "final",
        "settled",
        "closed",
    }
)
_PROVISIONAL_CONFIRMATION_STATES: Final[frozenset[str]] = frozenset(
    {
        "provisional",
        "pending",
        "tentative",
        "intraday",
        "unconfirmed",
        "draft",
    }
)
_CONFIRMED_QUALITY_VALUES: Final[frozenset[str]] = frozenset(
    {
        "confirmed",
        "high",
        "good",
        "usable",
        "final",
    }
)
_PROVISIONAL_QUALITY_VALUES: Final[frozenset[str]] = frozenset(
    {
        "provisional",
        "draft",
        "low",
        "intraday",
        "display_only",
    }
)


def _normalize_text(value: Any) -> str:
    return str(value or "").strip().lower()


def _coerce_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return _normalize_text(value) in {"1", "true", "yes", "on"}


def is_confirmed_market_semantics(
    *,
    confirmation_state: Any = None,
    quality: Any = None,
    display_only: Any = None,
) -> bool:
    """Return True only for confirmed, non-display-only market semantics."""

    if _coerce_bool(display_only):
        return False

    confirmation = _normalize_text(confirmation_state)
    if confirmation in _PROVISIONAL_CONFIRMATION_STATES:
        return False
    if confirmation in _CONFIRMED_CONFIRMATION_STATES:
        return True

    quality_value = _normalize_text(quality)
    if quality_value in _PROVISIONAL_QUALITY_VALUES:
        return False
    if quality_value in _CONFIRMED_QUALITY_VALUES:
        return True

    return False

