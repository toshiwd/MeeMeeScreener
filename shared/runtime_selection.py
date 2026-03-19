from __future__ import annotations

from typing import Final, Iterable, NotRequired, TypedDict

from shared.contracts.logic_selection import (
    DEFAULT_LOGIC_POINTER_NAME,
    LAST_KNOWN_GOOD_ARTIFACT_NAME,
    LOGIC_SELECTION_RESOLUTION_ORDER,
    SELECTED_LOGIC_OVERRIDE_NAME,
)

SAFE_FALLBACK_SOURCE: Final[str] = "safe_fallback"
UNRESOLVED_SOURCE: Final[str] = "unresolved"


class LogicSelectionResolution(TypedDict):
    selected_logic_key: str | None
    selected_source: str
    selected_pointer_name: str | None
    matched_available: bool
    notes: list[str]
    override_key: NotRequired[str | None]
    default_key: NotRequired[str | None]
    last_known_good_key: NotRequired[str | None]
    safe_fallback_key: NotRequired[str | None]


def _normalize_key(value: str | None) -> str | None:
    text = str(value or "").strip()
    return text or None


def resolve_runtime_logic_selection(
    *,
    selected_logic_override: str | None,
    default_logic_pointer: str | None,
    last_known_good: str | None,
    available_logic_keys: Iterable[str] = (),
    safe_fallback_key: str | None = None,
) -> LogicSelectionResolution:
    """
    Resolve runtime logic selection by explicit order only.

    Resolution order:
    selected_logic_override -> default_logic_pointer -> last_known_good -> safe_fallback.
    """

    available = {_normalize_key(key) for key in available_logic_keys}
    available.discard(None)

    key_by_source = {
        SELECTED_LOGIC_OVERRIDE_NAME: _normalize_key(selected_logic_override),
        DEFAULT_LOGIC_POINTER_NAME: _normalize_key(default_logic_pointer),
        LAST_KNOWN_GOOD_ARTIFACT_NAME: _normalize_key(last_known_good),
        SAFE_FALLBACK_SOURCE: _normalize_key(safe_fallback_key),
    }
    candidates: list[tuple[str, str | None]] = [
        (source, key_by_source[source]) for source in LOGIC_SELECTION_RESOLUTION_ORDER
    ]
    candidates.append((SAFE_FALLBACK_SOURCE, key_by_source[SAFE_FALLBACK_SOURCE]))

    for source, candidate_key in candidates:
        if candidate_key is None:
            continue
        if source != SAFE_FALLBACK_SOURCE and candidate_key not in available:
            continue
        return LogicSelectionResolution(
            selected_logic_key=candidate_key,
            selected_source=source,
            selected_pointer_name=source if source != SAFE_FALLBACK_SOURCE else None,
            matched_available=candidate_key in available,
            notes=[],
            override_key=_normalize_key(selected_logic_override),
            default_key=_normalize_key(default_logic_pointer),
            last_known_good_key=_normalize_key(last_known_good),
            safe_fallback_key=_normalize_key(safe_fallback_key),
        )

    return LogicSelectionResolution(
        selected_logic_key=None,
        selected_source=UNRESOLVED_SOURCE,
        selected_pointer_name=None,
        matched_available=False,
        notes=["no_logic_available"],
        override_key=_normalize_key(selected_logic_override),
        default_key=_normalize_key(default_logic_pointer),
        last_known_good_key=_normalize_key(last_known_good),
        safe_fallback_key=_normalize_key(safe_fallback_key),
    )
