from __future__ import annotations

from typing import Final, TypedDict

DEFAULT_LOGIC_POINTER_NAME: Final[str] = "default_logic_pointer"
SELECTED_LOGIC_OVERRIDE_NAME: Final[str] = "selected_logic_override"
LAST_KNOWN_GOOD_ARTIFACT_NAME: Final[str] = "last_known_good"
LOGIC_SELECTION_RESOLUTION_ORDER: Final[tuple[str, ...]] = (
    SELECTED_LOGIC_OVERRIDE_NAME,
    DEFAULT_LOGIC_POINTER_NAME,
    LAST_KNOWN_GOOD_ARTIFACT_NAME,
)
LOGIC_SELECTION_STATE_FIELDS: Final[tuple[str, ...]] = (
    SELECTED_LOGIC_OVERRIDE_NAME,
    DEFAULT_LOGIC_POINTER_NAME,
    LAST_KNOWN_GOOD_ARTIFACT_NAME,
)


class LogicSelectionState(TypedDict):
    selected_logic_override: str | None
    default_logic_pointer: str | None
    last_known_good: str | None
