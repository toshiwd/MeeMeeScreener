from __future__ import annotations

from shared.runtime_selection import resolve_runtime_logic_selection


def test_resolves_override_first() -> None:
    result = resolve_runtime_logic_selection(
        selected_logic_override="logic_override_v2",
        default_logic_pointer="logic_default_v1",
        last_known_good="logic_lkg_v1",
        available_logic_keys=["logic_override_v2", "logic_default_v1", "logic_lkg_v1"],
        safe_fallback_key="logic_safe",
    )
    assert result["selected_source"] == "selected_logic_override"
    assert result["selected_logic_key"] == "logic_override_v2"


def test_falls_back_to_default_then_last_known_good() -> None:
    default_result = resolve_runtime_logic_selection(
        selected_logic_override="missing_override",
        default_logic_pointer="logic_default_v1",
        last_known_good="logic_lkg_v1",
        available_logic_keys=["logic_default_v1", "logic_lkg_v1"],
        safe_fallback_key="logic_safe",
    )
    assert default_result["selected_source"] == "default_logic_pointer"
    assert default_result["selected_logic_key"] == "logic_default_v1"

    lkg_result = resolve_runtime_logic_selection(
        selected_logic_override="missing_override",
        default_logic_pointer="missing_default",
        last_known_good="logic_lkg_v1",
        available_logic_keys=["logic_lkg_v1"],
        safe_fallback_key="logic_safe",
    )
    assert lkg_result["selected_source"] == "last_known_good"
    assert lkg_result["selected_logic_key"] == "logic_lkg_v1"


def test_uses_safe_fallback_when_nothing_matches() -> None:
    result = resolve_runtime_logic_selection(
        selected_logic_override="missing_override",
        default_logic_pointer="missing_default",
        last_known_good="missing_lkg",
        available_logic_keys=[],
        safe_fallback_key="logic_safe",
    )
    assert result["selected_source"] == "safe_fallback"
    assert result["selected_logic_key"] == "logic_safe"

