from __future__ import annotations

from shared.contracts.logic_artifacts import (
    PUBLISHED_LOGIC_ARTIFACT_FIELDS,
    PUBLISHED_LOGIC_MANIFEST_FIELDS,
    PUBLISHED_RANKING_SNAPSHOT_AUDIT_ROLE,
    PUBLISHED_RANKING_SNAPSHOT_FIELDS,
)
from shared.contracts.logic_selection import (
    DEFAULT_LOGIC_POINTER_NAME,
    LAST_KNOWN_GOOD_ARTIFACT_NAME,
    LOGIC_SELECTION_RESOLUTION_ORDER,
    SELECTED_LOGIC_OVERRIDE_NAME,
)
from shared.contracts.market_bars import CONFIRMED_MARKET_BAR_FIELDS, PROVISIONAL_INTRADAY_OVERLAY_FIELDS
from shared.contracts.ranking_output import RANKING_OUTPUT_FIELDS
from shared.market_semantics import is_confirmed_market_semantics
from shared.runtime_selection import resolve_runtime_logic_selection


def test_meemee_boundary_is_confirmed_only() -> None:
    assert is_confirmed_market_semantics(
        confirmation_state="confirmed",
        quality="confirmed",
        display_only=False,
    )
    assert not is_confirmed_market_semantics(
        confirmation_state="provisional",
        quality="provisional",
        display_only=True,
    )
    assert not is_confirmed_market_semantics(
        confirmation_state="confirmed",
        quality="confirmed",
        display_only=True,
    )


def test_meemee_contract_fields_are_frozen() -> None:
    assert CONFIRMED_MARKET_BAR_FIELDS == (
        "code",
        "market_date",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "source",
        "confirmation_state",
    )
    assert PROVISIONAL_INTRADAY_OVERLAY_FIELDS == (
        "code",
        "overlay_at",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "source",
        "display_only",
        "freshness_state",
        "fetched_at",
    )
    assert PUBLISHED_LOGIC_ARTIFACT_FIELDS == (
        "artifact_version",
        "logic_id",
        "logic_version",
        "logic_family",
        "feature_spec_version",
        "required_inputs",
        "scorer_type",
        "params",
        "thresholds",
        "weights",
        "output_spec",
        "checksum",
    )
    assert PUBLISHED_LOGIC_MANIFEST_FIELDS == (
        "logic_id",
        "logic_version",
        "logic_family",
        "status",
        "input_schema_version",
        "output_schema_version",
        "trained_at",
        "published_at",
        "artifact_uri",
        "checksum",
    )
    assert PUBLISHED_RANKING_SNAPSHOT_FIELDS == (
        "artifact_version",
        "logic_id",
        "logic_version",
        "logic_family",
        "as_of_date",
        "generated_at",
        "universe_size",
        "rows",
        "audit_role",
    )
    assert PUBLISHED_RANKING_SNAPSHOT_AUDIT_ROLE == "runtime_cache_audit_artifact"
    assert RANKING_OUTPUT_FIELDS[:5] == ("logic_id", "logic_version", "logic_family", "as_of_date", "code")


def test_runtime_selection_prefers_default_pointer_after_override() -> None:
    assert LOGIC_SELECTION_RESOLUTION_ORDER == (
        SELECTED_LOGIC_OVERRIDE_NAME,
        DEFAULT_LOGIC_POINTER_NAME,
        LAST_KNOWN_GOOD_ARTIFACT_NAME,
    )

    selection = resolve_runtime_logic_selection(
        selected_logic_override=None,
        default_logic_pointer="logic_family_a:v1",
        last_known_good="logic_family_a:v0",
        available_logic_keys=["logic_family_a:v1", "logic_family_a:v0"],
        safe_fallback_key="builtin:fallback",
    )

    assert selection["selected_logic_key"] == "logic_family_a:v1"
    assert selection["selected_source"] == DEFAULT_LOGIC_POINTER_NAME
    assert selection["matched_available"] is True
