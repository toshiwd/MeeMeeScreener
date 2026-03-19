from __future__ import annotations

from app.backend.services.analysis_bridge import contracts as app_contracts
from shared.contracts.analysis_bridge import (
    ALLOWED_PUBLIC_TABLES,
    LATEST_POINTER_NAME,
    allowed_public_columns,
    is_allowed_public_table,
)
from shared.contracts.financial_facts import FINANCIAL_FACT_FIELDS, FINANCIAL_FACTS_BUNDLE_FIELDS
from shared.contracts.logic_artifacts import (
    PUBLISHED_LOGIC_ARTIFACT_FIELDS,
    PUBLISHED_LOGIC_MANIFEST_FIELDS,
    PUBLISHED_RANKING_SNAPSHOT_AUDIT_ROLE,
    PUBLISHED_RANKING_SNAPSHOT_FIELDS,
    VALIDATION_SUMMARY_FIELDS,
)
from shared.contracts.logic_selection import (
    DEFAULT_LOGIC_POINTER_NAME,
    LAST_KNOWN_GOOD_ARTIFACT_NAME,
    LOGIC_SELECTION_RESOLUTION_ORDER,
    LOGIC_SELECTION_STATE_FIELDS,
    SELECTED_LOGIC_OVERRIDE_NAME,
)
from shared.contracts.market_bars import (
    CONFIRMED_MARKET_BAR_FIELDS,
    PROVISIONAL_INTRADAY_OVERLAY_FIELDS,
)
from shared.contracts.ranking_output import RANKING_OUTPUT_FIELDS
from shared.contracts.trade_history import NORMALIZED_TRADE_HISTORY_FIELDS


def test_analysis_bridge_contract_alias_matches_shared_contract() -> None:
    assert app_contracts.ALLOWED_PUBLIC_TABLES == ALLOWED_PUBLIC_TABLES
    assert app_contracts.LATEST_POINTER_NAME == LATEST_POINTER_NAME
    assert is_allowed_public_table("candidate_daily") is True
    assert allowed_public_columns("candidate_daily") == app_contracts.PUBLIC_TABLE_COLUMNS["candidate_daily"]


def test_published_logic_artifact_contract_is_declarative() -> None:
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
    assert VALIDATION_SUMMARY_FIELDS == (
        "logic_id",
        "logic_version",
        "logic_family",
        "evaluation_scope",
        "decision",
        "champion_logic_version",
        "challenger_logic_version",
        "metrics",
        "notes",
        "created_at",
    )


def test_runtime_selection_contract_is_explicit() -> None:
    assert LOGIC_SELECTION_RESOLUTION_ORDER == (
        SELECTED_LOGIC_OVERRIDE_NAME,
        DEFAULT_LOGIC_POINTER_NAME,
        LAST_KNOWN_GOOD_ARTIFACT_NAME,
    )
    assert LOGIC_SELECTION_STATE_FIELDS == (
        "selected_logic_override",
        "default_logic_pointer",
        "last_known_good",
    )


def test_shared_market_and_trade_contract_fields_are_frozen() -> None:
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
    assert "source" in PROVISIONAL_INTRADAY_OVERLAY_FIELDS
    assert "display_only" in PROVISIONAL_INTRADAY_OVERLAY_FIELDS
    assert NORMALIZED_TRADE_HISTORY_FIELDS == (
        "broker",
        "trade_datetime",
        "code",
        "side",
        "quantity",
        "price",
        "fees",
        "taxes",
        "raw_ref",
    )
    assert FINANCIAL_FACT_FIELDS == (
        "code",
        "source",
        "report_date",
        "period_key",
        "fact_key",
        "value",
        "unit",
        "freshness_state",
    )
    assert FINANCIAL_FACTS_BUNDLE_FIELDS == ("source", "code", "report_date", "facts")
    assert RANKING_OUTPUT_FIELDS[:5] == ("logic_id", "logic_version", "logic_family", "as_of_date", "code")
