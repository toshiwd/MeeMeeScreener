from __future__ import annotations

from typing import Literal, TypedDict


ChartClassification = Literal["confirmed", "provisional", "mixed"]
ChartAggregationSource = Literal["direct", "derived", "mixed"]
ChartSourceType = Literal["confirmed", "provisional", "mixed"]
ChartCacheSource = Literal["memory", "indexeddb"]
ChartDateMatchStatus = Literal["exact", "lagged_provisional", "blocked"]
ChartSourceFreshnessStatus = Literal["exact", "lagged", "stale_blocking"]
ChartDisplayBasisClassification = Literal["confirmed", "provisional", "mixed"]
ChartJudgmentBasisClassification = Literal["confirmed", "provisional", "dual"]
ChartOverwriteStatus = Literal[
    "authoritative_confirmed",
    "provisional_only",
    "provisional_replaced_by_confirmed",
]


class ChartDataProvenance(TypedDict, total=False):
    chart_source_provider: str
    chart_source_type: ChartSourceType
    chart_source_path_or_identifier: str
    chart_requested_date: int | None
    chart_last_confirmed_date: int | None
    chart_last_provisional_date: int | None
    chart_date_match_status: ChartDateMatchStatus
    chart_source_freshness_status: ChartSourceFreshnessStatus
    chart_data_classification: ChartClassification
    chart_aggregation_source: ChartAggregationSource
    confirmed_chart_source_provider: str | None
    provisional_chart_source_provider: str | None
    confirmed_judgment_basis: str | None
    provisional_judgment_basis: str | None
    confirmed_judgment_available: bool | None
    provisional_judgment_available: bool | None
    display_basis_classification: ChartDisplayBasisClassification | None
    judgment_basis_classification: ChartJudgmentBasisClassification | None
    confirmed_last_available_date: int | None
    provisional_last_available_date: int | None
    overwrite_status: ChartOverwriteStatus | None
    chart_cache_source: ChartCacheSource | None
    chart_cache_generated_at: str | None
    chart_cache_upstream_source_class: ChartClassification | None
    chart_cache_freshness_status: str | None


def build_chart_data_provenance(
    *,
    chart_source_provider: str,
    chart_source_type: ChartSourceType,
    chart_source_path_or_identifier: str,
    chart_requested_date: int | None,
    chart_last_confirmed_date: int | None,
    chart_last_provisional_date: int | None,
    chart_date_match_status: ChartDateMatchStatus,
    chart_source_freshness_status: ChartSourceFreshnessStatus,
    chart_data_classification: ChartClassification,
    chart_aggregation_source: ChartAggregationSource,
    confirmed_chart_source_provider: str | None = None,
    provisional_chart_source_provider: str | None = None,
    confirmed_judgment_basis: str | None = None,
    provisional_judgment_basis: str | None = None,
    confirmed_judgment_available: bool | None = None,
    provisional_judgment_available: bool | None = None,
    display_basis_classification: ChartDisplayBasisClassification | None = None,
    judgment_basis_classification: ChartJudgmentBasisClassification | None = None,
    confirmed_last_available_date: int | None = None,
    provisional_last_available_date: int | None = None,
    overwrite_status: ChartOverwriteStatus | None = None,
    chart_cache_source: ChartCacheSource | None = None,
    chart_cache_generated_at: str | None = None,
    chart_cache_upstream_source_class: ChartClassification | None = None,
    chart_cache_freshness_status: str | None = None,
) -> ChartDataProvenance:
    payload: ChartDataProvenance = {
        "chart_source_provider": chart_source_provider,
        "chart_source_type": chart_source_type,
        "chart_source_path_or_identifier": chart_source_path_or_identifier,
        "chart_requested_date": chart_requested_date,
        "chart_last_confirmed_date": chart_last_confirmed_date,
        "chart_last_provisional_date": chart_last_provisional_date,
        "chart_date_match_status": chart_date_match_status,
        "chart_source_freshness_status": chart_source_freshness_status,
        "chart_data_classification": chart_data_classification,
        "chart_aggregation_source": chart_aggregation_source,
    }
    if confirmed_chart_source_provider is not None:
        payload["confirmed_chart_source_provider"] = confirmed_chart_source_provider
    if provisional_chart_source_provider is not None:
        payload["provisional_chart_source_provider"] = provisional_chart_source_provider
    if confirmed_judgment_basis is not None:
        payload["confirmed_judgment_basis"] = confirmed_judgment_basis
    if provisional_judgment_basis is not None:
        payload["provisional_judgment_basis"] = provisional_judgment_basis
    if confirmed_judgment_available is not None:
        payload["confirmed_judgment_available"] = confirmed_judgment_available
    if provisional_judgment_available is not None:
        payload["provisional_judgment_available"] = provisional_judgment_available
    if display_basis_classification is not None:
        payload["display_basis_classification"] = display_basis_classification
    if judgment_basis_classification is not None:
        payload["judgment_basis_classification"] = judgment_basis_classification
    if confirmed_last_available_date is not None:
        payload["confirmed_last_available_date"] = confirmed_last_available_date
    if provisional_last_available_date is not None:
        payload["provisional_last_available_date"] = provisional_last_available_date
    if overwrite_status is not None:
        payload["overwrite_status"] = overwrite_status
    if chart_cache_source is not None:
        payload["chart_cache_source"] = chart_cache_source
    if chart_cache_generated_at is not None:
        payload["chart_cache_generated_at"] = chart_cache_generated_at
    if chart_cache_upstream_source_class is not None:
        payload["chart_cache_upstream_source_class"] = chart_cache_upstream_source_class
    if chart_cache_freshness_status is not None:
        payload["chart_cache_freshness_status"] = chart_cache_freshness_status
    return payload
