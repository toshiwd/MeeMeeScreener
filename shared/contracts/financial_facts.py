from __future__ import annotations

from typing import Any, Final, NotRequired, TypedDict

FINANCIAL_FACT_FIELDS: Final[tuple[str, ...]] = (
    "code",
    "source",
    "report_date",
    "period_key",
    "fact_key",
    "value",
    "unit",
    "freshness_state",
)
FINANCIAL_FACTS_BUNDLE_FIELDS: Final[tuple[str, ...]] = (
    "source",
    "code",
    "report_date",
    "facts",
)


class FinancialFact(TypedDict):
    code: str
    source: str
    report_date: str
    period_key: str
    fact_key: str
    value: Any
    unit: NotRequired[str]
    freshness_state: NotRequired[str]


class FinancialFactsBundle(TypedDict):
    source: str
    code: str
    report_date: str
    facts: list[FinancialFact]
