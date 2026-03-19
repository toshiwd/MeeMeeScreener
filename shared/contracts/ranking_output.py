from __future__ import annotations

from typing import Any, Final, NotRequired, TypedDict

RANKING_OUTPUT_FIELDS: Final[tuple[str, ...]] = (
    "logic_id",
    "logic_version",
    "logic_family",
    "as_of_date",
    "code",
    "side",
    "rank_position",
    "ranking_score",
    "candidate_score",
    "reason_codes",
    "freshness_state",
    "generated_at",
    "metadata",
)


class RankingOutputRow(TypedDict):
    logic_id: str
    logic_version: str
    logic_family: str
    as_of_date: str
    code: str
    side: str
    rank_position: int
    ranking_score: float
    candidate_score: NotRequired[float | None]
    reason_codes: list[str]
    freshness_state: str
    generated_at: NotRequired[str]
    metadata: NotRequired[dict[str, Any]]
