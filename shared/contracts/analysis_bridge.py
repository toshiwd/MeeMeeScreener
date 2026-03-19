from __future__ import annotations

from typing import Final

ALLOWED_PUBLIC_TABLES: Final[tuple[str, ...]] = (
    "publish_pointer",
    "publish_manifest",
    "candidate_daily",
    "state_eval_daily",
    "similar_cases_daily",
    "similar_case_paths",
    "regime_daily",
)
PUBLIC_TABLE_COLUMNS: Final[dict[str, tuple[str, ...]]] = {
    "candidate_daily": (
        "publish_id",
        "as_of_date",
        "code",
        "side",
        "rank_position",
        "candidate_score",
        "expected_horizon_days",
        "primary_reason_codes",
        "regime_tag",
        "freshness_state",
    ),
    "regime_daily": (
        "publish_id",
        "as_of_date",
        "regime_tag",
        "regime_score",
        "breadth_score",
        "volatility_state",
    ),
    "state_eval_daily": (
        "publish_id",
        "as_of_date",
        "code",
        "side",
        "holding_band",
        "strategy_tags",
        "state_action",
        "decision_3way",
        "confidence",
        "reason_codes",
        "reason_text_top3",
        "freshness_state",
    ),
    "similar_cases_daily": (
        "publish_id",
        "as_of_date",
        "code",
        "query_type",
        "query_anchor_type",
        "neighbor_rank",
        "case_id",
        "neighbor_code",
        "neighbor_anchor_date",
        "case_type",
        "outcome_class",
        "success_flag",
        "similarity_score",
        "reason_codes",
    ),
    "similar_case_paths": (
        "publish_id",
        "as_of_date",
        "code",
        "case_id",
        "rel_day",
        "path_return_norm",
        "path_volume_norm",
    ),
}
LATEST_POINTER_NAME: Final[str] = "latest_successful"
MAX_PUBLIC_SIMILAR_CASE_ROWS: Final[int] = 5
MAX_PUBLIC_SIMILAR_PATH_ROWS: Final[int] = 20
DEGRADE_REASON_NO_PUBLISH: Final[str] = "no_latest_successful_publish"
DEGRADE_REASON_WARNING_STALE: Final[str] = "warning_stale"
DEGRADE_REASON_HARD_STALE: Final[str] = "hard_stale"
DEGRADE_REASON_POINTER_CORRUPTION: Final[str] = "pointer_corruption"
DEGRADE_REASON_MANIFEST_MISMATCH: Final[str] = "manifest_mismatch"
DEGRADE_REASON_SCHEMA_MISMATCH: Final[str] = "schema_mismatch"
DEGRADE_REASON_RESULT_DB_MISSING: Final[str] = "result_db_missing"
DEGRADE_REASON_REGIME_ROW_CORRUPTION: Final[str] = "regime_row_corruption"


def is_allowed_public_table(table_name: str) -> bool:
    return str(table_name) in ALLOWED_PUBLIC_TABLES


def allowed_public_columns(table_name: str) -> tuple[str, ...]:
    return PUBLIC_TABLE_COLUMNS.get(str(table_name), ())

