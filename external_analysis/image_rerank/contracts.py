from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any


SCHEMA_VERSION = "tradex_image_rerank_run_v1"
SPLIT_SCHEMA_VERSION = "tradex_image_rerank_split_v1"
LABEL_SCHEMA_VERSION = "tradex_image_rerank_label_v1"
RENDER_SCHEMA_VERSION = "tradex_image_rerank_render_v1"
BASE_SCORE_SCHEMA_VERSION = "tradex_image_rerank_base_score_v1"
PHASE2_METRICS_SCHEMA_VERSION = "tradex_image_rerank_phase2_metrics_v1"
PHASE3_COMPARE_SCHEMA_VERSION = "tradex_image_rerank_phase3_compare_v1"
MODEL_SCHEMA_VERSION = "tradex_image_rerank_model_v1"


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


@dataclass(frozen=True)
class ImageRerankJobConfig:
    run_id: str
    export_db_path: str
    as_of_snapshot_date: int
    verify_profile: str = "smoke"
    top_k: int = 10
    block_size_days: int = 30
    embargo_days: int = 20
    feature_lookback_days: int = 80
    label_horizon_days: int = 20
    positive_quantile: float = 0.85
    negative_quantile: float = 0.15
    neutral_weight: float = 0.25
    base_weight: float = 0.70
    image_weight: float = 0.30
    renderer_backend: str = "auto"


def build_run_manifest(
    *,
    config: ImageRerankJobConfig,
    candidate_universe_hash: str,
    base_score_artifact_uri: str,
    base_score_artifact_checksum: str,
    split_artifact_uri: str,
    label_artifact_uri: str,
    render_artifact_uri: str,
    phase2_metrics_artifact_uri: str | None,
    phase3_compare_artifact_uri: str | None,
    status: str,
    counts: dict[str, Any],
    artifacts: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "run_id": config.run_id,
        "created_at": utc_now_iso(),
        "status": status,
        "verify_profile": str(config.verify_profile),
        "as_of_snapshot_date": int(config.as_of_snapshot_date),
        "candidate_universe_hash": candidate_universe_hash,
        "block_size_days": int(config.block_size_days),
        "embargo_days": int(config.embargo_days),
        "purge_rule": {
            "name": "time-block split + purge + embargo",
            "basis": "trading_day_index",
            "protected_block_rule": "boundary checks are anchored to the protected block",
            "feature_window_overlap_check": True,
            "feature_lookback_days": int(config.feature_lookback_days),
            "label_horizon_days": int(config.label_horizon_days),
            "embargo_days": int(config.embargo_days),
            "mechanism": "purge rows whose feature window, label horizon, or embargo band intersects the protected block",
        },
        "feature_lookback_days": int(config.feature_lookback_days),
        "label_horizon_days": int(config.label_horizon_days),
        "base_score_artifact_uri": base_score_artifact_uri,
        "base_score_artifact_checksum": base_score_artifact_checksum,
        "split_artifact_uri": split_artifact_uri,
        "label_artifact_uri": label_artifact_uri,
        "render_artifact_uri": render_artifact_uri,
        "phase2_metrics_artifact_uri": phase2_metrics_artifact_uri,
        "phase3_compare_artifact_uri": phase3_compare_artifact_uri,
        "artifacts": artifacts,
        "counts": counts,
    }
