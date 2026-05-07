from __future__ import annotations

from pathlib import Path

from scripts.tradex_feature_complete_high_recall_surface_v1 import (
    FILTER_TAG,
    _base_surface_summary,
    _build_filtered_base_surface,
)


def test_feature_complete_high_recall_base_surface_contract() -> None:
    risk_filter_session = Path(r"G:\Tradex\risk_flag_filter_before_high_recall_surface_v1\20260502T125847Z-880922")
    min_pool_session = Path(r"G:\Tradex\side_aware_min_pool_feasibility_v1\20260502T114737Z-145239")
    base = _build_filtered_base_surface(
        risk_filter_session=risk_filter_session,
        min_pool_session=min_pool_session,
    )
    summary = _base_surface_summary(
        base,
        risk_filter_session=risk_filter_session,
        min_pool_session=min_pool_session,
    )

    assert summary["filter_variant"] == FILTER_TAG
    assert summary["row_count"] == 1329
    assert summary["group_count"] == 267
    assert base["risk_filter_variant"].eq(FILTER_TAG).all()
    assert base["selected_for_high_recall_surface"].all()
    assert base[["anchor_date", "symbol", "side"]].drop_duplicates().shape[0] == 1329
    assert "candidate_idx" in base.columns
    assert "included_by_filter_reason" in base.columns
