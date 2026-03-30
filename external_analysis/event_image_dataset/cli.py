from __future__ import annotations

from typing import Any

from external_analysis.event_image_dataset.analysis import (
    analyze_event_image_dataset_regime,
    build_event_image_pattern_adoption_compare,
    build_event_image_pattern_adoption_policy,
    build_event_image_pattern_adoption,
    build_event_image_pattern_breadth,
    build_event_image_pattern_gating,
    build_event_image_pattern_playbook,
    build_event_image_pattern_playbook_relax_compare,
    build_event_image_pattern_playbook_threshold_compare,
    build_event_image_pattern_veto_compare,
    build_event_image_pattern_veto_ablation,
    build_event_image_pattern_veto_thin_liquidity_compare,
    build_event_image_pattern_selection_contract,
    build_event_image_pattern_combo_rules,
    build_event_image_pattern_library,
    build_event_image_pattern_boundary,
    build_event_image_pattern_sequence_combo,
    build_event_image_rebound_live_monitor,
    decompose_event_image_pattern,
    run_event_image_dataset_rebound_v3_round,
    run_event_image_dataset_rebound_multi_research_round,
    run_event_image_dataset_robustness_batch,
    run_event_image_dataset_analysis_batch,
)
from external_analysis.event_image_dataset.build import build_event_image_dataset
from external_analysis.event_image_dataset.restricted_universe import build_meemee_registered_sample_universe
from external_analysis.event_image_dataset.train import run_event_image_dataset_repro, train_event_image_dataset


def run_event_image_dataset_build(
    *,
    export_db_path: str,
    dataset_id: str,
    source_db_path: str | None = None,
    start_month: str | int | None = None,
    end_month: str | int | None = None,
    renderer_backend: str = "agg",
    restricted_universe_path: str | None = None,
) -> dict[str, Any]:
    return build_event_image_dataset(
        export_db_path=export_db_path,
        dataset_id=dataset_id,
        source_db_path=source_db_path,
        start_month=start_month,
        end_month=end_month,
        renderer_backend=renderer_backend,
        restricted_universe_path=restricted_universe_path,
    )


def run_event_image_dataset_train(
    *,
    dataset_id: str,
    seed: int = 42,
    feature_size: int = 48,
) -> dict[str, Any]:
    return train_event_image_dataset(
        dataset_id=dataset_id,
        seed=seed,
        feature_size=feature_size,
    )


def run_event_image_dataset_repro_cli(
    *,
    dataset_id: str,
    seeds: list[int] | tuple[int, ...] | None = None,
    feature_size: int = 48,
) -> dict[str, Any]:
    return run_event_image_dataset_repro(
        dataset_id=dataset_id,
        seeds=seeds,
        feature_size=feature_size,
    )


def run_event_image_dataset_regime_cli(
    *,
    dataset_id: str,
) -> dict[str, Any]:
    return analyze_event_image_dataset_regime(dataset_id=dataset_id)


def run_event_image_dataset_analysis_batch_cli(
    *,
    dataset_ids: list[str] | tuple[str, ...],
    max_workers: int | None = None,
    refresh_train: bool = False,
    refresh_repro: bool = False,
    feature_size: int = 48,
    seeds: list[int] | tuple[int, ...] | None = None,
) -> dict[str, Any]:
    return run_event_image_dataset_analysis_batch(
        dataset_ids=dataset_ids,
        max_workers=max_workers,
        refresh_train=refresh_train,
        refresh_repro=refresh_repro,
        feature_size=feature_size,
        seeds=seeds,
    )


def run_event_image_dataset_pattern_cli(
    *,
    dataset_id: str,
    regime_tag: str = "rebound_onset",
) -> dict[str, Any]:
    return decompose_event_image_pattern(
        dataset_id=dataset_id,
        regime_tag=regime_tag,
    )


def run_event_image_dataset_pattern_library_cli(
    *,
    dataset_id: str,
    regime_tags: list[str] | tuple[str, ...],
) -> dict[str, Any]:
    return build_event_image_pattern_library(
        dataset_id=dataset_id,
        regime_tags=regime_tags,
    )


def run_event_image_dataset_boundary_cli(
    *,
    dataset_id: str,
    primary_regime: str,
    comparison_regime: str,
    max_workers: int = 2,
) -> dict[str, Any]:
    return build_event_image_pattern_boundary(
        dataset_id=dataset_id,
        primary_regime=primary_regime,
        comparison_regime=comparison_regime,
        max_workers=max_workers,
    )


def run_event_image_dataset_gating_cli(
    *,
    dataset_id: str,
    primary_regime: str,
    comparison_regime: str,
) -> dict[str, Any]:
    return build_event_image_pattern_gating(
        dataset_id=dataset_id,
        primary_regime=primary_regime,
        comparison_regime=comparison_regime,
    )


def run_event_image_dataset_combo_cli(
    *,
    dataset_id: str,
    primary_regime: str,
    comparison_regime: str,
) -> dict[str, Any]:
    return build_event_image_pattern_combo_rules(
        dataset_id=dataset_id,
        primary_regime=primary_regime,
        comparison_regime=comparison_regime,
    )


def run_event_image_dataset_adoption_cli(
    *,
    dataset_id: str,
    pattern: str = "rebound_onset",
) -> dict[str, Any]:
    return build_event_image_pattern_adoption(
        dataset_id=dataset_id,
        pattern=pattern,
    )


def run_event_image_dataset_adoption_compare_cli(
    *,
    dataset_id: str,
    pattern: str = "rebound_onset",
) -> dict[str, Any]:
    return build_event_image_pattern_adoption_compare(
        dataset_id=dataset_id,
        pattern=pattern,
    )


def run_event_image_dataset_pattern_breadth_cli(
    *,
    dataset_id: str,
    pattern: str = "rebound_onset",
) -> dict[str, Any]:
    return build_event_image_pattern_breadth(
        dataset_id=dataset_id,
        pattern=pattern,
    )


def run_event_image_dataset_sequence_combo_cli(
    *,
    dataset_id: str,
    pattern: str = "rebound_onset",
) -> dict[str, Any]:
    return build_event_image_pattern_sequence_combo(
        dataset_id=dataset_id,
        pattern=pattern,
    )


def run_event_image_dataset_adoption_policy_cli(
    *,
    dataset_id: str,
    pattern: str = "rebound_onset",
) -> dict[str, Any]:
    return build_event_image_pattern_adoption_policy(
        dataset_id=dataset_id,
        pattern=pattern,
    )


def run_event_image_dataset_playbook_cli(
    *,
    dataset_id: str,
    pattern: str = "rebound_onset",
) -> dict[str, Any]:
    return build_event_image_pattern_playbook(
        dataset_id=dataset_id,
        pattern=pattern,
    )


def run_event_image_dataset_playbook_relax_cli(
    *,
    dataset_id: str,
    pattern: str = "rebound_onset",
) -> dict[str, Any]:
    return build_event_image_pattern_playbook_relax_compare(
        dataset_id=dataset_id,
        pattern=pattern,
    )


def run_event_image_dataset_playbook_threshold_cli(
    *,
    dataset_id: str,
    pattern: str = "rebound_onset",
) -> dict[str, Any]:
    return build_event_image_pattern_playbook_threshold_compare(
        dataset_id=dataset_id,
        pattern=pattern,
    )


def run_event_image_dataset_veto_compare_cli(
    *,
    dataset_id: str,
    pattern: str = "rebound_onset",
) -> dict[str, Any]:
    return build_event_image_pattern_veto_compare(
        dataset_id=dataset_id,
        pattern=pattern,
    )


def run_event_image_dataset_veto_ablation_cli(
    *,
    dataset_id: str,
    pattern: str = "rebound_onset",
) -> dict[str, Any]:
    return build_event_image_pattern_veto_ablation(
        dataset_id=dataset_id,
        pattern=pattern,
    )


def run_event_image_dataset_veto_thin_liquidity_cli(
    *,
    dataset_id: str,
    pattern: str = "rebound_onset",
) -> dict[str, Any]:
    return build_event_image_pattern_veto_thin_liquidity_compare(
        dataset_id=dataset_id,
        pattern=pattern,
    )


def run_event_image_dataset_selection_contract_cli(
    *,
    dataset_id: str,
    pattern: str = "rebound_onset",
) -> dict[str, Any]:
    return build_event_image_pattern_selection_contract(
        dataset_id=dataset_id,
        pattern=pattern,
    )


def run_event_image_dataset_rebound_monitor_cli(
    *,
    dataset_id: str,
    days: int = 60,
) -> dict[str, Any]:
    return build_event_image_rebound_live_monitor(
        dataset_id=dataset_id,
        days=days,
    )


def run_event_image_dataset_robustness_batch_cli(
    *,
    dataset_ids: list[str] | tuple[str, ...],
    pattern: str = "rebound_onset",
    reference_dataset_id: str | None = None,
    max_workers: int | None = None,
) -> dict[str, Any]:
    return run_event_image_dataset_robustness_batch(
        dataset_ids=dataset_ids,
        pattern=pattern,
        reference_dataset_id=reference_dataset_id,
        max_workers=max_workers,
    )


def run_event_image_dataset_rebound_multi_research_cli(
    *,
    dataset_id: str,
    robustness_dataset_ids: list[str] | tuple[str, ...],
    max_workers: int = 2,
) -> dict[str, Any]:
    return run_event_image_dataset_rebound_multi_research_round(
        dataset_id=dataset_id,
        robustness_dataset_ids=robustness_dataset_ids,
        max_workers=max_workers,
    )


def run_event_image_dataset_rebound_v3_cli(
    *,
    dataset_id: str,
    max_workers: int = 4,
    monitor_days: int = 60,
    diagnosis_start_date: str | None = None,
    diagnosis_end_date: str | None = None,
) -> dict[str, Any]:
    return run_event_image_dataset_rebound_v3_round(
        dataset_id=dataset_id,
        max_workers=max_workers,
        monitor_days=monitor_days,
        diagnosis_start_date=diagnosis_start_date,
        diagnosis_end_date=diagnosis_end_date,
    )


def run_event_image_dataset_universe_build(
    *,
    source_db_path: str | None = None,
    output_path: str | None = None,
    start_month: str | int | None = None,
    end_month: str | int | None = None,
    sample_size: int = 100,
    sample_seed: int = 7,
) -> dict[str, Any]:
    if start_month is None or end_month is None:
        raise RuntimeError("start_month and end_month are required for restricted-universe build")
    return build_meemee_registered_sample_universe(
        source_db_path=source_db_path,
        output_path=output_path,
        start_month=start_month,
        end_month=end_month,
        sample_size=sample_size,
        sample_seed=sample_seed,
    )
