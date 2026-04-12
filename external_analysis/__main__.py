from __future__ import annotations

import argparse
import json

from external_analysis.event_image_dataset.cli import (
    run_event_image_dataset_adoption_cli,
    run_event_image_dataset_adoption_compare_cli,
    run_event_image_dataset_adoption_policy_cli,
    run_event_image_dataset_analysis_batch_cli,
    run_event_image_dataset_boundary_cli,
    run_event_image_dataset_build,
    run_event_image_dataset_combo_cli,
    run_event_image_dataset_gating_cli,
    run_event_image_dataset_pattern_breadth_cli,
    run_event_image_dataset_pattern_library_cli,
    run_event_image_dataset_pattern_cli,
    run_event_image_dataset_playbook_cli,
    run_event_image_dataset_playbook_relax_cli,
    run_event_image_dataset_playbook_threshold_cli,
    run_event_image_dataset_regime_cli,
    run_event_image_dataset_rebound_monitor_cli,
    run_event_image_dataset_rebound_multi_research_cli,
    run_event_image_dataset_rebound_v3_cli,
    run_event_image_dataset_repro_cli,
    run_event_image_dataset_robustness_batch_cli,
    run_event_image_dataset_sequence_combo_cli,
    run_event_image_dataset_train,
    run_event_image_dataset_universe_build,
    run_event_image_dataset_veto_ablation_cli,
    run_event_image_dataset_veto_compare_cli,
    run_event_image_dataset_selection_contract_cli,
    run_event_image_dataset_veto_thin_liquidity_cli,
)
from external_analysis.exporter.diff_export import run_diff_export
from external_analysis.exporter.export_schema import ensure_export_db
from external_analysis.exporter.snapshot_status import build_export_snapshot
from external_analysis.labels.anchor_windows import build_anchor_windows
from external_analysis.labels.rolling_labels import build_rolling_labels
from external_analysis.labels.store import ensure_label_db
from external_analysis.image_rerank.cli import run_image_rerank_phase0_3
from external_analysis.image_rerank.research_runner import run_image_rerank_disposition, run_image_rerank_research
from external_analysis.models.candidate_baseline import run_candidate_baseline
from external_analysis.models.forecast_surface_evaluation import evaluate_forecast_surface, summarize_forecast_surface_shadow_run
from external_analysis.ops.ops_schema import ensure_ops_db
from external_analysis.results.publish import publish_result
from external_analysis.results.publish_candidates import (
    backfill_publish_candidate_bundles,
    cleanup_publish_candidate_maintenance_state,
    load_publish_candidate_maintenance_state,
    sweep_publish_candidate_snapshots,
)
from external_analysis.results.result_schema import ensure_result_db
from external_analysis.runtime.historical_replay import run_historical_replay, run_replay_core
from external_analysis.runtime.daily_research import (
    build_daily_research_dispatch,
    build_daily_research_watchlist,
    build_daily_research_tag_report,
    format_daily_research_dispatch_text_report,
    format_daily_research_history_text_report,
    format_daily_research_tag_report_text_report,
    format_daily_research_watchlist_text_report,
    load_daily_research_history,
    run_daily_research_loop,
    run_daily_research_cycle,
)
from external_analysis.runtime.daily_research_prepare import run_daily_research_prepare
from external_analysis.runtime.nightly_pipeline import run_nightly_candidate_pipeline
from external_analysis.runtime.promotion_decision import run_promotion_decision_command
from external_analysis.runtime.challenger_eval import run_challenger_eval
from external_analysis.runtime.nightly_similarity_challenger_pipeline import run_nightly_similarity_challenger_pipeline
from external_analysis.runtime.nightly_similarity_pipeline import run_nightly_similarity_pipeline
from external_analysis.runtime.review_build import run_review_build
from external_analysis.similarity.baseline import run_similarity_baseline, run_similarity_challenger_shadow
from external_analysis.similarity.store import ensure_similarity_db


def _print_cli_payload(payload: object, *, mode: str = "full") -> None:
    if mode == "summary" and isinstance(payload, dict):
        summary = dict(payload)
        if "baseline" in summary and isinstance(summary["baseline"], dict):
            baseline = dict(summary["baseline"])
            summary["baseline"] = {
                "publish_id": baseline.get("publish_id"),
                "metrics_saved": baseline.get("metrics_saved"),
                "forecast_surface_saved": baseline.get("forecast_surface_saved"),
                "forecast_surface_evaluation_saved": baseline.get("forecast_surface_evaluation_saved"),
                "forecast_surface_evaluation_gate_reason": baseline.get("forecast_surface_evaluation_gate_reason"),
            }
        if "forecast_surface" in summary and isinstance(summary["forecast_surface"], dict):
            forecast_surface = dict(summary["forecast_surface"])
            summary["forecast_surface"] = {
                "publish_id": forecast_surface.get("publish_id"),
                "row_count": forecast_surface.get("row_count"),
                "coverage_ratio": forecast_surface.get("coverage_ratio"),
                "alerts": forecast_surface.get("alerts"),
            }
        if "forecast_surface_evaluation" in summary and isinstance(summary["forecast_surface_evaluation"], dict):
            evaluation = dict(summary["forecast_surface_evaluation"])
            summary["forecast_surface_evaluation"] = {
                "ok": evaluation.get("ok"),
                "scope_type": evaluation.get("scope_type"),
                "readiness_pass": evaluation.get("readiness_pass"),
                "gate_reason": evaluation.get("gate_reason"),
                "publish_id": evaluation.get("publish_id"),
            }
        if "candidate_bundle" in summary and isinstance(summary["candidate_bundle"], dict):
            bundle = dict(summary["candidate_bundle"])
            summary["candidate_bundle"] = {
                "publish_id": (bundle.get("bundle") or {}).get("publish_id") if isinstance(bundle.get("bundle"), dict) else bundle.get("publish_id"),
                "ok": bundle.get("ok"),
                "readiness_pass": (bundle.get("bundle") or {}).get("readiness_pass") if isinstance(bundle.get("bundle"), dict) else bundle.get("readiness_pass"),
            }
        print(json.dumps(summary, ensure_ascii=False, default=str, sort_keys=True))
        return
    print(payload)


def main() -> int:
    parser = argparse.ArgumentParser(prog="python -m external_analysis")
    sub = parser.add_subparsers(dest="cmd", required=True)

    init_parser = sub.add_parser("init-result-db", help="Create or verify the result DB schema.")
    init_parser.add_argument("--db-path", default=None)

    publish_parser = sub.add_parser("publish-stub", help="Write a minimal successful publish for Slice A verification.")
    publish_parser.add_argument("--db-path", default=None)
    publish_parser.add_argument("--publish-id", required=True)
    publish_parser.add_argument("--as-of-date", required=True)
    publish_parser.add_argument("--freshness-state", default="fresh")
    publish_parser.add_argument("--pointer-name", default="latest_successful")

    export_init_parser = sub.add_parser("init-export-db", help="Create or verify the export DB schema.")
    export_init_parser.add_argument("--db-path", default=None)

    label_init_parser = sub.add_parser("init-label-db", help="Create or verify the label DB schema.")
    label_init_parser.add_argument("--db-path", default=None)

    ops_init_parser = sub.add_parser("init-ops-db", help="Create or verify the ops DB schema.")
    ops_init_parser.add_argument("--db-path", default=None)

    similarity_init_parser = sub.add_parser("init-similarity-db", help="Create or verify the internal similarity DB schema.")
    similarity_init_parser.add_argument("--db-path", default=None)

    export_sync_parser = sub.add_parser("export-sync", help="Run Slice B diff export into the internal export DB.")
    export_sync_parser.add_argument("--source-db-path", default=None)
    export_sync_parser.add_argument("--export-db-path", default=None)

    export_snapshot_build_parser = sub.add_parser(
        "export-snapshot-build",
        help="Build a complete reusable export snapshot and write the snapshot status sidecar.",
    )
    export_snapshot_build_parser.add_argument("--source-db-path", default=None)
    export_snapshot_build_parser.add_argument("--export-db-path", default=None)

    label_build_parser = sub.add_parser("label-build", help="Build rolling labels into the internal label DB.")
    label_build_parser.add_argument("--export-db-path", default=None)
    label_build_parser.add_argument("--label-db-path", default=None)

    forecast_surface_eval_parser = sub.add_parser(
        "forecast-surface-evaluate-run",
        help="Evaluate forecast_surface_daily against label horizons and persist walk-forward metrics.",
    )
    forecast_surface_eval_parser.add_argument("--result-db-path", default=None)
    forecast_surface_eval_parser.add_argument("--label-db-path", default=None)
    forecast_surface_eval_parser.add_argument("--source-db-path", default=None)
    forecast_surface_eval_parser.add_argument("--publish-id", default=None)
    forecast_surface_eval_parser.add_argument("--publish-id-prefix", default=None)
    forecast_surface_eval_parser.add_argument("--top-k", type=int, default=20)
    forecast_surface_eval_parser.add_argument("--min-folds", type=int, default=3)
    forecast_surface_eval_parser.add_argument("--min-daily-count", type=int, default=30)

    forecast_surface_shadow_status_parser = sub.add_parser(
        "forecast-surface-shadow-status-run",
        help="Summarize forecast surface shadow-run acceptance status from persisted publish evaluations.",
    )
    forecast_surface_shadow_status_parser.add_argument("--result-db-path", default=None)
    forecast_surface_shadow_status_parser.add_argument("--publish-id-prefix", default="shadow20_")
    forecast_surface_shadow_status_parser.add_argument("--min-days", type=int, default=20)
    forecast_surface_shadow_status_parser.add_argument("--min-universe-code-count", type=int, default=650)

    anchor_build_parser = sub.add_parser("anchor-window-build", help="Build anchor windows into the internal label DB.")
    anchor_build_parser.add_argument("--export-db-path", default=None)
    anchor_build_parser.add_argument("--label-db-path", default=None)

    candidate_parser = sub.add_parser("candidate-baseline-run", help="Run the Slice D candidate baseline and publish candidate/regime rows.")
    candidate_parser.add_argument("--export-db-path", default=None)
    candidate_parser.add_argument("--label-db-path", default=None)
    candidate_parser.add_argument("--result-db-path", default=None)
    candidate_parser.add_argument("--source-db-path", default=None)
    candidate_parser.add_argument("--similarity-db-path", default=None)
    candidate_parser.add_argument("--as-of-date", required=True)
    candidate_parser.add_argument("--publish-id", default=None)
    candidate_parser.add_argument("--freshness-state", default="fresh")
    candidate_parser.add_argument("--ops-db-path", default=None)
    candidate_parser.add_argument("--no-publish-public", action="store_true")

    nightly_parser = sub.add_parser("nightly-candidate-run", help="Run export -> labels -> baseline -> publish -> metrics and record the run in ops DB.")
    nightly_parser.add_argument("--source-db-path", default=None)
    nightly_parser.add_argument("--export-db-path", default=None)
    nightly_parser.add_argument("--label-db-path", default=None)
    nightly_parser.add_argument("--result-db-path", default=None)
    nightly_parser.add_argument("--similarity-db-path", default=None)
    nightly_parser.add_argument("--ops-db-path", default=None)
    nightly_parser.add_argument("--as-of-date", required=True)
    nightly_parser.add_argument("--publish-id", default=None)
    nightly_parser.add_argument("--freshness-state", default="fresh")
    nightly_parser.add_argument("--no-source-snapshot", action="store_true")
    nightly_parser.add_argument("--snapshot-root", default=None)
    nightly_parser.add_argument("--require-prepared-environment", action="store_true")

    similarity_parser = sub.add_parser("similarity-baseline-run", help="Build similarity cases and publish similar_cases_daily / similar_case_paths.")
    similarity_parser.add_argument("--export-db-path", default=None)
    similarity_parser.add_argument("--label-db-path", default=None)
    similarity_parser.add_argument("--result-db-path", default=None)
    similarity_parser.add_argument("--similarity-db-path", default=None)
    similarity_parser.add_argument("--as-of-date", required=True)
    similarity_parser.add_argument("--publish-id", default=None)
    similarity_parser.add_argument("--freshness-state", default="fresh")

    challenger_parser = sub.add_parser("similarity-challenger-run", help="Build challenger embeddings and store shadow similarity results internally.")
    challenger_parser.add_argument("--export-db-path", default=None)
    challenger_parser.add_argument("--label-db-path", default=None)
    challenger_parser.add_argument("--result-db-path", default=None)
    challenger_parser.add_argument("--similarity-db-path", default=None)
    challenger_parser.add_argument("--as-of-date", required=True)
    challenger_parser.add_argument("--publish-id", default=None)

    similarity_nightly_parser = sub.add_parser("nightly-similarity-run", help="Run similarity baseline publish + internal metrics and record the run in ops DB.")
    similarity_nightly_parser.add_argument("--export-db-path", default=None)
    similarity_nightly_parser.add_argument("--label-db-path", default=None)
    similarity_nightly_parser.add_argument("--result-db-path", default=None)
    similarity_nightly_parser.add_argument("--similarity-db-path", default=None)
    similarity_nightly_parser.add_argument("--ops-db-path", default=None)
    similarity_nightly_parser.add_argument("--as-of-date", required=True)
    similarity_nightly_parser.add_argument("--publish-id", default=None)
    similarity_nightly_parser.add_argument("--freshness-state", default="fresh")

    challenger_nightly_parser = sub.add_parser("nightly-similarity-challenger-run", help="Run challenger shadow similarity nightly and record internal comparison metrics.")
    challenger_nightly_parser.add_argument("--export-db-path", default=None)
    challenger_nightly_parser.add_argument("--label-db-path", default=None)
    challenger_nightly_parser.add_argument("--result-db-path", default=None)
    challenger_nightly_parser.add_argument("--similarity-db-path", default=None)
    challenger_nightly_parser.add_argument("--ops-db-path", default=None)
    challenger_nightly_parser.add_argument("--as-of-date", required=True)
    challenger_nightly_parser.add_argument("--publish-id", default=None)

    challenger_eval_parser = sub.add_parser("challenger-eval-run", help="Run queued or direct challenger evaluation without rolling/review aggregation.")
    challenger_eval_parser.add_argument("--export-db-path", default=None)
    challenger_eval_parser.add_argument("--label-db-path", default=None)
    challenger_eval_parser.add_argument("--result-db-path", default=None)
    challenger_eval_parser.add_argument("--similarity-db-path", default=None)
    challenger_eval_parser.add_argument("--ops-db-path", default=None)
    challenger_eval_parser.add_argument("--work-id", default=None)
    challenger_eval_parser.add_argument("--scope-type", default=None)
    challenger_eval_parser.add_argument("--scope-id", default=None)
    challenger_eval_parser.add_argument("--as-of-date", default=None)
    challenger_eval_parser.add_argument("--publish-id", default=None)
    challenger_eval_parser.add_argument("--replay-id", default=None)

    review_build_parser = sub.add_parser("review-build-run", help="Build rolling comparison scopes and refresh the review summary.")
    review_build_parser.add_argument("--result-db-path", required=True)
    review_build_parser.add_argument("--similarity-db-path", required=True)
    review_build_parser.add_argument("--ops-db-path", required=True)
    review_build_parser.add_argument("--work-id", default=None)
    review_build_parser.add_argument("--scope-type", default=None)
    review_build_parser.add_argument("--scope-id", default=None)

    replay_core_parser = sub.add_parser("replay-core-run", help="Run replay core only and queue downstream challenger evaluation.")
    replay_core_parser.add_argument("--source-db-path", required=True)
    replay_core_parser.add_argument("--export-db-path", required=True)
    replay_core_parser.add_argument("--label-db-path", required=True)
    replay_core_parser.add_argument("--result-db-path", required=True)
    replay_core_parser.add_argument("--similarity-db-path", required=True)
    replay_core_parser.add_argument("--ops-db-path", required=True)
    replay_core_parser.add_argument("--start-as-of-date", required=True)
    replay_core_parser.add_argument("--end-as-of-date", required=True)
    replay_core_parser.add_argument("--replay-id", required=True)
    replay_core_parser.add_argument("--codes", default=None)
    replay_core_parser.add_argument("--max-days", type=int, default=None)
    replay_core_parser.add_argument("--max-codes", type=int, default=None)
    replay_core_parser.add_argument("--no-source-snapshot", action="store_true")
    replay_core_parser.add_argument("--snapshot-root", default=None)

    replay_parser = sub.add_parser("historical-replay-run", help="Replay a range of as_of_date values internally for candidate/similarity champion/challenger comparison.")
    replay_parser.add_argument("--source-db-path", required=True)
    replay_parser.add_argument("--export-db-path", required=True)
    replay_parser.add_argument("--label-db-path", required=True)
    replay_parser.add_argument("--result-db-path", required=True)
    replay_parser.add_argument("--similarity-db-path", required=True)
    replay_parser.add_argument("--ops-db-path", required=True)
    replay_parser.add_argument("--start-as-of-date", required=True)
    replay_parser.add_argument("--end-as-of-date", required=True)
    replay_parser.add_argument("--replay-id", required=True)
    replay_parser.add_argument("--codes", default=None)
    replay_parser.add_argument("--max-days", type=int, default=None)
    replay_parser.add_argument("--max-codes", type=int, default=None)
    replay_parser.add_argument("--no-source-snapshot", action="store_true")
    replay_parser.add_argument("--snapshot-root", default=None)

    daily_research_parser = sub.add_parser("daily-research-run", help="Run candidate + similarity + challenger nightly flow and emit a compact daily research report.")
    daily_research_parser.add_argument("--source-db-path", default=None)
    daily_research_parser.add_argument("--export-db-path", default=None)
    daily_research_parser.add_argument("--label-db-path", default=None)
    daily_research_parser.add_argument("--result-db-path", default=None)
    daily_research_parser.add_argument("--similarity-db-path", default=None)
    daily_research_parser.add_argument("--ops-db-path", default=None)
    daily_research_parser.add_argument("--as-of-date", default=None)
    daily_research_parser.add_argument("--publish-id", default=None)
    daily_research_parser.add_argument("--freshness-state", default="fresh")
    daily_research_parser.add_argument("--report-path", default=None)
    daily_research_parser.add_argument("--text-report-path", default=None)
    daily_research_parser.add_argument("--progress-path", default=None)
    daily_research_parser.add_argument("--no-source-snapshot", action="store_true")
    daily_research_parser.add_argument("--snapshot-root", default=None)

    daily_research_prepare_parser = sub.add_parser(
        "daily-research-prepare",
        help="Build and validate the reusable prepared environment for daily research.",
    )
    daily_research_prepare_parser.add_argument("--source-db-path", default=None)
    daily_research_prepare_parser.add_argument("--export-db-path", default=None)
    daily_research_prepare_parser.add_argument("--label-db-path", default=None)
    daily_research_prepare_parser.add_argument("--manifest-path", default=None)
    daily_research_prepare_parser.add_argument("--progress-path", default=None)

    daily_research_loop_parser = sub.add_parser(
        "daily-research-loop",
        help="Run daily research across the latest trading days until promotion-ready long candidates appear.",
    )
    daily_research_loop_parser.add_argument("--source-db-path", default=None)
    daily_research_loop_parser.add_argument("--export-db-path", default=None)
    daily_research_loop_parser.add_argument("--label-db-path", default=None)
    daily_research_loop_parser.add_argument("--result-db-path", default=None)
    daily_research_loop_parser.add_argument("--similarity-db-path", default=None)
    daily_research_loop_parser.add_argument("--ops-db-path", default=None)
    daily_research_loop_parser.add_argument("--freshness-state", default="fresh")
    daily_research_loop_parser.add_argument("--report-path", default=None)
    daily_research_loop_parser.add_argument("--text-report-path", default=None)
    daily_research_loop_parser.add_argument("--progress-path", default=None)
    daily_research_loop_parser.add_argument("--snapshot-root", default=None)
    daily_research_loop_parser.add_argument("--max-trading-days", type=int, default=5)

    image_rerank_parser = sub.add_parser("image-rerank-run", help="Run the TRADEX image rerank Phase0-Phase3 pipeline.")
    image_rerank_parser.add_argument("--export-db-path", default=None)
    image_rerank_parser.add_argument("--as-of-date", required=True)
    image_rerank_parser.add_argument("--run-id", default=None)
    image_rerank_parser.add_argument("--verify-profile", default="smoke")
    image_rerank_parser.add_argument("--top-k", type=int, default=10)
    image_rerank_parser.add_argument("--block-size-days", type=int, default=30)
    image_rerank_parser.add_argument("--embargo-days", type=int, default=20)
    image_rerank_parser.add_argument("--feature-lookback-days", type=int, default=80)
    image_rerank_parser.add_argument("--label-horizon-days", type=int, default=20)
    image_rerank_parser.add_argument("--positive-quantile", type=float, default=0.85)
    image_rerank_parser.add_argument("--negative-quantile", type=float, default=0.15)
    image_rerank_parser.add_argument("--neutral-weight", type=float, default=0.25)
    image_rerank_parser.add_argument("--base-weight", type=float, default=0.70)
    image_rerank_parser.add_argument("--image-weight", type=float, default=0.30)
    image_rerank_parser.add_argument("--renderer-backend", default="auto")

    image_rerank_research_parser = sub.add_parser(
        "image-rerank-research-run",
        help="Run the full-universe image rerank orchestration and derived JSON artifacts.",
    )
    image_rerank_research_parser.add_argument("--source-db-path", default=None)
    image_rerank_research_parser.add_argument("--export-db-path", default=None)
    image_rerank_research_parser.add_argument("--session-id", default=None)
    image_rerank_research_parser.add_argument("--as-of-date", default=None)
    image_rerank_research_parser.add_argument("--top-k", type=int, default=10)
    image_rerank_research_parser.add_argument("--renderer-backend", default="auto")

    image_rerank_disposition_parser = sub.add_parser(
        "image-rerank-disposition-run",
        help="Build the derived keep/drop/hold disposition artifact for an existing image rerank research session.",
    )
    image_rerank_disposition_parser.add_argument("--session-id", required=True)

    event_image_dataset_build_parser = sub.add_parser(
        "event-image-dataset-build",
        help="Build the monthly top20/bottom20 event image dataset.",
    )
    event_image_dataset_build_parser.add_argument("--export-db-path", required=True)
    event_image_dataset_build_parser.add_argument("--dataset-id", required=True)
    event_image_dataset_build_parser.add_argument("--source-db-path", default=None)
    event_image_dataset_build_parser.add_argument("--start-month", default=None)
    event_image_dataset_build_parser.add_argument("--end-month", default=None)
    event_image_dataset_build_parser.add_argument("--renderer-backend", default="agg")
    event_image_dataset_build_parser.add_argument("--restricted-universe-path", default=None)

    event_image_dataset_train_parser = sub.add_parser(
        "event-image-dataset-train",
        help="Train image-only and numeric-only baselines on a built event image dataset.",
    )
    event_image_dataset_train_parser.add_argument("--dataset-id", required=True)
    event_image_dataset_train_parser.add_argument("--seed", type=int, default=42)
    event_image_dataset_train_parser.add_argument("--feature-size", type=int, default=48)

    event_image_dataset_repro_parser = sub.add_parser(
        "event-image-dataset-repro-run",
        help="Run multi-seed reproducibility training on a built event image dataset.",
    )
    event_image_dataset_repro_parser.add_argument("--dataset-id", required=True)
    event_image_dataset_repro_parser.add_argument("--seeds", nargs="*", type=int, default=None)
    event_image_dataset_repro_parser.add_argument("--feature-size", type=int, default=48)

    event_image_dataset_regime_parser = sub.add_parser(
        "event-image-dataset-regime-run",
        help="Build regime-gate artifacts for a trained event image dataset.",
    )
    event_image_dataset_regime_parser.add_argument("--dataset-id", required=True)

    event_image_dataset_pattern_parser = sub.add_parser(
        "event-image-dataset-pattern-run",
        help="Build pattern decomposition artifacts for a trained event image dataset and regime.",
    )
    event_image_dataset_pattern_parser.add_argument("--dataset-id", required=True)
    event_image_dataset_pattern_parser.add_argument("--regime-tag", default="rebound_onset")

    event_image_dataset_pattern_library_parser = sub.add_parser(
        "event-image-dataset-pattern-library-run",
        help="Build the first pattern library candidate artifact from pattern decompositions.",
    )
    event_image_dataset_pattern_library_parser.add_argument("--dataset-id", required=True)
    event_image_dataset_pattern_library_parser.add_argument("--regime-tags", nargs="+", required=True)

    event_image_dataset_boundary_parser = sub.add_parser(
        "event-image-dataset-boundary-run",
        help="Build a boundary compare artifact between two regimes for a trained event image dataset.",
    )
    event_image_dataset_boundary_parser.add_argument("--dataset-id", required=True)
    event_image_dataset_boundary_parser.add_argument("--primary-regime", required=True)
    event_image_dataset_boundary_parser.add_argument("--comparison-regime", required=True)
    event_image_dataset_boundary_parser.add_argument("--max-workers", type=int, default=2)

    event_image_dataset_gating_parser = sub.add_parser(
        "event-image-dataset-gating-run",
        help="Build a gating-rule artifact between two regimes for a trained event image dataset.",
    )
    event_image_dataset_gating_parser.add_argument("--dataset-id", required=True)
    event_image_dataset_gating_parser.add_argument("--primary-regime", required=True)
    event_image_dataset_gating_parser.add_argument("--comparison-regime", required=True)

    event_image_dataset_combo_parser = sub.add_parser(
        "event-image-dataset-combo-run",
        help="Build combo-rule artifacts between two regimes for a trained event image dataset.",
    )
    event_image_dataset_combo_parser.add_argument("--dataset-id", required=True)
    event_image_dataset_combo_parser.add_argument("--primary-regime", required=True)
    event_image_dataset_combo_parser.add_argument("--comparison-regime", required=True)

    event_image_dataset_adoption_parser = sub.add_parser(
        "event-image-dataset-adoption-run",
        help="Build the rebound_onset auxiliary adoption artifact and publish the research prior bridge snapshot.",
    )
    event_image_dataset_adoption_parser.add_argument("--dataset-id", required=True)
    event_image_dataset_adoption_parser.add_argument("--pattern", default="rebound_onset")

    event_image_dataset_adoption_compare_parser = sub.add_parser(
        "event-image-dataset-adoption-compare-run",
        help="Compare rebound_onset adoption bonus thresholds under the same core gate contract.",
    )
    event_image_dataset_adoption_compare_parser.add_argument("--dataset-id", required=True)
    event_image_dataset_adoption_compare_parser.add_argument("--pattern", default="rebound_onset")

    event_image_dataset_pattern_breadth_parser = sub.add_parser(
        "event-image-dataset-pattern-breadth-run",
        help="Compare rebound_onset breadth candidates under the same core gate contract.",
    )
    event_image_dataset_pattern_breadth_parser.add_argument("--dataset-id", required=True)
    event_image_dataset_pattern_breadth_parser.add_argument("--pattern", default="rebound_onset")

    event_image_dataset_sequence_combo_parser = sub.add_parser(
        "event-image-dataset-sequence-combo-run",
        help="Build rebound_onset sequence-aware combo artifacts under the same core gate contract.",
    )
    event_image_dataset_sequence_combo_parser.add_argument("--dataset-id", required=True)
    event_image_dataset_sequence_combo_parser.add_argument("--pattern", default="rebound_onset")

    event_image_dataset_adoption_policy_parser = sub.add_parser(
        "event-image-dataset-adoption-policy-run",
        help="Compare rebound_onset adoption policies under the same core gate contract.",
    )
    event_image_dataset_adoption_policy_parser.add_argument("--dataset-id", required=True)
    event_image_dataset_adoption_policy_parser.add_argument("--pattern", default="rebound_onset")

    event_image_dataset_playbook_parser = sub.add_parser(
        "event-image-dataset-playbook-run",
        help="Build rebound_onset playbook score + veto artifacts under the same core gate contract.",
    )
    event_image_dataset_playbook_parser.add_argument("--dataset-id", required=True)
    event_image_dataset_playbook_parser.add_argument("--pattern", default="rebound_onset")

    event_image_dataset_playbook_relax_parser = sub.add_parser(
        "event-image-dataset-playbook-relax-run",
        help="Compare rebound_onset environment/setup relax variants while keeping veto fixed.",
    )
    event_image_dataset_playbook_relax_parser.add_argument("--dataset-id", required=True)
    event_image_dataset_playbook_relax_parser.add_argument("--pattern", default="rebound_onset")

    event_image_dataset_playbook_threshold_parser = sub.add_parser(
        "event-image-dataset-playbook-threshold-run",
        help="Compare rebound_onset balanced playbook score cutoffs while keeping features and veto fixed.",
    )
    event_image_dataset_playbook_threshold_parser.add_argument("--dataset-id", required=True)
    event_image_dataset_playbook_threshold_parser.add_argument("--pattern", default="rebound_onset")

    event_image_dataset_veto_compare_parser = sub.add_parser(
        "event-image-dataset-veto-run",
        help="Compare rebound_onset core gate with and without veto filters.",
    )
    event_image_dataset_veto_compare_parser.add_argument("--dataset-id", required=True)
    event_image_dataset_veto_compare_parser.add_argument("--pattern", default="rebound_onset")

    event_image_dataset_veto_ablation_parser = sub.add_parser(
        "event-image-dataset-veto-ablation-run",
        help="Compare rebound_onset core gate across one-rule-at-a-time veto ablations.",
    )
    event_image_dataset_veto_ablation_parser.add_argument("--dataset-id", required=True)
    event_image_dataset_veto_ablation_parser.add_argument("--pattern", default="rebound_onset")

    event_image_dataset_veto_thin_liquidity_parser = sub.add_parser(
        "event-image-dataset-veto-thin-liquidity-run",
        help="Compare rebound_onset thin_liquidity veto weakening variants while other veto rules stay fixed.",
    )
    event_image_dataset_veto_thin_liquidity_parser.add_argument("--dataset-id", required=True)
    event_image_dataset_veto_thin_liquidity_parser.add_argument("--pattern", default="rebound_onset")

    event_image_dataset_selection_contract_parser = sub.add_parser(
        "event-image-dataset-selection-contract-run",
        help="Diagnose where rebound_onset edge leaks across core gate, veto, ranking, and entryQualified stages.",
    )
    event_image_dataset_selection_contract_parser.add_argument("--dataset-id", required=True)
    event_image_dataset_selection_contract_parser.add_argument("--pattern", default="rebound_onset")

    event_image_dataset_rebound_monitor_parser = sub.add_parser(
        "event-image-dataset-rebound-monitor-run",
        help="Build a live ranking delta monitor artifact for rebound_onset policies.",
    )
    event_image_dataset_rebound_monitor_parser.add_argument("--dataset-id", required=True)
    event_image_dataset_rebound_monitor_parser.add_argument("--days", type=int, default=60)

    event_image_dataset_robustness_batch_parser = sub.add_parser(
        "event-image-dataset-robustness-batch-run",
        help="Run rebound_onset robustness compares across multiple datasets in parallel.",
    )
    event_image_dataset_robustness_batch_parser.add_argument("--dataset-ids", nargs="+", required=True)
    event_image_dataset_robustness_batch_parser.add_argument("--pattern", default="rebound_onset")
    event_image_dataset_robustness_batch_parser.add_argument("--reference-dataset-id", default=None)
    event_image_dataset_robustness_batch_parser.add_argument("--max-workers", type=int, default=None)

    event_image_dataset_multi_research_parser = sub.add_parser(
        "event-image-dataset-rebound-multi-research-run",
        help="Run the rebound_onset multi-research round and update the pattern library checkpoint.",
    )
    event_image_dataset_multi_research_parser.add_argument("--dataset-id", required=True)
    event_image_dataset_multi_research_parser.add_argument("--robustness-dataset-ids", nargs="+", required=True)
    event_image_dataset_multi_research_parser.add_argument("--max-workers", type=int, default=2)

    event_image_dataset_rebound_v3_parser = sub.add_parser(
        "event-image-dataset-rebound-v3-run",
        help="Run the rebound_onset v3 deep-research round and update the pattern library checkpoint.",
    )
    event_image_dataset_rebound_v3_parser.add_argument("--dataset-id", required=True)
    event_image_dataset_rebound_v3_parser.add_argument("--max-workers", type=int, default=4)
    event_image_dataset_rebound_v3_parser.add_argument("--monitor-days", type=int, default=60)
    event_image_dataset_rebound_v3_parser.add_argument("--diagnosis-start-date", default=None)
    event_image_dataset_rebound_v3_parser.add_argument("--diagnosis-end-date", default=None)

    event_image_dataset_batch_parser = sub.add_parser(
        "event-image-dataset-analysis-batch-run",
        help="Run event-image-dataset analysis jobs for multiple datasets in parallel.",
    )
    event_image_dataset_batch_parser.add_argument("--dataset-ids", nargs="+", required=True)
    event_image_dataset_batch_parser.add_argument("--max-workers", type=int, default=None)
    event_image_dataset_batch_parser.add_argument("--refresh-train", action="store_true")
    event_image_dataset_batch_parser.add_argument("--refresh-repro", action="store_true")
    event_image_dataset_batch_parser.add_argument("--feature-size", type=int, default=48)
    event_image_dataset_batch_parser.add_argument("--seeds", nargs="*", type=int, default=None)

    event_image_dataset_universe_parser = sub.add_parser(
        "event-image-dataset-universe-build",
        help="Build a fixed restricted-universe membership artifact from MeeMee registered codes.",
    )
    event_image_dataset_universe_parser.add_argument("--source-db-path", default=None)
    event_image_dataset_universe_parser.add_argument("--output-path", default=None)
    event_image_dataset_universe_parser.add_argument("--start-month", required=True)
    event_image_dataset_universe_parser.add_argument("--end-month", required=True)
    event_image_dataset_universe_parser.add_argument("--sample-size", type=int, default=100)
    event_image_dataset_universe_parser.add_argument("--sample-seed", type=int, default=7)

    daily_research_history_parser = sub.add_parser("daily-research-history", help="Read persisted daily research artifacts from ops DB.")
    daily_research_history_parser.add_argument("--ops-db-path", default=None)
    daily_research_history_parser.add_argument("--limit", type=int, default=10)
    daily_research_history_parser.add_argument("--report-path", default=None)
    daily_research_history_parser.add_argument("--text-report-path", default=None)

    daily_research_watchlist_parser = sub.add_parser("daily-research-watchlist", help="Aggregate pending promotions and persistent risks from persisted daily research artifacts.")
    daily_research_watchlist_parser.add_argument("--ops-db-path", default=None)
    daily_research_watchlist_parser.add_argument("--limit", type=int, default=10)
    daily_research_watchlist_parser.add_argument("--report-path", default=None)
    daily_research_watchlist_parser.add_argument("--text-report-path", default=None)

    daily_research_dispatch_parser = sub.add_parser("daily-research-dispatch", help="Select the next top action from the daily research watchlist.")
    daily_research_dispatch_parser.add_argument("--ops-db-path", default=None)
    daily_research_dispatch_parser.add_argument("--limit", type=int, default=10)
    daily_research_dispatch_parser.add_argument("--position", type=int, default=1)
    daily_research_dispatch_parser.add_argument("--report-path", default=None)
    daily_research_dispatch_parser.add_argument("--text-report-path", default=None)

    daily_research_tag_parser = sub.add_parser("daily-research-tag-report", help="Read persisted daily research history for a specific strategy tag.")
    daily_research_tag_parser.add_argument("--ops-db-path", default=None)
    daily_research_tag_parser.add_argument("--strategy-tag", required=True)
    daily_research_tag_parser.add_argument("--limit", type=int, default=10)
    daily_research_tag_parser.add_argument("--report-path", default=None)
    daily_research_tag_parser.add_argument("--text-report-path", default=None)

    promotion_decision_parser = sub.add_parser("promotion-decision-run", help="Record an approval / hold / reject decision for the latest promotion review.")
    promotion_decision_parser.add_argument("--result-db-path", default=None)
    promotion_decision_parser.add_argument("--ops-db-path", default=None)
    promotion_decision_parser.add_argument("--decision", required=True)
    promotion_decision_parser.add_argument("--note", default=None)
    promotion_decision_parser.add_argument("--actor", default="codex_cli")
    promotion_decision_parser.add_argument("--report-path", default=None)

    publish_backfill_parser = sub.add_parser("publish-maintenance-backfill", help="Backfill publish candidate bundles and update maintenance state.")
    publish_backfill_parser.add_argument("--result-db-path", default=None)
    publish_backfill_parser.add_argument("--ops-db-path", default=None)
    publish_backfill_parser.add_argument("--limit", type=int, default=None)
    publish_backfill_parser.add_argument("--dry-run", action="store_true")

    publish_sweep_parser = sub.add_parser("publish-maintenance-sweep", help="Sweep old published ranking snapshots and update maintenance state.")
    publish_sweep_parser.add_argument("--result-db-path", default=None)
    publish_sweep_parser.add_argument("--keep-approved-days", type=int, default=90)
    publish_sweep_parser.add_argument("--keep-rejected-days", type=int, default=14)
    publish_sweep_parser.add_argument("--keep-retired-days", type=int, default=14)
    publish_sweep_parser.add_argument("--dry-run", action="store_true")

    publish_cycle_parser = sub.add_parser("publish-maintenance-cycle", help="Run publish candidate backfill and snapshot sweep in a single maintenance cycle.")
    publish_cycle_parser.add_argument("--result-db-path", default=None)
    publish_cycle_parser.add_argument("--ops-db-path", default=None)
    publish_cycle_parser.add_argument("--limit", type=int, default=None)
    publish_cycle_parser.add_argument("--keep-approved-days", type=int, default=90)
    publish_cycle_parser.add_argument("--keep-rejected-days", type=int, default=14)
    publish_cycle_parser.add_argument("--keep-retired-days", type=int, default=14)
    publish_cycle_parser.add_argument("--dry-run", action="store_true")

    publish_cleanup_parser = sub.add_parser("publish-maintenance-cleanup", help="Cleanup legacy publish maintenance residue and normalize maintenance state.")
    publish_cleanup_parser.add_argument("--result-db-path", default=None)
    publish_cleanup_parser.add_argument("--dry-run", action="store_true")

    args = parser.parse_args()
    if args.cmd == "init-result-db":
        info = ensure_result_db(db_path=args.db_path)
        print(info)
        return 0
    if args.cmd == "publish-stub":
        payload = publish_result(
            db_path=args.db_path,
            publish_id=str(args.publish_id),
            as_of_date=str(args.as_of_date),
            freshness_state=str(args.freshness_state),
            pointer_name=str(args.pointer_name),
            table_row_counts={},
            degrade_ready=True,
        )
        print(payload)
        return 0
    if args.cmd == "init-export-db":
        print(ensure_export_db(db_path=args.db_path))
        return 0
    if args.cmd == "init-label-db":
        print(ensure_label_db(db_path=args.db_path))
        return 0
    if args.cmd == "init-ops-db":
        print(ensure_ops_db(db_path=args.db_path))
        return 0
    if args.cmd == "init-similarity-db":
        print(ensure_similarity_db(db_path=args.db_path))
        return 0
    if args.cmd == "export-sync":
        print(run_diff_export(source_db_path=args.source_db_path, export_db_path=args.export_db_path))
        return 0
    if args.cmd == "export-snapshot-build":
        print(build_export_snapshot(source_db_path=args.source_db_path, export_db_path=args.export_db_path))
        return 0
    if args.cmd == "label-build":
        print(build_rolling_labels(export_db_path=args.export_db_path, label_db_path=args.label_db_path))
        return 0
    if args.cmd == "forecast-surface-evaluate-run":
        _print_cli_payload(
            evaluate_forecast_surface(
                result_db_path=args.result_db_path,
                label_db_path=args.label_db_path,
                source_db_path=args.source_db_path,
                publish_id=args.publish_id,
                publish_id_prefix=args.publish_id_prefix,
                top_k=int(args.top_k),
                min_folds=int(args.min_folds),
                min_daily_count=int(args.min_daily_count),
                persist=True,
            ),
            mode="summary",
        )
        return 0
    if args.cmd == "forecast-surface-shadow-status-run":
        _print_cli_payload(
            summarize_forecast_surface_shadow_run(
                result_db_path=args.result_db_path,
                publish_id_prefix=args.publish_id_prefix,
                min_days=int(args.min_days),
                min_universe_code_count=int(args.min_universe_code_count),
            ),
            mode="summary",
        )
        return 0
    if args.cmd == "anchor-window-build":
        print(build_anchor_windows(export_db_path=args.export_db_path, label_db_path=args.label_db_path))
        return 0
    if args.cmd == "candidate-baseline-run":
        _print_cli_payload(
            run_candidate_baseline(
                export_db_path=args.export_db_path,
                label_db_path=args.label_db_path,
                result_db_path=args.result_db_path,
                source_db_path=args.source_db_path,
                similarity_db_path=args.similarity_db_path,
                as_of_date=args.as_of_date,
                publish_id=args.publish_id,
                freshness_state=args.freshness_state,
                publish_public=not bool(getattr(args, "no_publish_public", False)),
                ops_db_path=args.ops_db_path,
            ),
            mode="summary",
        )
        return 0
    if args.cmd == "nightly-candidate-run":
        _print_cli_payload(
            run_nightly_candidate_pipeline(
                source_db_path=args.source_db_path,
                export_db_path=args.export_db_path,
                label_db_path=args.label_db_path,
                result_db_path=args.result_db_path,
                similarity_db_path=args.similarity_db_path,
                ops_db_path=args.ops_db_path,
                as_of_date=args.as_of_date,
                publish_id=args.publish_id,
                freshness_state=args.freshness_state,
                snapshot_source=not bool(getattr(args, "no_source_snapshot", False)),
                snapshot_root=getattr(args, "snapshot_root", None),
                require_prepared_environment=bool(getattr(args, "require_prepared_environment", False)),
            ),
            mode="summary",
        )
        return 0
    if args.cmd == "similarity-baseline-run":
        print(
            run_similarity_baseline(
                export_db_path=args.export_db_path,
                label_db_path=args.label_db_path,
                result_db_path=args.result_db_path,
                similarity_db_path=args.similarity_db_path,
                as_of_date=args.as_of_date,
                publish_id=args.publish_id,
                freshness_state=args.freshness_state,
            )
        )
        return 0
    if args.cmd == "similarity-challenger-run":
        print(
            run_similarity_challenger_shadow(
                export_db_path=args.export_db_path,
                label_db_path=args.label_db_path,
                result_db_path=args.result_db_path,
                similarity_db_path=args.similarity_db_path,
                as_of_date=args.as_of_date,
                publish_id=args.publish_id,
            )
        )
        return 0
    if args.cmd == "nightly-similarity-run":
        print(
            run_nightly_similarity_pipeline(
                export_db_path=args.export_db_path,
                label_db_path=args.label_db_path,
                result_db_path=args.result_db_path,
                similarity_db_path=args.similarity_db_path,
                ops_db_path=args.ops_db_path,
                as_of_date=args.as_of_date,
                publish_id=args.publish_id,
                freshness_state=args.freshness_state,
            )
        )
        return 0
    if args.cmd == "nightly-similarity-challenger-run":
        print(
            run_nightly_similarity_challenger_pipeline(
                export_db_path=args.export_db_path,
                label_db_path=args.label_db_path,
                result_db_path=args.result_db_path,
                similarity_db_path=args.similarity_db_path,
                ops_db_path=args.ops_db_path,
                as_of_date=args.as_of_date,
                publish_id=args.publish_id,
            )
        )
        return 0
    if args.cmd == "challenger-eval-run":
        print(
            run_challenger_eval(
                export_db_path=args.export_db_path,
                label_db_path=args.label_db_path,
                result_db_path=args.result_db_path,
                similarity_db_path=args.similarity_db_path,
                ops_db_path=args.ops_db_path,
                work_id=args.work_id,
                scope_type=args.scope_type,
                scope_id=args.scope_id,
                as_of_date=args.as_of_date,
                publish_id=args.publish_id,
                replay_id=args.replay_id,
            )
        )
        return 0
    if args.cmd == "review-build-run":
        print(
            run_review_build(
                result_db_path=args.result_db_path,
                similarity_db_path=args.similarity_db_path,
                ops_db_path=args.ops_db_path,
                work_id=args.work_id,
                scope_type=args.scope_type,
                scope_id=args.scope_id,
            )
        )
        return 0
    if args.cmd == "replay-core-run":
        codes = None if not args.codes else [part.strip() for part in str(args.codes).split(",") if part.strip()]
        print(
            run_replay_core(
                source_db_path=args.source_db_path,
                export_db_path=args.export_db_path,
                label_db_path=args.label_db_path,
                result_db_path=args.result_db_path,
                similarity_db_path=args.similarity_db_path,
                ops_db_path=args.ops_db_path,
                start_as_of_date=args.start_as_of_date,
                end_as_of_date=args.end_as_of_date,
                replay_id=args.replay_id,
                codes=codes,
                max_days=args.max_days,
                max_codes=args.max_codes,
                snapshot_source=not bool(args.no_source_snapshot),
                snapshot_root=args.snapshot_root,
            )
        )
        return 0
    if args.cmd == "historical-replay-run":
        codes = None if not args.codes else [part.strip() for part in str(args.codes).split(",") if part.strip()]
        print(
            run_historical_replay(
                source_db_path=args.source_db_path,
                export_db_path=args.export_db_path,
                label_db_path=args.label_db_path,
                result_db_path=args.result_db_path,
                similarity_db_path=args.similarity_db_path,
                ops_db_path=args.ops_db_path,
                start_as_of_date=args.start_as_of_date,
                end_as_of_date=args.end_as_of_date,
                replay_id=args.replay_id,
                codes=codes,
                max_days=args.max_days,
                max_codes=args.max_codes,
                snapshot_source=not bool(args.no_source_snapshot),
                snapshot_root=args.snapshot_root,
            )
        )
        return 0
    if args.cmd == "daily-research-run":
        payload = run_daily_research_cycle(
            source_db_path=args.source_db_path,
            export_db_path=args.export_db_path,
            label_db_path=args.label_db_path,
            result_db_path=args.result_db_path,
            similarity_db_path=args.similarity_db_path,
            ops_db_path=args.ops_db_path,
            as_of_date=args.as_of_date,
            publish_id=args.publish_id,
            freshness_state=args.freshness_state,
            report_path=args.report_path,
            text_report_path=args.text_report_path,
            progress_path=args.progress_path,
            snapshot_source=not bool(args.no_source_snapshot),
            snapshot_root=args.snapshot_root,
        )
        print(payload)
        return 0
    if args.cmd == "daily-research-prepare":
        payload = run_daily_research_prepare(
            source_db_path=args.source_db_path,
            export_db_path=args.export_db_path,
            label_db_path=args.label_db_path,
            manifest_path=args.manifest_path,
            progress_path=args.progress_path,
        )
        print(payload)
        return 0
    if args.cmd == "daily-research-loop":
        payload = run_daily_research_loop(
            source_db_path=args.source_db_path,
            export_db_path=args.export_db_path,
            label_db_path=args.label_db_path,
            result_db_path=args.result_db_path,
            similarity_db_path=args.similarity_db_path,
            ops_db_path=args.ops_db_path,
            freshness_state=args.freshness_state,
            report_path=args.report_path,
            text_report_path=args.text_report_path,
            progress_path=args.progress_path,
            max_trading_days=args.max_trading_days,
        )
        print(payload)
        return 0
    if args.cmd == "daily-research-history":
        payload = load_daily_research_history(ops_db_path=args.ops_db_path, limit=args.limit)
        if args.report_path:
            from pathlib import Path
            import json

            Path(str(args.report_path)).expanduser().resolve().write_text(
                json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True),
                encoding="utf-8",
            )
        if args.text_report_path:
            from pathlib import Path

            Path(str(args.text_report_path)).expanduser().resolve().write_text(
                format_daily_research_history_text_report(payload),
                encoding="utf-8",
            )
        print(payload)
        return 0
    if args.cmd == "daily-research-watchlist":
        payload = build_daily_research_watchlist(ops_db_path=args.ops_db_path, limit=args.limit)
        if args.report_path:
            from pathlib import Path
            import json

            Path(str(args.report_path)).expanduser().resolve().write_text(
                json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True),
                encoding="utf-8",
            )
        if args.text_report_path:
            from pathlib import Path

            Path(str(args.text_report_path)).expanduser().resolve().write_text(
                format_daily_research_watchlist_text_report(payload),
                encoding="utf-8",
            )
        print(payload)
        return 0
    if args.cmd == "daily-research-dispatch":
        payload = build_daily_research_dispatch(
            ops_db_path=args.ops_db_path,
            limit=args.limit,
            position=args.position,
        )
        if args.report_path:
            from pathlib import Path
            import json

            Path(str(args.report_path)).expanduser().resolve().write_text(
                json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True),
                encoding="utf-8",
            )
        if args.text_report_path:
            from pathlib import Path

            Path(str(args.text_report_path)).expanduser().resolve().write_text(
                format_daily_research_dispatch_text_report(payload),
                encoding="utf-8",
            )
        print(payload)
        return 0
    if args.cmd == "daily-research-tag-report":
        payload = build_daily_research_tag_report(
            ops_db_path=args.ops_db_path,
            strategy_tag=args.strategy_tag,
            limit=args.limit,
        )
        if args.report_path:
            from pathlib import Path
            import json

            Path(str(args.report_path)).expanduser().resolve().write_text(
                json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True),
                encoding="utf-8",
            )
        if args.text_report_path:
            from pathlib import Path

            Path(str(args.text_report_path)).expanduser().resolve().write_text(
                format_daily_research_tag_report_text_report(payload),
                encoding="utf-8",
            )
        print(payload)
        return 0
    if args.cmd == "image-rerank-run":
        print(
            run_image_rerank_phase0_3(
                export_db_path=args.export_db_path,
                as_of_snapshot_date=args.as_of_date,
                run_id=args.run_id,
                verify_profile=args.verify_profile,
                top_k=int(args.top_k),
                block_size_days=int(args.block_size_days),
                embargo_days=int(args.embargo_days),
                feature_lookback_days=int(args.feature_lookback_days),
                label_horizon_days=int(args.label_horizon_days),
                positive_quantile=float(args.positive_quantile),
                negative_quantile=float(args.negative_quantile),
                neutral_weight=float(args.neutral_weight),
                base_weight=float(args.base_weight),
                image_weight=float(args.image_weight),
                renderer_backend=str(args.renderer_backend),
            )
        )
        return 0
    if args.cmd == "image-rerank-research-run":
        print(
            run_image_rerank_research(
                source_db_path=args.source_db_path,
                export_db_path=args.export_db_path,
                session_id=args.session_id,
                as_of_date=args.as_of_date,
                top_k=int(args.top_k),
                renderer_backend=str(args.renderer_backend),
            )
        )
        return 0
    if args.cmd == "image-rerank-disposition-run":
        print(run_image_rerank_disposition(session_id=args.session_id))
        return 0
    if args.cmd == "event-image-dataset-build":
        print(
            run_event_image_dataset_build(
                export_db_path=args.export_db_path,
                dataset_id=args.dataset_id,
                source_db_path=args.source_db_path,
                start_month=args.start_month,
                end_month=args.end_month,
                renderer_backend=str(args.renderer_backend),
                restricted_universe_path=args.restricted_universe_path,
            )
        )
        return 0
    if args.cmd == "event-image-dataset-train":
        print(
            run_event_image_dataset_train(
                dataset_id=args.dataset_id,
                seed=int(args.seed),
                feature_size=int(args.feature_size),
            )
        )
        return 0
    if args.cmd == "event-image-dataset-repro-run":
        print(
            run_event_image_dataset_repro_cli(
                dataset_id=args.dataset_id,
                seeds=args.seeds,
                feature_size=int(args.feature_size),
            )
        )
        return 0
    if args.cmd == "event-image-dataset-regime-run":
        print(
            run_event_image_dataset_regime_cli(
                dataset_id=args.dataset_id,
            )
        )
        return 0
    if args.cmd == "event-image-dataset-pattern-run":
        print(
            run_event_image_dataset_pattern_cli(
                dataset_id=args.dataset_id,
                regime_tag=args.regime_tag,
            )
        )
        return 0
    if args.cmd == "event-image-dataset-pattern-library-run":
        print(
            run_event_image_dataset_pattern_library_cli(
                dataset_id=args.dataset_id,
                regime_tags=args.regime_tags,
            )
        )
        return 0
    if args.cmd == "event-image-dataset-boundary-run":
        print(
            run_event_image_dataset_boundary_cli(
                dataset_id=args.dataset_id,
                primary_regime=args.primary_regime,
                comparison_regime=args.comparison_regime,
                max_workers=int(args.max_workers),
            )
        )
        return 0
    if args.cmd == "event-image-dataset-gating-run":
        print(
            run_event_image_dataset_gating_cli(
                dataset_id=args.dataset_id,
                primary_regime=args.primary_regime,
                comparison_regime=args.comparison_regime,
            )
        )
        return 0
    if args.cmd == "event-image-dataset-combo-run":
        print(
            run_event_image_dataset_combo_cli(
                dataset_id=args.dataset_id,
                primary_regime=args.primary_regime,
                comparison_regime=args.comparison_regime,
            )
        )
        return 0
    if args.cmd == "event-image-dataset-adoption-run":
        print(
            run_event_image_dataset_adoption_cli(
                dataset_id=args.dataset_id,
                pattern=args.pattern,
            )
        )
        return 0
    if args.cmd == "event-image-dataset-adoption-compare-run":
        print(
            run_event_image_dataset_adoption_compare_cli(
                dataset_id=args.dataset_id,
                pattern=args.pattern,
            )
        )
        return 0
    if args.cmd == "event-image-dataset-pattern-breadth-run":
        print(
            run_event_image_dataset_pattern_breadth_cli(
                dataset_id=args.dataset_id,
                pattern=args.pattern,
            )
        )
        return 0
    if args.cmd == "event-image-dataset-sequence-combo-run":
        print(
            run_event_image_dataset_sequence_combo_cli(
                dataset_id=args.dataset_id,
                pattern=args.pattern,
            )
        )
        return 0
    if args.cmd == "event-image-dataset-adoption-policy-run":
        print(
            run_event_image_dataset_adoption_policy_cli(
                dataset_id=args.dataset_id,
                pattern=args.pattern,
            )
        )
        return 0
    if args.cmd == "event-image-dataset-playbook-run":
        print(
            run_event_image_dataset_playbook_cli(
                dataset_id=args.dataset_id,
                pattern=args.pattern,
            )
        )
        return 0
    if args.cmd == "event-image-dataset-playbook-relax-run":
        print(
            run_event_image_dataset_playbook_relax_cli(
                dataset_id=args.dataset_id,
                pattern=args.pattern,
            )
        )
        return 0
    if args.cmd == "event-image-dataset-playbook-threshold-run":
        print(
            run_event_image_dataset_playbook_threshold_cli(
                dataset_id=args.dataset_id,
                pattern=args.pattern,
            )
        )
        return 0
    if args.cmd == "event-image-dataset-veto-run":
        print(
            run_event_image_dataset_veto_compare_cli(
                dataset_id=args.dataset_id,
                pattern=args.pattern,
            )
        )
        return 0
    if args.cmd == "event-image-dataset-veto-ablation-run":
        print(
            run_event_image_dataset_veto_ablation_cli(
                dataset_id=args.dataset_id,
                pattern=args.pattern,
            )
        )
        return 0
    if args.cmd == "event-image-dataset-veto-thin-liquidity-run":
        print(
            run_event_image_dataset_veto_thin_liquidity_cli(
                dataset_id=args.dataset_id,
                pattern=args.pattern,
            )
        )
        return 0
    if args.cmd == "event-image-dataset-selection-contract-run":
        print(
            run_event_image_dataset_selection_contract_cli(
                dataset_id=args.dataset_id,
                pattern=args.pattern,
            )
        )
        return 0
    if args.cmd == "event-image-dataset-rebound-monitor-run":
        print(
            run_event_image_dataset_rebound_monitor_cli(
                dataset_id=args.dataset_id,
                days=int(args.days),
            )
        )
        return 0
    if args.cmd == "event-image-dataset-robustness-batch-run":
        print(
            run_event_image_dataset_robustness_batch_cli(
                dataset_ids=args.dataset_ids,
                pattern=args.pattern,
                reference_dataset_id=args.reference_dataset_id,
                max_workers=args.max_workers,
            )
        )
        return 0
    if args.cmd == "event-image-dataset-rebound-multi-research-run":
        print(
            run_event_image_dataset_rebound_multi_research_cli(
                dataset_id=args.dataset_id,
                robustness_dataset_ids=args.robustness_dataset_ids,
                max_workers=int(args.max_workers),
            )
        )
        return 0
    if args.cmd == "event-image-dataset-rebound-v3-run":
        print(
            run_event_image_dataset_rebound_v3_cli(
                dataset_id=args.dataset_id,
                max_workers=int(args.max_workers),
                monitor_days=int(args.monitor_days),
                diagnosis_start_date=args.diagnosis_start_date,
                diagnosis_end_date=args.diagnosis_end_date,
            )
        )
        return 0
    if args.cmd == "event-image-dataset-analysis-batch-run":
        print(
            run_event_image_dataset_analysis_batch_cli(
                dataset_ids=args.dataset_ids,
                max_workers=args.max_workers,
                refresh_train=bool(args.refresh_train),
                refresh_repro=bool(args.refresh_repro),
                feature_size=int(args.feature_size),
                seeds=args.seeds,
            )
        )
        return 0
    if args.cmd == "event-image-dataset-universe-build":
        print(
            run_event_image_dataset_universe_build(
                source_db_path=args.source_db_path,
                output_path=args.output_path,
                start_month=args.start_month,
                end_month=args.end_month,
                sample_size=int(args.sample_size),
                sample_seed=int(args.sample_seed),
            )
        )
        return 0
    if args.cmd == "promotion-decision-run":
        print(
            run_promotion_decision_command(
                result_db_path=args.result_db_path,
                ops_db_path=args.ops_db_path,
                decision=args.decision,
                note=args.note,
                actor=args.actor,
                report_path=args.report_path,
            )
        )
        return 0
    if args.cmd == "publish-maintenance-backfill":
        print(
            backfill_publish_candidate_bundles(
                db_path=args.result_db_path,
                limit=args.limit,
                dry_run=bool(args.dry_run),
            )
        )
        return 0
    if args.cmd == "publish-maintenance-sweep":
        print(
            sweep_publish_candidate_snapshots(
                db_path=args.result_db_path,
                keep_approved_days=args.keep_approved_days,
                keep_rejected_days=args.keep_rejected_days,
                keep_retired_days=args.keep_retired_days,
                dry_run=bool(args.dry_run),
            )
        )
        return 0
    if args.cmd == "publish-maintenance-cycle":
        backfill = backfill_publish_candidate_bundles(
            db_path=args.result_db_path,
            limit=args.limit,
            dry_run=bool(args.dry_run),
        )
        sweep = sweep_publish_candidate_snapshots(
            db_path=args.result_db_path,
            keep_approved_days=args.keep_approved_days,
            keep_rejected_days=args.keep_rejected_days,
            keep_retired_days=args.keep_retired_days,
            dry_run=bool(args.dry_run),
        )
        maintenance_state = load_publish_candidate_maintenance_state(db_path=args.result_db_path)
        print(
            {
                "ok": bool(backfill.get("ok")) and bool(sweep.get("ok")),
                "dry_run": bool(args.dry_run),
                "backfill": backfill,
                "snapshot_sweep": sweep,
                "candidate_backfill_last_run": maintenance_state.get("candidate_backfill_last_run"),
                "snapshot_sweep_last_run": maintenance_state.get("snapshot_sweep_last_run"),
                "non_promotable_legacy_count": int(maintenance_state.get("non_promotable_legacy_count") or 0),
                "maintenance_degraded": bool(maintenance_state.get("maintenance_degraded")),
            }
        )
        return 0
    if args.cmd == "publish-maintenance-cleanup":
        print(
            cleanup_publish_candidate_maintenance_state(
                db_path=args.result_db_path,
                dry_run=bool(args.dry_run),
            )
        )
        return 0
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
