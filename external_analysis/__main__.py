from __future__ import annotations

import argparse

from external_analysis.exporter.diff_export import run_diff_export
from external_analysis.exporter.export_schema import ensure_export_db
from external_analysis.labels.anchor_windows import build_anchor_windows
from external_analysis.labels.rolling_labels import build_rolling_labels
from external_analysis.labels.store import ensure_label_db
from external_analysis.models.candidate_baseline import run_candidate_baseline
from external_analysis.ops.ops_schema import ensure_ops_db
from external_analysis.results.publish import publish_result
from external_analysis.results.publish_candidates import (
    backfill_publish_candidate_bundles,
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
    run_daily_research_cycle,
)
from external_analysis.runtime.nightly_pipeline import run_nightly_candidate_pipeline
from external_analysis.runtime.promotion_decision import run_promotion_decision_command
from external_analysis.runtime.challenger_eval import run_challenger_eval
from external_analysis.runtime.nightly_similarity_challenger_pipeline import run_nightly_similarity_challenger_pipeline
from external_analysis.runtime.nightly_similarity_pipeline import run_nightly_similarity_pipeline
from external_analysis.runtime.review_build import run_review_build
from external_analysis.similarity.baseline import run_similarity_baseline, run_similarity_challenger_shadow
from external_analysis.similarity.store import ensure_similarity_db


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

    label_build_parser = sub.add_parser("label-build", help="Build rolling labels into the internal label DB.")
    label_build_parser.add_argument("--export-db-path", default=None)
    label_build_parser.add_argument("--label-db-path", default=None)

    anchor_build_parser = sub.add_parser("anchor-window-build", help="Build anchor windows into the internal label DB.")
    anchor_build_parser.add_argument("--export-db-path", default=None)
    anchor_build_parser.add_argument("--label-db-path", default=None)

    candidate_parser = sub.add_parser("candidate-baseline-run", help="Run the Slice D candidate baseline and publish candidate/regime rows.")
    candidate_parser.add_argument("--export-db-path", default=None)
    candidate_parser.add_argument("--label-db-path", default=None)
    candidate_parser.add_argument("--result-db-path", default=None)
    candidate_parser.add_argument("--similarity-db-path", default=None)
    candidate_parser.add_argument("--as-of-date", required=True)
    candidate_parser.add_argument("--publish-id", default=None)
    candidate_parser.add_argument("--freshness-state", default="fresh")
    candidate_parser.add_argument("--ops-db-path", default=None)

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
    daily_research_parser.add_argument("--no-source-snapshot", action="store_true")
    daily_research_parser.add_argument("--snapshot-root", default=None)

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
    if args.cmd == "label-build":
        print(build_rolling_labels(export_db_path=args.export_db_path, label_db_path=args.label_db_path))
        return 0
    if args.cmd == "anchor-window-build":
        print(build_anchor_windows(export_db_path=args.export_db_path, label_db_path=args.label_db_path))
        return 0
    if args.cmd == "candidate-baseline-run":
        print(
            run_candidate_baseline(
                export_db_path=args.export_db_path,
                label_db_path=args.label_db_path,
                result_db_path=args.result_db_path,
                similarity_db_path=args.similarity_db_path,
                as_of_date=args.as_of_date,
                publish_id=args.publish_id,
                freshness_state=args.freshness_state,
                ops_db_path=args.ops_db_path,
            )
        )
        return 0
    if args.cmd == "nightly-candidate-run":
        print(
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
            )
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
            snapshot_source=not bool(args.no_source_snapshot),
            snapshot_root=args.snapshot_root,
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
                ops_db_path=args.ops_db_path,
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
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
