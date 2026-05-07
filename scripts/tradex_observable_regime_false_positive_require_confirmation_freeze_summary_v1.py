from __future__ import annotations

import argparse
import json
import math
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

SCRIPT_NAME = "tradex_observable_regime_false_positive_require_confirmation_freeze_summary_v1"
SCHEMA_VERSION = "tradex_observable_regime_false_positive_require_confirmation_freeze_summary_v1"
FREEZE_LINE_NAME = "observable_regime_false_positive_require_confirmation_line"

DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\research_freeze_summaries\observable_regime_false_positive_require_confirmation")
DEFAULT_CHALLENGER_SESSION = Path(r"G:\Tradex\observable_regime_false_positive_require_confirmation_v1\20260501T081501Z-999791")
DEFAULT_BOTTOM15_AUDIT_SESSION = Path(r"G:\Tradex\observable_regime_false_positive_bottom15_audit_v1\20260501T082434Z-859316")
DEFAULT_REBUILD_SESSION = Path(r"G:\Tradex\observable_regime_false_positive_bottom15_summary_rebuild_v1\20260501T085017Z-012155")


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _make_session_id() -> str:
    now = datetime.now(timezone.utc)
    return f"{now.strftime('%Y%m%dT%H%M%SZ')}-{now.microsecond:06d}"


def _json_ready(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _json_ready(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_json_ready(item) for item in value]
    if isinstance(value, tuple):
        return [_json_ready(item) for item in value]
    if isinstance(value, set):
        return [_json_ready(item) for item in sorted(value, key=str)]
    if isinstance(value, float) and not math.isfinite(value):
        return None
    try:
        if value is not None and hasattr(value, "__class__") and value.__class__.__name__ == "NAType":
            return None
    except Exception:
        pass
    return value


def _write_json(path: Path, payload: dict[str, Any]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(_json_ready(payload), ensure_ascii=False, indent=2, sort_keys=True, default=str) + "\n", encoding="utf-8")
    return path


def _load_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"missing required source artifact: {path}")
    return json.loads(path.read_text(encoding="utf-8"))


def _safe_git_output(args: list[str]) -> str:
    try:
        completed = subprocess.run(args, cwd=REPO_ROOT, capture_output=True, text=True, check=False)
        return (completed.stdout or completed.stderr or "").strip()
    except Exception as exc:  # pragma: no cover - best effort metadata
        return f"unavailable: {exc}"


def _resolve_path(value: str | Path | None, default: Path) -> Path:
    if value is None or str(value).strip() == "":
        return default.resolve()
    return Path(str(value)).expanduser().resolve()


def _build_lineage_summary(
    *,
    challenger_session: Path,
    bottom15_audit_session: Path,
    rebuild_session: Path,
    challenger_decision: dict[str, Any],
    route_quality: dict[str, Any],
    context_concentration: dict[str, Any],
    bottom15_decision: dict[str, Any],
    rebuild_precheck: dict[str, Any],
    metric_contract: dict[str, Any],
) -> dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "generated_at": _utc_now(),
        "freeze_line_name": FREEZE_LINE_NAME,
        "decision": "freeze_observable_regime_false_positive_require_confirmation_line",
        "status": "explanation_only",
        "same_condition_contract": True,
        "source_sessions": [
            {
                "session_role": "challenger",
                "session_dir": str(challenger_session),
                "decision": challenger_decision.get("decision"),
                "top5_changed_members_count": (challenger_decision.get("variant_pool_comparison") or {}).get("changed_members", {}).get("top5"),
                "top10_changed_members_count": (challenger_decision.get("variant_pool_comparison") or {}).get("changed_members", {}).get("top10"),
                "top20_changed_members_count": (challenger_decision.get("variant_pool_comparison") or {}).get("changed_members", {}).get("top20"),
            },
            {
                "session_role": "bottom15_audit",
                "session_dir": str(bottom15_audit_session),
                "decision": bottom15_decision.get("decision"),
                "route_summary": route_quality.get("schema_version"),
                "bottom15_context": context_concentration.get("schema_version"),
            },
            {
                "session_role": "metric_rebuild",
                "session_dir": str(rebuild_session),
                "decision": rebuild_precheck.get("decision"),
                "reconciled": rebuild_precheck.get("rebuild_ready"),
                "metric_contract": metric_contract.get("schema_version"),
            },
        ],
        "why_frozen": "topk_quality_improved_but_bottom15_false_positive_cost_not_cleanly_guardable",
        "multi_axis_risk": "further retuning would likely require multi-axis conditions and would be overfit-prone",
        "notes": [
            "require-confirmation materially moved top5/top10 membership",
            "top5/top10 mean return and path quality improved",
            "bottom15 audit showed the added winners and added losers overlap in broad observable context",
            "rebuilt bottom15 metrics are reconciled and supersede the old inconsistent summary",
        ],
    }


def _build_metric_reconciliation_summary(
    *,
    rebuild_session: Path,
    metric_contract: dict[str, Any],
    rebuild_summary: dict[str, Any],
    rebuild_diff: dict[str, Any],
    variant_pool_reconciliation: dict[str, Any],
    freeze_precheck: dict[str, Any],
) -> dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "generated_at": _utc_now(),
        "old_bottom15_summary_invalidated": True,
        "rebuilt_summary_source": str(rebuild_session / "bottom15_delta_summary_rebuilt.json"),
        "rebuilt_summary_matches": {
            "authoritative_parquet": bool(variant_pool_reconciliation.get("summary", {}).get("all_checks_passed")),
            "variant_pool_comparison": bool(variant_pool_reconciliation.get("summary", {}).get("all_checks_passed")),
        },
        "metric_definitions": metric_contract.get("definitions", {}),
        "definitions": {
            "count": "absolute count of rows in the selection bucket",
            "rate": "count divided by selected_count",
            "top15_capture": "top15_count divided by total top15 labels in the candidate surface",
            "bottom15_contamination": "bottom15_count divided by selected_count",
            "newly_added": "rows present in variant topK but not original topK",
            "removed": "rows present in original topK but not variant topK",
            "unchanged": "rows present in both original and variant topK",
        },
        "reconciliation_notes": [
            "the rebuilt summary recomputes directly from candidate_confirmation_rows.parquet",
            "the old bottom15_delta_summary.json is superseded because it did not recompute from the authoritative parquet",
            "the rebuilt summary and variant pool comparison now agree on top5/top10/top20 selection counts and bottom15/top15 counts",
        ],
        "freeze_precheck_result": freeze_precheck.get("decision"),
        "rebuilt_summary_schema_version": rebuild_summary.get("schema_version"),
        "rebuilt_diff_mismatch_count": len(rebuild_diff.get("mismatches", {})),
    }


def _build_freeze_decision() -> dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "generated_at": _utc_now(),
        "freeze_line_name": FREEZE_LINE_NAME,
        "decision": "freeze_observable_regime_false_positive_require_confirmation_line",
        "status": "explanation_only",
        "reason": "topk_quality_improved_but_bottom15_false_positive_cost_not_cleanly_guardable",
        "metric_source_status": "rebuilt_metrics_reconciled",
        "superseded_old_artifacts": True,
        "promote_ready": False,
        "meemee_reflectable": False,
        "same_condition_contract": True,
        "not_for_policy_use": True,
        "typed_reasons": [
            "topk_quality_improved_but_bottom15_false_positive_cost_not_cleanly_guardable",
            "bottom15_false_positive_cost_remains_unresolved",
            "added_winners_and_losers_overlap_in_the_same_broad_context",
        ],
    }


def _build_reusable_findings() -> dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "generated_at": _utc_now(),
        "findings": [
            {
                "finding_id": "enriched_context_backfill_was_valuable",
                "status": "confirmed",
                "finding": "the enriched context backfill made the bottom15 family visible enough to diagnose instead of leaving it unknown",
            },
            {
                "finding_id": "observable_regime_false_positive_is_real_and_large",
                "status": "confirmed",
                "finding": "observable_regime_false_positive is a real, large, top5-heavy failure family",
            },
            {
                "finding_id": "require_confirmation_moved_topk",
                "status": "confirmed",
                "finding": "require-confirmation materially moved top5/top10 membership and improved average top5/top10 return and path quality",
            },
            {
                "finding_id": "risk_family_presence_reduced",
                "status": "confirmed",
                "finding": "risk-family presence was reduced in the challenger relative to the original ranking",
            },
            {
                "finding_id": "bottom15_cost_unresolved",
                "status": "confirmed",
                "finding": "bottom15 / false-positive cost remains unresolved and was not cleanly guardable with one more observable condition",
            },
            {
                "finding_id": "same_context_overlap",
                "status": "confirmed",
                "finding": "added winners and added losers overlap in the same broad observable state context",
            },
            {
                "finding_id": "future_value",
                "status": "provisional",
                "finding": "the signal may still be useful for explanation, human review, future richer-feature design, and future data enrichment validation",
            },
        ],
    }


def _build_not_for_policy_reasons() -> dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "generated_at": _utc_now(),
        "reasons": [
            "not keep-grade",
            "no clean one-guard bottom15 fix was found",
            "bottom15 / false-positive cost remains unresolved",
            "added top15 and added bottom15 overlap heavily",
            "non-risk backfill side effect complicates interpretation",
            "further retuning would likely require multi-axis conditions",
            "no MeeMee or production ranking reflection is justified",
        ],
    }


def _build_future_reopen_conditions() -> dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "generated_at": _utc_now(),
        "reopen_only_if": [
            "a new no-lookahead-safe feature becomes available that can separate added winners from added losers",
            "the new feature can be evaluated on the same enriched candidate surface without future leakage",
            "the rule remains one-axis rather than widening into a multi-axis filter",
        ],
        "example_fields": [
            "stronger volume confirmation with sufficient coverage",
            "reliable entry-strength / signal-quality score",
            "richer candle confirmation field",
            "event / earnings / dividend / rights context",
            "improved liquidity / participation signal",
        ],
        "do_not_reopen_based_only_on": [
            "monthly state",
            "weekly state",
            "daily state",
            "broad regime label",
            "rank",
            "original score",
            "one more confirmation-route tweak",
        ],
        "notes": [
            "the current line is explanation-only and should not be reopened with the same field surface",
            "any future reopening should stay one-axis and require a genuinely new no-lookahead-safe field",
        ],
    }


def build_freeze_summary(
    output_root: Path,
    *,
    session_id: str | None = None,
    challenger_session: Path = DEFAULT_CHALLENGER_SESSION,
    bottom15_audit_session: Path = DEFAULT_BOTTOM15_AUDIT_SESSION,
    rebuild_session: Path = DEFAULT_REBUILD_SESSION,
) -> dict[str, Any]:
    challenger_session = _resolve_path(challenger_session, DEFAULT_CHALLENGER_SESSION)
    bottom15_audit_session = _resolve_path(bottom15_audit_session, DEFAULT_BOTTOM15_AUDIT_SESSION)
    rebuild_session = _resolve_path(rebuild_session, DEFAULT_REBUILD_SESSION)

    challenger_decision = _load_json(challenger_session / "observable_regime_false_positive_require_confirmation_v1_decision.json")
    variant_pool = _load_json(challenger_session / "variant_pool_comparison.json")
    confirmation_policy = _load_json(challenger_session / "confirmation_policy.json")
    route_quality = _load_json(bottom15_audit_session / "confirmation_route_quality_summary.json")
    context_concentration = _load_json(bottom15_audit_session / "bottom15_context_concentration_summary.json")
    bottom15_decision = _load_json(bottom15_audit_session / "observable_regime_false_positive_bottom15_audit_v1_decision.json")
    rebuild_summary = _load_json(rebuild_session / "bottom15_delta_summary_rebuilt.json")
    rebuild_diff = _load_json(rebuild_session / "bottom15_delta_summary_diff.json")
    variant_pool_reconciliation = _load_json(rebuild_session / "variant_pool_reconciliation.json")
    supersedes = _load_json(rebuild_session / "supersedes_bottom15_delta_summary.json")
    freeze_precheck = _load_json(rebuild_session / "freeze_precheck_after_rebuild.json")
    metric_contract = _load_json(rebuild_session / "metric_definition_contract.json")

    lineage_summary = _build_lineage_summary(
        challenger_session=challenger_session,
        bottom15_audit_session=bottom15_audit_session,
        rebuild_session=rebuild_session,
        challenger_decision={**challenger_decision, "variant_pool_comparison": variant_pool},
        route_quality=route_quality,
        context_concentration=context_concentration,
        bottom15_decision=bottom15_decision,
        rebuild_precheck=freeze_precheck,
        metric_contract=metric_contract,
    )
    metric_reconciliation_summary = _build_metric_reconciliation_summary(
        rebuild_session=rebuild_session,
        metric_contract=metric_contract,
        rebuild_summary=rebuild_summary,
        rebuild_diff=rebuild_diff,
        variant_pool_reconciliation=variant_pool_reconciliation,
        freeze_precheck=freeze_precheck,
    )
    freeze_decision = _build_freeze_decision()
    reusable_findings = _build_reusable_findings()
    not_for_policy_reasons = _build_not_for_policy_reasons()
    future_reopen_conditions = _build_future_reopen_conditions()

    final_session_id = session_id or _make_session_id()
    session_root = output_root / final_session_id
    session_root.mkdir(parents=True, exist_ok=False)

    _write_json(session_root / "run_manifest.json", {
        "schema_version": SCHEMA_VERSION,
        "script": SCRIPT_NAME,
        "session_id": final_session_id,
        "created_utc": _utc_now(),
        "output_root": str(output_root),
        "challenger_session": str(challenger_session),
        "bottom15_audit_session": str(bottom15_audit_session),
        "rebuild_session": str(rebuild_session),
        "jobs_requested": 1,
        "jobs_supported": 1,
        "decision": freeze_decision["decision"],
        "meemee_reflectable": False,
        "production_ranking_changed": False,
        "superseded_old_artifacts": True,
    })
    _write_json(session_root / "input_resolution.json", {
        "schema_version": SCHEMA_VERSION,
        "resolved_status": "ok",
        "challenger_session": str(challenger_session),
        "bottom15_audit_session": str(bottom15_audit_session),
        "rebuild_session": str(rebuild_session),
        "confirmation_policy_path": str(challenger_session / "confirmation_policy.json"),
        "variant_pool_comparison_path": str(challenger_session / "variant_pool_comparison.json"),
        "candidate_confirmation_rows_path": str(challenger_session / "candidate_confirmation_rows.parquet"),
        "bottom15_delta_summary_rebuilt_path": str(rebuild_session / "bottom15_delta_summary_rebuilt.json"),
        "variant_pool_reconciliation_path": str(rebuild_session / "variant_pool_reconciliation.json"),
        "freeze_precheck_path": str(rebuild_session / "freeze_precheck_after_rebuild.json"),
    })
    _write_json(session_root / "lineage_summary.json", lineage_summary)
    _write_json(session_root / "metric_reconciliation_summary.json", metric_reconciliation_summary)
    _write_json(session_root / "freeze_decision.json", freeze_decision)
    _write_json(session_root / "reusable_findings.json", reusable_findings)
    _write_json(session_root / "not_for_policy_reasons.json", not_for_policy_reasons)
    _write_json(session_root / "future_reopen_conditions.json", future_reopen_conditions)
    _write_json(
        session_root / "_ARTIFACT_COMPLETE.json",
        {
            "schema_version": SCHEMA_VERSION,
            "generated_at": _utc_now(),
            "session_id": final_session_id,
            "output_dir": str(session_root),
            "artifact_list": [
                "run_manifest.json",
                "input_resolution.json",
                "lineage_summary.json",
                "metric_reconciliation_summary.json",
                "freeze_decision.json",
                "reusable_findings.json",
                "not_for_policy_reasons.json",
                "future_reopen_conditions.json",
                "_ARTIFACT_COMPLETE.json",
            ],
            "verification_status": "generated",
            "commands_run": [
                f"python {SCRIPT_NAME}.py --output-root {output_root}",
                "git status --short",
                "git diff --name-only",
            ],
            "git_status_short": _safe_git_output(["git", "status", "--short"]),
            "git_diff_name_only": _safe_git_output(["git", "diff", "--name-only"]),
            "notes": [
                "freeze summary for the observable_regime_false_positive require-confirmation line",
                "old bottom15 summary artifacts are superseded by the rebuilt metrics",
            ],
        },
    )

    return {
        "session_id": final_session_id,
        "output_dir": str(session_root),
        "artifacts": {
            "run_manifest.json": str(session_root / "run_manifest.json"),
            "input_resolution.json": str(session_root / "input_resolution.json"),
            "lineage_summary.json": str(session_root / "lineage_summary.json"),
            "metric_reconciliation_summary.json": str(session_root / "metric_reconciliation_summary.json"),
            "freeze_decision.json": str(session_root / "freeze_decision.json"),
            "reusable_findings.json": str(session_root / "reusable_findings.json"),
            "not_for_policy_reasons.json": str(session_root / "not_for_policy_reasons.json"),
            "future_reopen_conditions.json": str(session_root / "future_reopen_conditions.json"),
            "_ARTIFACT_COMPLETE.json": str(session_root / "_ARTIFACT_COMPLETE.json"),
        },
        "decision": freeze_decision["decision"],
    }


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Freeze the observable_regime_false_positive require-confirmation line.")
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT, help="Directory that will receive the freeze summary session.")
    parser.add_argument("--session-id", default=None, help="Optional fixed session id for deterministic tests.")
    parser.add_argument("--challenger-session", type=Path, default=DEFAULT_CHALLENGER_SESSION)
    parser.add_argument("--bottom15-audit-session", type=Path, default=DEFAULT_BOTTOM15_AUDIT_SESSION)
    parser.add_argument("--rebuild-session", type=Path, default=DEFAULT_REBUILD_SESSION)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    build_freeze_summary(
        args.output_root,
        session_id=args.session_id,
        challenger_session=args.challenger_session,
        bottom15_audit_session=args.bottom15_audit_session,
        rebuild_session=args.rebuild_session,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
