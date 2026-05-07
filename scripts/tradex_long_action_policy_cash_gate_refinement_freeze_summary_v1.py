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

SCRIPT_NAME = "tradex_long_action_policy_cash_gate_refinement_freeze_summary_v1"
SCHEMA_VERSION = "tradex_long_action_policy_cash_gate_refinement_freeze_summary_v1"
FREEZE_LINE_NAME = "long_entry_cash_gate_refinement_line"

DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\research_freeze_summaries\long_entry_cash_gate_refinement")
DEFAULT_GATE_REDESIGN_DIR = Path(r"G:\Tradex\research_sessions\long_action_policy_foundation_v1_gate_redesign\20260501T031805Z-39d3bb84")
DEFAULT_RANK_GUARD_DIR = Path(r"G:\Tradex\research_sessions\long_action_policy_foundation_v1_rank_guard_tighten\20260501T032807Z-466663")
DEFAULT_SCORE_GUARD_DIR = Path(r"G:\Tradex\research_sessions\long_action_policy_foundation_v1_same_day_score_guard_diagnostic\20260501T033801Z-186859")


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
    path.write_text(
        json.dumps(_json_ready(payload), ensure_ascii=False, indent=2, sort_keys=True, default=str) + "\n",
        encoding="utf-8",
    )
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


def _source_snapshot(session_dir: Path) -> dict[str, Any]:
    snapshot: dict[str, Any] = {"session_dir": str(session_dir), "exists": session_dir.exists()}
    if not session_dir.exists():
        snapshot["available"] = False
        return snapshot
    snapshot["available"] = True
    snapshot["files"] = sorted(path.name for path in session_dir.iterdir())
    return snapshot


def _build_lineage_summary(
    *,
    gate_redesign_dir: Path,
    rank_guard_dir: Path,
    score_guard_dir: Path,
    gate_redesign_restoration: dict[str, Any],
    gate_redesign_branch_effect: dict[str, Any],
    rank_guard_decision: dict[str, Any],
    rank_guard_diagnostic: dict[str, Any],
    score_guard_decision: dict[str, Any],
    score_guard_conflict: dict[str, Any],
) -> dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "generated_at": _utc_now(),
        "freeze_line_name": FREEZE_LINE_NAME,
        "decision": "freeze_current_cash_gate_refinement_line",
        "status": "explanation_only",
        "same_condition_contract": True,
        "source_sessions": [
            {
                "session_role": "timing_relaxer",
                "session_dir": str(gate_redesign_dir),
                "decision": "hold_needs_larger_window_validation",
                "branch_real": bool((gate_redesign_branch_effect or {}).get("branch_effect_present")),
                "restored_buy_total": gate_redesign_restoration.get("restored_buy_count"),
                "restored_good_buy": gate_redesign_restoration.get("restored_good_buy"),
                "restored_bad_buy": gate_redesign_restoration.get("restored_bad_buy"),
                "false_negative_skip_reduction": gate_redesign_restoration.get("false_negative_skip_reduction"),
                "true_positive_skip_retention": gate_redesign_restoration.get("true_positive_skip_retention"),
                "portfolio_delta": (gate_redesign_branch_effect.get("current_vs_redesign") or {}).get("diff_counts") if isinstance(gate_redesign_branch_effect, dict) else None,
            },
            {
                "session_role": "rank_guard_stop",
                "session_dir": str(rank_guard_dir),
                "decision": rank_guard_decision.get("final_status"),
                "overlap_ranks": rank_guard_diagnostic.get("overlap_ranks"),
                "ranks_with_good": rank_guard_diagnostic.get("ranks_with_good"),
                "ranks_with_bad": rank_guard_diagnostic.get("ranks_with_bad"),
                "single_cutoff_justified": rank_guard_diagnostic.get("single_cutoff_justified"),
            },
            {
                "session_role": "same_day_score_guard_stop",
                "session_dir": str(score_guard_dir),
                "decision": score_guard_decision.get("final_status"),
                "best_separating_field": score_guard_conflict.get("best_separating_field"),
                "best_separating_score": score_guard_conflict.get("best_separating_score"),
                "rank_overlap_ranks": score_guard_conflict.get("rank_overlap_ranks"),
            },
        ],
        "why_frozen": "rank_and_same_day_score_fields_do_not_separate_restored_good_from_restored_bad_buys",
        "multi_axis_risk": "adding regime month or symbol filters would turn this into a multi-axis overfit-prone rule",
        "notes": [
            "current gate is branch-real and economically active",
            "timing relaxer rescued some profitable skipped buys",
            "rank-only continuation failed",
            "same-day score guard diagnostic failed",
        ],
    }


def _build_freeze_decision(lineage_summary: dict[str, Any]) -> dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "generated_at": _utc_now(),
        "freeze_line_name": lineage_summary.get("freeze_line_name"),
        "decision": "freeze_current_cash_gate_refinement_line",
        "status": "explanation_only",
        "reason": "rank_and_same_day_score_fields_do_not_separate_restored_good_from_restored_bad_buys",
        "promote_ready": False,
        "meemee_reflectable": False,
        "same_condition_contract": True,
        "not_for_policy_use": True,
        "typed_reasons": [
            "rank_and_same_day_score_fields_do_not_separate_restored_good_from_restored_bad_buys",
            "multi_axis_overfit_risk_if_regime_month_symbol_filters_are_added",
        ],
        "source_decisions": [
            {"session_role": "timing_relaxer", "decision": "hold_needs_larger_window_validation"},
            {"session_role": "rank_guard_stop", "decision": "insufficient_rank_separation"},
            {"session_role": "same_day_score_guard_stop", "decision": "insufficient_score_separation"},
        ],
    }


def _build_reusable_findings(
    gate_redesign_restoration: dict[str, Any],
    gate_redesign_branch_effect: dict[str, Any],
    rank_guard_diagnostic: dict[str, Any],
    score_guard_conflict: dict[str, Any],
) -> dict[str, Any]:
    findings = [
        {
            "finding_id": "current_gate_branch_real",
            "status": "confirmed",
            "finding": "current gate and relaxer branch into executed state under next-session-open replay",
            "evidence": {
                "branch_effect_present": bool(gate_redesign_branch_effect.get("branch_effect_present")),
                "current_vs_redesign_action_diffs": (gate_redesign_branch_effect.get("current_vs_redesign") or {}).get("diff_counts", {}).get("action"),
                "baseline_vs_current_action_diffs": (gate_redesign_branch_effect.get("baseline_vs_current") or {}).get("diff_counts", {}).get("action"),
            },
        },
        {
            "finding_id": "timing_relaxer_recovers_profitable_skipped_buys",
            "status": "confirmed",
            "finding": "the timing relaxer rescued part of the profitable skipped-buy bucket",
            "evidence": {
                "restored_buy_total": gate_redesign_restoration.get("restored_buy_count"),
                "restored_good_buy": gate_redesign_restoration.get("restored_good_buy"),
                "restored_bad_buy": gate_redesign_restoration.get("restored_bad_buy"),
                "restored_good_ret_20_mean": gate_redesign_restoration.get("restored_good_ret_20_mean"),
                "restored_bad_ret_20_mean": gate_redesign_restoration.get("restored_bad_ret_20_mean"),
            },
        },
        {
            "finding_id": "current_fields_insufficient_for_policy_use",
            "status": "confirmed",
            "finding": "rank, baseline score, top candidate score, and score gap do not cleanly separate restored-good from restored-bad buys",
            "evidence": {
                "rank_overlap_ranks": rank_guard_diagnostic.get("overlap_ranks"),
                "same_day_score_best_field": score_guard_conflict.get("best_separating_field"),
                "same_day_score_best_score": score_guard_conflict.get("best_separating_score"),
            },
        },
        {
            "finding_id": "same_day_score_proxies_explanatory_only",
            "status": "confirmed",
            "finding": "same-day score proxies are informative but not actionable as a policy guard",
            "evidence": {
                "rank_guard_final_status": "insufficient_rank_separation",
                "same_day_score_final_status": "insufficient_score_separation",
            },
        },
        {
            "finding_id": "future_richer_entry_strength_fields_needed",
            "status": "provisional",
            "finding": "if the line is revisited, the replay surface should expose richer no-lookahead-safe entry-strength fields",
            "evidence": {
                "required_field_family": [
                    "entry_strength",
                    "signal_quality",
                    "confidence_score",
                    "reason_code_strength",
                    "entry_threshold_distance",
                    "native_same_day_score_components",
                ]
            },
        },
    ]
    return {
        "schema_version": SCHEMA_VERSION,
        "generated_at": _utc_now(),
        "findings": findings,
    }


def _build_not_for_policy_reasons() -> dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "generated_at": _utc_now(),
        "reasons": [
            {
                "reason_id": "rank_not_separating",
                "reason": "rank alone does not cleanly separate restored-good from restored-bad buys",
                "excluded_fields": ["baseline_rank"],
            },
            {
                "reason_id": "score_not_separating",
                "reason": "same-day score proxies do not cleanly separate restored-good from restored-bad buys",
                "excluded_fields": ["baseline_score", "top_candidate_score", "score_gap"],
            },
            {
                "reason_id": "multi_axis_overfit_risk",
                "reason": "adding regime, month, or symbol filters now would widen the rule into a multi-axis overfit-prone gate",
                "excluded_fields": ["market_regime", "month_key", "week_key", "symbol"],
            },
        ],
        "not_for_policy_use": True,
    }


def _build_future_reopen_conditions() -> dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "generated_at": _utc_now(),
        "reopen_only_if": [
            "the replay surface exposes at least one new no-lookahead-safe entry-strength field",
            "the new field separates restored-good from restored-bad buys better than rank and score-gap proxies",
            "the field can be evaluated without future outcome leakage",
        ],
        "example_fields": [
            "entry_strength",
            "signal_quality",
            "confidence_score",
            "reason_code_strength",
            "entry_threshold_distance",
            "native_same_day_score_components",
        ],
        "do_not_reopen_based_only_on": [
            "rank",
            "baseline_score",
            "score_gap",
            "month",
            "symbol",
            "regime_filters",
        ],
        "notes": [
            "reopening should stay one-axis and should not widen into a multi-axis gate",
            "rank and current same-day score proxies are explanation-only, not policy-safe",
        ],
    }


def build_freeze_summary(
    output_root: Path,
    *,
    session_id: str | None = None,
    gate_redesign_dir: Path = DEFAULT_GATE_REDESIGN_DIR,
    rank_guard_dir: Path = DEFAULT_RANK_GUARD_DIR,
    score_guard_dir: Path = DEFAULT_SCORE_GUARD_DIR,
) -> dict[str, Any]:
    gate_redesign_dir = _resolve_path(gate_redesign_dir, DEFAULT_GATE_REDESIGN_DIR)
    rank_guard_dir = _resolve_path(rank_guard_dir, DEFAULT_RANK_GUARD_DIR)
    score_guard_dir = _resolve_path(score_guard_dir, DEFAULT_SCORE_GUARD_DIR)

    gate_redesign_restoration = _load_json(gate_redesign_dir / "skipped_buy_restoration_summary.json")
    gate_redesign_branch_effect = _load_json(gate_redesign_dir / "branch_effect_audit.json")
    rank_guard_decision = _load_json(rank_guard_dir / "rank_guard_tighten_decision.json")
    rank_guard_diagnostic = _load_json(rank_guard_dir / "rank_guard_diagnostic.json")
    score_guard_decision = _load_json(score_guard_dir / "same_day_score_guard_diagnostic_decision.json")
    score_guard_conflict = _load_json(score_guard_dir / "score_guard_conflict_summary.json")

    lineage_summary = _build_lineage_summary(
        gate_redesign_dir=gate_redesign_dir,
        rank_guard_dir=rank_guard_dir,
        score_guard_dir=score_guard_dir,
        gate_redesign_restoration=gate_redesign_restoration,
        gate_redesign_branch_effect=gate_redesign_branch_effect,
        rank_guard_decision=rank_guard_decision,
        rank_guard_diagnostic=rank_guard_diagnostic,
        score_guard_decision=score_guard_decision,
        score_guard_conflict=score_guard_conflict,
    )
    freeze_decision = _build_freeze_decision(lineage_summary)
    reusable_findings = _build_reusable_findings(
        gate_redesign_restoration,
        gate_redesign_branch_effect,
        rank_guard_diagnostic,
        score_guard_conflict,
    )
    not_for_policy_reasons = _build_not_for_policy_reasons()
    future_reopen_conditions = _build_future_reopen_conditions()

    final_session_id = session_id or _make_session_id()
    session_root = output_root / final_session_id
    session_root.mkdir(parents=True, exist_ok=False)

    _write_json(session_root / "lineage_summary.json", lineage_summary)
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
                "lineage_summary.json",
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
                "freeze summary for the long-entry cash-gate refinement line",
                "no new policy challenger was generated",
            ],
        },
    )

    return {
        "session_id": final_session_id,
        "output_dir": str(session_root),
        "artifacts": {
            "lineage_summary.json": str(session_root / "lineage_summary.json"),
            "freeze_decision.json": str(session_root / "freeze_decision.json"),
            "reusable_findings.json": str(session_root / "reusable_findings.json"),
            "not_for_policy_reasons.json": str(session_root / "not_for_policy_reasons.json"),
            "future_reopen_conditions.json": str(session_root / "future_reopen_conditions.json"),
            "_ARTIFACT_COMPLETE.json": str(session_root / "_ARTIFACT_COMPLETE.json"),
        },
        "decision": freeze_decision["decision"],
    }


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Freeze the long-entry cash-gate refinement line.")
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT, help="Directory that will receive the freeze summary session.")
    parser.add_argument("--session-id", default=None, help="Optional fixed session id for deterministic tests.")
    parser.add_argument("--gate-redesign-dir", type=Path, default=DEFAULT_GATE_REDESIGN_DIR, help="Timing-relaxer artifact directory.")
    parser.add_argument("--rank-guard-dir", type=Path, default=DEFAULT_RANK_GUARD_DIR, help="Rank-guard stop-session directory.")
    parser.add_argument("--score-guard-dir", type=Path, default=DEFAULT_SCORE_GUARD_DIR, help="Same-day score diagnostic directory.")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    build_freeze_summary(
        args.output_root,
        session_id=args.session_id,
        gate_redesign_dir=args.gate_redesign_dir,
        rank_guard_dir=args.rank_guard_dir,
        score_guard_dir=args.score_guard_dir,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
