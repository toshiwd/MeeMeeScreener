from __future__ import annotations

import argparse
import json
import shutil
import subprocess
import sys
from collections import Counter, defaultdict
from datetime import date, datetime, timezone
from pathlib import Path
from statistics import mean, median
from typing import Any

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

SCRIPT_NAME = "tradex_long_action_policy_rank_guard_tighten_v1"
FAMILY_ID = "long_action_policy_rank_guard_tighten_v1"
CURRENT_GATE_NAME = "long_entry_cash_gate_v1"
PRIOR_RELAXER_NAME = "long_entry_cash_gate_entry_signal_relax_v1"
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\research_sessions\long_action_policy_foundation_v1_rank_guard_tighten")
DEFAULT_PRIOR_REDESIGN_DIR = Path(r"G:\Tradex\research_sessions\long_action_policy_foundation_v1_gate_redesign\20260501T031805Z-39d3bb84")


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _make_session_id() -> str:
    return f"{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}-{datetime.now(timezone.utc).microsecond:06d}"


def _json_ready(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _json_ready(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_json_ready(item) for item in value]
    if isinstance(value, tuple):
        return [_json_ready(item) for item in value]
    if isinstance(value, set):
        return [_json_ready(item) for item in sorted(value, key=str)]
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    return value


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(_json_ready(payload), ensure_ascii=False, indent=2, sort_keys=True, default=str) + "\n", encoding="utf-8")


def _load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _safe_git_output(args: list[str]) -> str:
    try:
        completed = subprocess.run(args, cwd=REPO_ROOT, capture_output=True, text=True, check=False)
        return (completed.stdout or completed.stderr or "").strip()
    except Exception as exc:  # pragma: no cover - best effort metadata
        return f"unavailable: {exc}"


def _ensure_exists(path: Path) -> Path:
    if not path.exists():
        raise FileNotFoundError(path)
    return path


def _load_cases(path: Path) -> pd.DataFrame:
    frame = pd.read_parquet(_ensure_exists(path))
    for column in ("variant_reason_codes", "baseline_reason_codes", "reason_codes_key", "skip_class", "month_key", "week_key", "window_id", "window_label", "symbol"):
        if column in frame.columns:
            frame[column] = frame[column].astype("string")
    return frame


def _as_list(values: Any) -> list[str]:
    if values is None:
        return []
    if isinstance(values, list):
        return [str(item) for item in values if str(item)]
    if isinstance(values, tuple):
        return [str(item) for item in values if str(item)]
    if isinstance(values, pd.Series):  # pragma: no cover - convenience
        return [str(item) for item in values.tolist() if str(item)]
    text = str(values).strip()
    if text.startswith("[") and text.endswith("]"):
        try:
            parsed = json.loads(text.replace("'", '"'))
            if isinstance(parsed, list):
                return [str(item) for item in parsed if str(item)]
        except Exception:
            pass
    if "," in text:
        return [item.strip() for item in text.split(",") if item.strip()]
    return [text] if text else []


def _rank_diagnostic(cases: pd.DataFrame) -> dict[str, Any]:
    skipped = cases.loc[(cases["baseline_action"] == "buy") & (cases["variant_action"].isin(["stay_cash", "hold"]))].copy()
    restored = skipped.loc[
        (skipped["reason_codes_key"] == "entry_signal")
        & (skipped["variant_reason_codes"].map(lambda values: _as_list(values) == ["timing_block"]))
        & (skipped["baseline_score"].fillna(0.0).astype(float) >= 0.05)
    ].copy()

    grouped_rows: list[dict[str, Any]] = []
    for rank, group in restored.groupby("baseline_rank", dropna=False):
        rank_value = int(rank)
        good = group.loc[group["skip_class"] == "skipped_good_buy"].copy()
        bad = group.loc[group["skip_class"] == "skipped_bad_buy"].copy()
        grouped_rows.append(
            {
                "baseline_rank": rank_value,
                "restored_total": int(len(group)),
                "restored_good_buy_count": int(len(good)),
                "restored_bad_buy_count": int(len(bad)),
                "restored_good_buy_ret_20_mean": float(good["ret_20"].mean()) if not good.empty else None,
                "restored_good_buy_ret_20_median": float(good["ret_20"].median()) if not good.empty else None,
                "restored_bad_buy_ret_20_mean": float(bad["ret_20"].mean()) if not bad.empty else None,
                "restored_bad_buy_ret_20_median": float(bad["ret_20"].median()) if not bad.empty else None,
                "entry_delay_cost_mean": float(group["later_buy_delay_cost_20d"].dropna().astype(float).mean()) if "later_buy_delay_cost_20d" in group.columns and group["later_buy_delay_cost_20d"].notna().any() else None,
            }
        )

    ranks_with_good = {row["baseline_rank"] for row in grouped_rows if row["restored_good_buy_count"] > 0}
    ranks_with_bad = {row["baseline_rank"] for row in grouped_rows if row["restored_bad_buy_count"] > 0}
    overlap = sorted(ranks_with_good & ranks_with_bad)
    separation_supported = not overlap

    illustrative_cutoffs = []
    for cutoff in (4, 5, 8):
        kept = restored.loc[restored["baseline_rank"].astype(int) <= cutoff].copy()
        illustrative_cutoffs.append(
            {
                "cutoff": cutoff,
                "restored_good_buy": int((kept["skip_class"] == "skipped_good_buy").sum()),
                "restored_bad_buy": int((kept["skip_class"] == "skipped_bad_buy").sum()),
                "restored_total": int(len(kept)),
            }
        )

    return {
        "schema_version": "tradex_long_action_policy_rank_guard_tighten_v1",
        "restored_candidate_count": int(len(restored)),
        "restored_buy_total": int(len(restored)),
        "restored_good_buy_total": int((restored["skip_class"] == "skipped_good_buy").sum()),
        "restored_bad_buy_total": int((restored["skip_class"] == "skipped_bad_buy").sum()),
        "rank_rows": sorted(grouped_rows, key=lambda row: row["baseline_rank"]),
        "ranks_with_good": sorted(ranks_with_good),
        "ranks_with_bad": sorted(ranks_with_bad),
        "overlap_ranks": overlap,
        "single_cutoff_justified": separation_supported,
        "illustrative_cutoffs": illustrative_cutoffs,
        "notes": [
            "rank-only continuation is not justified when good and bad restored buys overlap at the same rank",
            "illustrative cutoffs are diagnostics only and are not used as policy rules",
        ],
    }


def _policy_spec(diagnostic: dict[str, Any]) -> dict[str, Any]:
    return {
        "schema_version": "tradex_long_action_policy_rank_guard_tighten_v1",
        "family_id": FAMILY_ID,
        "baseline_name": "long_entry_cash_gate_baseline_v1",
        "current_gate_name": CURRENT_GATE_NAME,
        "prior_relaxer_name": PRIOR_RELAXER_NAME,
        "candidate_variant_name": "long_entry_cash_gate_rank_guard_tighten_v1",
        "status": "insufficient_rank_separation",
        "selected_rank_cutoff": None,
        "preserved_conditions": [
            "next_session_open",
            "cost_slippage_enabled",
            "no_lookahead",
            "baseline_action == buy",
            "current_gate_action in {stay_cash, hold}",
            "reason_codes_key == entry_signal",
            "variant_reason_codes == ['timing_block']",
            "baseline_score >= 0.05",
        ],
        "excluded_conditions": [
            "exit logic",
            "hedge logic",
            "add-on logic",
            "rotation logic",
            "short-side logic",
            "score logic changes",
        ],
        "reason": "restored-good and restored-bad buys overlap at baseline_rank 4, so a rank-only continuation is not justified",
        "no_new_challenger": True,
        "diagnostic_summary": {
            "overlap_ranks": diagnostic["overlap_ranks"],
            "single_cutoff_justified": diagnostic["single_cutoff_justified"],
        },
    }


def _copy_context_artifacts(prior_dir: Path, output_dir: Path) -> dict[str, str]:
    filenames = [
        "branch_effect_audit.json",
        "portfolio_economic_comparison.json",
        "skipped_buy_restoration_summary.json",
        "entry_delay_cost_summary.json",
        "monthly_effectiveness_summary.json",
        "regime_effectiveness_summary.json",
        "drawdown_attribution_summary.json",
        "restored_buy_cases.parquet",
        "remaining_skipped_buy_cases.parquet",
    ]
    copied: dict[str, str] = {}
    for name in filenames:
        src = _ensure_exists(prior_dir / name)
        dst = output_dir / name
        dst.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(src, dst)
        copied[name] = str(dst)
    return copied


def build_rank_guard_tighten_review(
    output_root: Path,
    *,
    prior_design_dir: Path = DEFAULT_PRIOR_REDESIGN_DIR,
    jobs: int = 1,
) -> dict[str, Any]:
    prior_design_dir = _ensure_exists(prior_design_dir)
    cases = _load_cases(prior_design_dir / "restored_buy_cases.parquet")
    diagnostic = _rank_diagnostic(cases)
    decision = {
        "schema_version": "tradex_long_action_policy_rank_guard_tighten_v1",
        "final_status": "insufficient_rank_separation",
        "reason": diagnostic["notes"][0],
        "selected_rank_cutoff": None,
        "notes": ["no new challenger was implemented because the rank-only continuation failed the separation test"],
    }
    policy_spec = _policy_spec(diagnostic)

    session_dir = output_root / _make_session_id()
    session_dir.mkdir(parents=True, exist_ok=False)
    copied = _copy_context_artifacts(prior_design_dir, session_dir)

    run_manifest = {
        "schema_version": "tradex_long_action_policy_rank_guard_tighten_v1",
        "family_id": FAMILY_ID,
        "baseline_name": "long_entry_cash_gate_baseline_v1",
        "current_gate_name": CURRENT_GATE_NAME,
        "prior_relaxer_name": PRIOR_RELAXER_NAME,
        "candidate_variant_name": "long_entry_cash_gate_rank_guard_tighten_v1",
        "prior_design_dir": str(prior_design_dir),
        "output_dir": str(session_dir),
        "jobs_requested": int(jobs),
        "jobs_supported": 1,
        "candidate_generated": False,
        "research_fallback": False,
        "notes": [
            "rank-only continuation stopped because good and bad restored buys overlap by rank",
            "context artifacts are copied from the prior redesign session for traceability",
        ],
    }
    input_resolution = {
        "schema_version": "tradex_long_action_policy_rank_guard_tighten_v1",
        "prior_design_dir": str(prior_design_dir),
        "prior_design_found": prior_design_dir.exists(),
        "restored_cases_found": (prior_design_dir / "restored_buy_cases.parquet").exists(),
        "remaining_cases_found": (prior_design_dir / "remaining_skipped_buy_cases.parquet").exists(),
        "decision_mode": "diagnostic_only",
        "notes": ["no new challenger generated"],
    }

    rank_guard_policy_spec = policy_spec
    artifacts = {
        "run_manifest.json": run_manifest,
        "input_resolution.json": input_resolution,
        "rank_guard_diagnostic.json": diagnostic,
        "rank_guard_policy_spec.json": rank_guard_policy_spec,
        "portfolio_economic_comparison.json": _load_json(session_dir / "portfolio_economic_comparison.json"),
        "branch_effect_audit.json": _load_json(session_dir / "branch_effect_audit.json"),
        "skipped_buy_restoration_summary.json": _load_json(session_dir / "skipped_buy_restoration_summary.json"),
        "entry_delay_cost_summary.json": _load_json(session_dir / "entry_delay_cost_summary.json"),
        "monthly_effectiveness_summary.json": _load_json(session_dir / "monthly_effectiveness_summary.json"),
        "regime_effectiveness_summary.json": _load_json(session_dir / "regime_effectiveness_summary.json"),
        "drawdown_attribution_summary.json": _load_json(session_dir / "drawdown_attribution_summary.json"),
        "rank_guard_tighten_decision.json": decision,
    }

    # Re-write the copied JSONs into the new session to ensure a coherent artifact set.
    for filename in ("portfolio_economic_comparison.json", "branch_effect_audit.json", "skipped_buy_restoration_summary.json", "entry_delay_cost_summary.json", "monthly_effectiveness_summary.json", "regime_effectiveness_summary.json", "drawdown_attribution_summary.json"):
        _write_json(session_dir / filename, artifacts[filename])

    _write_json(session_dir / "run_manifest.json", run_manifest)
    _write_json(session_dir / "input_resolution.json", input_resolution)
    _write_json(session_dir / "rank_guard_diagnostic.json", diagnostic)
    _write_json(session_dir / "rank_guard_policy_spec.json", rank_guard_policy_spec)
    _write_json(session_dir / "rank_guard_tighten_decision.json", decision)

    complete = {
        "schema_version": "tradex_long_action_policy_rank_guard_tighten_v1",
        "family_id": FAMILY_ID,
        "generated_at": _utc_now(),
        "session_id": session_dir.name,
        "output_dir": str(session_dir),
        "artifact_list": [
            "run_manifest.json",
            "input_resolution.json",
            "rank_guard_diagnostic.json",
            "rank_guard_policy_spec.json",
            "branch_effect_audit.json",
            "portfolio_economic_comparison.json",
            "skipped_buy_restoration_summary.json",
            "restored_buy_cases.parquet",
            "remaining_skipped_buy_cases.parquet",
            "entry_delay_cost_summary.json",
            "monthly_effectiveness_summary.json",
            "regime_effectiveness_summary.json",
            "drawdown_attribution_summary.json",
            "rank_guard_tighten_decision.json",
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
            "TRADEX-only rank-guard diagnostic pass",
            "no new challenger was generated because no rank cutoff separated restored-good from restored-bad buys",
        ],
    }
    _write_json(session_dir / "_ARTIFACT_COMPLETE.json", complete)

    return {
        "session_id": session_dir.name,
        "output_dir": str(session_dir),
        "artifacts": {name: str(session_dir / name) for name in complete["artifact_list"]},
        "complete": str(session_dir / "_ARTIFACT_COMPLETE.json"),
        "candidate_generated": False,
    }


def _build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="TRADEX rank-guard tighten diagnostic runner")
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT, help="Root output directory for the research session")
    parser.add_argument("--prior-design-dir", type=Path, default=DEFAULT_PRIOR_REDESIGN_DIR, help="Prior redesign artifact directory")
    parser.add_argument("--jobs", type=int, default=1, help="Requested job count; recorded but executed sequentially")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _build_arg_parser().parse_args(argv)
    result = build_rank_guard_tighten_review(args.output_root, prior_design_dir=args.prior_design_dir, jobs=args.jobs)
    print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
