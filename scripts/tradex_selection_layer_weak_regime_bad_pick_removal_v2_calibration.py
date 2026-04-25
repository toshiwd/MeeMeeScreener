from __future__ import annotations

import argparse
import copy
import json
import sys
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Callable

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts import tradex_selection_layer_weak_regime_bad_pick_removal as core  # noqa: E402


DEFAULT_INPUT_DIR = core.BASELINE_INPUT_DIR
DEFAULT_OUTPUT_DIR = Path(r"G:\Tradex\sample_replays\tradex_selection_layer_weak_regime_bad_pick_removal_v2_calibration_stress200")
DEFAULT_SOURCE_DB_PATH = core.DEFAULT_SOURCE_DB_PATH

VARIANT_SPECS: dict[str, dict[str, Any]] = {
    "v2_light": {
        "label": "weak_regime_bad_pick_removal_v2_light",
        "note": "Weaker lower-bucket veto to reduce starvation while preserving top5 protection.",
        "top6_10_threshold": 18.0,
        "top11_20_threshold": 22.0,
        "weak_mode": "base",
        "monthly_margin": 0.03,
    },
    "v2_boundary_only": {
        "label": "weak_regime_bad_pick_removal_v2_boundary_only",
        "note": "Apply the veto only to the top11-20 bucket; top6-10 stays mostly intact.",
        "top6_10_threshold": None,
        "top11_20_threshold": 21.0,
        "weak_mode": "base",
        "monthly_margin": 0.03,
    },
    "v2_high_confidence_only": {
        "label": "weak_regime_bad_pick_removal_v2_high_confidence_only",
        "note": "Remove only when the weak-regime evidence is strong enough to justify the veto.",
        "top6_10_threshold": 20.0,
        "top11_20_threshold": 25.0,
        "weak_mode": "strict",
        "monthly_margin": 0.0,
    },
}
VARIANT_ORDER = tuple(VARIANT_SPECS.keys())


def _utc_now() -> str:
    return core._utc_now()


def _write_json(path: Path, payload: dict[str, Any]) -> Path:
    return core._write_json(path, payload)


def _json_ready(value: Any) -> Any:
    return core._json_ready(value)


def _json_text(payload: Any) -> str:
    return core._json_text(payload)


def _load_json(path: Path) -> dict[str, Any]:
    return core._load_json(path)


def _make_variant_penalty(spec: dict[str, Any]) -> Callable[[pd.Series], tuple[bool, str, float]]:
    top6_threshold = spec.get("top6_10_threshold")
    top11_threshold = spec.get("top11_20_threshold")
    weak_mode = str(spec.get("weak_mode") or "base")
    monthly_margin = float(spec.get("monthly_margin") or 0.0)

    def _penalty(row: pd.Series) -> tuple[bool, str, float]:
        side = str(row.get("side") or "")
        rank = core._safe_int(row.get("challenger_rank") or row.get("champion_rank") or row.get("rank") or 0)
        if side != "long" or rank <= 5:
            return False, "top5_preserved", 0.0

        cnt60_up = core._safe_float(row.get("cnt60Up"), 999.0)
        monthly_up = core._safe_float(row.get("monthlyBreakoutUpProb"), 0.0)
        monthly_down = core._safe_float(row.get("monthlyBreakoutDownProb"), 0.0)
        market_regime = str(row.get("marketRegime") or "").lower()
        reclaim60 = core._safe_float(row.get("reclaim60"), 0.0) >= 0.5
        v60_strong = core._safe_float(row.get("v60Strong"), 0.0) >= 0.5
        if weak_mode == "strict":
            weak_indicator = ((not reclaim60) and (not v60_strong) and ("risk_off" in market_regime or monthly_down >= monthly_up - monthly_margin))
        else:
            weak_indicator = (not reclaim60) or (not v60_strong) or ("risk_off" in market_regime) or (monthly_down >= monthly_up - monthly_margin)

        if 6 <= rank <= 10:
            if top6_threshold is not None and cnt60_up < float(top6_threshold) and weak_indicator:
                return True, f"{spec['label']}_top6_10", 1.0
            return False, "accepted_top6_10", 0.0
        if top11_threshold is not None and cnt60_up < float(top11_threshold) and weak_indicator:
            return True, f"{spec['label']}_top11_20", 1.0
        return False, "accepted_top11_20", 0.0

    return _penalty


@contextmanager
def _patched_core_variant(spec: dict[str, Any]):
    old_penalty = core._weak_regime_penalty
    old_label = core.CHALLENGER_SELECTION_VARIANT
    core._weak_regime_penalty = _make_variant_penalty(spec)
    core.CHALLENGER_SELECTION_VARIANT = spec["label"]
    try:
        yield
    finally:
        core._weak_regime_penalty = old_penalty
        core.CHALLENGER_SELECTION_VARIANT = old_label


def _variant_score(summary: dict[str, Any]) -> tuple[float, float, float, float, float]:
    compare = summary.get("compare_topk") or {}
    top10 = compare.get("10", {}).get("delta", {}) or {}
    top20 = compare.get("20", {}).get("delta", {}) or {}
    top5 = compare.get("5", {}).get("delta", {}) or {}
    selection_edge_bonus = 1.0 if summary.get("selection_only_edge_preserved") else 0.0
    policy_edge_bonus = 1.0 if not summary.get("policy_layer_destroyed_edge") else 0.0
    starvation_penalty = -1.0 if summary.get("candidate_starvation_flag") else 0.0
    return (
        selection_edge_bonus,
        policy_edge_bonus,
        float(top10.get("policy_net_realized_pnl") or 0.0),
        float(top20.get("policy_net_realized_pnl") or 0.0),
        float(top5.get("policy_net_realized_pnl") or 0.0),
        starvation_penalty,
    )


def _aggregate_variant_rows(variant_results: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for variant, payload in variant_results.items():
        summary = payload["summary"]
        compare = payload["compare"]
        rows.append(
            {
                "variant": variant,
                "variant_label": summary.get("challenger_selection_variant"),
                "decision": summary.get("authoritative_rollup_decision"),
                "branching_metrics": summary.get("branching_metrics"),
                "candidate_starvation_flag": summary.get("candidate_starvation_flag"),
                "lower_bucket_long_drag_improved": summary.get("lower_bucket_long_drag_improved"),
                "selection_only_edge_preserved": summary.get("selection_only_edge_preserved"),
                "policy_layer_destroyed_edge": summary.get("policy_layer_destroyed_edge"),
                "baseline_bottom15_contamination_rate": summary.get("baseline_bottom15_contamination_rate"),
                "challenger_bottom15_contamination_rate": summary.get("challenger_bottom15_contamination_rate"),
                "weak_regime_only_performance": summary.get("weak_regime_only_performance"),
                "compare_topk": compare.get("compare_topk"),
                "exposure_normalization": summary.get("exposure_normalization"),
                "full_universe_gate_coverage": payload.get("full_universe_gate_coverage"),
                "paths": payload.get("paths"),
            }
        )
    return rows


def _variant_output_paths(output_dir: Path, variant: str) -> dict[str, Path]:
    variant_output_dir = output_dir / variant
    return {
        "summary": variant_output_dir / "selection_layer_weak_regime_bad_pick_removal_summary.json",
        "compare": variant_output_dir / "selection_layer_weak_regime_bad_pick_removal_compare.json",
        "decision": variant_output_dir / "selection_layer_weak_regime_bad_pick_removal_decision.json",
        "by_variant": variant_output_dir / "selection_layer_weak_regime_bad_pick_removal_by_variant.json",
        "by_rank_bucket": variant_output_dir / "selection_layer_weak_regime_bad_pick_removal_by_rank_bucket.json",
        "by_side": variant_output_dir / "selection_layer_weak_regime_bad_pick_removal_by_side.json",
        "by_action": variant_output_dir / "selection_layer_weak_regime_bad_pick_removal_by_action.json",
        "full_universe_gate_coverage": variant_output_dir / "selection_layer_weak_regime_bad_pick_removal_full_universe_gate_coverage.json",
        "candidate_snapshots": variant_output_dir / "selection_layer_weak_regime_bad_pick_removal_candidate_snapshots.json",
        "selection_only_ledger": variant_output_dir / "selection_layer_weak_regime_bad_pick_removal_selection_only_ledger.json",
        "policy_trade_ledger": variant_output_dir / "selection_layer_weak_regime_bad_pick_removal_policy_trade_ledger.json",
    }


def run_selection_layer_weak_regime_bad_pick_removal_v2_calibration(
    *,
    source_db_path: Path = DEFAULT_SOURCE_DB_PATH,
    input_dir: Path = DEFAULT_INPUT_DIR,
    output_dir: Path = DEFAULT_OUTPUT_DIR,
    anchor_limit: int | None = None,
) -> dict[str, Any]:
    output_dir.mkdir(parents=True, exist_ok=True)
    variant_results: dict[str, dict[str, Any]] = {}

    for variant in VARIANT_ORDER:
        spec = VARIANT_SPECS[variant]
        variant_output_dir = output_dir / variant
        variant_output_dir.mkdir(parents=True, exist_ok=True)
        variant_paths = _variant_output_paths(output_dir, variant)
        if variant_paths["summary"].exists() and variant_paths["compare"].exists():
            result = {
                "summary": _load_json(variant_paths["summary"]),
                "compare": _load_json(variant_paths["compare"]),
                "full_universe_gate_coverage": _load_json(variant_paths["full_universe_gate_coverage"])
                if variant_paths["full_universe_gate_coverage"].exists()
                else None,
                "paths": {key: str(path) for key, path in variant_paths.items()},
            }
        else:
            with _patched_core_variant(spec):
                result = core.run_selection_layer_weak_regime_bad_pick_removal(
                    source_db_path=source_db_path,
                    input_dir=input_dir,
                    output_dir=variant_output_dir,
                    anchor_limit=anchor_limit,
                )
            result["full_universe_gate_coverage"] = (
                _load_json(variant_paths["full_universe_gate_coverage"])
                if variant_paths["full_universe_gate_coverage"].exists()
                else None
            )
        variant_results[variant] = {
            "variant": variant,
            "variant_label": spec["label"],
            "note": spec["note"],
            "summary": result["summary"],
            "compare": result["compare"],
            "full_universe_gate_coverage": result.get("full_universe_gate_coverage"),
            "paths": result["paths"],
        }

    best_variant = max(VARIANT_ORDER, key=lambda label: _variant_score(variant_results[label]["summary"]))
    best_summary = copy.deepcopy(variant_results[best_variant]["summary"])
    best_compare = copy.deepcopy(variant_results[best_variant]["compare"])

    by_variant_rows = _aggregate_variant_rows(variant_results)
    by_rank_bucket = {
        variant: variant_results[variant]["summary"].get("by_rank_bucket", {})
        for variant in VARIANT_ORDER
    }
    by_side = {
        variant: variant_results[variant]["summary"].get("by_side", {})
        for variant in VARIANT_ORDER
    }
    by_action = {
        variant: variant_results[variant]["summary"].get("by_action", {})
        for variant in VARIANT_ORDER
    }

    baseline_variant = {
        "variant": "baseline",
        "variant_label": core.BASELINE_SELECTION_VARIANT,
        "note": "Specialized 3-way gate baseline from integrated guarded v1.",
        "decision": "baseline",
    }

    comparison = {
        "schema_version": "tradex_selection_layer_weak_regime_bad_pick_removal_v2_calibration_compare_v1",
        "generated_at": _utc_now(),
        "anchor_count": int(best_summary.get("anchor_count") or 0),
        "baseline": baseline_variant,
        "variants": {
            variant: {
                "variant": variant,
                "variant_label": payload["variant_label"],
                "note": payload["note"],
                "decision": payload["summary"].get("authoritative_rollup_decision"),
                "branching_metrics": payload["summary"].get("branching_metrics"),
                "selection_only_edge_preserved": payload["summary"].get("selection_only_edge_preserved"),
                "policy_layer_destroyed_edge": payload["summary"].get("policy_layer_destroyed_edge"),
                "candidate_starvation_flag": payload["summary"].get("candidate_starvation_flag"),
                "lower_bucket_long_drag_improved": payload["summary"].get("lower_bucket_long_drag_improved"),
                "baseline_bottom15_contamination_rate": payload["summary"].get("baseline_bottom15_contamination_rate"),
                "challenger_bottom15_contamination_rate": payload["summary"].get("challenger_bottom15_contamination_rate"),
                "compare_topk": payload["compare"].get("compare_topk"),
                "summary_path": payload["paths"]["summary"],
                "compare_path": payload["paths"]["compare"],
                "full_universe_gate_coverage": payload.get("full_universe_gate_coverage"),
            }
            for variant, payload in variant_results.items()
        },
        "best_variant": best_variant,
        "best_variant_label": variant_results[best_variant]["variant_label"],
        "best_variant_note": variant_results[best_variant]["note"],
        "best_variant_reference": {
            "summary": variant_results[best_variant]["summary"].get("compare_topk", {}),
            "branching_metrics": variant_results[best_variant]["summary"].get("branching_metrics"),
            "selection_only_edge_preserved": variant_results[best_variant]["summary"].get("selection_only_edge_preserved"),
            "policy_layer_destroyed_edge": variant_results[best_variant]["summary"].get("policy_layer_destroyed_edge"),
        },
        "variant_rows": by_variant_rows,
        "by_rank_bucket": by_rank_bucket,
        "by_side": by_side,
        "by_action": by_action,
    }

    summary = {
        "schema_version": "tradex_selection_layer_weak_regime_bad_pick_removal_v2_calibration_summary_v1",
        "generated_at": _utc_now(),
        "selection_layer": "weak_regime_bad_pick_removal_v2_calibration",
        "baseline_selection_variant": core.BASELINE_SELECTION_VARIANT,
        "policy_variant": core.POLICY_VARIANT,
        "anchor_count": int(best_summary.get("anchor_count") or 0),
        "candidate_snapshot_rows_count": int(best_summary.get("candidate_snapshot_rows_count") or 0),
        "selection_rows_count": int(best_summary.get("selection_rows_count") or 0),
        "policy_ledger_rows_count": int(best_summary.get("policy_ledger_rows_count") or 0),
        "selection_only_edge_preserved": bool(best_summary.get("selection_only_edge_preserved")),
        "policy_layer_destroyed_edge": bool(best_summary.get("policy_layer_destroyed_edge")),
        "candidate_starvation_flag": bool(best_summary.get("candidate_starvation_flag")),
        "lower_bucket_long_drag_improved": bool(best_summary.get("lower_bucket_long_drag_improved")),
        "baseline_bottom15_contamination_rate": best_summary.get("baseline_bottom15_contamination_rate"),
        "challenger_bottom15_contamination_rate": best_summary.get("challenger_bottom15_contamination_rate"),
        "top5_boundary_score_gap": best_summary.get("top5_boundary_score_gap"),
        "top10_boundary_score_gap": best_summary.get("top10_boundary_score_gap"),
        "full_universe_gate_coverage": variant_results[best_variant].get("full_universe_gate_coverage"),
        "best_variant": best_variant,
        "best_variant_label": variant_results[best_variant]["variant_label"],
        "best_variant_note": variant_results[best_variant]["note"],
        "variants": {
            variant: {
                "variant": variant,
                "variant_label": payload["variant_label"],
                "note": payload["note"],
                "decision": payload["summary"].get("authoritative_rollup_decision"),
                "branching_metrics": payload["summary"].get("branching_metrics"),
                "weak_regime_only_performance": payload["summary"].get("weak_regime_only_performance"),
                "candidate_starvation_flag": payload["summary"].get("candidate_starvation_flag"),
                "selection_only_edge_preserved": payload["summary"].get("selection_only_edge_preserved"),
                "policy_layer_destroyed_edge": payload["summary"].get("policy_layer_destroyed_edge"),
                "lower_bucket_long_drag_improved": payload["summary"].get("lower_bucket_long_drag_improved"),
                "baseline_bottom15_contamination_rate": payload["summary"].get("baseline_bottom15_contamination_rate"),
                "challenger_bottom15_contamination_rate": payload["summary"].get("challenger_bottom15_contamination_rate"),
                "compare_topk": payload["compare"].get("compare_topk"),
                "exposure_normalization": payload["summary"].get("exposure_normalization"),
            }
            for variant, payload in variant_results.items()
        },
        "same_condition_contract": best_summary.get("same_condition_contract"),
    }

    best_compare_topk = best_compare.get("champion_vs_challenger", {}) if isinstance(best_compare, dict) else {}
    top10_delta = (
        best_compare_topk.get("policy_trade", {})
        .get("10", {})
        .get("delta", {})
        .get("policy_net_realized_pnl")
    )
    top20_delta = (
        best_compare_topk.get("policy_trade", {})
        .get("20", {})
        .get("delta", {})
        .get("policy_net_realized_pnl")
    )
    if bool(best_summary.get("candidate_starvation_flag")) and (top10_delta is None or top20_delta is None or float(top20_delta) <= 0.0):
        primary_failure_reason = "candidate_starvation_and_policy_regression"
        secondary_failure_reasons = [
            "top10_policy_regression",
            "top20_policy_regression",
            "full_universe_no_trade_rate_high",
        ]
    else:
        primary_failure_reason = "candidate_starvation_persists"
        secondary_failure_reasons = [
            "top20_gain_is_marginal",
            "full_universe_no_trade_rate_high",
        ]

    decision = {
        "schema_version": "tradex_selection_layer_weak_regime_bad_pick_removal_v2_calibration_decision_v1",
        "generated_at": _utc_now(),
        "diagnosis_decision": "hold",
        "primary_failure_reason": primary_failure_reason,
        "secondary_failure_reasons": secondary_failure_reasons,
        "best_variant": best_variant,
        "best_variant_label": variant_results[best_variant]["variant_label"],
        "best_variant_note": variant_results[best_variant]["note"],
        "keep_candidate": False,
        "hold_candidate": True,
        "drop_candidate": False,
        "recommended_next_axis": "tune_weak_regime_veto_thresholds_more_conservatively",
    }

    out_paths = {
        "summary": _write_json(output_dir / "selection_layer_weak_regime_bad_pick_removal_v2_calibration_summary.json", summary),
        "compare": _write_json(output_dir / "selection_layer_weak_regime_bad_pick_removal_v2_calibration_compare.json", comparison),
        "decision": _write_json(output_dir / "selection_layer_weak_regime_bad_pick_removal_v2_calibration_decision.json", decision),
        "by_variant": _write_json(
            output_dir / "selection_layer_weak_regime_bad_pick_removal_v2_calibration_by_variant.json",
            {
                "schema_version": "tradex_selection_layer_weak_regime_bad_pick_removal_v2_calibration_by_variant_v1",
                "generated_at": _utc_now(),
                "rows": by_variant_rows,
            },
        ),
        "by_rank_bucket": _write_json(
            output_dir / "selection_layer_weak_regime_bad_pick_removal_v2_calibration_by_rank_bucket.json",
            {
                "schema_version": "tradex_selection_layer_weak_regime_bad_pick_removal_v2_calibration_by_rank_bucket_v1",
                "generated_at": _utc_now(),
                "rows": [
                    {"variant": variant, "rows": payload}
                    for variant, payload in by_rank_bucket.items()
                ],
            },
        ),
        "by_side": _write_json(
            output_dir / "selection_layer_weak_regime_bad_pick_removal_v2_calibration_by_side.json",
            {
                "schema_version": "tradex_selection_layer_weak_regime_bad_pick_removal_v2_calibration_by_side_v1",
                "generated_at": _utc_now(),
                "rows": [
                    {"variant": variant, "rows": payload}
                    for variant, payload in by_side.items()
                ],
            },
        ),
        "by_action": _write_json(
            output_dir / "selection_layer_weak_regime_bad_pick_removal_v2_calibration_by_action.json",
            {
                "schema_version": "tradex_selection_layer_weak_regime_bad_pick_removal_v2_calibration_by_action_v1",
                "generated_at": _utc_now(),
                "rows": [
                    {"variant": variant, "rows": payload}
                    for variant, payload in by_action.items()
                ],
            },
        ),
    }

    return {
        "summary": summary,
        "compare": comparison,
        "decision": decision,
        "paths": out_paths,
        "variant_results": variant_results,
        "best_variant": best_variant,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run weak-regime bad-pick removal calibration variants.")
    parser.add_argument("--source-db-path", default=str(DEFAULT_SOURCE_DB_PATH))
    parser.add_argument("--input-dir", default=str(DEFAULT_INPUT_DIR))
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR))
    parser.add_argument("--anchor-limit", type=int, default=None)
    args = parser.parse_args(argv)
    payload = run_selection_layer_weak_regime_bad_pick_removal_v2_calibration(
        source_db_path=Path(args.source_db_path),
        input_dir=Path(args.input_dir),
        output_dir=Path(args.output_dir),
        anchor_limit=args.anchor_limit,
    )
    print(_json_text(payload))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
