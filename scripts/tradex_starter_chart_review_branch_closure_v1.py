from __future__ import annotations

import argparse
import json
import math
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


AXIS_ID = "starter_chart_review_branch_closure_v1"
DEFAULT_ENRICHED_ROOT = Path(r"G:\Tradex\starter_chart_context_enriched_replay_v1\20260525T071724Z-starter-chart-context-enriched-replay-v1")
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\starter_chart_review_branch_closure_v1")

REQUIRED_ARTIFACTS = (
    "branch_closure_summary.json",
    "signature_closure_metrics.json",
    "research_decision.json",
    "lineage.json",
    "_ARTIFACT_COMPLETE.json",
)


def _now_tag() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _json_ready(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(k): _json_ready(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_json_ready(v) for v in value]
    if isinstance(value, Path):
        return str(value)
    if hasattr(value, "item"):
        return _json_ready(value.item())
    if isinstance(value, float) and not math.isfinite(value):
        return None
    return value


def _write_json(path: Path, payload: dict[str, Any] | list[dict[str, Any]]) -> None:
    path.write_text(json.dumps(_json_ready(payload), ensure_ascii=False, sort_keys=True, indent=2) + "\n", encoding="utf-8")


def supported_signature_rows(metrics: dict[str, Any], min_sample: int = 10) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for sig, payload in metrics.items():
        comp = payload.get("comparison_vs_untagged_rows", {})
        delta = comp.get("mean_ret20_delta_tagged_minus_untagged")
        rows.append(
            {
                "signature": sig,
                "sample_count": int(payload.get("sample_count") or 0),
                "mean_ret20": payload.get("mean_ret20"),
                "mean_ret20_delta_vs_untagged": delta,
                "sample_allows_comparison": bool(comp.get("sample_allows_comparison")),
                "supported": int(payload.get("sample_count") or 0) >= min_sample,
                "worse_than_untagged": bool(delta is not None and delta < 0),
            }
        )
    return sorted(rows, key=lambda r: (r["supported"], r["sample_count"]), reverse=True)


def decide_closure(signature_rows: list[dict[str, Any]], enriched_decision: dict[str, Any]) -> str:
    supported = [r for r in signature_rows if r["supported"]]
    negative = [r for r in signature_rows if r["worse_than_untagged"]]
    supported_negative = [r for r in supported if r["worse_than_untagged"]]
    if not signature_rows:
        return "close_branch_no_reusable_signal"
    if supported_negative:
        return "needs_formal_signature_pretest_before_close"
    if negative and all(r["sample_count"] < 10 for r in negative):
        return "close_branch_no_reusable_signal"
    if supported and not supported_negative:
        return "close_branch_no_reusable_signal"
    if enriched_decision.get("decision") == "feature_context_created_but_underpowered":
        return "manual_card_only_keep"
    return "needs_formal_signature_pretest_before_close"


def run(enriched_root: Path, output_root: Path) -> Path:
    out = output_root / f"{_now_tag()}-starter-chart-review-branch-closure-v1"
    out.mkdir(parents=True, exist_ok=True)
    summary = json.loads((enriched_root / "enriched_replay_summary.json").read_text(encoding="utf-8"))
    candidates = json.loads((enriched_root / "feature_only_signature_candidates.json").read_text(encoding="utf-8"))
    metrics = json.loads((enriched_root / "feature_only_signature_metrics.json").read_text(encoding="utf-8"))
    enriched_decision = json.loads((enriched_root / "research_decision.json").read_text(encoding="utf-8"))

    signature_rows = supported_signature_rows(metrics)
    decision = decide_closure(signature_rows, enriched_decision)
    supported = [r for r in signature_rows if r["supported"]]
    negative_thin = [r for r in signature_rows if r["worse_than_untagged"] and r["sample_count"] < 10]

    _write_json(
        out / "signature_closure_metrics.json",
        {
            "signature_rows": signature_rows,
            "supported_signature_count": len(supported),
            "supported_negative_signature_count": len([r for r in supported if r["worse_than_untagged"]]),
            "thin_negative_signature_count": len(negative_thin),
            "feature_only_signature_candidates": candidates,
        },
    )
    _write_json(
        out / "research_decision.json",
        {
            "axis_id": AXIS_ID,
            "decision": decision,
            "starter_ready_promotable": False,
            "negative_tags_active_gate": False,
            "validated_buy_count": 0,
            "meemee_reflectable_candidate": False,
            "chart_review_pack_role": "manual_card_only",
            "branch_status": "closed_or_closure_pending",
            "no_active_gate_created": True,
            "runtime_db_write": False,
            "production_ranking_changed": False,
            "threshold_retune_attempted": False,
            "ret20_derived_signature_terms_used": False,
        },
    )
    _write_json(
        out / "lineage.json",
        {
            "enriched_replay_root": enriched_root,
            "enriched_replay_decision": summary.get("decision"),
            "input_replay_root": summary.get("input_replay_root"),
            "source_artifacts": [
                str(enriched_root / "enriched_replay_summary.json"),
                str(enriched_root / "feature_only_signature_candidates.json"),
                str(enriched_root / "feature_only_signature_metrics.json"),
                str(enriched_root / "research_decision.json"),
            ],
        },
    )
    _write_json(
        out / "branch_closure_summary.json",
        {
            "axis_id": AXIS_ID,
            "decision": decision,
            "input_enriched_decision": summary.get("decision"),
            "sample_count": summary.get("sample_count"),
            "date_count": summary.get("date_count"),
            "signature_count": summary.get("signature_count"),
            "supported_signature_count": len(supported),
            "supported_signatures_are_negative": bool([r for r in supported if r["worse_than_untagged"]]),
            "thin_negative_signatures": negative_thin,
            "starter_ready_promotable": False,
            "negative_tags_active_gate": False,
            "validated_buy_count": 0,
            "meemee_reflectable_candidate": False,
            "chart_review_pack_role": "manual_card_only",
            "branch_status": "closed_or_closure_pending",
            "no_active_gate_created": True,
        },
    )
    existing = [name for name in REQUIRED_ARTIFACTS if name != "_ARTIFACT_COMPLETE.json" and (out / name).exists()]
    _write_json(out / "_ARTIFACT_COMPLETE.json", {"axis_id": AXIS_ID, "required_artifacts": list(REQUIRED_ARTIFACTS), "complete": len(existing) == len(REQUIRED_ARTIFACTS) - 1})
    return out


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--enriched-root", type=Path, default=DEFAULT_ENRICHED_ROOT)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    args = parser.parse_args()
    print(run(args.enriched_root, args.output_root))


if __name__ == "__main__":
    main()
