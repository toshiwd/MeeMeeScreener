from __future__ import annotations

import argparse
import json
import math
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

if __package__ in {None, ""}:
    sys.path.append(str(Path(__file__).resolve().parents[1]))

from scripts import tradex_starter_ready_failure_decomposition_v1 as decomp


AXIS_ID = "starter_ready_negative_tag_pretest_v1"
DEFAULT_DECOMP_ROOT = Path(r"G:\Tradex\starter_ready_failure_decomposition_v1\20260525T065724Z-starter-ready-failure-decomposition-v1")
DEFAULT_REPLAY_ROOT = Path(
    r"G:\Tradex\starter_candidate_chart_review_historical_replay_v1\20260525T065259Z-starter-candidate-chart-review-historical-replay-v1"
)
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\starter_ready_negative_tag_pretest_v1")

REQUIRED_ARTIFACTS = (
    "negative_tag_pretest_summary.json",
    "negative_tag_pretest_rows.csv",
    "tag_metrics.json",
    "tag_vs_untagged_comparison.json",
    "label_interaction_metrics.json",
    "missing_column_audit.json",
    "research_decision.json",
    "_ARTIFACT_COMPLETE.json",
)

LOOKAHEAD_SIGNATURE_PARTS = {"flat_or_negative", "bad", "severe", "good"}


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
    try:
        if pd.isna(value):
            return None
    except Exception:
        pass
    return value


def _write_json(path: Path, payload: dict[str, Any] | list[dict[str, Any]]) -> None:
    path.write_text(json.dumps(_json_ready(payload), ensure_ascii=False, sort_keys=True, indent=2) + "\n", encoding="utf-8")


def load_tag_signatures(decomp_root: Path) -> list[str]:
    payload = json.loads((decomp_root / "reusable_negative_tags.json").read_text(encoding="utf-8"))
    return [str(row["failure_signature"]) for row in payload.get("tags", []) if row.get("failure_signature")]


def has_lookahead_dependency(signature: str) -> bool:
    return bool(set(signature.split("|")).intersection(LOOKAHEAD_SIGNATURE_PARTS))


def prepare_rows(rows: pd.DataFrame, signatures: list[str]) -> pd.DataFrame:
    out = rows.copy()
    out["pattern_type"] = out["research_candidate_source_family"].map(decomp.pattern_type)
    out["ret20_bucket"] = out["ret20"].map(decomp.ret20_bucket)
    out["failure_signature"] = out.apply(decomp.failure_signature, axis=1)
    out["negative_tag_hit"] = out["failure_signature"].isin(signatures)
    out["negative_tag_names"] = out["failure_signature"].where(out["negative_tag_hit"], "")
    return out


def metric_block(frame: pd.DataFrame) -> dict[str, Any]:
    ret20 = pd.to_numeric(frame.get("ret20"), errors="coerce").dropna()
    return {
        "sample_count": int(len(frame)),
        "date_count": int(frame["decision_date"].nunique()) if "decision_date" in frame else 0,
        "code_count": int(frame["code"].astype(str).nunique()) if "code" in frame else 0,
        "mean_ret5": _mean(frame, "ret5"),
        "mean_ret10": _mean(frame, "ret10"),
        "mean_ret20": _mean(frame, "ret20"),
        "median_ret20": float(ret20.median()) if not ret20.empty else None,
        "hit_rate_ret20_gt_0": float((ret20 > 0).mean()) if not ret20.empty else None,
        "bad_rate_ret20_lt_minus_5pct": float((ret20 < -0.05).mean()) if not ret20.empty else None,
        "severe_rate_ret20_lt_minus_10pct": float((ret20 < -0.10).mean()) if not ret20.empty else None,
    }


def _mean(frame: pd.DataFrame, col: str) -> float | None:
    if col not in frame:
        return None
    values = pd.to_numeric(frame.get(col), errors="coerce").dropna()
    return float(values.mean()) if not values.empty else None


def compare(left: pd.DataFrame, right: pd.DataFrame) -> dict[str, Any]:
    lm = metric_block(left)
    rm = metric_block(right)
    return {
        "tagged": lm,
        "untagged": rm,
        "mean_ret20_delta_tagged_minus_untagged": None if lm["mean_ret20"] is None or rm["mean_ret20"] is None else lm["mean_ret20"] - rm["mean_ret20"],
        "bad_rate_delta_tagged_minus_untagged": None
        if lm["bad_rate_ret20_lt_minus_5pct"] is None or rm["bad_rate_ret20_lt_minus_5pct"] is None
        else lm["bad_rate_ret20_lt_minus_5pct"] - rm["bad_rate_ret20_lt_minus_5pct"],
        "severe_rate_delta_tagged_minus_untagged": None
        if lm["severe_rate_ret20_lt_minus_10pct"] is None or rm["severe_rate_ret20_lt_minus_10pct"] is None
        else lm["severe_rate_ret20_lt_minus_10pct"] - rm["severe_rate_ret20_lt_minus_10pct"],
        "sample_allows_comparison": len(left) >= 10 and len(right) >= 10,
    }


def build_tag_metrics(rows: pd.DataFrame, signatures: list[str]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for sig in signatures:
        tagged = rows[rows["failure_signature"].eq(sig)]
        untagged = rows[~rows["failure_signature"].eq(sig)]
        same_label = {}
        same_pattern = {}
        if not tagged.empty:
            labels = sorted(tagged["manual_judgment"].dropna().astype(str).unique().tolist())
            patterns = sorted(tagged["pattern_type"].dropna().astype(str).unique().tolist())
            same_label_untagged = rows[(~rows["failure_signature"].eq(sig)) & (rows["manual_judgment"].astype(str).isin(labels))]
            same_pattern_untagged = rows[(~rows["failure_signature"].eq(sig)) & (rows["pattern_type"].astype(str).isin(patterns))]
            same_label = compare(tagged, same_label_untagged)
            same_pattern = compare(tagged, same_pattern_untagged)
        out[sig] = {
            "tag_sample_count": int(len(tagged)),
            "tagged_date_count": int(tagged["decision_date"].nunique()) if not tagged.empty else 0,
            "tagged_code_count": int(tagged["code"].astype(str).nunique()) if not tagged.empty else 0,
            "metrics": metric_block(tagged),
            "comparison_vs_untagged_rows": compare(tagged, untagged),
            "comparison_vs_same_label_untagged_rows": same_label,
            "comparison_vs_same_pattern_type_untagged_rows": same_pattern,
            "sample_under_10": int(len(tagged)) < 10,
            "lookahead_dependent_signature": has_lookahead_dependency(sig),
        }
    return out


def label_interactions(rows: pd.DataFrame) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for keys, group in rows.groupby(["manual_judgment", "negative_tag_hit"], dropna=False):
        label, hit = keys
        out.append({"manual_judgment": label, "negative_tag_hit": bool(hit), **metric_block(group)})
    return sorted(out, key=lambda r: (str(r["manual_judgment"]), not r["negative_tag_hit"]))


def decide(rows: pd.DataFrame, signatures: list[str], missing_audit: dict[str, Any], overall: dict[str, Any], tag_metrics: dict[str, Any]) -> str:
    if missing_audit["blocked_missing_columns"]:
        return "blocked_missing_columns"
    tagged_n = int(rows["negative_tag_hit"].sum()) if "negative_tag_hit" in rows else 0
    if tagged_n == 0:
        return "sample_too_thin"
    delta = overall.get("mean_ret20_delta_tagged_minus_untagged")
    bad_delta = overall.get("bad_rate_delta_tagged_minus_untagged")
    severe_delta = overall.get("severe_rate_delta_tagged_minus_untagged")
    worse = delta is not None and delta < 0 and ((bad_delta is not None and bad_delta >= 0) or (severe_delta is not None and severe_delta >= 0))
    if tagged_n < 20:
        return "negative_tag_worse_but_underpowered" if worse else "sample_too_thin"
    if not worse:
        return "negative_tag_no_generalization"
    if any(v.get("sample_under_10") for v in tag_metrics.values()):
        return "negative_tag_worse_but_underpowered"
    return "negative_tag_candidate_keep_for_enriched_replay"


def run(decomp_root: Path, replay_root: Path, output_root: Path) -> Path:
    out = output_root / f"{_now_tag()}-starter-ready-negative-tag-pretest-v1"
    out.mkdir(parents=True, exist_ok=True)
    signatures = load_tag_signatures(decomp_root)
    rows = pd.read_csv(replay_root / "historical_replay_rows.csv", low_memory=False)
    replay_summary = json.loads((replay_root / "historical_replay_summary.json").read_text(encoding="utf-8"))
    prepared = prepare_rows(rows, signatures)
    prepared.to_csv(out / "negative_tag_pretest_rows.csv", index=False)

    missing_columns = decomp.missing_column_report(prepared)
    lookahead_dependent = [sig for sig in signatures if has_lookahead_dependency(sig)]
    missing_audit = {
        "missing_columns": missing_columns,
        "lookahead_dependent_signatures": lookahead_dependent,
        "blocked_missing_columns": bool(lookahead_dependent),
        "reason": "diagnostic tag definitions include ret20 outcome bucket; not faithful point-in-time tag reconstruction"
        if lookahead_dependent
        else None,
    }
    tag_metrics = build_tag_metrics(prepared, signatures)
    tagged = prepared[prepared["negative_tag_hit"]]
    untagged = prepared[~prepared["negative_tag_hit"]]
    overall = compare(tagged, untagged)
    decision = decide(prepared, signatures, missing_audit, overall, tag_metrics)

    _write_json(out / "tag_metrics.json", tag_metrics)
    _write_json(out / "tag_vs_untagged_comparison.json", {"overall": overall})
    _write_json(out / "label_interaction_metrics.json", {"by_label_and_tag_hit": label_interactions(prepared)})
    _write_json(out / "missing_column_audit.json", missing_audit)
    _write_json(
        out / "research_decision.json",
        {
            "axis_id": AXIS_ID,
            "decision": decision,
            "starter_ready_promotable": False,
            "negative_tags_active_gate": False,
            "validated_buy_count": 0,
            "meemee_reflectable_candidate": False,
            "runtime_db_write": False,
            "production_ranking_changed": False,
            "threshold_retune_attempted": False,
            "ranking_usage": False,
        },
    )
    _write_json(
        out / "negative_tag_pretest_summary.json",
        {
            "axis_id": AXIS_ID,
            "input_decomposition_root": decomp_root,
            "input_replay_root": replay_root,
            "input_replay_decision": replay_summary.get("decision"),
            "tag_count": len(signatures),
            "sample_count": int(len(prepared)),
            "tagged_sample_count": int(prepared["negative_tag_hit"].sum()),
            "untagged_sample_count": int((~prepared["negative_tag_hit"]).sum()),
            "decision": decision,
            "starter_ready_promotable": False,
            "negative_tags_active_gate": False,
            "validated_buy_count": 0,
            "meemee_reflectable_candidate": False,
            "confirmed_source_only": bool(replay_summary.get("confirmed_source_only")),
            "blocked_missing_columns": missing_audit["blocked_missing_columns"],
        },
    )
    existing = [name for name in REQUIRED_ARTIFACTS if name != "_ARTIFACT_COMPLETE.json" and (out / name).exists()]
    _write_json(out / "_ARTIFACT_COMPLETE.json", {"axis_id": AXIS_ID, "required_artifacts": list(REQUIRED_ARTIFACTS), "complete": len(existing) == len(REQUIRED_ARTIFACTS) - 1})
    return out


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--decomp-root", type=Path, default=DEFAULT_DECOMP_ROOT)
    parser.add_argument("--replay-root", type=Path, default=DEFAULT_REPLAY_ROOT)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    args = parser.parse_args()
    print(run(args.decomp_root, args.replay_root, args.output_root))


if __name__ == "__main__":
    main()
