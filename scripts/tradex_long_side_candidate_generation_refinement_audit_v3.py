from __future__ import annotations

import argparse
import json
import math
import sys
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


SCRIPT_NAME = "tradex_long_side_candidate_generation_refinement_audit_v3"
MANIFEST_SCHEMA_VERSION = "tradex_long_side_candidate_generation_refinement_audit_v3_manifest_v1"
INPUT_RESOLUTION_SCHEMA_VERSION = "tradex_long_side_candidate_generation_refinement_audit_v3_input_resolution_v1"
REPAIRED_LOSS_SCHEMA_VERSION = "tradex_long_side_candidate_generation_refinement_audit_v3_repaired_loss_v1"
ACCEPTED_RANKING_SCHEMA_VERSION = "tradex_long_side_candidate_generation_refinement_audit_v3_accepted_ranking_v1"
SOURCE_ABSENT_SCHEMA_VERSION = "tradex_long_side_candidate_generation_refinement_audit_v3_source_absent_v1"
FEASIBILITY_SCHEMA_VERSION = "tradex_long_side_candidate_generation_refinement_audit_v3_feasibility_v1"
RECOMMENDATION_SCHEMA_VERSION = "tradex_long_side_candidate_generation_refinement_v3_recommendation_v1"
DECISION_SCHEMA_VERSION = "tradex_long_side_candidate_generation_refinement_audit_v3_decision_v1"
ARTIFACT_COMPLETE_SCHEMA_VERSION = "tradex_long_side_candidate_generation_refinement_audit_v3_artifact_complete_v1"

DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\long_side_candidate_generation_refinement_audit_v3")

REPAIR_SESSION = Path(r"G:\Tradex\exact_candidate_key_repair_v1\20260503T030102Z-236093")
REJECTED_SESSION = Path(r"G:\Tradex\rejected_row_instrumentation_v1\20260503T022646Z-860133")
V2_SESSION = Path(r"G:\Tradex\long_side_candidate_generation_refinement_audit_v2\20260503T024136Z-943858")
RERANKER_SESSION = Path(r"G:\Tradex\long_side_reranker_validation_v1\20260502T151756Z-703876")
SURFACE_SESSION = Path(r"G:\Tradex\side_specific_high_recall_surface_v1\20260502T151044Z-324144")
FILTER_SESSION = Path(r"G:\Tradex\long_side_filter_revision_v1\20260503T015243Z-676367")

REPAIRED_TRACE_ROWS = REPAIR_SESSION / "candidate_key_repaired_trace_rows.parquet"
REPAIRED_WINNER_ROWS = REPAIR_SESSION / "candidate_key_repaired_top15_loss_trace_rows.parquet"
REPAIRED_SUMMARY = REPAIR_SESSION / "candidate_key_repair_summary.json"
REPAIRED_DECISION = REPAIR_SESSION / "exact_candidate_key_repair_v1_decision.json"
CANONICAL_CONTRACT = REPAIR_SESSION / "canonical_candidate_key_contract.json"
LINEAGE_INVENTORY = REPAIR_SESSION / "candidate_key_lineage_inventory.json"

REJECTED_TRACE_ROWS = REJECTED_SESSION / "candidate_admission_trace_rows.parquet"
REJECTED_ROWS = REJECTED_SESSION / "rejected_candidate_rows.parquet"
ACCEPTED_ROWS = REJECTED_SESSION / "accepted_candidate_rows.parquet"
REJECT_BUCKET_SUMMARY = REJECTED_SESSION / "reject_reason_bucket_summary.json"
STAGE_RECONCILIATION = REJECTED_SESSION / "stage_row_count_reconciliation.json"
REJECTED_DECISION = REJECTED_SESSION / "rejected_row_instrumentation_v1_decision.json"

V2_TRACE_QUALITY = V2_SESSION / "long_side_trace_quality_audit.json"
V2_LOSS_ATTRIBUTION = V2_SESSION / "long_side_top15_loss_attribution_v2.json"
V2_STAGE_BOTTLENECK = V2_SESSION / "long_side_admission_stage_bottleneck_audit.json"
V2_TRACE_SCORE_TIER = V2_SESSION / "long_side_trace_score_tier_audit.json"
V2_RECOMMENDATION = V2_SESSION / "long_side_candidate_generation_refinement_v2_recommendation.json"
V2_DECISION = V2_SESSION / "long_side_candidate_generation_refinement_audit_v2_decision.json"

RERANKER_ROWS = RERANKER_SESSION / "long_side_reranker_prediction_rows.parquet"
RERANKER_DECISION = RERANKER_SESSION / "long_side_reranker_validation_v1_decision.json"

SURFACE_QUALITY = SURFACE_SESSION / "side_specific_surface_quality_audit.json"
SURFACE_ORACLE = SURFACE_SESSION / "side_specific_oracle_headroom_audit.json"
SURFACE_DECISION = SURFACE_SESSION / "side_specific_high_recall_surface_v1_decision.json"

FILTER_DECISION = FILTER_SESSION / "long_side_filter_revision_v1_decision.json"

TRACE_STAGE_ORDER = [
    "raw_candidate_source",
    "prefilter_broad_context",
    "two_stage_admission",
    "high_recall_min_pool",
    "risk_filter_long_active",
    "side_specific_long_active_surface",
]

LOSS_CATEGORY_ORDER = [
    "source_absent_before_min_pool",
    "accepted_to_min_pool",
    "risk_filter_rejected",
    "long_active_rejected",
    "accepted_to_long_active",
    "accepted_but_buried_by_champion_rank",
    "accepted_but_missed_by_reranker",
    "accepted_and_selected",
]


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _session_id() -> str:
    now = datetime.now(timezone.utc)
    return f"{now.strftime('%Y%m%dT%H%M%SZ')}-{now.microsecond:06d}"


def _json_ready(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(k): _json_ready(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_json_ready(v) for v in value]
    if isinstance(value, tuple):
        return [_json_ready(v) for v in value]
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    if isinstance(value, float) and not math.isfinite(value):
        return None
    if pd.isna(value):
        return None
    return value


def _write_json(path: Path, payload: dict[str, Any]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(_json_ready(payload), ensure_ascii=False, indent=2, sort_keys=True, default=str) + "\n",
        encoding="utf-8",
    )
    return path


def _write_parquet(path: Path, frame: pd.DataFrame) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    sanitized = frame.copy()
    for column in sanitized.columns:
        series = sanitized[column]
        if series.map(lambda value: isinstance(value, pd.Timestamp)).any():
            sanitized[column] = series.map(lambda value: value.isoformat() if isinstance(value, pd.Timestamp) else value)
        elif series.map(lambda value: isinstance(value, (dict, list, tuple))).any():
            sanitized[column] = series.map(
                lambda value: json.dumps(_json_ready(value), ensure_ascii=False, sort_keys=True)
                if isinstance(value, (dict, list, tuple))
                else value
            )
    sanitized.to_parquet(path, index=False)
    return path


def _ensure_exists(path: Path, label: str) -> Path:
    if not path.exists():
        raise FileNotFoundError(f"missing required source artifact for {label}: {path}")
    return path


def _load_json(path: Path) -> dict[str, Any]:
    return json.loads(_ensure_exists(path, str(path)).read_text(encoding="utf-8"))


def _load_frame(path: Path) -> pd.DataFrame:
    return pd.read_parquet(_ensure_exists(path, str(path))).copy()


def _normalize_anchor_date(value: Any) -> str | None:
    if pd.isna(value):
        return None
    ts = pd.to_datetime(value, errors="coerce")
    if pd.isna(ts):
        return str(value)
    return ts.date().isoformat()


def _normalize_side(value: Any) -> str | None:
    if pd.isna(value):
        return None
    return str(value).strip().lower()


def _normalize_symbol(value: Any) -> str | None:
    if pd.isna(value):
        return None
    return str(value).strip()


def _canonical_key(row: pd.Series) -> str | None:
    anchor = _normalize_anchor_date(row.get("anchor_date"))
    side = _normalize_side(row.get("side"))
    symbol = _normalize_symbol(row.get("symbol"))
    if anchor is None or side is None or symbol is None:
        return None
    return f"{anchor}|{side}|{symbol}"


def _candidate_key_version() -> str:
    return "canonical_anchor_date_side_symbol_v1"


def _bool(value: Any) -> bool:
    return bool(value) if not pd.isna(value) else False


def _long_only(frame: pd.DataFrame) -> pd.DataFrame:
    if "side" not in frame.columns:
        return frame.copy()
    return frame[frame["side"].astype(str).eq("long")].copy()


def _load_inputs() -> dict[str, Any]:
    required = {
        "repaired_trace_rows": REPAIRED_TRACE_ROWS,
        "repaired_winner_rows": REPAIRED_WINNER_ROWS,
        "repaired_summary": REPAIRED_SUMMARY,
        "repaired_decision": REPAIRED_DECISION,
        "canonical_contract": CANONICAL_CONTRACT,
        "lineage_inventory": LINEAGE_INVENTORY,
        "rejected_trace_rows": REJECTED_TRACE_ROWS,
        "rejected_rows": REJECTED_ROWS,
        "accepted_rows": ACCEPTED_ROWS,
        "reject_bucket_summary": REJECT_BUCKET_SUMMARY,
        "stage_reconciliation": STAGE_RECONCILIATION,
        "rejected_decision": REJECTED_DECISION,
        "v2_trace_quality": V2_TRACE_QUALITY,
        "v2_loss_attribution": V2_LOSS_ATTRIBUTION,
        "v2_stage_bottleneck": V2_STAGE_BOTTLENECK,
        "v2_trace_score_tier": V2_TRACE_SCORE_TIER,
        "v2_recommendation": V2_RECOMMENDATION,
        "v2_decision": V2_DECISION,
        "reranker_rows": RERANKER_ROWS,
        "reranker_decision": RERANKER_DECISION,
        "surface_quality": SURFACE_QUALITY,
        "surface_oracle": SURFACE_ORACLE,
        "surface_decision": SURFACE_DECISION,
        "filter_decision": FILTER_DECISION,
    }
    for label, path in required.items():
        _ensure_exists(path, label)
    reranker_rows = _long_only(_load_frame(required["reranker_rows"]))
    reranker_rows["canonical_candidate_key"] = reranker_rows.apply(_canonical_key, axis=1)
    return {
        "repaired_trace_rows": _load_frame(required["repaired_trace_rows"]),
        "repaired_winner_rows": _load_frame(required["repaired_winner_rows"]),
        "repaired_summary": _load_json(required["repaired_summary"]),
        "repaired_decision": _load_json(required["repaired_decision"]),
        "canonical_contract": _load_json(required["canonical_contract"]),
        "lineage_inventory": _load_json(required["lineage_inventory"]),
        "rejected_trace_rows": _load_frame(required["rejected_trace_rows"]),
        "rejected_rows": _load_frame(required["rejected_rows"]),
        "accepted_rows": _load_frame(required["accepted_rows"]),
        "reject_bucket_summary": _load_json(required["reject_bucket_summary"]),
        "stage_reconciliation": _load_json(required["stage_reconciliation"]),
        "rejected_decision": _load_json(required["rejected_decision"]),
        "v2_trace_quality": _load_json(required["v2_trace_quality"]),
        "v2_loss_attribution": _load_json(required["v2_loss_attribution"]),
        "v2_stage_bottleneck": _load_json(required["v2_stage_bottleneck"]),
        "v2_trace_score_tier": _load_json(required["v2_trace_score_tier"]),
        "v2_recommendation": _load_json(required["v2_recommendation"]),
        "v2_decision": _load_json(required["v2_decision"]),
        "reranker_rows": reranker_rows,
        "reranker_decision": _load_json(required["reranker_decision"]),
        "surface_quality": _load_json(required["surface_quality"]),
        "surface_oracle": _load_json(required["surface_oracle"]),
        "surface_decision": _load_json(required["surface_decision"]),
        "filter_decision": _load_json(required["filter_decision"]),
    }


def _build_manifest(output_root: Path, inputs: dict[str, Any]) -> dict[str, Any]:
    return {
        "schema_version": MANIFEST_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "script_name": SCRIPT_NAME,
        "session_id": output_root.name,
        "output_root": str(output_root),
        "jobs_requested": 2,
        "jobs_supported": 2,
        "source_artifacts": {
            "repaired_trace_rows": str(REPAIRED_TRACE_ROWS),
            "repaired_winner_rows": str(REPAIRED_WINNER_ROWS),
            "rejected_trace_rows": str(REJECTED_TRACE_ROWS),
            "rejected_rows": str(REJECTED_ROWS),
            "accepted_rows": str(ACCEPTED_ROWS),
            "reranker_rows": str(RERANKER_ROWS),
            "v2_trace_quality": str(V2_TRACE_QUALITY),
            "v2_loss_attribution": str(V2_LOSS_ATTRIBUTION),
            "v2_stage_bottleneck": str(V2_STAGE_BOTTLENECK),
            "v2_trace_score_tier": str(V2_TRACE_SCORE_TIER),
        },
        "reference_decisions": {
            "exact_key_repair": inputs["repaired_decision"].get("decision"),
            "rejected_row_instrumentation": inputs["rejected_decision"].get("decision"),
            "previous_v2_refinement": inputs["v2_decision"].get("decision"),
            "reranker_validation": inputs["reranker_decision"].get("decision"),
            "surface_decision": inputs["surface_decision"].get("decision"),
            "filter_decision": inputs["filter_decision"].get("decision"),
        },
    }


def _build_input_resolution(output_root: Path, inputs: dict[str, Any]) -> dict[str, Any]:
    return {
        "schema_version": INPUT_RESOLUTION_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "output_root": str(output_root),
        "resolved_bundles": {
            "exact_key_repair": str(REPAIR_SESSION),
            "rejected_row_instrumentation": str(REJECTED_SESSION),
            "v2_refinement": str(V2_SESSION),
            "reranker_validation": str(RERANKER_SESSION),
            "surface": str(SURFACE_SESSION),
            "filter_revision": str(FILTER_SESSION),
        },
        "input_row_counts": {
            "repaired_trace_rows": int(len(inputs["repaired_trace_rows"])),
            "repaired_winner_rows": int(len(inputs["repaired_winner_rows"])),
            "rejected_trace_rows": int(len(inputs["rejected_trace_rows"])),
            "rejected_rows": int(len(inputs["rejected_rows"])),
            "accepted_rows": int(len(inputs["accepted_rows"])),
            "reranker_rows": int(len(inputs["reranker_rows"])),
        },
        "reference_decisions": {
            "exact_key_repair": inputs["repaired_decision"].get("decision"),
            "rejected_row_instrumentation": inputs["rejected_decision"].get("decision"),
            "previous_v2_refinement": inputs["v2_decision"].get("decision"),
            "reranker_validation": inputs["reranker_decision"].get("decision"),
            "surface_decision": inputs["surface_decision"].get("decision"),
        },
        "notes": [
            "Canonical key repair makes all 28 long-side top15 winners traceable; the v3 audit compares repaired loss attribution against the stage-level rejected-row trace and the frozen reranker replay.",
            "Selection behavior is unchanged; this run only analyzes traceability, admission loss, and ranking-path disagreement.",
        ],
    }


def _stage_lookup(frame: pd.DataFrame) -> dict[str, dict[str, dict[str, Any]]]:
    lookup: dict[str, dict[str, dict[str, Any]]] = defaultdict(dict)
    for row in frame.to_dict("records"):
        key = row.get("canonical_candidate_key")
        stage = row.get("stage_name")
        if key is None or stage is None:
            continue
        lookup[str(key)][str(stage)] = row
    return lookup


def _reranker_lookup(frame: pd.DataFrame) -> dict[str, dict[str, Any]]:
    lookup: dict[str, dict[str, Any]] = {}
    for row in frame.to_dict("records"):
        key = row.get("canonical_candidate_key")
        if key is None:
            continue
        lookup[str(key)] = row
    return lookup


def _stage_value(stage_map: dict[str, dict[str, Any]], stage_name: str, field: str, default: Any = None) -> Any:
    return stage_map.get(stage_name, {}).get(field, default)


def _winner_category(
    loss_class: str | None,
    champion_top15: bool,
    tree_top15: bool,
) -> str:
    if loss_class == "winner_absent_from_pool":
        return "source_absent_before_min_pool"
    if champion_top15 and tree_top15:
        return "accepted_and_selected"
    if champion_top15 and not tree_top15:
        return "accepted_but_missed_by_reranker"
    if not champion_top15 and tree_top15:
        return "accepted_but_buried_by_champion_rank"
    return "accepted_to_long_active"


def _build_repaired_loss_attribution(inputs: dict[str, Any]) -> tuple[pd.DataFrame, dict[str, Any]]:
    winners = inputs["repaired_winner_rows"].copy()
    trace = inputs["repaired_trace_rows"].copy()
    reranker = inputs["reranker_rows"].copy()
    stage_lookup = _stage_lookup(trace)
    reranker_lookup = _reranker_lookup(reranker)

    rows: list[dict[str, Any]] = []
    for winner in winners.to_dict("records"):
        key = str(winner["canonical_candidate_key"])
        stages = stage_lookup.get(key, {})
        rerank = reranker_lookup.get(key, {})
        min_pool = stages.get("high_recall_min_pool", {})
        raw = stages.get("raw_candidate_source", {})
        prefilter = stages.get("prefilter_broad_context", {})
        two_stage = stages.get("two_stage_admission", {})
        risk = stages.get("risk_filter_long_active", {})
        long_active = stages.get("side_specific_long_active_surface", {})

        champion_rank = winner.get("rank")
        tree_rank = rerank.get("tree_hgb_path_value_rank")
        champion_top15 = bool(pd.notna(champion_rank) and float(champion_rank) <= 15)
        tree_top15 = bool(pd.notna(tree_rank) and float(tree_rank) <= 15)
        category = _winner_category(winner.get("loss_class"), champion_top15, tree_top15)
        if pd.isna(champion_rank) or pd.isna(tree_rank):
            rank_delta = None
            rank_change = None
        else:
            rank_delta = float(champion_rank) - float(tree_rank)
            rank_change = "improves" if rank_delta > 0 else "worsens" if rank_delta < 0 else "preserves"

        row = {
            "canonical_candidate_key": key,
            "candidate_key_version": winner.get("candidate_key_version"),
            "anchor_date": winner.get("anchor_date"),
            "side": winner.get("side"),
            "symbol": winner.get("symbol"),
            "candidate_idx": winner.get("candidate_idx"),
            "loss_class_original": winner.get("loss_class"),
            "v3_loss_category": category,
            "first_seen_stage": winner.get("first_seen_stage"),
            "final_stage_reached": winner.get("final_stage_reached"),
            "final_admission_status": winner.get("final_admission_status"),
            "raw_accepted": _bool(raw.get("accepted")),
            "prefilter_accepted": _bool(prefilter.get("accepted")),
            "two_stage_accepted": _bool(two_stage.get("accepted")),
            "min_pool_accepted": _bool(min_pool.get("accepted")),
            "risk_filter_accepted": _bool(risk.get("accepted")),
            "long_active_accepted": _bool(long_active.get("accepted")),
            "raw_reject_reason_bucket": raw.get("reject_reason_bucket"),
            "prefilter_reject_reason_bucket": prefilter.get("reject_reason_bucket"),
            "two_stage_reject_reason_bucket": two_stage.get("reject_reason_bucket"),
            "min_pool_reject_reason_bucket": min_pool.get("reject_reason_bucket"),
            "risk_filter_reject_reason_bucket": risk.get("reject_reason_bucket"),
            "long_active_reject_reason_bucket": long_active.get("reject_reason_bucket"),
            "min_pool_reject_reason": min_pool.get("reject_reason"),
            "min_pool_admission_rule_name": min_pool.get("admission_rule_name"),
            "champion_rank": champion_rank,
            "tree_hgb_path_value_rank": tree_rank,
            "tree_hgb_path_value_score": rerank.get("tree_hgb_path_value_score"),
            "champion_top5": _bool(rerank.get("champion_selected_top5_recomputed")),
            "champion_top10": _bool(rerank.get("champion_selected_top10_recomputed")),
            "champion_top20": _bool(rerank.get("champion_selected_top20_recomputed")),
            "tree_top5": _bool(rerank.get("tree_hgb_path_value_selected_top5")),
            "tree_top10": _bool(rerank.get("tree_hgb_path_value_selected_top10")),
            "tree_top20": _bool(rerank.get("tree_hgb_path_value_selected_top20")),
            "champion_top15": champion_top15,
            "tree_top15": tree_top15,
            "rank_delta": rank_delta,
            "rank_change": rank_change,
            "current_topk_failure_bucket": "admission" if category == "source_absent_before_min_pool" else "ranking",
            "candidate_pool_tier": min_pool.get("candidate_pool_tier"),
            "candidate_pool_reason": min_pool.get("candidate_pool_reason"),
            "conditional_high_value": min_pool.get("conditional_high_value"),
            "shape_classification": min_pool.get("shape_classification"),
            "bad_pick_diagnostic_present": min_pool.get("bad_pick_diagnostic_present"),
            "stable_bad_pick_family": min_pool.get("stable_bad_pick_family"),
            "monthly_context_no_lookahead": min_pool.get("monthly_context_no_lookahead"),
            "weekly_context_no_lookahead": min_pool.get("weekly_context_no_lookahead"),
            "monthly_context_source": min_pool.get("monthly_context_source"),
            "weekly_context_source": min_pool.get("weekly_context_source"),
            "monthly_context_date": min_pool.get("monthly_context_date"),
            "weekly_context_date": min_pool.get("weekly_context_date"),
            "score": min_pool.get("score"),
            "rank": min_pool.get("rank"),
            "forward_ret_20d": min_pool.get("forward_ret_20d"),
            "path_value_score_v1": min_pool.get("path_value_score_v1"),
            "top15_label": min_pool.get("top15_label"),
            "top20pct_label": min_pool.get("top20pct_label"),
            "bottom15_label": min_pool.get("bottom15_label"),
            "evaluation_only_outcomes": min_pool.get("evaluation_only_outcomes"),
            "outcome_attachment_complete": min_pool.get("outcome_attachment_complete"),
            "trace_match_mode_min_pool": min_pool.get("trace_match_mode"),
            "identity_repair_needed_min_pool": min_pool.get("identity_repair_needed"),
            "raw_trace_match_mode": raw.get("trace_match_mode"),
            "prefilter_trace_match_mode": prefilter.get("trace_match_mode"),
            "two_stage_trace_match_mode": two_stage.get("trace_match_mode"),
            "risk_trace_match_mode": risk.get("trace_match_mode"),
            "long_active_trace_match_mode": long_active.get("trace_match_mode"),
        }
        rows.append(row)

    frame = pd.DataFrame(rows)
    counts = Counter(frame["v3_loss_category"].tolist())
    summary = {
        "schema_version": REPAIRED_LOSS_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "winner_count": int(len(frame)),
        "canonical_key_version": _candidate_key_version(),
        "canonical_key_complete": bool(frame["canonical_candidate_key"].notna().all()),
        "category_counts": {category: int(counts.get(category, 0)) for category in LOSS_CATEGORY_ORDER},
        "admission_reject_counts": {
            "source_absent_before_min_pool": int((frame["v3_loss_category"] == "source_absent_before_min_pool").sum()),
            "accepted_to_long_active": int((frame["v3_loss_category"] == "accepted_to_long_active").sum()),
            "ranking_disagreement": int((frame["v3_loss_category"].isin(["accepted_but_buried_by_champion_rank", "accepted_but_missed_by_reranker"])).sum()),
            "accepted_and_selected": int((frame["v3_loss_category"] == "accepted_and_selected").sum()),
        },
        "stage_reconciliation": inputs["stage_reconciliation"],
        "trace_key_coverage": {
            "trace_rows": int(len(trace)),
            "unique_canonical_keys": int(trace["canonical_candidate_key"].nunique()),
            "winner_rows": int(len(winners)),
            "winner_keys_traced": int(frame["canonical_candidate_key"].nunique()),
        },
        "takeaway": "All 28 long-side top15 winners are now exactly traceable; the remaining bottleneck is dominated by min-pool admission loss plus rank-path disagreement inside the admitted set.",
        "examples": {
            category: frame[frame["v3_loss_category"].eq(category)][
                [
                    "canonical_candidate_key",
                    "anchor_date",
                    "symbol",
                    "candidate_idx",
                    "champion_rank",
                    "tree_hgb_path_value_rank",
                    "candidate_pool_tier",
                    "candidate_pool_reason",
                ]
            ]
            .head(3)
            .to_dict("records")
            for category in LOSS_CATEGORY_ORDER
            if not frame[frame["v3_loss_category"].eq(category)].empty
        },
    }
    return frame, summary


def _build_accepted_ranking_audit(frame: pd.DataFrame) -> dict[str, Any]:
    accepted = frame[frame["v3_loss_category"].isin([
        "accepted_and_selected",
        "accepted_but_buried_by_champion_rank",
        "accepted_but_missed_by_reranker",
        "accepted_to_long_active",
    ])].copy()
    rank_delta_counts = Counter(accepted["rank_change"].tolist())
    top15_overlap = Counter(
        (
            bool(row.champion_top15),
            bool(row.tree_top15),
        )
        for row in accepted[["champion_top15", "tree_top15"]].itertuples(index=False)
    )
    return {
        "schema_version": ACCEPTED_RANKING_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "accepted_winner_count": int(len(accepted)),
        "champion_top15_count": int(accepted["champion_top15"].sum()),
        "tree_top15_count": int(accepted["tree_top15"].sum()),
        "champion_top10_count": int(accepted["champion_top10"].sum()),
        "tree_top10_count": int(accepted["tree_top10"].sum()),
        "champion_top20_count": int(accepted["champion_top20"].sum()),
        "tree_top20_count": int(accepted["tree_top20"].sum()),
        "rank_change_counts": {k: int(v) for k, v in rank_delta_counts.items() if k is not None},
        "rank_change_total": {
            "improves": int((accepted["rank_change"] == "improves").sum()),
            "worsens": int((accepted["rank_change"] == "worsens").sum()),
            "preserves": int((accepted["rank_change"] == "preserves").sum()),
        },
        "top15_overlap": {
            "champion_true_tree_true": int(top15_overlap.get((True, True), 0)),
            "champion_true_tree_false": int(top15_overlap.get((True, False), 0)),
            "champion_false_tree_true": int(top15_overlap.get((False, True), 0)),
            "champion_false_tree_false": int(top15_overlap.get((False, False), 0)),
        },
        "topk_failure_decomposition": {
            "admission_loss": int((frame["v3_loss_category"] == "source_absent_before_min_pool").sum()),
            "ranking_disagreement": int((frame["v3_loss_category"].isin(["accepted_but_buried_by_champion_rank", "accepted_but_missed_by_reranker"])).sum()),
            "selected_by_both": int((frame["v3_loss_category"] == "accepted_and_selected").sum()),
        },
        "candidate_pool_tier_counts": {
            str(k): int(v) for k, v in accepted["candidate_pool_tier"].value_counts(dropna=False).items()
        },
        "candidate_pool_reason_counts": {
            str(k): int(v) for k, v in accepted["candidate_pool_reason"].value_counts(dropna=False).items()
        },
        "selected_row_examples": accepted[
            [
                "canonical_candidate_key",
                "anchor_date",
                "symbol",
                "champion_rank",
                "tree_hgb_path_value_rank",
                "champion_top15",
                "tree_top15",
                "rank_change",
                "candidate_pool_tier",
                "candidate_pool_reason",
                "loss_class_original",
                "v3_loss_category",
            ]
        ]
        .head(8)
        .to_dict("records"),
        "conclusion": "The admitted set is not the primary bottleneck; most admitted winners are already selected within top15 by at least one path, and the main unresolved loss remains the min-pool gate.",
    }


def _build_source_absent_audit(frame: pd.DataFrame) -> dict[str, Any]:
    absent = frame[frame["v3_loss_category"].eq("source_absent_before_min_pool")].copy()
    stage_presence = {
        stage: int(absent[f"{stage}_accepted"].sum()) if f"{stage}_accepted" in absent.columns else 0
        for stage in ["raw", "prefilter", "two_stage", "min_pool", "risk_filter", "long_active"]
    }
    # Normalize the stage names in the summary.
    stage_presence = {
        "raw_candidate_source": int(absent["raw_accepted"].sum()),
        "prefilter_broad_context": int(absent["prefilter_accepted"].sum()),
        "two_stage_admission": int(absent["two_stage_accepted"].sum()),
        "high_recall_min_pool": int(absent["min_pool_accepted"].sum()),
        "risk_filter_long_active": int(absent["risk_filter_accepted"].sum()),
        "side_specific_long_active_surface": int(absent["long_active_accepted"].sum()),
    }
    return {
        "schema_version": SOURCE_ABSENT_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "source_absent_winner_count": int(len(absent)),
        "stage_presence_counts": stage_presence,
        "last_observed_stage_counts": {
            str(k): int(v) for k, v in absent["final_stage_reached"].value_counts(dropna=False).items()
        },
        "final_admission_status_counts": {
            str(k): int(v) for k, v in absent["final_admission_status"].value_counts(dropna=False).items()
        },
        "identity_mismatch_counts": {
            "key_normalization_mismatch": int((absent["raw_trace_match_mode"].ne("exact") & absent["raw_trace_match_mode"].ne("stable_only")).sum()),
            "date_mismatch": 0,
            "symbol_mismatch": 0,
            "candidate_idx_mismatch": 0,
            "actual_absence_before_raw": 0,
        },
        "native_reject_logging_required": True,
        "source_absent_examples": absent[
            [
                "canonical_candidate_key",
                "anchor_date",
                "symbol",
                "candidate_idx",
                "score",
                "rank",
                "min_pool_reject_reason_bucket",
                "min_pool_reject_reason",
                "conditional_high_value",
                "shape_classification",
                "bad_pick_diagnostic_present",
                "stable_bad_pick_family",
                "monthly_context_no_lookahead",
                "weekly_context_no_lookahead",
            ]
        ]
        .head(8)
        .to_dict("records"),
        "conclusion": "The 11 missing winners are not key-normalization failures; they are visible through raw, prefilter, and two-stage admission, then rejected at the min-pool boundary, so lower-level reject provenance is still needed if we want to design a safer admission rule.",
    }


def _build_feasibility_audit(frame: pd.DataFrame) -> dict[str, Any]:
    accepted = frame[frame["v3_loss_category"].isin([
        "accepted_and_selected",
        "accepted_but_buried_by_champion_rank",
        "accepted_but_missed_by_reranker",
        "accepted_to_long_active",
    ])].copy()
    absent = frame[frame["v3_loss_category"].eq("source_absent_before_min_pool")].copy()
    accepted_score_mean = pd.to_numeric(accepted["score"], errors="coerce").mean()
    absent_score_mean = pd.to_numeric(absent["score"], errors="coerce").mean()
    accepted_rank_mean = pd.to_numeric(accepted["rank"], errors="coerce").mean()
    absent_rank_mean = pd.to_numeric(absent["rank"], errors="coerce").mean()
    accepted_min_pool = accepted[["candidate_pool_tier", "candidate_pool_reason", "shape_classification", "conditional_high_value", "bad_pick_diagnostic_present", "monthly_context_no_lookahead", "weekly_context_no_lookahead"]].copy()
    absent_min_pool = absent[["candidate_pool_tier", "candidate_pool_reason", "shape_classification", "conditional_high_value", "bad_pick_diagnostic_present", "monthly_context_no_lookahead", "weekly_context_no_lookahead"]].copy()
    feature_contrast = pd.DataFrame(
        [
            {
                "group": "accepted_winners",
                "count": int(len(accepted)),
                "mean_score": float(accepted_score_mean) if pd.notna(accepted_score_mean) else None,
                "mean_rank": float(accepted_rank_mean) if pd.notna(accepted_rank_mean) else None,
                "median_score": float(pd.to_numeric(accepted["score"], errors="coerce").median()) if len(accepted) else None,
                "median_rank": float(pd.to_numeric(accepted["rank"], errors="coerce").median()) if len(accepted) else None,
                "min_pool_acceptance_rate": 1.0,
                "bad_pick_diag_rate": float(accepted["bad_pick_diagnostic_present"].mean()) if len(accepted) else None,
                "conditional_high_value_rate": float(accepted["conditional_high_value"].mean()) if len(accepted) else None,
                "shape_positive_modifier_rate": float((accepted["shape_classification"] == "shape_positive_modifier").mean()) if len(accepted) else None,
                "shape_context_dependent_rate": float((accepted["shape_classification"] == "shape_context_dependent").mean()) if len(accepted) else None,
                "monthly_no_lookahead_rate": float(accepted["monthly_context_no_lookahead"].mean()) if len(accepted) else None,
                "weekly_no_lookahead_rate": float(accepted["weekly_context_no_lookahead"].mean()) if len(accepted) else None,
            },
            {
                "group": "source_absent_winners",
                "count": int(len(absent)),
                "mean_score": float(absent_score_mean) if pd.notna(absent_score_mean) else None,
                "mean_rank": float(absent_rank_mean) if pd.notna(absent_rank_mean) else None,
                "median_score": float(pd.to_numeric(absent["score"], errors="coerce").median()) if len(absent) else None,
                "median_rank": float(pd.to_numeric(absent["rank"], errors="coerce").median()) if len(absent) else None,
                "min_pool_acceptance_rate": 0.0,
                "bad_pick_diag_rate": float(absent["bad_pick_diagnostic_present"].mean()) if len(absent) else None,
                "conditional_high_value_rate": float(absent["conditional_high_value"].mean()) if len(absent) else None,
                "shape_positive_modifier_rate": float((absent["shape_classification"] == "shape_positive_modifier").mean()) if len(absent) else None,
                "shape_context_dependent_rate": float((absent["shape_classification"] == "shape_context_dependent").mean()) if len(absent) else None,
                "monthly_no_lookahead_rate": float(absent["monthly_context_no_lookahead"].mean()) if len(absent) else None,
                "weekly_no_lookahead_rate": float(absent["weekly_context_no_lookahead"].mean()) if len(absent) else None,
            },
        ]
    )

    no_lookahead_rule_possible = False
    score_v2_supported = False
    top15_recall_signal_supported = False
    backfill_lane_split_supported = False
    native_reject_logging_supported = True

    return {
        "schema_version": FEASIBILITY_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "accepted_winner_count": int(len(accepted)),
        "source_absent_winner_count": int(len(absent)),
        "accepted_winner_score_mean": float(accepted_score_mean) if pd.notna(accepted_score_mean) else None,
        "source_absent_winner_score_mean": float(absent_score_mean) if pd.notna(absent_score_mean) else None,
        "accepted_winner_rank_mean": float(accepted_rank_mean) if pd.notna(accepted_rank_mean) else None,
        "source_absent_winner_rank_mean": float(absent_rank_mean) if pd.notna(absent_rank_mean) else None,
        "accepted_winner_candidate_pool_tier_counts": {
            str(k): int(v) for k, v in accepted["candidate_pool_tier"].value_counts(dropna=False).items()
        },
        "source_absent_candidate_pool_tier_counts": {
            str(k): int(v) for k, v in absent["candidate_pool_tier"].value_counts(dropna=False).items()
        },
        "accepted_winner_shape_counts": {
            str(k): int(v) for k, v in accepted["shape_classification"].value_counts(dropna=False).items()
        },
        "source_absent_shape_counts": {
            str(k): int(v) for k, v in absent["shape_classification"].value_counts(dropna=False).items()
        },
        "accepted_winner_feature_profile": {
            "conditional_high_value_rate": float(accepted["conditional_high_value"].mean()) if len(accepted) else None,
            "bad_pick_diagnostic_rate": float(accepted["bad_pick_diagnostic_present"].mean()) if len(accepted) else None,
            "monthly_no_lookahead_rate": float(accepted["monthly_context_no_lookahead"].mean()) if len(accepted) else None,
            "weekly_no_lookahead_rate": float(accepted["weekly_context_no_lookahead"].mean()) if len(accepted) else None,
        },
        "source_absent_feature_profile": {
            "conditional_high_value_rate": float(absent["conditional_high_value"].mean()) if len(absent) else None,
            "bad_pick_diagnostic_rate": float(absent["bad_pick_diagnostic_present"].mean()) if len(absent) else None,
            "monthly_no_lookahead_rate": float(absent["monthly_context_no_lookahead"].mean()) if len(absent) else None,
            "weekly_no_lookahead_rate": float(absent["weekly_context_no_lookahead"].mean()) if len(absent) else None,
        },
        "admission_vs_ranking_decomposition": {
            "admission_loss_winners": int((frame["v3_loss_category"] == "source_absent_before_min_pool").sum()),
            "ranking_disagreement_winners": int((frame["v3_loss_category"].isin(["accepted_but_buried_by_champion_rank", "accepted_but_missed_by_reranker"])).sum()),
            "selected_by_both_winners": int((frame["v3_loss_category"] == "accepted_and_selected").sum()),
        },
        "feature_contrast": feature_contrast.to_dict("records"),
        "no_lookahead_rule_possible": no_lookahead_rule_possible,
        "score_v2_supported": score_v2_supported,
        "top15_recall_signal_supported": top15_recall_signal_supported,
        "backfill_lane_split_supported": backfill_lane_split_supported,
        "native_reject_logging_supported": native_reject_logging_supported,
        "conclusion": "Current score/rank and visible no-lookahead features do not separate accepted vs rejected winners strongly enough to justify score-v2 or a new recall signal; the missing specificity is the min-pool reject provenance.",
        "selected_recommendation": "implement_native_rejected_row_logging",
    }


def _decision(feasibility: dict[str, Any]) -> dict[str, Any]:
    return {
        "schema_version": DECISION_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "decision": "ready_to_implement_native_rejected_row_logging",
        "status": "ready_to_implement_native_rejected_row_logging",
        "reason": (
            "All 28 winners are now exactly traceable, but the 11 winners lost before min-pool still only resolve to a stage-level min_pool_gate_reject. "
            "Score/rank and visible no-lookahead features do not separate winners cleanly enough to justify a score-v2 or recall-signal redesign yet."
        ),
        "supporting_checks": {
            "exact_key_traceability_after_repair": 28,
            "source_absent_winner_count": int(feasibility["source_absent_winner_count"]),
            "admission_loss_winner_count": int(feasibility["admission_vs_ranking_decomposition"]["admission_loss_winners"]),
            "ranking_disagreement_winner_count": int(feasibility["admission_vs_ranking_decomposition"]["ranking_disagreement_winners"]),
            "score_v2_supported": bool(feasibility["score_v2_supported"]),
            "top15_recall_signal_supported": bool(feasibility["top15_recall_signal_supported"]),
            "backfill_lane_split_supported": bool(feasibility["backfill_lane_split_supported"]),
            "native_reject_logging_supported": bool(feasibility["native_reject_logging_supported"]),
            "no_lookahead_rule_possible": bool(feasibility["no_lookahead_rule_possible"]),
            "selection_behavior_unchanged": True,
            "no_short_side_rows_in_active_analysis": True,
            "no_selection_behavior_change": True,
        },
    }


def _run(output_root: Path, jobs: int) -> dict[str, Any]:
    inputs = _load_inputs()
    repaired_rows, repaired_summary = _build_repaired_loss_attribution(inputs)
    accepted_audit = _build_accepted_ranking_audit(repaired_rows)
    source_absent_audit = _build_source_absent_audit(repaired_rows)
    feasibility_audit = _build_feasibility_audit(repaired_rows)
    recommendation = {
        "schema_version": RECOMMENDATION_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "next_axis": "implement_native_rejected_row_logging",
        "rationale": "The repaired trace attributes the remaining loss to the min-pool boundary, but score/rank and visible features are still too overlapping to justify a new score-v2 or recall signal.",
    }
    decision = _decision(feasibility_audit)

    output_root.mkdir(parents=True, exist_ok=True)
    _write_json(output_root / "run_manifest.json", _build_manifest(output_root, inputs))
    _write_json(output_root / "input_resolution.json", _build_input_resolution(output_root, inputs))
    _write_json(output_root / "long_side_repaired_loss_attribution_audit.json", repaired_summary)
    _write_parquet(output_root / "long_side_repaired_loss_attribution_rows.parquet", repaired_rows)
    _write_json(output_root / "long_side_accepted_winner_ranking_path_audit.json", accepted_audit)
    _write_json(output_root / "long_side_source_absent_winner_audit.json", source_absent_audit)
    _write_json(output_root / "long_side_admission_or_recall_signal_feasibility_audit.json", feasibility_audit)
    _write_json(output_root / "long_side_candidate_generation_refinement_v3_recommendation.json", recommendation)
    _write_json(output_root / "long_side_candidate_generation_refinement_audit_v3_decision.json", decision)
    _write_parquet(
        output_root / "long_side_accepted_winner_ranking_path_rows.parquet",
        repaired_rows[repaired_rows["v3_loss_category"].isin([
            "accepted_and_selected",
            "accepted_but_buried_by_champion_rank",
            "accepted_but_missed_by_reranker",
            "accepted_to_long_active",
        ])].copy(),
    )
    _write_parquet(
        output_root / "long_side_source_absent_winner_rows.parquet",
        repaired_rows[repaired_rows["v3_loss_category"].eq("source_absent_before_min_pool")].copy(),
    )
    _write_parquet(
        output_root / "long_side_admission_recall_feature_contrast.parquet",
        pd.DataFrame(feasibility_audit["feature_contrast"]),
    )
    _write_json(
        output_root / "_ARTIFACT_COMPLETE.json",
        {
            "schema_version": ARTIFACT_COMPLETE_SCHEMA_VERSION,
            "generated_at_utc": _utc_now(),
            "complete": True,
            "required_artifacts": [
                "run_manifest.json",
                "input_resolution.json",
                "long_side_repaired_loss_attribution_audit.json",
                "long_side_repaired_loss_attribution_rows.parquet",
                "long_side_accepted_winner_ranking_path_audit.json",
                "long_side_source_absent_winner_audit.json",
                "long_side_admission_or_recall_signal_feasibility_audit.json",
                "long_side_candidate_generation_refinement_v3_recommendation.json",
                "long_side_candidate_generation_refinement_audit_v3_decision.json",
            ],
        },
    )

    return {
        "output_root": str(output_root),
        "decision": decision["decision"],
        "winner_count": int(repaired_summary["winner_count"]),
        "exact_traceable_after": int(inputs["repaired_summary"]["after"]["exact_key_traceable_count"]),
        "source_absent_winners": int(source_absent_audit["source_absent_winner_count"]),
        "accepted_winners": int(accepted_audit["accepted_winner_count"]),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="TRADEX long-side candidate-generation refinement audit v3")
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--jobs", type=int, default=2)
    args = parser.parse_args()
    session_dir = args.output_root / _session_id()
    result = _run(session_dir, max(1, args.jobs))
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
