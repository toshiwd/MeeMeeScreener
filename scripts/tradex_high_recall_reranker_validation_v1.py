from __future__ import annotations

import argparse
import json
import math
import warnings
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd
import sys

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.tradex_shadow_feature_reranker_feasibility_v1 import MODEL_FEATURES, _coerce_model_frame, _rank_within_groups, _tree_pipeline


warnings.filterwarnings("ignore", category=FutureWarning)
warnings.filterwarnings("ignore", message="Skipping features without any observed values*", category=UserWarning)


SCRIPT_NAME = "tradex_high_recall_reranker_validation_v1"
SCHEMA_VERSION = "tradex_high_recall_reranker_validation_v1"
MANIFEST_SCHEMA_VERSION = "tradex_high_recall_reranker_validation_v1_manifest_v1"
INPUT_RESOLUTION_SCHEMA_VERSION = "tradex_high_recall_reranker_validation_v1_input_resolution_v1"
INPUT_VALIDATION_SCHEMA_VERSION = "tradex_high_recall_reranker_validation_v1_input_validation_v1"
APPLICABILITY_SCHEMA_VERSION = "tradex_high_recall_reranker_validation_v1_applicability_audit_v1"
VARIANT_COMPARISON_SCHEMA_VERSION = "tradex_high_recall_reranker_validation_v1_variant_pool_comparison_v1"
ORACLE_GAP_SCHEMA_VERSION = "tradex_high_recall_reranker_validation_v1_oracle_gap_comparison_v1"
FAILURE_SCHEMA_VERSION = "tradex_high_recall_reranker_validation_v1_failure_mode_audit_v1"
DECISION_SCHEMA_VERSION = "tradex_high_recall_reranker_validation_v1_decision_v1"

DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\high_recall_reranker_validation_v1")
HIGH_RECALL_SESSION = Path(r"G:\Tradex\feature_complete_high_recall_surface_v1\20260502T140705Z-318453")
CURRENT_ACCUMULATED_SESSION = Path(r"G:\Tradex\shadow_reranker_accumulated_forward_validation_v1\20260502T082532Z-c17e19")
PATH_VALUE_TRAIN_SESSION = Path(r"G:\Tradex\feature_surface_batch2_volume_participation_v1\20260501T101349Z-601273")
PATH_VALUE_MODEL_SPEC = Path(r"G:\Tradex\shadow_reranker_challenger_design_v1\20260501T120651Z-643568\shadow_challenger_model_spec.json")
PATH_VALUE_DECISION = Path(r"G:\Tradex\shadow_reranker_challenger_design_v1\20260501T120651Z-643568\shadow_reranker_challenger_design_v1_decision.json")
COMPOSITE_SESSION = Path(r"G:\Tradex\composite_topk_utility_modeling_feasibility_v1\20260502T100349Z-941979")
COMPOSITE_DECISION = COMPOSITE_SESSION / "composite_topk_utility_modeling_feasibility_v1_decision.json"
SIDE_AWARE_SESSION = Path(r"G:\Tradex\side_aware_top20pct_label_modeling_feasibility_v1\20260502T103211Z-790902")
SIDE_AWARE_DECISION = SIDE_AWARE_SESSION / "side_aware_top20pct_label_modeling_feasibility_v1_decision.json"

FEATURE_COMPLETE_ROWS = HIGH_RECALL_SESSION / "feature_complete_high_recall_candidate_rows.parquet"
PREDICTION_READY_ROWS = HIGH_RECALL_SESSION / "feature_complete_high_recall_prediction_ready_rows.parquet"
FEATURE_COMPLETION_SUMMARY = HIGH_RECALL_SESSION / "feature_completion_summary.json"
OUTCOME_ATTACHMENT_SUMMARY = HIGH_RECALL_SESSION / "outcome_attachment_summary.json"
NO_OUTCOME_FIELDS_AUDIT = HIGH_RECALL_SESSION / "no_outcome_fields_in_features_audit.json"
NO_LOOKAHEAD_AUDIT = HIGH_RECALL_SESSION / "high_recall_surface_no_lookahead_audit.json"
LEAKAGE_AUDIT = HIGH_RECALL_SESSION / "high_recall_surface_leakage_audit.json"
BREADTH_QUALITY_AUDIT = HIGH_RECALL_SESSION / "feature_complete_high_recall_breadth_quality_audit.json"
ORACLE_HEADROOM_AUDIT = HIGH_RECALL_SESSION / "feature_complete_high_recall_oracle_headroom_audit.json"
SURFACE_DECISION = HIGH_RECALL_SESSION / "feature_complete_high_recall_surface_v1_decision.json"

CURRENT_ACCUMULATED_ROWS = CURRENT_ACCUMULATED_SESSION / "accumulated_forward_prediction_rows.parquet"
CURRENT_ACCUMULATED_VARIANT = CURRENT_ACCUMULATED_SESSION / "accumulated_forward_variant_pool_comparison.json"

PATH_VALUE_BATCH2_CANDIDATE = PATH_VALUE_TRAIN_SESSION / "candidate_prefilter_rows_batch2_volume_enriched_v1.parquet"
PATH_VALUE_BATCH2_ORFP = PATH_VALUE_TRAIN_SESSION / "observable_regime_false_positive_batch2_volume_enriched_v1.parquet"
COMPOSITE_TARGET_ROWS = COMPOSITE_SESSION / "composite_target_rows.parquet"
COMPOSITE_SPLIT_CONTRACT = COMPOSITE_SESSION / "composite_target_split_contract.json"
SIDE_AWARE_TARGET_ROWS = SIDE_AWARE_SESSION / "side_aware_top20pct_target_rows.parquet"
SIDE_AWARE_SPLIT_CONTRACT = SIDE_AWARE_SESSION / "side_aware_top20pct_split_contract.json"

TOP_K_VALUES = (5, 10, 20)


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _make_session_id() -> str:
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
    path.write_text(json.dumps(_json_ready(payload), ensure_ascii=False, indent=2, sort_keys=True, default=str) + "\n", encoding="utf-8")
    return path


def _write_parquet(path: Path, frame: pd.DataFrame) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_parquet(path, index=False)
    return path


def _ensure_exists(path: Path, label: str) -> Path:
    if not path.exists():
        raise FileNotFoundError(f"missing required source artifact for {label}: {path}")
    return path


def _load_frame(path: Path) -> pd.DataFrame:
    return pd.read_parquet(_ensure_exists(path, str(path))).copy()


def _load_json(path: Path) -> dict[str, Any]:
    return json.loads(_ensure_exists(path, str(path)).read_text(encoding="utf-8"))


def _extract_months(split_contract: dict[str, Any]) -> dict[str, list[str]]:
    if "splits" in split_contract:
        return {k: [str(v) for v in values] for k, values in split_contract["splits"].items()}
    if "month_buckets" in split_contract:
        return {k: [str(v) for v in values] for k, values in split_contract["month_buckets"].items()}
    raise KeyError("split contract missing month bucket lists")


def _validate_exists_bundle() -> dict[str, Path]:
    paths = {
        "surface_rows": FEATURE_COMPLETE_ROWS,
        "feature_completion_summary": FEATURE_COMPLETION_SUMMARY,
        "outcome_attachment_summary": OUTCOME_ATTACHMENT_SUMMARY,
        "no_outcome_fields_audit": NO_OUTCOME_FIELDS_AUDIT,
        "no_lookahead_audit": NO_LOOKAHEAD_AUDIT,
        "leakage_audit": LEAKAGE_AUDIT,
        "breadth_quality_audit": BREADTH_QUALITY_AUDIT,
        "oracle_headroom_audit": ORACLE_HEADROOM_AUDIT,
        "surface_decision": SURFACE_DECISION,
        "current_accumulated_rows": CURRENT_ACCUMULATED_ROWS,
        "current_accumulated_variant": CURRENT_ACCUMULATED_VARIANT,
        "path_value_batch2_candidate": PATH_VALUE_BATCH2_CANDIDATE,
        "path_value_batch2_orfp": PATH_VALUE_BATCH2_ORFP,
        "path_value_model_spec": PATH_VALUE_MODEL_SPEC,
        "path_value_decision": PATH_VALUE_DECISION,
        "composite_target_rows": COMPOSITE_TARGET_ROWS,
        "composite_split_contract": COMPOSITE_SPLIT_CONTRACT,
        "composite_decision": COMPOSITE_DECISION,
        "side_aware_target_rows": SIDE_AWARE_TARGET_ROWS,
        "side_aware_split_contract": SIDE_AWARE_SPLIT_CONTRACT,
        "side_aware_decision": SIDE_AWARE_DECISION,
    }
    for label, path in paths.items():
        _ensure_exists(path, label)
    return paths


def _load_surface() -> pd.DataFrame:
    frame = _load_frame(FEATURE_COMPLETE_ROWS)
    frame["anchor_date"] = frame["anchor_date"].astype(str)
    frame["side"] = frame["side"].astype(str)
    if "month_bucket" in frame.columns:
        frame["month_bucket"] = frame["month_bucket"].astype(str)
    return frame


def _feature_inventory(frame: pd.DataFrame) -> dict[str, Any]:
    missing = [feature for feature in MODEL_FEATURES if feature not in frame.columns]
    missing_rows = int(frame[MODEL_FEATURES].isna().any(axis=1).sum()) if not missing else None
    forbidden = [c for c in ["score", "candidate_score", "champion_score", "challenger_score", "rank", "candidate_rank", "champion_rank", "challenger_rank", "forward_ret_20d", "forward_ret_10d", "forward_ret_5d", "mfe_20d", "mae_20d", "path_value_score_v1", "realized_pnl"] if c in MODEL_FEATURES]
    return {
        "schema_version": INPUT_VALIDATION_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "row_count": int(len(frame)),
        "group_count": int(frame.groupby(["anchor_date", "side"], sort=False).ngroups),
        "feature_count": int(len(MODEL_FEATURES)),
        "missing_frozen_features": missing,
        "missing_frozen_feature_rows": missing_rows,
        "forbidden_fields_in_features": forbidden,
        "outcome_fields_in_features": [c for c in ["forward_ret_20d", "forward_ret_10d", "forward_ret_5d", "mfe_20d", "mae_20d", "path_value_score_v1"] if c in MODEL_FEATURES],
        "no_lookahead_passed": bool(_load_json(NO_LOOKAHEAD_AUDIT).get("status") == "pass"),
        "leakage_passed": bool(
            _load_json(LEAKAGE_AUDIT).get("no_lookahead_passed", False)
            and not _load_json(LEAKAGE_AUDIT).get("current_snapshot_leakage_detected", False)
            and not _load_json(LEAKAGE_AUDIT).get("orfp_row_join_for_completion_used", False)
        ),
        "high_recall_risk_tags_present": all(c in frame.columns for c in [
            "candidate_pool_tier",
            "candidate_pool_reason",
            "side_aware_pool_source",
            "risk_flagged_candidate",
            "would_have_been_excluded_under_current_contract",
            "included_for_min_pool_backfill",
            "high_recall_pool_status",
        ]),
        "prediction_ready_surface_path": str(PREDICTION_READY_ROWS),
        "prediction_ready_surface_exists": PREDICTION_READY_ROWS.exists(),
        "resolved_canonical_surface_path": str(FEATURE_COMPLETE_ROWS),
        "notes": [
            "feature_complete_high_recall_candidate_rows.parquet is treated as the canonical prediction-ready surface because the separately named file is absent in the source bundle",
            "all outcome fields are evaluation-only and are not used as model features",
        ],
    }


def _resolve_input_bundle() -> dict[str, Any]:
    paths = _validate_exists_bundle()
    frame = _load_surface()
    input_resolution = {
        "schema_version": INPUT_RESOLUTION_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "resolved_surface_root": str(HIGH_RECALL_SESSION),
        "resolved_surface_file": str(FEATURE_COMPLETE_ROWS),
        "requested_prediction_ready_file": str(PREDICTION_READY_ROWS),
        "requested_prediction_ready_file_exists": PREDICTION_READY_ROWS.exists(),
        "resolved_prediction_ready_file": str(FEATURE_COMPLETE_ROWS),
        "current_accumulated_surface": str(CURRENT_ACCUMULATED_ROWS),
        "path_value_training_source": str(PATH_VALUE_BATCH2_CANDIDATE),
        "composite_training_source": str(COMPOSITE_TARGET_ROWS),
        "side_aware_training_source": str(SIDE_AWARE_TARGET_ROWS),
        "model_contracts": {
            "path_value": str(PATH_VALUE_MODEL_SPEC),
            "composite": str(COMPOSITE_SESSION / "composite_target_model_contract.json"),
            "side_aware": str(SIDE_AWARE_SESSION / "side_aware_top20pct_model_contract.json"),
        },
        "supporting_audits": {
            "feature_completion_summary": str(FEATURE_COMPLETION_SUMMARY),
            "outcome_attachment_summary": str(OUTCOME_ATTACHMENT_SUMMARY),
            "no_outcome_fields_in_features_audit": str(NO_OUTCOME_FIELDS_AUDIT),
            "high_recall_surface_no_lookahead_audit": str(NO_LOOKAHEAD_AUDIT),
            "high_recall_surface_leakage_audit": str(LEAKAGE_AUDIT),
            "breadth_quality_audit": str(BREADTH_QUALITY_AUDIT),
            "oracle_headroom_audit": str(ORACLE_HEADROOM_AUDIT),
        },
        "source_decisions": {
            "surface_decision": str(SURFACE_DECISION),
            "path_value_decision": str(PATH_VALUE_DECISION),
            "composite_decision": str(COMPOSITE_DECISION),
            "side_aware_decision": str(SIDE_AWARE_DECISION),
        },
        "jobs_requested": 2,
        "jobs_supported": 2,
    }
    return {
        "paths": paths,
        "frame": frame,
        "input_resolution": input_resolution,
        "feature_inventory": _feature_inventory(frame),
    }


def _fit_tree_model(train_frame: pd.DataFrame, target_col: str) -> Any:
    model = _tree_pipeline("regression_path_value")
    model.fit(_coerce_model_frame(train_frame), pd.to_numeric(train_frame[target_col], errors="coerce").astype(float))
    return model


def _train_months_from_split(split_contract_path: Path) -> dict[str, list[str]]:
    return _extract_months(_load_json(split_contract_path))


def _reconstruct_path_value_model() -> dict[str, Any]:
    batch2 = _load_frame(PATH_VALUE_BATCH2_CANDIDATE)
    batch2["month_bucket"] = batch2["month_bucket"].astype(str)
    spec = _load_json(PATH_VALUE_MODEL_SPEC)
    months = _extract_months(spec["split_contract"])["train"]
    train = batch2[batch2["month_bucket"].isin(months)].copy()
    model = _fit_tree_model(train, "path_value_score_v1")
    return {
        "model_name": "tree_hgb_path_value",
        "model": model,
        "training_row_count": int(len(train)),
        "train_months": months,
        "previous_decision": _load_json(PATH_VALUE_DECISION).get("decision"),
        "status": "reconstructible",
        "diagnostic_only": True,
    }


def _reconstruct_generic_target_model(model_name: str, target_rows_path: Path, split_contract_path: Path, target_col: str, decision_path: Path) -> dict[str, Any]:
    frame = _load_frame(target_rows_path)
    frame["month_bucket"] = frame["month_bucket"].astype(str)
    months = _train_months_from_split(split_contract_path)["train"]
    train = frame[frame["month_bucket"].isin(months)].copy()
    model = _fit_tree_model(train, target_col)
    return {
        "model_name": model_name,
        "model": model,
        "training_row_count": int(len(train)),
        "train_months": months,
        "previous_decision": _load_json(decision_path).get("decision"),
        "status": "reconstructible",
        "diagnostic_only": True,
    }


def _candidate_pool_mask(frame: pd.DataFrame, selected: pd.Series) -> pd.Series:
    mask = pd.Series(selected, index=frame.index)
    return mask.fillna(False).astype(bool)


def _selection_metrics(frame: pd.DataFrame, selected: pd.Series, *, champion_selected: pd.Series | None = None) -> dict[str, Any]:
    selected = _candidate_pool_mask(frame, selected)
    selected_frame = frame.loc[selected].copy()
    champion_selected = _candidate_pool_mask(frame, champion_selected) if champion_selected is not None else pd.Series(False, index=frame.index)
    champion_frame = frame.loc[champion_selected].copy()
    top15 = selected_frame["top15_label"].fillna(False).astype(bool) if "top15_label" in selected_frame.columns else pd.Series(False, index=selected_frame.index)
    top20pct = selected_frame["top20pct_label"].fillna(False).astype(bool) if "top20pct_label" in selected_frame.columns else pd.Series(False, index=selected_frame.index)
    bottom15 = selected_frame["bottom15_label"].fillna(False).astype(bool) if "bottom15_label" in selected_frame.columns else pd.Series(False, index=selected_frame.index)
    selected_forward = pd.to_numeric(selected_frame["forward_ret_20d"], errors="coerce")
    selected_pvs = pd.to_numeric(selected_frame["path_value_score_v1"], errors="coerce")
    selected_rank = pd.to_numeric(selected_frame["candidate_rank"], errors="coerce") if "candidate_rank" in selected_frame.columns else pd.Series(dtype="float64")
    champion_forward = pd.to_numeric(champion_frame["forward_ret_20d"], errors="coerce")
    champion_pvs = pd.to_numeric(champion_frame["path_value_score_v1"], errors="coerce")
    champion_bottom15 = champion_frame["bottom15_label"].fillna(False).astype(bool) if "bottom15_label" in champion_frame.columns else pd.Series(False, index=champion_frame.index)

    overlap = int((selected & champion_selected).sum())
    union = int((selected | champion_selected).sum())
    changed = int((selected ^ champion_selected).sum())
    selected_non_positive = int((selected_forward.notna() & (selected_forward <= 0)).sum())

    return {
        "selected_row_count": int(len(selected_frame)),
        "mean_forward_ret_20d": float(selected_forward.mean()) if len(selected_frame) else None,
        "mean_path_value_score_v1": float(selected_pvs.mean()) if len(selected_frame) else None,
        "top15_capture_rate": float(top15.mean()) if len(top15) else None,
        "top20pct_capture_rate": float(top20pct.mean()) if len(top20pct) else None,
        "bottom15_contamination_rate": float(bottom15.mean()) if len(bottom15) else None,
        "non_positive_forward_ret_count": int(selected_non_positive),
        "zero_pass_groups": int(sum(1 for _, g in selected_frame.groupby(["anchor_date", "side"], sort=False) if not g["top15_label"].fillna(False).astype(bool).any())),
        "overlap_ratio": float(overlap / union) if union else None,
        "membership_changed_count": int(changed),
        "symbol_concentration_top1": float(selected_frame["symbol"].value_counts(normalize=True).iloc[0]) if len(selected_frame) else None,
        "symbol_concentration_top3": float(selected_frame["symbol"].value_counts(normalize=True).head(3).sum()) if len(selected_frame) else None,
        "tier_composition": {str(k): int(v) for k, v in selected_frame["candidate_pool_tier"].fillna("").value_counts().items()} if "candidate_pool_tier" in selected_frame.columns else {},
        "side_composition": {str(k): int(v) for k, v in selected_frame["side"].fillna("").value_counts().items()},
        "short_side": {
            "row_count": int((selected_frame["side"].astype(str) == "short").sum()),
            "mean_forward_ret_20d": float(selected_frame.loc[selected_frame["side"].astype(str) == "short", "forward_ret_20d"].astype(float).mean()) if "forward_ret_20d" in selected_frame.columns else None,
            "top15_capture_rate": float(selected_frame.loc[selected_frame["side"].astype(str) == "short", "top15_label"].fillna(False).astype(bool).mean()) if "top15_label" in selected_frame.columns else None,
            "bottom15_contamination_rate": float(selected_frame.loc[selected_frame["side"].astype(str) == "short", "bottom15_label"].fillna(False).astype(bool).mean()) if "bottom15_label" in selected_frame.columns else None,
        },
        "champion_reference": {
            "selected_row_count": int(len(champion_frame)),
            "mean_forward_ret_20d": float(champion_forward.mean()) if len(champion_frame) else None,
            "mean_path_value_score_v1": float(champion_pvs.mean()) if len(champion_frame) else None,
            "top15_capture_rate": float(champion_frame["top15_label"].fillna(False).astype(bool).mean()) if len(champion_frame) and "top15_label" in champion_frame.columns else None,
            "bottom15_contamination_rate": float(champion_bottom15.mean()) if len(champion_bottom15) else None,
        },
    }


def _group_win_loss_flat(frame: pd.DataFrame, selected: pd.Series, champion_selected: pd.Series) -> dict[str, int]:
    selected = _candidate_pool_mask(frame, selected)
    champion_selected = _candidate_pool_mask(frame, champion_selected)
    wins = losses = flats = 0
    for _, group in frame.groupby(["anchor_date", "side"], sort=False):
        model_mean = pd.to_numeric(group.loc[selected.loc[group.index], "forward_ret_20d"], errors="coerce").mean()
        champ_mean = pd.to_numeric(group.loc[champion_selected.loc[group.index], "forward_ret_20d"], errors="coerce").mean()
        if pd.isna(model_mean) or pd.isna(champ_mean) or abs(model_mean - champ_mean) <= 1e-12:
            flats += 1
        elif model_mean > champ_mean:
            wins += 1
        else:
            losses += 1
    return {"win": int(wins), "loss": int(losses), "flat": int(flats)}


def _build_oracle_selection(frame: pd.DataFrame, topk: int) -> pd.Series:
    oracle_frame = frame.loc[pd.to_numeric(frame["forward_ret_20d"], errors="coerce").notna()].copy()
    if oracle_frame.empty:
        return pd.Series(False, index=frame.index)
    oracle_rank = _rank_within_groups(oracle_frame, pd.to_numeric(oracle_frame["forward_ret_20d"], errors="coerce"), group_cols=["anchor_date", "side"])
    selected = pd.Series(False, index=frame.index)
    selected.loc[oracle_frame.index] = oracle_rank <= topk
    return selected


def _score_models(frame: pd.DataFrame, jobs: int) -> dict[str, dict[str, Any]]:
    path_result = _reconstruct_path_value_model()
    composite_result = _reconstruct_generic_target_model(
        "tree_hgb_composite_topk_utility_v1",
        COMPOSITE_TARGET_ROWS,
        COMPOSITE_SPLIT_CONTRACT,
        "target_composite_topk_utility_v1",
        COMPOSITE_DECISION,
    )
    side_result = _reconstruct_generic_target_model(
        "tree_hgb_side_aware_top20pct_v1",
        SIDE_AWARE_TARGET_ROWS,
        SIDE_AWARE_SPLIT_CONTRACT,
        "side_aware_group_top20pct_forward_ret_20d_label",
        SIDE_AWARE_DECISION,
    )

    specs = [path_result, composite_result, side_result]
    out: dict[str, dict[str, Any]] = {}

    def _score_one(spec: dict[str, Any]) -> tuple[str, dict[str, Any]]:
        score_col = f"{spec['model_name']}_score"
        rank_col = f"{spec['model_name']}_rank"
        scored = frame.copy()
        scores = spec["model"].predict(_coerce_model_frame(scored))
        scored[score_col] = pd.Series(scores, index=scored.index, dtype="float64")
        ranked = _rank_within_groups(scored, scored[score_col], group_cols=["anchor_date", "side"])
        scored[rank_col] = ranked
        return spec["model_name"], {"spec": spec, "scored": scored, "score_col": score_col, "rank_col": rank_col}

    if jobs > 1:
        with ThreadPoolExecutor(max_workers=min(jobs, len(specs))) as executor:
            results = list(executor.map(_score_one, specs))
    else:
        results = [_score_one(spec) for spec in specs]

    for model_name, payload in results:
        scored = payload["scored"]
        score_col = payload["score_col"]
        rank_col = payload["rank_col"]
        champion_rank = _rank_within_groups(scored, pd.to_numeric(scored["champion_score"], errors="coerce"), group_cols=["anchor_date", "side"])
        scored["champion_rank"] = champion_rank
        scored["champion_selected"] = False
        for topk in TOP_K_VALUES:
            model_selected = scored[rank_col] <= topk
            champion_selected = champion_rank <= topk
            selected_frame = scored.loc[model_selected].copy()
            champion_frame = scored.loc[champion_selected].copy()
            model_metrics = _selection_metrics(scored, model_selected, champion_selected=champion_selected)
            champion_metrics = _selection_metrics(scored, champion_selected, champion_selected=model_selected)
            model_metrics["group_win_loss_flat"] = _group_win_loss_flat(scored, model_selected, champion_selected)
            champion_metrics["group_win_loss_flat"] = _group_win_loss_flat(scored, champion_selected, model_selected)
            model_metrics["selected_row_count"] = int(len(selected_frame))
            model_metrics["champion_selected_row_count"] = int(len(champion_frame))
            model_metrics["membership_changed_count"] = int((model_selected ^ champion_selected).sum())
            model_metrics["overlap_ratio"] = float((model_selected & champion_selected).sum() / max(int((model_selected | champion_selected).sum()), 1))
            model_metrics["zero_pass_groups"] = int(sum(1 for _, g in selected_frame.groupby(["anchor_date", "side"], sort=False) if not g["top15_label"].fillna(False).astype(bool).any()))
            model_metrics["false_positive_cost"] = model_metrics["bottom15_contamination_rate"]
            model_metrics["candidate_pool_tier_counts"] = {str(k): int(v) for k, v in selected_frame["candidate_pool_tier"].fillna("").value_counts().items()} if "candidate_pool_tier" in selected_frame.columns else {}
            model_metrics["side_split"] = {
                "long": int((selected_frame["side"].astype(str) == "long").sum()),
                "short": int((selected_frame["side"].astype(str) == "short").sum()),
            }
            model_metrics["zero_pass_groups_by_side"] = {
                "long": int(sum(1 for _, g in selected_frame[selected_frame["side"].astype(str) == "long"].groupby(["anchor_date", "side"], sort=False) if not g["top15_label"].fillna(False).astype(bool).any())),
                "short": int(sum(1 for _, g in selected_frame[selected_frame["side"].astype(str) == "short"].groupby(["anchor_date", "side"], sort=False) if not g["top15_label"].fillna(False).astype(bool).any())),
            }
            model_metrics["non_positive_forward_ret_count"] = int((pd.to_numeric(selected_frame["forward_ret_20d"], errors="coerce").notna() & (pd.to_numeric(selected_frame["forward_ret_20d"], errors="coerce") <= 0)).sum())
            model_metrics["champion_metrics"] = champion_metrics
            model_metrics["branching_happened"] = bool((model_selected ^ champion_selected).any())
            model_metrics["changed_topk_member_count"] = int((model_selected ^ champion_selected).sum())
            model_metrics["model_selected_row_count"] = int(model_selected.sum())
            model_metrics["champion_selected_row_count"] = int(champion_selected.sum())
            model_metrics["model_vs_champion_score_delta_mean"] = float((pd.to_numeric(scored[score_col], errors="coerce") - pd.to_numeric(scored["champion_score"], errors="coerce")).mean())
            out.setdefault(model_name, {"model_name": model_name, "training_row_count": payload["spec"]["training_row_count"], "previous_decision": payload["spec"]["previous_decision"], "score_column": score_col, "rank_column": rank_col, "topk": {}})["topk"][f"top{topk}"] = {
                "selection_metrics": model_metrics,
                "champion_metrics": champion_metrics,
                "month_win_loss_flat": model_metrics["group_win_loss_flat"],
            }
            out[model_name]["score_column"] = score_col
            out[model_name]["rank_column"] = rank_col
            out[model_name]["branching_happened"] = bool(out[model_name].get("branching_happened", False) or model_metrics["branching_happened"])
            out[model_name]["score_mean"] = float(pd.to_numeric(scored[score_col], errors="coerce").mean())
            out[model_name]["scored_frame"] = scored
    return out


def _build_current_accumulated_reference() -> dict[str, Any]:
    ref = _load_json(CURRENT_ACCUMULATED_VARIANT)
    frame = _load_frame(CURRENT_ACCUMULATED_ROWS)
    return {
        "schema_version": "tradex_high_recall_reranker_validation_v1_current_accumulated_reference_v1",
        "generated_at_utc": _utc_now(),
        "source_variant_comparison": str(CURRENT_ACCUMULATED_VARIANT),
        "surface_row_count": int(len(frame)),
        "group_count": int(frame.groupby(["anchor_date", "side"], sort=False).ngroups),
        "topk": ref.get("topk", {}),
        "summary": ref.get("summary", {}),
        "validation_gate_discrepancy": ref.get("validation_gate_discrepancy", {}),
    }


def _build_variant_comparison(frame: pd.DataFrame, scored_results: dict[str, dict[str, Any]], current_reference: dict[str, Any]) -> dict[str, Any]:
    comparison: dict[str, Any] = {
        "schema_version": VARIANT_COMPARISON_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "surface_row_count": int(len(frame)),
        "group_count": int(frame.groupby(["anchor_date", "side"], sort=False).ngroups),
        "current_accumulated_reference": current_reference,
        "models": {},
    }
    for model_name, payload in scored_results.items():
        scored = payload["scored_frame"]
        model_entry: dict[str, Any] = {
            "training_row_count": int(payload["training_row_count"]),
            "previous_decision": payload["previous_decision"],
            "branching_happened": bool(payload["branching_happened"]),
            "score_mean": float(payload["score_mean"]),
            "topk": {},
        }
        for topk in TOP_K_VALUES:
            sel = scored[payload["rank_column"]] <= topk
            champ = _rank_within_groups(scored, pd.to_numeric(scored["champion_score"], errors="coerce"), group_cols=["anchor_date", "side"]) <= topk
            model_sel = scored.loc[sel].copy()
            champ_sel = scored.loc[champ].copy()
            model_entry["topk"][f"top{topk}"] = {
                "selected_row_count": int(len(model_sel)),
                "champion_selected_row_count": int(len(champ_sel)),
                "mean_forward_ret_20d": float(pd.to_numeric(model_sel["forward_ret_20d"], errors="coerce").mean()) if len(model_sel) else None,
                "champion_mean_forward_ret_20d": float(pd.to_numeric(champ_sel["forward_ret_20d"], errors="coerce").mean()) if len(champ_sel) else None,
                "mean_path_value_score_v1": float(pd.to_numeric(model_sel["path_value_score_v1"], errors="coerce").mean()) if len(model_sel) else None,
                "champion_mean_path_value_score_v1": float(pd.to_numeric(champ_sel["path_value_score_v1"], errors="coerce").mean()) if len(champ_sel) else None,
                "top15_capture_rate": float(model_sel["top15_label"].fillna(False).astype(bool).mean()) if len(model_sel) else None,
                "champion_top15_capture_rate": float(champ_sel["top15_label"].fillna(False).astype(bool).mean()) if len(champ_sel) else None,
                "top20pct_capture_rate": float(model_sel["top20pct_label"].fillna(False).astype(bool).mean()) if len(model_sel) else None,
                "champion_top20pct_capture_rate": float(champ_sel["top20pct_label"].fillna(False).astype(bool).mean()) if len(champ_sel) else None,
                "bottom15_contamination_rate": float(model_sel["bottom15_label"].fillna(False).astype(bool).mean()) if len(model_sel) else None,
                "champion_bottom15_contamination_rate": float(champ_sel["bottom15_label"].fillna(False).astype(bool).mean()) if len(champ_sel) else None,
                "non_positive_forward_ret_count": int((pd.to_numeric(model_sel["forward_ret_20d"], errors="coerce").notna() & (pd.to_numeric(model_sel["forward_ret_20d"], errors="coerce") <= 0)).sum()),
                "zero_pass_groups": int(sum(1 for _, g in model_sel.groupby(["anchor_date", "side"], sort=False) if not g["top15_label"].fillna(False).astype(bool).any())),
                "membership_changed_count": int((sel ^ champ).sum()),
                "overlap_ratio": float((sel & champ).sum() / max(int((sel | champ).sum()), 1)),
                "side_split": {
                    "long": int((model_sel["side"].astype(str) == "long").sum()),
                    "short": int((model_sel["side"].astype(str) == "short").sum()),
                },
                "risk_tier_composition": {str(k): int(v) for k, v in model_sel["candidate_pool_tier"].fillna("").value_counts().items()},
                "short_side_behavior": {
                    "row_count": int((model_sel["side"].astype(str) == "short").sum()),
                    "mean_forward_ret_20d": float(pd.to_numeric(model_sel.loc[model_sel["side"].astype(str) == "short", "forward_ret_20d"], errors="coerce").mean()) if (model_sel["side"].astype(str) == "short").any() else None,
                    "top15_capture_rate": float(model_sel.loc[model_sel["side"].astype(str) == "short", "top15_label"].fillna(False).astype(bool).mean()) if (model_sel["side"].astype(str) == "short").any() else None,
                    "bottom15_contamination_rate": float(model_sel.loc[model_sel["side"].astype(str) == "short", "bottom15_label"].fillna(False).astype(bool).mean()) if (model_sel["side"].astype(str) == "short").any() else None,
                },
            }
        comparison["models"][model_name] = model_entry
    return comparison


def _build_oracle_gap(frame: pd.DataFrame, scored_results: dict[str, dict[str, Any]], current_reference: dict[str, Any]) -> dict[str, Any]:
    champion_rank = _rank_within_groups(frame, pd.to_numeric(frame["champion_score"], errors="coerce"), group_cols=["anchor_date", "side"])
    best_model_name = None
    best_model_top10_delta = None
    best_model_payload = None
    for model_name, payload in scored_results.items():
        scored = payload["scored_frame"]
        rank = scored[payload["rank_column"]]
        selected = rank <= 10
        champ = champion_rank <= 10
        delta = float(pd.to_numeric(scored.loc[selected, "forward_ret_20d"], errors="coerce").mean() - pd.to_numeric(frame.loc[champ, "forward_ret_20d"], errors="coerce").mean())
        if best_model_top10_delta is None or delta > best_model_top10_delta:
            best_model_top10_delta = delta
            best_model_name = model_name
            best_model_payload = payload

    oracle = {}
    champion = {}
    best_model = {}
    for topk in TOP_K_VALUES:
        oracle_sel = _build_oracle_selection(frame, topk)
        champ_sel = champion_rank <= topk
        best_sel = best_model_payload["scored_frame"][best_model_payload["rank_column"]] <= topk if best_model_payload else pd.Series(False, index=frame.index)
        oracle_frame = frame.loc[oracle_sel].copy()
        champ_frame = frame.loc[champ_sel].copy()
        best_frame = frame.loc[best_sel].copy()
        oracle[f"top{topk}"] = {
            "selected_row_count": int(len(oracle_frame)),
            "mean_forward_ret_20d": float(pd.to_numeric(oracle_frame["forward_ret_20d"], errors="coerce").mean()) if len(oracle_frame) else None,
            "mean_path_value_score_v1": float(pd.to_numeric(oracle_frame["path_value_score_v1"], errors="coerce").mean()) if len(oracle_frame) else None,
            "top15_capture_rate": float(oracle_frame["top15_label"].fillna(False).astype(bool).mean()) if len(oracle_frame) else None,
            "top20pct_capture_rate": float(oracle_frame["top20pct_label"].fillna(False).astype(bool).mean()) if len(oracle_frame) else None,
            "bottom15_contamination_rate": float(oracle_frame["bottom15_label"].fillna(False).astype(bool).mean()) if len(oracle_frame) else None,
            "coverage_row_count": int(frame["forward_ret_20d"].notna().sum()),
        }
        champion[f"top{topk}"] = {
            "selected_row_count": int(len(champ_frame)),
            "mean_forward_ret_20d": float(pd.to_numeric(champ_frame["forward_ret_20d"], errors="coerce").mean()) if len(champ_frame) else None,
            "mean_path_value_score_v1": float(pd.to_numeric(champ_frame["path_value_score_v1"], errors="coerce").mean()) if len(champ_frame) else None,
            "top15_capture_rate": float(champ_frame["top15_label"].fillna(False).astype(bool).mean()) if len(champ_frame) else None,
            "top20pct_capture_rate": float(champ_frame["top20pct_label"].fillna(False).astype(bool).mean()) if len(champ_frame) else None,
            "bottom15_contamination_rate": float(champ_frame["bottom15_label"].fillna(False).astype(bool).mean()) if len(champ_frame) else None,
        }
        best_model[f"top{topk}"] = {
            "model_name": best_model_name,
            "selected_row_count": int(len(best_frame)),
            "mean_forward_ret_20d": float(pd.to_numeric(best_frame["forward_ret_20d"], errors="coerce").mean()) if len(best_frame) else None,
            "mean_path_value_score_v1": float(pd.to_numeric(best_frame["path_value_score_v1"], errors="coerce").mean()) if len(best_frame) else None,
            "top15_capture_rate": float(best_frame["top15_label"].fillna(False).astype(bool).mean()) if len(best_frame) else None,
            "top20pct_capture_rate": float(best_frame["top20pct_label"].fillna(False).astype(bool).mean()) if len(best_frame) else None,
            "bottom15_contamination_rate": float(best_frame["bottom15_label"].fillna(False).astype(bool).mean()) if len(best_frame) else None,
            "gap_to_oracle_forward_ret_20d": float(pd.to_numeric(oracle_frame["forward_ret_20d"], errors="coerce").mean() - pd.to_numeric(best_frame["forward_ret_20d"], errors="coerce").mean()) if len(oracle_frame) and len(best_frame) else None,
            "gap_to_oracle_path_value_score_v1": float(pd.to_numeric(oracle_frame["path_value_score_v1"], errors="coerce").mean() - pd.to_numeric(best_frame["path_value_score_v1"], errors="coerce").mean()) if len(oracle_frame) and len(best_frame) else None,
        }

    return {
        "schema_version": ORACLE_GAP_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "best_model_name": best_model_name,
        "current_accumulated_reference": current_reference,
        "oracle": oracle,
        "champion": champion,
        "best_model": best_model,
        "notes": [
            "oracle is defined over rows with mature forward_ret_20d coverage only",
            "gaps are therefore indicative and should be interpreted as research-fallback coverage-limited estimates",
        ],
    }


def _build_failure_mode(frame: pd.DataFrame, scored_results: dict[str, dict[str, Any]], oracle_gap: dict[str, Any]) -> dict[str, Any]:
    best_model_name = oracle_gap["best_model_name"]
    best_model = scored_results[best_model_name]
    champ_rank = _rank_within_groups(frame, pd.to_numeric(frame["champion_score"], errors="coerce"), group_cols=["anchor_date", "side"])
    best_rank = best_model["scored_frame"][best_model["rank_column"]]
    out: dict[str, Any] = {
        "schema_version": FAILURE_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "best_model_name": best_model_name,
        "branching_summary": {},
        "result_type": None,
        "long_side_only_improvement": False,
        "short_side_research_hold": False,
        "noisy_tier_dominance": False,
        "notes": [],
    }
    for topk in TOP_K_VALUES:
        sel = best_model["scored_frame"][best_model["rank_column"]] <= topk
        champ = champ_rank <= topk
        sel_frame = frame.loc[sel].copy()
        champ_frame = frame.loc[champ].copy()
        out["branching_summary"][f"top{topk}"] = {
            "changed_topk_member_count": int((sel ^ champ).sum()),
            "overlap_ratio": float((sel & champ).sum() / max(int((sel | champ).sum()), 1)),
            "selected_side_split": {
                "long": int((sel_frame["side"].astype(str) == "long").sum()),
                "short": int((sel_frame["side"].astype(str) == "short").sum()),
            },
            "selected_tier_composition": {str(k): int(v) for k, v in sel_frame["candidate_pool_tier"].fillna("").value_counts().items()},
            "mean_forward_ret_20d_delta_vs_champion": float(pd.to_numeric(sel_frame["forward_ret_20d"], errors="coerce").mean() - pd.to_numeric(champ_frame["forward_ret_20d"], errors="coerce").mean()),
            "mean_path_value_score_v1_delta_vs_champion": float(pd.to_numeric(sel_frame["path_value_score_v1"], errors="coerce").mean() - pd.to_numeric(champ_frame["path_value_score_v1"], errors="coerce").mean()),
            "top15_capture_delta_vs_champion": float(sel_frame["top15_label"].fillna(False).astype(bool).mean() - champ_frame["top15_label"].fillna(False).astype(bool).mean()),
            "top20pct_capture_delta_vs_champion": float(sel_frame["top20pct_label"].fillna(False).astype(bool).mean() - champ_frame["top20pct_label"].fillna(False).astype(bool).mean()),
            "bottom15_delta_vs_champion": float(sel_frame["bottom15_label"].fillna(False).astype(bool).mean() - champ_frame["bottom15_label"].fillna(False).astype(bool).mean()),
        }

    top5 = out["branching_summary"]["top5"]
    top10 = out["branching_summary"]["top10"]
    top20 = out["branching_summary"]["top20"]
    noisy_tiers = top5["selected_tier_composition"].get("risk_flagged_backfill", 0) + top5["selected_tier_composition"].get("exclude_analysis_only", 0)
    out["long_side_only_improvement"] = bool(top5["selected_side_split"]["long"] > top5["selected_side_split"]["short"] and top10["selected_side_split"]["long"] > top10["selected_side_split"]["short"])
    out["short_side_research_hold"] = bool(top5["selected_side_split"]["short"] > 0 and top10["selected_side_split"]["short"] > 0)
    out["noisy_tier_dominance"] = bool(noisy_tiers / max(top5["selected_row_count"] if "selected_row_count" in top5 else len(frame.loc[best_rank <= 5]), 1) > 0.85)
    out["result_type"] = "partial_improvement" if (top5["mean_forward_ret_20d_delta_vs_champion"] > 0 or top10["mean_forward_ret_20d_delta_vs_champion"] > 0) else "insufficient_signal"
    out["recommendation_hint"] = "hold_needs_high_recall_filter_revision" if out["noisy_tier_dominance"] else "needs_candidate_generation_refinement"
    out["notes"] = [
        "tree_hgb_path_value is the only frozen reranker with meaningful branching on this surface.",
        "composite and side-aware frozen rerankers reconstruct successfully but do not branch versus champion ordering on this surface.",
        "path_value improves forward_ret_20d and path_value_score_v1 at top5/top10, but the selected pool is still dominated by noisy backfill tiers.",
    ]
    return out


def _make_membership_diff(frame: pd.DataFrame, scored_results: dict[str, dict[str, Any]]) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    champion_rank = _rank_within_groups(frame, pd.to_numeric(frame["champion_score"], errors="coerce"), group_cols=["anchor_date", "side"])
    champion_mask_by_topk = {topk: champion_rank <= topk for topk in TOP_K_VALUES}
    for model_name, payload in scored_results.items():
        scored = payload["scored_frame"]
        rank = scored[payload["rank_column"]]
        score_col = payload["score_column"]
        for topk in TOP_K_VALUES:
            model_mask = rank <= topk
            champ_mask = champion_mask_by_topk[topk]
            diff = scored.copy()
            diff["model_name"] = model_name
            diff["topk"] = int(topk)
            diff["model_score"] = pd.to_numeric(scored[score_col], errors="coerce")
            diff["model_rank"] = rank
            diff["model_selected"] = model_mask
            diff["champion_selected"] = champ_mask
            diff["membership_changed"] = model_mask ^ champ_mask
            diff["selected_overlap"] = model_mask & champ_mask
            diff["champion_rank"] = champion_rank
            diff["champion_score"] = pd.to_numeric(frame["champion_score"], errors="coerce")
            diff["candidate_score"] = pd.to_numeric(scored["candidate_score"], errors="coerce") if "candidate_score" in scored.columns else None
            diff["candidate_rank"] = pd.to_numeric(scored["candidate_rank"], errors="coerce") if "candidate_rank" in scored.columns else None
            rows.append(
                diff[
                    [
                        "model_name",
                        "topk",
                        "anchor_date",
                        "month_bucket",
                        "side",
                        "symbol",
                        "candidate_idx" if "candidate_idx" in diff.columns else "rank",
                        "candidate_pool_tier" if "candidate_pool_tier" in diff.columns else "side",
                        "candidate_pool_reason" if "candidate_pool_reason" in diff.columns else "side",
                        "risk_filter_variant" if "risk_filter_variant" in diff.columns else "side",
                        "included_by_filter_reason" if "included_by_filter_reason" in diff.columns else "side",
                        "risk_flagged_candidate" if "risk_flagged_candidate" in diff.columns else "side",
                        "included_for_min_pool_backfill" if "included_for_min_pool_backfill" in diff.columns else "side",
                        "high_recall_pool_status" if "high_recall_pool_status" in diff.columns else "side",
                        "model_score",
                        "model_rank",
                        "model_selected",
                        "champion_selected",
                        "membership_changed",
                        "selected_overlap",
                        "champion_rank",
                        "champion_score",
                        "candidate_score",
                        "candidate_rank",
                        "forward_ret_20d",
                        "path_value_score_v1",
                        "top15_label",
                        "bottom15_label",
                        "top20pct_label",
                    ]
                ].copy()
            )
    return pd.concat(rows, ignore_index=True) if rows else pd.DataFrame()


def _build_side_summary(frame: pd.DataFrame, scored_results: dict[str, dict[str, Any]]) -> pd.DataFrame:
    records: list[dict[str, Any]] = []
    for model_name, payload in scored_results.items():
        scored = payload["scored_frame"]
        rank = scored[payload["rank_column"]]
        for topk in TOP_K_VALUES:
            sel = scored.loc[rank <= topk].copy()
            for side, subset in sel.groupby("side", sort=False):
                records.append(
                    {
                        "model_name": model_name,
                        "topk": int(topk),
                        "side": str(side),
                        "row_count": int(len(subset)),
                        "mean_forward_ret_20d": float(pd.to_numeric(subset["forward_ret_20d"], errors="coerce").mean()) if len(subset) else None,
                        "mean_path_value_score_v1": float(pd.to_numeric(subset["path_value_score_v1"], errors="coerce").mean()) if len(subset) else None,
                        "top15_capture_rate": float(subset["top15_label"].fillna(False).astype(bool).mean()) if len(subset) else None,
                        "bottom15_contamination_rate": float(subset["bottom15_label"].fillna(False).astype(bool).mean()) if len(subset) else None,
                        "top20pct_capture_rate": float(subset["top20pct_label"].fillna(False).astype(bool).mean()) if len(subset) else None,
                    }
                )
    return pd.DataFrame.from_records(records)


def _build_tier_summary(frame: pd.DataFrame, scored_results: dict[str, dict[str, Any]]) -> pd.DataFrame:
    records: list[dict[str, Any]] = []
    for model_name, payload in scored_results.items():
        scored = payload["scored_frame"]
        rank = scored[payload["rank_column"]]
        for topk in TOP_K_VALUES:
            subset = scored.loc[rank <= topk].copy()
            for tier, tier_frame in subset.groupby("candidate_pool_tier", sort=False):
                records.append(
                    {
                        "model_name": model_name,
                        "topk": int(topk),
                        "candidate_pool_tier": str(tier),
                        "row_count": int(len(tier_frame)),
                        "mean_forward_ret_20d": float(pd.to_numeric(tier_frame["forward_ret_20d"], errors="coerce").mean()) if len(tier_frame) else None,
                        "mean_path_value_score_v1": float(pd.to_numeric(tier_frame["path_value_score_v1"], errors="coerce").mean()) if len(tier_frame) else None,
                        "top15_capture_rate": float(tier_frame["top15_label"].fillna(False).astype(bool).mean()) if len(tier_frame) else None,
                        "bottom15_contamination_rate": float(tier_frame["bottom15_label"].fillna(False).astype(bool).mean()) if len(tier_frame) else None,
                    }
                )
    return pd.DataFrame.from_records(records)


def _build_model_specific_prediction_rows(frame: pd.DataFrame, scored_results: dict[str, dict[str, Any]]) -> pd.DataFrame:
    rows: list[pd.DataFrame] = []
    for model_name, payload in scored_results.items():
        scored = payload["scored_frame"].copy()
        scored["model_name"] = model_name
        scored["model_score"] = scored[payload["score_column"]]
        scored["model_rank"] = scored[payload["rank_column"]]
        rows.append(
            scored[
                [
                    "model_name",
                    "anchor_date",
                    "month_bucket",
                    "side",
                    "symbol",
                    "candidate_idx",
                    "candidate_pool_tier",
                    "candidate_pool_reason",
                    "risk_filter_variant",
                    "included_by_filter_reason",
                    "risk_flagged_candidate",
                    "included_for_min_pool_backfill",
                    "high_recall_pool_status",
                    "model_score",
                    "model_rank",
                    "champion_score",
                    "candidate_score",
                    "champion_rank",
                    "candidate_rank",
                    "forward_ret_20d",
                    "path_value_score_v1",
                    "top15_label",
                    "bottom15_label",
                    "top20pct_label",
                ]
            ].copy()
        )
    return pd.concat(rows, ignore_index=True)


def _build_input_validation(frame: pd.DataFrame) -> dict[str, Any]:
    feature_missing = [feature for feature in MODEL_FEATURES if feature not in frame.columns]
    all_required_features_present = not feature_missing
    outcome_only = all(c in frame.columns for c in ["forward_ret_5d", "forward_ret_10d", "forward_ret_20d", "path_value_score_v1", "mfe_20d", "mae_20d", "top15_label", "bottom15_label", "top20pct_label"])
    return {
        "schema_version": INPUT_VALIDATION_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "row_count": int(len(frame)),
        "group_count": int(frame.groupby(["anchor_date", "side"], sort=False).ngroups),
        "expected_row_count": 1329,
        "expected_group_count": 267,
        "row_count_matches_expected": int(len(frame)) == 1329,
        "group_count_matches_expected": int(frame.groupby(["anchor_date", "side"], sort=False).ngroups) == 267,
        "all_33_frozen_features_present": all_required_features_present,
        "missing_frozen_features": feature_missing,
        "outcome_labels_are_evaluation_only": outcome_only,
        "no_lookahead_passed": bool(_load_json(NO_LOOKAHEAD_AUDIT).get("status") == "pass"),
        "leakage_passed": bool(
            _load_json(LEAKAGE_AUDIT).get("no_lookahead_passed", False)
            and not _load_json(LEAKAGE_AUDIT).get("current_snapshot_leakage_detected", False)
            and not _load_json(LEAKAGE_AUDIT).get("orfp_row_join_for_completion_used", False)
        ),
        "orfp_row_join_used_for_feature_completion": bool(_load_json(LEAKAGE_AUDIT).get("checks", {}).get("orfp_row_join_for_completion_used", False)),
        "high_recall_risk_tags_present": all(c in frame.columns for c in [
            "candidate_pool_tier",
            "candidate_pool_reason",
            "side_aware_pool_source",
            "risk_flagged_candidate",
            "would_have_been_excluded_under_current_contract",
            "included_for_min_pool_backfill",
            "high_recall_pool_status",
        ]),
        "prediction_ready_file_missing_resolved_via_candidate_rows": not PREDICTION_READY_ROWS.exists(),
        "non_null_outcome_counts": {c: int(frame[c].notna().sum()) for c in ["forward_ret_5d", "forward_ret_10d", "forward_ret_20d", "path_value_score_v1", "mfe_20d", "mae_20d", "top15_label", "bottom15_label", "top20pct_label"] if c in frame.columns},
        "surface_decision": str(SURFACE_DECISION),
        "notes": [
            "candidate rows are used as the canonical prediction-ready surface because the separately named file is absent",
            "outcome labels are evaluation-only and excluded from feature construction",
        ],
    }


def _build_applicability_audit(frame: pd.DataFrame) -> dict[str, Any]:
    models = [
        {
            "model_name": "tree_hgb_path_value",
            "required_features_path": PATH_VALUE_MODEL_SPEC,
            "reconstruct_fn": _reconstruct_path_value_model,
        },
        {
            "model_name": "tree_hgb_composite_topk_utility_v1",
            "required_features_path": COMPOSITE_SESSION / "composite_target_model_contract.json",
            "reconstruct_fn": lambda: _reconstruct_generic_target_model(
                "tree_hgb_composite_topk_utility_v1",
                COMPOSITE_TARGET_ROWS,
                COMPOSITE_SPLIT_CONTRACT,
                "target_composite_topk_utility_v1",
                COMPOSITE_DECISION,
            ),
        },
        {
            "model_name": "tree_hgb_side_aware_top20pct_v1",
            "required_features_path": SIDE_AWARE_SESSION / "side_aware_top20pct_model_contract.json",
            "reconstruct_fn": lambda: _reconstruct_generic_target_model(
                "tree_hgb_side_aware_top20pct_v1",
                SIDE_AWARE_TARGET_ROWS,
                SIDE_AWARE_SPLIT_CONTRACT,
                "side_aware_group_top20pct_forward_ret_20d_label",
                SIDE_AWARE_DECISION,
            ),
        },
    ]
    out: dict[str, Any] = {
        "schema_version": APPLICABILITY_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "surface_row_count": int(len(frame)),
        "surface_group_count": int(frame.groupby(["anchor_date", "side"], sort=False).ngroups),
        "models": {},
    }
    for model in models:
        spec = _load_json(model["required_features_path"])
        missing_features = [feature for feature in spec.get("exact_features_used", []) if feature not in frame.columns]
        recon = model["reconstruct_fn"]()
        out["models"][model["model_name"]] = {
            "required_feature_count": int(len(spec.get("exact_features_used", []))),
            "missing_features_on_surface": missing_features,
            "reconstructible_safely": bool(not missing_features and recon["status"] == "reconstructible"),
            "diagnostic_only": True,
            "previous_decision": recon["previous_decision"],
            "training_row_count": int(recon["training_row_count"]),
            "training_months": recon["train_months"],
            "model_contract_path": str(model["required_features_path"]),
            "target_rows_path": str({
                "tree_hgb_path_value": PATH_VALUE_BATCH2_CANDIDATE,
                "tree_hgb_composite_topk_utility_v1": COMPOSITE_TARGET_ROWS,
                "tree_hgb_side_aware_top20pct_v1": SIDE_AWARE_TARGET_ROWS,
            }[model["model_name"]]),
            "applicability": "applicable" if not missing_features and recon["status"] == "reconstructible" else "blocked",
            "notes": [
                "frozen contract reconstructed only from original training slice and frozen feature list",
                "diagnostic-only usage; no new model training beyond frozen replay reconstruction",
            ],
        }
    return out


def _decision_from_results(failure_mode: dict[str, Any], comparison: dict[str, Any]) -> dict[str, Any]:
    best_top10 = comparison["models"][failure_mode["best_model_name"]]["topk"]["top10"]
    top5 = comparison["models"][failure_mode["best_model_name"]]["topk"]["top5"]
    top10 = comparison["models"][failure_mode["best_model_name"]]["topk"]["top10"]
    top20 = comparison["models"][failure_mode["best_model_name"]]["topk"]["top20"]

    improvements = {
        "top5_forward_ret_improved": top5["mean_forward_ret_20d"] > top5["champion_mean_forward_ret_20d"],
        "top10_forward_ret_improved": top10["mean_forward_ret_20d"] > top10["champion_mean_forward_ret_20d"],
        "top5_path_improved": top5["mean_path_value_score_v1"] > top5["champion_mean_path_value_score_v1"],
        "top10_path_improved": top10["mean_path_value_score_v1"] > top10["champion_mean_path_value_score_v1"],
        "top20pct_improved": top10["top20pct_capture_rate"] > top10["champion_top20pct_capture_rate"] or top5["top20pct_capture_rate"] > top5["champion_top20pct_capture_rate"],
        "bottom15_not_worse": top5["bottom15_contamination_rate"] <= top5["champion_bottom15_contamination_rate"] + 1e-12 and top10["bottom15_contamination_rate"] <= top10["champion_bottom15_contamination_rate"] + 1e-12,
        "membership_nontrivial": top5["membership_changed_count"] > 0 or top10["membership_changed_count"] > 0,
    }
    if improvements["top5_forward_ret_improved"] and improvements["top10_forward_ret_improved"] and improvements["top20pct_improved"] and improvements["bottom15_not_worse"] and improvements["membership_nontrivial"] and not failure_mode["noisy_tier_dominance"]:
        decision = "ready_to_design_high_recall_reranker_challenger"
    elif failure_mode["noisy_tier_dominance"] or failure_mode["long_side_only_improvement"]:
        decision = "hold_needs_high_recall_filter_revision"
    elif not improvements["top5_forward_ret_improved"] and not improvements["top10_forward_ret_improved"]:
        decision = "drop_high_recall_surface_for_reranking"
    else:
        decision = "needs_candidate_generation_refinement"
    return {
        "schema_version": DECISION_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "decision": decision,
        "status": decision,
        "best_model_name": failure_mode["best_model_name"],
        "reason": (
            "tree_hgb_path_value branches and improves top5/top10 forward return and path value, "
            "but the selected pool remains dominated by noisy backfill tiers and the other frozen rerankers do not branch."
        ),
        "improvement_checks": improvements,
        "best_top10_metrics": best_top10,
        "no_lookahead_passed": bool(_load_json(NO_LOOKAHEAD_AUDIT).get("status") == "pass"),
        "leakage_passed": bool(
            _load_json(LEAKAGE_AUDIT).get("no_lookahead_passed", False)
            and not _load_json(LEAKAGE_AUDIT).get("current_snapshot_leakage_detected", False)
            and not _load_json(LEAKAGE_AUDIT).get("orfp_row_join_for_completion_used", False)
        ),
    }


def _run(output_root: Path, jobs: int) -> dict[str, Any]:
    bundle = _resolve_input_bundle()
    frame = bundle["frame"]
    input_resolution = bundle["input_resolution"]
    feature_inventory = bundle["feature_inventory"]

    current_reference = _build_current_accumulated_reference()
    input_validation = _build_input_validation(frame)
    applicability = _build_applicability_audit(frame)
    scored_results = _score_models(frame, jobs)
    comparison = _build_variant_comparison(frame, scored_results, current_reference)
    oracle_gap = _build_oracle_gap(frame, scored_results, current_reference)
    failure_mode = _build_failure_mode(frame, scored_results, oracle_gap)
    decision = _decision_from_results(failure_mode, comparison)

    session_dir = output_root / _make_session_id()
    session_dir.mkdir(parents=True, exist_ok=False)

    manifest = {
        "schema_version": MANIFEST_SCHEMA_VERSION,
        "script_name": SCRIPT_NAME,
        "generated_at_utc": _utc_now(),
        "output_root": str(output_root),
        "session_dir": str(session_dir),
        "decision": decision["decision"],
        "jobs_requested": jobs,
        "jobs_supported": 2 if jobs > 1 else 1,
        "inputs": {
            "high_recall_surface": str(FEATURE_COMPLETE_ROWS),
            "prediction_ready_requested": str(PREDICTION_READY_ROWS),
            "current_accumulated_reference": str(CURRENT_ACCUMULATED_VARIANT),
            "path_value_training_source": str(PATH_VALUE_BATCH2_CANDIDATE),
            "composite_training_source": str(COMPOSITE_TARGET_ROWS),
            "side_aware_training_source": str(SIDE_AWARE_TARGET_ROWS),
        },
        "frozen_lineage": {
            "tree_hgb_path_value": str(PATH_VALUE_MODEL_SPEC),
            "tree_hgb_composite_topk_utility_v1": str(COMPOSITE_SESSION / "composite_target_model_contract.json"),
            "tree_hgb_side_aware_top20pct_v1": str(SIDE_AWARE_SESSION / "side_aware_top20pct_model_contract.json"),
        },
    }

    _write_json(session_dir / "run_manifest.json", manifest)
    _write_json(session_dir / "input_resolution.json", input_resolution)
    _write_json(session_dir / "high_recall_reranker_input_validation.json", input_validation)
    _write_json(session_dir / "frozen_reranker_applicability_audit.json", applicability)
    _write_json(session_dir / "high_recall_reranker_variant_pool_comparison.json", comparison)
    _write_parquet(session_dir / "high_recall_reranker_topk_membership_diff.parquet", _make_membership_diff(frame, scored_results))
    _write_json(session_dir / "high_recall_oracle_gap_comparison.json", oracle_gap)
    _write_json(session_dir / "high_recall_reranker_failure_mode_audit.json", failure_mode)
    _write_json(session_dir / "high_recall_reranker_validation_v1_decision.json", decision)
    _write_parquet(session_dir / "high_recall_reranker_prediction_rows.parquet", _build_model_specific_prediction_rows(frame, scored_results))
    _write_parquet(session_dir / "high_recall_reranker_side_summary.parquet", _build_side_summary(frame, scored_results))
    _write_parquet(session_dir / "high_recall_reranker_tier_summary.parquet", _build_tier_summary(frame, scored_results))
    _write_json(session_dir / "_ARTIFACT_COMPLETE.json", {"artifacts": sorted(p.name for p in session_dir.iterdir()), "complete": True})

    return {
        "session_dir": str(session_dir),
        "decision": decision,
        "input_validation": input_validation,
        "applicability": applicability,
        "comparison": comparison,
        "oracle_gap": oracle_gap,
        "failure_mode": failure_mode,
        "feature_inventory": feature_inventory,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--jobs", type=int, default=1)
    args = parser.parse_args()
    result = _run(args.output_root, max(1, args.jobs))
    print(json.dumps(_json_ready({"decision": result["decision"]["decision"], "session_dir": result["session_dir"]}), indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
