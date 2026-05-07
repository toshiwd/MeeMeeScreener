from __future__ import annotations

import argparse
import json
import math
import sys
import warnings
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.tradex_shadow_feature_reranker_feasibility_v1 import MODEL_FEATURES, _coerce_model_frame, _rank_within_groups, _tree_pipeline


warnings.filterwarnings("ignore", category=FutureWarning)
warnings.filterwarnings("ignore", message="Skipping features without any observed values*", category=UserWarning)

SCRIPT_NAME = "tradex_long_side_reranker_validation_v1"
MANIFEST_SCHEMA_VERSION = "tradex_long_side_reranker_validation_v1_manifest_v1"
INPUT_RESOLUTION_SCHEMA_VERSION = "tradex_long_side_reranker_validation_v1_input_resolution_v1"
INPUT_VALIDATION_SCHEMA_VERSION = "tradex_long_side_reranker_validation_v1_input_validation_v1"
VARIANT_COMPARISON_SCHEMA_VERSION = "tradex_long_side_reranker_validation_v1_variant_pool_comparison_v1"
ORACLE_GAP_SCHEMA_VERSION = "tradex_long_side_reranker_validation_v1_oracle_gap_comparison_v1"
FAILURE_SCHEMA_VERSION = "tradex_long_side_reranker_validation_v1_failure_mode_audit_v1"
DECISION_SCHEMA_VERSION = "tradex_long_side_reranker_validation_v1_decision_v1"
ARTIFACT_COMPLETE_SCHEMA_VERSION = "tradex_long_side_reranker_validation_v1_artifact_complete_v1"

DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\long_side_reranker_validation_v1")
SIDE_SPECIFIC_SESSION = Path(r"G:\Tradex\side_specific_high_recall_surface_v1\20260502T151044Z-324144")
PATH_VALUE_TRAIN_SESSION = Path(r"G:\Tradex\feature_surface_batch2_volume_participation_v1\20260501T101349Z-601273")
PATH_VALUE_MODEL_SPEC = Path(r"G:\Tradex\shadow_reranker_challenger_design_v1\20260501T120651Z-643568\shadow_challenger_model_spec.json")
PATH_VALUE_DECISION = Path(r"G:\Tradex\shadow_reranker_challenger_design_v1\20260501T120651Z-643568\shadow_reranker_challenger_design_v1_decision.json")
PATH_VALUE_LEAKAGE = Path(r"G:\Tradex\shadow_reranker_challenger_design_v1\20260501T120651Z-643568\shadow_challenger_leakage_audit.json")
PATH_VALUE_VARIANT = Path(r"G:\Tradex\shadow_reranker_challenger_design_v1\20260501T120651Z-643568\shadow_challenger_variant_pool_comparison.json")
CURRENT_ACCUMULATED_SESSION = Path(r"G:\Tradex\high_recall_reranker_validation_v1\20260502T143633Z-122839")

LONG_SURFACE = SIDE_SPECIFIC_SESSION / "long_side_active_surface.parquet"
LONG_SURFACE_SUMMARY = SIDE_SPECIFIC_SESSION / "long_side_active_surface_summary.json"
FEATURE_CHECK = SIDE_SPECIFIC_SESSION / "side_specific_feature_contract_check.json"
NO_LOOKAHEAD = SIDE_SPECIFIC_SESSION / "side_specific_no_lookahead_audit.json"
LEAKAGE = SIDE_SPECIFIC_SESSION / "side_specific_leakage_audit.json"
QUALITY_AUDIT = SIDE_SPECIFIC_SESSION / "side_specific_surface_quality_audit.json"
ORACLE_HEADROOM = SIDE_SPECIFIC_SESSION / "side_specific_oracle_headroom_audit.json"
SURFACE_DECISION = SIDE_SPECIFIC_SESSION / "side_specific_high_recall_surface_v1_decision.json"

TRAIN_ROWS = PATH_VALUE_TRAIN_SESSION / "candidate_prefilter_rows_batch2_volume_enriched_v1.parquet"
CURRENT_ACCUMULATED_VARIANT = CURRENT_ACCUMULATED_SESSION / "high_recall_reranker_variant_pool_comparison.json"

TOP_K_VALUES = (5, 10, 20)
MODEL_NAME = "tree_hgb_path_value"
MODEL_SCORE_COL = f"{MODEL_NAME}_score"
MODEL_RANK_COL = f"{MODEL_NAME}_rank"
CHAMPION_RANK_COL = "champion_rank_recomputed"


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


def _group_stats(frame: pd.DataFrame) -> dict[str, Any]:
    groups = frame.groupby(["anchor_date", "side"], sort=False).size()
    return {
        "row_count": int(len(frame)),
        "group_count": int(groups.shape[0]),
        "min_group_size": int(groups.min()) if len(groups) else None,
        "median_group_size": float(groups.median()) if len(groups) else None,
        "mean_group_size": float(groups.mean()) if len(groups) else None,
        "max_group_size": int(groups.max()) if len(groups) else None,
        "top5_thin_groups": int((groups < 5).sum()),
        "top10_thin_groups": int((groups < 10).sum()),
        "top20_thin_groups": int((groups < 20).sum()),
    }


def _model_replay_model() -> dict[str, Any]:
    spec = _load_json(PATH_VALUE_MODEL_SPEC)
    if spec.get("selected_variant") != MODEL_NAME:
        raise RuntimeError(f"unexpected selected_variant in frozen spec: {spec.get('selected_variant')}")
    train_rows = _load_frame(TRAIN_ROWS)
    train_rows["month_bucket"] = train_rows["month_bucket"].astype(str)
    train_months = spec["split_contract"]["splits"]["train"]
    train = train_rows[train_rows["month_bucket"].isin(train_months)].copy()
    model = _tree_pipeline("regression_path_value")
    model.fit(_coerce_model_frame(train), pd.to_numeric(train["path_value_score_v1"], errors="coerce").astype(float))
    return {
        "model_name": MODEL_NAME,
        "model": model,
        "training_row_count": int(len(train)),
        "train_months": train_months,
        "previous_decision": _load_json(PATH_VALUE_DECISION).get("decision"),
        "leakage_status": _load_json(PATH_VALUE_LEAKAGE).get("status"),
        "variant_reference": _load_json(PATH_VALUE_VARIANT),
    }


def _load_long_surface() -> pd.DataFrame:
    frame = _load_frame(LONG_SURFACE)
    frame["anchor_date"] = frame["anchor_date"].astype(str)
    frame["side"] = frame["side"].astype(str)
    if "month_bucket" in frame.columns:
        frame["month_bucket"] = frame["month_bucket"].astype(str)
    return frame


def _source_reference() -> dict[str, Any]:
    ref = _load_json(CURRENT_ACCUMULATED_VARIANT)
    return {
        "schema_version": "tradex_long_side_reranker_validation_v1_current_accumulated_reference_v1",
        "generated_at_utc": _utc_now(),
        "source_variant_comparison": str(CURRENT_ACCUMULATED_VARIANT),
        "surface_row_count": int(ref.get("surface_row_count", 0)),
        "group_count": int(ref.get("group_count", 0)),
        "summary": ref.get("current_accumulated_reference", {}).get("summary", {}),
        "topk": ref.get("current_accumulated_reference", {}).get("topk", {}),
    }


def _input_validation(frame: pd.DataFrame) -> dict[str, Any]:
    feature_missing = [col for col in MODEL_FEATURES if col not in frame.columns]
    outcome_cols = ["forward_ret_5d", "forward_ret_10d", "forward_ret_20d", "path_value_score_v1", "mfe_20d", "mae_20d", "top15_label", "bottom15_label", "top20pct_label"]
    evaluation_only = bool(frame.get("evaluation_only_outcomes", pd.Series(False, index=frame.index)).fillna(False).astype(bool).all()) if "evaluation_only_outcomes" in frame.columns else False
    champion_cols_present = all(col in frame.columns for col in ["champion_score", "champion_rank"])
    no_short_rows = int((frame["side"].astype(str) == "short").sum()) == 0
    return {
        "schema_version": INPUT_VALIDATION_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "row_count": int(len(frame)),
        "group_count": int(frame.groupby(["anchor_date", "side"], sort=False).ngroups),
        "requested_surface": str(LONG_SURFACE),
        "only_long_rows_used": bool(frame["side"].astype(str).eq("long").all()),
        "short_rows_in_active_validation": int((frame["side"].astype(str) == "short").sum()),
        "no_short_rows_in_active_validation": no_short_rows,
        "frozen_feature_count": int(len(MODEL_FEATURES)),
        "missing_frozen_features": feature_missing,
        "frozen_features_present": len(feature_missing) == 0,
        "no_lookahead_passed": bool(_load_json(NO_LOOKAHEAD).get("passed", False)),
        "leakage_passed": bool(_load_json(LEAKAGE).get("passed", False)),
        "outcome_labels_evaluation_only": evaluation_only,
        "outcome_columns_present": [c for c in outcome_cols if c in frame.columns],
        "champion_fields_preserved_separately": champion_cols_present,
        "champion_score_preserved": "champion_score" in frame.columns,
        "champion_rank_preserved": "champion_rank" in frame.columns,
        "surface_decision": _load_json(SURFACE_DECISION).get("decision"),
        "feature_contract_status": _load_json(FEATURE_CHECK).get("long", {}).get("feature_complete", False),
        "source_summary_row_count": _load_json(LONG_SURFACE_SUMMARY).get("row_count"),
        "source_summary_group_count": _load_json(LONG_SURFACE_SUMMARY).get("group_count"),
        "notes": [
            "Short-side rows are excluded from active validation and remain research-hold only.",
            "Outcome fields are retained only as evaluation labels.",
        ],
    }


def _score_surface(frame: pd.DataFrame, model: Any) -> pd.DataFrame:
    scored = frame.copy()
    scored[MODEL_SCORE_COL] = pd.Series(model.predict(_coerce_model_frame(scored)), index=scored.index, dtype="float64")
    scored[MODEL_RANK_COL] = _rank_within_groups(scored, scored[MODEL_SCORE_COL], group_cols=["anchor_date", "side"])
    scored[CHAMPION_RANK_COL] = _rank_within_groups(scored, pd.to_numeric(scored["champion_score"], errors="coerce"), group_cols=["anchor_date", "side"])
    for topk in TOP_K_VALUES:
        scored[f"{MODEL_NAME}_selected_top{topk}"] = scored[MODEL_RANK_COL] <= topk
        scored[f"champion_selected_top{topk}_recomputed"] = scored[CHAMPION_RANK_COL] <= topk
    return scored


def _selection_metrics(frame: pd.DataFrame, selected: pd.Series, champion_selected: pd.Series) -> dict[str, Any]:
    selected = selected.fillna(False).astype(bool)
    champion_selected = champion_selected.fillna(False).astype(bool)
    model_frame = frame.loc[selected].copy()
    champ_frame = frame.loc[champion_selected].copy()
    forward = pd.to_numeric(model_frame["forward_ret_20d"], errors="coerce")
    path = pd.to_numeric(model_frame["path_value_score_v1"], errors="coerce")
    champ_forward = pd.to_numeric(champ_frame["forward_ret_20d"], errors="coerce")
    champ_path = pd.to_numeric(champ_frame["path_value_score_v1"], errors="coerce")
    top15 = model_frame["top15_label"].fillna(False).astype(bool) if "top15_label" in model_frame.columns else pd.Series(False, index=model_frame.index)
    top20pct = model_frame["top20pct_label"].fillna(False).astype(bool) if "top20pct_label" in model_frame.columns else pd.Series(False, index=model_frame.index)
    bottom15 = model_frame["bottom15_label"].fillna(False).astype(bool) if "bottom15_label" in model_frame.columns else pd.Series(False, index=model_frame.index)
    overlap = int((selected & champion_selected).sum())
    union = int((selected | champion_selected).sum())
    changed = int((selected ^ champion_selected).sum())
    return {
        "selected_row_count": int(len(model_frame)),
        "champion_selected_row_count": int(len(champ_frame)),
        "mean_forward_ret_20d": float(forward.mean()) if len(model_frame) else None,
        "champion_mean_forward_ret_20d": float(champ_forward.mean()) if len(champ_frame) else None,
        "mean_path_value_score_v1": float(path.mean()) if len(model_frame) else None,
        "champion_mean_path_value_score_v1": float(champ_path.mean()) if len(champ_frame) else None,
        "top15_capture_rate": float(top15.mean()) if len(top15) else None,
        "champion_top15_capture_rate": float(champ_frame["top15_label"].fillna(False).astype(bool).mean()) if len(champ_frame) else None,
        "top20pct_capture_rate": float(top20pct.mean()) if len(top20pct) else None,
        "champion_top20pct_capture_rate": float(champ_frame["top20pct_label"].fillna(False).astype(bool).mean()) if len(champ_frame) else None,
        "bottom15_contamination_rate": float(bottom15.mean()) if len(bottom15) else None,
        "champion_bottom15_contamination_rate": float(champ_frame["bottom15_label"].fillna(False).astype(bool).mean()) if len(champ_frame) else None,
        "non_positive_forward_ret_count": int((forward.notna() & (forward <= 0)).sum()),
        "champion_non_positive_forward_ret_count": int((champ_forward.notna() & (champ_forward <= 0)).sum()),
        "membership_changed_count": int(changed),
        "overlap_ratio": float(overlap / union) if union else None,
        "zero_pass_groups": int(sum(1 for _, g in model_frame.groupby(["anchor_date", "side"], sort=False) if not g["top15_label"].fillna(False).astype(bool).any())),
        "symbol_concentration_top1": float(model_frame["symbol"].value_counts(normalize=True).iloc[0]) if len(model_frame) else None,
        "symbol_concentration_top3": float(model_frame["symbol"].value_counts(normalize=True).head(3).sum()) if len(model_frame) else None,
        "tier_composition": {str(k): int(v) for k, v in model_frame["candidate_pool_tier"].fillna("").value_counts().items()},
        "risk_flagged_backfill_count": int(model_frame["risk_flagged_backfill"].sum()) if "risk_flagged_backfill" in model_frame.columns else int((model_frame["candidate_pool_tier"].astype(str) == "risk_flagged_backfill").sum()),
        "risk_flagged_backfill_share": float((model_frame["candidate_pool_tier"].astype(str) == "risk_flagged_backfill").mean()) if len(model_frame) else None,
        "side_split": {str(k): int(v) for k, v in model_frame["side"].value_counts().items()},
    }


def _group_summary(frame: pd.DataFrame, selected_col: str, topk: int) -> pd.DataFrame:
    selected = frame[frame[selected_col].fillna(False).astype(bool)].copy()
    rows: list[dict[str, Any]] = []
    for (anchor_date, side), group in selected.groupby(["anchor_date", "side"], sort=False):
        rows.append(
            {
                "anchor_date": anchor_date,
                "side": side,
                "topk": int(topk),
                "row_count": int(len(group)),
                "mean_forward_ret_20d": float(pd.to_numeric(group["forward_ret_20d"], errors="coerce").mean()) if len(group) else None,
                "mean_path_value_score_v1": float(pd.to_numeric(group["path_value_score_v1"], errors="coerce").mean()) if len(group) else None,
                "top15_capture_rate": float(group["top15_label"].fillna(False).astype(bool).mean()) if len(group) else None,
                "top20pct_capture_rate": float(group["top20pct_label"].fillna(False).astype(bool).mean()) if len(group) else None,
                "bottom15_contamination_rate": float(group["bottom15_label"].fillna(False).astype(bool).mean()) if len(group) else None,
            }
        )
    return pd.DataFrame(rows)


def _build_variant_comparison(frame: pd.DataFrame, scored: pd.DataFrame, current_reference: dict[str, Any]) -> dict[str, Any]:
    champ_rank = scored[CHAMPION_RANK_COL]
    model_rank = scored[MODEL_RANK_COL]
    out: dict[str, Any] = {
        "schema_version": VARIANT_COMPARISON_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "surface_row_count": int(len(scored)),
        "group_count": int(scored.groupby(["anchor_date", "side"], sort=False).ngroups),
        "current_accumulated_reference": current_reference,
        "models": {
            MODEL_NAME: {
                "score_column": MODEL_SCORE_COL,
                "rank_column": MODEL_RANK_COL,
                "branching_happened": bool((model_rank != champ_rank).any()),
                "topk": {},
            }
        },
    }
    model_entry = out["models"][MODEL_NAME]
    for topk in TOP_K_VALUES:
        model_sel = model_rank <= topk
        champ_sel = champ_rank <= topk
        metrics = _selection_metrics(scored, model_sel, champ_sel)
        metrics["champion_reference"] = {
            "selected_row_count": int(champ_sel.sum()),
            "mean_forward_ret_20d": float(pd.to_numeric(scored.loc[champ_sel, "forward_ret_20d"], errors="coerce").mean()) if int(champ_sel.sum()) else None,
            "mean_path_value_score_v1": float(pd.to_numeric(scored.loc[champ_sel, "path_value_score_v1"], errors="coerce").mean()) if int(champ_sel.sum()) else None,
            "top15_capture_rate": float(scored.loc[champ_sel, "top15_label"].fillna(False).astype(bool).mean()) if int(champ_sel.sum()) else None,
            "top20pct_capture_rate": float(scored.loc[champ_sel, "top20pct_label"].fillna(False).astype(bool).mean()) if int(champ_sel.sum()) else None,
            "bottom15_contamination_rate": float(scored.loc[champ_sel, "bottom15_label"].fillna(False).astype(bool).mean()) if int(champ_sel.sum()) else None,
        }
        metrics["selected_vs_champion_forward_delta"] = None if metrics["mean_forward_ret_20d"] is None or metrics["champion_reference"]["mean_forward_ret_20d"] is None else metrics["mean_forward_ret_20d"] - metrics["champion_reference"]["mean_forward_ret_20d"]
        metrics["selected_vs_champion_path_delta"] = None if metrics["mean_path_value_score_v1"] is None or metrics["champion_reference"]["mean_path_value_score_v1"] is None else metrics["mean_path_value_score_v1"] - metrics["champion_reference"]["mean_path_value_score_v1"]
        model_entry["topk"][f"top{topk}"] = metrics
    return out


def _build_prediction_rows(scored: pd.DataFrame) -> pd.DataFrame:
    cols = list(dict.fromkeys(scored.columns))
    return scored[cols].copy()


def _build_membership_diff(scored: pd.DataFrame) -> pd.DataFrame:
    rows: list[pd.DataFrame] = []
    champ_rank = scored[CHAMPION_RANK_COL]
    for topk in TOP_K_VALUES:
        model_sel = scored[MODEL_RANK_COL] <= topk
        champ_sel = champ_rank <= topk
        diff = scored.copy()
        diff["topk"] = int(topk)
        diff["model_name"] = MODEL_NAME
        diff["model_score"] = pd.to_numeric(scored[MODEL_SCORE_COL], errors="coerce")
        diff["model_rank"] = scored[MODEL_RANK_COL]
        diff["model_selected"] = model_sel
        diff["champion_selected"] = champ_sel
        diff["membership_changed"] = model_sel ^ champ_sel
        diff["selected_overlap"] = model_sel & champ_sel
        diff["champion_score_recomputed"] = pd.to_numeric(scored["champion_score"], errors="coerce")
        diff["champion_rank_recomputed"] = champ_rank
        rows.append(
            diff[
                [
                    "model_name",
                    "topk",
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
                    "model_selected",
                    "champion_selected",
                    "membership_changed",
                    "selected_overlap",
                    "champion_rank_recomputed",
                    "champion_score_recomputed",
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
    return pd.concat(rows, ignore_index=True)


def _build_oracle_gap(scored: pd.DataFrame, current_reference: dict[str, Any]) -> dict[str, Any]:
    champ_rank = scored[CHAMPION_RANK_COL]
    model_rank = scored[MODEL_RANK_COL]

    def _oracle_sel(topk: int) -> pd.Series:
        oracle = scored.loc[pd.to_numeric(scored["forward_ret_20d"], errors="coerce").notna()].copy()
        if oracle.empty:
            return pd.Series(False, index=scored.index)
        oracle_rank = _rank_within_groups(oracle, pd.to_numeric(oracle["forward_ret_20d"], errors="coerce"), group_cols=["anchor_date", "side"])
        selected = pd.Series(False, index=scored.index)
        selected.loc[oracle.index] = oracle_rank <= topk
        return selected

    oracle: dict[str, Any] = {}
    champion: dict[str, Any] = {}
    best_model: dict[str, Any] = {}
    for topk in TOP_K_VALUES:
        oracle_sel = _oracle_sel(topk)
        champ_sel = champ_rank <= topk
        model_sel = model_rank <= topk
        oracle_frame = scored.loc[oracle_sel].copy()
        champ_frame = scored.loc[champ_sel].copy()
        model_frame = scored.loc[model_sel].copy()
        oracle[f"top{topk}"] = {
            "selected_row_count": int(len(oracle_frame)),
            "mean_forward_ret_20d": float(pd.to_numeric(oracle_frame["forward_ret_20d"], errors="coerce").mean()) if len(oracle_frame) else None,
            "mean_path_value_score_v1": float(pd.to_numeric(oracle_frame["path_value_score_v1"], errors="coerce").mean()) if len(oracle_frame) else None,
            "top15_capture_rate": float(oracle_frame["top15_label"].fillna(False).astype(bool).mean()) if len(oracle_frame) else None,
            "top20pct_capture_rate": float(oracle_frame["top20pct_label"].fillna(False).astype(bool).mean()) if len(oracle_frame) else None,
            "bottom15_contamination_rate": float(oracle_frame["bottom15_label"].fillna(False).astype(bool).mean()) if len(oracle_frame) else None,
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
            "model_name": MODEL_NAME,
            "selected_row_count": int(len(model_frame)),
            "mean_forward_ret_20d": float(pd.to_numeric(model_frame["forward_ret_20d"], errors="coerce").mean()) if len(model_frame) else None,
            "mean_path_value_score_v1": float(pd.to_numeric(model_frame["path_value_score_v1"], errors="coerce").mean()) if len(model_frame) else None,
            "top15_capture_rate": float(model_frame["top15_label"].fillna(False).astype(bool).mean()) if len(model_frame) else None,
            "top20pct_capture_rate": float(model_frame["top20pct_label"].fillna(False).astype(bool).mean()) if len(model_frame) else None,
            "bottom15_contamination_rate": float(model_frame["bottom15_label"].fillna(False).astype(bool).mean()) if len(model_frame) else None,
            "gap_to_oracle_forward_ret_20d": None if len(model_frame) == 0 or len(oracle_frame) == 0 else float(pd.to_numeric(oracle_frame["forward_ret_20d"], errors="coerce").mean() - pd.to_numeric(model_frame["forward_ret_20d"], errors="coerce").mean()),
            "gap_to_oracle_path_value_score_v1": None if len(model_frame) == 0 or len(oracle_frame) == 0 else float(pd.to_numeric(oracle_frame["path_value_score_v1"], errors="coerce").mean() - pd.to_numeric(model_frame["path_value_score_v1"], errors="coerce").mean()),
        }
    return {
        "schema_version": ORACLE_GAP_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "best_model_name": MODEL_NAME,
        "current_accumulated_reference": current_reference,
        "oracle": oracle,
        "champion": champion,
        "best_model": best_model,
        "notes": [
            "Oracle is defined over rows with mature forward_ret_20d coverage only.",
            "Gaps are indicative and compare the frozen model replay against champion ordering within the same long-only surface.",
        ],
    }


def _build_failure_mode(scored: pd.DataFrame, comparison: dict[str, Any], oracle_gap: dict[str, Any]) -> dict[str, Any]:
    out: dict[str, Any] = {
        "schema_version": FAILURE_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "best_model_name": MODEL_NAME,
        "branching_summary": {},
        "result_type": None,
        "long_side_only_improvement": True,
        "top15_capture_improved": False,
        "top20pct_capture_improved": False,
        "bottom15_controlled": False,
        "noisy_backfill_dominance": False,
        "notes": [],
    }
    model_entry = comparison["models"][MODEL_NAME]["topk"]
    for topk in TOP_K_VALUES:
        block = model_entry[f"top{topk}"]
        out["branching_summary"][f"top{topk}"] = {
            "selected_row_count": int(block["selected_row_count"]),
            "changed_topk_member_count": int(block["membership_changed_count"]),
            "overlap_ratio": block["overlap_ratio"],
            "selected_side_split": block["side_split"],
            "selected_tier_composition": block["tier_composition"],
            "mean_forward_ret_20d_delta_vs_champion": None if block["selected_vs_champion_forward_delta"] is None else float(block["selected_vs_champion_forward_delta"]),
            "mean_path_value_score_v1_delta_vs_champion": None if block["selected_vs_champion_path_delta"] is None else float(block["selected_vs_champion_path_delta"]),
            "top15_capture_delta_vs_champion": None if block["top15_capture_rate"] is None or block["champion_top15_capture_rate"] is None else float(block["top15_capture_rate"] - block["champion_top15_capture_rate"]),
            "top20pct_capture_delta_vs_champion": None if block["top20pct_capture_rate"] is None or block["champion_top20pct_capture_rate"] is None else float(block["top20pct_capture_rate"] - block["champion_top20pct_capture_rate"]),
            "bottom15_delta_vs_champion": None if block["bottom15_contamination_rate"] is None or block["champion_bottom15_contamination_rate"] is None else float(block["bottom15_contamination_rate"] - block["champion_bottom15_contamination_rate"]),
        }
    top5 = out["branching_summary"]["top5"]
    top10 = out["branching_summary"]["top10"]
    top20 = out["branching_summary"]["top20"]
    out["top15_capture_improved"] = bool(
        (top5["top15_capture_delta_vs_champion"] or 0) > 0
        or (top10["top15_capture_delta_vs_champion"] or 0) > 0
        or (top20["top15_capture_delta_vs_champion"] or 0) > 0
    )
    out["top20pct_capture_improved"] = bool(
        (top5["top20pct_capture_delta_vs_champion"] or 0) > 0
        or (top10["top20pct_capture_delta_vs_champion"] or 0) > 0
        or (top20["top20pct_capture_delta_vs_champion"] or 0) > 0
    )
    out["bottom15_controlled"] = bool(
        (top5["bottom15_delta_vs_champion"] or 0) <= 0
        and (top10["bottom15_delta_vs_champion"] or 0) <= 0
        and (top20["bottom15_delta_vs_champion"] or 0) <= 0
    )
    out["noisy_backfill_dominance"] = bool(
        max(
            top5["selected_tier_composition"].get("risk_flagged_backfill", 0),
            top10["selected_tier_composition"].get("risk_flagged_backfill", 0),
            top20["selected_tier_composition"].get("risk_flagged_backfill", 0),
        )
        / max(
            top5["selected_row_count"],
            top10["selected_row_count"],
            top20["selected_row_count"],
            1,
        )
        > 0.80
    )
    if (top5["mean_forward_ret_20d_delta_vs_champion"] or 0) > 0 or (top10["mean_forward_ret_20d_delta_vs_champion"] or 0) > 0:
        out["result_type"] = "partial_improvement"
    else:
        out["result_type"] = "insufficient_signal"
    if out["top15_capture_improved"] and out["top20pct_capture_improved"] and out["bottom15_controlled"] and not out["noisy_backfill_dominance"]:
        out["recommendation_hint"] = "ready_to_design_long_side_reranker_challenger"
    elif out["noisy_backfill_dominance"] or not out["top15_capture_improved"]:
        out["recommendation_hint"] = "hold_needs_long_side_filter_revision"
    else:
        out["recommendation_hint"] = "needs_candidate_generation_refinement"
    out["notes"] = [
        "tree_hgb_path_value is the only frozen reranker replayed on the cleaned long surface.",
        "Short-side rows are excluded from active validation and remain research-hold only.",
    ]
    return out


def _build_decision(comparison: dict[str, Any], failure_mode: dict[str, Any], input_validation: dict[str, Any]) -> dict[str, Any]:
    model_block = comparison["models"][MODEL_NAME]["topk"]
    top5 = model_block["top5"]
    top10 = model_block["top10"]
    top20 = model_block["top20"]
    top5_return_gain = top5["mean_forward_ret_20d"] is not None and top5["champion_reference"]["mean_forward_ret_20d"] is not None and top5["mean_forward_ret_20d"] > top5["champion_reference"]["mean_forward_ret_20d"]
    top10_return_gain = top10["mean_forward_ret_20d"] is not None and top10["champion_reference"]["mean_forward_ret_20d"] is not None and top10["mean_forward_ret_20d"] > top10["champion_reference"]["mean_forward_ret_20d"]
    top15_gain = any(
        (block["top15_capture_rate"] is not None and block["champion_reference"]["top15_capture_rate"] is not None and block["top15_capture_rate"] > block["champion_reference"]["top15_capture_rate"])
        for block in (top5, top10, top20)
    )
    top20pct_gain = any(
        (block["top20pct_capture_rate"] is not None and block["champion_reference"]["top20pct_capture_rate"] is not None and block["top20pct_capture_rate"] > block["champion_reference"]["top20pct_capture_rate"])
        for block in (top5, top10, top20)
    )
    bottom15_ok = all(
        (block["bottom15_contamination_rate"] is None or block["champion_reference"]["bottom15_contamination_rate"] is None or block["bottom15_contamination_rate"] <= block["champion_reference"]["bottom15_contamination_rate"] + 0.002)
        for block in (top5, top10, top20)
    )
    changed_nontrivial = any(int(block["membership_changed_count"]) > 0 for block in (top5, top10, top20))
    decision = "hold_needs_long_side_filter_revision"
    if (
        top5_return_gain
        and top10_return_gain
        and (top15_gain or top20pct_gain)
        and bottom15_ok
        and changed_nontrivial
        and not failure_mode["noisy_backfill_dominance"]
        and input_validation["no_lookahead_passed"]
        and input_validation["leakage_passed"]
    ):
        decision = "ready_to_design_long_side_reranker_challenger"
    elif not (top5_return_gain or top10_return_gain or top15_gain or top20pct_gain):
        if failure_mode["result_type"] == "insufficient_signal":
            decision = "drop_long_side_high_recall_surface"
        else:
            decision = "needs_candidate_generation_refinement"
    elif failure_mode["noisy_backfill_dominance"] and not (top15_gain or top20pct_gain):
        decision = "hold_needs_long_side_filter_revision"
    return {
        "schema_version": DECISION_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "decision": decision,
        "status": decision,
        "reason": (
            "Long-side frozen reranker replay is evaluated on the cleaned side-specific surface only; "
            "decision is gated by top5/top10 forward-return improvement, top15/top20pct capture, bottom15 control, and branching."
        ),
        "supporting_checks": {
            "row_count": input_validation["row_count"],
            "group_count": input_validation["group_count"],
            "frozen_features_present": input_validation["frozen_features_present"],
            "no_lookahead_passed": input_validation["no_lookahead_passed"],
            "leakage_passed": input_validation["leakage_passed"],
            "no_short_rows_in_active_validation": input_validation["no_short_rows_in_active_validation"],
            "long_active_validation_allowed": True,
            "short_active_validation_allowed": False,
            "tree_hgb_path_value_reconstructible": True,
        },
    }


def _build_manifest(output_root: Path) -> dict[str, Any]:
    return {
        "schema_version": MANIFEST_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "script_name": SCRIPT_NAME,
        "session_id": output_root.name,
        "output_root": str(output_root),
        "jobs_requested": 2,
        "jobs_supported": 2,
        "source_artifacts": {
            "long_surface": str(LONG_SURFACE),
            "long_surface_summary": str(LONG_SURFACE_SUMMARY),
            "feature_contract_check": str(FEATURE_CHECK),
            "no_lookahead_audit": str(NO_LOOKAHEAD),
            "leakage_audit": str(LEAKAGE),
            "quality_audit": str(QUALITY_AUDIT),
            "oracle_headroom_audit": str(ORACLE_HEADROOM),
            "surface_decision": str(SURFACE_DECISION),
            "model_spec": str(PATH_VALUE_MODEL_SPEC),
            "model_decision": str(PATH_VALUE_DECISION),
            "model_leakage_audit": str(PATH_VALUE_LEAKAGE),
            "model_variant_comparison": str(PATH_VALUE_VARIANT),
            "current_accumulated_variant": str(CURRENT_ACCUMULATED_VARIANT),
            "training_rows": str(TRAIN_ROWS),
        },
    }


def _build_input_resolution(output_root: Path, model_spec: dict[str, Any]) -> dict[str, Any]:
    return {
        "schema_version": INPUT_RESOLUTION_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "output_root": str(output_root),
        "resolved_surface_root": str(SIDE_SPECIFIC_SESSION),
        "resolved_long_surface": str(LONG_SURFACE),
        "resolved_long_surface_summary": str(LONG_SURFACE_SUMMARY),
        "resolved_model_spec": str(PATH_VALUE_MODEL_SPEC),
        "resolved_model_decision": str(PATH_VALUE_DECISION),
        "resolved_model_leakage_audit": str(PATH_VALUE_LEAKAGE),
        "resolved_model_variant_comparison": str(PATH_VALUE_VARIANT),
        "resolved_current_accumulated_reference": str(CURRENT_ACCUMULATED_VARIANT),
        "jobs_requested": 2,
        "jobs_supported": 2,
        "frozen_model_name": model_spec.get("selected_variant"),
        "surface_scope": "long_active_only",
        "short_side_excluded_from_active_validation": True,
    }


def _run(output_root: Path, jobs: int) -> dict[str, Any]:
    long_surface = _load_long_surface()
    model_spec = _load_json(PATH_VALUE_MODEL_SPEC)
    input_validation = _input_validation(long_surface)
    model_bundle = _model_replay_model()
    scored = _score_surface(long_surface, model_bundle["model"])

    comparison = _build_variant_comparison(long_surface, scored, _source_reference())
    oracle_gap = _build_oracle_gap(scored, _source_reference())
    failure_mode = _build_failure_mode(scored, comparison, oracle_gap)
    decision = _build_decision(comparison, failure_mode, input_validation)

    prediction_rows = _build_prediction_rows(scored)
    membership_diff = _build_membership_diff(scored)
    tier_summary = (
        pd.concat(
            [
                scored.loc[scored[MODEL_RANK_COL] <= topk]
                .groupby("candidate_pool_tier", sort=False)
                .agg(
                    row_count=("candidate_pool_tier", "size"),
                    mean_forward_ret_20d=("forward_ret_20d", lambda s: float(pd.to_numeric(s, errors="coerce").mean())),
                    mean_path_value_score_v1=("path_value_score_v1", lambda s: float(pd.to_numeric(s, errors="coerce").mean())),
                    top15_capture_rate=("top15_label", lambda s: float(s.fillna(False).astype(bool).mean())),
                    bottom15_contamination_rate=("bottom15_label", lambda s: float(s.fillna(False).astype(bool).mean())),
                )
                .assign(topk=int(topk))
                .reset_index()
                for topk in TOP_K_VALUES
            ],
            ignore_index=True,
        )
        if len(scored)
        else pd.DataFrame(columns=["candidate_pool_tier", "row_count", "mean_forward_ret_20d", "mean_path_value_score_v1", "top15_capture_rate", "bottom15_contamination_rate", "topk"])
    )
    symbol_concentration = {
        "model_name": MODEL_NAME,
        "topk": {
            f"top{topk}": {
                "top1_symbol_share": float(scored.loc[scored[MODEL_RANK_COL] <= topk, "symbol"].value_counts(normalize=True).iloc[0]) if int((scored[MODEL_RANK_COL] <= topk).sum()) else None,
                "top3_symbol_share": float(scored.loc[scored[MODEL_RANK_COL] <= topk, "symbol"].value_counts(normalize=True).head(3).sum()) if int((scored[MODEL_RANK_COL] <= topk).sum()) else None,
            }
            for topk in TOP_K_VALUES
        },
    }

    manifest = _build_manifest(output_root)
    input_resolution = _build_input_resolution(output_root, model_spec)

    output_root.mkdir(parents=True, exist_ok=True)
    _write_json(output_root / "run_manifest.json", manifest)
    _write_json(output_root / "input_resolution.json", input_resolution)
    _write_json(output_root / "long_side_reranker_input_validation.json", input_validation)
    _write_parquet(output_root / "long_side_reranker_prediction_rows.parquet", prediction_rows)
    _write_json(output_root / "long_side_reranker_variant_pool_comparison.json", comparison)
    _write_parquet(output_root / "long_side_reranker_topk_membership_diff.parquet", membership_diff)
    _write_json(output_root / "long_side_oracle_gap_comparison.json", oracle_gap)
    _write_json(output_root / "long_side_reranker_failure_mode_audit.json", failure_mode)
    _write_json(output_root / "long_side_reranker_validation_v1_decision.json", decision)
    _write_parquet(output_root / "long_side_reranker_tier_summary.parquet", tier_summary)
    _write_json(output_root / "long_side_reranker_symbol_concentration.json", symbol_concentration)
    _write_parquet(output_root / "long_side_reranker_group_summary.parquet", _group_summary(scored, MODEL_RANK_COL, 5).assign(model_name=MODEL_NAME))
    _write_json(
        output_root / "_ARTIFACT_COMPLETE.json",
        {
            "schema_version": ARTIFACT_COMPLETE_SCHEMA_VERSION,
            "generated_at_utc": _utc_now(),
            "complete": True,
            "required_artifacts": [
                "run_manifest.json",
                "input_resolution.json",
                "long_side_reranker_input_validation.json",
                "long_side_reranker_prediction_rows.parquet",
                "long_side_reranker_variant_pool_comparison.json",
                "long_side_reranker_topk_membership_diff.parquet",
                "long_side_oracle_gap_comparison.json",
                "long_side_reranker_failure_mode_audit.json",
                "long_side_reranker_validation_v1_decision.json",
            ],
        },
    )
    return {
        "output_root": str(output_root),
        "decision": decision["decision"],
        "row_count": int(len(scored)),
        "group_count": int(scored.groupby(["anchor_date", "side"], sort=False).ngroups),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="TRADEX long-side reranker validation v1")
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--jobs", type=int, default=2)
    args = parser.parse_args()
    session_dir = args.output_root / _session_id()
    result = _run(session_dir, max(1, args.jobs))
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
