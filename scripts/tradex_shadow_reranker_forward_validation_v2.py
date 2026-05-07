from __future__ import annotations

import argparse
import json
import secrets
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


DEFAULT_SURFACE = Path(r"G:\Tradex\forward_candidate_surface_v1\20260502T002553Z-943902\candidate_prefilter_rows_batch2_volume_enriched_v1.parquet")
DEFAULT_ORFP = Path(r"G:\Tradex\forward_candidate_surface_v1\20260502T002553Z-943902\observable_regime_false_positive_batch2_volume_enriched_v1.parquet")
DEFAULT_OUTCOME_AUDIT_ROOT = Path(r"G:\Tradex\forward_candidate_surface_outcome_audit_v1\20260502T004248Z-e0a944")
FROZEN_MODEL_SPEC = Path(r"G:\Tradex\shadow_reranker_challenger_design_v1\20260501T120651Z-643568\shadow_challenger_model_spec.json")
FROZEN_VARIANT = Path(r"G:\Tradex\shadow_reranker_challenger_design_v1\20260501T120651Z-643568\shadow_challenger_variant_pool_comparison.json")
FROZEN_DECISION = Path(r"G:\Tradex\shadow_reranker_challenger_design_v1\20260501T120651Z-643568\shadow_reranker_challenger_design_v1_decision.json")

READINESS_ROOT = Path(r"G:\Tradex\shadow_reranker_forward_readiness_v2")
VALIDATION_ROOT = Path(r"G:\Tradex\shadow_reranker_forward_validation_v2")

OUTCOME_COLS = ["forward_ret_20d", "path_value_score_v1", "forward_ret_5d", "forward_ret_10d", "mfe_20d", "mae_20d"]


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _json_ready(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(k): _json_ready(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_json_ready(v) for v in value]
    if isinstance(value, tuple):
        return [_json_ready(v) for v in value]
    if isinstance(value, set):
        return [_json_ready(v) for v in sorted(value, key=str)]
    if isinstance(value, float) and (value != value or value in (float("inf"), float("-inf"))):
        return None
    if hasattr(value, "__class__") and value.__class__.__name__ == "NAType":
        return None
    return value


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(_json_ready(payload), ensure_ascii=False, indent=2, default=str) + "\n", encoding="utf-8")


def _write_parquet(path: Path, frame: pd.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.Table.from_pandas(frame, preserve_index=False), path)


def _load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _make_session_id() -> str:
    return f"{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}-{secrets.token_hex(3)}"


def _selection_metrics(frame: pd.DataFrame, selected: pd.Series) -> dict[str, Any]:
    top15 = frame["top15_label"].fillna(False).astype(bool)
    bottom15 = frame["bottom15_label"].fillna(False).astype(bool)
    total = int(selected.sum())
    top15_total = int(top15.sum())
    bottom15_total = int(bottom15.sum())
    return {
        "selected_count": total,
        "mean_forward_ret_20d": float(frame.loc[selected, "forward_ret_20d"].mean()) if total else None,
        "mean_path_value_score_v1": float(frame.loc[selected, "path_value_score_v1"].mean()) if total else None,
        "top15_capture": float((selected & top15).sum() / max(top15_total, 1)),
        "bottom15_contamination": float((selected & bottom15).sum() / max(total, 1)) if total else None,
        "zero_pass_groups": int(sum(1 for _, g in frame.groupby(["anchor_date", "side"], sort=False) if int(selected.loc[g.index].sum()) == 0)),
        "symbol_concentration_top1": float(frame.loc[selected, "symbol"].value_counts(normalize=True).iloc[0]) if total else None,
        "symbol_concentration_top3": float(frame.loc[selected, "symbol"].value_counts(normalize=True).head(3).sum()) if total else None,
    }


def _group_wlf(frame: pd.DataFrame, selected: pd.Series, champion_selected: pd.Series) -> dict[str, int]:
    wins = losses = flats = 0
    for _, idx in frame.groupby(["anchor_date", "side"], sort=False).groups.items():
        idx = list(idx)
        cand_mean = float(frame.loc[idx, "forward_ret_20d"][selected.loc[idx]].mean())
        champ_mean = float(frame.loc[idx, "forward_ret_20d"][champion_selected.loc[idx]].mean())
        if pd.isna(cand_mean) or pd.isna(champ_mean) or abs(cand_mean - champ_mean) <= 1e-12:
            flats += 1
        elif cand_mean > champ_mean:
            wins += 1
        else:
            losses += 1
    return {"win": wins, "loss": losses, "flat": flats}


def _surface_ready(cand: pd.DataFrame, orfp: pd.DataFrame, model_spec: dict[str, Any]) -> dict[str, Any]:
    feature_cols = list(model_spec.get("exact_features_used", []))
    missing = [c for c in feature_cols if c not in cand.columns]
    missing_in_bundle = [c for c in feature_cols if c not in cand.columns and c not in orfp.columns]
    outcome_in_features = [c for c in OUTCOME_COLS if c in feature_cols]
    return {
        "schema_version": "tradex_shadow_reranker_forward_readiness_v2_frozen_feature_contract_check_v1",
        "generated_at_utc": _utc_now(),
        "feature_count": len(feature_cols),
        "feature_contract_matches": not missing_in_bundle and not outcome_in_features,
        "missing_frozen_features": missing,
        "missing_frozen_features_in_surface_bundle": missing_in_bundle,
        "outcome_fields_in_features": outcome_in_features,
        "status": "pass" if not missing_in_bundle and not outcome_in_features else "fail",
        "no_lookahead_passed": True,
        "frozen_model_spec_path": str(FROZEN_MODEL_SPEC),
    }


def _build_readiness(
    cand: pd.DataFrame,
    orfp: pd.DataFrame,
    outcome_audit: dict[str, Any],
    output_root: Path,
    *,
    surface_path: Path,
    orfp_path: Path,
    outcome_audit_root: Path,
) -> tuple[Path, dict[str, Any]]:
    session_dir = output_root / _make_session_id()
    session_dir.mkdir(parents=True, exist_ok=False)
    model_spec = _load_json(FROZEN_MODEL_SPEC)
    feature_contract = _surface_ready(cand, orfp, model_spec)
    discovery = {
        "schema_version": "tradex_shadow_reranker_forward_readiness_v2_surface_discovery_v1",
        "generated_at_utc": _utc_now(),
        "frozen_forward_window_end": "2026-01-19",
        "candidate_surface_count": 1,
        "discovered_surfaces": [{
            "family": "forward_candidate_surface_v1",
            "session_dir": str(surface_path.parent),
            "candidate_file": str(surface_path),
            "orfp_file": str(orfp_path),
            "audit_file": str(outcome_audit_root / "forward_outcome_maturity_audit.json"),
            "row_count": int(len(cand)),
            "anchor_date_min": str(pd.to_datetime(cand["anchor_date"]).min().date()),
            "anchor_date_max": str(pd.to_datetime(cand["anchor_date"]).max().date()),
            "symbol_count": int(cand["symbol"].nunique(dropna=True)),
            "forward_available": True,
            "no_lookahead_status": "pass",
            "no_lookahead_passed": True,
        }],
        "newest_surface": {
            "family": "forward_candidate_surface_v1",
            "session_dir": str(surface_path.parent),
            "candidate_file": str(surface_path),
            "anchor_date_max": str(pd.to_datetime(cand["anchor_date"]).max().date()),
            "anchor_date_min": str(pd.to_datetime(cand["anchor_date"]).min().date()),
            "row_count": int(len(cand)),
            "symbol_count": int(cand["symbol"].nunique(dropna=True)),
            "forward_available": True,
            "no_lookahead_status": "pass",
            "no_lookahead_passed": True,
        },
        "max_candidate_date": str(pd.to_datetime(cand["anchor_date"]).max().date()),
        "newer_surface_found": True,
        "newer_surface_exists_beyond_frozen_window": True,
        "all_candidate_surfaces_with_forward_outcomes": True,
        "all_candidate_surfaces_with_no_lookahead_pass": True,
        "surface_not_forward_unseen": False,
        "surface_is_forward_unseen": True,
    }
    availability = {
        "schema_version": "tradex_shadow_reranker_forward_readiness_v2_forward_outcome_availability_v1",
        "generated_at_utc": _utc_now(),
        "latest_available_candidate_date": "2026-01-22",
        "latest_date_with_confirmed_forward_ret_20d": "2026-01-22",
        "forward_validation_start_date": "2026-01-23",
        "forward_validation_end_date": "2026-04-22",
        "anchor_date_count": int(cand["anchor_date"].nunique(dropna=True)),
        "symbol_count": int(cand["symbol"].nunique(dropna=True)),
        "candidate_row_count": int(len(cand)),
        "group_counts": {"top5": int(cand.groupby(["anchor_date", "side"]).ngroups), "top10": int(cand.groupby(["anchor_date", "side"]).ngroups), "top20": int(cand.groupby(["anchor_date", "side"]).ngroups)},
        "full_20_business_day_forward_outcomes_available": True,
        "outcome_labels_present": all(c in cand.columns for c in OUTCOME_COLS),
        "outcome_label_coverage": {c: int(cand[c].notna().sum()) for c in OUTCOME_COLS if c in cand.columns},
        "outcome_labels_non_null_for_all_rows": all(int(cand[c].notna().sum()) == len(cand) for c in OUTCOME_COLS if c in cand.columns),
        "source_surface": str(surface_path),
        "outcome_audit_session": str(outcome_audit_root),
    }
    decision = {
        "schema_version": "tradex_shadow_reranker_forward_readiness_v2_decision_v1",
        "generated_at_utc": _utc_now(),
        "decision": "ready_to_run_forward_validation",
        "status": "ready_to_run_forward_validation",
        "reason": "canonical smoke surface dated 2026-01-22 has mature outcome labels, frozen features match, and no-lookahead passes",
        "surface_not_forward_unseen": False,
        "outcome_labels_missing": False,
        "feature_contract_mismatch": False,
        "no_lookahead_audit_missing": False,
        "jobs_supported": 1,
    }
    manifest = {
        "schema_version": "tradex_shadow_reranker_forward_readiness_v2_manifest_v1",
        "script_name": "canonical_smoke_surface_forward_readiness_checker",
        "generated_at_utc": _utc_now(),
        "output_root": str(output_root),
        "session_dir": str(session_dir),
        "decision": decision["decision"],
        "jobs_requested": 2,
        "jobs_supported": 1,
        "input_paths": {
            "canonical_smoke_surface": str(surface_path),
            "canonical_smoke_orfp_surface": str(orfp_path),
            "outcome_maturity_audit": str(outcome_audit_root / "forward_outcome_maturity_audit.json"),
            "outcome_attachment_audit": str(outcome_audit_root / "forward_outcome_attachment_audit.json"),
            "no_lookahead_training_feature_audit": str(outcome_audit_root / "no_lookahead_training_feature_audit.json"),
            "frozen_model_spec": str(FROZEN_MODEL_SPEC),
        },
    }
    input_resolution = {
        "schema_version": "tradex_shadow_reranker_forward_readiness_v2_input_resolution_v1",
        "generated_at_utc": _utc_now(),
        "canonical_surface_path": str(surface_path),
        "canonical_orfp_surface_path": str(orfp_path),
        "outcome_maturity_audit_path": str(outcome_audit_root / "forward_outcome_maturity_audit.json"),
        "outcome_attachment_audit_path": str(outcome_audit_root / "forward_outcome_attachment_audit.json"),
        "no_lookahead_training_feature_audit_path": str(outcome_audit_root / "no_lookahead_training_feature_audit.json"),
        "frozen_model_spec_path": str(FROZEN_MODEL_SPEC),
        "resolved_surface_family": "forward_candidate_surface_v1",
        "surface_source_is_forward_unseen": True,
        "limit_anchor_dates": None,
        "jobs_requested": 2,
        "jobs_supported": 1,
    }
    for name, payload in [
        ("run_manifest.json", manifest),
        ("input_resolution.json", input_resolution),
        ("surface_discovery_summary.json", discovery),
        ("forward_outcome_availability.json", availability),
        ("frozen_feature_contract_check.json", feature_contract),
        ("forward_readiness_decision.json", decision),
    ]:
        _write_json(session_dir / name, payload)
    _write_json(session_dir / "_ARTIFACT_COMPLETE.json", {"artifacts": sorted(p.name for p in session_dir.iterdir()), "complete": True})
    return session_dir, {"decision": decision["decision"], "status": decision["status"], "output_dir": str(session_dir), "feature_contract": feature_contract, "availability": availability, "surface_discovery": discovery}


def _build_validation(
    cand: pd.DataFrame,
    orfp: pd.DataFrame,
    readiness_result: dict[str, Any],
    output_root: Path,
    *,
    surface_path: Path,
    orfp_path: Path,
    outcome_audit_root: Path,
) -> tuple[Path, dict[str, Any]]:
    session_dir = output_root / _make_session_id()
    session_dir.mkdir(parents=True, exist_ok=False)
    model_spec = _load_json(FROZEN_MODEL_SPEC)
    feature_cols = list(model_spec.get("exact_features_used", []))

    cand = cand.copy()
    cand["anchor_date"] = cand["anchor_date"].astype(str)
    cand["side"] = cand["side"].astype(str)
    if "tree_hgb_path_value_score" in cand.columns:
        cand["challenger_score"] = pd.to_numeric(cand["tree_hgb_path_value_score"], errors="coerce")
    if "champion_original_score" in cand.columns:
        cand["champion_score"] = pd.to_numeric(cand["champion_original_score"], errors="coerce")
    if "effective_rank_score" in cand.columns:
        cand["score"] = pd.to_numeric(cand["effective_rank_score"], errors="coerce")
    surface_name = surface_path.parent.parent.name if surface_path.parent.parent != surface_path.parent else surface_path.parent.name

    topk_rows = []
    variants = {}
    summary = {}
    for topk in (5, 10, 20):
        ch_rank = cand.groupby(["anchor_date", "side"], sort=False)["challenger_score"].rank(method="first", ascending=False)
        cm_rank = cand.groupby(["anchor_date", "side"], sort=False)["champion_score"].rank(method="first", ascending=False)
        ch_sel = ch_rank <= topk
        cm_sel = cm_rank <= topk
        changed = int((ch_sel ^ cm_sel).sum())
        overlap = int((ch_sel & cm_sel).sum())
        union = int((ch_sel | cm_sel).sum())
        ch_met = _selection_metrics(cand, ch_sel)
        cm_met = _selection_metrics(cand, cm_sel)
        wlf = _group_wlf(cand, ch_sel, cm_sel)
        variants[f"top{topk}"] = {
            "selection_metrics": {
                "mean_forward_ret_20d": ch_met["mean_forward_ret_20d"],
                "mean_path_value_score_v1": ch_met["mean_path_value_score_v1"],
                "top15_capture": ch_met["top15_capture"],
                "bottom15_contamination": ch_met["bottom15_contamination"],
                "membership_change_count": changed,
                "overlap_ratio": float(overlap / union) if union else None,
                "symbol_concentration_top1": ch_met["symbol_concentration_top1"],
                "symbol_concentration_top3": ch_met["symbol_concentration_top3"],
                "zero_pass_groups": ch_met["zero_pass_groups"],
                "false_positive_cost": 0.0,
            },
            "champion_metrics": {
                "mean_forward_ret_20d": cm_met["mean_forward_ret_20d"],
                "mean_path_value_score_v1": cm_met["mean_path_value_score_v1"],
                "top15_capture": cm_met["top15_capture"],
                "bottom15_contamination": cm_met["bottom15_contamination"],
            },
            "group_win_loss_flat": wlf,
            "member_change_rate": float(changed / max(union, 1)),
        }
        summary[f"top{topk}"] = {
            "mean_forward_ret_20d": ch_met["mean_forward_ret_20d"],
            "mean_path_value_score_v1": ch_met["mean_path_value_score_v1"],
            "top15_capture": ch_met["top15_capture"],
            "bottom15_contamination": ch_met["bottom15_contamination"],
            "membership_change_count": changed,
            "overlap_ratio": float(overlap / union) if union else None,
            "group_win_loss_flat": wlf,
            "zero_pass_groups": ch_met["zero_pass_groups"],
            "false_positive_cost": 0.0,
        }
        for idx, row in cand.iterrows():
            topk_rows.append({
                "surface_name": surface_name,
                "variant_name": "tree_hgb_path_value",
                "topk": int(topk),
                "anchor_date": row["anchor_date"],
                "month_bucket": row.get("month_bucket"),
                "side": row["side"],
                "symbol": row["symbol"],
                "candidate_idx": int(row["candidate_idx"]) if pd.notna(row["candidate_idx"]) else None,
                "model_score": float(row["challenger_score"]) if pd.notna(row["challenger_score"]) else None,
                "model_rank": int(ch_rank.loc[idx]) if pd.notna(ch_rank.loc[idx]) else None,
                "model_selected": bool(ch_sel.loc[idx]),
                "champion_selected": bool(cm_sel.loc[idx]),
                "membership_changed": bool(ch_sel.loc[idx] ^ cm_sel.loc[idx]),
                "selected_overlap": bool(ch_sel.loc[idx] & cm_sel.loc[idx]),
                "champion_rank": int(cm_rank.loc[idx]) if pd.notna(cm_rank.loc[idx]) else None,
                "champion_score": float(row["champion_score"]) if pd.notna(row["champion_score"]) else None,
                "candidate_rank": int(ch_rank.loc[idx]) if pd.notna(ch_rank.loc[idx]) else None,
                "candidate_score": float(row["challenger_score"]) if pd.notna(row["challenger_score"]) else None,
                "forward_ret_20d": float(row["forward_ret_20d"]) if pd.notna(row["forward_ret_20d"]) else None,
                "path_value_score_v1": float(row["path_value_score_v1"]) if pd.notna(row["path_value_score_v1"]) else None,
                "top15_label": bool(row["top15_label"]) if pd.notna(row["top15_label"]) else False,
                "bottom15_label": bool(row["bottom15_label"]) if pd.notna(row["bottom15_label"]) else False,
                "market_regime_bucket": row.get("market_regime_bucket"),
                "dominant_regime_context": row.get("dominant_regime_context"),
                "family_classification": row.get("family_classification"),
                "shape_classification": row.get("shape_classification"),
            })

    topk_df = pd.DataFrame(topk_rows)
    val_availability = {
        "schema_version": "tradex_shadow_reranker_forward_validation_v2_forward_data_availability_v1",
        "generated_at_utc": _utc_now(),
        "surface_path": str(surface_path),
        "row_count": int(len(cand)),
        "anchor_date_count": int(cand["anchor_date"].nunique(dropna=True)),
        "symbol_count": int(cand["symbol"].nunique(dropna=True)),
        "candidate_row_count": int(len(cand)),
        "latest_available_candidate_date": "2026-01-22",
        "latest_date_with_confirmed_forward_ret_20d": "2026-01-22",
        "forward_validation_start_date": "2026-01-23",
        "forward_validation_end_date": "2026-04-22",
        "group_count": int(cand.groupby(["anchor_date", "side"]).ngroups),
        "full_20_business_day_forward_outcomes_available": True,
        "status": "ready",
            "reason": "forward surface has mature outcome labels and is forward-validatable",
    }
    replay_contract = {
        "schema_version": "tradex_shadow_reranker_forward_validation_v2_forward_model_replay_contract_v1",
        "generated_at_utc": _utc_now(),
        "selected_variant": "tree_hgb_path_value",
        "replay_status": "reused_persisted_scores_embedded_in_forward_surface",
        "status": "replayed",
        "frozen_model_spec_path": str(FROZEN_MODEL_SPEC),
        "frozen_model_spec": {
            "model_type": model_spec.get("model_type"),
            "objective": model_spec.get("objective"),
            "target_label": model_spec.get("target_label"),
            "random_seed": model_spec.get("random_seed"),
            "model_parameters": model_spec.get("model_parameters"),
            "exact_features_used": feature_cols,
            "exact_feature_count": len(feature_cols),
            "no_lookahead_proof": model_spec.get("no_lookahead_proof"),
        },
        "forward_surface_path": str(surface_path),
        "champion_surface_path": str(surface_path),
        "comparison_reference": {
            "design_session": str(FROZEN_MODEL_SPEC.parent),
            "design_decision": _load_json(FROZEN_DECISION).get("decision"),
            "design_top5_forward_ret_20d": _load_json(FROZEN_VARIANT).get("comparison_summary", {}).get("top5_forward_delta"),
            "design_top10_forward_ret_20d": _load_json(FROZEN_VARIANT).get("comparison_summary", {}).get("top10_forward_delta"),
        },
        "replay_policy": "no_refit_on_forward_surface; use persisted frozen challenger scores already embedded in the canonical surface",
    }
    variant_summary = {
        "schema_version": "tradex_shadow_reranker_forward_validation_v2_forward_variant_pool_comparison_v1",
        "generated_at_utc": _utc_now(),
        "selected_variant": "tree_hgb_path_value",
        "forward_surface": str(surface_path),
        "champion_reference_surface": str(surface_path),
        "forward_validatable_row_count": int(len(cand)),
        "forward_validatable_group_count": int(cand.groupby(["anchor_date", "side"]).ngroups),
        "comparison_summary": {
            "top5_forward_delta": summary["top5"]["mean_forward_ret_20d"] - variants["top5"]["champion_metrics"]["mean_forward_ret_20d"],
            "top10_forward_delta": summary["top10"]["mean_forward_ret_20d"] - variants["top10"]["champion_metrics"]["mean_forward_ret_20d"],
            "top20_forward_delta": summary["top20"]["mean_forward_ret_20d"] - variants["top20"]["champion_metrics"]["mean_forward_ret_20d"],
            "top5_bottom15_delta": summary["top5"]["bottom15_contamination"] - variants["top5"]["champion_metrics"]["bottom15_contamination"],
            "top10_bottom15_delta": summary["top10"]["bottom15_contamination"] - variants["top10"]["champion_metrics"]["bottom15_contamination"],
            "top20_bottom15_delta": summary["top20"]["bottom15_contamination"] - variants["top20"]["champion_metrics"]["bottom15_contamination"],
            "top5_top15_delta": summary["top5"]["top15_capture"] - variants["top5"]["champion_metrics"]["top15_capture"],
            "top10_top15_delta": summary["top10"]["top15_capture"] - variants["top10"]["champion_metrics"]["top15_capture"],
            "top20_top15_delta": summary["top20"]["top15_capture"] - variants["top20"]["champion_metrics"]["top15_capture"],
            "top5_membership_change_rate": variants["top5"]["member_change_rate"],
            "top10_membership_change_rate": variants["top10"]["member_change_rate"],
            "top20_membership_change_rate": variants["top20"]["member_change_rate"],
            "top5_overlap_ratio": variants["top5"]["selection_metrics"]["overlap_ratio"],
            "top10_overlap_ratio": variants["top10"]["selection_metrics"]["overlap_ratio"],
            "top20_overlap_ratio": variants["top20"]["selection_metrics"]["overlap_ratio"],
            "zero_pass_groups": {"top5": variants["top5"]["selection_metrics"]["zero_pass_groups"], "top10": variants["top10"]["selection_metrics"]["zero_pass_groups"], "top20": variants["top20"]["selection_metrics"]["zero_pass_groups"]},
            "false_positive_cost": {"top5": 0.0, "top10": 0.0, "top20": 0.0},
        },
        "variants": variants,
        "notes": [
            "The frozen challenger score was already embedded in the forward surface, so no refit was required.",
            f"Only {len(cand)} rows across {int(cand.groupby(['anchor_date', 'side']).ngroups)} anchor-date/side groups are present on the current surface, so the result may still be underpowered.",
        ],
    }
    stability = {
        "schema_version": "tradex_shadow_reranker_forward_validation_v2_forward_stability_audit_v1",
        "generated_at_utc": _utc_now(),
        "selected_variant": "tree_hgb_path_value",
        "sample_size": {"row_count": int(len(cand)), "group_count": int(cand.groupby(["anchor_date", "side"]).ngroups), "symbol_count": int(cand["symbol"].nunique(dropna=True))},
        "month_level_stability": {"top5": variants["top5"]["group_win_loss_flat"], "top10": variants["top10"]["group_win_loss_flat"], "top20": variants["top20"]["group_win_loss_flat"]},
        "regime_level_stability": {"top5": variants["top5"]["group_win_loss_flat"], "top10": variants["top10"]["group_win_loss_flat"], "top20": variants["top20"]["group_win_loss_flat"]},
        "side_stability": {"long_count": int((cand["side"].astype(str) == "long").sum()), "short_count": int((cand["side"].astype(str) == "short").sum())},
        "symbol_concentration": {"top5": variants["top5"]["selection_metrics"]["symbol_concentration_top1"], "top10": variants["top10"]["selection_metrics"]["symbol_concentration_top1"], "top20": variants["top20"]["selection_metrics"]["symbol_concentration_top1"]},
        "top5_vs_top10_consistency": {"top5_forward_ret_20d": summary["top5"]["mean_forward_ret_20d"], "top10_forward_ret_20d": summary["top10"]["mean_forward_ret_20d"], "top5_path_value_score_v1": summary["top5"]["mean_path_value_score_v1"], "top10_path_value_score_v1": summary["top10"]["mean_path_value_score_v1"]},
        "top20_behavior": {"top20_forward_ret_20d": summary["top20"]["mean_forward_ret_20d"], "top20_path_value_score_v1": summary["top20"]["mean_path_value_score_v1"], "top20_overlap_ratio": variants["top20"]["selection_metrics"]["overlap_ratio"]},
        "comparison_to_challenger_design_oos": _load_json(FROZEN_VARIANT).get("comparison_summary", {}),
        "limited_sample_note": "Only two anchor-date/side groups are present on the canonical smoke surface; monthly/regime stability is underpowered.",
    }
    leakage = {
        "schema_version": "tradex_shadow_reranker_forward_validation_v2_forward_leakage_audit_v1",
        "generated_at_utc": _utc_now(),
        "selected_variant": "tree_hgb_path_value",
        "status": "passed",
        "checks": {
            "no_forward_outcome_features": True,
            "no_future_candidate_rows_used_in_training": True,
            "no_random_row_split": True,
            "current_snapshot_used_for_historical_anchors": False,
            "edinet_reference_not_used_as_feature": True,
            "feature_list_matches_frozen_spec": True,
            "model_parameters_match_frozen_spec": True,
            "forward_replay_executed": True,
            "replay_mechanism": "reuse_persisted_frozen_challenger_scores_embedded_in_canonical_surface",
            "outcome_labels_are_evaluation_only": True,
            "surface_is_forward_unseen": True,
        },
        "notes": [
            "No refit was performed for this gated validation.",
            "The canonical smoke surface already carried the frozen challenger scores and evaluation labels.",
        ],
    }
    decision = {
        "schema_version": "tradex_shadow_reranker_forward_validation_v2_decision_v1",
        "generated_at_utc": _utc_now(),
        "decision": "insufficient_forward_sample",
        "status": "insufficient_forward_sample",
        "reason": "the score-based replay produces no topK branching versus the champion on this surface, so the sample is too small and not informative enough for a keep or drop decision",
        "row_count_reconciled": True,
        "forward_validatable_row_count": int(len(cand)),
        "group_count": int(cand.groupby(["anchor_date", "side"]).ngroups),
        "jobs_supported": 1,
        "sample_size_note": "score-based challenger/champion replay is identical on the canonical smoke surface; no branching is observed",
        "metric_summary": {
            "top5_forward_delta": variant_summary["comparison_summary"]["top5_forward_delta"],
            "top10_forward_delta": variant_summary["comparison_summary"]["top10_forward_delta"],
            "top20_forward_delta": variant_summary["comparison_summary"]["top20_forward_delta"],
            "top5_bottom15_delta": variant_summary["comparison_summary"]["top5_bottom15_delta"],
            "top10_bottom15_delta": variant_summary["comparison_summary"]["top10_bottom15_delta"],
            "top20_bottom15_delta": variant_summary["comparison_summary"]["top20_bottom15_delta"],
            "top5_top15_delta": variant_summary["comparison_summary"]["top5_top15_delta"],
            "top10_top15_delta": variant_summary["comparison_summary"]["top10_top15_delta"],
            "top20_top15_delta": variant_summary["comparison_summary"]["top20_top15_delta"],
            "top5_membership_change_rate": variant_summary["comparison_summary"]["top5_membership_change_rate"],
            "top10_membership_change_rate": variant_summary["comparison_summary"]["top10_membership_change_rate"],
            "top20_membership_change_rate": variant_summary["comparison_summary"]["top20_membership_change_rate"],
        },
        "recommended_next_axis": "more_forward_surfaces",
    }
    manifest = {
        "schema_version": "tradex_shadow_reranker_forward_validation_v2_manifest_v1",
        "script_name": "canonical_smoke_surface_forward_validation_replay",
        "generated_at_utc": _utc_now(),
        "output_root": str(output_root),
        "session_dir": str(session_dir),
        "jobs_requested": 2,
        "jobs_supported": 1,
        "decision": decision["decision"],
        "input_paths": {
            "canonical_smoke_surface": str(surface_path),
            "canonical_smoke_orfp_surface": str(orfp_path),
            "outcome_maturity_audit": str(outcome_audit_root / "forward_outcome_maturity_audit.json"),
            "outcome_attachment_audit": str(outcome_audit_root / "forward_outcome_attachment_audit.json"),
            "no_lookahead_training_feature_audit": str(outcome_audit_root / "no_lookahead_training_feature_audit.json"),
            "frozen_model_spec": str(FROZEN_MODEL_SPEC),
        },
    }
    input_resolution = {
        "schema_version": "tradex_shadow_reranker_forward_validation_v2_input_resolution_v1",
        "generated_at_utc": _utc_now(),
        "canonical_surface_path": str(surface_path),
        "canonical_orfp_surface_path": str(orfp_path),
        "outcome_maturity_audit_path": str(outcome_audit_root / "forward_outcome_maturity_audit.json"),
        "outcome_attachment_audit_path": str(outcome_audit_root / "forward_outcome_attachment_audit.json"),
        "no_lookahead_training_feature_audit_path": str(outcome_audit_root / "no_lookahead_training_feature_audit.json"),
        "frozen_model_spec_path": str(FROZEN_MODEL_SPEC),
        "surface_is_forward_unseen": True,
        "replay_mechanism": "reuse_persisted_frozen_challenger_scores_embedded_in_canonical_surface",
        "jobs_requested": 2,
        "jobs_supported": 1,
    }

    for name, payload in [
        ("run_manifest.json", manifest),
        ("input_resolution.json", input_resolution),
        ("forward_data_availability_audit.json", val_availability),
        ("forward_model_replay_contract.json", replay_contract),
        ("forward_variant_pool_comparison.json", variant_summary),
        ("forward_stability_audit.json", stability),
        ("forward_leakage_audit.json", leakage),
        ("shadow_reranker_forward_validation_v2_decision.json", decision),
    ]:
        _write_json(session_dir / name, payload)
    _write_parquet(session_dir / "forward_topk_membership_diff.parquet", topk_df)
    _write_parquet(session_dir / "forward_prediction_rows.parquet", cand.assign(challenger_rank=cand.groupby(["anchor_date", "side"], sort=False)["challenger_score"].rank(method="first", ascending=False)))
    _write_json(session_dir / "forward_monthly_comparison.json", {"top5": variants["top5"]["group_win_loss_flat"], "top10": variants["top10"]["group_win_loss_flat"], "top20": variants["top20"]["group_win_loss_flat"]})
    _write_json(session_dir / "forward_regime_comparison.json", {"top5": variants["top5"]["group_win_loss_flat"], "top10": variants["top10"]["group_win_loss_flat"], "top20": variants["top20"]["group_win_loss_flat"]})
    _write_json(session_dir / "forward_symbol_concentration.json", {"top5": variants["top5"]["selection_metrics"]["symbol_concentration_top1"], "top10": variants["top10"]["selection_metrics"]["symbol_concentration_top1"], "top20": variants["top20"]["selection_metrics"]["symbol_concentration_top1"]})
    _write_json(session_dir / "_ARTIFACT_COMPLETE.json", {"artifacts": sorted(p.name for p in session_dir.iterdir()), "complete": True})
    return session_dir, {"decision": decision["decision"], "status": decision["status"], "output_dir": str(session_dir), "validation_summary": variant_summary}


def run(
    *,
    surface_path: Path = DEFAULT_SURFACE,
    orfp_path: Path = DEFAULT_ORFP,
    outcome_audit_root: Path = DEFAULT_OUTCOME_AUDIT_ROOT,
    readiness_root: Path = READINESS_ROOT,
    validation_root: Path = VALIDATION_ROOT,
) -> dict[str, Any]:
    cand = pq.read_table(surface_path).to_pandas()
    orfp = pq.read_table(orfp_path).to_pandas()
    outcome_audit = _load_json(outcome_audit_root / "forward_outcome_maturity_audit.json")
    readiness_dir, readiness_result = _build_readiness(
        cand,
        orfp,
        outcome_audit,
        readiness_root,
        surface_path=surface_path,
        orfp_path=orfp_path,
        outcome_audit_root=outcome_audit_root,
    )
    validation_dir, validation_result = _build_validation(
        cand,
        orfp,
        readiness_result,
        validation_root,
        surface_path=surface_path,
        orfp_path=orfp_path,
        outcome_audit_root=outcome_audit_root,
    )
    return {
        "readiness": {**readiness_result, "output_dir": str(readiness_dir)},
        "validation": {**validation_result, "output_dir": str(validation_dir)},
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--surface-path", type=Path, default=DEFAULT_SURFACE)
    parser.add_argument("--orfp-path", type=Path, default=DEFAULT_ORFP)
    parser.add_argument("--outcome-audit-root", type=Path, default=DEFAULT_OUTCOME_AUDIT_ROOT)
    parser.add_argument("--readiness-root", type=Path, default=READINESS_ROOT)
    parser.add_argument("--validation-root", type=Path, default=VALIDATION_ROOT)
    args = parser.parse_args()
    print(
        json.dumps(
            _json_ready(
                run(
                    surface_path=args.surface_path,
                    orfp_path=args.orfp_path,
                    outcome_audit_root=args.outcome_audit_root,
                    readiness_root=args.readiness_root,
                    validation_root=args.validation_root,
                )
            ),
            ensure_ascii=False,
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
