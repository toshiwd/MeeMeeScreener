from __future__ import annotations

import argparse
import json
import math
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

SCRIPT_NAME = "tradex_candidate_generation_breadth_quality_redesign_audit_v1"
SCHEMA_VERSION = "tradex_candidate_generation_breadth_quality_redesign_audit_v1"
MANIFEST_SCHEMA_VERSION = "tradex_candidate_generation_breadth_quality_redesign_audit_v1_manifest_v1"
INPUT_RESOLUTION_SCHEMA_VERSION = "tradex_candidate_generation_breadth_quality_redesign_audit_v1_input_resolution_v1"
BREADTH_SCHEMA_VERSION = "tradex_candidate_generation_breadth_quality_redesign_audit_v1_breadth_audit_v1"
WINNER_SCHEMA_VERSION = "tradex_candidate_generation_breadth_quality_redesign_audit_v1_winner_inclusion_v1"
ORACLE_SCHEMA_VERSION = "tradex_candidate_generation_breadth_quality_redesign_audit_v1_oracle_headroom_v1"
ADMISSION_SCHEMA_VERSION = "tradex_candidate_generation_breadth_quality_redesign_audit_v1_admission_failure_v1"
OPTIONS_SCHEMA_VERSION = "tradex_candidate_generation_breadth_quality_redesign_audit_v1_redesign_options_v1"
RECOMMENDATION_SCHEMA_VERSION = "tradex_candidate_generation_breadth_quality_redesign_audit_v1_recommendation_v1"
DECISION_SCHEMA_VERSION = "tradex_candidate_generation_breadth_quality_redesign_audit_v1_decision_v1"
ARTIFACT_COMPLETE_SCHEMA_VERSION = "tradex_candidate_generation_breadth_quality_redesign_audit_v1_artifact_complete_v1"

DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\candidate_generation_redesign_audit_v1")
SIDE_AWARE_SESSION = Path(r"G:\Tradex\side_aware_top20pct_label_modeling_feasibility_v1\20260502T103211Z-790902")
LABEL_COVERAGE_SESSION = Path(r"G:\Tradex\candidate_generation_label_coverage_audit_v1\20260502T101135Z-344988")
ACCUMULATED_SESSION = Path(r"G:\Tradex\shadow_reranker_accumulated_forward_validation_v1\20260502T082532Z-c17e19")
TOP_K_VALUES = (5, 10, 20)


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _session_id() -> str:
    now = datetime.now(timezone.utc)
    return f"{now.strftime('%Y%m%dT%H%M%SZ')}-{now.microsecond:06d}"


def _json_ready(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _json_ready(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_json_ready(item) for item in value]
    if isinstance(value, tuple):
        return [_json_ready(item) for item in value]
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    if isinstance(value, float) and not math.isfinite(value):
        return None
    if pd.isna(value):
        return None
    return value


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(_json_ready(payload), ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _load_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"missing required source artifact: {path}")
    return json.loads(path.read_text(encoding="utf-8"))


def _ensure_exists(path: Path, label: str) -> Path:
    if not path.exists():
        raise FileNotFoundError(f"{label} not found: {path}")
    return path


def _load_frame(path: Path, *, require_mae: bool = True) -> pd.DataFrame:
    frame = pd.read_parquet(_ensure_exists(path, str(path))).copy()
    return _normalize_frame(frame, require_mae=require_mae)


def _normalize_frame(frame: pd.DataFrame, *, require_mae: bool = True) -> pd.DataFrame:
    out = frame.copy()
    required = ["anchor_date", "side", "symbol", "candidate_idx", "forward_ret_20d", "path_value_score_v1"]
    if require_mae:
        required.append("mae_20d")
    missing = [col for col in required if col not in out.columns]
    if missing:
        raise RuntimeError(f"missing required columns: {missing}")
    if "month_bucket" not in out.columns:
        out["month_bucket"] = pd.to_datetime(out["anchor_date"], errors="coerce").dt.strftime("%Y-%m")
    if "split" not in out.columns:
        out["split"] = "all"

    for col in ["anchor_date", "side", "symbol", "month_bucket", "split"]:
        out[col] = out[col].astype(str)
    out["candidate_idx"] = pd.to_numeric(out["candidate_idx"], errors="coerce").fillna(pd.Series(out.index, index=out.index)).astype(int)
    for col in ["forward_ret_20d", "path_value_score_v1", "mae_20d", "forward_ret_10d", "forward_ret_5d"]:
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce")
    for col in ["top15_label", "bottom15_label", "side_aware_group_top20pct_forward_ret_20d_label"]:
        if col in out.columns:
            out[col] = out[col].fillna(False).astype(bool)
    for col in ["model_score", "diagnostic_score", "reference_score", "champion_original_score", "tree_hgb_path_value_score", "effective_rank_score", "score", "rank"]:
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce")
    return out


def _group_sizes(frame: pd.DataFrame) -> pd.DataFrame:
    groups = frame.groupby(["anchor_date", "side"], sort=False)
    rows: list[dict[str, Any]] = []
    for (anchor_date, side), group in groups:
        group_size = int(len(group))
        rows.append(
            {
                "anchor_date": str(anchor_date),
                "side": str(side),
                "month_bucket": str(group["month_bucket"].iloc[0]),
                "group_size": group_size,
                "top5_available": bool(group_size >= 5),
                "top10_available": bool(group_size >= 10),
                "top20_available": bool(group_size >= 20),
                "too_thin_for_top5": int(group_size < 5),
                "too_thin_for_top10": int(group_size < 10),
                "too_thin_for_top20": int(group_size < 20),
            }
        )
    return pd.DataFrame(rows)


def _count_summary_by_side(frame: pd.DataFrame) -> dict[str, Any]:
    summary: dict[str, Any] = {}
    group_sizes = frame.groupby(["anchor_date", "side"], sort=False).size()
    for side, side_frame in frame.groupby("side", sort=False):
        side_groups = side_frame.groupby("anchor_date", sort=False).size()
        summary[str(side)] = {
            "row_count": int(len(side_frame)),
            "group_count": int(side_groups.shape[0]),
            "mean_group_size": float(side_groups.mean()),
            "median_group_size": float(side_groups.median()),
            "min_group_size": int(side_groups.min()),
            "max_group_size": int(side_groups.max()),
            "top5_thin_groups": int((side_groups < 5).sum()),
            "top10_thin_groups": int((side_groups < 10).sum()),
            "top20_thin_groups": int((side_groups < 20).sum()),
        }
    summary["overall"] = {
        "row_count": int(len(frame)),
        "group_count": int(group_sizes.shape[0]),
        "mean_group_size": float(group_sizes.mean()),
        "median_group_size": float(group_sizes.median()),
        "min_group_size": int(group_sizes.min()),
        "max_group_size": int(group_sizes.max()),
        "top5_thin_groups": int((group_sizes < 5).sum()),
        "top10_thin_groups": int((group_sizes < 10).sum()),
        "top20_thin_groups": int((group_sizes < 20).sum()),
    }
    return summary


def _build_candidate_pool_breadth_audit(frame: pd.DataFrame, accumulated_frame: pd.DataFrame) -> tuple[dict[str, Any], pd.DataFrame]:
    group_rows = _group_sizes(frame)
    acc_group_rows = _group_sizes(accumulated_frame)
    counts = _count_summary_by_side(frame)
    acc_counts = _count_summary_by_side(accumulated_frame)
    breadth = {
        "schema_version": BREADTH_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "candidate_pool_scope": "all_surface",
        "row_count": int(len(frame)),
        "group_count": int(group_rows.shape[0]),
        "anchor_date_min": str(frame["anchor_date"].min()),
        "anchor_date_max": str(frame["anchor_date"].max()),
        "surface_comparison": {
            "side_aware_surface": counts,
            "accumulated_surface": acc_counts,
            "same_row_count": bool(len(frame) == len(accumulated_frame)),
            "same_group_count": bool(group_rows.shape[0] == acc_group_rows.shape[0]),
            "same_thin_counts": bool(
                counts["overall"]["top5_thin_groups"] == acc_counts["overall"]["top5_thin_groups"]
                and counts["overall"]["top10_thin_groups"] == acc_counts["overall"]["top10_thin_groups"]
                and counts["overall"]["top20_thin_groups"] == acc_counts["overall"]["top20_thin_groups"]
            ),
        },
        "overall_thin_groups": {
            "top5": counts["overall"]["top5_thin_groups"],
            "top10": counts["overall"]["top10_thin_groups"],
            "top20": counts["overall"]["top20_thin_groups"],
        },
        "by_side": {side: stats for side, stats in counts.items() if side != "overall"},
        "by_month_side": (
            group_rows.groupby(["month_bucket", "side"], sort=True)
            .agg(
                row_count=("group_size", "sum"),
                group_count=("anchor_date", "count"),
                mean_group_size=("group_size", "mean"),
                min_group_size=("group_size", "min"),
                max_group_size=("group_size", "max"),
                top5_thin_groups=("too_thin_for_top5", "sum"),
                top10_thin_groups=("too_thin_for_top10", "sum"),
                top20_thin_groups=("too_thin_for_top20", "sum"),
            )
            .reset_index()
            .to_dict("records")
        ),
        "too_thin_for_intended_topk": {
            "top5": bool(counts["overall"]["top5_thin_groups"] > 0),
            "top10": bool(counts["overall"]["top10_thin_groups"] > 0),
            "top20": bool(counts["overall"]["top20_thin_groups"] > 0),
        },
        "short_side_structurally_under_supplied": bool(
            counts["short"]["mean_group_size"] < counts["long"]["mean_group_size"] / 4 if "short" in counts and "long" in counts else True
        ),
    }
    return breadth, group_rows


def _label_profile(frame: pd.DataFrame, name: str, mask: pd.Series, definition: str) -> dict[str, Any]:
    mask = mask.fillna(False).astype(bool)
    by_group = frame.groupby(["anchor_date", "side"], sort=False)[mask.name].sum() if mask.name and mask.name in frame.columns else None
    if by_group is None:
        tmp = frame.copy()
        tmp["_mask"] = mask
        by_group = tmp.groupby(["anchor_date", "side"], sort=False)["_mask"].sum()
    long_mask = frame["side"].astype(str).eq("long")
    short_mask = frame["side"].astype(str).eq("short")
    tmp = frame.copy()
    tmp["_mask"] = mask
    long_group_positive_count = int((tmp.loc[long_mask].groupby(["anchor_date", "side"], sort=False)["_mask"].sum() > 0).sum())
    short_group_positive_count = int((tmp.loc[short_mask].groupby(["anchor_date", "side"], sort=False)["_mask"].sum() > 0).sum())
    return {
        "name": name,
        "definition": definition,
        "positive_count": int(mask.sum()),
        "positive_rate": float(mask.mean()),
        "long_positive_count": int(mask.loc[long_mask].sum()),
        "short_positive_count": int(mask.loc[short_mask].sum()),
        "group_positive_count": int((by_group > 0).sum()),
        "long_group_positive_count": long_group_positive_count,
        "short_group_positive_count": short_group_positive_count,
        "long_positive_rate": float(mask.loc[long_mask].mean()) if long_mask.any() else None,
        "short_positive_rate": float(mask.loc[short_mask].mean()) if short_mask.any() else None,
        "class_imbalance": f"{int(mask.sum())}:{int((~mask).sum())}",
    }


def _build_winner_inclusion_audit(frame: pd.DataFrame, label_sensitivity: pd.DataFrame) -> tuple[dict[str, Any], pd.DataFrame]:
    definitions = {
        "side_aware_group_top20pct_forward_ret_20d_label": {
            "definition": "positive if row is in the top ceil(20% of group size) by forward_ret_20d within each anchor_date / side group with tie breaks on path_value_score_v1, mae_20d, candidate_idx",
            "mask": frame["side_aware_group_top20pct_forward_ret_20d_label"],
        },
        "top15_label": {
            "definition": "existing winner label from the current corpus",
            "mask": frame["top15_label"],
        },
        "return_threshold_gt_0_label": {
            "definition": "forward_ret_20d > 0",
            "mask": pd.to_numeric(frame["forward_ret_20d"], errors="coerce") > 0,
        },
        "path_quality_threshold_gt_0_label": {
            "definition": "path_value_score_v1 > 0",
            "mask": pd.to_numeric(frame["path_value_score_v1"], errors="coerce") > 0,
        },
    }
    profiles = {name: _label_profile(frame, name, spec["mask"].rename(name), spec["definition"]) for name, spec in definitions.items()}
    group_rows = []
    for (anchor_date, side), group in frame.groupby(["anchor_date", "side"], sort=False):
        group_rows.append(
            {
                "anchor_date": str(anchor_date),
                "side": str(side),
                "group_size": int(len(group)),
                "top20pct_positive_count": int(group["side_aware_group_top20pct_forward_ret_20d_label"].sum()),
                "top15_positive_count": int(group["top15_label"].sum()),
                "return_threshold_positive_count": int((pd.to_numeric(group["forward_ret_20d"], errors="coerce") > 0).sum()),
                "path_threshold_positive_count": int((pd.to_numeric(group["path_value_score_v1"], errors="coerce") > 0).sum()),
                "top20pct_positive_present": bool(group["side_aware_group_top20pct_forward_ret_20d_label"].any()),
                "top15_positive_present": bool(group["top15_label"].any()),
                "return_threshold_positive_present": bool((pd.to_numeric(group["forward_ret_20d"], errors="coerce") > 0).any()),
                "path_threshold_positive_present": bool((pd.to_numeric(group["path_value_score_v1"], errors="coerce") > 0).any()),
            }
        )
    group_table = pd.DataFrame(group_rows)
    side_summary = {
        side: {
            "row_count": int(len(side_frame)),
            "group_count": int(side_frame.groupby("anchor_date", sort=False).ngroups),
            "top20pct_positive_count": int(side_frame["side_aware_group_top20pct_forward_ret_20d_label"].sum()),
            "top15_positive_count": int(side_frame["top15_label"].sum()),
            "return_threshold_positive_count": int((pd.to_numeric(side_frame["forward_ret_20d"], errors="coerce") > 0).sum()),
            "path_threshold_positive_count": int((pd.to_numeric(side_frame["path_value_score_v1"], errors="coerce") > 0).sum()),
            "top20pct_positive_rate": float(side_frame["side_aware_group_top20pct_forward_ret_20d_label"].mean()),
            "top15_positive_rate": float(side_frame["top15_label"].mean()),
        }
        for side, side_frame in frame.groupby("side", sort=False)
    }
    label_from_sensitivity = {
        row["name"]: {
            "definition": row["definition"],
            "positive_count": int(row["positive_count"]),
            "positive_rate": float(row["positive_rate"]),
            "long_positive_count": int(row["long_positive_count"]),
            "short_positive_count": int(row["short_positive_count"]),
            "group_positive_count": int(row["group_positive_count"]),
            "long_group_positive_count": int(row["long_group_positive_count"]),
            "short_group_positive_count": int(row["short_group_positive_count"]),
            "long_positive_rate": float(row["long_positive_rate"]) if row["long_positive_rate"] is not None else None,
            "short_positive_rate": float(row["short_positive_rate"]) if row["short_positive_rate"] is not None else None,
            "class_imbalance": row["class_imbalance"],
        }
        for row in label_sensitivity.to_dict("records")
    }
    group_count = int(frame.groupby(["anchor_date", "side"], sort=False).ngroups)
    audit = {
        "schema_version": WINNER_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "row_count": int(len(frame)),
        "group_count": group_count,
        "profiles": profiles,
        "side_summary": side_summary,
        "by_group": group_table.to_dict("records"),
        "from_label_sensitivity": label_from_sensitivity,
        "candidate_generation_can_admit_enough_potential_winners": bool(
            profiles["top15_label"]["group_positive_count"] == group_count
            and profiles["top20pct_label"]["group_positive_count"] == group_count
        ),
        "side_specific_modeling_feasible": {
            "long": bool(side_summary.get("long", {}).get("top15_positive_count", 0) > 0),
            "short": bool(side_summary.get("short", {}).get("top15_positive_count", 0) > 0),
        },
        "winner_absent_group_count_by_label": {
            name: int(
                frame.assign(_mask=spec["mask"].fillna(False).astype(bool))
                .groupby(["anchor_date", "side"], sort=False)["_mask"]
                .sum()
                .eq(0)
                .sum()
            )
            for name, spec in definitions.items()
        },
    }
    return audit, group_table


def _rank_within_groups(frame: pd.DataFrame, *, sort_cols: list[str], ascending: list[bool], prefix: str) -> pd.DataFrame:
    if len(sort_cols) != len(ascending):
        raise ValueError("sort_cols and ascending must have the same length")
    ordered = frame.sort_values(["anchor_date", "side", *sort_cols], ascending=[True, True, *ascending], kind="stable").copy()
    ordered[f"{prefix}_rank"] = ordered.groupby(["anchor_date", "side"], sort=False).cumcount() + 1
    for topk in TOP_K_VALUES:
        ordered[f"{prefix}_selected_top{topk}"] = ordered[f"{prefix}_rank"] <= topk
    return ordered.sort_index()


def _selection_summary(
    frame: pd.DataFrame,
    *,
    prefix: str,
    topk: int,
    label_col: str = "top15_label",
    reference_keys: set[tuple[str, str, str]] | None = None,
) -> dict[str, Any]:
    selected = frame.loc[frame[f"{prefix}_selected_top{topk}"].fillna(False).astype(bool)].copy()
    if selected.empty:
        return {
            "selected_row_count": 0,
            "selected_group_count": 0,
            "mean_forward_ret_20d": None,
            "mean_path_value_score_v1": None,
            "top15_capture_rate": None,
            "bottom15_contamination_rate": None,
            "side_aware_capture_rate": None,
            "zero_pass_groups": 0,
            "membership_changed_count": 0,
            "overlap_ratio": None,
            "side_split": {},
            "month_split": {},
            "top1_symbol_share": None,
            "top3_symbol_share": None,
        }
    selected_keys = set(map(tuple, selected[["anchor_date", "side", "symbol"]].astype(str).values.tolist()))
    ref_keys = reference_keys or set()
    union = len(selected_keys | ref_keys) if ref_keys else len(selected_keys)
    inter = len(selected_keys & ref_keys) if ref_keys else len(selected_keys)
    group_selected = selected.groupby(["anchor_date", "side"], sort=False)
    zero_pass_groups = int(sum(not bool(g[label_col].fillna(False).astype(bool).any()) for _, g in group_selected))
    symbol_counts = selected["symbol"].value_counts(dropna=False)
    label_series = selected[label_col].fillna(False).astype(bool)
    return {
        "selected_row_count": int(len(selected)),
        "selected_group_count": int(group_selected.ngroups),
        "mean_forward_ret_20d": float(pd.to_numeric(selected["forward_ret_20d"], errors="coerce").mean()),
        "mean_path_value_score_v1": float(pd.to_numeric(selected["path_value_score_v1"], errors="coerce").mean()),
        "top15_capture_rate": float(pd.to_numeric(label_series if label_col == "top15_label" else selected["top15_label"], errors="coerce").mean()),
        "bottom15_contamination_rate": float(pd.to_numeric(selected["bottom15_label"], errors="coerce").mean()),
        "side_aware_capture_rate": float(pd.to_numeric(selected["side_aware_group_top20pct_forward_ret_20d_label"], errors="coerce").mean()),
        "zero_pass_groups": zero_pass_groups,
        "membership_changed_count": int(len(selected_keys ^ ref_keys)) if ref_keys else 0,
        "overlap_ratio": float(inter / union) if union else None,
        "side_split": {str(k): int(v) for k, v in selected["side"].value_counts(dropna=False).items()},
        "month_split": {str(k): int(v) for k, v in selected["month_bucket"].value_counts(dropna=False).items()},
        "top1_symbol_share": float(symbol_counts.iloc[0] / len(selected)) if len(symbol_counts) else None,
        "top3_symbol_share": float(symbol_counts.iloc[:3].sum() / len(selected)) if len(symbol_counts) else None,
    }


def _build_oracle_headroom_audit(frame: pd.DataFrame, validation_test_frame: pd.DataFrame) -> tuple[dict[str, Any], pd.DataFrame]:
    selections = {
        "champion": _rank_within_groups(frame, sort_cols=["reference_score", "rank", "symbol", "candidate_idx"], ascending=[False, True, True, True], prefix="champion"),
        "diagnostic": _rank_within_groups(frame, sort_cols=["diagnostic_score", "rank", "symbol", "candidate_idx"], ascending=[False, True, True, True], prefix="diagnostic"),
        "model": _rank_within_groups(frame, sort_cols=["model_score", "rank", "symbol", "candidate_idx"], ascending=[False, True, True, True], prefix="model"),
        "oracle": _rank_within_groups(frame, sort_cols=["forward_ret_20d", "path_value_score_v1", "mae_20d", "candidate_idx", "symbol"], ascending=[False, False, True, True, True], prefix="oracle"),
    }
    selections_validation_test = {
        "champion": _rank_within_groups(validation_test_frame, sort_cols=["reference_score", "rank", "symbol", "candidate_idx"], ascending=[False, True, True, True], prefix="champion"),
        "diagnostic": _rank_within_groups(validation_test_frame, sort_cols=["diagnostic_score", "rank", "symbol", "candidate_idx"], ascending=[False, True, True, True], prefix="diagnostic"),
        "model": _rank_within_groups(validation_test_frame, sort_cols=["model_score", "rank", "symbol", "candidate_idx"], ascending=[False, True, True, True], prefix="model"),
        "oracle": _rank_within_groups(validation_test_frame, sort_cols=["forward_ret_20d", "path_value_score_v1", "mae_20d", "candidate_idx", "symbol"], ascending=[False, False, True, True, True], prefix="oracle"),
    }

    def _per_scope(scope_name: str, scope_frame: pd.DataFrame, scope_selections: dict[str, pd.DataFrame]) -> tuple[dict[str, Any], pd.DataFrame]:
        by_group_rows: list[dict[str, Any]] = []
        for topk in TOP_K_VALUES:
            champ = scope_selections["champion"]
            diag = scope_selections["diagnostic"]
            model = scope_selections["model"]
            oracle = scope_selections["oracle"]
            champ_sel = champ.loc[champ[f"champion_selected_top{topk}"].fillna(False).astype(bool)]
            diag_sel = diag.loc[diag[f"diagnostic_selected_top{topk}"].fillna(False).astype(bool)]
            model_sel = model.loc[model[f"model_selected_top{topk}"].fillna(False).astype(bool)]
            oracle_sel = oracle.loc[oracle[f"oracle_selected_top{topk}"].fillna(False).astype(bool)]
            champ_keys = set(map(tuple, champ_sel[["anchor_date", "side", "symbol"]].astype(str).values.tolist()))
            diag_keys = set(map(tuple, diag_sel[["anchor_date", "side", "symbol"]].astype(str).values.tolist()))
            model_keys = set(map(tuple, model_sel[["anchor_date", "side", "symbol"]].astype(str).values.tolist()))
            oracle_keys = set(map(tuple, oracle_sel[["anchor_date", "side", "symbol"]].astype(str).values.tolist()))
            group_stats = []
            for (anchor_date, side), group in scope_frame.groupby(["anchor_date", "side"], sort=False):
                g_champ = champ.loc[(champ["anchor_date"].astype(str) == str(anchor_date)) & (champ["side"].astype(str) == str(side))]
                g_diag = diag.loc[(diag["anchor_date"].astype(str) == str(anchor_date)) & (diag["side"].astype(str) == str(side))]
                g_model = model.loc[(model["anchor_date"].astype(str) == str(anchor_date)) & (model["side"].astype(str) == str(side))]
                g_oracle = oracle.loc[(oracle["anchor_date"].astype(str) == str(anchor_date)) & (oracle["side"].astype(str) == str(side))]
                g_champ_sel = g_champ.loc[g_champ[f"champion_selected_top{topk}"].fillna(False).astype(bool)]
                g_diag_sel = g_diag.loc[g_diag[f"diagnostic_selected_top{topk}"].fillna(False).astype(bool)]
                g_model_sel = g_model.loc[g_model[f"model_selected_top{topk}"].fillna(False).astype(bool)]
                g_oracle_sel = g_oracle.loc[g_oracle[f"oracle_selected_top{topk}"].fillna(False).astype(bool)]
                group_size = int(len(group))
                group_stats.append(
                    {
                        "scope": scope_name,
                        "anchor_date": str(anchor_date),
                        "side": str(side),
                        "topk": int(topk),
                        "group_size": group_size,
                        "top15_available": bool(group["top15_label"].any()),
                        "top20pct_available": bool(group["side_aware_group_top20pct_forward_ret_20d_label"].any()),
                        "champion_selected_count": int(len(g_champ_sel)),
                        "diagnostic_selected_count": int(len(g_diag_sel)),
                        "model_selected_count": int(len(g_model_sel)),
                        "oracle_selected_count": int(len(g_oracle_sel)),
                        "champion_top15_capture_count": int(g_champ_sel["top15_label"].sum()),
                        "diagnostic_top15_capture_count": int(g_diag_sel["top15_label"].sum()),
                        "model_top15_capture_count": int(g_model_sel["top15_label"].sum()),
                        "oracle_top15_capture_count": int(g_oracle_sel["top15_label"].sum()),
                        "champion_top20pct_capture_count": int(g_champ_sel["side_aware_group_top20pct_forward_ret_20d_label"].sum()),
                        "diagnostic_top20pct_capture_count": int(g_diag_sel["side_aware_group_top20pct_forward_ret_20d_label"].sum()),
                        "model_top20pct_capture_count": int(g_model_sel["side_aware_group_top20pct_forward_ret_20d_label"].sum()),
                        "oracle_top20pct_capture_count": int(g_oracle_sel["side_aware_group_top20pct_forward_ret_20d_label"].sum()),
                        "champion_bottom15_contamination_count": int(g_champ_sel["bottom15_label"].sum()),
                        "diagnostic_bottom15_contamination_count": int(g_diag_sel["bottom15_label"].sum()),
                        "model_bottom15_contamination_count": int(g_model_sel["bottom15_label"].sum()),
                        "oracle_bottom15_contamination_count": int(g_oracle_sel["bottom15_label"].sum()),
                        "champion_mean_forward_ret_20d": float(pd.to_numeric(g_champ_sel["forward_ret_20d"], errors="coerce").mean()) if len(g_champ_sel) else None,
                        "diagnostic_mean_forward_ret_20d": float(pd.to_numeric(g_diag_sel["forward_ret_20d"], errors="coerce").mean()) if len(g_diag_sel) else None,
                        "model_mean_forward_ret_20d": float(pd.to_numeric(g_model_sel["forward_ret_20d"], errors="coerce").mean()) if len(g_model_sel) else None,
                        "oracle_mean_forward_ret_20d": float(pd.to_numeric(g_oracle_sel["forward_ret_20d"], errors="coerce").mean()) if len(g_oracle_sel) else None,
                        "champion_mean_path_value_score_v1": float(pd.to_numeric(g_champ_sel["path_value_score_v1"], errors="coerce").mean()) if len(g_champ_sel) else None,
                        "diagnostic_mean_path_value_score_v1": float(pd.to_numeric(g_diag_sel["path_value_score_v1"], errors="coerce").mean()) if len(g_diag_sel) else None,
                        "model_mean_path_value_score_v1": float(pd.to_numeric(g_model_sel["path_value_score_v1"], errors="coerce").mean()) if len(g_model_sel) else None,
                        "oracle_mean_path_value_score_v1": float(pd.to_numeric(g_oracle_sel["path_value_score_v1"], errors="coerce").mean()) if len(g_oracle_sel) else None,
                        "champion_top15_capture_rate": float(g_champ_sel["top15_label"].mean()) if len(g_champ_sel) else None,
                        "diagnostic_top15_capture_rate": float(g_diag_sel["top15_label"].mean()) if len(g_diag_sel) else None,
                        "model_top15_capture_rate": float(g_model_sel["top15_label"].mean()) if len(g_model_sel) else None,
                        "oracle_top15_capture_rate": float(g_oracle_sel["top15_label"].mean()) if len(g_oracle_sel) else None,
                        "champion_top20pct_capture_rate": float(g_champ_sel["side_aware_group_top20pct_forward_ret_20d_label"].mean()) if len(g_champ_sel) else None,
                        "diagnostic_top20pct_capture_rate": float(g_diag_sel["side_aware_group_top20pct_forward_ret_20d_label"].mean()) if len(g_diag_sel) else None,
                        "model_top20pct_capture_rate": float(g_model_sel["side_aware_group_top20pct_forward_ret_20d_label"].mean()) if len(g_model_sel) else None,
                        "oracle_top20pct_capture_rate": float(g_oracle_sel["side_aware_group_top20pct_forward_ret_20d_label"].mean()) if len(g_oracle_sel) else None,
                        "champion_bottom15_contamination_rate": float(g_champ_sel["bottom15_label"].mean()) if len(g_champ_sel) else None,
                        "diagnostic_bottom15_contamination_rate": float(g_diag_sel["bottom15_label"].mean()) if len(g_diag_sel) else None,
                        "model_bottom15_contamination_rate": float(g_model_sel["bottom15_label"].mean()) if len(g_model_sel) else None,
                        "oracle_bottom15_contamination_rate": float(g_oracle_sel["bottom15_label"].mean()) if len(g_oracle_sel) else None,
                        "gap_oracle_to_champion_top15_capture_count": int(g_oracle_sel["top15_label"].sum() - g_champ_sel["top15_label"].sum()),
                        "gap_oracle_to_diagnostic_top15_capture_count": int(g_oracle_sel["top15_label"].sum() - g_diag_sel["top15_label"].sum()),
                        "gap_oracle_to_model_top15_capture_count": int(g_oracle_sel["top15_label"].sum() - g_model_sel["top15_label"].sum()),
                        "gap_oracle_to_champion_target_capture_count": int(g_oracle_sel["side_aware_group_top20pct_forward_ret_20d_label"].sum() - g_champ_sel["side_aware_group_top20pct_forward_ret_20d_label"].sum()),
                        "gap_oracle_to_diagnostic_target_capture_count": int(g_oracle_sel["side_aware_group_top20pct_forward_ret_20d_label"].sum() - g_diag_sel["side_aware_group_top20pct_forward_ret_20d_label"].sum()),
                        "gap_oracle_to_model_target_capture_count": int(g_oracle_sel["side_aware_group_top20pct_forward_ret_20d_label"].sum() - g_model_sel["side_aware_group_top20pct_forward_ret_20d_label"].sum()),
                        "reranking_can_help": bool((g_oracle_sel["top15_label"].sum() > g_champ_sel["top15_label"].sum()) or (g_oracle_sel["side_aware_group_top20pct_forward_ret_20d_label"].sum() > g_champ_sel["side_aware_group_top20pct_forward_ret_20d_label"].sum())),
                        "winners_absent": bool(not group["top15_label"].any()),
                        "topk_breadth_adequate": bool(group_size >= topk),
                        "champion_rank_gap_to_oracle_top15": int(g_champ_sel["top15_label"].sum() - g_oracle_sel["top15_label"].sum()),
                    }
                )
            by_group_rows.extend(group_stats)

        scope_summary: dict[str, Any] = {
            "scope": scope_name,
            "row_count": int(len(scope_frame)),
            "group_count": int(scope_frame.groupby(["anchor_date", "side"], sort=False).ngroups),
            "months": sorted(scope_frame["month_bucket"].astype(str).unique().tolist()),
            "topk": {},
        }
        for topk in TOP_K_VALUES:
            ranked_champion = scope_selections["champion"]
            ranked_diag = scope_selections["diagnostic"]
            ranked_model = scope_selections["model"]
            ranked_oracle = scope_selections["oracle"]
            champ_sel = ranked_champion.loc[ranked_champion[f"champion_selected_top{topk}"].fillna(False).astype(bool)]
            diag_sel = ranked_diag.loc[ranked_diag[f"diagnostic_selected_top{topk}"].fillna(False).astype(bool)]
            model_sel = ranked_model.loc[ranked_model[f"model_selected_top{topk}"].fillna(False).astype(bool)]
            oracle_sel = ranked_oracle.loc[ranked_oracle[f"oracle_selected_top{topk}"].fillna(False).astype(bool)]
            champ_keys = set(map(tuple, champ_sel[["anchor_date", "side", "symbol"]].astype(str).values.tolist()))
            diag_keys = set(map(tuple, diag_sel[["anchor_date", "side", "symbol"]].astype(str).values.tolist()))
            model_keys = set(map(tuple, model_sel[["anchor_date", "side", "symbol"]].astype(str).values.tolist()))
            oracle_keys = set(map(tuple, oracle_sel[["anchor_date", "side", "symbol"]].astype(str).values.tolist()))
            scope_summary["topk"][str(topk)] = {
                "champion": _selection_summary(ranked_champion, prefix="champion", topk=topk, label_col="top15_label"),
                "diagnostic": _selection_summary(ranked_diag, prefix="diagnostic", topk=topk, label_col="top15_label", reference_keys=champ_keys),
                "model": _selection_summary(ranked_model, prefix="model", topk=topk, label_col="top15_label", reference_keys=champ_keys),
                "oracle": _selection_summary(ranked_oracle, prefix="oracle", topk=topk, label_col="top15_label", reference_keys=champ_keys),
                "gaps": {
                    "oracle_minus_champion_mean_forward_ret_20d": None
                    if champ_sel.empty or oracle_sel.empty
                    else float(pd.to_numeric(oracle_sel["forward_ret_20d"], errors="coerce").mean() - pd.to_numeric(champ_sel["forward_ret_20d"], errors="coerce").mean()),
                    "oracle_minus_diagnostic_mean_forward_ret_20d": None
                    if diag_sel.empty or oracle_sel.empty
                    else float(pd.to_numeric(oracle_sel["forward_ret_20d"], errors="coerce").mean() - pd.to_numeric(diag_sel["forward_ret_20d"], errors="coerce").mean()),
                    "oracle_minus_model_mean_forward_ret_20d": None
                    if model_sel.empty or oracle_sel.empty
                    else float(pd.to_numeric(oracle_sel["forward_ret_20d"], errors="coerce").mean() - pd.to_numeric(model_sel["forward_ret_20d"], errors="coerce").mean()),
                    "oracle_minus_champion_mean_path_value_score_v1": None
                    if champ_sel.empty or oracle_sel.empty
                    else float(pd.to_numeric(oracle_sel["path_value_score_v1"], errors="coerce").mean() - pd.to_numeric(champ_sel["path_value_score_v1"], errors="coerce").mean()),
                    "oracle_minus_diagnostic_mean_path_value_score_v1": None
                    if diag_sel.empty or oracle_sel.empty
                    else float(pd.to_numeric(oracle_sel["path_value_score_v1"], errors="coerce").mean() - pd.to_numeric(diag_sel["path_value_score_v1"], errors="coerce").mean()),
                    "oracle_minus_model_mean_path_value_score_v1": None
                    if model_sel.empty or oracle_sel.empty
                    else float(pd.to_numeric(oracle_sel["path_value_score_v1"], errors="coerce").mean() - pd.to_numeric(model_sel["path_value_score_v1"], errors="coerce").mean()),
                    "oracle_minus_champion_top15_capture_rate": None
                    if champ_sel.empty or oracle_sel.empty
                    else float(oracle_sel["top15_label"].mean() - champ_sel["top15_label"].mean()),
                    "oracle_minus_diagnostic_top15_capture_rate": None
                    if diag_sel.empty or oracle_sel.empty
                    else float(oracle_sel["top15_label"].mean() - diag_sel["top15_label"].mean()),
                    "oracle_minus_model_top15_capture_rate": None
                    if model_sel.empty or oracle_sel.empty
                    else float(oracle_sel["top15_label"].mean() - model_sel["top15_label"].mean()),
                    "oracle_minus_champion_top20pct_capture_rate": None
                    if champ_sel.empty or oracle_sel.empty
                    else float(oracle_sel["side_aware_group_top20pct_forward_ret_20d_label"].mean() - champ_sel["side_aware_group_top20pct_forward_ret_20d_label"].mean()),
                    "oracle_minus_diagnostic_top20pct_capture_rate": None
                    if diag_sel.empty or oracle_sel.empty
                    else float(oracle_sel["side_aware_group_top20pct_forward_ret_20d_label"].mean() - diag_sel["side_aware_group_top20pct_forward_ret_20d_label"].mean()),
                    "oracle_minus_model_top20pct_capture_rate": None
                    if model_sel.empty or oracle_sel.empty
                    else float(oracle_sel["side_aware_group_top20pct_forward_ret_20d_label"].mean() - model_sel["side_aware_group_top20pct_forward_ret_20d_label"].mean()),
                    "oracle_minus_champion_bottom15_contamination_rate": None
                    if champ_sel.empty or oracle_sel.empty
                    else float(oracle_sel["bottom15_label"].mean() - champ_sel["bottom15_label"].mean()),
                    "oracle_minus_diagnostic_bottom15_contamination_rate": None
                    if diag_sel.empty or oracle_sel.empty
                    else float(oracle_sel["bottom15_label"].mean() - diag_sel["bottom15_label"].mean()),
                    "oracle_minus_model_bottom15_contamination_rate": None
                    if model_sel.empty or oracle_sel.empty
                    else float(oracle_sel["bottom15_label"].mean() - model_sel["bottom15_label"].mean()),
                },
                "membership_changes_vs_champion": {
                    "diagnostic_changed_count": int(len(diag_keys ^ champ_keys)),
                    "model_changed_count": int(len(model_keys ^ champ_keys)),
                    "oracle_changed_count": int(len(oracle_keys ^ champ_keys)),
                    "diagnostic_overlap_ratio": float(len(diag_keys & champ_keys) / len(diag_keys | champ_keys)) if diag_keys or champ_keys else None,
                    "model_overlap_ratio": float(len(model_keys & champ_keys) / len(model_keys | champ_keys)) if model_keys or champ_keys else None,
                    "oracle_overlap_ratio": float(len(oracle_keys & champ_keys) / len(oracle_keys | champ_keys)) if oracle_keys or champ_keys else None,
                },
            }
        return scope_summary, pd.DataFrame(by_group_rows)

    all_scope, group_rows = _per_scope("all_surface", frame, selections)
    validation_test_scope, _ = _per_scope("validation_test_only", validation_test_frame, selections_validation_test)
    audit = {
        "schema_version": ORACLE_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "scope_order": ["all_surface", "validation_test_only"],
        "all_surface": all_scope,
        "validation_test_only": validation_test_scope,
        "breadth_headroom": {
            "top5": {
                "oracle_top15_capture_rate": all_scope["topk"]["5"]["oracle"]["top15_capture_rate"],
                "champion_top15_capture_rate": all_scope["topk"]["5"]["champion"]["top15_capture_rate"],
                "diagnostic_top15_capture_rate": all_scope["topk"]["5"]["diagnostic"]["top15_capture_rate"],
                "model_top15_capture_rate": all_scope["topk"]["5"]["model"]["top15_capture_rate"],
                "oracle_bottom15_contamination_rate": all_scope["topk"]["5"]["oracle"]["bottom15_contamination_rate"],
            },
            "top10": {
                "oracle_top15_capture_rate": all_scope["topk"]["10"]["oracle"]["top15_capture_rate"],
                "champion_top15_capture_rate": all_scope["topk"]["10"]["champion"]["top15_capture_rate"],
                "diagnostic_top15_capture_rate": all_scope["topk"]["10"]["diagnostic"]["top15_capture_rate"],
                "model_top15_capture_rate": all_scope["topk"]["10"]["model"]["top15_capture_rate"],
                "oracle_bottom15_contamination_rate": all_scope["topk"]["10"]["oracle"]["bottom15_contamination_rate"],
            },
            "top20": {
                "oracle_top15_capture_rate": all_scope["topk"]["20"]["oracle"]["top15_capture_rate"],
                "champion_top15_capture_rate": all_scope["topk"]["20"]["champion"]["top15_capture_rate"],
                "diagnostic_top15_capture_rate": all_scope["topk"]["20"]["diagnostic"]["top15_capture_rate"],
                "model_top15_capture_rate": all_scope["topk"]["20"]["model"]["top15_capture_rate"],
                "oracle_bottom15_contamination_rate": all_scope["topk"]["20"]["oracle"]["bottom15_contamination_rate"],
            },
        },
    }
    return audit, group_rows


def _build_admission_failure_audit(frame: pd.DataFrame) -> tuple[dict[str, Any], pd.DataFrame]:
    ranked_champion = _rank_within_groups(frame, sort_cols=["reference_score", "rank", "symbol", "candidate_idx"], ascending=[False, True, True, True], prefix="champion")
    ranked_diag = _rank_within_groups(frame, sort_cols=["diagnostic_score", "rank", "symbol", "candidate_idx"], ascending=[False, True, True, True], prefix="diagnostic")
    ranked_model = _rank_within_groups(frame, sort_cols=["model_score", "rank", "symbol", "candidate_idx"], ascending=[False, True, True, True], prefix="model")
    ranked_oracle = _rank_within_groups(frame, sort_cols=["forward_ret_20d", "path_value_score_v1", "mae_20d", "candidate_idx", "symbol"], ascending=[False, False, True, True, True], prefix="oracle")
    rows: list[dict[str, Any]] = []
    example_rows: list[dict[str, Any]] = []
    label_specs = {
        "top15_label": frame["top15_label"].fillna(False).astype(bool),
        "top20pct_label": frame["side_aware_group_top20pct_forward_ret_20d_label"].fillna(False).astype(bool),
        "return_threshold_gt_0_label": pd.to_numeric(frame["forward_ret_20d"], errors="coerce") > 0,
        "path_quality_threshold_gt_0_label": pd.to_numeric(frame["path_value_score_v1"], errors="coerce") > 0,
    }

    for topk in TOP_K_VALUES:
        for label_name, label_mask in label_specs.items():
            for (anchor_date, side), group in frame.groupby(["anchor_date", "side"], sort=False):
                group_mask = group.index
                champion_group = ranked_champion.loc[group_mask]
                diag_group = ranked_diag.loc[group_mask]
                model_group = ranked_model.loc[group_mask]
                oracle_group = ranked_oracle.loc[group_mask]
                champ_sel = champion_group.loc[champion_group[f"champion_selected_top{topk}"].fillna(False).astype(bool)]
                diag_sel = diag_group.loc[diag_group[f"diagnostic_selected_top{topk}"].fillna(False).astype(bool)]
                model_sel = model_group.loc[model_group[f"model_selected_top{topk}"].fillna(False).astype(bool)]
                oracle_sel = oracle_group.loc[oracle_group[f"oracle_selected_top{topk}"].fillna(False).astype(bool)]
                winners = group.loc[label_mask.loc[group.index]]
                champion_winner_ranks = champion_group.loc[label_mask.loc[group.index], "champion_rank"]
                winner_rank_values = pd.to_numeric(champion_winner_ranks, errors="coerce").dropna()
                cutoff_rank = int(topk)
                near_cutoff = int(((winner_rank_values > cutoff_rank) & (winner_rank_values <= cutoff_rank + 3)).sum())
                far_below = int((winner_rank_values > cutoff_rank + 3).sum())
                admitted = int((winner_rank_values <= cutoff_rank).sum())
                cutoff_score = None
                if len(champion_group) >= topk:
                    cutoff_score = float(champion_group.loc[champion_group["champion_rank"] == cutoff_rank, "reference_score"].iloc[0])
                score_gaps = []
                if cutoff_score is not None and not winners.empty:
                    score_gaps = [float(cutoff_score - float(v)) for v in pd.to_numeric(champion_group.loc[label_mask.loc[group.index], "reference_score"], errors="coerce").dropna().tolist()]
                rows.append(
                    {
                        "anchor_date": str(anchor_date),
                        "side": str(side),
                        "topk": int(topk),
                        "winner_label": label_name,
                        "group_size": int(len(group)),
                        "winner_count_in_pool": int(label_mask.loc[group.index].sum()),
                        "champion_selected_winner_count": int(champ_sel[label_mask.loc[group.index]].shape[0]) if len(champ_sel) else 0,
                        "diagnostic_selected_winner_count": int(diag_sel[label_mask.loc[group.index]].shape[0]) if len(diag_sel) else 0,
                        "model_selected_winner_count": int(model_sel[label_mask.loc[group.index]].shape[0]) if len(model_sel) else 0,
                        "oracle_selected_winner_count": int(oracle_sel[label_mask.loc[group.index]].shape[0]) if len(oracle_sel) else 0,
                        "winner_rank_best": int(winner_rank_values.min()) if len(winner_rank_values) else None,
                        "winner_rank_median": float(winner_rank_values.median()) if len(winner_rank_values) else None,
                        "winner_rank_p90": float(winner_rank_values.quantile(0.9)) if len(winner_rank_values) else None,
                        "winners_just_below_cutoff_count": near_cutoff,
                        "winners_far_below_cutoff_count": far_below,
                        "winners_admitted_by_champion_count": admitted,
                        "winner_absent": bool(len(winner_rank_values) == 0),
                        "champion_cutoff_score": cutoff_score,
                        "mean_score_gap_to_cutoff": float(pd.Series(score_gaps).mean()) if score_gaps else None,
                        "median_score_gap_to_cutoff": float(pd.Series(score_gaps).median()) if score_gaps else None,
                        "reranker_can_fix": bool((near_cutoff > 0) or (admitted < int(label_mask.loc[group.index].sum()))),
                    }
                )
                if len(winner_rank_values):
                    example_rows.append(
                        {
                            "anchor_date": str(anchor_date),
                            "side": str(side),
                            "topk": int(topk),
                            "winner_label": label_name,
                            "winner_rank_min": int(winner_rank_values.min()),
                            "winner_rank_median": float(winner_rank_values.median()),
                            "winner_rank_max": int(winner_rank_values.max()),
                            "champion_selected_winner_count": int(champ_sel[label_mask.loc[group.index]].shape[0]) if len(champ_sel) else 0,
                            "model_selected_winner_count": int(model_sel[label_mask.loc[group.index]].shape[0]) if len(model_sel) else 0,
                            "oracle_selected_winner_count": int(oracle_sel[label_mask.loc[group.index]].shape[0]) if len(oracle_sel) else 0,
                            "champion_cutoff_score": cutoff_score,
                            "mean_score_gap_to_cutoff": float(pd.Series(score_gaps).mean()) if score_gaps else None,
                        }
                    )
    audit = {
        "schema_version": ADMISSION_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "group_count": int(frame.groupby(["anchor_date", "side"], sort=False).ngroups),
        "row_count": int(len(frame)),
        "by_group": rows,
        "label_coverage_context": {
            "top15_positive_total": int(frame["top15_label"].sum()),
            "top20pct_positive_total": int(frame["side_aware_group_top20pct_forward_ret_20d_label"].sum()),
            "return_threshold_positive_total": int((pd.to_numeric(frame["forward_ret_20d"], errors="coerce") > 0).sum()),
            "path_threshold_positive_total": int((pd.to_numeric(frame["path_value_score_v1"], errors="coerce") > 0).sum()),
        },
        "summary_by_topk": {
            str(topk): {
                "top15_zero_group_count": int(sum(row["topk"] == topk and row["winner_label"] == "top15_label" and row["winner_absent"] for row in rows)),
                "top20pct_zero_group_count": int(sum(row["topk"] == topk and row["winner_label"] == "top20pct_label" and row["winner_absent"] for row in rows)),
                "return_threshold_zero_group_count": int(sum(row["topk"] == topk and row["winner_label"] == "return_threshold_gt_0_label" and row["winner_absent"] for row in rows)),
                "path_threshold_zero_group_count": int(sum(row["topk"] == topk and row["winner_label"] == "path_quality_threshold_gt_0_label" and row["winner_absent"] for row in rows)),
                "top15_just_below_cutoff_count": int(sum(row["topk"] == topk and row["winner_label"] == "top15_label" and row["winners_just_below_cutoff_count"] > 0 for row in rows)),
                "top15_far_below_cutoff_count": int(sum(row["topk"] == topk and row["winner_label"] == "top15_label" and row["winners_far_below_cutoff_count"] > 0 for row in rows)),
            }
            for topk in TOP_K_VALUES
        },
    }
    return audit, pd.DataFrame(example_rows)


def _build_redesign_options(breadth_audit: dict[str, Any], winner_audit: dict[str, Any], oracle_audit: dict[str, Any]) -> dict[str, Any]:
    top20_thin = breadth_audit["overall_thin_groups"]["top20"]
    top10_thin = breadth_audit["overall_thin_groups"]["top10"]
    top15_zero_groups = winner_audit["group_count"] - winner_audit["profiles"]["top15_label"]["group_positive_count"]
    options = [
        {
            "axis": "high_recall_candidate_pool_v1",
            "expected_benefit": "highest; widen pool before reranking so more future winners are admitted and buried winners can surface",
            "risk": "may increase false positives and require a later admission-cap pass",
            "data_requirements": "broader candidate recall surface by anchor and side",
            "no_lookahead_compatibility": True,
            "implementation_complexity": "medium",
            "evaluation": "rerun the same frozen-target gate and measure oracle headroom shrinkage, top15 capture, and bottom15 contamination",
            "priority": 1,
        },
        {
            "axis": "long_side_candidate_generator_v1",
            "expected_benefit": "moderate; could improve the stronger long side if thinness is mostly long-side driven",
            "risk": "does not directly fix short-side sparsity or cross-side pool breadth",
            "data_requirements": "long-side recall analysis and long-only candidate traces",
            "no_lookahead_compatibility": True,
            "implementation_complexity": "medium",
            "evaluation": "compare long-side winner inclusion, top5/top10 capture, and oracle gap on long-only slices",
            "priority": 3,
        },
        {
            "axis": "short_side_data_accumulation",
            "expected_benefit": "moderate; could make short-side labels and pool more learnable over time",
            "risk": "slow; does not solve current overall topK thinness immediately",
            "data_requirements": "more post-window short-side observations and a stable short-side label contract",
            "no_lookahead_compatibility": True,
            "implementation_complexity": "low",
            "evaluation": "track whether short-side positive coverage and oracle gap improve over time",
            "priority": 4,
        },
        {
            "axis": "side_aware_candidate_admission_caps",
            "expected_benefit": "moderate; can prevent brittle overfitting and control contamination after recall is widened",
            "risk": "premature caps can suppress the very winners we need to expose",
            "data_requirements": "group-level breadth statistics and side-specific recall diagnostics",
            "no_lookahead_compatibility": True,
            "implementation_complexity": "medium",
            "evaluation": "compare cap-induced changes in winner inclusion, zero-pass groups, and bottom15 contamination",
            "priority": 2,
        },
        {
            "axis": "stop_model_and_candidate_generation_line",
            "expected_benefit": "high if no candidate-generation axis can improve oracle headroom or group breadth",
            "risk": "halts this line entirely",
            "data_requirements": "none",
            "no_lookahead_compatibility": True,
            "implementation_complexity": "low",
            "evaluation": "use only if breadth and oracle headroom fail to show actionable gaps",
            "priority": 99,
        },
    ]
    return {
        "schema_version": OPTIONS_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "primary_evidence": {
            "top20_thin_groups": int(top20_thin),
            "top10_thin_groups": int(top10_thin),
            "top15_zero_groups": int(winner_audit["group_count"] - winner_audit["profiles"]["top15_label"]["group_positive_count"]),
            "oracle_headroom_top5": oracle_audit["breadth_headroom"]["top5"]["oracle_top15_capture_rate"] - oracle_audit["breadth_headroom"]["top5"]["champion_top15_capture_rate"],
            "oracle_headroom_top10": oracle_audit["breadth_headroom"]["top10"]["oracle_top15_capture_rate"] - oracle_audit["breadth_headroom"]["top10"]["champion_top15_capture_rate"],
            "oracle_headroom_top20": oracle_audit["breadth_headroom"]["top20"]["oracle_top15_capture_rate"] - oracle_audit["breadth_headroom"]["top20"]["champion_top15_capture_rate"],
            "short_side_under_supply": bool(winner_audit["side_summary"].get("short", {}).get("mean_group_size", 0) < 5),
        },
        "options": options,
    }


def _recommend_axis(breadth_audit: dict[str, Any], winner_audit: dict[str, Any], oracle_audit: dict[str, Any]) -> tuple[str, dict[str, Any], str]:
    top20_thin = breadth_audit["overall_thin_groups"]["top20"]
    top10_thin = breadth_audit["overall_thin_groups"]["top10"]
    top5_thin = breadth_audit["overall_thin_groups"]["top5"]
    top15_zero = winner_audit["group_count"] - winner_audit["profiles"]["top15_label"]["group_positive_count"]
    oracle_gain_top10 = oracle_audit["breadth_headroom"]["top10"]["oracle_top15_capture_rate"] - oracle_audit["breadth_headroom"]["top10"]["champion_top15_capture_rate"]
    oracle_gain_top20 = oracle_audit["breadth_headroom"]["top20"]["oracle_top15_capture_rate"] - oracle_audit["breadth_headroom"]["top20"]["champion_top15_capture_rate"]
    short_under = winner_audit["side_summary"].get("short", {}).get("mean_group_size", 0) < 5

    if top20_thin >= 20 and top10_thin >= 15 and top15_zero >= 20 and oracle_gain_top10 > 0 and oracle_gain_top20 > 0:
        return (
            "high_recall_candidate_pool_v1",
            {
                "reason": "candidate_pool_thinness_and_oracle_headroom_support_higher_recall_before_more_model_shopping",
                "evidence": {
                    "top5_thin_groups": top5_thin,
                    "top10_thin_groups": top10_thin,
                    "top20_thin_groups": top20_thin,
                    "top15_zero_group_count": top15_zero,
                    "oracle_gain_top10": oracle_gain_top10,
                    "oracle_gain_top20": oracle_gain_top20,
                    "short_side_under_supply": bool(short_under),
                },
            },
            "ready_to_design_high_recall_candidate_pool",
        )
    if short_under and top15_zero >= 20:
        return (
            "short_side_data_accumulation",
            {
                "reason": "short_side_is_structurally_sparse_and_candidate_pool_is_not_yet_broad_enough_for_robust_short_side_learning",
                "evidence": {
                    "top15_zero_group_count": top15_zero,
                    "short_side_under_supply": bool(short_under),
                },
            },
            "ready_to_accumulate_short_side_data",
        )
    if top5_thin > 0 or top10_thin > 0 or top20_thin > 0:
        return (
            "side_aware_candidate_admission_caps",
            {
                "reason": "admission_capping_can_help_after_recall_is_widened_but_current_evidence_prioritizes_breadth_first",
                "evidence": {
                    "top5_thin_groups": top5_thin,
                    "top10_thin_groups": top10_thin,
                    "top20_thin_groups": top20_thin,
                },
            },
            "ready_to_design_side_aware_admission_caps",
        )
    return (
        "stop_model_and_candidate_generation_line",
        {
            "reason": "candidate_generation_and_oracle_headroom_do_not_show_actionable_gap",
            "evidence": {
                "top5_thin_groups": top5_thin,
                "top10_thin_groups": top10_thin,
                "top20_thin_groups": top20_thin,
            },
        },
        "stop_candidate_generation_line",
    )


def run(*, output_root: Path = DEFAULT_OUTPUT_ROOT, jobs_requested: int = 2, session_id: str | None = None) -> dict[str, Any]:
    session = session_id or _session_id()
    out = output_root / session
    out.mkdir(parents=True, exist_ok=False)

    side_frame = _load_frame(SIDE_AWARE_SESSION / "side_aware_top20pct_prediction_rows.parquet", require_mae=False)
    accumulated_frame = _load_frame(ACCUMULATED_SESSION / "accumulated_forward_prediction_rows.parquet")
    label_sensitivity = pd.read_parquet(_ensure_exists(LABEL_COVERAGE_SESSION / "label_definition_sensitivity.parquet", "label_definition_sensitivity.parquet"))
    candidate_oracle_by_group = pd.read_parquet(_ensure_exists(LABEL_COVERAGE_SESSION / "candidate_pool_oracle_by_group.parquet", "candidate_pool_oracle_by_group.parquet"))
    side_variant = _load_json(SIDE_AWARE_SESSION / "side_aware_top20pct_variant_pool_comparison.json")
    side_failure = _load_json(SIDE_AWARE_SESSION / "side_aware_top20pct_failure_mode_audit.json")
    side_lineage = _load_json(SIDE_AWARE_SESSION / "side_aware_top20pct_lineage_comparison.json")
    side_decision = _load_json(SIDE_AWARE_SESSION / "side_aware_top20pct_label_modeling_feasibility_v1_decision.json")
    acc_variant = _load_json(ACCUMULATED_SESSION / "accumulated_forward_variant_pool_comparison.json")
    acc_decision = _load_json(ACCUMULATED_SESSION / "shadow_reranker_accumulated_forward_validation_v1_decision.json")

    side_subset_cols = [
        "anchor_date",
        "side",
        "symbol",
        "candidate_idx",
        "split",
        "reference_score",
        "diagnostic_score",
        "model_score",
        "side_aware_group_top20pct_forward_ret_20d_label",
        "label_rank",
    ]
    side_subset = side_frame[[c for c in side_subset_cols if c in side_frame.columns]].copy()
    side_subset = side_subset.rename(
        columns={
            "reference_score": "side_reference_score",
            "diagnostic_score": "side_diagnostic_score",
            "model_score": "side_model_score",
            "split": "side_split",
            "side_aware_group_top20pct_forward_ret_20d_label": "side_target_label",
            "label_rank": "side_label_rank",
        }
    )
    analysis_frame = accumulated_frame.merge(
        side_subset,
        on=["anchor_date", "side", "symbol", "candidate_idx"],
        how="left",
        suffixes=("", "_sideaware"),
    )
    if "side_split" in analysis_frame.columns:
        analysis_frame["split"] = analysis_frame["side_split"]
    elif "split" not in analysis_frame.columns:
        analysis_frame["split"] = "all"
    if "month_bucket" not in analysis_frame.columns:
        analysis_frame["month_bucket"] = "unknown"
    if "side_reference_score" in analysis_frame.columns:
        analysis_frame["reference_score"] = pd.to_numeric(analysis_frame["side_reference_score"], errors="coerce")
    if "side_diagnostic_score" in analysis_frame.columns:
        analysis_frame["diagnostic_score"] = pd.to_numeric(analysis_frame["side_diagnostic_score"], errors="coerce")
    if "side_model_score" in analysis_frame.columns:
        analysis_frame["model_score"] = pd.to_numeric(analysis_frame["side_model_score"], errors="coerce")
    if "side_target_label" in analysis_frame.columns:
        analysis_frame["side_target_label"] = analysis_frame["side_target_label"].fillna(False).astype(bool)
        analysis_frame["side_aware_group_top20pct_forward_ret_20d_label"] = analysis_frame["side_target_label"]
    if "side_label_rank" in analysis_frame.columns:
        analysis_frame["side_label_rank"] = pd.to_numeric(analysis_frame["side_label_rank"], errors="coerce")

    breadth_audit, breadth_group_rows = _build_candidate_pool_breadth_audit(side_frame, accumulated_frame)
    winner_audit, winner_group_rows = _build_winner_inclusion_audit(side_frame, label_sensitivity)
    oracle_audit, oracle_group_rows = _build_oracle_headroom_audit(analysis_frame, analysis_frame.loc[analysis_frame["split"].ne("train")].copy())
    admission_audit, admission_examples = _build_admission_failure_audit(analysis_frame)
    recommendation_axis, recommendation_payload, final_decision = _recommend_axis(breadth_audit, winner_audit, oracle_audit)
    redesign_options = _build_redesign_options(breadth_audit, winner_audit, oracle_audit)
    breadth_audit["oracle_reference"] = {
        "group_count": int(candidate_oracle_by_group.shape[0] // len(TOP_K_VALUES)),
        "winners_absent_group_count": int(candidate_oracle_by_group.loc[candidate_oracle_by_group["k"] == 5, "winners_absent"].sum()) if "winners_absent" in candidate_oracle_by_group.columns else None,
        "top15_groups": int(candidate_oracle_by_group.loc[candidate_oracle_by_group["k"] == 5, "top15_candidate_count"].gt(0).sum()) if "top15_candidate_count" in candidate_oracle_by_group.columns else None,
        "summary_by_k": {
            str(k): {
                "oracle_top15_capture_rate": float(candidate_oracle_by_group.loc[candidate_oracle_by_group["k"] == k, "oracle_top15_capture_rate"].mean()) if "oracle_top15_capture_rate" in candidate_oracle_by_group.columns else None,
                "champion_top15_capture_rate": float(candidate_oracle_by_group.loc[candidate_oracle_by_group["k"] == k, "champion_top15_capture_rate"].mean()) if "champion_top15_capture_rate" in candidate_oracle_by_group.columns else None,
                "challenger_top15_capture_rate": float(candidate_oracle_by_group.loc[candidate_oracle_by_group["k"] == k, "challenger_top15_capture_rate"].mean()) if "challenger_top15_capture_rate" in candidate_oracle_by_group.columns else None,
                "groups_where_reranking_can_help": int(candidate_oracle_by_group.loc[candidate_oracle_by_group["k"] == k, "reranking_can_help"].sum()) if "reranking_can_help" in candidate_oracle_by_group.columns else None,
            }
            for k in TOP_K_VALUES
        },
    }

    run_manifest = {
        "schema_version": MANIFEST_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "session_id": session,
        "task": "TRADEX candidate-generation breadth / quality redesign audit v1",
        "boundary": "TRADEX-only",
        "non_scope": ["MeeMee", "production ranking", "publish / promotion", "research_inventory.json", "model training", "label tuning", "challenger generation"],
        "source_roots": {
            "side_aware_label_feasibility": str(SIDE_AWARE_SESSION),
            "candidate_generation_label_coverage_audit": str(LABEL_COVERAGE_SESSION),
            "accumulated_forward_validation": str(ACCUMULATED_SESSION),
        },
        "jobs_requested": int(jobs_requested),
        "jobs_supported": 1,
        "authoritative_sources": {
            "side_aware_variant_pool_comparison": str(SIDE_AWARE_SESSION / "side_aware_top20pct_variant_pool_comparison.json"),
            "side_aware_failure_mode": str(SIDE_AWARE_SESSION / "side_aware_top20pct_failure_mode_audit.json"),
            "side_aware_lineage": str(SIDE_AWARE_SESSION / "side_aware_top20pct_lineage_comparison.json"),
            "candidate_pool_winner_availability": str(LABEL_COVERAGE_SESSION / "candidate_pool_winner_availability_audit.json"),
            "candidate_generation_ceiling": str(LABEL_COVERAGE_SESSION / "candidate_generation_ceiling_audit_v2.json"),
            "candidate_pool_oracle_by_group": str(LABEL_COVERAGE_SESSION / "candidate_pool_oracle_by_group.parquet"),
            "label_definition_sensitivity": str(LABEL_COVERAGE_SESSION / "label_definition_sensitivity.parquet"),
            "accumulated_forward_variant_pool_comparison": str(ACCUMULATED_SESSION / "accumulated_forward_variant_pool_comparison.json"),
            "accumulated_forward_prediction_rows": str(ACCUMULATED_SESSION / "accumulated_forward_prediction_rows.parquet"),
        },
    }
    input_resolution = {
        "schema_version": INPUT_RESOLUTION_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "resolved_sources": [
            {
                "role": "side_aware_feasibility_surface",
                "path": str(SIDE_AWARE_SESSION),
                "files_used": [
                    "side_aware_top20pct_prediction_rows.parquet",
                    "side_aware_top20pct_variant_pool_comparison.json",
                    "side_aware_top20pct_failure_mode_audit.json",
                    "side_aware_top20pct_lineage_comparison.json",
                    "side_aware_top20pct_label_modeling_feasibility_v1_decision.json",
                ],
            },
            {
                "role": "label_coverage_audit",
                "path": str(LABEL_COVERAGE_SESSION),
                "files_used": [
                    "candidate_pool_winner_availability_audit.json",
                    "candidate_generation_ceiling_audit_v2.json",
                    "candidate_pool_oracle_by_group.parquet",
                    "label_definition_sensitivity.parquet",
                ],
            },
            {
                "role": "accumulated_validation_surface",
                "path": str(ACCUMULATED_SESSION),
                "files_used": [
                    "accumulated_forward_prediction_rows.parquet",
                    "accumulated_forward_variant_pool_comparison.json",
                    "shadow_reranker_accumulated_forward_validation_v1_decision.json",
                ],
            },
        ],
        "authoritative_decision_source": str(SIDE_AWARE_SESSION / "side_aware_top20pct_label_modeling_feasibility_v1_decision.json"),
    }
    admission_audit["primary_recommendation"] = recommendation_axis
    admission_audit["recommendation_payload"] = recommendation_payload

    _write_json(out / "run_manifest.json", run_manifest)
    _write_json(out / "input_resolution.json", input_resolution)
    _write_json(out / "candidate_pool_breadth_audit.json", breadth_audit)
    _write_json(out / "winner_inclusion_audit.json", winner_audit)
    _write_json(out / "candidate_pool_oracle_headroom_audit.json", oracle_audit)
    _write_json(out / "candidate_admission_failure_audit.json", admission_audit)
    _write_json(out / "candidate_generation_redesign_options.json", redesign_options)
    _write_json(
        out / "candidate_generation_redesign_recommendation.json",
        {
            "schema_version": RECOMMENDATION_SCHEMA_VERSION,
            "generated_at_utc": _utc_now(),
            "recommended_axis": recommendation_axis,
            "recommendation_payload": recommendation_payload,
            "authoritative_result": str(out / "candidate_pool_oracle_headroom_audit.json"),
        },
    )
    _write_json(
        out / "candidate_generation_redesign_audit_v1_decision.json",
        {
            "schema_version": DECISION_SCHEMA_VERSION,
            "generated_at_utc": _utc_now(),
            "decision": final_decision,
            "reason": recommendation_payload["reason"],
            "authoritative_result": str(out / "candidate_generation_redesign_recommendation.json"),
        },
    )
    _write_json(
        out / "_ARTIFACT_COMPLETE.json",
        {
            "schema_version": ARTIFACT_COMPLETE_SCHEMA_VERSION,
            "generated_at_utc": _utc_now(),
            "session_id": session,
            "required_artifacts": [
                "run_manifest.json",
                "input_resolution.json",
                "candidate_pool_breadth_audit.json",
                "winner_inclusion_audit.json",
                "candidate_pool_oracle_headroom_audit.json",
                "candidate_admission_failure_audit.json",
                "candidate_generation_redesign_options.json",
                "candidate_generation_redesign_recommendation.json",
                "candidate_generation_redesign_audit_v1_decision.json",
                "_ARTIFACT_COMPLETE.json",
            ],
            "status": "complete",
        },
    )

    breadth_group_rows.to_parquet(out / "candidate_pool_breadth_by_group.parquet", index=False)
    winner_group_rows.to_parquet(out / "winner_rank_positions.parquet", index=False)
    oracle_group_rows.to_parquet(out / "oracle_headroom_by_group.parquet", index=False)
    admission_examples.to_parquet(out / "admission_failure_examples.parquet", index=False)

    return {
        "session_id": session,
        "output_root": str(out),
        "decision": final_decision,
        "recommended_axis": recommendation_axis,
        "breadth": breadth_audit,
        "winner_inclusion": winner_audit,
        "oracle_headroom": oracle_audit,
        "admission_failure": admission_audit,
    }


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Research whether candidate generation breadth/quality must be redesigned.")
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--jobs", type=int, default=2)
    parser.add_argument("--session-id", default=None)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    result = run(output_root=args.output_root, jobs_requested=args.jobs, session_id=args.session_id)
    print(
        json.dumps(
            _json_ready(
                {
                    "session_id": result["session_id"],
                    "output_root": result["output_root"],
                    "decision": result["decision"],
                    "recommended_axis": result["recommended_axis"],
                    "breadth_top_thin_groups": result["breadth"]["overall_thin_groups"],
                    "winner_top15_groups": result["winner_inclusion"]["profiles"]["top15_label"]["group_positive_count"],
                }
            ),
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
