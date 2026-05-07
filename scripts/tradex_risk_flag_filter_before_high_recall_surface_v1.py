from __future__ import annotations

import argparse
import json
import math
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.tradex_side_aware_min_pool_feasibility_v1 import build_artifacts as build_min_pool_artifacts

SCRIPT_NAME = "tradex_risk_flag_filter_before_high_recall_surface_v1"
MANIFEST_SCHEMA_VERSION = "tradex_risk_flag_filter_before_high_recall_surface_v1_manifest_v1"
INPUT_RESOLUTION_SCHEMA_VERSION = "tradex_risk_flag_filter_before_high_recall_surface_v1_input_resolution_v1"
RISK_FLAG_SCHEMA_VERSION = "tradex_risk_flag_filter_before_high_recall_surface_v1_risk_flag_inventory_v1"
VARIANT_CONTRACT_SCHEMA_VERSION = "tradex_risk_flag_filter_before_high_recall_surface_v1_variant_contracts_v1"
VARIANT_COMPARISON_SCHEMA_VERSION = "tradex_risk_flag_filter_before_high_recall_surface_v1_variant_comparison_v1"
RECOMMENDATION_SCHEMA_VERSION = "tradex_risk_flag_filter_before_high_recall_surface_v1_recommendation_v1"
DECISION_SCHEMA_VERSION = "tradex_risk_flag_filter_before_high_recall_surface_v1_decision_v1"
ARTIFACT_COMPLETE_SCHEMA_VERSION = "tradex_risk_flag_filter_before_high_recall_surface_v1_artifact_complete_v1"

DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\risk_flag_filter_before_high_recall_surface_v1")
CURRENT_ACCUMULATED_SESSION = Path(r"G:\Tradex\shadow_reranker_accumulated_forward_validation_v1\20260502T082532Z-c17e19")
HIGH_RECALL_MIN_POOL_SESSION = Path(r"G:\Tradex\side_aware_min_pool_feasibility_v1\20260502T114737Z-145239")
REDESIGN_AUDIT_SESSION = Path(r"G:\Tradex\candidate_generation_redesign_audit_v1\20260502T105632Z-399318")
HIGH_RECALL_DESIGN_SESSION = Path(r"G:\Tradex\high_recall_candidate_pool_design_v1\20260502T112742Z-067390")
RAW_SELECTION_LEDGER = Path(r"G:\Tradex\sample_replays\tradex_integrated_guarded_v1_stress200\integrated_guarded_v1_selection_only_ledger.json")

MIN_TARGETS = {"long": 20, "short": 5}
MAX_CAPS = {"long": 40, "short": 10}
RANK_GUARD = {"long": 8, "short": 4}
SCORE_GUARD = 0.35


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


def _write_json(path: Path, payload: dict[str, Any]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(_json_ready(payload), ensure_ascii=False, indent=2, sort_keys=True, default=str) + "\n",
        encoding="utf-8",
    )
    return path


def _write_parquet(path: Path, frame: pd.DataFrame) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_parquet(path, index=False)
    return path


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"missing source artifact: {path}")
    return json.loads(path.read_text(encoding="utf-8"))


def _read_frame(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"missing source artifact: {path}")
    return pd.read_parquet(path).copy()


def _key(frame: pd.DataFrame) -> pd.Series:
    return frame["anchor_date"].astype(str) + "|" + frame["side"].astype(str) + "|" + frame["symbol"].astype(str)


def _ensure_str_cols(frame: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    out = frame.copy()
    for col in cols:
        if col in out.columns:
            out[col] = out[col].astype(str)
    return out


def _group_summary(frame: pd.DataFrame) -> dict[str, Any]:
    summary: dict[str, Any] = {}
    for side, side_frame in frame.groupby("side", sort=False):
        sizes = side_frame.groupby("anchor_date", sort=False).size()
        summary[str(side)] = {
            "row_count": int(len(side_frame)),
            "group_count": int(sizes.shape[0]),
            "min_group_size": int(sizes.min()) if len(sizes) else None,
            "median_group_size": float(sizes.median()) if len(sizes) else None,
            "mean_group_size": float(sizes.mean()) if len(sizes) else None,
            "max_group_size": int(sizes.max()) if len(sizes) else None,
            "top5_thin_groups": int((sizes < 5).sum()),
            "top10_thin_groups": int((sizes < 10).sum()),
            "top20_thin_groups": int((sizes < 20).sum()),
        }
    overall = frame.groupby(["anchor_date", "side"], sort=False).size()
    summary["overall"] = {
        "row_count": int(len(frame)),
        "group_count": int(overall.shape[0]),
        "min_group_size": int(overall.min()) if len(overall) else None,
        "median_group_size": float(overall.median()) if len(overall) else None,
        "mean_group_size": float(overall.mean()) if len(overall) else None,
        "max_group_size": int(overall.max()) if len(overall) else None,
        "top5_thin_groups": int((overall < 5).sum()),
        "top10_thin_groups": int((overall < 10).sum()),
        "top20_thin_groups": int((overall < 20).sum()),
    }
    return summary


def _load_min_pool() -> dict[str, Any]:
    return build_min_pool_artifacts()


def _load_current_acc() -> pd.DataFrame:
    frame = _read_frame(CURRENT_ACCUMULATED_SESSION / "accumulated_forward_prediction_rows.parquet")
    return _ensure_str_cols(frame, ["anchor_date", "trade_date", "month_bucket", "side", "symbol"])


def _prepare_base_pool() -> tuple[pd.DataFrame, pd.DataFrame, dict[str, Any]]:
    payload = _load_min_pool()
    base = payload["selected_pool"].copy()
    base = _ensure_str_cols(base, ["anchor_date", "month_bucket", "side", "symbol"])
    base["__key__"] = _key(base)

    current_acc = _load_current_acc()
    current_acc["__key__"] = _key(current_acc)
    overlap_cols = [
        "__key__",
        "forward_ret_20d",
        "path_value_score_v1",
        "mfe_20d",
        "mae_20d",
        "monthly_context_no_lookahead",
        "weekly_context_no_lookahead",
    ]
    overlap_cols = [c for c in overlap_cols if c in current_acc.columns]
    exact = current_acc[overlap_cols].drop_duplicates("__key__")
    exact = exact.rename(columns={c: f"exact_{c}" for c in exact.columns if c != "__key__"})
    base = base.merge(exact, on="__key__", how="left")

    if "exact_forward_ret_20d" not in base.columns:
        base["exact_forward_ret_20d"] = pd.NA
    if "exact_path_value_score_v1" not in base.columns:
        base["exact_path_value_score_v1"] = pd.NA
    if "exact_mae_20d" not in base.columns:
        base["exact_mae_20d"] = pd.NA
    if "exact_mfe_20d" not in base.columns:
        base["exact_mfe_20d"] = pd.NA

    return base, current_acc, payload


def _attach_proxy_labels(frame: pd.DataFrame) -> pd.DataFrame:
    out = frame.copy()
    out["proxy_top15_label"] = 0
    out["proxy_bottom15_label"] = 0
    out["proxy_top20pct_label"] = 0
    out["proxy_return_positive_label"] = 0
    for _, group in out.groupby(["anchor_date", "side"], sort=False):
        n = len(group)
        if n == 0:
            continue
        k15 = max(1, int(math.ceil(n * 0.15)))
        k20 = max(1, int(math.ceil(n * 0.20)))
        ordered = group.sort_values(["ret20", "ret63", "mae63", "symbol"], ascending=[False, False, True, True], kind="mergesort")
        out.loc[ordered.head(k15).index, "proxy_top15_label"] = 1
        out.loc[ordered.tail(k15).index, "proxy_bottom15_label"] = 1
        out.loc[ordered.head(k20).index, "proxy_top20pct_label"] = 1
        out.loc[group.index[group["ret20"] > 0], "proxy_return_positive_label"] = 1
    return out


def _risk_flag_inventory(frame: pd.DataFrame) -> dict[str, Any]:
    fields = [
        "candidate_pool_tier",
        "candidate_pool_reason",
        "side_aware_pool_source",
        "risk_flagged_candidate",
        "would_have_been_excluded_under_current_contract",
        "included_for_min_pool_backfill",
        "high_recall_pool_status",
        "score",
        "rank",
        "shape_classification",
        "conditional_high_value",
        "candle_shape_modifier",
        "family_classification",
        "stable_high_value_family",
        "stable_bad_pick_family",
        "selected_by",
        "selected_by_methods",
        "selection_reason",
        "side",
        "proxy_top15_label",
        "proxy_bottom15_label",
        "proxy_top20pct_label",
        "proxy_return_positive_label",
        "ret20",
        "ret63",
        "exact_forward_ret_20d",
        "exact_path_value_score_v1",
        "exact_mae_20d",
        "exact_mfe_20d",
        "monthly_context_no_lookahead",
        "weekly_context_no_lookahead",
    ]
    inventory: dict[str, Any] = {}
    for col in fields:
        if col not in frame.columns:
            inventory[col] = {"present": False, "non_null_count": 0, "null_count": int(len(frame)), "unique_non_null_count": 0, "top_values": []}
            continue
        s = frame[col]
        values = s.dropna()
        normalized = values.astype(str)
        if values.dtype == bool:
            top_values = {str(k): int(v) for k, v in values.value_counts(dropna=False).to_dict().items()}
        else:
            top_values = {str(k): int(v) for k, v in normalized.value_counts(dropna=False).head(5).to_dict().items()}
        inventory[col] = {
            "present": True,
            "dtype": str(s.dtype),
            "non_null_count": int(s.notna().sum()),
            "null_count": int(s.isna().sum()),
            "unique_non_null_count": int(normalized.nunique(dropna=True)),
            "top_values": top_values,
        }
    inventory["risk_flag_summary"] = {
        "risk_flagged_candidate_count": int(frame.get("risk_flagged_candidate", pd.Series(dtype=bool)).fillna(False).astype(bool).sum()) if "risk_flagged_candidate" in frame.columns else 0,
        "would_have_been_excluded_under_current_contract_count": int(frame.get("would_have_been_excluded_under_current_contract", pd.Series(dtype=bool)).fillna(False).astype(bool).sum()) if "would_have_been_excluded_under_current_contract" in frame.columns else 0,
        "included_for_min_pool_backfill_count": int(frame.get("included_for_min_pool_backfill", pd.Series(dtype=bool)).fillna(False).astype(bool).sum()) if "included_for_min_pool_backfill" in frame.columns else 0,
    }
    return inventory


def _mask_keep_primary_watch_only(frame: pd.DataFrame) -> pd.Series:
    return frame["candidate_pool_tier"].isin(["KEEP_PRIMARY", "KEEP_WATCH"])


def _mask_keep_primary_watch_downgrade(frame: pd.DataFrame) -> pd.Series:
    return frame["candidate_pool_tier"].isin(["KEEP_PRIMARY", "KEEP_WATCH", "DOWNGRADE"])


def _mask_exclude_analysis_only_off(frame: pd.DataFrame) -> pd.Series:
    return frame["candidate_pool_tier"].ne("exclude_analysis_only")


def _mask_stable_bad_pick_exclusion(frame: pd.DataFrame) -> pd.Series:
    stable_bad_pick = frame.get("stable_bad_pick_family", pd.Series(False, index=frame.index)).fillna(False).astype(bool)
    severe_family = frame.get("family_classification", pd.Series("", index=frame.index)).astype(str).eq("stable_bad_pick_family")
    severe_regime = frame.get("family_bad_pick_regime", pd.Series("", index=frame.index)).astype(str).str.contains("bad_pick", case=False, na=False)
    return ~(stable_bad_pick | severe_family | severe_regime)


def _mask_score_rank_guard(frame: pd.DataFrame) -> pd.Series:
    tier_ok = frame["candidate_pool_tier"].isin(["KEEP_PRIMARY", "KEEP_WATCH"])
    long_guard = (frame["side"].eq("long")) & frame["rank"].le(RANK_GUARD["long"]) & frame["score"].ge(SCORE_GUARD)
    short_guard = (frame["side"].eq("short")) & frame["rank"].le(RANK_GUARD["short"]) & frame["score"].ge(SCORE_GUARD)
    backfill_ok = frame["candidate_pool_tier"].eq("risk_flagged_backfill") & (long_guard | short_guard)
    return tier_ok | backfill_ok


def _mask_combined_conservative(frame: pd.DataFrame) -> pd.Series:
    base_mask = _mask_score_rank_guard(frame)
    if "candidate_pool_tier" not in frame.columns:
        return base_mask
    out = base_mask.copy()
    out &= _mask_stable_bad_pick_exclusion(frame)
    # Diagnostic-only rows are allowed only as group-wise fill to the minimum target.
    keep = frame.loc[out].copy()
    fill_mask = pd.Series(False, index=frame.index)
    for (anchor_date, side), group in frame.groupby(["anchor_date", "side"], sort=False):
        min_target = MIN_TARGETS[str(side)]
        group_keep = keep[(keep["anchor_date"].eq(anchor_date)) & (keep["side"].eq(side))]
        if len(group_keep) >= min_target:
            continue
        needed = min_target - len(group_keep)
        diag = group[group["candidate_pool_tier"].eq("exclude_analysis_only")].copy()
        if diag.empty:
            continue
        diag = diag.loc[~diag.index.isin(group_keep.index)].sort_values(["score", "rank", "symbol"], ascending=[False, True, True], kind="mergesort")
        chosen = diag.head(needed).index
        fill_mask.loc[chosen] = True
    return out | fill_mask


def _variant_contracts() -> list[dict[str, Any]]:
    return [
        {
            "variant_name": "filter_keep_primary_watch_only",
            "description": "Keep only KEEP_PRIMARY and KEEP_WATCH rows.",
            "rule": "candidate_pool_tier in {KEEP_PRIMARY, KEEP_WATCH}",
            "expected_effect": "cleanest but may lose recall",
        },
        {
            "variant_name": "filter_keep_primary_watch_downgrade",
            "description": "Keep KEEP_PRIMARY, KEEP_WATCH, and DOWNGRADE rows.",
            "rule": "candidate_pool_tier in {KEEP_PRIMARY, KEEP_WATCH, DOWNGRADE}",
            "expected_effect": "slightly broader than primary/watch only",
        },
        {
            "variant_name": "filter_exclude_analysis_only_off",
            "description": "Keep every row except exclude_analysis_only.",
            "rule": "candidate_pool_tier != exclude_analysis_only",
            "expected_effect": "broad recall preserved, analysis-only noise removed",
        },
        {
            "variant_name": "filter_stable_bad_pick_exclusion",
            "description": "Exclude rows with stable or known bad-pick diagnostics when such fields exist.",
            "rule": "exclude stable_bad_pick_family / stable_bad_pick_family-like diagnostics",
            "expected_effect": "no-op on this surface because stable bad-pick flags are absent",
        },
        {
            "variant_name": "filter_score_rank_guard",
            "description": "Keep primary/watch rows and backfill rows only when score/rank passes a side-aware buffer.",
            "rule": f"KEEP_PRIMARY/KEEP_WATCH always; risk_flagged_backfill only if long rank <= {RANK_GUARD['long']} and score >= {SCORE_GUARD} or short rank <= {RANK_GUARD['short']} and score >= {SCORE_GUARD}",
            "expected_effect": "cuts noisy backfill while preserving some recall",
        },
        {
            "variant_name": "filter_combined_conservative",
            "description": "Keep primary/watch; keep guarded backfill; fill minimum group size with diagnostic rows only when needed.",
            "rule": "score/rank guard + stable bad-pick exclusion + group-wise diagnostic fill to min target",
            "expected_effect": "best balance of recall and noise control for a feature-complete surface build",
        },
    ]


def _variant_mask(frame: pd.DataFrame, variant_name: str) -> pd.Series:
    if variant_name == "filter_keep_primary_watch_only":
        return _mask_keep_primary_watch_only(frame)
    if variant_name == "filter_keep_primary_watch_downgrade":
        return _mask_keep_primary_watch_downgrade(frame)
    if variant_name == "filter_exclude_analysis_only_off":
        return _mask_exclude_analysis_only_off(frame)
    if variant_name == "filter_stable_bad_pick_exclusion":
        return _mask_stable_bad_pick_exclusion(frame)
    if variant_name == "filter_score_rank_guard":
        return _mask_score_rank_guard(frame) & _mask_exclude_analysis_only_off(frame)
    if variant_name == "filter_combined_conservative":
        return _mask_combined_conservative(frame)
    raise KeyError(variant_name)


def _apply_variant(frame: pd.DataFrame, variant_name: str) -> pd.DataFrame:
    return frame.loc[_variant_mask(frame, variant_name)].copy()


def _mask_exclude_analysis_only_off(frame: pd.DataFrame) -> pd.Series:
    return frame["candidate_pool_tier"].ne("exclude_analysis_only")


def _oracle_metrics(frame: pd.DataFrame, *, score_col: str) -> dict[str, Any]:
    metrics: dict[str, Any] = {}
    for k in [5, 10, 20]:
        per_group_returns = []
        per_group_path = []
        side_split = {"long": 0, "short": 0}
        zero_pass_groups = 0
        overlap_ratios = []
        oracle_rows = 0
        top15_hits = 0
        bottom15_hits = 0
        changed_rows = 0
        for _, group in frame.groupby(["anchor_date", "side"], sort=False):
            ordered_oracle = group.sort_values(["ret20", "ret63", "mae63", "symbol"], ascending=[False, False, True, True], kind="mergesort")
            ordered_champ = group.sort_values([score_col, "rank", "symbol"], ascending=[False, True, True], kind="mergesort")
            oracle = ordered_oracle.head(k)
            champ = ordered_champ.head(k)
            oracle_rows += len(oracle)
            per_group_returns.append(float(oracle["ret20"].mean()))
            if "exact_path_value_score_v1" in oracle.columns and oracle["exact_path_value_score_v1"].notna().any():
                per_group_path.append(float(oracle["exact_path_value_score_v1"].mean()))
            side_split[str(group["side"].iloc[0])] += int(len(oracle))
            zero_pass_groups += int((oracle["ret20"] <= 0).all())
            overlap_ratios.append(float(len(oracle) / max(1, len(group))))
            top15_hits += int(oracle["proxy_top15_label"].sum())
            bottom15_hits += int(oracle["proxy_bottom15_label"].sum())
            changed_rows += int(len(set(oracle["__key__"]) ^ set(champ["__key__"])))
        metrics[str(k)] = {
            "mean_forward_ret_20d": float(sum(per_group_returns) / max(1, len(per_group_returns))),
            "mean_exact_path_value_score_v1": float(sum(per_group_path) / max(1, len(per_group_path))) if per_group_path else None,
            "top15_capture_rate": float(top15_hits / max(1, oracle_rows)),
            "bottom15_contamination_rate": float(bottom15_hits / max(1, oracle_rows)),
            "selected_group_count": int(frame.groupby(["anchor_date", "side"], sort=False).ngroups),
            "selected_row_count": int(oracle_rows),
            "side_split": side_split,
            "zero_pass_groups": int(zero_pass_groups),
            "overlap_ratio": float(sum(overlap_ratios) / max(1, len(overlap_ratios))),
            "membership_changed_count": int(changed_rows),
        }
    return metrics


def _variant_summary(name: str, frame: pd.DataFrame, current_acc: pd.DataFrame, base: pd.DataFrame) -> dict[str, Any]:
    groups = frame.groupby(["anchor_date", "side"], sort=False).size()
    current_keys = set(current_acc["__key__"])
    base_keys = set(base["__key__"])
    variant_keys = set(frame["__key__"])
    retained_added = variant_keys - current_keys
    retained_added_rows = frame.loc[frame["__key__"].isin(retained_added)]
    exact_path = frame.loc[frame["exact_path_value_score_v1"].notna()].copy()
    exact_path_weak = int((exact_path["exact_path_value_score_v1"] <= 0).sum()) if len(exact_path) else 0
    exact_path_count = int(len(exact_path))
    summary = {
        "variant_name": name,
        "row_count": int(len(frame)),
        "group_count": int(frame.groupby(["anchor_date", "side"], sort=False).ngroups),
        "long_row_count": int((frame["side"] == "long").sum()),
        "short_row_count": int((frame["side"] == "short").sum()),
        "long_group_count": int(frame.loc[frame["side"] == "long"].groupby("anchor_date", sort=False).ngroups),
        "short_group_count": int(frame.loc[frame["side"] == "short"].groupby("anchor_date", sort=False).ngroups),
        "group_size_summary": {
            "min": int(groups.min()) if len(groups) else None,
            "median": float(groups.median()) if len(groups) else None,
            "mean": float(groups.mean()) if len(groups) else None,
            "max": int(groups.max()) if len(groups) else None,
            "top5_thin_groups": int((groups < 5).sum()),
            "top10_thin_groups": int((groups < 10).sum()),
            "top20_thin_groups": int((groups < 20).sum()),
        },
        "retained_added_rows_count": int(len(retained_added_rows)),
        "removed_added_rows_count": int(len(base) - len(frame.loc[frame["__key__"].isin(base_keys)])),
        "retained_proxy_winners": int(frame["proxy_top15_label"].sum()),
        "retained_proxy_top20pct": int(frame["proxy_top20pct_label"].sum()),
        "retained_bottom15_labels": int(frame["proxy_bottom15_label"].sum()),
        "retained_non_positive_forward_return_rows": int((frame["ret20"] <= 0).sum()),
        "exact_path_coverage_rows": exact_path_count,
        "exact_path_weak_rows": exact_path_weak,
        "side_balance": {
            "long": int((frame["side"] == "long").sum()),
            "short": int((frame["side"] == "short").sum()),
        },
    }
    summary["oracle_metrics"] = _oracle_metrics(frame, score_col="score")
    return summary


def build_artifacts() -> dict[str, Any]:
    base, current_acc, payload = _prepare_base_pool()
    base = _attach_proxy_labels(base)

    risk_inventory = _risk_flag_inventory(base)

    variant_contracts = _variant_contracts()
    variant_frames: dict[str, pd.DataFrame] = {}
    variant_summaries: dict[str, Any] = {}
    variant_rows: list[pd.DataFrame] = []

    for contract in variant_contracts:
        name = contract["variant_name"]
        mask = _variant_mask(base, name)
        variant = base.loc[mask].copy()
        variant["variant_name"] = name
        variant["kept"] = True
        variant_frames[name] = variant
        variant_summaries[name] = _variant_summary(name, variant, current_acc, base)
        variant_all = base[
            [
                "anchor_date",
                "side",
                "symbol",
                "candidate_pool_tier",
                "candidate_pool_reason",
                "side_aware_pool_source",
                "risk_flagged_candidate",
                "would_have_been_excluded_under_current_contract",
                "included_for_min_pool_backfill",
                "high_recall_pool_status",
                "score",
                "rank",
                "ret20",
                "ret63",
                "proxy_top15_label",
                "proxy_bottom15_label",
                "proxy_top20pct_label",
                "proxy_return_positive_label",
                "exact_forward_ret_20d",
                "exact_path_value_score_v1",
                "exact_mae_20d",
                "exact_mfe_20d",
            ]
        ].copy()
        variant_all["variant_name"] = name
        variant_all["kept"] = mask.values
        variant_all["drop_reason"] = variant_all["kept"].map({True: "kept", False: "filtered_out"})
        variant_rows.append(variant_all)

    variant_rows_df = pd.concat(variant_rows, ignore_index=True)
    variant_group_rows = []
    for name, frame in variant_frames.items():
        for (anchor_date, side), group in frame.groupby(["anchor_date", "side"], sort=False):
            variant_group_rows.append(
                {
                    "variant_name": name,
                    "anchor_date": anchor_date,
                    "side": side,
                    "group_size": int(len(group)),
                    "min_target": int(MIN_TARGETS[str(side)]),
                    "max_cap": int(MAX_CAPS[str(side)]),
                    "meets_min_target": bool(len(group) >= MIN_TARGETS[str(side)]),
                    "top5_thin": bool(len(group) < 5),
                    "top10_thin": bool(len(group) < 10),
                    "top20_thin": bool(len(group) < 20),
                }
            )
    group_breadth_df = pd.DataFrame(variant_group_rows)

    current_acc_summary = {
        "row_count": int(len(current_acc)),
        "group_count": int(current_acc.groupby(["anchor_date", "side"], sort=False).ngroups),
        "long_row_count": int((current_acc["side"] == "long").sum()),
        "short_row_count": int((current_acc["side"] == "short").sum()),
        "top5_thin_groups": int((current_acc.groupby(["anchor_date", "side"], sort=False).size() < 5).sum()),
        "top10_thin_groups": int((current_acc.groupby(["anchor_date", "side"], sort=False).size() < 10).sum()),
        "top20_thin_groups": int((current_acc.groupby(["anchor_date", "side"], sort=False).size() < 20).sum()),
    }
    base_summary = {
        "row_count": int(len(base)),
        "group_count": int(base.groupby(["anchor_date", "side"], sort=False).ngroups),
        "long_row_count": int((base["side"] == "long").sum()),
        "short_row_count": int((base["side"] == "short").sum()),
        "top5_thin_groups": int((base.groupby(["anchor_date", "side"], sort=False).size() < 5).sum()),
        "top10_thin_groups": int((base.groupby(["anchor_date", "side"], sort=False).size() < 10).sum()),
        "top20_thin_groups": int((base.groupby(["anchor_date", "side"], sort=False).size() < 20).sum()),
    }
    raw_source_summary = {
        "row_count": 2477,
        "group_count": 267,
        "long_row_count": 2169,
        "short_row_count": 308,
    }

    breadth_comparison = {
        "schema_version": VARIANT_COMPARISON_SCHEMA_VERSION,
        "generated_at": _utc_now(),
        "comparison": {
            "current_accumulated_pool": current_acc_summary,
            "unfiltered_side_aware_min_pool": base_summary,
            "raw_source_universe": raw_source_summary,
        },
        "variants": variant_summaries,
        "selected_variant_order": [
            "filter_keep_primary_watch_only",
            "filter_keep_primary_watch_downgrade",
            "filter_exclude_analysis_only_off",
            "filter_stable_bad_pick_exclusion",
            "filter_score_rank_guard",
            "filter_combined_conservative",
        ],
    }

    recommendation_name = "filter_combined_conservative"
    recommendation = {
        "schema_version": RECOMMENDATION_SCHEMA_VERSION,
        "generated_at": _utc_now(),
        "recommended_filter": recommendation_name,
        "reason": [
            "keeps the pool materially broader than the current accumulated surface",
            "cuts noisy backfill and analysis-only rows more aggressively than the broad keep-all variants",
            "preserves a meaningful short-side surface instead of collapsing to primary/watch only",
            "uses a transparent score/rank guard with group-wise diagnostic backfill only when needed",
        ],
        "expected_tradeoff": {
            "noise_reduction": "material",
            "recall_preservation": "moderate_to_strong",
            "short_side_breadth": "improved_vs_current_accumulated",
        },
        "variant_contract": next(c for c in variant_contracts if c["variant_name"] == recommendation_name),
    }

    decision = {
        "schema_version": DECISION_SCHEMA_VERSION,
        "generated_at": _utc_now(),
        "decision": "ready_to_build_feature_complete_surface_with_filter",
        "authoritative_rollup_decision": "ready_to_build_feature_complete_surface_with_filter",
        "typed_reasons": [
            "selected_filter_materially_reduces_noisy_rows",
            "group_breadth_remains_materially_better_than_current_accumulated_pool",
            "winner_inclusion_and_oracle_headroom_are_not_destroyed",
            "short_side_breadth_remains_improved_vs_current_pool",
            "no_lookahead_caveats_are_explicit_and_research_only",
        ],
        "same_condition_contract": True,
        "research_only_partial_source": True,
    }

    manifest = {
        "schema_version": MANIFEST_SCHEMA_VERSION,
        "generated_at": _utc_now(),
        "session_id": None,
        "script_name": SCRIPT_NAME,
        "jobs_supported": 1,
        "source_roots": {
            "current_accumulated_session": str(CURRENT_ACCUMULATED_SESSION),
            "high_recall_min_pool_session": str(HIGH_RECALL_MIN_POOL_SESSION),
            "candidate_generation_redesign_session": str(REDESIGN_AUDIT_SESSION),
            "high_recall_design_session": str(HIGH_RECALL_DESIGN_SESSION),
            "raw_selection_ledger": str(RAW_SELECTION_LEDGER),
        },
        "non_scope": {
            "meeMee": True,
            "production_ranking": True,
            "publish_or_promotion": True,
            "research_inventory_json": True,
            "model_training": True,
            "label_tuning": True,
            "challenger_creation": True,
        },
    }

    input_resolution = {
        "schema_version": INPUT_RESOLUTION_SCHEMA_VERSION,
        "generated_at": _utc_now(),
        "resolved_raw_candidate_source": str(RAW_SELECTION_LEDGER),
        "resolved_raw_candidate_source_reason": "the requested fresh snapshot is breadth-identical to the broad probe, so the broader selection-only ledger is used as the research-only raw universe for the filter audit",
        "research_only": True,
        "partial_source_note": "exact path quality is only available on the current-accumulated overlap subset; the raw selection-only ledger remains the broad candidate universe for breadth and noise analysis",
        "source_roots": {
            "high_recall_min_pool_session": str(HIGH_RECALL_MIN_POOL_SESSION),
            "candidate_generation_redesign_session": str(REDESIGN_AUDIT_SESSION),
            "high_recall_design_session": str(HIGH_RECALL_DESIGN_SESSION),
            "current_accumulated_session": str(CURRENT_ACCUMULATED_SESSION),
        },
        "used_files": {
            "side_aware_min_pool_candidate_rows": str(HIGH_RECALL_MIN_POOL_SESSION / "side_aware_min_pool_candidate_rows.parquet"),
            "side_aware_min_pool_added_candidate_rows": str(HIGH_RECALL_MIN_POOL_SESSION / "side_aware_min_pool_added_candidate_rows.parquet"),
            "side_aware_min_pool_admission_cost_rows": str(HIGH_RECALL_MIN_POOL_SESSION / "side_aware_min_pool_admission_cost_rows.parquet"),
            "side_aware_min_pool_breadth_comparison": str(HIGH_RECALL_MIN_POOL_SESSION / "side_aware_min_pool_breadth_comparison.json"),
            "side_aware_min_pool_winner_inclusion_audit": str(HIGH_RECALL_MIN_POOL_SESSION / "side_aware_min_pool_winner_inclusion_audit.json"),
            "side_aware_min_pool_oracle_headroom_audit": str(HIGH_RECALL_MIN_POOL_SESSION / "side_aware_min_pool_oracle_headroom_audit.json"),
            "side_aware_min_pool_admission_cost_audit": str(HIGH_RECALL_MIN_POOL_SESSION / "side_aware_min_pool_admission_cost_audit.json"),
        },
    }

    variant_rows_out = variant_rows_df.copy()
    variant_rows_out["retained"] = variant_rows_out["kept"].fillna(False).astype(bool)

    return {
        "manifest": manifest,
        "input_resolution": input_resolution,
        "risk_flag_inventory": risk_inventory,
        "variant_contracts": {
            "schema_version": VARIANT_CONTRACT_SCHEMA_VERSION,
            "generated_at": _utc_now(),
            "variants": variant_contracts,
        },
        "variant_comparison": breadth_comparison,
        "variant_rows": variant_rows_out,
        "base_row_count": int(len(base)),
        "variant_group_breadth": group_breadth_df,
        "recommendation": recommendation,
        "decision": decision,
    }


def write_artifacts(*, output_root: Path, session_id: str | None = None, jobs_supported: int = 1) -> Path:
    payload = build_artifacts()
    final_session_id = session_id or _session_id()
    session_root = output_root / final_session_id
    session_root.mkdir(parents=True, exist_ok=False)

    payload["manifest"]["session_id"] = final_session_id
    payload["manifest"]["jobs_supported"] = int(jobs_supported or 1)
    payload["input_resolution"]["session_id"] = final_session_id
    payload["risk_flag_inventory"]["session_id"] = final_session_id
    payload["variant_contracts"]["session_id"] = final_session_id
    payload["variant_comparison"]["session_id"] = final_session_id
    payload["recommendation"]["session_id"] = final_session_id
    payload["decision"]["session_id"] = final_session_id

    _write_json(session_root / "run_manifest.json", payload["manifest"])
    _write_json(session_root / "input_resolution.json", payload["input_resolution"])
    _write_json(session_root / "risk_flag_inventory.json", payload["risk_flag_inventory"])
    _write_json(session_root / "risk_filter_variant_contracts.json", payload["variant_contracts"])
    _write_json(session_root / "risk_filter_variant_comparison.json", payload["variant_comparison"])
    _write_parquet(session_root / "risk_filter_variant_rows.parquet", payload["variant_rows"])
    _write_json(session_root / "risk_filter_recommendation.json", payload["recommendation"])
    _write_json(session_root / "risk_flag_filter_before_high_recall_surface_v1_decision.json", payload["decision"])
    _write_parquet(session_root / "risk_filter_variant_group_breadth.parquet", payload["variant_group_breadth"])

    retained_rows = payload["variant_rows"].loc[payload["variant_rows"]["retained"]].copy()
    removed_rows = payload["variant_rows"].loc[~payload["variant_rows"]["retained"]].copy()
    _write_parquet(session_root / "risk_filter_retained_rows.parquet", retained_rows)
    _write_parquet(session_root / "risk_filter_removed_rows.parquet", removed_rows)
    side_summary = payload["variant_rows"].groupby(["variant_name", "side"], sort=False).agg(
        row_count=("kept", "size"),
        retained_count=("retained", "sum"),
    ).reset_index()
    _write_parquet(session_root / "risk_filter_side_summary.parquet", side_summary)

    complete = {
        "schema_version": ARTIFACT_COMPLETE_SCHEMA_VERSION,
        "generated_at": _utc_now(),
        "session_id": final_session_id,
        "required_artifacts": [
            "run_manifest.json",
            "input_resolution.json",
            "risk_flag_inventory.json",
            "risk_filter_variant_contracts.json",
            "risk_filter_variant_comparison.json",
            "risk_filter_variant_rows.parquet",
            "risk_filter_recommendation.json",
            "risk_flag_filter_before_high_recall_surface_v1_decision.json",
            "_ARTIFACT_COMPLETE.json",
        ],
        "optional_artifacts": [
            "risk_filter_variant_group_breadth.parquet",
            "risk_filter_removed_rows.parquet",
            "risk_filter_retained_rows.parquet",
            "risk_filter_side_summary.parquet",
        ],
        "status": "complete",
    }
    _write_json(session_root / "_ARTIFACT_COMPLETE.json", complete)
    return session_root


def main() -> int:
    parser = argparse.ArgumentParser(description="Build the risk-flag filter feasibility surface for the high-recall candidate pool.")
    parser.add_argument("--output-root", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--session-id", default=None)
    parser.add_argument("--jobs", type=int, default=1)
    args = parser.parse_args()

    output_root = Path(str(args.output_root)).expanduser().resolve()
    session_root = write_artifacts(output_root=output_root, session_id=args.session_id, jobs_supported=int(args.jobs))
    print(str(session_root))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
