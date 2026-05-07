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


SCRIPT_NAME = "tradex_side_aware_min_pool_feasibility_v1"
MANIFEST_SCHEMA_VERSION = "tradex_side_aware_min_pool_feasibility_v1_manifest_v1"
INPUT_RESOLUTION_SCHEMA_VERSION = "tradex_side_aware_min_pool_feasibility_v1_input_resolution_v1"
GENERATION_SCHEMA_VERSION = "tradex_side_aware_min_pool_feasibility_v1_generation_summary_v1"
NO_LOOKAHEAD_SCHEMA_VERSION = "tradex_side_aware_min_pool_feasibility_v1_no_lookahead_audit_v1"
BREADTH_SCHEMA_VERSION = "tradex_side_aware_min_pool_feasibility_v1_breadth_comparison_v1"
WINNER_SCHEMA_VERSION = "tradex_side_aware_min_pool_feasibility_v1_winner_inclusion_v1"
ORACLE_SCHEMA_VERSION = "tradex_side_aware_min_pool_feasibility_v1_oracle_headroom_v1"
ADMISSION_SCHEMA_VERSION = "tradex_side_aware_min_pool_feasibility_v1_admission_cost_v1"
DECISION_SCHEMA_VERSION = "tradex_side_aware_min_pool_feasibility_v1_decision_v1"
ARTIFACT_COMPLETE_SCHEMA_VERSION = "tradex_side_aware_min_pool_feasibility_v1_artifact_complete_v1"

DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\side_aware_min_pool_feasibility_v1")
RAW_SELECTION_LEDGER = Path(r"G:\Tradex\sample_replays\tradex_integrated_guarded_v1_stress200\integrated_guarded_v1_selection_only_ledger.json")
REQUESTED_RAW_SNAPSHOT_SOURCE = Path(r"G:\Tradex\sample_replays\tradex_integrated_guarded_v1_fresh20260502d\integrated_guarded_v1_candidate_snapshots.json")
CURRENT_BROAD_PREFILTER_SESSION = Path(r"G:\Tradex\candidate_generation_pre_filter_context_shape_v1_larger\20260502T034011Z-d76e6794")
CURRENT_ACCUMULATED_SESSION = Path(r"G:\Tradex\shadow_reranker_accumulated_forward_validation_v1\20260502T082532Z-c17e19")
REDESIGN_AUDIT_SESSION = Path(r"G:\Tradex\candidate_generation_redesign_audit_v1\20260502T105632Z-399318")
HIGH_RECALL_DESIGN_SESSION = Path(r"G:\Tradex\high_recall_candidate_pool_design_v1\20260502T112742Z-067390")


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
    path.write_text(json.dumps(_json_ready(payload), ensure_ascii=False, indent=2, sort_keys=True, default=str) + "\n", encoding="utf-8")
    return path


def _write_parquet(path: Path, frame: pd.DataFrame) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_parquet(path, index=False)
    return path


def _load_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"missing required source artifact: {path}")
    return json.loads(path.read_text(encoding="utf-8"))


def _load_frame(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"missing required source artifact: {path}")
    return pd.read_parquet(path).copy()


def _load_rows_json(path: Path) -> pd.DataFrame:
    payload = _load_json(path)
    rows = payload.get("rows", [])
    if isinstance(rows, dict) and "rows" in rows:
        rows = rows["rows"]
    frame = pd.DataFrame(rows or [])
    if frame.empty:
        raise RuntimeError(f"no rows found in {path}")
    for col in ("anchor_date", "side", "symbol", "month_bucket"):
        if col in frame.columns:
            frame[col] = frame[col].astype(str)
    return frame


def _make_key(frame: pd.DataFrame) -> pd.Series:
    return frame["anchor_date"].astype(str) + "|" + frame["side"].astype(str) + "|" + frame["symbol"].astype(str)


def _canonical_key(row: pd.Series) -> str | None:
    anchor = row.get("anchor_date")
    side = row.get("side")
    symbol = row.get("symbol")
    if pd.isna(anchor) or pd.isna(side) or pd.isna(symbol):
        return None
    anchor_str = pd.to_datetime(anchor, errors="coerce")
    anchor_text = anchor_str.date().isoformat() if not pd.isna(anchor_str) else str(anchor)
    return f"{anchor_text}|{str(side).strip().lower()}|{str(symbol).strip()}"


def _dedupe_exact_rows(frame: pd.DataFrame, *, score_col: str, rank_col: str) -> pd.DataFrame:
    frame = frame.copy()
    frame["__key__"] = _make_key(frame)
    sort_cols = ["__key__", score_col, rank_col, "symbol"]
    ascending = [True, False, True, True]
    if score_col not in frame.columns:
        raise KeyError(f"missing score column {score_col}")
    if rank_col not in frame.columns:
        raise KeyError(f"missing rank column {rank_col}")
    frame = frame.sort_values(sort_cols, ascending=ascending, kind="mergesort")
    frame = frame.drop_duplicates("__key__", keep="first")
    return frame.drop(columns=["__key__"]).reset_index(drop=True)


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


def _load_broad_prefilter() -> pd.DataFrame:
    frame = _load_frame(CURRENT_BROAD_PREFILTER_SESSION / "candidate_prefilter_rows.parquet")
    for col in ("anchor_date", "side", "symbol", "month_bucket"):
        if col in frame.columns:
            frame[col] = frame[col].astype(str)
    frame["__key__"] = _make_key(frame)
    return frame


def _load_accumulated() -> pd.DataFrame:
    frame = _load_frame(CURRENT_ACCUMULATED_SESSION / "accumulated_forward_prediction_rows.parquet")
    for col in ("anchor_date", "side", "symbol", "month_bucket"):
        if col in frame.columns:
            frame[col] = frame[col].astype(str)
    frame["__key__"] = _make_key(frame)
    return frame


def _load_raw_source() -> tuple[pd.DataFrame, dict[str, Any]]:
    requested = REQUESTED_RAW_SNAPSHOT_SOURCE
    requested_ok = requested.exists()
    selected = _load_rows_json(RAW_SELECTION_LEDGER)
    selected = selected.assign(
        candidate_rank=selected.get("rank"),
        candidate_score=selected.get("champion_score"),
        score=selected.get("champion_score"),
        rank=selected.get("champion_rank"),
    )
    selected = _dedupe_exact_rows(selected, score_col="champion_score", rank_col="champion_rank")
    selected["__key__"] = _make_key(selected)
    source_meta = {
        "requested_raw_snapshot_source": str(requested),
        "requested_raw_snapshot_exists": bool(requested_ok),
        "resolved_raw_candidate_source": str(RAW_SELECTION_LEDGER),
        "resolved_raw_candidate_source_reason": "the requested fresh snapshot is breadth-identical to the broad probe, so the broader selection-only ledger was used as the raw feasibility source because it carries the extended universe and realized outcomes",
        "raw_source_research_only": True,
        "raw_source_partial_note": "selection-only ledger is the broader source; feature/no-lookahead enrichment is partial and joined from current broad/current accumulated artifacts where available",
    }
    return selected, source_meta


def _attach_broad_context(raw: pd.DataFrame, broad: pd.DataFrame) -> pd.DataFrame:
    columns = [
        "__key__",
        "prefilter_bucket",
        "prefilter_reason",
        "include_in_broad_pool",
        "include_in_strict_pool",
        "include_in_exclude_only_pool",
        "monthly_context_no_lookahead",
        "weekly_context_no_lookahead",
        "monthly_context_date",
        "weekly_context_date",
        "monthly_context_source",
        "weekly_context_source",
        "shape_classification",
        "conditional_high_value",
        "candle_shape_modifier",
        "family_classification",
        "stable_high_value_family",
        "stable_bad_pick_family",
    ]
    cols = [c for c in columns if c in broad.columns]
    joined = raw.merge(broad[cols].drop_duplicates("__key__"), on="__key__", how="left", suffixes=("", "_broad"))
    joined["side_aware_pool_source"] = joined["__key__"].where(joined["prefilter_bucket"].notna(), None)
    joined["side_aware_pool_source"] = joined["prefilter_bucket"].fillna("raw_selection_only_ledger")
    joined["side_aware_pool_source"] = joined["side_aware_pool_source"].replace({None: "raw_selection_only_ledger"})
    return joined


def _attach_accumulated(joined: pd.DataFrame, acc: pd.DataFrame) -> pd.DataFrame:
    columns = [
        "__key__",
        "forward_ret_5d",
        "forward_ret_10d",
        "forward_ret_20d",
        "path_value_score_v1",
        "mfe_20d",
        "mae_20d",
        "hit_plus_5_before_minus_5",
        "hit_minus_5_before_plus_5",
        "top15_label",
        "bottom15_label",
        "monthly_context_no_lookahead",
        "weekly_context_no_lookahead",
        "monthly_context_date",
        "weekly_context_date",
        "monthly_context_source",
        "weekly_context_source",
        "candle_body_ratio",
        "candle_upper_wick_ratio",
        "candle_lower_wick_ratio",
        "candle_triplet_up_prob",
        "candle_triplet_down_prob",
        "gap_pct",
        "vol_ratio5_20",
        "shape_joined",
        "shape_classification",
    ]
    cols = [c for c in columns if c in acc.columns]
    enriched = joined.merge(acc[cols].drop_duplicates("__key__"), on="__key__", how="left", suffixes=("", "_acc"))
    return enriched


def _tier_priority(tier: str) -> int:
    return {
        "KEEP_PRIMARY": 0,
        "KEEP_WATCH": 1,
        "DOWNGRADE": 2,
        "risk_flagged_backfill": 3,
        "exclude_analysis_only": 4,
    }.get(tier, 9)


def _min_pool_reject_subreason(row: pd.Series, *, cutoff: pd.Series | None, cap: int, side: str) -> str:
    if pd.isna(row.get("anchor_date")) or pd.isna(row.get("side")) or pd.isna(row.get("symbol")):
        return "min_pool_missing_required_key"
    if pd.isna(row.get("candidate_idx")):
        return "min_pool_missing_required_key"
    if _bool(row.get("duplicate_key_excluded")):
        return "min_pool_duplicate_key_excluded"
    if str(side).lower() != "long" and cap <= 10:
        return "min_pool_side_cap_excluded"
    if cutoff is None or cutoff.empty:
        return "min_pool_unclassified_reject"

    row_priority = int(row.get("pool_priority")) if not pd.isna(row.get("pool_priority")) else 9
    cutoff_priority = int(cutoff.get("pool_priority")) if not pd.isna(cutoff.get("pool_priority")) else 9
    if row_priority > cutoff_priority:
        return "min_pool_tier_priority_excluded"

    row_score = pd.to_numeric(row.get("score"), errors="coerce")
    cutoff_score = pd.to_numeric(cutoff.get("score"), errors="coerce")
    if pd.notna(row_score) and pd.notna(cutoff_score) and row_score < cutoff_score:
        return "min_pool_cap_exhausted_by_score"

    row_rank = pd.to_numeric(row.get("rank"), errors="coerce")
    cutoff_rank = pd.to_numeric(cutoff.get("rank"), errors="coerce")
    if pd.notna(row_rank) and pd.notna(cutoff_rank) and row_rank > cutoff_rank:
        return "min_pool_cap_exhausted_by_rank"

    return "min_pool_cap_exhausted_by_rank"


def _assign_pool_tier(frame: pd.DataFrame) -> pd.DataFrame:
    out = frame.copy()
    out["candidate_pool_tier"] = "exclude_analysis_only"
    out.loc[out["include_in_strict_pool"].fillna(False), "candidate_pool_tier"] = "KEEP_PRIMARY"
    out.loc[(~out["include_in_strict_pool"].fillna(False)) & (out["include_in_broad_pool"].fillna(False)), "candidate_pool_tier"] = "KEEP_WATCH"
    out.loc[
        out["candidate_pool_tier"].eq("exclude_analysis_only") & out["selected_by"].eq("both"),
        "candidate_pool_tier",
    ] = "risk_flagged_backfill"
    out.loc[
        out["candidate_pool_tier"].eq("exclude_analysis_only") & out["selected_by"].eq("champion"),
        "candidate_pool_tier",
    ] = "exclude_analysis_only"
    out.loc[
        out["candidate_pool_tier"].eq("exclude_analysis_only") & out["include_in_exclude_only_pool"].fillna(False),
        "candidate_pool_tier",
    ] = "DOWNGRADE"
    out["candidate_pool_reason"] = out["candidate_pool_tier"].map(
        {
            "KEEP_PRIMARY": "broad_prefilter_strict_pool",
            "KEEP_WATCH": "broad_prefilter_broad_pool",
            "DOWNGRADE": "broad_prefilter_exclude_backfill",
            "risk_flagged_backfill": "raw_source_backfill_high_recall",
            "exclude_analysis_only": "diagnostic_only_outside_primary_backfill",
        }
    )
    out["risk_flagged_candidate"] = out["candidate_pool_tier"].isin(["DOWNGRADE", "risk_flagged_backfill", "exclude_analysis_only"])
    out["would_have_been_excluded_under_current_contract"] = ~out["include_in_broad_pool"].fillna(False)
    out["included_for_min_pool_backfill"] = out["candidate_pool_tier"].isin(["DOWNGRADE", "risk_flagged_backfill"])
    return out


def _select_min_pool(frame: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    selected: list[pd.DataFrame] = []
    excluded: list[pd.DataFrame] = []
    for (anchor_date, side), group in frame.groupby(["anchor_date", "side"], sort=False):
        cap = 40 if str(side) == "long" else 10
        min_target = 20 if str(side) == "long" else 5
        group = group.copy()
        group["pool_priority"] = group["candidate_pool_tier"].map(_tier_priority)
        group["min_pool_rule_name"] = "side_aware_min_pool_feasibility_v1"
        group = group.sort_values(
            ["pool_priority", "score", "champion_rank", "symbol"],
            ascending=[True, False, True, True],
            kind="mergesort",
        ).reset_index(drop=True)
        group["pool_rank"] = range(1, len(group) + 1)
        group["selected_for_min_pool"] = group["pool_rank"] <= cap
        group["min_target"] = min_target
        group["max_cap"] = cap
        group["min_target_met"] = len(group) >= min_target
        group["high_recall_pool_status"] = "selected" if len(group) <= cap else "selected_or_excluded_by_cap"
        group["group_candidate_count_before_cap"] = int(len(group))
        group["group_candidate_count_after_cap"] = int(min(len(group), cap))
        group["group_min_target"] = int(min_target)
        group["group_max_cap"] = int(cap)
        group["min_pool_priority_rank"] = group["pool_priority"]
        group["min_pool_candidate_pool_tier_before_reject"] = group["candidate_pool_tier"]
        group["min_pool_candidate_pool_reason_before_reject"] = group["candidate_pool_reason"]
        cutoff = group.loc[group["selected_for_min_pool"]].tail(1)
        cutoff_row = cutoff.iloc[0] if len(cutoff) else None
        selected_group = group.loc[group["selected_for_min_pool"]].copy()
        excluded_group = group.loc[~group["selected_for_min_pool"]].copy()
        selected_group["accepted"] = True
        selected_group["min_pool_reject_reason_bucket"] = "accepted"
        selected_group["min_pool_reject_reason"] = None
        selected_group["min_pool_reject_subreason"] = None
        selected_group["min_pool_priority_cutoff"] = None if cutoff_row is None else int(cutoff_row["pool_priority"])
        selected_group["min_pool_score_cutoff"] = None if cutoff_row is None or pd.isna(cutoff_row.get("score")) else cutoff_row.get("score")
        selected_group["min_pool_rank_cutoff"] = None if cutoff_row is None or pd.isna(cutoff_row.get("rank")) else cutoff_row.get("rank")
        selected_group["min_pool_rule_name"] = "side_aware_min_pool_feasibility_v1"
        if len(excluded_group):
            excluded_group["accepted"] = False
            excluded_group["min_pool_reject_reason_bucket"] = "min_pool_gate_reject"
            excluded_group["min_pool_reject_reason"] = "min_pool_cap_exhausted"
            excluded_group["min_pool_priority_cutoff"] = None if cutoff_row is None else int(cutoff_row["pool_priority"])
            excluded_group["min_pool_score_cutoff"] = None if cutoff_row is None or pd.isna(cutoff_row.get("score")) else cutoff_row.get("score")
            excluded_group["min_pool_rank_cutoff"] = None if cutoff_row is None or pd.isna(cutoff_row.get("rank")) else cutoff_row.get("rank")
            excluded_group["min_pool_reject_subreason"] = excluded_group.apply(
                lambda row: _min_pool_reject_subreason(row, cutoff=cutoff_row, cap=cap, side=str(side)),
                axis=1,
            )
            selected.append(selected_group)
            excluded.append(excluded_group)
        else:
            selected.append(selected_group)
    selected_frame = pd.concat(selected, ignore_index=True) if selected else frame.head(0).copy()
    excluded_frame = pd.concat(excluded, ignore_index=True) if excluded else frame.head(0).copy()
    return selected_frame, excluded_frame


def _proxy_labels(frame: pd.DataFrame, *, score_col: str, tie_cols: list[str], group_cols: list[str] = ["anchor_date", "side"]) -> pd.DataFrame:
    out = frame.copy()
    top15 = []
    bottom15 = []
    top20pct = []
    pos = []
    for _, group in out.groupby(group_cols, sort=False):
        n = len(group)
        if n == 0:
            continue
        k15 = max(1, int(math.ceil(n * 0.15)))
        k20 = max(1, int(math.ceil(n * 0.20)))
        sort_cols = [score_col] + tie_cols
        ascending = [False] + [False if col.endswith("score") or col.startswith("ret") or col.startswith("forward_ret") else True for col in tie_cols]
        ordered = group.sort_values(sort_cols, ascending=ascending, kind="mergesort")
        top15_idx = set(ordered.head(k15).index)
        bottom15_idx = set(ordered.tail(k15).index)
        top20_idx = set(ordered.head(k20).index)
        pos_idx = set(group.index[group[score_col] > 0])
        top15.extend([(idx, 1) for idx in top15_idx])
        bottom15.extend([(idx, 1) for idx in bottom15_idx])
        top20pct.extend([(idx, 1) for idx in top20_idx])
        pos.extend([(idx, 1) for idx in pos_idx])
    out["proxy_top15_label"] = 0
    out["proxy_bottom15_label"] = 0
    out["proxy_top20pct_label"] = 0
    out["proxy_return_positive_label"] = 0
    for idx, _ in top15:
        out.loc[idx, "proxy_top15_label"] = 1
    for idx, _ in bottom15:
        out.loc[idx, "proxy_bottom15_label"] = 1
    for idx, _ in top20pct:
        out.loc[idx, "proxy_top20pct_label"] = 1
    for idx, _ in pos:
        out.loc[idx, "proxy_return_positive_label"] = 1
    return out


def _count_proxy_stats(frame: pd.DataFrame) -> dict[str, Any]:
    return {
        "row_count": int(len(frame)),
        "group_count": int(frame.groupby(["anchor_date", "side"], sort=False).ngroups),
        "top15_positive_count": int(frame["proxy_top15_label"].sum()),
        "bottom15_positive_count": int(frame["proxy_bottom15_label"].sum()),
        "top20pct_positive_count": int(frame["proxy_top20pct_label"].sum()),
        "return_positive_count": int(frame["proxy_return_positive_label"].sum()),
        "top15_positive_groups": int(frame.groupby(["anchor_date", "side"], sort=False)["proxy_top15_label"].sum().gt(0).sum()),
        "bottom15_positive_groups": int(frame.groupby(["anchor_date", "side"], sort=False)["proxy_bottom15_label"].sum().gt(0).sum()),
    }


def _oracle_metrics(frame: pd.DataFrame, *, score_col: str, tie_cols: list[str]) -> dict[str, Any]:
    metrics: dict[str, Any] = {}
    for k in [5, 10, 20]:
        per_group = []
        side_split = {"long": 0, "short": 0}
        changed_count = 0
        overlap_ratio_values = []
        zero_pass_groups = 0
        for _, group in frame.groupby(["anchor_date", "side"], sort=False):
            ordered = group.sort_values([score_col] + tie_cols, ascending=[False] + [False if c.endswith("score") or c.startswith("ret") or c.startswith("forward_ret") else True for c in tie_cols], kind="mergesort")
            selected = ordered.head(k)
            per_group.append(selected["ret20"].mean())
            side_split[str(group["side"].iloc[0])] += int(len(selected))
            zero_pass_groups += int(selected["ret20"].le(0).all())
            overlap_ratio_values.append(float(len(selected) / max(1, len(group))))
        metrics[str(k)] = {
            "mean_forward_ret_20d": float(sum(per_group) / max(1, len(per_group))),
            "mean_path_value_score_v1": None,
            "top15_capture_rate": None,
            "bottom15_contamination_rate": None,
            "selected_group_count": int(frame.groupby(["anchor_date", "side"], sort=False).ngroups),
            "selected_row_count": int(frame.groupby(["anchor_date", "side"], sort=False).apply(lambda g: min(k, len(g))).sum()),
            "side_split": side_split,
            "zero_pass_groups": int(zero_pass_groups),
            "overlap_ratio": float(sum(overlap_ratio_values) / max(1, len(overlap_ratio_values))),
            "membership_changed_count": changed_count,
        }
    return metrics


def _build_breadth_comparison(current_acc: pd.DataFrame, broad: pd.DataFrame, selected: pd.DataFrame, raw_unique: pd.DataFrame) -> dict[str, Any]:
    return {
        "schema_version": BREADTH_SCHEMA_VERSION,
        "generated_at": _utc_now(),
        "comparison": {
            "current_accumulated_pool": _group_summary(current_acc),
            "broad_prefilter_probe": _group_summary(broad),
            "side_aware_min_pool": _group_summary(selected),
            "raw_candidate_universe": _group_summary(raw_unique),
        },
        "selected_pool_counts": {
            "row_count": int(len(selected)),
            "group_count": int(selected.groupby(["anchor_date", "side"], sort=False).ngroups),
            "long_row_count": int((selected["side"] == "long").sum()),
            "short_row_count": int((selected["side"] == "short").sum()),
            "long_group_count": int(selected.loc[selected["side"] == "long"].groupby("anchor_date", sort=False).ngroups),
            "short_group_count": int(selected.loc[selected["side"] == "short"].groupby("anchor_date", sort=False).ngroups),
        },
        "broad_probe_counts": {
            "row_count": int(len(broad)),
            "group_count": int(broad.groupby(["anchor_date", "side"], sort=False).ngroups),
            "long_row_count": int((broad["side"] == "long").sum()),
            "short_row_count": int((broad["side"] == "short").sum()),
        },
        "raw_source_counts": {
            "row_count": int(len(raw_unique)),
            "group_count": int(raw_unique.groupby(["anchor_date", "side"], sort=False).ngroups),
            "long_row_count": int((raw_unique["side"] == "long").sum()),
            "short_row_count": int((raw_unique["side"] == "short").sum()),
            "raw_rows_total_ledger": int(raw_unique.attrs.get("raw_rows_total_ledger", len(raw_unique))),
        },
    }


def _build_winner_audit(current_acc: pd.DataFrame, selected: pd.DataFrame, path_overlap: pd.DataFrame) -> tuple[dict[str, Any], pd.DataFrame]:
    current_proxy = _proxy_labels(
        current_acc.rename(columns={"forward_ret_20d": "ret20"}),
        score_col="ret20",
        tie_cols=["path_value_score_v1", "mae_20d", "candidate_idx"],
    )
    selected_proxy = _proxy_labels(
        selected.rename(columns={"ret20": "ret20"}),
        score_col="ret20",
        tie_cols=["ret63", "mae63", "symbol"],
    )
    overlap_path = path_overlap.copy()
    current_stats = _count_proxy_stats(current_proxy)
    selected_stats = _count_proxy_stats(selected_proxy)
    audit = {
        "schema_version": WINNER_SCHEMA_VERSION,
        "generated_at": _utc_now(),
        "proxy_label_definition": {
            "top15": "top ceil(15% of rows within each anchor_date / side group by forward return",
            "bottom15": "bottom ceil(15% of rows within each anchor_date / side group by forward return",
            "top20pct": "top ceil(20% of rows within each anchor_date / side group by forward return",
            "return_threshold": "forward_ret_20d > 0",
            "path_quality_scope": "overlap_only_from_current_accumulated_surface",
        },
        "current_accumulated_pool": current_stats,
        "side_aware_min_pool": selected_stats,
        "path_quality_scope": {
            "overlap_row_count": int(len(overlap_path)),
            "exact_path_value_available": bool(len(overlap_path)),
            "missing_reason": None if len(overlap_path) else "raw selection ledger does not contain exact path_value_score_v1; only overlap-enriched rows can be scored for path quality",
        },
    }
    return audit, selected_proxy


def _build_oracle_headroom(current_acc: pd.DataFrame, selected: pd.DataFrame, selected_proxy: pd.DataFrame) -> dict[str, Any]:
    current_metrics = _oracle_metrics(current_acc.rename(columns={"forward_ret_20d": "ret20"}), score_col="ret20", tie_cols=["path_value_score_v1", "mae_20d", "candidate_idx"])
    selected_metrics = _oracle_metrics(selected_proxy, score_col="ret20", tie_cols=["ret63", "mae63", "symbol"])
    breadth_headroom = {}
    for k in ["5", "10", "20"]:
        breadth_headroom[k] = {
            "current_champion_mean_forward_ret_20d": current_metrics[k]["mean_forward_ret_20d"],
            "selected_champion_mean_forward_ret_20d": selected_metrics[k]["mean_forward_ret_20d"],
            "oracle_mean_forward_ret_20d": selected_metrics[k]["mean_forward_ret_20d"],
            "current_zero_pass_groups": current_metrics[k]["zero_pass_groups"],
            "selected_zero_pass_groups": selected_metrics[k]["zero_pass_groups"],
            "oracle_minus_champion_mean_forward_ret_20d": selected_metrics[k]["mean_forward_ret_20d"] - current_metrics[k]["mean_forward_ret_20d"],
            "champion_overlap_ratio": current_metrics[k]["overlap_ratio"],
            "oracle_overlap_ratio": selected_metrics[k]["overlap_ratio"],
        }
    return {
        "schema_version": ORACLE_SCHEMA_VERSION,
        "generated_at": _utc_now(),
        "breadth_headroom": breadth_headroom,
        "current_pool_reference": current_metrics,
        "selected_pool_reference": selected_metrics,
        "path_quality": {
            "scope": "overlap_only",
            "coverage_row_count": int(len(selected_proxy)),
            "exact_path_value_available": bool(len(selected_proxy)),
            "missing_reason": None if len(selected_proxy) else "exact path_value_score_v1 unavailable on raw selection ledger",
        },
    }


def _build_admission_cost(selected: pd.DataFrame, current_acc: pd.DataFrame, selected_proxy: pd.DataFrame) -> tuple[dict[str, Any], pd.DataFrame]:
    added = selected.merge(
        current_acc[["__key__"]].drop_duplicates(),
        on="__key__",
        how="left",
        indicator=True,
    )
    added = added.loc[added["_merge"].eq("left_only")].copy()
    added["added_by_side"] = added["side"]
    added["added_by_tier"] = added["candidate_pool_tier"]
    added["added_ret20_poor"] = added["ret20"].le(0)
    added["added_result_loss"] = added.get("result_bucket", pd.Series(index=added.index, dtype="object")).astype(str).eq("loss")
    if "path_value_score_v1" in added.columns:
        added["added_weak_path_quality"] = added["path_value_score_v1"].isna() | added["path_value_score_v1"].lt(0)
    else:
        added["added_weak_path_quality"] = False
    cost = {
        "schema_version": ADMISSION_SCHEMA_VERSION,
        "generated_at": _utc_now(),
        "added_candidates_count": int(len(added)),
        "added_candidates_by_tier": added["added_by_tier"].value_counts(dropna=False).to_dict(),
        "added_candidates_by_side": added["added_by_side"].value_counts(dropna=False).to_dict(),
        "added_candidates_with_ret20_le_0": int(added["added_ret20_poor"].sum()),
        "added_candidates_with_bottom15_label": int(added.get("proxy_bottom15_label", pd.Series(index=added.index, dtype=int)).sum()),
        "added_candidates_with_weak_path_quality": int(added["added_weak_path_quality"].sum()),
        "tier_quality_summary": {
            "KEEP_PRIMARY_mean_ret20": float(selected.loc[selected["candidate_pool_tier"].eq("KEEP_PRIMARY"), "ret20"].mean()) if (selected["candidate_pool_tier"].eq("KEEP_PRIMARY").any()) else None,
            "KEEP_WATCH_mean_ret20": float(selected.loc[selected["candidate_pool_tier"].eq("KEEP_WATCH"), "ret20"].mean()) if (selected["candidate_pool_tier"].eq("KEEP_WATCH").any()) else None,
            "DOWNGRADE_mean_ret20": float(selected.loc[selected["candidate_pool_tier"].eq("DOWNGRADE"), "ret20"].mean()) if (selected["candidate_pool_tier"].eq("DOWNGRADE").any()) else None,
            "risk_flagged_backfill_mean_ret20": float(selected.loc[selected["candidate_pool_tier"].eq("risk_flagged_backfill"), "ret20"].mean()) if (selected["candidate_pool_tier"].eq("risk_flagged_backfill").any()) else None,
            "exclude_analysis_only_mean_ret20": float(selected.loc[selected["candidate_pool_tier"].eq("exclude_analysis_only"), "ret20"].mean()) if (selected["candidate_pool_tier"].eq("exclude_analysis_only").any()) else None,
        },
        "noise_judgment": {
            "risk_flagged_rows_exist": bool(selected["candidate_pool_tier"].isin(["DOWNGRADE", "risk_flagged_backfill", "exclude_analysis_only"]).any()),
            "champion_only_rows_are_noisy": bool((selected["selected_by"] == "champion").any() and selected.loc[selected["selected_by"].eq("champion"), "ret20"].mean() < 0),
            "research_hold_reason": "champion_only_backfill_is_noisy_and_short_side_still_needs a separate risk filter",
        },
    }
    return cost, added


def build_artifacts() -> dict[str, Any]:
    raw_source, raw_source_meta = _load_raw_source()
    broad = _load_broad_prefilter()
    current_acc = _load_accumulated()

    raw_source["__key__"] = _make_key(raw_source)
    raw_source["canonical_candidate_key"] = raw_source.apply(_canonical_key, axis=1)
    raw_source.attrs["raw_rows_total_ledger"] = 3127
    raw_source = _attach_broad_context(raw_source, broad)
    raw_source = _attach_accumulated(raw_source, current_acc)
    raw_source = _assign_pool_tier(raw_source)

    selected, excluded = _select_min_pool(raw_source)
    selected = selected.copy()
    excluded = excluded.copy()
    selected["selected_for_min_pool"] = True
    selected["high_recall_pool_status"] = selected["candidate_pool_tier"].map(
        {
            "KEEP_PRIMARY": "selected_primary",
            "KEEP_WATCH": "selected_watch",
            "DOWNGRADE": "selected_risk_flagged_backfill",
            "risk_flagged_backfill": "selected_risk_flagged_backfill",
            "exclude_analysis_only": "selected_diagnostic_only",
        }
    )
    selected["risk_flagged_candidate"] = selected["candidate_pool_tier"].isin(["DOWNGRADE", "risk_flagged_backfill", "exclude_analysis_only"])
    selected["included_for_min_pool_backfill"] = selected["candidate_pool_tier"].isin(["DOWNGRADE", "risk_flagged_backfill"])
    selected["would_have_been_excluded_under_current_contract"] = ~selected["include_in_broad_pool"].fillna(False)

    # Preserve champion fields and create explicit pool-rank aliases.
    selected["score"] = selected["champion_score"]
    selected["rank"] = selected["champion_rank"]
    selected = selected.sort_values(
        ["anchor_date", "side", "pool_priority", "score", "rank", "symbol"],
        ascending=[True, True, True, False, True, True],
        kind="mergesort",
    ).reset_index(drop=True)
    selected["pool_rank"] = selected.groupby(["anchor_date", "side"], sort=False).cumcount() + 1
    selected["pool_rank_within_side"] = selected.groupby(["side"], sort=False).cumcount() + 1
    selected["pool_rank_within_month"] = selected.groupby(["month_bucket"], sort=False).cumcount() + 1 if "month_bucket" in selected.columns else selected["pool_rank"]
    excluded["canonical_candidate_key"] = excluded.apply(_canonical_key, axis=1)
    selected["canonical_candidate_key"] = selected.apply(_canonical_key, axis=1)

    # Join current accumulated labels/metrics on the overlap only for partial path/no-lookahead auditing.
    overlap_keys = set(selected["__key__"]) & set(current_acc["__key__"])
    current_pool_proxy = _proxy_labels(
        current_acc.rename(columns={"forward_ret_20d": "ret20"}),
        score_col="ret20",
        tie_cols=["path_value_score_v1", "mae_20d", "candidate_idx"],
    )
    selected_pool_proxy = _proxy_labels(
        selected.copy(),
        score_col="ret20",
        tie_cols=["ret63", "mae63", "symbol"],
    )

    breadth = _build_breadth_comparison(current_acc, broad, selected, raw_source)
    path_overlap = selected.loc[selected["__key__"].isin(overlap_keys)].copy()
    winner_audit, selected_proxy = _build_winner_audit(current_acc, selected, path_overlap)
    selected = selected_proxy.copy()
    oracle = _build_oracle_headroom(current_acc, selected, selected_proxy)
    admission_cost, added = _build_admission_cost(selected, current_acc, selected_proxy)

    no_lookahead_verified = bool(
        ((broad["monthly_context_no_lookahead"].fillna(False)) & (broad["weekly_context_no_lookahead"].fillna(False))).sum() > 0
    )
    no_lookahead_audit = {
        "schema_version": NO_LOOKAHEAD_SCHEMA_VERSION,
        "generated_at": _utc_now(),
        "selected_pool_row_count": int(len(selected)),
        "feature_enriched_row_count": int(len(path_overlap)),
        "broad_overlap_row_count": int(len(selected.loc[selected["include_in_broad_pool"].fillna(False)])),
        "accumulated_overlap_row_count": int(len(path_overlap)),
        "verified_no_lookahead_rows": int(len(broad.loc[broad["monthly_context_no_lookahead"].fillna(False) & broad["weekly_context_no_lookahead"].fillna(False)])),
        "verified_no_lookahead_pass": bool(no_lookahead_verified),
        "full_pool_verified": False,
        "partial_source_note": "no-lookahead is verified on overlap-enriched rows only; the broader raw selection-only ledger is research-only until feature-complete enrichment is built",
        "missing_reason": "raw selection ledger does not carry monthly/weekly no-lookahead flags; they are only available on the overlap with the current broad prefilter/session artifacts",
    }

    generation_summary = {
        "schema_version": GENERATION_SCHEMA_VERSION,
        "generated_at": _utc_now(),
        "contract_name": "side_aware_minimum_pool_size_v1",
        "implementation_style": "two_stage_high_recall_then_rerank",
        "recommended_axis": "side_aware_candidate_admission_caps",
        "source_resolution": raw_source_meta,
        "source_counts": {
            "raw_selection_ledger_total_rows": 3127,
            "raw_selection_ledger_unique_rows": int(len(raw_source)),
            "raw_selection_ledger_groups": int(raw_source.groupby(["anchor_date", "side"], sort=False).ngroups),
            "current_accumulated_rows": int(len(current_acc)),
            "current_accumulated_groups": int(current_acc.groupby(["anchor_date", "side"], sort=False).ngroups),
            "broad_prefilter_rows": int(len(broad)),
            "broad_prefilter_groups": int(broad.groupby(["anchor_date", "side"], sort=False).ngroups),
        },
        "target_caps": {
            "long_min": 20,
            "short_min": 5,
            "long_max": 40,
            "short_max": 10,
        },
        "selected_counts": {
            "row_count": int(len(selected)),
            "group_count": int(selected.groupby(["anchor_date", "side"], sort=False).ngroups),
            "long_row_count": int((selected["side"] == "long").sum()),
            "short_row_count": int((selected["side"] == "short").sum()),
            "long_group_count": int(selected.loc[selected["side"] == "long"].groupby("anchor_date", sort=False).ngroups),
            "short_group_count": int(selected.loc[selected["side"] == "short"].groupby("anchor_date", sort=False).ngroups),
            "selected_by_tier": selected["candidate_pool_tier"].value_counts(dropna=False).to_dict(),
            "selected_by_side": selected["side"].value_counts(dropna=False).to_dict(),
        },
        "cap_trim": {
            "selected_rows_vs_raw_unique": int(len(raw_source) - len(selected)),
            "selected_rows_vs_raw_total": int(3127 - len(selected)),
            "cap_trimmed_rows": int(len(excluded)),
        },
        "breadth_by_side": _group_summary(selected),
        "no_lookahead_status": {
            "verified_rows": int(no_lookahead_audit["verified_no_lookahead_rows"]),
            "selected_pool_row_count": int(len(selected)),
            "verified_pass": bool(no_lookahead_audit["verified_no_lookahead_pass"]),
            "full_pool_verified": False,
            "research_only_partial": True,
        },
    }

    # Additional diagnostic tables.
    group_breadth_rows = []
    for (anchor_date, side), g in selected.groupby(["anchor_date", "side"], sort=False):
        group_breadth_rows.append(
            {
                "anchor_date": anchor_date,
                "side": side,
                "group_size": int(len(g)),
                "min_target": 20 if side == "long" else 5,
                "max_cap": 40 if side == "long" else 10,
                "meets_min_target": bool(len(g) >= (20 if side == "long" else 5)),
                "pool_primary_count": int((g["candidate_pool_tier"] == "KEEP_PRIMARY").sum()),
                "pool_watch_count": int((g["candidate_pool_tier"] == "KEEP_WATCH").sum()),
                "pool_downgrade_count": int((g["candidate_pool_tier"] == "DOWNGRADE").sum()),
                "pool_risk_flagged_backfill_count": int((g["candidate_pool_tier"] == "risk_flagged_backfill").sum()),
                "pool_exclude_only_count": int((g["candidate_pool_tier"] == "exclude_analysis_only").sum()),
                "top15_positive_count": int(g["proxy_top15_label"].sum()),
                "bottom15_positive_count": int(g["proxy_bottom15_label"].sum()),
                "top20pct_positive_count": int(g["proxy_top20pct_label"].sum()),
                "return_positive_count": int(g["proxy_return_positive_label"].sum()),
            }
        )
    group_breadth_df = pd.DataFrame(group_breadth_rows)

    oracle_by_group_rows = []
    for (anchor_date, side), g in selected.groupby(["anchor_date", "side"], sort=False):
        oracle_sorted = g.sort_values(["ret20", "ret63", "mae63", "symbol"], ascending=[False, False, True, True], kind="mergesort")
        champion_sorted = g.sort_values(["champion_score", "rank", "symbol"], ascending=[False, True, True], kind="mergesort")
        for k in [5, 10, 20]:
            oracle_by_group_rows.append(
                {
                    "anchor_date": anchor_date,
                    "side": side,
                    "topk": k,
                    "oracle_selected_row_count": int(min(k, len(g))),
                    "oracle_mean_forward_ret_20d": float(oracle_sorted.head(k)["ret20"].mean()) if len(g) else None,
                    "champion_mean_forward_ret_20d": float(champion_sorted.head(k)["ret20"].mean()) if len(g) else None,
                    "oracle_bottom15_contamination_rate": float(oracle_sorted.head(k)["proxy_bottom15_label"].mean()) if len(g) else None,
                    "oracle_top15_capture_rate": float(oracle_sorted.head(k)["proxy_top15_label"].mean()) if len(g) else None,
                    "champion_top15_capture_rate": float(champion_sorted.head(k)["proxy_top15_label"].mean()) if len(g) else None,
                }
            )
    oracle_by_group_df = pd.DataFrame(oracle_by_group_rows)

    # Breadth comparison and cost rows help with verification.
    selected_added = added.copy()
    selected_added["added_reason"] = selected_added["candidate_pool_tier"]
    selected_added["research_only_partial"] = True

    decision = {
        "schema_version": DECISION_SCHEMA_VERSION,
        "generated_at": _utc_now(),
        "decision": "needs_risk_flag_filter_before_surface_build",
        "authoritative_rollup_decision": "needs_risk_flag_filter_before_surface_build",
        "typed_reasons": [
            "broad_recall_expansion_is_material",
            "champion_only_backfill_is_noisy_and_short_side_heavy",
            "exact_path_quality_and_no_lookahead_are_only_partially_observable_on_the_raw_selection_ledger",
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
            "requested_raw_snapshot_source": str(REQUESTED_RAW_SNAPSHOT_SOURCE),
            "resolved_raw_candidate_source": str(RAW_SELECTION_LEDGER),
            "current_broad_prefilter_session": str(CURRENT_BROAD_PREFILTER_SESSION),
            "current_accumulated_session": str(CURRENT_ACCUMULATED_SESSION),
            "redesign_audit_session": str(REDESIGN_AUDIT_SESSION),
            "high_recall_design_session": str(HIGH_RECALL_DESIGN_SESSION),
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
        "requested_raw_snapshot_source": str(REQUESTED_RAW_SNAPSHOT_SOURCE),
        "resolved_raw_candidate_source_reason": raw_source_meta["resolved_raw_candidate_source_reason"],
        "source_roots": {
            "high_recall_design_session": str(HIGH_RECALL_DESIGN_SESSION),
            "candidate_generation_redesign_audit_session": str(REDESIGN_AUDIT_SESSION),
            "current_broad_prefilter_session": str(CURRENT_BROAD_PREFILTER_SESSION),
            "current_accumulated_session": str(CURRENT_ACCUMULATED_SESSION),
        },
        "used_files": {
            "raw_selection_ledger": str(RAW_SELECTION_LEDGER),
            "broad_prefilter_rows": str(CURRENT_BROAD_PREFILTER_SESSION / "candidate_prefilter_rows.parquet"),
            "accumulated_forward_prediction_rows": str(CURRENT_ACCUMULATED_SESSION / "accumulated_forward_prediction_rows.parquet"),
            "candidate_pool_breadth_audit": str(REDESIGN_AUDIT_SESSION / "candidate_pool_breadth_audit.json"),
            "candidate_pool_oracle_headroom_audit": str(REDESIGN_AUDIT_SESSION / "candidate_pool_oracle_headroom_audit.json"),
            "candidate_admission_failure_audit": str(REDESIGN_AUDIT_SESSION / "candidate_admission_failure_audit.json"),
        },
        "partial_source_note": raw_source_meta["raw_source_partial_note"],
        "research_only": True,
    }

    return {
        "manifest": manifest,
        "input_resolution": input_resolution,
        "generation_summary": generation_summary,
        "no_lookahead_audit": no_lookahead_audit,
        "breadth_comparison": breadth,
        "winner_audit": winner_audit,
        "oracle_headroom": oracle,
        "admission_cost": admission_cost,
        "decision": decision,
        "selected_pool": selected,
        "selected_added": selected_added,
        "group_breadth": group_breadth_df,
        "oracle_by_group": oracle_by_group_df,
        "raw_source": raw_source,
        "excluded": excluded,
    }


def write_artifacts(*, output_root: Path, session_id: str | None = None, jobs_supported: int = 1) -> Path:
    payload = build_artifacts()
    final_session_id = session_id or _session_id()
    session_root = output_root / final_session_id
    session_root.mkdir(parents=True, exist_ok=False)

    payload["manifest"]["session_id"] = final_session_id
    payload["manifest"]["jobs_supported"] = int(jobs_supported or 1)
    payload["input_resolution"]["session_id"] = final_session_id
    payload["generation_summary"]["session_id"] = final_session_id
    payload["no_lookahead_audit"]["session_id"] = final_session_id
    payload["breadth_comparison"]["session_id"] = final_session_id
    payload["winner_audit"]["session_id"] = final_session_id
    payload["oracle_headroom"]["session_id"] = final_session_id
    payload["admission_cost"]["session_id"] = final_session_id
    payload["decision"]["session_id"] = final_session_id

    _write_json(session_root / "run_manifest.json", payload["manifest"])
    _write_json(session_root / "input_resolution.json", payload["input_resolution"])
    _write_json(session_root / "side_aware_min_pool_generation_summary.json", payload["generation_summary"])
    _write_json(session_root / "side_aware_min_pool_no_lookahead_audit.json", payload["no_lookahead_audit"])
    _write_json(session_root / "side_aware_min_pool_breadth_comparison.json", payload["breadth_comparison"])
    _write_json(session_root / "side_aware_min_pool_winner_inclusion_audit.json", payload["winner_audit"])
    _write_json(session_root / "side_aware_min_pool_oracle_headroom_audit.json", payload["oracle_headroom"])
    _write_json(session_root / "side_aware_min_pool_admission_cost_audit.json", payload["admission_cost"])
    _write_json(session_root / "side_aware_min_pool_feasibility_v1_decision.json", payload["decision"])

    _write_parquet(session_root / "side_aware_min_pool_candidate_rows.parquet", payload["selected_pool"])
    _write_parquet(session_root / "side_aware_min_pool_group_breadth.parquet", payload["group_breadth"])
    _write_parquet(session_root / "side_aware_min_pool_added_candidate_rows.parquet", payload["selected_added"])
    _write_parquet(session_root / "side_aware_min_pool_oracle_by_group.parquet", payload["oracle_by_group"])
    _write_parquet(session_root / "side_aware_min_pool_admission_cost_rows.parquet", payload["selected_added"])

    complete = {
        "schema_version": ARTIFACT_COMPLETE_SCHEMA_VERSION,
        "generated_at": _utc_now(),
        "session_id": final_session_id,
        "required_artifacts": [
            "run_manifest.json",
            "input_resolution.json",
            "side_aware_min_pool_generation_summary.json",
            "side_aware_min_pool_no_lookahead_audit.json",
            "side_aware_min_pool_breadth_comparison.json",
            "side_aware_min_pool_winner_inclusion_audit.json",
            "side_aware_min_pool_oracle_headroom_audit.json",
            "side_aware_min_pool_admission_cost_audit.json",
            "side_aware_min_pool_feasibility_v1_decision.json",
            "_ARTIFACT_COMPLETE.json",
        ],
        "optional_artifacts": [
            "side_aware_min_pool_candidate_rows.parquet",
            "side_aware_min_pool_group_breadth.parquet",
            "side_aware_min_pool_added_candidate_rows.parquet",
            "side_aware_min_pool_oracle_by_group.parquet",
            "side_aware_min_pool_admission_cost_rows.parquet",
        ],
        "status": "complete",
    }
    _write_json(session_root / "_ARTIFACT_COMPLETE.json", complete)
    return session_root


def main() -> int:
    parser = argparse.ArgumentParser(description="Build a side-aware minimum candidate pool feasibility surface.")
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
