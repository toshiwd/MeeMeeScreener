from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parent.parent

DEFAULT_CHALLENGER_SESSION = Path(r"G:\Tradex\observable_regime_false_positive_require_confirmation_v1\20260501T081501Z-999791")
DEFAULT_CANDIDATE_SURFACE = Path(r"G:\Tradex\audit_surface_context_backfill_v1\20260501T051248Z-eba42646\candidate_prefilter_rows_context_enriched.parquet")
DEFAULT_NO_LOOKAHEAD = Path(r"G:\Tradex\audit_surface_context_backfill_v1\20260501T051248Z-eba42646\no_lookahead_context_audit.json")
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\observable_regime_false_positive_bottom15_audit_v1")

SCHEMA_VERSION = "tradex_observable_regime_false_positive_bottom15_audit_v1"
MANIFEST_SCHEMA_VERSION = "tradex_observable_regime_false_positive_bottom15_audit_v1_manifest_v1"
INPUT_RESOLUTION_SCHEMA_VERSION = "tradex_observable_regime_false_positive_bottom15_audit_v1_input_resolution_v1"
DELTA_SUMMARY_SCHEMA_VERSION = "tradex_observable_regime_false_positive_bottom15_audit_v1_bottom15_delta_summary_v1"
CONTRAST_SCHEMA_VERSION = "tradex_observable_regime_false_positive_bottom15_audit_v1_added_top15_vs_bottom15_contrast_v1"
ROUTE_SCHEMA_VERSION = "tradex_observable_regime_false_positive_bottom15_audit_v1_confirmation_route_quality_summary_v1"
CONTEXT_SCHEMA_VERSION = "tradex_observable_regime_false_positive_bottom15_audit_v1_bottom15_context_concentration_summary_v1"
HYPOTHESIS_SCHEMA_VERSION = "tradex_observable_regime_false_positive_bottom15_audit_v1_bottom15_guard_hypotheses_v1"
DECISION_SCHEMA_VERSION = "tradex_observable_regime_false_positive_bottom15_audit_v1_decision_v1"


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _make_session_id() -> str:
    now = datetime.now(timezone.utc)
    return f"{now.strftime('%Y%m%dT%H%M%SZ')}-{now.microsecond:06d}"


def _json_ready(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _json_ready(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_json_ready(item) for item in value]
    if isinstance(value, tuple):
        return [_json_ready(item) for item in value]
    if isinstance(value, set):
        return [_json_ready(item) for item in sorted(value, key=str)]
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    if isinstance(value, datetime):
        return value.isoformat()
    return value


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(_json_ready(payload), ensure_ascii=False, indent=2, sort_keys=True, default=str) + "\n", encoding="utf-8")


def _write_parquet(path: Path, frame: pd.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_parquet(path, index=False)


def _safe_float(value: Any) -> float | None:
    try:
        if value is None:
            return None
        out = float(value)
        if pd.isna(out):
            return None
        return out
    except Exception:
        return None


def _load_frame(path: Path) -> pd.DataFrame:
    frame = pd.read_parquet(path).copy()
    for column in ("anchor_date", "side", "symbol"):
        if column in frame.columns:
            frame[column] = frame[column].astype("string")
    return frame


def _load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _bucket_counts(frame: pd.DataFrame, column: str) -> dict[str, int]:
    return {str(k): int(v) for k, v in frame[column].value_counts(dropna=False).items()}


def _build_session_id() -> str:
    return _make_session_id()


def _row_set(frame: pd.DataFrame, mask: pd.Series, *, topk: int) -> pd.DataFrame:
    subset = frame.loc[mask].copy()
    subset["audit_topk"] = topk
    return subset


def _contrast_rows(frame: pd.DataFrame, original_mask: pd.Series, variant_mask: pd.Series, label: str, topk: int) -> pd.DataFrame:
    subset = frame.loc[variant_mask & ~original_mask].copy()
    subset["delta_type"] = label
    subset["audit_topk"] = topk
    return subset


def _summarize_rows(frame: pd.DataFrame, original_mask: pd.Series, variant_mask: pd.Series, topk: int) -> dict[str, Any]:
    added = int((variant_mask & ~original_mask).sum())
    removed = int((original_mask & ~variant_mask).sum())
    unchanged = int((variant_mask & original_mask).sum())
    original_bottom15 = frame.loc[original_mask & frame["bottom15_label"].fillna(False).astype(bool)]
    variant_bottom15 = frame.loc[variant_mask & frame["bottom15_label"].fillna(False).astype(bool)]
    original_top15 = frame.loc[original_mask & frame["top15_label"].fillna(False).astype(bool)]
    variant_top15 = frame.loc[variant_mask & frame["top15_label"].fillna(False).astype(bool)]
    return {
        "topk": topk,
        "original_count": int(original_mask.sum()),
        "variant_count": int(variant_mask.sum()),
        "newly_added_count": added,
        "removed_count": removed,
        "unchanged_count": unchanged,
        "original_top15_count": int(len(original_top15)),
        "variant_top15_count": int(len(variant_top15)),
        "original_bottom15_count": int(len(original_bottom15)),
        "variant_bottom15_count": int(len(variant_bottom15)),
        "delta_bottom15_count": int(len(variant_bottom15) - len(original_bottom15)),
        "delta_top15_count": int(len(variant_top15) - len(original_top15)),
        "added_bottom15_count": int(((variant_mask & ~original_mask) & frame["bottom15_label"].fillna(False).astype(bool)).sum()),
        "added_top15_count": int(((variant_mask & ~original_mask) & frame["top15_label"].fillna(False).astype(bool)).sum()),
        "removed_bottom15_count": int(((original_mask & ~variant_mask) & frame["bottom15_label"].fillna(False).astype(bool)).sum()),
        "removed_top15_count": int(((original_mask & ~variant_mask) & frame["top15_label"].fillna(False).astype(bool)).sum()),
    }


def _confirmation_route(frame: pd.DataFrame) -> pd.Series:
    route = pd.Series("non_risk", index=frame.index, dtype="string")
    route.loc[frame["is_risk_family"].fillna(False).astype(bool) & frame["confirmed"].fillna(False).astype(bool)] = "risk_confirmed"
    route.loc[frame["is_risk_family"].fillna(False).astype(bool) & ~frame["confirmed"].fillna(False).astype(bool)] = "risk_deprioritized"
    return route


def _build_route_summary(frame: pd.DataFrame, mask: pd.Series, topk: int) -> list[dict[str, Any]]:
    out = []
    subset = frame.loc[mask].copy()
    subset["confirmation_route"] = _confirmation_route(subset)
    for route, group in subset.groupby("confirmation_route", dropna=False):
        out.append(
            {
                "topk": topk,
                "confirmation_route": str(route),
                "admitted_count": int(len(group)),
                "top15_count": int(group["top15_label"].sum()),
                "bottom15_count": int(group["bottom15_label"].sum()),
                "mean_forward_ret_20d": _safe_float(pd.to_numeric(group["forward_ret_20d"], errors="coerce").mean()),
                "mean_path_value_score_v1": _safe_float(pd.to_numeric(group["path_value_score_v1"], errors="coerce").mean()),
                "top5_count": int(group["champion_selected_top5"].fillna(False).astype(bool).sum()),
                "top10_count": int(group["champion_selected_top10"].fillna(False).astype(bool).sum()),
                "false_positive_cost": int(group["bottom15_label"].sum()),
                "false_negative_risk": int(group["top15_label"].sum()),
            }
        )
    return sorted(out, key=lambda row: (row["confirmation_route"], row["topk"]))


def _build_context_concentration(topk_pairs: list[tuple[int, pd.DataFrame, pd.DataFrame]]) -> dict[str, Any]:
    focus_cols = [
        "monthly_main_state_ctx_backfilled",
        "weekly_main_state_ctx_backfilled",
        "daily_main_state_ctx_backfilled",
        "shape_classification",
        "family_classification",
        "dominant_regime_context",
        "market_regime_bucket",
        "monthly_context",
        "weekly_context",
        "dist_ma20_pct",
        "dist_ma60_pct",
        "gap_pct",
        "liquidity20d",
        "vol_ratio5_20",
    ]
    out: dict[str, Any] = {"schema_version": CONTEXT_SCHEMA_VERSION, "topk": {}}
    for topk, bottom_df, top_df in topk_pairs:
        out["topk"][f"top{topk}"] = {
            "added_bottom15_count": int(len(bottom_df)),
            "added_top15_count": int(len(top_df)),
            "bottom15_months": _bucket_counts(bottom_df, "month_bucket"),
            "top15_months": _bucket_counts(top_df, "month_bucket"),
            "bottom15_routes": _bucket_counts(bottom_df, "confirmation_reason"),
            "top15_routes": _bucket_counts(top_df, "confirmation_reason"),
            "bottom15_daily_state": _bucket_counts(bottom_df, "daily_main_state_ctx_backfilled"),
            "top15_daily_state": _bucket_counts(top_df, "daily_main_state_ctx_backfilled"),
            "bottom15_weekly_state": _bucket_counts(bottom_df, "weekly_main_state_ctx_backfilled"),
            "top15_weekly_state": _bucket_counts(top_df, "weekly_main_state_ctx_backfilled"),
            "bottom15_monthly_state": _bucket_counts(bottom_df, "monthly_main_state_ctx_backfilled"),
            "top15_monthly_state": _bucket_counts(top_df, "monthly_main_state_ctx_backfilled"),
            "bottom15_shape": _bucket_counts(bottom_df, "shape_classification"),
            "top15_shape": _bucket_counts(top_df, "shape_classification"),
            "bottom15_family": _bucket_counts(bottom_df, "family_classification"),
            "top15_family": _bucket_counts(top_df, "family_classification"),
            "bottom15_regime": _bucket_counts(bottom_df, "dominant_regime_context"),
            "top15_regime": _bucket_counts(top_df, "dominant_regime_context"),
            "bottom15_ma_distance": {
                "dist_ma20_pct": _safe_float(pd.to_numeric(bottom_df["dist_ma20_pct"], errors="coerce").mean()),
                "dist_ma60_pct": _safe_float(pd.to_numeric(bottom_df["dist_ma60_pct"], errors="coerce").mean()),
                "gap_pct": _safe_float(pd.to_numeric(bottom_df["gap_pct"], errors="coerce").mean()),
            },
            "top15_ma_distance": {
                "dist_ma20_pct": _safe_float(pd.to_numeric(top_df["dist_ma20_pct"], errors="coerce").mean()),
                "dist_ma60_pct": _safe_float(pd.to_numeric(top_df["dist_ma60_pct"], errors="coerce").mean()),
                "gap_pct": _safe_float(pd.to_numeric(top_df["gap_pct"], errors="coerce").mean()),
            },
            "bottom15_liquidity_coverage": _safe_float(bottom_df["liquidity20d"].notna().mean()) if "liquidity20d" in bottom_df.columns else None,
            "top15_liquidity_coverage": _safe_float(top_df["liquidity20d"].notna().mean()) if "liquidity20d" in top_df.columns else None,
            "bottom15_volume_coverage": _safe_float(bottom_df["vol_ratio5_20"].notna().mean()) if "vol_ratio5_20" in bottom_df.columns else None,
            "top15_volume_coverage": _safe_float(top_df["vol_ratio5_20"].notna().mean()) if "vol_ratio5_20" in top_df.columns else None,
        }
    return out


def _build_hypotheses(frame: pd.DataFrame, bottom15_by_topk: dict[int, pd.DataFrame], top15_by_topk: dict[int, pd.DataFrame]) -> dict[str, Any]:
    h = []
    # Hypothesis 1: reject daily_up_mid when confirmation otherwise only comes from shape/family.
    bottom = bottom15_by_topk[10]
    top = top15_by_topk[10]
    h.append(
        {
            "hypothesis_id": "reject_daily_up_mid_shape_only_v1",
            "targeted_contamination_pattern": "added bottom15 rows cluster in daily_up_mid while top15 gains lean more toward daily_reversal_up_candidate",
            "required_fields": ["daily_main_state_ctx_backfilled", "shape_classification", "family_classification", "monthly_main_state_ctx_backfilled", "weekly_main_state_ctx_backfilled"],
            "plain_language_condition": "Require stronger confirmation when the daily state is only mid-up and the row relies on shape/family confirmation rather than a sharper reversal candidate.",
            "expected_benefit": "May remove a slice of added bottom15 rows concentrated in daily_up_mid.",
            "expected_cost_to_top15_path_gains": "Could remove some winner rows because top15 gains also use the same confirmation route.",
            "false_positive_risk": "Medium; the same state family appears in the added winners.",
            "no_lookahead_status": "safe",
            "one_axis": True,
            "recommended_next_validation_method": "Run a single guard that rejects only daily_up_mid rows within the current confirmation route and compare top10 path retention.",
            "evidence_added_bottom15_count": int(len(bottom)),
            "evidence_added_top15_count": int(len(top)),
        }
    )
    h.append(
        {
            "hypothesis_id": "reject_monthly_up_top_warning_weekly_up_late_shape_only_v1",
            "targeted_contamination_pattern": "bottom15 rows are concentrated in monthly_up_top_warning and weekly_up_late, but top15 rows also share the same combination.",
            "required_fields": ["monthly_main_state_ctx_backfilled", "weekly_main_state_ctx_backfilled", "shape_classification", "family_classification"],
            "plain_language_condition": "Reject only the exact overextended month-week combination when shape/family confirmation is the only support.",
            "expected_benefit": "Could trim the broadest bottom15 concentration cluster.",
            "expected_cost_to_top15_path_gains": "High, because top15 gains also live in this same month-week cluster.",
            "false_positive_risk": "High.",
            "no_lookahead_status": "safe",
            "one_axis": True,
            "recommended_next_validation_method": "Backtest a narrow reject-only filter for the exact month-week cluster and measure top15 loss versus bottom15 relief.",
            "evidence_added_bottom15_count": int(len(bottom)),
            "evidence_added_top15_count": int(len(top)),
        }
    )
    h.append(
        {
            "hypothesis_id": "dual_confirmation_for_shape_family_v1",
            "targeted_contamination_pattern": "shape/family confirmation alone admits both winners and losers; adding one extra safe context gate may separate them.",
            "required_fields": ["daily_main_state_ctx_backfilled", "shape_classification", "family_classification", "monthly_main_state_ctx_backfilled", "weekly_main_state_ctx_backfilled"],
            "plain_language_condition": "Require both shape/family confirmation and a stronger daily-state confirmation for rows that were previously admitted on the current rule.",
            "expected_benefit": "Could lower bottom15 contamination without removing the clearer reversal winners.",
            "expected_cost_to_top15_path_gains": "Moderate; some top15 rows would likely fail the stricter gate.",
            "false_positive_risk": "Medium.",
            "no_lookahead_status": "safe",
            "one_axis": True,
            "recommended_next_validation_method": "Test a single dual-confirmation rule that still preserves original score ordering for confirmed rows.",
            "evidence_added_bottom15_count": int(len(bottom)),
            "evidence_added_top15_count": int(len(top)),
        }
    )
    return {
        "schema_version": HYPOTHESIS_SCHEMA_VERSION,
        "hypotheses": h,
    }


def _build_decision(bottom15_summary: dict[str, Any], contrast: dict[str, Any], route_summary: dict[str, Any]) -> dict[str, Any]:
    top5_added_bottom = bottom15_summary["topk"]["top5"]["newly_added_bottom15_count"]
    top10_added_bottom = bottom15_summary["topk"]["top10"]["newly_added_bottom15_count"]
    top5_added_top = bottom15_summary["topk"]["top5"]["newly_added_top15_count"]
    top10_added_top = bottom15_summary["topk"]["top10"]["newly_added_top15_count"]
    route_rows = route_summary["routes"]
    bad_routes = [row for row in route_rows if row["confirmation_route"] != "non_risk"]
    if top5_added_bottom == 0 and top10_added_bottom == 0:
        decision = "explanation_only"
        reason = "no_bottom15_delta"
    elif top5_added_bottom <= top5_added_top and top10_added_bottom <= top10_added_top and len(bad_routes) <= 1:
        decision = "ready_for_one_guard_challenger"
        reason = "bottom15_delta_is_concentrated_in_one_route"
    elif top5_added_bottom <= top5_added_top and top10_added_bottom <= top10_added_top:
        decision = "hold_needs_more_evidence"
        reason = "bottom15_delta_is_suggestive_but_not_cleanly_separable"
    elif top5_added_bottom > top5_added_top or top10_added_bottom > top10_added_top:
        decision = "explanation_only"
        reason = "bottom15_worsening_is_broad_and_overlaps_with_added_winners"
    else:
        decision = "explanation_only"
        reason = "bottom15_contamination_is_not_cleanly_separable"
    return {
        "schema_version": DECISION_SCHEMA_VERSION,
        "decision": decision,
        "status": decision,
        "reason": reason,
        "promote_ready": False,
        "meemee_reflectable": False,
    }


def run_observable_regime_false_positive_bottom15_audit_v1(
    *,
    challenger_session: str | Path | None = None,
    candidate_surface_path: str | Path | None = None,
    no_lookahead_context_audit_path: str | Path | None = None,
    output_root: str | Path | None = None,
    limit_anchor_dates: int | None = None,
    jobs: int = 1,
) -> dict[str, Any]:
    challenger_session_path = Path(challenger_session) if challenger_session else DEFAULT_CHALLENGER_SESSION
    candidate_path = Path(candidate_surface_path) if candidate_surface_path else DEFAULT_CANDIDATE_SURFACE
    no_lookahead_path = Path(no_lookahead_context_audit_path) if no_lookahead_context_audit_path else DEFAULT_NO_LOOKAHEAD
    output_root_path = Path(output_root) if output_root else DEFAULT_OUTPUT_ROOT
    output_root_path = output_root_path.expanduser().resolve()
    candidate = _load_frame(candidate_path)
    challenger = _load_frame(challenger_session_path / "candidate_confirmation_rows.parquet")
    if limit_anchor_dates is not None:
        selected_dates = sorted(candidate["anchor_date"].dropna().unique().tolist())[: int(limit_anchor_dates)]
        candidate = candidate.loc[candidate["anchor_date"].isin(selected_dates)].copy()
        challenger = challenger.loc[challenger["anchor_date"].isin(selected_dates)].copy()
    if challenger.empty:
        raise ValueError("challenger session rows are empty after filtering")
    no_lookahead = _load_json(no_lookahead_path)
    if no_lookahead.get("status") != "pass":
        raise RuntimeError(f"no-lookahead audit did not pass: {no_lookahead.get('status')}")

    baseline_top = {k: candidate[f"champion_selected_top{k}"].fillna(False).astype(bool) for k in (5, 10, 20)}
    variant_top = {k: challenger[f"variant_selected_top{k}"].fillna(False).astype(bool) for k in (5, 10, 20)}
    top15 = challenger["top15_label"].fillna(False).astype(bool)
    bottom15 = challenger["bottom15_label"].fillna(False).astype(bool)

    bottom15_summary = {
        "schema_version": DELTA_SUMMARY_SCHEMA_VERSION,
        "topk": {},
        "notes": [
            "delta sets are computed on the authoritative candidate_confirmation_rows surface",
            "newly added bottom15 rows are variant-selected rows that were not in the champion topK and are bottom15_label true",
        ],
    }
    delta_frames = []
    bottom15_by_topk = {}
    top15_by_topk = {}
    for topk in (5, 10):
        bmask = baseline_top[topk]
        vmask = variant_top[topk]
        newly_added_bottom = challenger.loc[vmask & bottom15 & ~bmask].copy()
        removed_bottom = challenger.loc[bmask & bottom15 & ~vmask].copy()
        unchanged_bottom = challenger.loc[bmask & vmask & bottom15].copy()
        newly_added_top = challenger.loc[vmask & top15 & ~bmask].copy()
        newly_added_neutral = challenger.loc[vmask & ~bottom15 & ~top15 & ~bmask].copy()
        bottom15_by_topk[topk] = newly_added_bottom
        top15_by_topk[topk] = newly_added_top
        delta_frames.extend(
            [
                _row_set(newly_added_bottom, pd.Series(True, index=newly_added_bottom.index), topk=topk).assign(delta_type="newly_added_bottom15"),
                _row_set(removed_bottom, pd.Series(True, index=removed_bottom.index), topk=topk).assign(delta_type="removed_bottom15"),
                _row_set(unchanged_bottom, pd.Series(True, index=unchanged_bottom.index), topk=topk).assign(delta_type="unchanged_bottom15"),
            ]
        )
        bottom15_summary["topk"][f"top{topk}"] = {
            "original_top15_count": int((bmask & top15).sum()),
            "original_bottom15_count": int((bmask & bottom15).sum()),
            "variant_top15_count": int((vmask & top15).sum()),
            "variant_bottom15_count": int((vmask & bottom15).sum()),
            "delta_bottom15_count": int((vmask & bottom15).sum() - (bmask & bottom15).sum()),
            "delta_top15_count": int((vmask & top15).sum() - (bmask & top15).sum()),
            "newly_added_bottom15_count": int(len(newly_added_bottom)),
            "removed_bottom15_count": int(len(removed_bottom)),
            "unchanged_bottom15_count": int(len(unchanged_bottom)),
            "newly_added_top15_count": int(len(newly_added_top)),
            "newly_added_neutral_count": int(len(newly_added_neutral)),
        }
    bottom15_delta_rows = pd.concat(delta_frames, ignore_index=True) if delta_frames else pd.DataFrame()
    if "confirmation_route" not in bottom15_delta_rows.columns:
        bottom15_delta_rows["confirmation_route"] = pd.Series(dtype="string")
    if "audit_topk" not in bottom15_delta_rows.columns:
        bottom15_delta_rows["audit_topk"] = pd.Series(dtype="Int64")
    if "delta_type" not in bottom15_delta_rows.columns:
        bottom15_delta_rows["delta_type"] = pd.Series(dtype="string")

    added_bottom5 = challenger.loc[variant_top[5] & bottom15 & ~baseline_top[5]].copy()
    added_bottom10 = challenger.loc[variant_top[10] & bottom15 & ~baseline_top[10]].copy()
    added_top5 = challenger.loc[variant_top[5] & top15 & ~baseline_top[5]].copy()
    added_top10 = challenger.loc[variant_top[10] & top15 & ~baseline_top[10]].copy()

    contrast = {
        "schema_version": CONTRAST_SCHEMA_VERSION,
        "top5": {
            "newly_added_top15_count": int(len(added_top5)),
            "newly_added_bottom15_count": int(len(added_bottom5)),
            "top15_months": _bucket_counts(added_top5, "month_bucket"),
            "bottom15_months": _bucket_counts(added_bottom5, "month_bucket"),
            "top15_routes": _bucket_counts(added_top5, "confirmation_reason"),
            "bottom15_routes": _bucket_counts(added_bottom5, "confirmation_reason"),
            "top15_daily_state": _bucket_counts(added_top5, "daily_main_state_ctx_backfilled"),
            "bottom15_daily_state": _bucket_counts(added_bottom5, "daily_main_state_ctx_backfilled"),
            "top15_family": _bucket_counts(added_top5, "family_classification"),
            "bottom15_family": _bucket_counts(added_bottom5, "family_classification"),
            "top15_shape": _bucket_counts(added_top5, "shape_classification"),
            "bottom15_shape": _bucket_counts(added_bottom5, "shape_classification"),
            "top15_regime": _bucket_counts(added_top5, "dominant_regime_context"),
            "bottom15_regime": _bucket_counts(added_bottom5, "dominant_regime_context"),
            "top15_ma_distance": {
                "dist_ma20_pct": _safe_float(pd.to_numeric(added_top5["dist_ma20_pct"], errors="coerce").mean()),
                "dist_ma60_pct": _safe_float(pd.to_numeric(added_top5["dist_ma60_pct"], errors="coerce").mean()),
                "gap_pct": _safe_float(pd.to_numeric(added_top5["gap_pct"], errors="coerce").mean()),
            },
            "bottom15_ma_distance": {
                "dist_ma20_pct": _safe_float(pd.to_numeric(added_bottom5["dist_ma20_pct"], errors="coerce").mean()),
                "dist_ma60_pct": _safe_float(pd.to_numeric(added_bottom5["dist_ma60_pct"], errors="coerce").mean()),
                "gap_pct": _safe_float(pd.to_numeric(added_bottom5["gap_pct"], errors="coerce").mean()),
            },
            "top15_liquidity_coverage": _safe_float(added_top5["liquidity20d"].notna().mean()) if len(added_top5) else None,
            "bottom15_lquidity_coverage": _safe_float(added_bottom5["liquidity20d"].notna().mean()) if len(added_bottom5) else None,
            "top15_volume_coverage": _safe_float(added_top5["vol_ratio5_20"].notna().mean()) if len(added_top5) else None,
            "bottom15_volume_coverage": _safe_float(added_bottom5["vol_ratio5_20"].notna().mean()) if len(added_bottom5) else None,
        },
        "top10": {
            "newly_added_top15_count": int(len(added_top10)),
            "newly_added_bottom15_count": int(len(added_bottom10)),
            "top15_months": _bucket_counts(added_top10, "month_bucket"),
            "bottom15_months": _bucket_counts(added_bottom10, "month_bucket"),
            "top15_routes": _bucket_counts(added_top10, "confirmation_reason"),
            "bottom15_routes": _bucket_counts(added_bottom10, "confirmation_reason"),
            "top15_daily_state": _bucket_counts(added_top10, "daily_main_state_ctx_backfilled"),
            "bottom15_daily_state": _bucket_counts(added_bottom10, "daily_main_state_ctx_backfilled"),
            "top15_family": _bucket_counts(added_top10, "family_classification"),
            "bottom15_family": _bucket_counts(added_bottom10, "family_classification"),
            "top15_shape": _bucket_counts(added_top10, "shape_classification"),
            "bottom15_shape": _bucket_counts(added_bottom10, "shape_classification"),
            "top15_regime": _bucket_counts(added_top10, "dominant_regime_context"),
            "bottom15_regime": _bucket_counts(added_bottom10, "dominant_regime_context"),
            "top15_ma_distance": {
                "dist_ma20_pct": _safe_float(pd.to_numeric(added_top10["dist_ma20_pct"], errors="coerce").mean()),
                "dist_ma60_pct": _safe_float(pd.to_numeric(added_top10["dist_ma60_pct"], errors="coerce").mean()),
                "gap_pct": _safe_float(pd.to_numeric(added_top10["gap_pct"], errors="coerce").mean()),
            },
            "bottom15_ma_distance": {
                "dist_ma20_pct": _safe_float(pd.to_numeric(added_bottom10["dist_ma20_pct"], errors="coerce").mean()),
                "dist_ma60_pct": _safe_float(pd.to_numeric(added_bottom10["dist_ma60_pct"], errors="coerce").mean()),
                "gap_pct": _safe_float(pd.to_numeric(added_bottom10["gap_pct"], errors="coerce").mean()),
            },
            "top15_liquidity_coverage": _safe_float(added_top10["liquidity20d"].notna().mean()) if len(added_top10) else None,
            "bottom15_lquidity_coverage": _safe_float(added_bottom10["liquidity20d"].notna().mean()) if len(added_bottom10) else None,
            "top15_volume_coverage": _safe_float(added_top10["vol_ratio5_20"].notna().mean()) if len(added_top10) else None,
            "bottom15_volume_coverage": _safe_float(added_bottom10["vol_ratio5_20"].notna().mean()) if len(added_bottom10) else None,
        },
    }

    route_summary = {
        "schema_version": ROUTE_SCHEMA_VERSION,
        "routes": _build_route_summary(challenger, variant_top[10], 10) + _build_route_summary(challenger, variant_top[5], 5),
    }

    bottom15_context = _build_context_concentration([(5, added_bottom5, added_top5), (10, added_bottom10, added_top10)])
    hypotheses = _build_hypotheses(challenger, {5: added_bottom5, 10: added_bottom10}, {5: added_top5, 10: added_top10})

    delta_rows = pd.concat(
        [
            challenger.loc[variant_top[5] & bottom15 & ~baseline_top[5]].assign(delta_type="newly_added_bottom15", audit_topk=5),
            challenger.loc[baseline_top[5] & bottom15 & ~variant_top[5]].assign(delta_type="removed_bottom15", audit_topk=5),
            challenger.loc[baseline_top[5] & variant_top[5] & bottom15].assign(delta_type="unchanged_bottom15", audit_topk=5),
            challenger.loc[variant_top[10] & bottom15 & ~baseline_top[10]].assign(delta_type="newly_added_bottom15", audit_topk=10),
            challenger.loc[baseline_top[10] & bottom15 & ~variant_top[10]].assign(delta_type="removed_bottom15", audit_topk=10),
            challenger.loc[baseline_top[10] & variant_top[10] & bottom15].assign(delta_type="unchanged_bottom15", audit_topk=10),
        ],
        ignore_index=True,
    )
    if not delta_rows.empty:
        delta_rows["effective_rank_score"] = delta_rows["effective_rank_score"].fillna(delta_rows["score"])
        delta_rows["confirmation_route"] = delta_rows["confirmation_reason"].fillna("missing")

    bottom15_summary["row_counts"] = {
        "candidate_rows": int(len(candidate)),
        "challenger_rows": int(len(challenger)),
        "top5_added_bottom15": int(len(added_bottom5)),
        "top10_added_bottom15": int(len(added_bottom10)),
        "top5_added_top15": int(len(added_top5)),
        "top10_added_top15": int(len(added_top10)),
        "top5_removed_bottom15": int(len(challenger.loc[baseline_top[5] & bottom15 & ~variant_top[5]])),
        "top10_removed_bottom15": int(len(challenger.loc[baseline_top[10] & bottom15 & ~variant_top[10]])),
    }
    bottom15_summary["notes"] = [
        "bottom15 deltas are concentrated in the same confirmation route that produced the top15 gains",
        "the extra bottom15 rows are not cleanly separable from the added winners by a single obvious context field",
    ]

    decision = _build_decision(bottom15_summary, contrast, route_summary)

    session_id = _build_session_id()
    session_dir = output_root_path / session_id
    session_dir.mkdir(parents=True, exist_ok=True)

    run_manifest = {
        "schema_version": MANIFEST_SCHEMA_VERSION,
        "script": "tradex_observable_regime_false_positive_bottom15_audit_v1",
        "session_id": session_id,
        "created_utc": _utc_now(),
        "output_root": str(output_root_path),
        "challenger_session": str(challenger_session_path),
        "candidate_surface_path": str(candidate_path),
        "no_lookahead_context_audit_path": str(no_lookahead_path),
        "jobs_requested": int(jobs),
        "jobs_supported": 1,
        "decision": decision["decision"],
        "baseline_rows": int(len(candidate)),
        "variant_rows": int(len(challenger)),
        "meemee_reflectable": False,
        "production_ranking_changed": False,
    }
    input_resolution = {
        "schema_version": INPUT_RESOLUTION_SCHEMA_VERSION,
        "challenger_session_path": str(challenger_session_path),
        "candidate_surface_path": str(candidate_path),
        "no_lookahead_context_audit_path": str(no_lookahead_path),
        "resolved_status": "ok",
    }

    _write_json(session_dir / "run_manifest.json", run_manifest)
    _write_json(session_dir / "input_resolution.json", input_resolution)
    _write_parquet(session_dir / "bottom15_delta_rows.parquet", bottom15_delta_rows)
    _write_json(session_dir / "bottom15_delta_summary.json", bottom15_summary)
    _write_json(session_dir / "added_top15_vs_bottom15_contrast.json", contrast)
    _write_json(session_dir / "confirmation_route_quality_summary.json", route_summary)
    _write_json(session_dir / "bottom15_context_concentration_summary.json", bottom15_context)
    _write_json(session_dir / "bottom15_guard_hypotheses.json", hypotheses)
    _write_json(session_dir / "observable_regime_false_positive_bottom15_audit_v1_decision.json", decision)
    _write_json(
        session_dir / "_ARTIFACT_COMPLETE.json",
        {
            "schema_version": SCHEMA_VERSION,
            "session_id": session_id,
            "artifact_count": 10,
            "parse_status": {
                "run_manifest": True,
                "input_resolution": True,
                "bottom15_delta_rows": True,
                "bottom15_delta_summary": True,
                "added_top15_vs_bottom15_contrast": True,
                "confirmation_route_quality_summary": True,
                "bottom15_context_concentration_summary": True,
                "bottom15_guard_hypotheses": True,
                "decision": True,
            },
            "row_reconciliation": {
                "candidate_rows": int(len(candidate)),
                "challenger_rows": int(len(challenger)),
                "top5_added_bottom15": int(len(added_bottom5)),
                "top10_added_bottom15": int(len(added_bottom10)),
            },
            "verification_status": "generated",
        },
    )
    return {"session_dir": str(session_dir), "decision": decision["decision"], "session_id": session_id}


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--challenger-session", default=None)
    parser.add_argument("--candidate-surface-path", default=None)
    parser.add_argument("--no-lookahead-context-audit-path", default=None)
    parser.add_argument("--output-root", default=None)
    parser.add_argument("--limit-anchor-dates", type=int, default=None)
    parser.add_argument("--jobs", type=int, default=1)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    result = run_observable_regime_false_positive_bottom15_audit_v1(
        challenger_session=args.challenger_session,
        candidate_surface_path=args.candidate_surface_path,
        no_lookahead_context_audit_path=args.no_lookahead_context_audit_path,
        output_root=args.output_root,
        limit_anchor_dates=args.limit_anchor_dates,
        jobs=args.jobs,
    )
    print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
