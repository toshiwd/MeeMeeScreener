from __future__ import annotations

import argparse
import sys
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.tradex_iizuka_pre_decisive_long_candidate_v2 import (  # noqa: E402
    _json_ready,
    _load_frame,
    _load_json,
    _metric_bundle,
    _safe_bool,
    _safe_float,
    _write_json,
    _write_parquet,
)

SCRIPT_NAME = "tradex_iizuka_pre_decisive_long_candidate_v2_top10_safe_ordering_v1"
SCHEMA_VERSION = "tradex_iizuka_pre_decisive_long_candidate_v2_top10_safe_ordering_v1"
MANIFEST_SCHEMA_VERSION = "tradex_iizuka_pre_decisive_long_candidate_v2_top10_safe_ordering_v1_manifest_v1"
INPUT_RESOLUTION_SCHEMA_VERSION = "tradex_iizuka_pre_decisive_long_candidate_v2_top10_safe_ordering_v1_input_resolution_v1"
CONTRACT_SCHEMA_VERSION = "tradex_iizuka_pre_decisive_long_candidate_v2_top10_safe_ordering_v1_contract_v1"
FAILURE_AUDIT_SCHEMA_VERSION = "tradex_iizuka_pre_decisive_long_candidate_v2_top10_safe_ordering_v1_failure_audit_v1"
COMPARISON_SCHEMA_VERSION = "tradex_iizuka_pre_decisive_long_candidate_v2_top10_safe_ordering_v1_comparison_v1"
MONTH_AUDIT_SCHEMA_VERSION = "tradex_iizuka_pre_decisive_long_candidate_v2_top10_safe_ordering_v1_month_audit_v1"
FAILURE_MODE_SCHEMA_VERSION = "tradex_iizuka_pre_decisive_long_candidate_v2_top10_safe_ordering_v1_failure_mode_v1"
NO_LOOKAHEAD_SCHEMA_VERSION = "tradex_iizuka_pre_decisive_long_candidate_v2_top10_safe_ordering_v1_no_lookahead_v1"
LEAKAGE_SCHEMA_VERSION = "tradex_iizuka_pre_decisive_long_candidate_v2_top10_safe_ordering_v1_leakage_v1"
DECISION_SCHEMA_VERSION = "tradex_iizuka_pre_decisive_long_candidate_v2_top10_safe_ordering_v1_decision_v1"

TOP_K_VALUES = (5, 10, 20)
EVAL_LABEL_COLUMNS = ("forward_ret_20d", "path_value_score_v1", "top15_label", "bottom15_label", "top20pct_label")

DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\iizuka_pre_decisive_long_candidate_v2_top10_safe_ordering")
APPROVED_V2_SESSION = Path(r"G:\Tradex\iizuka_pre_decisive_long_candidate_v2\20260503T122320Z-161925")
DROPPED_CHALLENGER_SESSION = Path(r"G:\Tradex\iizuka_pre_decisive_long_candidate_v2_challenger_design\20260503T124818Z-490993")

APPROVED_ACTIVE_ROWS = APPROVED_V2_SESSION / "iizuka_v2_active_candidate_rows.parquet"
APPROVED_ALL_ROLE_ROWS = APPROVED_V2_SESSION / "iizuka_v2_all_role_rows.parquet"
APPROVED_VARIANT_COMPARISON = APPROVED_V2_SESSION / "iizuka_v2_variant_pool_comparison.json"
APPROVED_FAILURE_AUDIT = APPROVED_V2_SESSION / "iizuka_v2_failure_mode_audit.json"
APPROVED_LINEAGE = APPROVED_V2_SESSION / "iizuka_v1_v2_lineage_comparison.json"

DROPPED_COMPARISON = DROPPED_CHALLENGER_SESSION / "v2_challenger_comparison.json"
DROPPED_DIFF = DROPPED_CHALLENGER_SESSION / "v2_challenger_topk_membership_diff.parquet"
DROPPED_FAILURE = DROPPED_CHALLENGER_SESSION / "v2_challenger_failure_mode_audit.json"
DROPPED_ROWS = DROPPED_CHALLENGER_SESSION / "v2_challenger_candidate_rows.parquet"


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _session_id() -> str:
    now = datetime.now(timezone.utc)
    return f"{now.strftime('%Y%m%dT%H%M%SZ')}-{now.microsecond:06d}"


def _ensure_exists(path: Path, label: str) -> Path:
    if not path.exists():
        raise FileNotFoundError(f"missing required source artifact for {label}: {path}")
    return path


def _month_bucket(frame: pd.DataFrame) -> pd.Series:
    return pd.to_datetime(frame["anchor_date"], errors="coerce").dt.strftime("%Y-%m")


def _load_inputs() -> dict[str, Any]:
    required = {
        "approved_active_rows": APPROVED_ACTIVE_ROWS,
        "approved_all_role_rows": APPROVED_ALL_ROLE_ROWS,
        "approved_variant_comparison": APPROVED_VARIANT_COMPARISON,
        "approved_failure_audit": APPROVED_FAILURE_AUDIT,
        "approved_lineage": APPROVED_LINEAGE,
        "dropped_comparison": DROPPED_COMPARISON,
        "dropped_diff": DROPPED_DIFF,
        "dropped_failure": DROPPED_FAILURE,
        "dropped_rows": DROPPED_ROWS,
    }
    for label, path in required.items():
        _ensure_exists(path, label)
    return {
        "approved_active_rows": _load_frame(required["approved_active_rows"]),
        "approved_all_role_rows": _load_frame(required["approved_all_role_rows"]),
        "approved_variant_comparison": _load_json(required["approved_variant_comparison"]),
        "approved_failure_audit": _load_json(required["approved_failure_audit"]),
        "approved_lineage": _load_json(required["approved_lineage"]),
        "dropped_comparison": _load_json(required["dropped_comparison"]),
        "dropped_diff": _load_frame(required["dropped_diff"]),
        "dropped_failure": _load_json(required["dropped_failure"]),
        "dropped_rows": _load_frame(required["dropped_rows"]),
    }


def _build_manifest(output_root: Path, session_root: Path) -> dict[str, Any]:
    return {
        "schema_version": MANIFEST_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "script_name": SCRIPT_NAME,
        "session_id": session_root.name,
        "output_root": str(output_root),
        "research_only": True,
        "boundary": "TRADEX-only",
        "source_artifacts": {
            "approved_active_rows": str(APPROVED_ACTIVE_ROWS),
            "approved_all_role_rows": str(APPROVED_ALL_ROLE_ROWS),
            "approved_variant_comparison": str(APPROVED_VARIANT_COMPARISON),
            "approved_failure_audit": str(APPROVED_FAILURE_AUDIT),
            "approved_lineage": str(APPROVED_LINEAGE),
            "dropped_comparison": str(DROPPED_COMPARISON),
            "dropped_diff": str(DROPPED_DIFF),
            "dropped_failure": str(DROPPED_FAILURE),
            "dropped_rows": str(DROPPED_ROWS),
        },
        "notes": [
            "single top10-safe ordering hypothesis only",
            "approved v2 gate unchanged",
            "dropped challenger frozen and only audited",
            "no MeeMee changes",
            "no production ranking changes",
            "no publish or promotion mutation",
            "no research_inventory.json mutation",
        ],
    }


def _build_input_resolution(output_root: Path, session_root: Path, inputs: dict[str, Any]) -> dict[str, Any]:
    approved = inputs["approved_active_rows"]
    return {
        "schema_version": INPUT_RESOLUTION_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "output_root": str(output_root),
        "session_root": str(session_root),
        "approved_v2_session": str(APPROVED_V2_SESSION),
        "dropped_challenger_session": str(DROPPED_CHALLENGER_SESSION),
        "row_coverage": {
            "approved_v2_active_rows": int(len(approved)),
            "approved_v2_active_groups": int(approved["anchor_date"].nunique()) if "anchor_date" in approved.columns else 0,
            "approved_v2_active_symbols": int(approved["symbol"].nunique()) if "symbol" in approved.columns else 0,
            "approved_v2_active_months": int(_month_bucket(approved).nunique()) if len(approved) else 0,
        },
        "artifacts_present": {
            "approved_variant_comparison": True,
            "approved_failure_audit": True,
            "approved_lineage": True,
            "dropped_comparison": True,
            "dropped_diff": True,
            "dropped_failure": True,
            "dropped_rows": True,
        },
        "notes": [
            "approved v2 active rows are the candidate universe",
            "dropped challenger artifacts are used only for failure auditing",
            "top20pct_label is still unavailable in the approved bundle and is not imputed",
        ],
    }


def _safe_abs(value: Any) -> float:
    if value is None:
        return float("inf")
    try:
        if pd.isna(value):
            return float("inf")
    except Exception:
        pass
    return abs(float(value))


def _build_failure_audit(inputs: dict[str, Any]) -> dict[str, Any]:
    approved = inputs["approved_active_rows"]
    diff = inputs["dropped_diff"].copy()
    k10 = diff.loc[diff["top_k"] == 10].copy()
    merged = k10.merge(
        approved[
            [
                "surface_key",
                "anchor_date",
                "symbol",
                "signal_quality_bucket",
                "volume_participation_bucket",
                "decision_candle_quality",
                "shape_classification",
                "support_wick",
                "bull_engulfing",
                "close_vs_ma20_pct",
                "close_vs_ma60_pct",
                "iizuka_v2_candidate_score",
                "iizuka_v2_candidate_rank",
                "iizuka_v2_reason",
                "forward_ret_20d",
                "path_value_score_v1",
                "top15_label",
                "bottom15_label",
            ]
        ],
        on="surface_key",
        how="left",
    )
    entrants = merged.loc[(~merged["selected_in_v2_active"]) & (merged["selected_in_challenger"])].copy()
    leavers = merged.loc[(merged["selected_in_v2_active"]) & (~merged["selected_in_challenger"])].copy()
    sort_cols = [col for col in ("forward_ret_20d", "path_value_score_v1", "iizuka_v2_candidate_score") if col in entrants.columns]
    entrant_preview = entrants.sort_values(sort_cols, ascending=[False] * len(sort_cols), kind="stable").head(20) if sort_cols else entrants.head(20)
    leaver_preview = leavers.sort_values(sort_cols, ascending=[False] * len(sort_cols), kind="stable").head(20) if sort_cols else leavers.head(20)
    return {
        "schema_version": FAILURE_AUDIT_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "baseline": "approved_v2_active_surface",
        "challenger": "dropped_stability_first_rerank",
        "top10_entry_count": int(len(entrants)),
        "top10_leaver_count": int(len(leavers)),
        "entrants": {
            "rows": _records(
                entrant_preview,
                [
                    "anchor_date",
                    "symbol",
                    "surface_key",
                    "signal_quality_bucket",
                    "volume_participation_bucket",
                    "decision_candle_quality",
                    "shape_classification",
                    "support_wick",
                    "bull_engulfing",
                    "close_vs_ma20_pct",
                    "close_vs_ma60_pct",
                    "iizuka_v2_candidate_score",
                    "iizuka_v2_reason",
                    "forward_ret_20d",
                    "top15_label",
                    "bottom15_label",
                ],
            ),
            "signal_quality_bucket": {str(k): int(v) for k, v in entrants["signal_quality_bucket"].value_counts().items()},
            "volume_participation_bucket": {str(k): int(v) for k, v in entrants["volume_participation_bucket"].value_counts().items()},
            "decision_candle_quality": {str(k): int(v) for k, v in entrants["decision_candle_quality"].value_counts().items()},
            "shape_classification": {str(k): int(v) for k, v in entrants["shape_classification"].value_counts().items()},
            "support_wick_rate": float(entrants["support_wick"].fillna(False).astype(bool).mean()) if len(entrants) else None,
            "bull_engulfing_rate": float(entrants["bull_engulfing"].fillna(False).astype(bool).mean()) if len(entrants) else None,
            "ma20_abs_mean": float(entrants["close_vs_ma20_pct"].abs().mean()) if len(entrants) else None,
            "ma60_abs_mean": float(entrants["close_vs_ma60_pct"].abs().mean()) if len(entrants) else None,
        },
        "leavers": {
            "rows": _records(
                leaver_preview,
                [
                    "anchor_date",
                    "symbol",
                    "surface_key",
                    "signal_quality_bucket",
                    "volume_participation_bucket",
                    "decision_candle_quality",
                    "shape_classification",
                    "support_wick",
                    "bull_engulfing",
                    "close_vs_ma20_pct",
                    "close_vs_ma60_pct",
                    "iizuka_v2_candidate_score",
                    "iizuka_v2_reason",
                    "forward_ret_20d",
                    "top15_label",
                    "bottom15_label",
                ],
            ),
            "signal_quality_bucket": {str(k): int(v) for k, v in leavers["signal_quality_bucket"].value_counts().items()},
            "volume_participation_bucket": {str(k): int(v) for k, v in leavers["volume_participation_bucket"].value_counts().items()},
            "decision_candle_quality": {str(k): int(v) for k, v in leavers["decision_candle_quality"].value_counts().items()},
            "shape_classification": {str(k): int(v) for k, v in leavers["shape_classification"].value_counts().items()},
            "support_wick_rate": float(leavers["support_wick"].fillna(False).astype(bool).mean()) if len(leavers) else None,
            "bull_engulfing_rate": float(leavers["bull_engulfing"].fillna(False).astype(bool).mean()) if len(leavers) else None,
            "ma20_abs_mean": float(leavers["close_vs_ma20_pct"].abs().mean()) if len(leavers) else None,
            "ma60_abs_mean": float(leavers["close_vs_ma60_pct"].abs().mean()) if len(leavers) else None,
        },
        "interpretation": [
            "dropped challenger over-weighted signal_quality_bucket and volume_participation_bucket",
            "entrants were mostly signal_quality_high or signal_quality_mid with volume_neutral",
            "leavers included more volume_weak_with_support and confirmed_with_reversal rows with better realized returns",
        ],
    }


def _build_safe_frame(frame: pd.DataFrame) -> pd.DataFrame:
    out = frame.copy()
    if "research_fallback_label_source" not in out.columns:
        out["research_fallback_label_source"] = "ml_label_20d"
    out["candidate_contract_name"] = "v2_score_anchored_top10_safe_ordering_v1"
    out["research_only"] = True
    safe_score = pd.to_numeric(out["iizuka_v2_candidate_score"], errors="coerce").fillna(0.0)
    safe_score = safe_score \
        + 0.020 * out.get("support_wick", False).fillna(False).astype(bool).astype(int) \
        + 0.010 * out.get("bull_engulfing", False).fillna(False).astype(bool).astype(int) \
        + 0.005 * out.get("decision_candle_quality", "").eq("candle_strong").astype(int) \
        + 0.005 * out.get("shape_classification", "").eq("shape_positive_modifier").astype(int) \
        - 0.015 * out.get("close_vs_ma20_pct", 0.0).abs().fillna(0.0) \
        - 0.005 * out.get("close_vs_ma60_pct", 0.0).abs().fillna(0.0)
    out["top10_safe_candidate_score"] = safe_score.astype(float)
    out["top10_safe_order_reason"] = out.apply(
        lambda row: "|".join(
            [
                f"v2score={row['iizuka_v2_candidate_score']:.4f}",
                "support_wick" if _safe_bool(row.get("support_wick")) else "no_support_wick",
                "bull_engulfing" if _safe_bool(row.get("bull_engulfing")) else "no_bull_engulfing",
                str(row.get("decision_candle_quality", "")),
                str(row.get("shape_classification", "")),
            ]
        ),
        axis=1,
    )
    sort_cols = [
        "anchor_date",
        "top10_safe_candidate_score",
        "iizuka_v2_candidate_score",
        "champion_rank",
        "symbol",
    ]
    out = out.sort_values(sort_cols, ascending=[True, False, False, True, True], kind="stable").reset_index(drop=True)
    out["top10_safe_candidate_rank"] = out.groupby("anchor_date").cumcount() + 1
    return out


def _records(frame: pd.DataFrame, fields: list[str]) -> list[dict[str, Any]]:
    if frame.empty:
        return []
    use = [field for field in fields if field in frame.columns]
    return [_json_ready(dict(row)) for row in frame[use].to_dict(orient="records")]


def _metric_for_selection(frame: pd.DataFrame) -> dict[str, Any]:
    metrics = _metric_bundle(frame)
    row_count = int(metrics["row_count"])
    metrics["group_count"] = int(frame["anchor_date"].nunique()) if "anchor_date" in frame.columns and len(frame) else 0
    metrics["symbol_count"] = int(frame["symbol"].nunique()) if "symbol" in frame.columns and len(frame) else 0
    metrics["month_count"] = int(_month_bucket(frame).nunique()) if len(frame) else 0
    metrics["median_forward_ret_20d"] = float(pd.to_numeric(frame["forward_ret_20d"], errors="coerce").median()) if len(frame) else None
    metrics["median_path_value_score_v1"] = float(pd.to_numeric(frame["path_value_score_v1"], errors="coerce").median()) if len(frame) else None
    metrics["top15_capture_rate"] = float(metrics["top15_count"] / row_count) if row_count else None
    metrics["bottom15_contamination_rate"] = float(metrics["bottom15_count"] / row_count) if row_count else None
    metrics["top15_to_bottom15_ratio"] = float(metrics["top15_count"] / max(metrics["bottom15_count"], 1)) if row_count else None
    metrics["non_positive_return_rate"] = float(pd.to_numeric(frame["forward_ret_20d"], errors="coerce").le(0).mean()) if len(frame) else None
    metrics["top20pct_available"] = "top20pct_label" in frame.columns
    metrics["top20pct_rate"] = float(pd.to_numeric(frame["top20pct_label"], errors="coerce").mean()) if "top20pct_label" in frame.columns and len(frame) else None
    symbol_counts = Counter(frame["symbol"].astype(str).tolist()) if len(frame) and "symbol" in frame.columns else Counter()
    metrics["top_symbol_counts"] = {str(symbol): int(count) for symbol, count in symbol_counts.most_common(10)}
    metrics["top_symbol_share"] = float(sum(count for _, count in symbol_counts.most_common(1)) / row_count) if row_count and symbol_counts else None
    metrics["top5_symbol_share"] = float(sum(count for _, count in symbol_counts.most_common(5)) / row_count) if row_count and symbol_counts else None
    return metrics


def _select_topk(frame: pd.DataFrame, score_col: str, k: int) -> pd.DataFrame:
    selected = frame.copy()
    selected = selected.sort_values(["anchor_date", score_col, "champion_rank", "symbol"], ascending=[True, False, True, True], kind="stable").reset_index(drop=True)
    selected["selected_rank"] = selected.groupby("anchor_date").cumcount() + 1
    return selected.loc[selected["selected_rank"] <= k].copy()


def _compare_topk(frame: pd.DataFrame) -> tuple[dict[str, Any], pd.DataFrame, dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    diff_rows: list[pd.DataFrame] = []
    safe = frame.loc[frame["top10_safe_candidate_rank"].notna()].copy()
    dropped = frame.loc[frame["v2_challenger_candidate_rank"].notna()].copy()
    approved_v2 = frame.loc[frame["iizuka_v2_role"] == "active"].copy()
    for k in TOP_K_VALUES:
        champion_sel = _select_topk(frame, "champion_score", k)
        v1_sel = _select_topk(frame, "iizuka_candidate_score", k)
        v2_sel = _select_topk(approved_v2, "iizuka_v2_candidate_score", k)
        dropped_sel = _select_topk(dropped, "v2_challenger_candidate_score", k)
        safe_sel = _select_topk(safe, "top10_safe_candidate_score", k)

        champ_keys = set(champion_sel["surface_key"].astype(str))
        v1_keys = set(v1_sel["surface_key"].astype(str))
        v2_keys = set(v2_sel["surface_key"].astype(str))
        dropped_keys = set(dropped_sel["surface_key"].astype(str))
        safe_keys = set(safe_sel["surface_key"].astype(str))
        union = champ_keys | v1_keys | v2_keys | dropped_keys | safe_keys
        diff = pd.DataFrame({"top_k": k, "surface_key": list(union)})
        diff = diff.merge(
            frame[[
                "surface_key",
                "anchor_date",
                "symbol",
                "side",
                "champion_score",
                "champion_rank",
                "iizuka_candidate_score",
                "iizuka_candidate_rank",
                "iizuka_v2_candidate_score",
                "iizuka_v2_candidate_rank",
                "iizuka_v2_role",
                "iizuka_v2_reason",
                "v2_challenger_candidate_score",
                "v2_challenger_candidate_rank",
                "v2_challenger_order_reason",
                "top10_safe_candidate_score",
                "top10_safe_candidate_rank",
                "top10_safe_order_reason",
                "forward_ret_20d",
                "path_value_score_v1",
                "top15_label",
                "bottom15_label",
                *([c for c in ("top20pct_label",) if c in frame.columns]),
            ] + [c for c in ("month_bucket",) if c in frame.columns]],
            on="surface_key",
            how="left",
        )
        diff["selected_in_champion"] = diff["surface_key"].isin(champ_keys)
        diff["selected_in_v1"] = diff["surface_key"].isin(v1_keys)
        diff["selected_in_v2_active"] = diff["surface_key"].isin(v2_keys)
        diff["selected_in_dropped"] = diff["surface_key"].isin(dropped_keys)
        diff["selected_in_safe"] = diff["surface_key"].isin(safe_keys)
        diff["selection_state"] = diff.apply(
            lambda row: "|".join(
                [
                    label
                    for label, flag in (
                        ("champion", row["selected_in_champion"]),
                        ("v1", row["selected_in_v1"]),
                        ("v2_active", row["selected_in_v2_active"]),
                        ("dropped", row["selected_in_dropped"]),
                        ("safe", row["selected_in_safe"]),
                    )
                    if flag
                ]
            )
            or "none",
            axis=1,
        )
        diff["member_change_v2_safe"] = diff["selected_in_v2_active"] != diff["selected_in_safe"]
        diff["member_change_champion_safe"] = diff["selected_in_champion"] != diff["selected_in_safe"]
        diff["member_change_v1_safe"] = diff["selected_in_v1"] != diff["selected_in_safe"]
        diff["member_change_dropped_safe"] = diff["selected_in_dropped"] != diff["selected_in_safe"]
        diff["top_k"] = k
        diff_rows.append(diff)

        rows.append(
            {
                "top_k": k,
                "champion": _metric_for_selection(champion_sel),
                "v1": _metric_for_selection(v1_sel),
                "approved_v2_active": _metric_for_selection(v2_sel),
                "dropped_challenger": _metric_for_selection(dropped_sel),
                "safe_challenger": _metric_for_selection(safe_sel),
                "changed_top5_members_count": int(len(v2_keys ^ safe_keys)) if k == 5 else None,
                "changed_top10_members_count": int(len(v2_keys ^ safe_keys)) if k == 10 else None,
                "changed_top20_members_count": int(len(v2_keys ^ safe_keys)) if k == 20 else None,
                "membership_changed_count_vs_v2": int(len(v2_keys ^ safe_keys)),
                "membership_changed_count_vs_dropped": int(len(dropped_keys ^ safe_keys)),
                "overlap_ratio_vs_v2": float(len(v2_keys & safe_keys) / len(v2_keys | safe_keys)) if (v2_keys | safe_keys) else None,
                "overlap_ratio_vs_dropped": float(len(dropped_keys & safe_keys) / len(dropped_keys | safe_keys)) if (dropped_keys | safe_keys) else None,
                "top15_retained_vs_v2": int(len(set(v2_sel["surface_key"]) & safe_keys)),
                "top15_lost_vs_v2": int(len(set(v2_sel["surface_key"]) - safe_keys)),
                "bottom15_removed_vs_v2": int(len(set(approved_v2.loc[approved_v2["bottom15_label"].fillna(False).astype(bool), "surface_key"]) - safe_keys)),
            }
        )

    comparison = {
        "schema_version": COMPARISON_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "candidate_contract_name": "v2_score_anchored_top10_safe_ordering_v1",
        "metric_mode": "per_anchor_date_topK",
        "top20pct_available": False,
        "top20pct_note": "top20pct_label is missing from the approved bundle and is not imputed",
        "per_k": rows,
    }
    diff_frame = pd.concat(diff_rows, ignore_index=True) if diff_rows else pd.DataFrame()
    failure_mode = {
        "schema_version": FAILURE_MODE_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "per_k": {},
    }
    for k in TOP_K_VALUES:
        safe_sel = _select_topk(safe, "top10_safe_candidate_score", k)
        approved_sel = _select_topk(approved_v2, "iizuka_v2_candidate_score", k)
        failure_mode["per_k"][str(k)] = {
            "reason_block_contribution_safe": {str(key): int(value) for key, value in safe_sel["top10_safe_order_reason"].value_counts().head(10).items()},
            "reason_block_contribution_approved_v2": {str(key): int(value) for key, value in approved_sel["iizuka_v2_reason"].value_counts().head(10).items()},
            "false_positive_cost_safe": {
                "bottom15_count": int(pd.to_numeric(safe_sel["bottom15_label"], errors="coerce").fillna(0).sum()) if len(safe_sel) else 0,
                "bottom15_rate": float(pd.to_numeric(safe_sel["bottom15_label"], errors="coerce").mean()) if len(safe_sel) else None,
            },
            "false_positive_cost_approved_v2": {
                "bottom15_count": int(pd.to_numeric(approved_sel["bottom15_label"], errors="coerce").fillna(0).sum()) if len(approved_sel) else 0,
                "bottom15_rate": float(pd.to_numeric(approved_sel["bottom15_label"], errors="coerce").mean()) if len(approved_sel) else None,
            },
        }
    return comparison, diff_frame, failure_mode


def _build_month_audit(frame: pd.DataFrame) -> dict[str, Any]:
    safe = frame.loc[frame["top10_safe_candidate_rank"].notna()].copy()
    approved_v2 = frame.loc[frame["iizuka_v2_role"] == "active"].copy()
    active_months = _month_bucket(approved_v2).value_counts().sort_index()
    per_k: dict[str, list[dict[str, Any]]] = {}
    for k in TOP_K_VALUES:
        safe_sel = _select_topk(safe, "top10_safe_candidate_score", k).assign(month=_month_bucket(_select_topk(safe, "top10_safe_candidate_score", k)))
        v2_sel = _select_topk(approved_v2, "iizuka_v2_candidate_score", k).assign(month=_month_bucket(_select_topk(approved_v2, "iizuka_v2_candidate_score", k)))
        month_entries = []
        for month in active_months.index.tolist():
            safe_month = safe_sel.loc[safe_sel["month"] == month].copy()
            v2_month = v2_sel.loc[v2_sel["month"] == month].copy()
            safe_metrics = _metric_for_selection(safe_month)
            v2_metrics = _metric_for_selection(v2_month)
            month_entries.append(
                {
                    "month": month,
                    "safe_row_count": int(safe_metrics["row_count"]),
                    "safe_share_of_topk": float(safe_metrics["row_count"] / max(len(safe_sel), 1)) if len(safe_sel) else None,
                    "safe_mean_forward_ret_20d": safe_metrics["mean_forward_ret_20d"],
                    "safe_median_forward_ret_20d": safe_metrics["median_forward_ret_20d"],
                    "safe_bottom15_contamination_rate": safe_metrics["bottom15_contamination_rate"],
                    "v2_active_row_count": int(v2_metrics["row_count"]),
                    "v2_active_share_of_topk": float(v2_metrics["row_count"] / max(len(v2_sel), 1)) if len(v2_sel) else None,
                    "v2_active_mean_forward_ret_20d": v2_metrics["mean_forward_ret_20d"],
                    "v2_active_median_forward_ret_20d": v2_metrics["median_forward_ret_20d"],
                    "v2_active_bottom15_contamination_rate": v2_metrics["bottom15_contamination_rate"],
                    "delta_mean_forward_ret_20d": _safe_float((safe_metrics["mean_forward_ret_20d"] or 0.0) - (v2_metrics["mean_forward_ret_20d"] or 0.0)) if safe_metrics["mean_forward_ret_20d"] is not None and v2_metrics["mean_forward_ret_20d"] is not None else None,
                    "delta_bottom15_contamination_rate": _safe_float((safe_metrics["bottom15_contamination_rate"] or 0.0) - (v2_metrics["bottom15_contamination_rate"] or 0.0)) if safe_metrics["bottom15_contamination_rate"] is not None and v2_metrics["bottom15_contamination_rate"] is not None else None,
                }
            )
        per_k[str(k)] = month_entries
    global_safe_sel = _select_topk(safe, "top10_safe_candidate_score", 20)
    global_v2_sel = _select_topk(approved_v2, "iizuka_v2_candidate_score", 20)
    delta_records = []
    for month in active_months.index.tolist():
        safe_month = global_safe_sel.assign(month=_month_bucket(global_safe_sel)).loc[lambda d: d["month"] == month]
        v2_month = global_v2_sel.assign(month=_month_bucket(global_v2_sel)).loc[lambda d: d["month"] == month]
        safe_metrics = _metric_for_selection(safe_month)
        v2_metrics = _metric_for_selection(v2_month)
        delta_records.append(
            {
                "month": month,
                "safe_mean_forward_ret_20d": safe_metrics["mean_forward_ret_20d"],
                "v2_active_mean_forward_ret_20d": v2_metrics["mean_forward_ret_20d"],
                "safe_bottom15_contamination_rate": safe_metrics["bottom15_contamination_rate"],
                "v2_active_bottom15_contamination_rate": v2_metrics["bottom15_contamination_rate"],
                "delta_mean_forward_ret_20d": _safe_float((safe_metrics["mean_forward_ret_20d"] or 0.0) - (v2_metrics["mean_forward_ret_20d"] or 0.0)) if safe_metrics["mean_forward_ret_20d"] is not None and v2_metrics["mean_forward_ret_20d"] is not None else None,
            }
        )
    total_weight = 0.0
    month_weights = []
    for record in delta_records:
        weight = abs(record["delta_mean_forward_ret_20d"] or 0.0) * max(int(active_months.get(record["month"], 0)), 1)
        month_weights.append((record["month"], weight))
        total_weight += weight
    dominant_month = None
    dominant_share = None
    if month_weights:
        dominant_month, dominant_weight = max(month_weights, key=lambda item: item[1])
        dominant_share = float(dominant_weight / total_weight) if total_weight else None
    return {
        "schema_version": MONTH_AUDIT_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "active_month_count": int(len(active_months)),
        "active_month_rows": {str(month): int(count) for month, count in active_months.items()},
        "per_k": per_k,
        "top20_month_delta": delta_records,
        "improvement_concentration": {
            "dominant_month": dominant_month,
            "dominant_month_share": dominant_share,
            "one_month_dominated": bool(dominant_share is not None and dominant_share >= 0.60),
        },
        "notes": [
            "month audit compares safe challenger against approved v2 active surface",
            "approved v2 active breadth is only three months, so breadth remains a constraint even when month dominance is absent",
        ],
    }


def _build_no_lookahead_audit(frame: pd.DataFrame) -> dict[str, Any]:
    flag_violations: dict[str, int] = {}
    for field in ("monthly_context_no_lookahead", "weekly_context_no_lookahead"):
        if field in frame.columns:
            flag_violations[f"{field}_false_count"] = int((~frame[field].fillna(False).astype(bool)).sum())
    date_violations: dict[str, int] = {}
    for field in ("monthly_context_date", "weekly_context_date"):
        if field in frame.columns:
            asof = pd.to_datetime(frame["anchor_date"], errors="coerce")
            context = pd.to_datetime(frame[field], errors="coerce")
            date_violations[f"{field}_future_count"] = int((context > asof).sum())
    return {
        "schema_version": NO_LOOKAHEAD_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "no_lookahead_pass": all(value == 0 for value in flag_violations.values()) and all(value == 0 for value in date_violations.values()),
        "flag_violations": flag_violations,
        "date_violations": date_violations,
        "notes": [
            "safe challenger uses only row-local non-outcome fields",
            "evaluation labels are attached after candidate construction",
        ],
    }


def _build_leakage_audit(frame: pd.DataFrame) -> dict[str, Any]:
    feature_fields_used = {
        "iizuka_v2_candidate_score",
        "support_wick",
        "bull_engulfing",
        "decision_candle_quality",
        "shape_classification",
        "close_vs_ma20_pct",
        "close_vs_ma60_pct",
        "top10_safe_candidate_score",
        "top10_safe_order_reason",
        "top10_safe_candidate_rank",
    }
    outcome_fields = set(EVAL_LABEL_COLUMNS)
    return {
        "schema_version": LEAKAGE_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "feature_fields_used": sorted(feature_fields_used),
        "outcome_fields": sorted(outcome_fields),
        "outcome_fields_used_as_features": sorted(feature_fields_used.intersection(outcome_fields)),
        "outcome_fields_attached_after_candidate_construction": sorted([field for field in EVAL_LABEL_COLUMNS if field in frame.columns]),
        "leakage_free": not feature_fields_used.intersection(outcome_fields),
        "note": "safe challenger uses approved v2 active row features only",
    }


def _build_contract(frame: pd.DataFrame) -> dict[str, Any]:
    return {
        "schema_version": CONTRACT_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "candidate_contract_name": "v2_score_anchored_top10_safe_ordering_v1",
        "scope": "TRADEX-only",
        "preserved": [
            "approved v2 gate",
            "long-side only",
            "approved active lane only",
            "no-lookahead contract",
            "label source policy",
        ],
        "ordering_principle": [
            "iizuka_v2_candidate_score as the primary anchor",
            "small support_wick and bull_engulfing boosts",
            "small positive bonuses for strong candle and positive shape",
            "small penalties for larger MA distance",
            "champion_rank and symbol as deterministic tie-breakers",
        ],
        "required_non_outcome_fields": [
            "iizuka_v2_candidate_score",
            "support_wick",
            "bull_engulfing",
            "decision_candle_quality",
            "shape_classification",
            "close_vs_ma20_pct",
            "close_vs_ma60_pct",
            "champion_rank",
        ],
        "required_non_outcome_fields_present": {field: field in frame.columns for field in [
            "iizuka_v2_candidate_score",
            "support_wick",
            "bull_engulfing",
            "decision_candle_quality",
            "shape_classification",
            "close_vs_ma20_pct",
            "close_vs_ma60_pct",
            "champion_rank",
        ]},
        "non_scope": [
            "no gate threshold changes",
            "no diagnostic-only to active promotion",
            "no outcome-field feature use",
            "no MeeMee changes",
            "no production ranking changes",
            "no publish or promotion mutation",
            "no research_inventory.json mutation",
        ],
        "notes": [
            "the dropped stability-first challenger is frozen and only audited",
            "this ordering is intentionally score-anchored to reduce top10 churn",
            "top20pct_label is unavailable in the approved bundle and is not imputed",
        ],
    }


def _build_decision(comparison: dict[str, Any], month_audit: dict[str, Any], no_lookahead: dict[str, Any], leakage: dict[str, Any]) -> dict[str, Any]:
    top10 = next(item for item in comparison["per_k"] if item["top_k"] == 10)
    top20 = next(item for item in comparison["per_k"] if item["top_k"] == 20)
    safe = top20["safe_challenger"]
    v2_top10 = top10["approved_v2_active"]
    v2_top20 = top20["approved_v2_active"]
    decision = "hold"
    reason = "safe ordering improves or matches the approved v2 active surface, but sample breadth remains only three months"
    if not no_lookahead["no_lookahead_pass"] or not leakage["leakage_free"]:
        decision = "drop"
        reason = "no-lookahead or leakage audit failed"
    elif safe["row_count"] == 0:
        decision = "drop"
        reason = "safe challenger selection is empty"
    else:
        top10_improves = top10["safe_challenger"]["mean_forward_ret_20d"] is not None and v2_top10["mean_forward_ret_20d"] is not None and top10["safe_challenger"]["mean_forward_ret_20d"] > v2_top10["mean_forward_ret_20d"]
        top20_improves = top20["safe_challenger"]["mean_forward_ret_20d"] is not None and v2_top20["mean_forward_ret_20d"] is not None and top20["safe_challenger"]["mean_forward_ret_20d"] >= v2_top20["mean_forward_ret_20d"]
        bottom15_ok = top20["safe_challenger"]["bottom15_contamination_rate"] is not None and v2_top20["bottom15_contamination_rate"] is not None and top20["safe_challenger"]["bottom15_contamination_rate"] <= v2_top20["bottom15_contamination_rate"]
        non_positive_ok = top20["safe_challenger"]["non_positive_return_rate"] is not None and v2_top20["non_positive_return_rate"] is not None and top20["safe_challenger"]["non_positive_return_rate"] <= v2_top20["non_positive_return_rate"]
        membership_changed_enough = bool(top10["changed_top10_members_count"] is not None and top10["changed_top10_members_count"] >= 1)
        not_month_dominated = not month_audit["improvement_concentration"]["one_month_dominated"]
        breadth_narrow = month_audit["active_month_count"] <= 3
        if top10_improves and top20_improves and bottom15_ok and non_positive_ok and membership_changed_enough and not_month_dominated and not breadth_narrow:
            decision = "keep"
            reason = "safe ordering improves top10 and top20 without worsening contamination or non-positive rate and is not month-dominated"
        elif top10_improves and top20_improves and bottom15_ok and non_positive_ok and membership_changed_enough and not_month_dominated and breadth_narrow:
            decision = "hold"
            reason = "safe ordering is promising but breadth is still only three months, so keep is not yet justified"
        elif top10_improves and top20_improves and bottom15_ok and non_positive_ok and not_month_dominated:
            decision = "hold"
            reason = "safe ordering improves the approved v2 active surface, but breadth remains narrow enough to hold"
        elif not top10_improves or not top20_improves:
            decision = "drop"
            reason = "safe ordering does not improve top10 and top20 versus approved v2 active"
        elif not bottom15_ok:
            decision = "drop"
            reason = "safe ordering worsens bottom15 contamination versus approved v2 active"
        elif not non_positive_ok:
            decision = "drop"
            reason = "safe ordering worsens non-positive return rate versus approved v2 active"
        elif not membership_changed_enough:
            decision = "drop"
            reason = "safe ordering is effectively identical to approved v2 active"
        elif month_audit["improvement_concentration"]["one_month_dominated"]:
            decision = "hold"
            reason = "improvement is month-dominated, so keep is not justified"
    return {
            "schema_version": DECISION_SCHEMA_VERSION,
            "generated_at_utc": _utc_now(),
            "decision": decision,
            "status": decision,
            "reason": reason,
            "candidate_contract_name": "v2_score_anchored_top10_safe_ordering_v1",
            "summary": {
                "approved_v2_active_row_count": int(v2_top20["row_count"]),
                "safe_row_count": int(safe["row_count"]),
                "safe_group_count": int(safe["group_count"]),
                "safe_symbol_count": int(safe["symbol_count"]),
                "safe_month_count": int(safe["month_count"]),
                "no_lookahead_pass": bool(no_lookahead["no_lookahead_pass"]),
            "leakage_free": bool(leakage["leakage_free"]),
            "top20pct_available": False,
        },
        "comparison_snapshot": {
            "top10": top10,
            "top20": top20,
        },
        "month_audit_snapshot": month_audit,
        "notes": [
            "safe ordering is score anchored to reduce churn",
            "top20pct_label remains unavailable in the approved bundle and is not imputed",
        ],
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="TRADEX Iizuka pre-decisive long candidate v2 score anchored top10 safe ordering")
    parser.add_argument("--output-root", type=str, default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--jobs", type=int, default=2)
    args = parser.parse_args()

    output_root = Path(args.output_root).expanduser().resolve()
    session_root = output_root / _session_id()
    session_root.mkdir(parents=True, exist_ok=True)

    inputs = _load_inputs()
    approved_active = inputs["approved_active_rows"]
    approved_all_role = inputs["approved_all_role_rows"]
    dropped_rows = inputs["dropped_rows"]

    safe_frame = _build_safe_frame(approved_active)

    comparison_frame = approved_all_role.merge(
        safe_frame[[
            "surface_key",
            "top10_safe_candidate_score",
            "top10_safe_candidate_rank",
            "top10_safe_order_reason",
        ]],
        on="surface_key",
        how="left",
    ).merge(
        dropped_rows[[
            "surface_key",
            "v2_challenger_candidate_score",
            "v2_challenger_candidate_rank",
            "v2_challenger_order_reason",
        ]],
        on="surface_key",
        how="left",
    )

    failure_audit = _build_failure_audit(inputs)
    comparison, diff_frame, failure_mode = _compare_topk(comparison_frame)
    month_audit = _build_month_audit(comparison_frame)
    no_lookahead = _build_no_lookahead_audit(safe_frame)
    leakage = _build_leakage_audit(safe_frame)
    contract = _build_contract(safe_frame)
    decision = _build_decision(comparison, month_audit, no_lookahead, leakage)

    _write_json(session_root / "run_manifest.json", _build_manifest(output_root, session_root))
    _write_json(session_root / "input_resolution.json", _build_input_resolution(output_root, session_root, inputs))
    _write_json(session_root / "top10_safe_ordering_contract.json", contract)
    _write_json(session_root / "top10_safe_failure_audit.json", failure_audit)
    _write_parquet(session_root / "top10_safe_candidate_rows.parquet", safe_frame)
    _write_json(session_root / "top10_safe_comparison.json", comparison)
    _write_parquet(session_root / "top10_safe_topk_membership_diff.parquet", diff_frame)
    _write_json(session_root / "top10_safe_month_dependence_audit.json", month_audit)
    _write_json(session_root / "top10_safe_failure_mode_audit.json", failure_mode)
    _write_json(session_root / "top10_safe_no_lookahead_audit.json", no_lookahead)
    _write_json(session_root / "top10_safe_leakage_audit.json", leakage)
    _write_json(session_root / "top10_safe_decision.json", decision)
    _write_parquet(
        session_root / "v2_challenger_candidate_examples.parquet",
        safe_frame.head(50)[[
            "anchor_date",
            "symbol",
            "iizuka_v2_reason",
            "top10_safe_order_reason",
            "forward_ret_20d",
            "path_value_score_v1",
            "top15_label",
            "bottom15_label",
            "top10_safe_candidate_score",
            "top10_safe_candidate_rank",
        ]].copy(),
    )
    _write_parquet(
        session_root / "v2_challenger_reason_block_summary.parquet",
        safe_frame[[
            "anchor_date",
            "symbol",
            "iizuka_v2_reason",
            "top10_safe_order_reason",
            "forward_ret_20d",
            "path_value_score_v1",
            "top15_label",
            "bottom15_label",
        ]].copy(),
    )
    _write_json(
        session_root / "_ARTIFACT_COMPLETE.json",
        {
            "schema_version": SCHEMA_VERSION,
            "generated_at_utc": _utc_now(),
            "session_root": str(session_root),
            "output_root": str(output_root),
            "artifact_count": 13,
            "artifacts": [
                "run_manifest.json",
                "input_resolution.json",
                "top10_safe_ordering_contract.json",
                "top10_safe_failure_audit.json",
                "top10_safe_candidate_rows.parquet",
                "top10_safe_comparison.json",
                "top10_safe_topk_membership_diff.parquet",
                "top10_safe_month_dependence_audit.json",
                "top10_safe_failure_mode_audit.json",
                "top10_safe_no_lookahead_audit.json",
                "top10_safe_leakage_audit.json",
                "top10_safe_decision.json",
                "_ARTIFACT_COMPLETE.json",
            ],
            "decision": decision["decision"],
        },
    )


if __name__ == "__main__":
    main()
