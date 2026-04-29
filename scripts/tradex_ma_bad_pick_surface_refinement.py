from __future__ import annotations

import argparse
import json
import math
import shutil
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd


DEFAULT_SOURCE_FAMILY_SESSION = Path(r"G:\Tradex\ma_position_path_research_family_filter\20260429T062945Z-87844c56")
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\ma_position_path_research_family_bad_pick_refinement")

SCHEMA_VERSION = "tradex_ma_bad_pick_surface_refinement_v1"
WATERFALL_SCHEMA_VERSION = "tradex_ma_bad_pick_surface_waterfall_v1"
SHADOW_SCHEMA_VERSION = "tradex_ma_bad_pick_shadow_classification_v1"
COMPARE_SCHEMA_VERSION = "tradex_ma_bad_pick_relaxation_compare_v1"
DECISION_SCHEMA_VERSION = "tradex_ma_bad_pick_surface_refinement_decision_v1"
MANIFEST_SCHEMA_VERSION = "tradex_ma_bad_pick_surface_refinement_manifest_v1"

DEFAULT_MIN_SAMPLE_COUNT = 300
DEFAULT_MIN_UNIQUE_SYMBOL_COUNT = 30
DEFAULT_MIN_MONTH_COUNT = 12
DEFAULT_LIMIT_FAMILIES = 50
DEFAULT_RELAX_POSITIVE_MONTH_RATE = 0.50
DEFAULT_RELAX_MAE_MARGIN = 0.01


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


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
    path.write_text(json.dumps(_json_ready(payload), ensure_ascii=False, indent=2, sort_keys=True, default=str) + "\n", encoding="utf-8")


def _load_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"missing required source artifact: {path}")
    return json.loads(path.read_text(encoding="utf-8"))


def _safe_float(value: Any, fallback: float | None = None) -> float | None:
    if value is None:
        return fallback
    try:
        out = float(value)
    except Exception:
        return fallback
    if not math.isfinite(out):
        return fallback
    return float(out)


def _safe_int(value: Any, fallback: int | None = None) -> int | None:
    if value is None:
        return fallback
    try:
        return int(value)
    except Exception:
        return fallback


def _progress_log(message: str) -> None:
    print(f"[ma_bad_pick_refinement] {message}", file=sys.stderr, flush=True)


def _make_session_id() -> str:
    return f"{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}-{uuid.uuid4().hex[:8]}"


def _resolve_source_session(source_session: str | Path | None) -> Path:
    if source_session and str(source_session).strip():
        path = Path(str(source_session)).expanduser().resolve()
        if not path.exists():
            raise FileNotFoundError(f"source family session not found: {path}")
        return path
    if DEFAULT_SOURCE_FAMILY_SESSION.exists():
        return DEFAULT_SOURCE_FAMILY_SESSION.resolve()
    raise FileNotFoundError("Could not resolve source family session. Pass --source-family-session.")


def _load_source_family_session(source_session: Path) -> dict[str, Any]:
    manifest = _load_json(source_session / "run_manifest.json")
    summary = _load_json(source_session / "state_family_summary.json")
    by_regime = _load_json(source_session / "state_family_by_regime.json")
    monthly = _load_json(source_session / "state_family_monthly_stability.json")
    classification = _load_json(source_session / "state_family_classification.json")
    decision = _load_json(source_session / "state_family_filter_v1_decision.json")
    source_ma_session_path = Path(summary.get("source_session_path") or source_session)
    source_ma_manifest = _load_json(source_ma_session_path / "run_manifest.json")
    row_parquet = source_session / "state_family_rows.parquet"
    if not row_parquet.exists():
        raise FileNotFoundError(f"missing required source artifact: {row_parquet}")
    return {
        "manifest": manifest,
        "summary": summary,
        "by_regime": by_regime,
        "monthly": monthly,
        "classification": classification,
        "decision": decision,
        "source_ma_session_path": source_ma_session_path,
        "source_ma_manifest": source_ma_manifest,
        "row_parquet": row_parquet,
    }


def _finalize_session_dir(session_tmp: Path, session_final: Path) -> None:
    if session_final.exists():
        raise FileExistsError(f"final session output already exists: {session_final}")
    try:
        session_tmp.replace(session_final)
    except Exception:
        shutil.move(str(session_tmp), str(session_final))


def _aggregate_family_frame(row_parquet: Path, *, bottom15_score_threshold: float, top15_score_threshold: float) -> pd.DataFrame:
    conn = duckdb.connect()
    try:
        conn.execute(f"CREATE TEMP VIEW family_rows AS SELECT * FROM read_parquet('{row_parquet.as_posix()}')")
        base = conn.execute(
            f"""
            SELECT
                state_family_id,
                COUNT(*) AS sample_count,
                COUNT(DISTINCT code) AS unique_symbol_count,
                COUNT(DISTINCT trade_month) AS month_count,
                AVG(forward_ret_3d) AS mean_forward_ret_3d,
                AVG(forward_ret_5d) AS mean_forward_ret_5d,
                AVG(forward_ret_10d) AS mean_forward_ret_10d,
                AVG(forward_ret_20d) AS mean_forward_ret_20d,
                MEDIAN(forward_ret_20d) AS median_forward_ret_20d,
                AVG(mfe_20d) AS mean_mfe_20d,
                AVG(mae_20d) AS mean_mae_20d,
                AVG(path_value_score_v1) AS mean_path_value_score_v1,
                MEDIAN(path_value_score_v1) AS median_path_value_score_v1,
                AVG(hit_plus_5_before_minus_5) AS plus5_before_minus5_rate,
                AVG(hit_minus_5_before_plus_5) AS minus5_before_plus5_rate,
                AVG(CASE WHEN path_value_score_v1 >= {top15_score_threshold} THEN 1 ELSE 0 END) AS top15_rate,
                AVG(CASE WHEN path_value_score_v1 <= {bottom15_score_threshold} THEN 1 ELSE 0 END) AS bottom15_rate
            FROM family_rows
            GROUP BY 1
            """
        ).fetchdf()

        monthly_detail = conn.execute(
            """
            SELECT
                state_family_id,
                trade_month,
                COUNT(*) AS month_sample_count,
                AVG(path_value_score_v1) AS month_mean_path_value
            FROM family_rows
            GROUP BY 1, 2
            """
        ).fetchdf()
        monthly_rollup = (
            monthly_detail.groupby("state_family_id", dropna=False)
            .agg(
                months_observed=("trade_month", "nunique"),
                positive_month_rate=("month_mean_path_value", lambda s: float((s > 0).mean()) if len(s) else 0.0),
                worst_month_mean_path_value=("month_mean_path_value", "min"),
                best_month_mean_path_value=("month_mean_path_value", "max"),
                mean_monthly_path_value=("month_mean_path_value", "mean"),
                std_monthly_path_value=("month_mean_path_value", "std"),
                month_sample_count=("month_sample_count", "sum"),
            )
            .reset_index()
        )
        monthly_rollup["std_monthly_path_value"] = monthly_rollup["std_monthly_path_value"].fillna(0.0)

        regime_detail = conn.execute(
            """
            SELECT
                state_family_id,
                family_regime_context,
                COUNT(*) AS sample_count,
                COUNT(DISTINCT code) AS unique_symbol_count,
                AVG(forward_ret_20d) AS mean_forward_ret_20d,
                AVG(mae_20d) AS mean_mae_20d,
                AVG(path_value_score_v1) AS mean_path_value_score_v1,
                MIN(path_value_score_v1) AS min_path_value_score_v1,
                MAX(path_value_score_v1) AS max_path_value_score_v1
            FROM family_rows
            GROUP BY 1, 2
            """
        ).fetchdf()
        if regime_detail.empty:
            regime_rollup = pd.DataFrame(columns=[
                "state_family_id",
                "regime_count",
                "regime_consistency_score",
                "score_spread",
                "dominant_regime_context",
                "min_regime_mean_path_value",
                "max_regime_mean_path_value",
            ])
        else:
            dominant = regime_detail.loc[
                regime_detail.groupby("state_family_id")["sample_count"].idxmax(),
                ["state_family_id", "family_regime_context"],
            ].rename(columns={"family_regime_context": "dominant_regime_context"})
            regime_rollup = (
                regime_detail.groupby("state_family_id", dropna=False)
                .agg(
                    regime_count=("family_regime_context", "nunique"),
                    regime_consistency_score=("sample_count", lambda s: float(s.max() / s.sum()) if s.sum() else 0.0),
                    score_spread=("mean_path_value_score_v1", lambda s: float(s.max() - s.min()) if len(s) else 0.0),
                    min_regime_mean_path_value=("mean_path_value_score_v1", "min"),
                    max_regime_mean_path_value=("mean_path_value_score_v1", "max"),
                )
                .reset_index()
                .merge(dominant, on="state_family_id", how="left")
            )

        family_frame = base.merge(monthly_rollup, on="state_family_id", how="left").merge(regime_rollup, on="state_family_id", how="left")
        family_frame["std_monthly_path_value"] = family_frame["std_monthly_path_value"].fillna(0.0)
        family_frame["months_observed"] = family_frame["months_observed"].fillna(0).astype(int)
        family_frame["month_sample_count"] = family_frame["month_sample_count"].fillna(0.0)
        family_frame["regime_count"] = family_frame["regime_count"].fillna(0).astype(int)
        family_frame["regime_consistency_score"] = family_frame["regime_consistency_score"].fillna(0.0)
        family_frame["score_spread"] = family_frame["score_spread"].fillna(0.0)
        family_frame["min_regime_mean_path_value"] = family_frame["min_regime_mean_path_value"].fillna(family_frame["mean_path_value_score_v1"])
        family_frame["max_regime_mean_path_value"] = family_frame["max_regime_mean_path_value"].fillna(family_frame["mean_path_value_score_v1"])
        family_frame["dominant_regime_context"] = family_frame["dominant_regime_context"].fillna("unknown")
        return family_frame
    finally:
        conn.close()


def _count_stability_hits(row: pd.Series, thresholds: dict[str, Any]) -> int:
    hits = 0
    if _safe_float(row.get("median_path_value_score_v1")) is not None and _safe_float(row.get("median_path_value_score_v1")) < 0:
        hits += 1
    if _safe_float(row.get("minus5_before_plus5_rate")) is not None and _safe_float(row.get("minus5_before_plus5_rate")) > thresholds["baseline_minus5_before_plus5_rate"]:
        hits += 1
    if _safe_float(row.get("positive_month_rate")) is not None and _safe_float(row.get("positive_month_rate")) <= 0.45:
        hits += 1
    return hits


def _apply_labels(frame: pd.DataFrame, thresholds: dict[str, Any]) -> pd.DataFrame:
    out = frame.copy()
    sample_ok = (
        (out["sample_count"] >= thresholds["min_sample_count"]) &
        (out["unique_symbol_count"] >= thresholds["min_unique_symbol_count"]) &
        (out["month_count"] >= thresholds["min_month_count"])
    )
    core_bad = sample_ok & (out["mean_path_value_score_v1"] < thresholds["baseline_mean_path_value_score_v1"]) & (out["bottom15_rate"] > thresholds["baseline_bottom15_rate"])

    out["sample_qualified"] = sample_ok
    out["core_bad_pick"] = core_bad
    out["strict_bad_pick_family"] = core_bad & (out["median_path_value_score_v1"] < 0) & (out["minus5_before_plus5_rate"] > thresholds["baseline_minus5_before_plus5_rate"]) & (out["positive_month_rate"] <= 0.45)
    out["relaxed_bad_pick_family"] = core_bad & (out["positive_month_rate"] <= thresholds["relax_positive_month_rate"]) & (
        (out["median_path_value_score_v1"] < 0) | (out["mean_mae_20d"] <= thresholds["baseline_mean_mae_20d"] - thresholds["relax_mae_margin"])
    )

    stability_hits = out.apply(lambda row: _count_stability_hits(row, thresholds), axis=1)
    out["stability_hits_3"] = stability_hits
    out["bad_pick_watch_family"] = core_bad & (out["strict_bad_pick_family"] == False) & (out["stability_hits_3"] == 2)
    out["regime_bad_pick_family"] = (
        sample_ok
        & (~out["strict_bad_pick_family"])
        & (out["regime_count"] >= 2)
        & (out["regime_consistency_score"] < 0.70)
        & (out["score_spread"] >= 0.05)
        & (out["min_regime_mean_path_value"] < thresholds["baseline_mean_path_value_score_v1"])
    )
    out["mae_risk_family"] = (
        sample_ok
        & (~out["strict_bad_pick_family"])
        & (out["mean_mae_20d"] <= thresholds["baseline_mean_mae_20d"] - thresholds["relax_mae_margin"])
        & (out["minus5_before_plus5_rate"] > thresholds["baseline_minus5_before_plus5_rate"])
    )
    out["endpoint_bad_pick_family"] = (
        sample_ok
        & (~out["strict_bad_pick_family"])
        & (out["mean_forward_ret_20d"] <= thresholds["baseline_mean_forward_ret_20d"])
        & (out["bottom15_rate"] > thresholds["baseline_bottom15_rate"])
        & (
            (out["median_path_value_score_v1"] >= -0.01)
            | (out["mean_mae_20d"] > thresholds["baseline_mean_mae_20d"] - thresholds["relax_mae_margin"])
            | (out["positive_month_rate"] > 0.45)
        )
    )
    out["shadow_label_count"] = (
        out["bad_pick_watch_family"].astype(int)
        + out["regime_bad_pick_family"].astype(int)
        + out["mae_risk_family"].astype(int)
        + out["endpoint_bad_pick_family"].astype(int)
    )
    return out


def _summary_examples(frame: pd.DataFrame, label: str, *, limit: int) -> list[dict[str, Any]]:
    subset = frame.loc[frame[label]].copy()
    if subset.empty:
        return []
    if label == "bad_pick_watch_family":
        subset = subset.sort_values(["mean_path_value_score_v1", "sample_count", "positive_month_rate"], ascending=[True, False, True])
    elif label == "regime_bad_pick_family":
        subset = subset.sort_values(["score_spread", "regime_consistency_score", "sample_count"], ascending=[False, True, False])
    elif label == "mae_risk_family":
        subset = subset.sort_values(["mean_mae_20d", "minus5_before_plus5_rate", "sample_count"], ascending=[True, False, False])
    elif label == "endpoint_bad_pick_family":
        subset = subset.sort_values(["mean_forward_ret_20d", "bottom15_rate", "sample_count"], ascending=[True, False, False])
    elif label == "relaxed_bad_pick_family":
        subset = subset.sort_values(["mean_path_value_score_v1", "sample_count"], ascending=[True, False])
    else:
        subset = subset.sort_values(["mean_path_value_score_v1", "sample_count"], ascending=[True, False])
    return subset.head(limit).to_dict(orient="records")


def _stage_count(frame: pd.DataFrame, mask: pd.Series) -> int:
    return int(mask.fillna(False).sum())


def _build_waterfall(frame: pd.DataFrame, thresholds: dict[str, Any]) -> dict[str, Any]:
    sample_ok = frame["sample_qualified"]
    core_bad = frame["core_bad_pick"]
    path_negative = sample_ok & core_bad & (frame["median_path_value_score_v1"] < 0)
    downside_first = path_negative & (frame["minus5_before_plus5_rate"] > thresholds["baseline_minus5_before_plus5_rate"])
    bottom15_ok = downside_first & (frame["bottom15_rate"] > thresholds["baseline_bottom15_rate"])
    month_stable = bottom15_ok & (frame["positive_month_rate"] <= 0.45)
    strict_bad = frame["strict_bad_pick_family"]
    relaxed_bad = frame["relaxed_bad_pick_family"]
    return {
        "schema_version": WATERFALL_SCHEMA_VERSION,
        "generated_at": _utc_now(),
        "stages": [
            {
                "stage": "all_families",
                "count": int(len(frame)),
                "definition": "all unique state_family_id values in the verified family session",
            },
            {
                "stage": "sample_qualified",
                "count": _stage_count(frame, sample_ok),
                "definition": f"sample_count >= {thresholds['min_sample_count']} and unique_symbol_count >= {thresholds['min_unique_symbol_count']} and month_count >= {thresholds['min_month_count']}",
            },
            {
                "stage": "return_path_negative",
                "count": _stage_count(frame, path_negative),
                "definition": "sample-qualified families with mean_path_value_score_v1 below baseline and median_path_value_score_v1 below zero",
            },
            {
                "stage": "downside_first_qualified",
                "count": _stage_count(frame, downside_first),
                "definition": "return/path negative families with minus5_before_plus5_rate above baseline",
            },
            {
                "stage": "bottom15_qualified",
                "count": _stage_count(frame, bottom15_ok),
                "definition": "downside-first families with bottom15_rate above baseline",
            },
            {
                "stage": "month_stability_qualified",
                "count": _stage_count(frame, month_stable),
                "definition": "bottom15-qualified families with positive_month_rate <= 0.45",
            },
            {
                "stage": "final_stable_bad_pick_family",
                "count": _stage_count(frame, strict_bad),
                "definition": "the original strict bad-pick rule from the verified family filter session",
            },
            {
                "stage": "relaxed_bad_pick_family",
                "count": _stage_count(frame, relaxed_bad),
                "definition": f"strict core bad-pick signal with positive_month_rate <= {thresholds['relax_positive_month_rate']:.2f} and either median_path_value_score_v1 < 0 or mean_mae_20d <= baseline_mean_mae_20d - {thresholds['relax_mae_margin']:.2f}",
            },
        ],
    }


def _build_shadow_payload(frame: pd.DataFrame, *, limit_families: int) -> dict[str, Any]:
    shadow_counts = {
        "bad_pick_watch_family": int(frame["bad_pick_watch_family"].sum()),
        "regime_bad_pick_family": int(frame["regime_bad_pick_family"].sum()),
        "mae_risk_family": int(frame["mae_risk_family"].sum()),
        "endpoint_bad_pick_family": int(frame["endpoint_bad_pick_family"].sum()),
        "multi_label_family_count": int((frame["shadow_label_count"] >= 2).sum()),
        "strict_bad_pick_family": int(frame["strict_bad_pick_family"].sum()),
        "relaxed_bad_pick_family": int(frame["relaxed_bad_pick_family"].sum()),
    }
    return {
        "schema_version": SHADOW_SCHEMA_VERSION,
        "generated_at": _utc_now(),
        "shadow_counts": shadow_counts,
        "examples": {
            "bad_pick_watch_family": _summary_examples(frame, "bad_pick_watch_family", limit=limit_families),
            "regime_bad_pick_family": _summary_examples(frame, "regime_bad_pick_family", limit=limit_families),
            "mae_risk_family": _summary_examples(frame, "mae_risk_family", limit=limit_families),
            "endpoint_bad_pick_family": _summary_examples(frame, "endpoint_bad_pick_family", limit=limit_families),
        },
        "notes": [
            "Shadow labels are research-only and may overlap.",
            "The strict bad-pick rule is intentionally preserved as a baseline for comparison.",
        ],
    }


def _build_relaxation_compare(frame: pd.DataFrame, *, thresholds: dict[str, Any], limit_families: int) -> dict[str, Any]:
    strict = frame.loc[frame["strict_bad_pick_family"]].copy()
    relaxed = frame.loc[frame["relaxed_bad_pick_family"]].copy()
    relaxed_only = relaxed.loc[~relaxed["state_family_id"].isin(strict["state_family_id"])].copy()
    strict_only = strict.loc[~strict["state_family_id"].isin(relaxed["state_family_id"])].copy()
    return {
        "schema_version": COMPARE_SCHEMA_VERSION,
        "generated_at": _utc_now(),
        "strict_rule": {
            "positive_month_rate_max": 0.45,
            "median_path_value_score_v1_lt": 0.0,
            "minus5_before_plus5_rate_gt": thresholds["baseline_minus5_before_plus5_rate"],
            "bottom15_rate_gt": thresholds["baseline_bottom15_rate"],
        },
        "relaxed_rule": {
            "positive_month_rate_max": thresholds["relax_positive_month_rate"],
            "median_path_value_score_v1_lt": 0.0,
            "or_mean_mae_20d_le": thresholds["baseline_mean_mae_20d"] - thresholds["relax_mae_margin"],
            "bottom15_rate_gt": thresholds["baseline_bottom15_rate"],
        },
        "counts": {
            "strict_bad_pick_family_count": int(len(strict)),
            "relaxed_bad_pick_family_count": int(len(relaxed)),
            "delta": int(len(relaxed) - len(strict)),
            "new_relaxed_only_family_count": int(len(relaxed_only)),
            "strict_only_family_count": int(len(strict_only)),
        },
        "before_examples": strict.head(limit_families).to_dict(orient="records"),
        "after_examples": relaxed.head(limit_families).to_dict(orient="records"),
        "new_relaxed_only_examples": relaxed_only.sort_values(["mean_path_value_score_v1", "sample_count"], ascending=[True, False]).head(limit_families).to_dict(orient="records"),
    }


def _build_decision_payload(
    *,
    source_family_session_id: str,
    source_session_path: Path,
    family_frame: pd.DataFrame,
    source_artifacts: dict[str, str],
    thresholds: dict[str, Any],
    waterfall: dict[str, Any],
    shadow_payload: dict[str, Any],
    compare_payload: dict[str, Any],
    limit_families: int,
) -> dict[str, Any]:
    strict_count = int(family_frame["strict_bad_pick_family"].sum())
    relaxed_count = int(family_frame["relaxed_bad_pick_family"].sum())
    watch_count = int(family_frame["bad_pick_watch_family"].sum())
    regime_count = int(family_frame["regime_bad_pick_family"].sum())
    mae_count = int(family_frame["mae_risk_family"].sum())
    endpoint_count = int(family_frame["endpoint_bad_pick_family"].sum())
    if relaxed_count >= 30 and (watch_count >= 20 or regime_count >= 500):
        recommendation = "keep"
        reason = "bad_pick_surface_expanded_materially_without_collapsing_stability"
    elif relaxed_count >= 15 or watch_count >= 10 or regime_count >= 200:
        recommendation = "hold"
        reason = "bad_pick_signal_exists_but_is_still_threshold_sensitive_or_regime_dependent"
    else:
        recommendation = "drop"
        reason = "bad_pick_surface_remains_too_narrow_after_bounded_refinement"
    return {
        "schema_version": DECISION_SCHEMA_VERSION,
        "generated_at": _utc_now(),
        "source_family_session_id": source_family_session_id,
        "source_family_session_path": str(source_session_path),
        "source_artifacts": source_artifacts,
        "family_definition_frozen": {
            "state_family_id_contract": "frozen from the verified state_family_filter_v1 session; this refinement only reclassifies surface stability",
            "analysis_axes": [
                "strict_bad_pick_family",
                "relaxed_bad_pick_family",
                "bad_pick_watch_family",
                "regime_bad_pick_family",
                "mae_risk_family",
                "endpoint_bad_pick_family",
            ],
        },
        "thresholds": thresholds,
        "waterfall": waterfall["stages"],
        "shadow_counts": shadow_payload["shadow_counts"],
        "relaxation_compare": compare_payload["counts"],
        "family_counts": {
            "total_families": int(len(family_frame)),
            "strict_bad_pick_family_count": strict_count,
            "relaxed_bad_pick_family_count": relaxed_count,
            "bad_pick_watch_family_count": watch_count,
            "regime_bad_pick_family_count": regime_count,
            "mae_risk_family_count": mae_count,
            "endpoint_bad_pick_family_count": endpoint_count,
        },
        "top_50_strict_bad_pick_families": family_frame.loc[family_frame["strict_bad_pick_family"]].sort_values(
            ["mean_path_value_score_v1", "sample_count"], ascending=[True, False]
        ).head(min(50, limit_families)).to_dict(orient="records"),
        "top_50_relaxed_bad_pick_families": family_frame.loc[family_frame["relaxed_bad_pick_family"]].sort_values(
            ["mean_path_value_score_v1", "sample_count"], ascending=[True, False]
        ).head(min(50, limit_families)).to_dict(orient="records"),
        "top_50_watch_families": family_frame.loc[family_frame["bad_pick_watch_family"]].sort_values(
            ["mean_path_value_score_v1", "sample_count"], ascending=[True, False]
        ).head(min(50, limit_families)).to_dict(orient="records"),
        "top_50_regime_bad_pick_families": family_frame.loc[family_frame["regime_bad_pick_family"]].sort_values(
            ["score_spread", "sample_count"], ascending=[False, False]
        ).head(min(50, limit_families)).to_dict(orient="records"),
        "top_50_mae_risk_families": family_frame.loc[family_frame["mae_risk_family"]].sort_values(
            ["mean_mae_20d", "sample_count"], ascending=[True, False]
        ).head(min(50, limit_families)).to_dict(orient="records"),
        "top_50_endpoint_bad_pick_families": family_frame.loc[family_frame["endpoint_bad_pick_family"]].sort_values(
            ["mean_forward_ret_20d", "sample_count"], ascending=[True, False]
        ).head(min(50, limit_families)).to_dict(orient="records"),
        "recommendation": recommendation,
        "typed_reasons": [reason],
        "pruning_challenger_justified": recommendation == "keep",
    }


def _build_manifest_payload(
    *,
    session_id: str,
    source_family_session_id: str,
    source_session_path: Path,
    output_root: Path,
    source_artifacts: dict[str, str],
    artifact_paths: dict[str, str],
    family_counts: dict[str, int],
    thresholds: dict[str, Any],
) -> dict[str, Any]:
    return {
        "schema_version": MANIFEST_SCHEMA_VERSION,
        "generated_at": _utc_now(),
        "session_id": session_id,
        "source_family_session_id": source_family_session_id,
        "source_family_session_path": str(source_session_path),
        "output_root": str(output_root),
        "source_artifacts": source_artifacts,
        "output_artifacts": artifact_paths,
        "family_counts": family_counts,
        "thresholds": thresholds,
        "no_lookahead_inherited": True,
    }


def run_bad_pick_surface_refinement(
    *,
    source_family_session: str | Path | None = None,
    output_root: str | Path | None = None,
    min_sample_count: int = DEFAULT_MIN_SAMPLE_COUNT,
    min_unique_symbol_count: int = DEFAULT_MIN_UNIQUE_SYMBOL_COUNT,
    min_month_count: int = DEFAULT_MIN_MONTH_COUNT,
    limit_families: int = DEFAULT_LIMIT_FAMILIES,
    relax_positive_month_rate: float = DEFAULT_RELAX_POSITIVE_MONTH_RATE,
    relax_mae_margin: float = DEFAULT_RELAX_MAE_MARGIN,
) -> dict[str, Any]:
    run_started = time.perf_counter()
    source_session_path = _resolve_source_session(source_family_session)
    source_payloads = _load_source_family_session(source_session_path)
    output_root_path = Path(output_root).expanduser().resolve() if output_root else DEFAULT_OUTPUT_ROOT.resolve()
    output_root_path.mkdir(parents=True, exist_ok=True)
    session_id = _make_session_id()
    session_tmp = output_root_path / f"{session_id}.tmp"
    session_final = output_root_path / session_id
    session_tmp.mkdir(parents=True, exist_ok=False)

    _progress_log(f"start source={source_session_path} out_root={output_root_path} session={session_id}")

    thresholds = {
        "min_sample_count": int(min_sample_count),
        "min_unique_symbol_count": int(min_unique_symbol_count),
        "min_month_count": int(min_month_count),
        "relax_positive_month_rate": float(relax_positive_month_rate),
        "relax_mae_margin": float(relax_mae_margin),
        "baseline_mean_path_value_score_v1": _safe_float(source_payloads["source_ma_manifest"]["overall_metrics"]["mean_path_value_score_v1"]),
        "baseline_median_path_value_score_v1": _safe_float(source_payloads["source_ma_manifest"]["overall_metrics"]["median_path_value_score_v1"]),
        "baseline_mean_forward_ret_20d": _safe_float(source_payloads["source_ma_manifest"]["overall_metrics"]["mean_forward_ret_20d"]),
        "baseline_mean_mae_20d": _safe_float(source_payloads["source_ma_manifest"]["overall_metrics"]["mean_mae_20d"]),
        "baseline_minus5_before_plus5_rate": _safe_float(source_payloads["source_ma_manifest"]["overall_metrics"]["hit_minus_5_before_plus_5_rate"]),
        "baseline_plus5_before_minus5_rate": _safe_float(source_payloads["source_ma_manifest"]["overall_metrics"]["hit_plus_5_before_minus_5_rate"]),
        "baseline_bottom15_rate": _safe_float(source_payloads["summary"]["baseline_metrics"]["bottom15_rate"]),
        "baseline_top15_rate": _safe_float(source_payloads["summary"]["baseline_metrics"]["top15_rate"]),
        "bottom15_score_threshold": _safe_float(source_payloads["summary"]["baseline_metrics"].get("bottom15_score_threshold")),
        "top15_score_threshold": _safe_float(source_payloads["summary"]["baseline_metrics"].get("top15_score_threshold")),
    }
    if thresholds["baseline_mean_path_value_score_v1"] is None or thresholds["baseline_mean_mae_20d"] is None:
        raise RuntimeError("missing baseline metrics from source family session manifest")
    if thresholds["bottom15_score_threshold"] is None or thresholds["top15_score_threshold"] is None:
        raise RuntimeError("missing score thresholds from source family session summary")

    family_frame = _aggregate_family_frame(
        source_payloads["row_parquet"],
        bottom15_score_threshold=thresholds["bottom15_score_threshold"],
        top15_score_threshold=thresholds["top15_score_threshold"],
    )
    if family_frame.empty:
        raise RuntimeError("no_family_rows")

    family_frame = _apply_labels(family_frame, thresholds)
    family_frame = family_frame.sort_values(["mean_path_value_score_v1", "sample_count"], ascending=[True, False]).reset_index(drop=True)

    source_artifacts = {
        "run_manifest_json": str(source_session_path / "run_manifest.json"),
        "state_family_summary_json": str(source_session_path / "state_family_summary.json"),
        "state_family_by_regime_json": str(source_session_path / "state_family_by_regime.json"),
        "state_family_monthly_stability_json": str(source_session_path / "state_family_monthly_stability.json"),
        "state_family_classification_json": str(source_session_path / "state_family_classification.json"),
        "state_family_filter_v1_decision_json": str(source_session_path / "state_family_filter_v1_decision.json"),
        "state_family_rows_parquet": str(source_session_path / "state_family_rows.parquet"),
        "source_ma_run_manifest_json": str(source_payloads["source_ma_session_path"] / "run_manifest.json"),
    }

    family_counts = {
        "total_families": int(len(family_frame)),
        "strict_bad_pick_family_count": int(family_frame["strict_bad_pick_family"].sum()),
        "relaxed_bad_pick_family_count": int(family_frame["relaxed_bad_pick_family"].sum()),
        "bad_pick_watch_family_count": int(family_frame["bad_pick_watch_family"].sum()),
        "regime_bad_pick_family_count": int(family_frame["regime_bad_pick_family"].sum()),
        "mae_risk_family_count": int(family_frame["mae_risk_family"].sum()),
        "endpoint_bad_pick_family_count": int(family_frame["endpoint_bad_pick_family"].sum()),
        "sample_qualified_family_count": int(family_frame["sample_qualified"].sum()),
    }

    waterfall_payload = _build_waterfall(family_frame, thresholds)
    shadow_payload = _build_shadow_payload(family_frame, limit_families=limit_families)
    compare_payload = _build_relaxation_compare(family_frame, thresholds=thresholds, limit_families=limit_families)
    decision_payload = _build_decision_payload(
        source_family_session_id=source_payloads["manifest"]["session_id"],
        source_session_path=source_session_path,
        family_frame=family_frame,
        source_artifacts=source_artifacts,
        thresholds=thresholds,
        waterfall=waterfall_payload,
        shadow_payload=shadow_payload,
        compare_payload=compare_payload,
        limit_families=limit_families,
    )

    family_rows_out = family_frame.copy()
    family_rows_out["source_family_session_id"] = source_payloads["manifest"]["session_id"]
    family_rows_out["source_family_session_path"] = str(source_session_path)
    family_rows_out["source_row_count"] = int(source_payloads["source_ma_manifest"].get("output_rows_count", len(family_frame)))
    family_rows_out["strict_relax_gap"] = family_rows_out["relaxed_bad_pick_family"].astype(int) - family_rows_out["strict_bad_pick_family"].astype(int)
    family_rows_out["shadow_label_count"] = family_rows_out["shadow_label_count"].astype(int)

    family_rows_parquet = session_tmp / "bad_pick_refinement_rows.parquet"
    conn = duckdb.connect()
    try:
        conn.register("family_rows_out", family_rows_out)
        conn.execute(f"COPY family_rows_out TO '{family_rows_parquet.as_posix()}' (FORMAT PARQUET, COMPRESSION ZSTD)")
    finally:
        conn.close()

    final_artifact_paths = {
        "bad_pick_filter_waterfall_json": str(session_tmp / "bad_pick_filter_waterfall.json"),
        "bad_pick_shadow_classification_json": str(session_tmp / "bad_pick_shadow_classification.json"),
        "bad_pick_relaxation_compare_json": str(session_tmp / "bad_pick_relaxation_compare.json"),
        "bad_pick_refinement_rows_parquet": str(session_tmp / "bad_pick_refinement_rows.parquet"),
        "state_family_bad_pick_surface_refinement_v1_decision_json": str(session_tmp / "state_family_bad_pick_surface_refinement_v1_decision.json"),
        "run_manifest_json": str(session_tmp / "run_manifest.json"),
        "_artifact_complete_json": str(session_tmp / "_ARTIFACT_COMPLETE.json"),
    }
    manifest_payload = _build_manifest_payload(
        session_id=session_id,
        source_family_session_id=source_payloads["manifest"]["session_id"],
        source_session_path=source_session_path,
        output_root=output_root_path,
        source_artifacts=source_artifacts,
        artifact_paths=final_artifact_paths,
        family_counts=family_counts,
        thresholds=thresholds,
    )

    _write_json(session_tmp / "bad_pick_filter_waterfall.json", waterfall_payload)
    _write_json(session_tmp / "bad_pick_shadow_classification.json", shadow_payload)
    _write_json(session_tmp / "bad_pick_relaxation_compare.json", compare_payload)
    _write_json(session_tmp / "state_family_bad_pick_surface_refinement_v1_decision.json", decision_payload)
    _write_json(session_tmp / "run_manifest.json", manifest_payload)
    _write_json(session_tmp / "_ARTIFACT_COMPLETE.json", {
        "schema_version": MANIFEST_SCHEMA_VERSION,
        "generated_at": _utc_now(),
        "session_id": session_id,
        "validated": True,
    })

    _finalize_session_dir(session_tmp, session_final)
    _progress_log(f"finalized session={session_id} elapsed={time.perf_counter() - run_started:.1f}s")

    final_paths = {
        "bad_pick_filter_waterfall_json": str(session_final / "bad_pick_filter_waterfall.json"),
        "bad_pick_shadow_classification_json": str(session_final / "bad_pick_shadow_classification.json"),
        "bad_pick_relaxation_compare_json": str(session_final / "bad_pick_relaxation_compare.json"),
        "bad_pick_refinement_rows_parquet": str(session_final / "bad_pick_refinement_rows.parquet"),
        "state_family_bad_pick_surface_refinement_v1_decision_json": str(session_final / "state_family_bad_pick_surface_refinement_v1_decision.json"),
        "run_manifest_json": str(session_final / "run_manifest.json"),
        "_artifact_complete_json": str(session_final / "_ARTIFACT_COMPLETE.json"),
    }
    manifest_payload["output_artifacts"] = final_paths
    _write_json(session_final / "bad_pick_filter_waterfall.json", waterfall_payload)
    _write_json(session_final / "bad_pick_shadow_classification.json", shadow_payload)
    _write_json(session_final / "bad_pick_relaxation_compare.json", compare_payload)
    _write_json(session_final / "state_family_bad_pick_surface_refinement_v1_decision.json", decision_payload)
    _write_json(session_final / "run_manifest.json", manifest_payload)

    return {
        "session_id": session_id,
        "session_dir": str(session_final),
        "waterfall_path": final_paths["bad_pick_filter_waterfall_json"],
        "shadow_path": final_paths["bad_pick_shadow_classification_json"],
        "compare_path": final_paths["bad_pick_relaxation_compare_json"],
        "rows_path": final_paths["bad_pick_refinement_rows_parquet"],
        "decision_path": final_paths["state_family_bad_pick_surface_refinement_v1_decision_json"],
        "manifest_path": final_paths["run_manifest_json"],
        "waterfall": waterfall_payload,
        "shadow": shadow_payload,
        "compare": compare_payload,
        "decision": decision_payload,
        "manifest": manifest_payload,
        "family_frame": family_frame,
    }


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="TRADEX MA state-family bad-pick surface refinement.")
    parser.add_argument("--source-family-session", default=str(DEFAULT_SOURCE_FAMILY_SESSION))
    parser.add_argument("--output-root", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--min-sample-count", type=int, default=DEFAULT_MIN_SAMPLE_COUNT)
    parser.add_argument("--min-unique-symbol-count", type=int, default=DEFAULT_MIN_UNIQUE_SYMBOL_COUNT)
    parser.add_argument("--min-month-count", type=int, default=DEFAULT_MIN_MONTH_COUNT)
    parser.add_argument("--limit-families", type=int, default=DEFAULT_LIMIT_FAMILIES)
    parser.add_argument("--relax-positive-month-rate", type=float, default=DEFAULT_RELAX_POSITIVE_MONTH_RATE)
    parser.add_argument("--relax-mae-margin", type=float, default=DEFAULT_RELAX_MAE_MARGIN)
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    result = run_bad_pick_surface_refinement(
        source_family_session=args.source_family_session,
        output_root=args.output_root,
        min_sample_count=args.min_sample_count,
        min_unique_symbol_count=args.min_unique_symbol_count,
        min_month_count=args.min_month_count,
        limit_families=args.limit_families,
        relax_positive_month_rate=args.relax_positive_month_rate,
        relax_mae_margin=args.relax_mae_margin,
    )
    print(json.dumps(result["manifest"]["output_artifacts"], ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
