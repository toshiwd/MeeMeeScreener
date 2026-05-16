from __future__ import annotations

import argparse
import hashlib
import json
import math
import random
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

import numpy as np
import pandas as pd

if __package__ in {None, ""}:
    sys.path.append(str(Path(__file__).resolve().parents[1]))

from app.backend.services import tradex_research_contracts as contracts
from scripts import tradex_wide_pool_winner_nonwinner_feature_diagnosis_v1 as feature_diag_mod


AXIS_ID = "ranking_loss_or_topk_objective_repair_v1"
SCHEMA_PREFIX = "tradex_ranking_loss_or_topk_objective_repair_v1"
DEFAULT_WIDE_RUN_ID = "20260513T030000Z-wide-strength-pool-upside-rerank-v1"
DEFAULT_RISK_RUN_ID = "20260513T040000Z-selection-risk-control-for-wide-pool-v1"
DEFAULT_THRESHOLD_RUN_ID = "20260513T050000Z-threshold-no-trade-control-for-wide-pool-v1"
DEFAULT_FEATURE_DIAGNOSIS_RUN_ID = "20260513T060000Z-wide-pool-winner-nonwinner-feature-diagnosis-v1"
DEFAULT_IMAGE_PHASE2_RUN_ID = "20260513T090000Z-image-only-classifier-baseline-phase2"
DEFAULT_IMAGE_CNN_PHASE2B_RUN_ID = "20260513T111000Z-image-cnn-baseline-phase2b-torch"
DEFAULT_WIDE_ROOT = Path(r"G:\Tradex\wide_strength_pool_upside_rerank_v1")
DEFAULT_RISK_ROOT = Path(r"G:\Tradex\selection_risk_control_for_wide_pool_v1")
DEFAULT_THRESHOLD_ROOT = Path(r"G:\Tradex\threshold_no_trade_control_for_wide_pool_v1")
DEFAULT_FEATURE_DIAGNOSIS_ROOT = Path(r"G:\Tradex\wide_pool_winner_nonwinner_feature_diagnosis_v1")
DEFAULT_IMAGE_PHASE2_ROOT = Path(r"G:\Tradex\image_only_classifier_baseline_phase2")
DEFAULT_IMAGE_CNN_PHASE2B_ROOT = Path(r"G:\Tradex\image_cnn_baseline_phase2b")
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\ranking_loss_or_topk_objective_repair_v1")

TOP_K = 3
TOP5_K = 5
RANDOM_SEED = 20260513
EMBARGO_DAYS = 20
RIDGE_LAMBDA = 1.0
BASELINE_FAMILY_ID = "previous_best_wide_score_baseline"
RANDOM_FAMILY_ID = "random_top3_within_candidate_day"
ORACLE_FAMILY_ID = "scoreless_oracle_top3"
PAIRWISE_FAMILY_ID = "daily_pairwise_ranker_v1"
LISTWISE_FAMILY_ID = "daily_listwise_topk_ranker_v1"
BADPICK_FAMILY_ID = "top3_badpick_penalty_ranker_v1"
RANKER_FAMILIES = (
    BASELINE_FAMILY_ID,
    RANDOM_FAMILY_ID,
    ORACLE_FAMILY_ID,
    PAIRWISE_FAMILY_ID,
    LISTWISE_FAMILY_ID,
    BADPICK_FAMILY_ID,
)

BASE_SCORE_COLUMN = "score_momentum_continuation_soft_boost_v1"
FUTURE_LABEL_COLUMNS = set(feature_diag_mod.FUTURE_LABEL_COLUMNS) | {
    "ret20_fwd",
    "mfe20",
    "mae20",
    "severe_loss20",
    "win20",
    "is_future_top15_by_ret20",
    "is_future_top10_by_ret20",
    "is_future_top5_by_ret20",
    "is_big_winner_ret20_ge_10pct",
    "is_big_winner_MFE20_ge_15pct",
    "ret20_rank_by_date",
    "ret20_rank_pct_by_date",
    "MFE20_rank_by_date",
    "MFE20_rank_pct_by_date",
    "future_winner",
    "selected_nonwinner",
    "selected_severe_loser",
    "negative_guard_continuation_winner",
    "negative_guard_blowoff_loser",
    "same_day_oracle_miss",
}
FORBIDDEN_INPUT_COLUMNS = FUTURE_LABEL_COLUMNS | {
    "image_score",
    "cnn_image_score",
    "logistic_image_score",
    "image_only_score",
}
MANDATORY_NUMERIC_FEATURES = (
    BASE_SCORE_COLUMN,
    "score_hybrid_reclaim_momentum_soft_risk_v1",
    "score_baseline_momentum_continuation_soft_boost_v1",
    "score_extended_continuation_vs_blowoff_risk_v1",
    "score_severe_loss_soft_penalty_v1",
    "continuation_signal_score",
    "blowoff_signal_score",
    "past_only_severe_loss_risk_estimate",
    "past_only_bad_pick_risk_estimate",
    "past_only_mae_abs_risk_estimate",
    "threshold_risk_value",
    "same_date_score_rank",
    "score_margin_to_next_candidate",
    "guard_safe_full",
    "negative_guard_match",
)
REQUIRED_ARTIFACTS = (
    "evaluation_contract.json",
    "run_manifest.json",
    "source_artifact_refs.json",
    "ranking_objective_contract.json",
    "feature_input_contract.json",
    "split_contract.json",
    "leakage_audit.json",
    "training_log.jsonl",
    "ranker_score_ledger.jsonl",
    "ranker_leaderboard.json",
    "top1_selection_report.json",
    "top3_selection_report.json",
    "day_level_top3_report.json",
    "oracle_gap_report.json",
    "badpick_report.json",
    "branching_report.json",
    "time_block_stability.json",
    "baseline_comparison_report.json",
    "research_decision.json",
    "_ARTIFACT_COMPLETE.json",
)


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _default_run_id() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + f"-{AXIS_ID}"


def _json_ready(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _json_ready(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_json_ready(item) for item in value]
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    if isinstance(value, np.generic):
        return _json_ready(value.item())
    if isinstance(value, float) and not math.isfinite(value):
        return None
    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass
    return value


def _json_text(payload: Any) -> str:
    return json.dumps(_json_ready(payload), ensure_ascii=False, sort_keys=True, default=str)


def _write_json(path: Path, payload: dict[str, Any]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(_json_text(payload) + "\n", encoding="utf-8")
    return path


def _write_jsonl(path: Path, rows: Iterable[dict[str, Any]]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("".join(_json_text(row) + "\n" for row in rows), encoding="utf-8")
    return path


def _load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


def _stable_hash(payload: Any) -> str:
    return hashlib.sha256(_json_text(payload).encode("utf-8")).hexdigest()


def _file_hash(path: Path) -> str | None:
    if not path.exists() or not path.is_file():
        return None
    h = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _safe_path(value: str | Path | None, default: Path) -> Path:
    if value is None or not str(value).strip():
        return default.resolve()
    return Path(str(value)).expanduser().resolve()


def _run_dir(root: str | Path, run_id: str, default_root: Path) -> Path:
    return _safe_path(root, default_root) / run_id


def _safe_rate(count: int | float, total: int | float) -> float:
    if not total:
        return 0.0
    return float(count) / float(total)


def _stable_random_score(event_date: str, code: str) -> float:
    seed = int(hashlib.sha256(f"{RANDOM_SEED}|{event_date}|{code}".encode("utf-8")).hexdigest()[:16], 16)
    return random.Random(seed).random()


def _require_source_json(source_dir: Path, names: list[str], source_name: str) -> dict[str, Any]:
    missing = [name for name in names if not (source_dir / name).exists()]
    if missing:
        raise FileNotFoundError(f"{source_name} source missing required artifacts: {missing} at {source_dir}")
    payload = {name: _load_json(source_dir / name) for name in names}
    complete = payload.get("_ARTIFACT_COMPLETE.json", {})
    decision = payload.get("research_decision.json", {})
    if complete.get("complete") is not True:
        raise RuntimeError(f"{source_name} source artifact is not complete")
    if complete.get("silent_fallback_used") is not False or decision.get("silent_fallback_used") is not False:
        raise RuntimeError(f"{source_name} source used silent fallback")
    if complete.get("research_fallback_used") not in (False, None) or decision.get("research_fallback_used") not in (False, None):
        raise RuntimeError(f"{source_name} source used research fallback")
    return payload


def _root_from_source_refs(source_refs: dict[str, Any], source: str) -> Path:
    for ref in source_refs.get("refs", []):
        if ref.get("source") == source and ref.get("path"):
            return Path(str(ref["path"])).parent
    raise RuntimeError(f"cannot resolve {source} artifact root from threshold source_artifact_refs.json")


def validate_sources(
    *,
    wide_dir: Path,
    risk_dir: Path,
    threshold_dir: Path,
    feature_diagnosis_dir: Path,
    image_phase2_dir: Path,
    image_cnn_phase2b_dir: Path,
) -> dict[str, Any]:
    wide = _require_source_json(wide_dir, ["_ARTIFACT_COMPLETE.json", "research_decision.json", "top3_selection_report.json", "score_leaderboard.json"], "wide")
    risk = _require_source_json(risk_dir, ["_ARTIFACT_COMPLETE.json", "research_decision.json", "risk_leaderboard.json"], "risk")
    threshold = _require_source_json(threshold_dir, ["_ARTIFACT_COMPLETE.json", "research_decision.json", "threshold_leaderboard.json", "source_artifact_refs.json"], "threshold")
    feature = _require_source_json(feature_diagnosis_dir, ["_ARTIFACT_COMPLETE.json", "research_decision.json", "candidate_feature_shortlist.json"], "feature_diagnosis")
    image_phase2 = _require_source_json(image_phase2_dir, ["_ARTIFACT_COMPLETE.json", "research_decision.json", "phase3_readiness_report.json", "classifier_metrics.json", "topk_proxy_report.json", "negative_guard_image_diagnostics.json"], "image_phase2")
    image_cnn = _require_source_json(image_cnn_phase2b_dir, ["_ARTIFACT_COMPLETE.json", "research_decision.json", "phase3_readiness_report.json", "classifier_metrics.json", "topk_proxy_report.json", "negative_guard_cnn_diagnostics.json"], "image_cnn_phase2b")
    expected_decisions = {
        "wide": (wide["research_decision.json"].get("authoritative_research_decision"), "wide_strength_pool_upside_rerank_hold"),
        "risk": (risk["research_decision.json"].get("authoritative_research_decision"), "selection_risk_control_drop"),
        "threshold": (threshold["research_decision.json"].get("authoritative_research_decision"), "threshold_no_trade_control_drop"),
        "feature_diagnosis": (feature["research_decision.json"].get("authoritative_research_decision"), "winner_nonwinner_feature_diagnosis_hold"),
        "image_phase2": (image_phase2["research_decision.json"].get("authoritative_research_decision"), "image_only_classifier_phase2_failed"),
        "image_cnn_phase2b": (image_cnn["research_decision.json"].get("authoritative_research_decision"), "image_cnn_phase2b_failed"),
    }
    bad = {name: {"actual": actual, "expected": expected} for name, (actual, expected) in expected_decisions.items() if actual != expected}
    if bad:
        raise RuntimeError(f"unexpected source decisions: {bad}")
    if image_phase2["phase3_readiness_report.json"].get("ready_for_fusion") is not False:
        raise RuntimeError("image phase2 source unexpectedly allows fusion")
    if image_cnn["phase3_readiness_report.json"].get("ready_for_fusion") is not False:
        raise RuntimeError("image cnn phase2b source unexpectedly allows fusion")
    threshold_refs = threshold["source_artifact_refs.json"]
    pattern_dir = _root_from_source_refs(threshold_refs, "pattern")
    guard_dir = _root_from_source_refs(threshold_refs, "guard")
    upside_dir = _root_from_source_refs(threshold_refs, "upside")
    return {
        "wide": wide,
        "risk": risk,
        "threshold": threshold,
        "feature": feature,
        "image_phase2": image_phase2,
        "image_cnn": image_cnn,
        "pattern_dir": pattern_dir,
        "guard_dir": guard_dir,
        "upside_dir": upside_dir,
    }


def load_ranker_events(
    *,
    pattern_dir: Path,
    guard_dir: Path,
    upside_dir: Path,
    wide_dir: Path,
    risk_dir: Path,
    threshold_dir: Path,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    loaded = feature_diag_mod.load_source_artifacts(pattern_dir, guard_dir, upside_dir, wide_dir, risk_dir, threshold_dir)
    events = feature_diag_mod.add_diagnostic_labels(loaded["events"], loaded["selected"])
    events["event_date"] = events["event_date"].astype(str).str.slice(0, 10)
    events["event_dt"] = pd.to_datetime(events["event_date"])
    events["event_year"] = events["event_date"].str.slice(0, 4).astype(int)
    events["code"] = events["code"].astype(str)
    return events, loaded["selected"]


def _recommended_shortlist(feature_payload: dict[str, Any]) -> list[str]:
    rows = feature_payload.get("rows", [])
    out = []
    for row in rows:
        if row.get("recommended_for_next_scorer") is True and row.get("leakage_safe") is True:
            feature_id = str(row.get("feature_id", "")).strip()
            if feature_id:
                out.append(feature_id)
    return sorted(set(out))


def build_feature_input_contract(events: pd.DataFrame, feature_payload: dict[str, Any]) -> dict[str, Any]:
    shortlist = _recommended_shortlist(feature_payload)
    requested = [*shortlist, *MANDATORY_NUMERIC_FEATURES]
    used = []
    unavailable = []
    forbidden = []
    for feature in requested:
        if feature in FORBIDDEN_INPUT_COLUMNS:
            forbidden.append(feature)
        elif feature in events.columns:
            used.append(feature)
        else:
            unavailable.append(feature)
    used = sorted(set(used))
    rows = []
    for feature in used:
        numeric = pd.to_numeric(events[feature], errors="coerce")
        present_rate = float(numeric.notna().mean()) if len(numeric) else 0.0
        rows.append(
            {
                "feature_id": feature,
                "source": "previous_shortlist_or_existing_wide_pool_field",
                "feature_type": "tag" if feature in {"guard_safe_full", "negative_guard_match"} else "numeric",
                "present_rate": present_rate,
                "missing_rate": 1.0 - present_rate,
            }
        )
    payload = {
        "schema_version": f"{SCHEMA_PREFIX}_feature_input_contract_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "previous_shortlist_feature_count": len(shortlist),
        "previous_shortlist_features": shortlist,
        "mandatory_numeric_features": list(MANDATORY_NUMERIC_FEATURES),
        "used_feature_count": len(used),
        "used_features": used,
        "unavailable_requested_features": sorted(set(unavailable)),
        "forbidden_feature_inputs": sorted(set(forbidden)),
        "image_score_used": False,
        "cnn_score_used": False,
        "future_labels_used_in_score_inputs": bool(set(used).intersection(FUTURE_LABEL_COLUMNS)),
        "explicit_missing_value_policy": "train_median_fill_recorded_per_time_block",
        "silent_fallback_used": False,
        "rows": rows,
    }
    payload["contract_hash"] = _stable_hash(payload)
    return payload


def build_split_contract(events: pd.DataFrame) -> dict[str, Any]:
    years = sorted(int(year) for year in events["event_year"].dropna().unique().tolist())
    rows = []
    for year in years[1:]:
        eval_start = events.loc[events["event_year"].eq(year), "event_dt"].min()
        train_cutoff = eval_start - pd.Timedelta(days=EMBARGO_DAYS)
        train = events[events["event_dt"].lt(train_cutoff)]
        eval_rows = events[events["event_year"].eq(year)]
        rows.append(
            {
                "eval_time_block": str(year),
                "train_end_before": train_cutoff.date().isoformat(),
                "train_sample_count": int(len(train)),
                "train_day_count": int(train["event_date"].nunique()),
                "eval_sample_count": int(len(eval_rows)),
                "eval_day_count": int(eval_rows["event_date"].nunique()),
            }
        )
    payload = {
        "schema_version": f"{SCHEMA_PREFIX}_split_contract_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "split_policy": "rolling_time_block_train_past_only",
        "split_by": "event_date_year",
        "embargo_days": EMBARGO_DAYS,
        "random_sample_split_used": False,
        "full_period_tuning_used": False,
        "train_past_only": True,
        "eval_time_blocks": [row["eval_time_block"] for row in rows if row["train_sample_count"] > 0],
        "time_block_rows": rows,
        "split_created": bool(any(row["train_sample_count"] > 0 and row["eval_sample_count"] > 0 for row in rows)),
        "research_fallback_used": False,
    }
    payload["contract_hash"] = _stable_hash(payload)
    return payload


def _fit_transform(frame: pd.DataFrame, feature_columns: list[str]) -> tuple[pd.DataFrame, dict[str, float], dict[str, float], dict[str, int]]:
    values = frame[feature_columns].apply(pd.to_numeric, errors="coerce")
    medians = values.median(numeric_only=True).fillna(0.0).to_dict()
    filled = values.fillna(medians)
    std = filled.std(ddof=0).replace(0.0, 1.0).fillna(1.0).to_dict()
    means = filled.mean().fillna(0.0).to_dict()
    transformed = (filled - pd.Series(means)) / pd.Series(std)
    missing_counts = values.isna().sum().astype(int).to_dict()
    return transformed, {str(k): float(v) for k, v in medians.items()}, {str(k): float(v) for k, v in std.items()}, {str(k): int(v) for k, v in missing_counts.items()}


def _apply_transform(frame: pd.DataFrame, feature_columns: list[str], medians: dict[str, float], std: dict[str, float], means: dict[str, float] | None = None) -> pd.DataFrame:
    values = frame[feature_columns].apply(pd.to_numeric, errors="coerce").fillna(medians)
    if means is None:
        means = {column: 0.0 for column in feature_columns}
    return (values - pd.Series(means)) / pd.Series(std)


def _feature_stats(frame: pd.DataFrame, feature_columns: list[str]) -> dict[str, dict[str, float]]:
    values = frame[feature_columns].apply(pd.to_numeric, errors="coerce")
    filled = values.fillna(values.median(numeric_only=True).fillna(0.0))
    means = filled.mean().fillna(0.0).to_dict()
    std = filled.std(ddof=0).replace(0.0, 1.0).fillna(1.0).to_dict()
    return {
        "mean": {str(k): float(v) for k, v in means.items()},
        "std": {str(k): float(v) for k, v in std.items()},
        "median": {str(k): float(v) for k, v in filled.median(numeric_only=True).fillna(0.0).to_dict().items()},
    }


def _ridge_solution(x: np.ndarray, y: np.ndarray, *, fit_intercept: bool) -> tuple[np.ndarray, float]:
    if x.size == 0 or y.size == 0:
        return np.zeros(x.shape[1] if x.ndim == 2 else 0), 0.0
    if fit_intercept:
        x_aug = np.column_stack([np.ones(len(x)), x])
        penalty = np.eye(x_aug.shape[1]) * RIDGE_LAMBDA
        penalty[0, 0] = 0.0
        coef = np.linalg.pinv(x_aug.T @ x_aug + penalty) @ x_aug.T @ y
        return coef[1:], float(coef[0])
    penalty = np.eye(x.shape[1]) * RIDGE_LAMBDA
    coef = np.linalg.pinv(x.T @ x + penalty) @ x.T @ y
    return coef, 0.0


def _date_zscore_target(train: pd.DataFrame, column: str) -> pd.Series:
    values = pd.to_numeric(train[column], errors="coerce").fillna(0.0)
    centered = values - values.groupby(train["event_date"]).transform("mean")
    scale = values.groupby(train["event_date"]).transform("std").replace(0.0, 1.0).fillna(1.0)
    return centered / scale


def _fit_pairwise(train: pd.DataFrame, feature_columns: list[str]) -> dict[str, Any]:
    stats = _feature_stats(train, feature_columns)
    x = _apply_transform(train, feature_columns, stats["median"], stats["std"], stats["mean"])
    work = train[["event_date", "ret20_fwd"]].copy()
    rows = []
    for _, group in work.groupby("event_date", sort=True):
        if len(group) < 2:
            continue
        ordered = group.sort_values("ret20_fwd", ascending=False)
        top_idx = ordered.head(min(3, len(ordered))).index
        bottom_idx = ordered.tail(min(3, len(ordered))).index
        for left in top_idx:
            for right in bottom_idx:
                if left == right:
                    continue
                rows.append((x.loc[left].to_numpy(dtype=float) - x.loc[right].to_numpy(dtype=float), 1.0))
    if not rows:
        coef = np.zeros(len(feature_columns))
        pair_count = 0
    else:
        xdiff = np.vstack([row[0] for row in rows])
        y = np.array([row[1] for row in rows], dtype=float)
        coef, _ = _ridge_solution(xdiff, y, fit_intercept=False)
        pair_count = int(len(rows))
    return {"coef": coef, "intercept": 0.0, "stats": stats, "train_pair_count": pair_count}


def _fit_listwise(train: pd.DataFrame, feature_columns: list[str]) -> dict[str, Any]:
    stats = _feature_stats(train, feature_columns)
    x = _apply_transform(train, feature_columns, stats["median"], stats["std"], stats["mean"]).to_numpy(dtype=float)
    target = _date_zscore_target(train, "ret20_fwd").to_numpy(dtype=float)
    coef, intercept = _ridge_solution(x, target, fit_intercept=True)
    return {"coef": coef, "intercept": intercept, "stats": stats, "target": "same_date_ret20_zscore"}


def _fit_badpick_penalty(train: pd.DataFrame, feature_columns: list[str]) -> dict[str, Any]:
    stats = _feature_stats(train, feature_columns)
    x = _apply_transform(train, feature_columns, stats["median"], stats["std"], stats["mean"]).to_numpy(dtype=float)
    ret_z = _date_zscore_target(train, "ret20_fwd")
    target = (
        ret_z
        + train["is_future_top10_by_ret20"].astype(float) * 0.20
        + train["is_big_winner_ret20_ge_10pct"].astype(float) * 0.10
        - train["severe_loss20"].astype(float) * 0.25
        + pd.to_numeric(train["mae20"], errors="coerce").fillna(0.0).clip(upper=0.0) * 0.35
    ).to_numpy(dtype=float)
    coef, intercept = _ridge_solution(x, target, fit_intercept=True)
    return {"coef": coef, "intercept": intercept, "stats": stats, "target": "ret20_zscore_with_top3_badpick_penalty"}


def _score_with_model(frame: pd.DataFrame, feature_columns: list[str], model: dict[str, Any]) -> np.ndarray:
    x = _apply_transform(frame, feature_columns, model["stats"]["median"], model["stats"]["std"], model["stats"]["mean"]).to_numpy(dtype=float)
    return x @ np.asarray(model["coef"], dtype=float) + float(model.get("intercept", 0.0))


def _rank_within_day(frame: pd.DataFrame, score_column: str) -> pd.Series:
    work = frame[["event_date", "code", score_column]].copy()
    work["_score"] = pd.to_numeric(work[score_column], errors="coerce").fillna(-999999.0)
    work = work.sort_values(["event_date", "_score", "code"], ascending=[True, False, True])
    work["_rank"] = work.groupby("event_date").cumcount() + 1
    return work.sort_index()["_rank"].astype(float)


def train_and_score_rankers(events: pd.DataFrame, feature_columns: list[str], split_contract: dict[str, Any]) -> tuple[pd.DataFrame, list[dict[str, Any]]]:
    eval_years = [int(year) for year in split_contract["eval_time_blocks"]]
    score_frames = []
    training_rows = []
    for year in eval_years:
        eval_mask = events["event_year"].eq(year)
        eval_rows = events[eval_mask].copy()
        eval_start = eval_rows["event_dt"].min()
        train_cutoff = eval_start - pd.Timedelta(days=EMBARGO_DAYS)
        train = events[events["event_dt"].lt(train_cutoff)].copy()
        if train.empty or eval_rows.empty:
            continue
        pairwise = _fit_pairwise(train, feature_columns)
        listwise = _fit_listwise(train, feature_columns)
        badpick = _fit_badpick_penalty(train, feature_columns)
        eval_scored = eval_rows.copy()
        eval_scored[f"score_{BASELINE_FAMILY_ID}"] = pd.to_numeric(eval_scored[BASE_SCORE_COLUMN], errors="coerce").fillna(0.0)
        eval_scored[f"score_{RANDOM_FAMILY_ID}"] = [_stable_random_score(str(row.event_date), str(row.code)) for row in eval_scored.itertuples(index=False)]
        eval_scored[f"score_{ORACLE_FAMILY_ID}"] = pd.to_numeric(eval_scored["ret20_fwd"], errors="coerce").fillna(0.0)
        eval_scored[f"score_{PAIRWISE_FAMILY_ID}"] = _score_with_model(eval_scored, feature_columns, pairwise)
        eval_scored[f"score_{LISTWISE_FAMILY_ID}"] = _score_with_model(eval_scored, feature_columns, listwise)
        eval_scored[f"score_{BADPICK_FAMILY_ID}"] = _score_with_model(eval_scored, feature_columns, badpick)
        eval_scored["eval_time_block"] = str(year)
        eval_scored["train_sample_count"] = int(len(train))
        score_frames.append(eval_scored)
        for family_id, model in [
            (PAIRWISE_FAMILY_ID, pairwise),
            (LISTWISE_FAMILY_ID, listwise),
            (BADPICK_FAMILY_ID, badpick),
        ]:
            coefs = {feature: float(coef) for feature, coef in zip(feature_columns, model["coef"], strict=False)}
            top_features = sorted(coefs.items(), key=lambda item: abs(item[1]), reverse=True)[:8]
            training_rows.append(
                {
                    "schema_version": f"{SCHEMA_PREFIX}_training_log_row_v1",
                    "axis_id": AXIS_ID,
                    "ranker_family_id": family_id,
                    "eval_time_block": str(year),
                    "train_sample_count": int(len(train)),
                    "train_day_count": int(train["event_date"].nunique()),
                    "eval_sample_count": int(len(eval_rows)),
                    "eval_day_count": int(eval_rows["event_date"].nunique()),
                    "train_end_before": train_cutoff.date().isoformat(),
                    "feature_count": len(feature_columns),
                    "target": model.get("target", "pairwise_top_minus_bottom"),
                    "train_pair_count": model.get("train_pair_count"),
                    "ridge_lambda": RIDGE_LAMBDA,
                    "top_abs_coefficients": [{"feature_id": key, "coefficient": value} for key, value in top_features],
                    "future_labels_used_as_training_targets_only": True,
                    "future_labels_used_in_score_inputs": False,
                    "full_period_tuning_used": False,
                    "research_fallback_used": False,
                }
            )
    if not score_frames:
        raise RuntimeError("no train-past-only time block could be scored")
    scored = pd.concat(score_frames, ignore_index=True, sort=False)
    ledger_rows = []
    for family_id in RANKER_FAMILIES:
        score_col = f"score_{family_id}"
        scored[f"rank_{family_id}"] = _rank_within_day(scored, score_col)
        for row in scored.itertuples(index=False):
            ledger_rows.append(
                {
                    "schema_version": f"{SCHEMA_PREFIX}_ranker_score_ledger_row_v1",
                    "axis_id": AXIS_ID,
                    "ranker_family_id": family_id,
                    "event_date": str(row.event_date),
                    "event_ymd": int(row.event_ymd),
                    "code": str(row.code),
                    "eval_time_block": str(row.eval_time_block),
                    "train_sample_count": int(row.train_sample_count),
                    "ranker_score": float(getattr(row, score_col)),
                    "selection_rank": float(getattr(row, f"rank_{family_id}")),
                    "selected_top1": bool(getattr(row, f"rank_{family_id}") <= 1),
                    "selected_top3": bool(getattr(row, f"rank_{family_id}") <= TOP_K),
                    "selected_top5": bool(getattr(row, f"rank_{family_id}") <= TOP5_K),
                    "ret20_fwd": float(row.ret20_fwd),
                    "mfe20": float(row.mfe20),
                    "mae20": float(row.mae20),
                    "win20": bool(row.win20),
                    "severe_loss20": bool(row.severe_loss20),
                    "is_future_top10_by_ret20": bool(row.is_future_top10_by_ret20),
                    "is_future_top5_by_ret20": bool(row.is_future_top5_by_ret20),
                    "is_big_winner_ret20_ge_10pct": bool(row.is_big_winner_ret20_ge_10pct),
                    "negative_guard_match": bool(row.negative_guard_match),
                    "guard_safe_full": bool(row.guard_safe_full),
                    "image_score_used": False,
                    "cnn_score_used": False,
                    "production_ranking_changed": False,
                }
            )
    return pd.DataFrame(ledger_rows), training_rows


def _selection_sets(ledger: pd.DataFrame, family_id: str, rank_limit: int) -> dict[str, dict[str, int]]:
    scope = ledger[(ledger["ranker_family_id"].eq(family_id)) & (pd.to_numeric(ledger["selection_rank"], errors="coerce").le(rank_limit))]
    out: dict[str, dict[str, int]] = {}
    for event_date, group in scope.groupby("event_date", sort=True):
        out[str(event_date)] = {str(row.code): int(row.selection_rank) for row in group.itertuples(index=False)}
    return out


def _branching_metrics(ledger: pd.DataFrame, family_id: str) -> dict[str, Any]:
    base3 = _selection_sets(ledger, BASELINE_FAMILY_ID, TOP_K)
    base5 = _selection_sets(ledger, BASELINE_FAMILY_ID, TOP5_K)
    cand3 = _selection_sets(ledger, family_id, TOP_K)
    cand5 = _selection_sets(ledger, family_id, TOP5_K)
    changed3_members = 0
    changed5_members = 0
    changed3_days = 0
    changed_rank_count = 0
    for date in sorted(set(base3) | set(cand3)):
        base_codes = set(base3.get(date, {}))
        cand_codes = set(cand3.get(date, {}))
        if base_codes != cand_codes:
            changed3_days += 1
        changed3_members += len(cand_codes - base_codes)
        for code in cand_codes & base_codes:
            if cand3[date][code] != base3[date][code]:
                changed_rank_count += 1
    for date in sorted(set(base5) | set(cand5)):
        changed5_members += len(set(cand5.get(date, {})) - set(base5.get(date, {})))
    day_count = len(set(base3) | set(cand3))
    return {
        "ranker_family_id": family_id,
        "changed_top3_members_count_vs_previous_best": int(changed3_members),
        "changed_top5_members_count_vs_previous_best": int(changed5_members),
        "changed_top3_days_count_vs_previous_best": int(changed3_days),
        "changed_top3_day_rate_vs_previous_best": _safe_rate(changed3_days, day_count),
        "changed_rank_count": int(changed_rank_count),
        "selection_divergence_reason": "same_as_previous_best" if changed3_members == 0 and changed_rank_count == 0 else "ranking_objective_changed_topk_members_or_order",
    }


def _day_rows_for_family(ledger: pd.DataFrame, family_id: str) -> list[dict[str, Any]]:
    family = ledger[ledger["ranker_family_id"].eq(family_id)]
    rows = []
    for event_date, day in family.groupby("event_date", sort=True):
        top3 = day[pd.to_numeric(day["selection_rank"], errors="coerce").le(TOP_K)]
        rows.append(
            {
                "ranker_family_id": family_id,
                "event_date": str(event_date),
                "selected_count": int(len(top3)),
                "top3_day_avg_ret20": float(top3["ret20_fwd"].mean()) if len(top3) else 0.0,
                "top3_day_worst_MAE20": float(top3["mae20"].min()) if len(top3) else 0.0,
                "top3_day_worst_ret20": float(top3["ret20_fwd"].min()) if len(top3) else 0.0,
                "top3_day_any_severe_loss": bool(top3["severe_loss20"].any()) if len(top3) else False,
                "day_had_future_winner": bool(day["is_future_top10_by_ret20"].any()),
                "selected_future_winner": bool(top3["is_future_top10_by_ret20"].any()) if len(top3) else False,
                "eval_time_block": str(day["eval_time_block"].iloc[0]) if len(day) else None,
            }
        )
    return rows


def _selection_metrics(ledger: pd.DataFrame, family_id: str) -> dict[str, Any]:
    family = ledger[ledger["ranker_family_id"].eq(family_id)].copy()
    top1 = family[pd.to_numeric(family["selection_rank"], errors="coerce").eq(1.0)]
    top3 = family[pd.to_numeric(family["selection_rank"], errors="coerce").le(TOP_K)]
    day_rows = pd.DataFrame(_day_rows_for_family(ledger, family_id))
    winner_dates = set(family.loc[family["is_future_top10_by_ret20"], "event_date"].unique().tolist())
    selected_winner_dates = set(top3.loc[top3["is_future_top10_by_ret20"], "event_date"].unique().tolist())
    total_big_winner = int(family["is_big_winner_ret20_ge_10pct"].sum())
    branching = _branching_metrics(ledger, family_id)
    return {
        **branching,
        "selected_top1_avg_ret20": float(top1["ret20_fwd"].mean()) if len(top1) else 0.0,
        "selected_top3_avg_ret20": float(top3["ret20_fwd"].mean()) if len(top3) else 0.0,
        "selected_top3_win_rate20": float(top3["win20"].mean()) if len(top3) else 0.0,
        "selected_top3_avg_MFE20": float(top3["mfe20"].mean()) if len(top3) else 0.0,
        "selected_top3_avg_MAE20": float(top3["mae20"].mean()) if len(top3) else 0.0,
        "selected_top3_severe_loss_rate20": float(top3["severe_loss20"].mean()) if len(top3) else 0.0,
        "selected_top3_big_winner_capture_rate": _safe_rate(int(top3["is_big_winner_ret20_ge_10pct"].sum()), total_big_winner),
        "selected_nonwinner_when_winner_available_rate": _safe_rate(len(winner_dates - selected_winner_dates), len(winner_dates)),
        "top3_day_any_severe_loss_rate": float(day_rows["top3_day_any_severe_loss"].mean()) if len(day_rows) else 0.0,
        "top3_day_worst_MAE20": float(day_rows["top3_day_worst_MAE20"].mean()) if len(day_rows) else 0.0,
        "selected_event_count": int(len(top3)),
        "selected_day_count": int(top3["event_date"].nunique()),
        "negative_guard_selected_rate": float(top3["negative_guard_match"].mean()) if len(top3) else 0.0,
        "negative_guard_winner_selected_count": int((top3["negative_guard_match"] & top3["is_big_winner_ret20_ge_10pct"]).sum()) if len(top3) else 0,
        "negative_guard_severe_loser_selected_count": int((top3["negative_guard_match"] & top3["severe_loss20"]).sum()) if len(top3) else 0,
    }


def build_reports(ledger: pd.DataFrame) -> dict[str, Any]:
    metrics = [_selection_metrics(ledger, family_id) for family_id in RANKER_FAMILIES]
    by_id = {row["ranker_family_id"]: row for row in metrics}
    base = by_id[BASELINE_FAMILY_ID]
    random_base = by_id[RANDOM_FAMILY_ID]
    oracle = by_id[ORACLE_FAMILY_ID]
    for row in metrics:
        row["selected_top3_avg_ret20_delta_vs_previous_best"] = row["selected_top3_avg_ret20"] - base["selected_top3_avg_ret20"]
        row["selected_top3_avg_ret20_delta_vs_random"] = row["selected_top3_avg_ret20"] - random_base["selected_top3_avg_ret20"]
        row["selected_top3_severe_loss_delta_vs_previous_best"] = row["selected_top3_severe_loss_rate20"] - base["selected_top3_severe_loss_rate20"]
        row["selected_nonwinner_delta_vs_previous_best"] = row["selected_nonwinner_when_winner_available_rate"] - base["selected_nonwinner_when_winner_available_rate"]
        row["oracle_top3_gap_ret20"] = row["selected_top3_avg_ret20"] - oracle["selected_top3_avg_ret20"]
        row["oracle_top3_gap_delta_vs_previous_best"] = row["oracle_top3_gap_ret20"] - (base["selected_top3_avg_ret20"] - oracle["selected_top3_avg_ret20"])
        row["top3_day_any_severe_loss_delta_vs_previous_best"] = row["top3_day_any_severe_loss_rate"] - base["top3_day_any_severe_loss_rate"]
    leaderboard_rows = sorted(metrics, key=lambda row: (row["ranker_family_id"] == ORACLE_FAMILY_ID, row["selected_top3_avg_ret20"], -row["selected_top3_severe_loss_rate20"]), reverse=True)
    day_rows = []
    for family_id in RANKER_FAMILIES:
        day_rows.extend(_day_rows_for_family(ledger, family_id))
    return {
        "metrics": metrics,
        "by_id": by_id,
        "day_rows": day_rows,
        "ranker_leaderboard": {"schema_version": f"{SCHEMA_PREFIX}_ranker_leaderboard_v1", "generated_at": _utc_now(), "axis_id": AXIS_ID, "rows": leaderboard_rows},
        "top1_selection_report": {"schema_version": f"{SCHEMA_PREFIX}_top1_selection_report_v1", "generated_at": _utc_now(), "axis_id": AXIS_ID, "rows": [{k: row[k] for k in ("ranker_family_id", "selected_top1_avg_ret20", "changed_rank_count", "selection_divergence_reason")} for row in metrics]},
        "top3_selection_report": {"schema_version": f"{SCHEMA_PREFIX}_top3_selection_report_v1", "generated_at": _utc_now(), "axis_id": AXIS_ID, "rows": metrics},
        "day_level_top3_report": {"schema_version": f"{SCHEMA_PREFIX}_day_level_top3_report_v1", "generated_at": _utc_now(), "axis_id": AXIS_ID, "rows": day_rows},
        "oracle_gap_report": {"schema_version": f"{SCHEMA_PREFIX}_oracle_gap_report_v1", "generated_at": _utc_now(), "axis_id": AXIS_ID, "rows": [{"ranker_family_id": row["ranker_family_id"], "oracle_top3_gap_ret20": row["oracle_top3_gap_ret20"], "oracle_top3_gap_delta_vs_previous_best": row["oracle_top3_gap_delta_vs_previous_best"]} for row in metrics]},
        "badpick_report": {"schema_version": f"{SCHEMA_PREFIX}_badpick_report_v1", "generated_at": _utc_now(), "axis_id": AXIS_ID, "rows": [{"ranker_family_id": row["ranker_family_id"], "selected_top3_severe_loss_rate20": row["selected_top3_severe_loss_rate20"], "top3_day_any_severe_loss_rate": row["top3_day_any_severe_loss_rate"], "top3_day_worst_MAE20": row["top3_day_worst_MAE20"], "selected_nonwinner_when_winner_available_rate": row["selected_nonwinner_when_winner_available_rate"]} for row in metrics]},
        "branching_report": {"schema_version": f"{SCHEMA_PREFIX}_branching_report_v1", "generated_at": _utc_now(), "axis_id": AXIS_ID, "rows": [{key: row[key] for key in ("ranker_family_id", "changed_top3_members_count_vs_previous_best", "changed_top5_members_count_vs_previous_best", "changed_top3_days_count_vs_previous_best", "changed_top3_day_rate_vs_previous_best", "changed_rank_count", "selection_divergence_reason")} for row in metrics]},
    }


def build_time_block_stability(ledger: pd.DataFrame, reports: dict[str, Any]) -> dict[str, Any]:
    rows = []
    base_day = pd.DataFrame(_day_rows_for_family(ledger, BASELINE_FAMILY_ID))
    base_by_block = base_day.groupby("eval_time_block").agg(base_avg_ret20=("top3_day_avg_ret20", "mean"), base_any_severe=("top3_day_any_severe_loss", "mean")).reset_index()
    for family_id in RANKER_FAMILIES:
        day = pd.DataFrame(_day_rows_for_family(ledger, family_id))
        by_block = day.groupby("eval_time_block").agg(avg_ret20=("top3_day_avg_ret20", "mean"), any_severe=("top3_day_any_severe_loss", "mean")).reset_index()
        merged = by_block.merge(base_by_block, on="eval_time_block", how="left")
        merged["ret20_delta_vs_previous_best"] = merged["avg_ret20"] - merged["base_avg_ret20"]
        merged["severe_delta_vs_previous_best"] = merged["any_severe"] - merged["base_any_severe"]
        positive_delta_rate = float(merged["ret20_delta_vs_previous_best"].gt(0.0).mean()) if len(merged) else 0.0
        nonworse_severe_rate = float(merged["severe_delta_vs_previous_best"].le(0.01).mean()) if len(merged) else 0.0
        rows.append(
            {
                "ranker_family_id": family_id,
                "time_block_count": int(len(merged)),
                "positive_ret20_delta_vs_previous_best_rate": positive_delta_rate,
                "severe_loss_nonworse_time_block_rate": nonworse_severe_rate,
                "time_block_effect_size_min": float(merged["ret20_delta_vs_previous_best"].min()) if len(merged) else None,
                "time_block_effect_size_max": float(merged["ret20_delta_vs_previous_best"].max()) if len(merged) else None,
                "feature_passes_stability_gate": bool(family_id == BASELINE_FAMILY_ID or (len(merged) >= 3 and positive_delta_rate >= 0.55 and nonworse_severe_rate >= 0.55)),
                "rows": merged.to_dict(orient="records"),
            }
        )
    return {"schema_version": f"{SCHEMA_PREFIX}_time_block_stability_v1", "generated_at": _utc_now(), "axis_id": AXIS_ID, "rows": rows}


def build_baseline_comparison_report(reports: dict[str, Any], source_status: dict[str, Any]) -> dict[str, Any]:
    rows = []
    base = reports["by_id"][BASELINE_FAMILY_ID]
    random_base = reports["by_id"][RANDOM_FAMILY_ID]
    for family_id in (PAIRWISE_FAMILY_ID, LISTWISE_FAMILY_ID, BADPICK_FAMILY_ID):
        row = reports["by_id"][family_id]
        rows.append(
            {
                "ranker_family_id": family_id,
                "vs_previous_best": {
                    "selected_top3_avg_ret20_delta": row["selected_top3_avg_ret20"] - base["selected_top3_avg_ret20"],
                    "severe_loss_rate_delta": row["selected_top3_severe_loss_rate20"] - base["selected_top3_severe_loss_rate20"],
                    "nonwinner_when_winner_available_delta": row["selected_nonwinner_when_winner_available_rate"] - base["selected_nonwinner_when_winner_available_rate"],
                    "oracle_gap_delta": row["oracle_top3_gap_delta_vs_previous_best"],
                },
                "vs_random": {
                    "selected_top3_avg_ret20_delta": row["selected_top3_avg_ret20"] - random_base["selected_top3_avg_ret20"],
                    "severe_loss_rate_delta": row["selected_top3_severe_loss_rate20"] - random_base["selected_top3_severe_loss_rate20"],
                },
            }
        )
    image_phase2 = source_status["image_phase2"]
    image_cnn = source_status["image_cnn"]
    return {
        "schema_version": f"{SCHEMA_PREFIX}_baseline_comparison_report_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "rows": rows,
        "historical_references_not_used_as_inputs": {
            "image_only_logistic_baseline": {
                "decision": image_phase2["research_decision.json"].get("authoritative_research_decision"),
                "test_roc_auc": image_phase2["classifier_metrics.json"].get("test_roc_auc"),
                "test_mcc": image_phase2["classifier_metrics.json"].get("test_mcc"),
                "image_score_used": False,
            },
            "simple_cnn_phase2b": {
                "decision": image_cnn["research_decision.json"].get("authoritative_research_decision"),
                "test_roc_auc": image_cnn["classifier_metrics.json"].get("test_roc_auc"),
                "test_mcc": image_cnn["classifier_metrics.json"].get("test_mcc"),
                "cnn_score_used": False,
            },
        },
    }


def build_leakage_audit(feature_columns: list[str], split_contract: dict[str, Any]) -> dict[str, Any]:
    forbidden_used = sorted(set(feature_columns).intersection(FORBIDDEN_INPUT_COLUMNS))
    payload = {
        "schema_version": f"{SCHEMA_PREFIX}_leakage_audit_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "feature_columns": feature_columns,
        "forbidden_input_columns_used": forbidden_used,
        "future_labels_used_as_training_targets_only": True,
        "future_labels_used_in_score_inputs": bool(forbidden_used),
        "image_score_used": False,
        "cnn_score_used": False,
        "split_leakage_audit_passed": bool(split_contract.get("split_created") is True and not forbidden_used),
        "random_sample_split_used": False,
        "full_period_tuning_used": False,
        "silent_fallback_used": False,
        "research_fallback_used": False,
    }
    payload["contract_hash"] = _stable_hash(payload)
    return payload


def build_contract_artifacts(
    *,
    output_dir: Path,
    source_dirs: dict[str, Path],
    source_status: dict[str, Any],
    events: pd.DataFrame,
    feature_contract: dict[str, Any],
    split_contract: dict[str, Any],
) -> dict[str, dict[str, Any]]:
    refs = []
    for source_name, root in source_dirs.items():
        for path in sorted(root.glob("*.json")):
            refs.append({"source": source_name, "name": path.name, "path": str(path), "exists": path.exists(), "content_hash": _stable_hash(_load_json(path))})
        for path in sorted(root.glob("*.jsonl")):
            refs.append({"source": source_name, "name": path.name, "path": str(path), "exists": path.exists(), "file_hash": _file_hash(path)})
    source_refs = {
        "schema_version": f"{SCHEMA_PREFIX}_source_artifact_refs_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "source_roots": {key: str(value) for key, value in source_dirs.items()},
        "refs": refs,
    }
    ranking_objective_contract = {
        "schema_version": f"{SCHEMA_PREFIX}_ranking_objective_contract_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "objective": "daily_topk_ranking_with_train_past_only_pairwise_and_listwise_targets",
        "top_k": TOP_K,
        "max_families": list(RANKER_FAMILIES),
        "classification_objective_used": False,
        "image_route_paused": True,
        "image_score_used": False,
        "cnn_score_used": False,
        "fusion_reranker_created": False,
        "safe_full_used_as_hard_filter": False,
        "negative_guard_used_as_hard_veto": False,
        "threshold_policy_created": False,
        "production_ranking_changed": False,
        "publish_bundle_created": False,
        "meemee_reflectable": False,
    }
    evaluation_contract = {
        "schema_version": f"{SCHEMA_PREFIX}_evaluation_contract_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "research_phase": "ranking_loss_or_topk_objective_repair",
        "boundary": "TRADEX-only",
        "axis_moved": "ranking_loss_or_topk_objective_repair",
        "source_image_cnn_decision": source_status["image_cnn"]["research_decision.json"].get("authoritative_research_decision"),
        "event_count": int(len(events)),
        "event_day_count": int(events["event_date"].nunique()),
        "period": {"start_date": str(int(events["event_ymd"].min())), "end_date": str(int(events["event_ymd"].max()))},
        "same_condition_controls": {
            "same_universe": True,
            "same_period": True,
            "same_top_k": TOP_K,
            "same_regime_condition": True,
            "same_cost_slippage": True,
            "cost_slippage_evaluated": False,
            "cost_slippage_ignored_by_user_intent": True,
            "same_artifact_detail_level": contracts.TRADEX_ARTIFACT_DETAIL_LEVEL_AUTHORITATIVE,
        },
        "ranking_objective_created": True,
        "candidate_scoring_created": True,
        "candidate_scoring_scope": "research_only",
        "production_ranking_changed": False,
        "publish_bundle_created": False,
        "meemee_reflectable": False,
        "silent_fallback_used": False,
        "research_fallback_used": False,
    }
    evaluation_contract["contract_hash"] = _stable_hash(evaluation_contract)
    return {
        "evaluation_contract.json": evaluation_contract,
        "source_artifact_refs.json": source_refs,
        "ranking_objective_contract.json": ranking_objective_contract,
        "feature_input_contract.json": feature_contract,
        "split_contract.json": split_contract,
    }


def build_research_decision(
    *,
    reports: dict[str, Any],
    time_stability: dict[str, Any],
    leakage_audit: dict[str, Any],
    artifact_complete: bool,
) -> dict[str, Any]:
    candidate_ids = [PAIRWISE_FAMILY_ID, LISTWISE_FAMILY_ID, BADPICK_FAMILY_ID]
    rows = [reports["by_id"][family_id] for family_id in candidate_ids]
    best = max(rows, key=lambda row: (row["selected_top3_avg_ret20"], -row["selected_top3_severe_loss_rate20"], -row["selected_nonwinner_when_winner_available_rate"]))
    base = reports["by_id"][BASELINE_FAMILY_ID]
    random_base = reports["by_id"][RANDOM_FAMILY_ID]
    stability_by_id = {row["ranker_family_id"]: row for row in time_stability["rows"]}
    stable = bool(stability_by_id.get(best["ranker_family_id"], {}).get("feature_passes_stability_gate") is True)
    ret_vs_base = best["selected_top3_avg_ret20"] > base["selected_top3_avg_ret20"]
    ret_vs_random = best["selected_top3_avg_ret20"] > random_base["selected_top3_avg_ret20"]
    severe_nonworse = best["selected_top3_severe_loss_rate20"] <= base["selected_top3_severe_loss_rate20"] + 0.01
    nonwinner_improves = best["selected_nonwinner_when_winner_available_rate"] < base["selected_nonwinner_when_winner_available_rate"]
    oracle_improves = best["oracle_top3_gap_delta_vs_previous_best"] > 0.0
    any_severe_nonworse = best["top3_day_any_severe_loss_rate"] <= base["top3_day_any_severe_loss_rate"] + 0.01
    branching = best["changed_top3_members_count_vs_previous_best"] > 0 and best["changed_top3_day_rate_vs_previous_best"] >= 0.05
    no_leak = leakage_audit["split_leakage_audit_passed"] is True
    keep_pass = all([artifact_complete, ret_vs_base, ret_vs_random, severe_nonworse, nonwinner_improves, oracle_improves, any_severe_nonworse, branching, stable, no_leak])
    drop_pass = (
        not ret_vs_base
        or not ret_vs_random
        or not oracle_improves
        or not nonwinner_improves
        or leakage_audit["future_labels_used_in_score_inputs"] is True
        or not artifact_complete
    )
    if keep_pass:
        decision = "keep_candidate"
        authoritative = "ranking_objective_keep_candidate"
    elif drop_pass:
        decision = "drop"
        authoritative = "ranking_objective_drop"
    else:
        decision = "hold"
        authoritative = "ranking_objective_hold"
    typed_reasons = [
        "image_route_paused",
        "daily_topk_ranking_objective_tested",
        "top3_avg_ret20_improved" if ret_vs_base else "top3_avg_ret20_not_improved",
        "random_top3_beaten" if ret_vs_random else "random_top3_not_beaten",
        "severe_loss_nonworse" if severe_nonworse else "severe_loss_worsened",
        "nonwinner_when_winner_available_improved" if nonwinner_improves else "nonwinner_when_winner_available_not_improved",
        "oracle_gap_improved" if oracle_improves else "oracle_gap_not_improved",
        "actual_top3_branching" if branching else "insufficient_top3_branching",
        "time_block_stability_passed" if stable else "time_block_stability_failed",
    ]
    return {
        "schema_version": f"{SCHEMA_PREFIX}_research_decision_v1",
        "generated_at": _utc_now(),
        "research_phase": "ranking_loss_or_topk_objective_repair",
        "boundary": "TRADEX-only",
        "axis_moved": "ranking_loss_or_topk_objective_repair",
        "source_image_cnn_decision": "image_cnn_phase2b_failed",
        "image_route_paused": True,
        "image_score_used": False,
        "cnn_score_used": False,
        "fusion_reranker_created": False,
        "ranking_objective_created": True,
        "candidate_scoring_created": True,
        "candidate_scoring_scope": "research_only",
        "production_ranking_changed": False,
        "publish_bundle_created": False,
        "meemee_reflectable": False,
        "safe_full_used_as_hard_filter": False,
        "negative_guard_used_as_hard_veto": False,
        "future_labels_used_as_training_targets_only": True,
        "future_labels_used_in_score_inputs": False,
        "split_leakage_audit_passed": no_leak,
        "silent_fallback_used": False,
        "research_fallback_used": False,
        "decision": decision,
        "authoritative_research_decision": authoritative,
        "best_ranker_family_id": best["ranker_family_id"],
        "typed_reasons": typed_reasons,
        "decision_reasons": [
            {"code": "selected_top3_avg_ret20_improves_vs_previous_best", "status": "pass" if ret_vs_base else "fail", "value": best["selected_top3_avg_ret20"], "baseline": base["selected_top3_avg_ret20"]},
            {"code": "selected_top3_avg_ret20_improves_vs_random_top3", "status": "pass" if ret_vs_random else "fail", "value": best["selected_top3_avg_ret20"], "baseline": random_base["selected_top3_avg_ret20"]},
            {"code": "selected_top3_severe_loss_rate20_not_materially_worse", "status": "pass" if severe_nonworse else "fail", "value": best["selected_top3_severe_loss_rate20"], "baseline": base["selected_top3_severe_loss_rate20"], "material_worse_margin": 0.01},
            {"code": "selected_nonwinner_when_winner_available_improves", "status": "pass" if nonwinner_improves else "fail", "value": best["selected_nonwinner_when_winner_available_rate"], "baseline": base["selected_nonwinner_when_winner_available_rate"]},
            {"code": "oracle_top3_gap_ret20_improves", "status": "pass" if oracle_improves else "fail", "value": best["oracle_top3_gap_ret20"], "baseline": base["oracle_top3_gap_ret20"]},
            {"code": "top3_day_any_severe_loss_rate_nonworse", "status": "pass" if any_severe_nonworse else "fail", "value": best["top3_day_any_severe_loss_rate"], "baseline": base["top3_day_any_severe_loss_rate"]},
            {"code": "actual_top3_membership_changes", "status": "pass" if branching else "fail", "value": best["changed_top3_members_count_vs_previous_best"], "changed_top3_day_rate": best["changed_top3_day_rate_vs_previous_best"]},
            {"code": "time_block_stability", "status": "pass" if stable else "fail", "value": stability_by_id.get(best["ranker_family_id"], {})},
            {"code": "split_leakage_audit_passed", "status": "pass" if no_leak else "fail", "value": no_leak},
            {"code": "artifact_complete", "status": "pass" if artifact_complete else "fail", "value": artifact_complete},
        ],
        "best_ranker_metrics": best,
        "previous_best_wide_score_metrics": base,
        "random_top3_metrics": random_base,
    }


def _artifact_complete(output_dir: Path, paths: dict[str, str], decision: dict[str, Any] | None = None) -> dict[str, Any]:
    excluded = {"_ARTIFACT_COMPLETE.json"}
    if decision is None:
        excluded.add("research_decision.json")
    required = {name: (output_dir / name).exists() for name in REQUIRED_ARTIFACTS if name not in excluded}
    return {
        "schema_version": f"{SCHEMA_PREFIX}_artifact_complete_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "artifact_root": str(output_dir),
        "complete": all(required.values()),
        "required_artifacts": required,
        "paths": paths,
        "decision": decision.get("decision") if decision else None,
        "authoritative_research_decision": decision.get("authoritative_research_decision") if decision else None,
        "silent_fallback_used": False,
        "research_fallback_used": False,
        "image_route_paused": True,
        "image_score_used": False,
        "cnn_score_used": False,
        "fusion_reranker_created": False,
        "ranking_objective_created": True,
        "candidate_scoring_created": True,
        "candidate_scoring_scope": "research_only",
        "production_ranking_changed": False,
        "publish_bundle_created": False,
        "meemee_reflectable": False,
    }


def run_ranking_loss_or_topk_objective_repair_v1(
    *,
    source_wide_run_id: str = DEFAULT_WIDE_RUN_ID,
    source_risk_run_id: str = DEFAULT_RISK_RUN_ID,
    source_threshold_run_id: str = DEFAULT_THRESHOLD_RUN_ID,
    source_feature_diagnosis_run_id: str = DEFAULT_FEATURE_DIAGNOSIS_RUN_ID,
    source_image_phase2_run_id: str = DEFAULT_IMAGE_PHASE2_RUN_ID,
    source_image_cnn_phase2b_run_id: str = DEFAULT_IMAGE_CNN_PHASE2B_RUN_ID,
    wide_root: str | Path = DEFAULT_WIDE_ROOT,
    risk_root: str | Path = DEFAULT_RISK_ROOT,
    threshold_root: str | Path = DEFAULT_THRESHOLD_ROOT,
    feature_diagnosis_root: str | Path = DEFAULT_FEATURE_DIAGNOSIS_ROOT,
    image_phase2_root: str | Path = DEFAULT_IMAGE_PHASE2_ROOT,
    image_cnn_phase2b_root: str | Path = DEFAULT_IMAGE_CNN_PHASE2B_ROOT,
    output_root: str | Path = DEFAULT_OUTPUT_ROOT,
    run_id: str | None = None,
) -> dict[str, Any]:
    wide_dir = _run_dir(wide_root, source_wide_run_id, DEFAULT_WIDE_ROOT)
    risk_dir = _run_dir(risk_root, source_risk_run_id, DEFAULT_RISK_ROOT)
    threshold_dir = _run_dir(threshold_root, source_threshold_run_id, DEFAULT_THRESHOLD_ROOT)
    feature_dir = _run_dir(feature_diagnosis_root, source_feature_diagnosis_run_id, DEFAULT_FEATURE_DIAGNOSIS_ROOT)
    image_phase2_dir = _run_dir(image_phase2_root, source_image_phase2_run_id, DEFAULT_IMAGE_PHASE2_ROOT)
    image_cnn_dir = _run_dir(image_cnn_phase2b_root, source_image_cnn_phase2b_run_id, DEFAULT_IMAGE_CNN_PHASE2B_ROOT)
    output_dir = _safe_path(output_root, DEFAULT_OUTPUT_ROOT) / (run_id.strip() if isinstance(run_id, str) and run_id.strip() else _default_run_id())
    source_status = validate_sources(
        wide_dir=wide_dir,
        risk_dir=risk_dir,
        threshold_dir=threshold_dir,
        feature_diagnosis_dir=feature_dir,
        image_phase2_dir=image_phase2_dir,
        image_cnn_phase2b_dir=image_cnn_dir,
    )
    events, _selected = load_ranker_events(
        pattern_dir=source_status["pattern_dir"],
        guard_dir=source_status["guard_dir"],
        upside_dir=source_status["upside_dir"],
        wide_dir=wide_dir,
        risk_dir=risk_dir,
        threshold_dir=threshold_dir,
    )
    feature_contract = build_feature_input_contract(events, source_status["feature"]["candidate_feature_shortlist.json"])
    feature_columns = list(feature_contract["used_features"])
    if not feature_columns:
        raise RuntimeError("no non-leaky feature inputs available for ranking objective")
    split_contract = build_split_contract(events)
    if split_contract["split_created"] is not True:
        raise RuntimeError("train-past-only time-block split could not be created")
    leakage_audit = build_leakage_audit(feature_columns, split_contract)
    if leakage_audit["split_leakage_audit_passed"] is not True:
        raise RuntimeError("leakage audit failed before scoring")
    score_ledger, training_rows = train_and_score_rankers(events, feature_columns, split_contract)
    reports = build_reports(score_ledger)
    time_stability = build_time_block_stability(score_ledger, reports)
    baseline_comparison = build_baseline_comparison_report(reports, source_status)
    source_dirs = {
        "wide": wide_dir,
        "risk": risk_dir,
        "threshold": threshold_dir,
        "feature_diagnosis": feature_dir,
        "image_phase2_reference_only": image_phase2_dir,
        "image_cnn_phase2b_reference_only": image_cnn_dir,
    }
    contracts_payload = build_contract_artifacts(
        output_dir=output_dir,
        source_dirs=source_dirs,
        source_status=source_status,
        events=events,
        feature_contract=feature_contract,
        split_contract=split_contract,
    )
    run_manifest = contracts.build_run_manifest(
        session_id=output_dir.name,
        seed=RANDOM_SEED,
        random_seed=RANDOM_SEED,
        input_artifacts=[
            {"name": "source_wide_artifact_root", "path": str(wide_dir)},
            {"name": "source_risk_artifact_root", "path": str(risk_dir)},
            {"name": "source_threshold_artifact_root", "path": str(threshold_dir)},
            {"name": "source_feature_diagnosis_artifact_root", "path": str(feature_dir)},
            {"name": "source_image_phase2_reference_root", "path": str(image_phase2_dir), "used_as_input": False},
            {"name": "source_image_cnn_phase2b_reference_root", "path": str(image_cnn_dir), "used_as_input": False},
            {"name": "evaluation_contract", "contract_hash": contracts_payload["evaluation_contract.json"]["contract_hash"]},
        ],
        asof=str(int(events["event_ymd"].max())),
        config={
            "axis_id": AXIS_ID,
            "top_k": TOP_K,
            "ranking_objective_created": True,
            "candidate_scoring_scope": "research_only",
            "image_route_paused": True,
            "image_score_used": False,
            "cnn_score_used": False,
            "production_ranking_changed": False,
        },
        universe=sorted(events["code"].astype(str).unique().tolist()),
        period={"start_date": str(int(events["event_ymd"].min())), "end_date": str(int(events["event_ymd"].max())), "label": "ranking_loss_or_topk_objective_repair"},
        horizon="20d",
        artifact_detail_level=contracts.TRADEX_ARTIFACT_DETAIL_LEVEL_AUTHORITATIVE,
        fallback_status=contracts.TRADEX_FALLBACK_STATUS_AUTHORITATIVE,
        cost_model=contracts.TRADEX_DEFAULT_COST_MODEL,
    )
    contracts.validate_run_manifest(run_manifest)
    paths: dict[str, str] = {}
    for name, payload in {
        **contracts_payload,
        "run_manifest.json": run_manifest,
        "leakage_audit.json": leakage_audit,
        "ranker_leaderboard.json": reports["ranker_leaderboard"],
        "top1_selection_report.json": reports["top1_selection_report"],
        "top3_selection_report.json": reports["top3_selection_report"],
        "day_level_top3_report.json": reports["day_level_top3_report"],
        "oracle_gap_report.json": reports["oracle_gap_report"],
        "badpick_report.json": reports["badpick_report"],
        "branching_report.json": reports["branching_report"],
        "time_block_stability.json": time_stability,
        "baseline_comparison_report.json": baseline_comparison,
    }.items():
        paths[name] = str(_write_json(output_dir / name, payload))
    paths["training_log.jsonl"] = str(_write_jsonl(output_dir / "training_log.jsonl", training_rows))
    paths["ranker_score_ledger.jsonl"] = str(_write_jsonl(output_dir / "ranker_score_ledger.jsonl", score_ledger.to_dict(orient="records")))
    pre_complete = _artifact_complete(output_dir, paths)
    decision = build_research_decision(
        reports=reports,
        time_stability=time_stability,
        leakage_audit=leakage_audit,
        artifact_complete=bool(pre_complete["complete"]),
    )
    paths["research_decision.json"] = str(_write_json(output_dir / "research_decision.json", decision))
    complete = _artifact_complete(output_dir, paths, decision)
    paths["_ARTIFACT_COMPLETE.json"] = str(_write_json(output_dir / "_ARTIFACT_COMPLETE.json", complete))
    return {
        "output_dir": str(output_dir),
        "decision": decision["decision"],
        "authoritative_research_decision": decision["authoritative_research_decision"],
        "best_ranker_family_id": decision["best_ranker_family_id"],
        "best_ranker_metrics": decision["best_ranker_metrics"],
        "image_route_paused": True,
        "image_score_used": False,
        "cnn_score_used": False,
        "fusion_reranker_created": False,
        "production_ranking_changed": False,
        "publish_bundle_created": False,
        "meemee_reflectable": False,
        "silent_fallback_used": False,
        "research_fallback_used": False,
    }


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-wide-run-id", default=DEFAULT_WIDE_RUN_ID)
    parser.add_argument("--source-risk-run-id", default=DEFAULT_RISK_RUN_ID)
    parser.add_argument("--source-threshold-run-id", default=DEFAULT_THRESHOLD_RUN_ID)
    parser.add_argument("--source-feature-diagnosis-run-id", default=DEFAULT_FEATURE_DIAGNOSIS_RUN_ID)
    parser.add_argument("--source-image-phase2-run-id", default=DEFAULT_IMAGE_PHASE2_RUN_ID)
    parser.add_argument("--source-image-cnn-phase2b-run-id", default=DEFAULT_IMAGE_CNN_PHASE2B_RUN_ID)
    parser.add_argument("--wide-root", default=str(DEFAULT_WIDE_ROOT))
    parser.add_argument("--risk-root", default=str(DEFAULT_RISK_ROOT))
    parser.add_argument("--threshold-root", default=str(DEFAULT_THRESHOLD_ROOT))
    parser.add_argument("--feature-diagnosis-root", default=str(DEFAULT_FEATURE_DIAGNOSIS_ROOT))
    parser.add_argument("--image-phase2-root", default=str(DEFAULT_IMAGE_PHASE2_ROOT))
    parser.add_argument("--image-cnn-phase2b-root", default=str(DEFAULT_IMAGE_CNN_PHASE2B_ROOT))
    parser.add_argument("--output-root", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--run-id", default="")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_arg_parser().parse_args(argv)
    result = run_ranking_loss_or_topk_objective_repair_v1(
        source_wide_run_id=args.source_wide_run_id,
        source_risk_run_id=args.source_risk_run_id,
        source_threshold_run_id=args.source_threshold_run_id,
        source_feature_diagnosis_run_id=args.source_feature_diagnosis_run_id,
        source_image_phase2_run_id=args.source_image_phase2_run_id,
        source_image_cnn_phase2b_run_id=args.source_image_cnn_phase2b_run_id,
        wide_root=args.wide_root,
        risk_root=args.risk_root,
        threshold_root=args.threshold_root,
        feature_diagnosis_root=args.feature_diagnosis_root,
        image_phase2_root=args.image_phase2_root,
        image_cnn_phase2b_root=args.image_cnn_phase2b_root,
        output_root=args.output_root,
        run_id=args.run_id.strip() or None,
    )
    print(_json_text(result))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
