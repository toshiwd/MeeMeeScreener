from __future__ import annotations

import argparse
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb
import numpy as np
import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score, balanced_accuracy_score, roc_auc_score
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler


AXIS_ID = "tradex_sideways_state_definition_v1"
DEFAULT_OUT = Path(r"G:\Tradex\tradex_sideways_state_definition_v1")
WINDOW = 15
EFFICIENCY_VARIANTS = {
    "direction_efficiency_20": 0.20,
    "direction_efficiency_30": 0.30,
    "direction_efficiency_40": 0.40,
}
MAX_SLOPE_SHARE = 0.35
EXPANSION_ATR = 2.0
HORIZONS = (5, 10, 20)
SELECTED_VARIANT = "direction_efficiency_20"
EVENT_GAP_SESSIONS = 20
DIRECTION_CHECKPOINTS = tuple(range(6))
DIRECTION_BALANCED_ACCURACY_GATE = 0.60
DIRECTION_ROC_AUC_GATE = 0.65
DIRECTION_MIN_SAMPLE = 1_000
DIRECTION_FEATURES = (
    "direction_efficiency",
    "signed_slope_share",
    "close_pos15",
    "candle_close_pos",
    "body_atr",
    "upper_wick_atr",
    "lower_wick_atr",
    "ret5",
    "ret20",
    "ret60",
    "ma20_ma60",
    "volume5_20",
    "atr14_atr60",
)


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, allow_nan=False) + "\n", encoding="utf-8")


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def load_bars(db_path: Path) -> pd.DataFrame:
    with duckdb.connect(str(db_path), read_only=True) as con:
        return con.execute(
            """
            select cast(strftime(to_timestamp(date), '%Y%m%d') as integer) as signal_ymd,
                   cast(code as varchar) as code,
                   cast(o as double) as o, cast(h as double) as h,
                   cast(l as double) as l, cast(c as double) as c,
                   cast(v as double) as v
            from daily_bars
            where source = 'pan' and c > 0 and h >= l
            order by code, signal_ymd
            """
        ).fetchdf()


def _rolling_signed_slope_share(close: pd.Series, window: int) -> pd.Series:
    x = np.arange(window, dtype=float)
    x_centered = x - x.mean()
    denominator = float(np.square(x_centered).sum())

    def calculate(values: np.ndarray) -> float:
        price_range = float(np.max(values) - np.min(values))
        if price_range <= 0:
            return 0.0
        slope = float(np.dot(x_centered, values - values.mean()) / denominator)
        return slope * (window - 1) / price_range

    return close.rolling(window, min_periods=window).apply(calculate, raw=True)


def build_observables(bars: pd.DataFrame, window: int = WINDOW) -> pd.DataFrame:
    required = {"signal_ymd", "code", "o", "h", "l", "c", "v"}
    missing = required.difference(bars.columns)
    if missing:
        raise ValueError(f"missing columns: {sorted(missing)}")
    out = bars.copy().sort_values(["code", "signal_ymd"]).reset_index(drop=True)
    parts: list[pd.DataFrame] = []
    for _, part in out.groupby("code", sort=False):
        part = part.copy().reset_index(drop=True)
        part["bar_index"] = np.arange(len(part), dtype=np.int32)
        close = part.c.astype(float)
        prev_close = close.shift(1)
        true_range = pd.concat(
            [(part.h - part.l).abs(), (part.h - prev_close).abs(), (part.l - prev_close).abs()], axis=1
        ).max(axis=1)
        path = close.diff().abs().rolling(window - 1, min_periods=window - 1).sum()
        net = (close - close.shift(window - 1)).abs()
        part["direction_efficiency"] = net / path.replace(0.0, np.nan)
        part.loc[(path == 0.0) & (net == 0.0), "direction_efficiency"] = 0.0
        part["signed_slope_share"] = _rolling_signed_slope_share(close, window)
        part["slope_share"] = part.signed_slope_share.abs()
        part["atr14"] = true_range.rolling(14, min_periods=14).mean()
        atr60 = true_range.rolling(60, min_periods=60).mean()
        low15 = part.l.rolling(window, min_periods=window).min()
        high15 = part.h.rolling(window, min_periods=window).max()
        part["close_pos15"] = (close - low15) / (high15 - low15).replace(0.0, np.nan)
        part["candle_close_pos"] = (close - part.l) / (part.h - part.l).replace(0.0, np.nan)
        part["body_atr"] = (close - part.o).abs() / part["atr14"]
        part["upper_wick_atr"] = (part.h - pd.concat([part.o, close], axis=1).max(axis=1)) / part["atr14"]
        part["lower_wick_atr"] = (pd.concat([part.o, close], axis=1).min(axis=1) - part.l) / part["atr14"]
        part["ret5"] = close / close.shift(5) - 1.0
        part["ret20"] = close / close.shift(20) - 1.0
        part["ret60"] = close / close.shift(60) - 1.0
        part["ma20_ma60"] = close.rolling(20, min_periods=20).mean() / close.rolling(60, min_periods=60).mean() - 1.0
        part["volume5_20"] = part.v.rolling(5, min_periods=5).mean() / part.v.rolling(20, min_periods=20).mean().replace(0.0, np.nan)
        part["atr14_atr60"] = part["atr14"] / atr60.replace(0.0, np.nan)
        part["window_range_pct"] = (
            part.h.rolling(window, min_periods=window).max()
            - part.l.rolling(window, min_periods=window).min()
        ) / close
        parts.append(part)
    return pd.concat(parts, ignore_index=True)


def mark_sideways(observables: pd.DataFrame, efficiency_limit: float) -> pd.DataFrame:
    out = observables.copy().sort_values(["code", "signal_ymd"]).reset_index(drop=True)
    out["sideways_state"] = (
        out.direction_efficiency.le(efficiency_limit)
        & out.slope_share.le(MAX_SLOPE_SHARE)
        & out.atr14.gt(0)
    ).fillna(False)
    previous = out.groupby("code").sideways_state.shift(1).eq(True)
    out["sideways_start"] = out.sideways_state & ~previous
    out["definition_variant"] = next(
        (name for name, value in EFFICIENCY_VARIANTS.items() if value == efficiency_limit),
        f"direction_efficiency_{efficiency_limit:g}",
    )
    return out


def attach_forward_labels(frame: pd.DataFrame) -> pd.DataFrame:
    parts: list[pd.DataFrame] = []
    for _, part in frame.groupby("code", sort=False):
        part = part.sort_values("signal_ymd").copy().reset_index(drop=True)
        for horizon in HORIZONS:
            future_high = pd.concat([part.h.shift(-i) for i in range(1, horizon + 1)], axis=1).max(axis=1)
            future_low = pd.concat([part.l.shift(-i) for i in range(1, horizon + 1)], axis=1).min(axis=1)
            complete_horizon = part.c.shift(-horizon).notna()
            part[f"up_atr_{horizon}"] = ((future_high - part.c) / part.atr14).where(complete_horizon)
            part[f"down_atr_{horizon}"] = ((part.c - future_low) / part.atr14).where(complete_horizon)
            part[f"expansion_{horizon}"] = (
                part[[f"up_atr_{horizon}", f"down_atr_{horizon}"]].max(axis=1) >= EXPANSION_ATR
            ).where(complete_horizon).astype("boolean")
        upper = part.c + EXPANSION_ATR * part.atr14
        lower = part.c - EXPANSION_ATR * part.atr14
        first_up = np.zeros(len(part), dtype=np.int16)
        first_down = np.zeros(len(part), dtype=np.int16)
        for offset in range(1, max(HORIZONS) + 1):
            up_hit = part.h.shift(-offset).ge(upper).fillna(False).to_numpy()
            down_hit = part.l.shift(-offset).le(lower).fillna(False).to_numpy()
            first_up[(first_up == 0) & up_hit] = offset
            first_down[(first_down == 0) & down_hit] = offset
        part["first_up_expansion_day"] = pd.array(pd.Series(first_up).mask(first_up == 0), dtype="Int64")
        part["first_down_expansion_day"] = pd.array(pd.Series(first_down).mask(first_down == 0), dtype="Int64")
        part["realized_direction_20"] = np.select(
            [
                (first_up > 0) & ((first_down == 0) | (first_up < first_down)),
                (first_down > 0) & ((first_up == 0) | (first_down < first_up)),
                (first_up > 0) & (first_up == first_down),
            ],
            ["up", "down", "same_day_both"],
            default="unresolved",
        )
        parts.append(part)
    return pd.concat(parts, ignore_index=True)


def deduplicate_events(events: pd.DataFrame, min_gap: int = EVENT_GAP_SESSIONS) -> pd.DataFrame:
    selected: list[int] = []
    for _, part in events.sort_values(["code", "bar_index"]).groupby("code", sort=False):
        last_index = -10**9
        for index, current_value in zip(part.index.to_numpy(), part.bar_index.to_numpy()):
            current = int(current_value)
            if current - last_index >= min_gap:
                selected.append(int(index))
                last_index = current
    return events.loc[selected].sort_values(["signal_ymd", "code"]).reset_index(drop=True)


def matched_non_sideways_controls(labeled: pd.DataFrame, events: pd.DataFrame) -> pd.DataFrame:
    candidates = labeled[
        ~labeled.sideways_state
        & labeled.up_atr_20.notna()
        & labeled.down_atr_20.notna()
    ].copy()
    candidates["year"] = candidates.signal_ymd // 10000
    event_counts = events.assign(year=events.signal_ymd // 10000).groupby(["code", "year"]).size()
    candidate_groups = {
        key: part.sort_values("bar_index")
        for key, part in candidates.groupby(["code", "year"], sort=False)
    }
    controls: list[pd.DataFrame] = []
    for (code, year), requested in event_counts.items():
        pool = candidate_groups.get((code, year))
        if pool is None or pool.empty:
            continue
        spaced = deduplicate_events(pool, EVENT_GAP_SESSIONS)
        take = min(int(requested), len(spaced))
        if take <= 0:
            continue
        positions = np.linspace(0, len(spaced) - 1, num=take, dtype=int)
        controls.append(spaced.iloc[np.unique(positions)])
    if not controls:
        return candidates.iloc[:0].copy()
    return pd.concat(controls, ignore_index=True).sort_values(["signal_ymd", "code"]).reset_index(drop=True)


def _split_metrics(frame: pd.DataFrame) -> dict[str, Any]:
    valid = frame[frame.up_atr_20.notna() & frame.down_atr_20.notna()].copy()
    valid[["up_atr_20", "down_atr_20"]] = valid[["up_atr_20", "down_atr_20"]].replace([np.inf, -np.inf], np.nan)
    valid = valid[valid.up_atr_20.notna() & valid.down_atr_20.notna()]
    return {
        "sample_count": int(len(valid)),
        "expansion_rate_5": float(valid.expansion_5.mean()) if len(valid) else None,
        "expansion_rate_10": float(valid.expansion_10.mean()) if len(valid) else None,
        "expansion_rate_20": float(valid.expansion_20.mean()) if len(valid) else None,
        "mean_up_atr_20": float(valid.up_atr_20.mean()) if len(valid) else None,
        "mean_down_atr_20": float(valid.down_atr_20.mean()) if len(valid) else None,
    }


def compare_with_controls(events: pd.DataFrame, controls: pd.DataFrame) -> dict[str, Any]:
    event_metrics = _split_metrics(events)
    control_metrics = _split_metrics(controls)
    uplift = None
    if event_metrics["expansion_rate_20"] is not None and control_metrics["expansion_rate_20"] is not None:
        uplift = event_metrics["expansion_rate_20"] - control_metrics["expansion_rate_20"]
    yearly: dict[str, Any] = {}
    for year in sorted(set((events.signal_ymd // 10000).tolist())):
        event_year = events[events.signal_ymd // 10000 == year]
        control_year = controls[controls.signal_ymd // 10000 == year]
        event_year_metrics = _split_metrics(event_year)
        control_year_metrics = _split_metrics(control_year)
        year_uplift = None
        if event_year_metrics["expansion_rate_20"] is not None and control_year_metrics["expansion_rate_20"] is not None:
            year_uplift = event_year_metrics["expansion_rate_20"] - control_year_metrics["expansion_rate_20"]
        yearly[str(year)] = {"sideways": event_year_metrics, "control": control_year_metrics, "expansion_rate_20_uplift": year_uplift}
    return {
        "sideways": event_metrics,
        "matched_non_sideways_control": control_metrics,
        "expansion_rate_20_uplift": uplift,
        "positive_uplift_year_count": sum(1 for row in yearly.values() if (row["expansion_rate_20_uplift"] or 0.0) > 0.0),
        "comparable_year_count": sum(1 for row in yearly.values() if row["expansion_rate_20_uplift"] is not None),
        "yearly": yearly,
    }


def _variant_metrics(events: pd.DataFrame) -> dict[str, Any]:
    result: dict[str, Any] = {"event_count": int(len(events))}
    for horizon in HORIZONS:
        valid = events[f"up_atr_{horizon}"].notna() & events[f"down_atr_{horizon}"].notna()
        result[f"valid_{horizon}"] = int(valid.sum())
        result[f"expansion_rate_{horizon}"] = float(events.loc[valid, f"expansion_{horizon}"].mean()) if valid.any() else None
    counts = events.realized_direction_20.value_counts().to_dict()
    result["direction_counts_20"] = {str(key): int(value) for key, value in counts.items()}
    resolved = events[events.realized_direction_20.isin(["up", "down"])]
    result["resolved_count_20"] = int(len(resolved))
    result["up_share_resolved_20"] = float((resolved.realized_direction_20 == "up").mean()) if len(resolved) else None
    first = pd.concat([resolved.first_up_expansion_day, resolved.first_down_expansion_day], axis=1).min(axis=1)
    result["median_first_decisive_day_20"] = float(first.median()) if len(first.dropna()) else None
    return result


def build_direction_snapshots(observables: pd.DataFrame, events: pd.DataFrame) -> pd.DataFrame:
    resolved = events[events.realized_direction_20.isin(["up", "down"])].copy().reset_index(drop=True)
    resolved["event_id"] = np.arange(len(resolved), dtype=np.int64)
    resolved["direction_up"] = (resolved.realized_direction_20 == "up").astype(np.int8)
    resolved["decision_day"] = resolved[["first_up_expansion_day", "first_down_expansion_day"]].min(axis=1)
    lookup_columns = ["code", "bar_index", *DIRECTION_FEATURES]
    lookup = observables[lookup_columns]
    snapshots: list[pd.DataFrame] = []
    base_columns = ["event_id", "code", "signal_ymd", "bar_index", "direction_up", "decision_day"]
    for checkpoint in DIRECTION_CHECKPOINTS:
        keys = resolved[base_columns].copy()
        keys["snapshot_bar_index"] = keys.bar_index + checkpoint
        snapshot = keys.merge(
            lookup,
            left_on=["code", "snapshot_bar_index"],
            right_on=["code", "bar_index"],
            how="inner",
            suffixes=("_event", "_snapshot"),
            validate="many_to_one",
        )
        snapshot["checkpoint"] = checkpoint
        snapshot = snapshot[snapshot.decision_day > checkpoint]
        snapshots.append(snapshot)
    return pd.concat(snapshots, ignore_index=True)


def _direction_model() -> Pipeline:
    feature_list = list(DIRECTION_FEATURES)
    return Pipeline(
        [
            (
                "prepare",
                ColumnTransformer(
                    [
                        (
                            "numeric",
                            Pipeline(
                                [
                                    ("impute", SimpleImputer(strategy="median")),
                                    ("scale", StandardScaler()),
                                ]
                            ),
                            feature_list,
                        )
                    ]
                ),
            ),
            ("model", LogisticRegression(max_iter=500, class_weight="balanced", C=0.3)),
        ]
    )


def _classification_metrics(model: Pipeline, frame: pd.DataFrame) -> dict[str, Any]:
    if frame.empty or frame.direction_up.nunique() < 2:
        return {"sample_count": int(len(frame)), "accuracy": None, "balanced_accuracy": None, "roc_auc": None, "prior20_sign_balanced_accuracy": None}
    feature_frame = frame[list(DIRECTION_FEATURES)].replace([np.inf, -np.inf], np.nan)
    probabilities = model.predict_proba(feature_frame)[:, 1]
    predictions = (probabilities >= 0.5).astype(np.int8)
    baseline = frame.ret20.fillna(0.0).ge(0.0).astype(np.int8)
    return {
        "sample_count": int(len(frame)),
        "accuracy": float(accuracy_score(frame.direction_up, predictions)),
        "balanced_accuracy": float(balanced_accuracy_score(frame.direction_up, predictions)),
        "roc_auc": float(roc_auc_score(frame.direction_up, probabilities)),
        "prior20_sign_balanced_accuracy": float(balanced_accuracy_score(frame.direction_up, baseline)),
        "up_share": float(frame.direction_up.mean()),
    }


def _empty_classification_metrics(frame: pd.DataFrame) -> dict[str, Any]:
    return {
        "sample_count": int(len(frame)),
        "accuracy": None,
        "balanced_accuracy": None,
        "roc_auc": None,
        "prior20_sign_balanced_accuracy": None,
    }


def evaluate_direction_timing(snapshots: pd.DataFrame) -> dict[str, Any]:
    rows: list[dict[str, Any]] = []
    for checkpoint in DIRECTION_CHECKPOINTS:
        frame = snapshots[snapshots.checkpoint == checkpoint]
        train = frame[frame.signal_ymd <= 20211231]
        validation = frame[frame.signal_ymd.between(20220101, 20231231)]
        test = frame[frame.signal_ymd >= 20240101]
        model = _direction_model()
        if train.empty or train.direction_up.nunique() < 2:
            validation_metrics = _empty_classification_metrics(validation)
            test_metrics = _empty_classification_metrics(test)
        else:
            model.fit(train[list(DIRECTION_FEATURES)].replace([np.inf, -np.inf], np.nan), train.direction_up)
            validation_metrics = _classification_metrics(model, validation)
            test_metrics = _classification_metrics(model, test)
        gate = all(
            metrics["sample_count"] >= DIRECTION_MIN_SAMPLE
            and (metrics["balanced_accuracy"] or 0.0) >= DIRECTION_BALANCED_ACCURACY_GATE
            and (metrics["roc_auc"] or 0.0) >= DIRECTION_ROC_AUC_GATE
            for metrics in (validation_metrics, test_metrics)
        )
        rows.append(
            {
                "checkpoint": checkpoint,
                "train_sample_count": int(len(train)),
                "validation": validation_metrics,
                "test": test_metrics,
                "identifiability_gate_pass": gate,
            }
        )
    return {
        "rows": rows,
        "first_identifiable_checkpoint": first_identifiable_checkpoint(rows),
        "gate": {
            "balanced_accuracy_min": DIRECTION_BALANCED_ACCURACY_GATE,
            "roc_auc_min": DIRECTION_ROC_AUC_GATE,
            "minimum_sample_each_validation_surface": DIRECTION_MIN_SAMPLE,
            "must_pass": ["validation_2022_2023", "test_2024_plus"],
        },
    }


def first_identifiable_checkpoint(rows: list[dict[str, Any]]) -> int | None:
    for row in rows:
        if row.get("identifiability_gate_pass"):
            return int(row["checkpoint"])
    return None


def generate(db_path: Path, out_root: Path) -> Path:
    bars = load_bars(db_path)
    observables = attach_forward_labels(build_observables(bars))
    variant_frames: list[pd.DataFrame] = []
    control_frames: list[pd.DataFrame] = []
    comparisons: dict[str, Any] = {}
    for name, limit in EFFICIENCY_VARIANTS.items():
        labeled = mark_sideways(observables, limit)
        raw_events = labeled[labeled.sideways_start & labeled.up_atr_20.notna() & labeled.down_atr_20.notna()].copy()
        events = deduplicate_events(raw_events)
        controls = matched_non_sideways_controls(labeled, events)
        controls["definition_variant"] = name
        variant_frames.append(events)
        control_frames.append(controls)
        comparisons[name] = {
            "raw_event_count": int(len(raw_events)),
            "deduplicated_event_metrics": _variant_metrics(events),
            "matched_control_comparison": compare_with_controls(events, controls),
        }
    ledger = pd.concat(variant_frames, ignore_index=True)
    control_ledger = pd.concat(control_frames, ignore_index=True)
    selected = SELECTED_VARIANT
    selected_events = ledger[ledger.definition_variant == selected].copy().reset_index(drop=True)
    snapshots = build_direction_snapshots(observables, selected_events)
    direction_evaluation = evaluate_direction_timing(snapshots)
    created = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    root = out_root / f"{created}-{AXIS_ID}"
    root.mkdir(parents=True, exist_ok=False)
    ledger.to_parquet(root / "event_ledger.parquet", index=False)
    control_ledger.to_parquet(root / "matched_control_ledger.parquet", index=False)
    snapshots.to_parquet(root / "direction_snapshots.parquet", index=False)
    fixed = {
        "universe": "all PAN daily_bars in supplied formal DB",
        "window_sessions": WINDOW,
        "sideways_semantics": "low directional efficiency and low regression slope share; no edge-touch or fixed box boundary requirement",
        "changed_axis": "direction_efficiency_limit only",
        "fixed_max_slope_share": MAX_SLOPE_SHARE,
        "efficiency_variants": EFFICIENCY_VARIANTS,
        "outcome_horizons": HORIZONS,
        "expansion_threshold_atr": EXPANSION_ATR,
        "event_dedup_minimum_trading_sessions": EVENT_GAP_SESSIONS,
        "matched_control": "same code and calendar year; non-sideways; deterministic evenly spaced sample after 20-session dedup",
        "selected_definition": selected,
        "selected_definition_policy": "precommitted strictest direction-efficiency threshold; 0.30 and 0.40 are sensitivity checks only",
        "direction_splits": {"train": "through 2021", "validation": "2022-2023", "test": "2024+"},
        "direction_checkpoints_after_sideways_start": DIRECTION_CHECKPOINTS,
        "direction_features": DIRECTION_FEATURES,
        "costs": "ignored",
        "runtime_db_write": False,
        "meemee_changed": False,
        "production_ranking_changed": False,
    }
    definition_compare = {
        "schema_version": f"{AXIS_ID}.definition_compare.v1",
        "artifact_role": "authoritative",
        "research_phase": "comparison_stabilization",
        "fixed_evaluation_conditions": fixed,
        "variants": comparisons,
        "selected_for_next_axis": selected,
        "selection_reason": "precommitted strictest low-direction-efficiency definition; no outcome-based winner selection",
        "judgment": "keep_definition_drop_expansion_hypothesis" if (comparisons[selected]["matched_control_comparison"]["expansion_rate_20_uplift"] or 0.0) <= 0.0 else "keep_definition_hold_expansion_hypothesis",
    }
    direction_timing = {
        "schema_version": f"{AXIS_ID}.direction_timing.v2",
        "artifact_role": "authoritative",
        "selected_variant": selected,
        "label": "which 2ATR barrier is reached first within 20 sessions",
        "checkpoint_semantics": "sessions after sideways start; events already resolved by the checkpoint are excluded",
        "point_in_time_features_only": True,
        "evaluation": direction_evaluation,
        "judgment": "keep_direction_timing_review" if direction_evaluation["first_identifiable_checkpoint"] is not None else "drop_direction_identifiability",
    }
    leaderboard = {
        "schema_version": f"{AXIS_ID}.family_leaderboard.v2",
        "artifact_role": "authoritative",
        "rows": [dict(variant=name, **metrics) for name, metrics in comparisons.items()],
        "leader": selected,
        "leader_policy": "semantic precommitment, not best realized outcome",
    }
    selected_comparison = comparisons[selected]["matched_control_comparison"]
    expansion_uplift = selected_comparison["expansion_rate_20_uplift"]
    first_checkpoint = direction_evaluation["first_identifiable_checkpoint"]
    compare = {
        "schema_version": f"{AXIS_ID}.compare.v2",
        "artifact_role": "authoritative",
        "axis_id": AXIS_ID,
        "fixed_evaluation_conditions": fixed,
        "source_artifact": {"path": str(db_path), "sha256": _sha256(db_path)},
        "source_coverage": {
            "min_ymd": int(bars.signal_ymd.min()), "max_ymd": int(bars.signal_ymd.max()),
            "rows": int(len(bars)), "codes": int(bars.code.nunique()),
        },
        "selected_variant": selected,
        "authoritative_result": {
            "sideways_expansion_rate_20": selected_comparison["sideways"]["expansion_rate_20"],
            "matched_control_expansion_rate_20": selected_comparison["matched_non_sideways_control"]["expansion_rate_20"],
            "expansion_rate_20_uplift": expansion_uplift,
            "first_direction_identifiable_checkpoint": first_checkpoint,
        },
        "observed_branching": {
            "changed_top5_members_count": None,
            "changed_top10_members_count": None,
            "changed_rank_count": None,
            "selection_divergence_reason": "not a ranking challenger; state-definition research only",
            "boundary_metrics": {"raw_to_deduplicated_events": [comparisons[selected]["raw_event_count"], comparisons[selected]["deduplicated_event_metrics"]["event_count"]]},
        },
        "decision": {
            "candidate_local_decision": "keep_direction_timing_only" if first_checkpoint is not None else "drop",
            "session_aggregate_decision": "drop_expansion_keep_direction_timing_review" if (expansion_uplift or 0.0) <= 0.0 and first_checkpoint is not None else "hold",
            "authoritative_rollup_decision": "review_only",
            "reason_type": "matched_control_expansion_and_fixed_oos_direction_checkpoint_gates",
        },
        "verify": {
            "fixed_condition_comparison_check": True,
            "artifact_existence_check": True,
            "branching_happened": "not_applicable",
            "branching_helped_or_hurt": "not_applicable",
        },
        "silent_fallback_used": False,
        "runtime_db_write": False,
        "production_ranking_changed": False,
        "meemee_changed": False,
    }
    _write_json(root / "definition_compare.json", definition_compare)
    _write_json(root / "direction_timing.json", direction_timing)
    _write_json(root / "family_leaderboard.json", leaderboard)
    _write_json(root / "compare.json", compare)
    _write_json(root / "_ARTIFACT_COMPLETE.json", {"axis_id": AXIS_ID, "complete": True, "compare": str(root / "compare.json")})
    return root / "compare.json"


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", type=Path, required=True)
    parser.add_argument("--out", type=Path, default=DEFAULT_OUT)
    args = parser.parse_args()
    print(generate(args.db, args.out))


if __name__ == "__main__":
    main()
