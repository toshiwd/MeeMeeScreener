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
from sklearn.tree import DecisionTreeClassifier, export_text


AXIS_ID = "tradex_early_signal_first_detect_v1"
DEFAULT_OUT = Path(r"G:\Tradex\early_signal_first_detect_v1")
BASE_FEATURES = [
    "weekly_direction_prob", "weekly_range_prob", "monthly_direction_prob", "monthly_range_prob",
    "candle_triplet_direction_prob", "candle_body_ratio", "candle_upper_wick_ratio",
    "candle_lower_wick_ratio", "close_ret2", "close_ret3", "close_ret20", "close_ret60",
    "atr14_pct", "range_pct", "gap_pct", "vol_ratio5_20", "turnover_z20", "high20_dist",
    "low20_dist", "rel_ret20",
]
DERIVED_FEATURES = ["ma_spread_20_60", "high_block_delta_5d", "low_block_delta_5d"]
FEATURES = BASE_FEATURES + DERIVED_FEATURES
CHECKPOINTS = (60, 20, 10, 5, 1)


def _json_ready(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(k): _json_ready(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_ready(v) for v in value]
    if isinstance(value, (np.integer,)): return int(value)
    if isinstance(value, (np.floating,)): return None if not np.isfinite(value) else float(value)
    if isinstance(value, float): return None if not np.isfinite(value) else value
    if isinstance(value, Path): return str(value)
    return value


def _write_json(path: Path, value: dict[str, Any]) -> None:
    path.write_text(json.dumps(_json_ready(value), ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _sha(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for block in iter(lambda: f.read(8 << 20), b""): h.update(block)
    return h.hexdigest()


def load_source(db_path: Path) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, Any]]:
    with duckdb.connect(str(db_path), read_only=True) as con:
        cols = {r[0] for r in con.execute("describe ml_feature_daily").fetchall()}
        required = {
            "dt", "code", "close", "ma20", "ma60", "weekly_breakout_up_prob", "weekly_breakout_down_prob",
            "weekly_range_prob", "monthly_breakout_up_prob", "monthly_breakout_down_prob", "monthly_range_prob",
            "candle_triplet_up_prob", "candle_triplet_down_prob", "candle_body_ratio", "candle_upper_wick_ratio",
            "candle_lower_wick_ratio", "close_ret2", "close_ret3", "close_ret20", "close_ret60", "atr14_pct",
            "range_pct", "gap_pct", "vol_ratio5_20", "turnover_z20", "high20_dist", "low20_dist", "rel_ret20",
        }
        missing = sorted(required - cols)
        if missing: raise ValueError("FEATURE_COVERAGE_MISSING:" + ",".join(missing))
        features = con.execute("""
            select cast(strftime(to_timestamp(dt),'%Y%m%d') as int) signal_ymd, cast(code as varchar) code,
                   close,ma20,ma60,weekly_breakout_up_prob,weekly_breakout_down_prob,weekly_range_prob,
                   monthly_breakout_up_prob,monthly_breakout_down_prob,monthly_range_prob,
                   candle_triplet_up_prob,candle_triplet_down_prob,candle_body_ratio,candle_upper_wick_ratio,
                   candle_lower_wick_ratio,close_ret2,close_ret3,close_ret20,close_ret60,atr14_pct,range_pct,
                   gap_pct,vol_ratio5_20,turnover_z20,high20_dist,low20_dist,rel_ret20
            from ml_feature_daily
            where dt between epoch(strptime('20240101','%Y%m%d')) and epoch(strptime('20261231','%Y%m%d'))
            order by code, signal_ymd
        """).fetchdf()
        bars = con.execute("""
            select cast(code as varchar) code, cast(strftime(to_timestamp(date),'%Y%m%d') as int) signal_ymd,
                   o,h,l,c from daily_bars where source='pan'
            order by code, signal_ymd
        """).fetchdf()
        rankings = con.execute("""
            select dt signal_ymd,cast(code as varchar) code,dir,rank
            from ranking_appearance_daily
            where ranking_logic_version='ranking:trade:top50:v1' and rank<=10 and dt between 20240101 and 20261231
            order by dt,dir,rank,code
        """).fetchdf()
    features["code"] = features.code.astype(str); bars["code"] = bars.code.astype(str)
    coverage = {
        "feature_min_date": int(features.signal_ymd.min()), "feature_max_date": int(features.signal_ymd.max()),
        "feature_rows": int(len(features)), "feature_codes": int(features.code.nunique()),
        "pan_min_date": int(bars.signal_ymd.min()), "pan_max_date": int(bars.signal_ymd.max()),
        "pan_rows": int(len(bars)), "pan_codes": int(bars.code.nunique()),
        "ranking_min_date": int(rankings.signal_ymd.min()), "ranking_max_date": int(rankings.signal_ymd.max()),
        "ranking_rows_top10": int(len(rankings)),
    }
    return features, bars, {"coverage": coverage, "rankings": rankings}


def attach_past_features(features: pd.DataFrame, side: str) -> pd.DataFrame:
    out = features.copy().sort_values(["code", "signal_ymd"])
    is_buy = side == "BUY"
    out["weekly_direction_prob"] = out["weekly_breakout_up_prob" if is_buy else "weekly_breakout_down_prob"]
    out["monthly_direction_prob"] = out["monthly_breakout_up_prob" if is_buy else "monthly_breakout_down_prob"]
    out["candle_triplet_direction_prob"] = out["candle_triplet_up_prob" if is_buy else "candle_triplet_down_prob"]
    sign = 1.0 if is_buy else -1.0
    for col in ("close_ret2", "close_ret3", "close_ret20", "close_ret60", "gap_pct", "rel_ret20"):
        out[col] = pd.to_numeric(out[col], errors="coerce") * sign
    out["ma_spread_20_60"] = (out.ma20 / out.ma60 - 1.0) * sign
    # Exact lagged block change: no current/future value beyond the signal row is referenced.
    out["high_block_delta_5d"] = (out.high20_dist - out.groupby("code").high20_dist.shift(5)) * sign
    out["low_block_delta_5d"] = (out.low20_dist - out.groupby("code").low20_dist.shift(5)) * sign
    out["side"] = side
    return out


def attach_outcomes(frame: pd.DataFrame, bars: pd.DataFrame, side: str) -> pd.DataFrame:
    records: list[pd.DataFrame] = []
    sign = 1.0 if side == "BUY" else -1.0
    for code, part in bars.groupby("code", sort=False):
        part = part.sort_values("signal_ymd").reset_index(drop=True)
        n = len(part); entry = part.o.shift(-1).to_numpy(float)
        first20 = np.zeros(n, dtype=np.int16); label20 = np.zeros(n, dtype=np.int8); mover20 = np.zeros(n, dtype=np.int8)
        first10 = np.zeros(n, dtype=np.int16); label10 = np.zeros(n, dtype=np.int8)
        for day in range(1, 21):
            hi = part.h.shift(-day).to_numpy(float); lo = part.l.shift(-day).to_numpy(float)
            target = hi >= entry * 1.08 if side == "BUY" else lo <= entry * .92
            stop = lo <= entry * .95 if side == "BUY" else hi >= entry * 1.05
            mover20 |= target.astype(np.int8)
            unresolved20 = first20 == 0; event20 = unresolved20 & (target | stop)
            first20[event20] = day; label20[event20] = (target[event20] & ~stop[event20]).astype(np.int8)
            if day <= 10:
                unresolved10 = first10 == 0; event10 = unresolved10 & (target | stop)
                first10[event10] = day; label10[event10] = (target[event10] & ~stop[event10]).astype(np.int8)
        terminal10 = part.c.shift(-10).to_numpy(float)
        trade_ret = np.where(first10 > 0, np.where(label10 == 1, .08, -.05), sign * (terminal10 / entry - 1.0)) - .001
        # Full 20 sessions are mandatory for the diagnostic label and mover cohort.
        valid20 = np.arange(n) + 20 < n
        records.append(pd.DataFrame({"code": str(code), "signal_ymd": part.signal_ymd, "side": side,
                                     "target_before_stop20": np.where(valid20, label20, np.nan),
                                     "realized_mover20": np.where(valid20, mover20, np.nan),
                                     "trade_return_h10": np.where(np.arange(n) + 10 < n, trade_ret, np.nan),
                                     "exit_day_h10": np.where(np.arange(n) + 10 < n, np.where(first10 > 0, first10, 10), np.nan)}))
    outcomes = pd.concat(records, ignore_index=True) if records else pd.DataFrame()
    return frame.merge(outcomes, on=["code", "signal_ymd", "side"], how="left", validate="one_to_one")


def fit_and_score(frame: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, Any], float]:
    train = frame[(frame.signal_ymd >= 20240101) & (frame.signal_ymd <= 20241231) & frame.target_before_stop20.notna()].copy()
    medians = {c: float(pd.to_numeric(train[c], errors="coerce").median()) for c in FEATURES}
    if len(train) < 1000 or any(not np.isfinite(v) for v in medians.values()): raise ValueError("TRAIN_FEATURE_COVERAGE_INSUFFICIENT")
    y = train.target_before_stop20.astype(int)
    if y.nunique() != 2: raise ValueError("TRAIN_LABEL_SINGLE_CLASS")
    model = DecisionTreeClassifier(max_depth=3, min_samples_leaf=200, random_state=0)
    model.fit(train[FEATURES].apply(pd.to_numeric, errors="coerce").fillna(medians), y)
    scored = frame.copy()
    classes = list(model.classes_)
    scored["score"] = model.predict_proba(scored[FEATURES].apply(pd.to_numeric, errors="coerce").fillna(medians))[:, classes.index(1)]
    scored = scored.sort_values(["signal_ymd", "score", "code"], ascending=[True, False, True])
    scored["rank"] = scored.groupby("signal_ymd").cumcount() + 1
    scored["percentile"] = 1.0 - ((scored["rank"] - 1) / scored.groupby("signal_ymd").code.transform("size"))
    scored["top10"] = scored["rank"] <= 10
    train_counts = scored[(scored.signal_ymd >= 20240101) & (scored.signal_ymd <= 20241231)].groupby("signal_ymd").size()
    percentile_cut = 1.0 - 10.0 / float(train_counts.mean())
    threshold = float(scored[(scored.signal_ymd >= 20240101) & (scored.signal_ymd <= 20241231)].score.quantile(percentile_cut))
    tree = model.tree_
    payload = {"estimator": "DecisionTreeClassifier", "parameters": {"max_depth": 3, "min_samples_leaf": 200, "random_state": 0},
               "features": FEATURES, "train_medians": medians, "classes": [int(x) for x in model.classes_],
               "train_fixed_top10_equivalent_threshold": threshold, "train_mean_daily_universe": float(train_counts.mean()),
               "text": export_text(model, feature_names=FEATURES),
               "nodes": [{"node": i, "feature": FEATURES[tree.feature[i]] if tree.feature[i] >= 0 else None,
                          "threshold": float(tree.threshold[i]) if tree.feature[i] >= 0 else None,
                          "samples": int(tree.n_node_samples[i])} for i in range(tree.node_count)]}
    return scored, payload, threshold


def _pf(s: pd.Series) -> float | None:
    win, loss = float(s[s > 0].sum()), float(-s[s < 0].sum())
    return win / loss if loss else None


def _metrics(frame: pd.DataFrame, calendar_days: int) -> dict[str, Any]:
    if frame.empty: return {"n": 0, "signal_days": 0, "profit_factor": None, "expectancy": None, "calendar_expectancy": 0.0, "cvar10": None, "max_drawdown": None, "signals_per_week": 0.0}
    daily = frame.groupby("signal_ymd").trade_return_h10.mean(); cutoff = daily.quantile(.1)
    curve = (1 + daily).cumprod(); dd = curve / curve.cummax() - 1
    weeks = pd.to_datetime(daily.index.astype(str), format="%Y%m%d").strftime("%G-W%V").nunique()
    return {"n": int(len(frame)), "signal_days": int(len(daily)), "profit_factor": _pf(daily), "event_profit_factor": _pf(frame.trade_return_h10),
            "expectancy": float(daily.mean()), "calendar_expectancy": float(daily.sum() / calendar_days) if calendar_days else None,
            "precision": float(frame.target_before_stop20.mean()), "cvar10": float(daily[daily <= cutoff].mean()),
            "max_drawdown": float(dd.min()), "signals_per_week": float(len(daily) / weeks) if weeks else 0.0}


def _branching(model_top: pd.DataFrame, baseline: pd.DataFrame, side: str, split_start: int, split_end: int) -> dict[str, Any]:
    left = model_top[(model_top.side == side) & model_top.signal_ymd.between(split_start, split_end)]
    right = baseline[(baseline.side == side) & baseline.signal_ymd.between(split_start, split_end)]
    dates = sorted(set(left.signal_ymd) & set(right.signal_ymd)); rows = []
    for d in dates:
        a = left[left.signal_ymd == d].sort_values("rank").code.tolist(); b = right[right.signal_ymd == d].sort_values("baseline_rank").code.tolist()
        for k in (5, 10):
            sa, sb = set(a[:k]), set(b[:k]); union = sa | sb
            rows.append({"signal_ymd": d, "k": k, "changed_members": len(sa ^ sb), "changed_rank_count": sum(x != y for x, y in zip(a[:k], b[:k])), "jaccard": len(sa & sb) / len(union) if union else 1.0})
    out = {}
    for k in (5, 10):
        p = pd.DataFrame([r for r in rows if r["k"] == k])
        out[f"top{k}"] = {"common_days": int(len(p)), "changed_members_count": int(p.changed_members.sum()) if len(p) else 0,
                           "changed_rank_count": int(p.changed_rank_count.sum()) if len(p) else 0,
                           "mean_jaccard": float(p.jaccard.mean()) if len(p) else None,
                           "changed_day_rate": float((p.changed_members > 0).mean()) if len(p) else None}
    return out


def build_miss_audit(scored: pd.DataFrame, threshold: float) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for code, part in scored.sort_values("signal_ymd").groupby("code", sort=False):
        part = part.reset_index(drop=True); movers = part.index[part.realized_mover20.eq(1)].tolist()
        for anchor_i in movers:
            anchor = part.iloc[anchor_i]; prior = part.iloc[max(0, anchor_i - 60):anchor_i]
            detected = prior[prior.score >= threshold]
            first = detected.iloc[0] if len(detected) else None
            if first is not None: reason = "detected_early"
            elif len(prior) < 60: reason = "insufficient_60_session_history"
            elif prior.score.max() >= threshold: reason = "rank_suppression"
            elif prior[prior.top10].shape[0]: reason = "top10_without_threshold"
            else: reason = "chart_signal_absent_under_frozen_model"
            base = {"mover_ymd": int(anchor.signal_ymd), "code": str(code), "side": anchor.side,
                    "first_detect_ymd": int(first.signal_ymd) if first is not None else None,
                    "first_detect_lead_sessions": int(anchor_i - first.name) if first is not None else None,
                    "miss_reason": reason, "train_fixed_threshold": threshold}
            for lag in CHECKPOINTS:
                row = part.iloc[anchor_i - lag] if anchor_i >= lag else None
                base.update({f"d{lag}_ymd": int(row.signal_ymd) if row is not None else None,
                             f"d{lag}_score": float(row.score) if row is not None else None,
                             f"d{lag}_rank": int(row["rank"]) if row is not None else None,
                             f"d{lag}_percentile": float(row.percentile) if row is not None else None,
                             f"d{lag}_top10": bool(row.top10) if row is not None else None})
            rows.append(base)
    return pd.DataFrame(rows)


def generate(db_path: Path, out_root: Path) -> Path:
    if not db_path.exists(): raise FileNotFoundError(db_path)
    features, bars, source = load_source(db_path); rankings = source.pop("rankings")
    scored_parts, models, thresholds = [], {}, {}
    for side in ("BUY", "SELL"):
        frame = attach_outcomes(attach_past_features(features, side), bars, side)
        scored, model, threshold = fit_and_score(frame)
        scored_parts.append(scored); models[side] = model; thresholds[side] = threshold
    scored = pd.concat(scored_parts, ignore_index=True)
    scored["split"] = np.select([scored.signal_ymd < 20250101, scored.signal_ymd < 20260101], ["train", "validation"], default="shadow")
    model_top = scored[scored.top10 & scored.trade_return_h10.notna()].copy()
    baseline = rankings.copy(); baseline["side"] = baseline.dir.map({"up": "BUY", "down": "SELL"}); baseline = baseline.dropna(subset=["side"])
    baseline = baseline.rename(columns={"rank": "baseline_rank"}).merge(scored[["signal_ymd", "code", "side", "trade_return_h10", "target_before_stop20"]], on=["signal_ymd", "code", "side"], how="left", validate="many_to_one")
    baseline = baseline[baseline.trade_return_h10.notna()].copy()
    calendar = sorted(features.signal_ymd.unique().tolist())
    periods = {"train": (20240101, 20241231), "validation": (20250101, 20251231), "shadow": (20260101, int(features.signal_ymd.max()))}
    metrics, branch, coverage = {}, {}, {}
    for split, (start, end) in periods.items():
        days = sum(start <= d <= end for d in calendar); metrics[split] = {}; branch[split] = {}
        for side in ("BUY", "SELL"):
            m = model_top[(model_top.side == side) & model_top.signal_ymd.between(start, end)]
            b = baseline[(baseline.side == side) & baseline.signal_ymd.between(start, end)]
            eligible = scored[(scored.side == side) & scored.signal_ymd.between(start, end) & scored.target_before_stop20.notna()]
            movers = eligible[eligible.realized_mover20.eq(1)]
            metrics[split][side] = {"model_top10": _metrics(m, days), "meemee_top10": _metrics(b, days),
                                    "recall": float(m.realized_mover20.sum() / movers.realized_mover20.sum()) if len(movers) else None,
                                    "mover_count": int(len(movers))}
            branch[split][side] = _branching(model_top, baseline, side, start, end)
            counts = scored[(scored.side == side) & scored.signal_ymd.between(start, end)].groupby("signal_ymd").size()
            coverage[f"{split}_{side}"] = {"days": int(len(counts)), "min_ranked": int(counts.min()) if len(counts) else 0,
                                                   "median_ranked": float(counts.median()) if len(counts) else 0, "all_days_ranked": bool((counts > 0).all())}
    vg = {}
    for side in ("BUY", "SELL"):
        v, b = metrics["validation"][side]["model_top10"], metrics["validation"][side]["meemee_top10"]
        br = branch["validation"][side]["top10"]
        vg[side] = {"pf_ge_1_30": (v["profit_factor"] or 0) >= 1.30, "pf_delta_ge_0_10": v["profit_factor"] is not None and b["profit_factor"] is not None and v["profit_factor"] - b["profit_factor"] >= .10,
                    "calendar_expectancy_improves": v["calendar_expectancy"] is not None and b["calendar_expectancy"] is not None and v["calendar_expectancy"] > b["calendar_expectancy"],
                    "cvar_non_degrade": v["cvar10"] is not None and b["cvar10"] is not None and v["cvar10"] >= b["cvar10"],
                    "drawdown_non_degrade": v["max_drawdown"] is not None and b["max_drawdown"] is not None and v["max_drawdown"] >= b["max_drawdown"],
                    "frequency_ge_weekly_one": v["signals_per_week"] >= 1, "branch_ge_20pct": (br["changed_day_rate"] or 0) >= .20}
    decision = {side: "keep" if all(vg[side].values()) else "drop" if not vg[side]["pf_ge_1_30"] else "hold" for side in vg}
    root = out_root / f"{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}-{AXIS_ID}"; root.mkdir(parents=True, exist_ok=False)
    scored[["signal_ymd", "code", "side", "score", "rank", "percentile", "top10", "split", "target_before_stop20", "realized_mover20", "trade_return_h10"]].to_parquet(root / "all_symbol_daily_scores.parquet", index=False)
    miss = pd.concat([build_miss_audit(scored[scored.side == side], thresholds[side]) for side in ("BUY", "SELL")], ignore_index=True)
    miss.to_parquet(root / "miss_audit.parquet", index=False); model_top.to_parquet(root / "model_top10_events.parquet", index=False); baseline.to_parquet(root / "meemee_top10_events.parquet", index=False)
    _write_json(root / "frozen_models.json", models)
    payload = {"schema_version": f"{AXIS_ID}.compare.v1", "artifact_role": "authoritative", "axis_id": AXIS_ID,
               "research_phase": "effectiveness_judgment", "fixed_evaluation_conditions": {"source": "ml_feature_daily plus PAN daily_bars", "universe": "all feature-covered current PAN symbols every session", "features": FEATURES,
               "direction_mapping": "BUY uses up probabilities; SELL uses corresponding down probabilities and side-aligned returns", "derived_features": "same-day MA20/60 spread plus exact 5-session lag deltas for high20/low20 blocks",
               "imputation": "2024 side-specific train medians frozen", "model": "side-specific DecisionTreeClassifier max_depth3 min_samples_leaf200 random_state0", "label": "next-open, within20 direction MFE>=8 and no prior -5 adverse touch; stop-first and same-bar both=loss",
               "trade_evaluation": "next-open TP8/SL5/H10/10bp", "selection": "all symbols ranked daily; direction-specific top10", "first_detect": "earliest prior60 daily score crossing 2024-only fixed top10-equivalent threshold", "splits": periods,
               "shadow_tuning": False, "fallback": False}, "source_artifacts": [{"path": str(db_path), "sha256": _sha(db_path)}], "source_coverage": source["coverage"], "rank_coverage": coverage,
               "frozen_models": models, "metrics": metrics, "branching": branch, "miss_audit": {"rows": int(len(miss)), "reason_counts": miss.miss_reason.value_counts().to_dict(), "checkpoints": list(CHECKPOINTS)},
               "validation_keep_gates": vg, "decision": {"candidate_local_decision": decision, "authoritative_rollup_decision": "review_only", "reason_type": "fixed_2024_all_symbol_direction_probability_validation"},
               "shadow_tuning_used": False, "silent_fallback_used": False, "runtime_db_write": False, "production_ranking_changed": False, "meemee_changed": False}
    path = root / "compare.json"; _write_json(path, payload); _write_json(root / "_ARTIFACT_COMPLETE.json", {"axis_id": AXIS_ID, "compare": str(path), "complete": True}); return path


def main() -> None:
    ap = argparse.ArgumentParser(); ap.add_argument("--db", type=Path, required=True); ap.add_argument("--out", type=Path, default=DEFAULT_OUT); args = ap.parse_args(); print(generate(args.db, args.out))


if __name__ == "__main__": main()
