#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

import duckdb
import numpy as np
import pandas as pd

HORIZONS = (3, 5, 7, 10, 15, 20)
RULE_PDOWN_GRID = (0.52, 0.55, 0.58, 0.60, 0.62)
RULE_PTURN_GRID = (0.50, 0.55, 0.58, 0.60, 0.62)


@dataclass(frozen=True)
class RuleMetric:
    name: str
    n: int
    hit10: float
    lift: float
    avg_ret20: float
    win20: float


def _default_db_path() -> Path:
    local_app = os.environ.get("LOCALAPPDATA", "")
    if not local_app:
        raise RuntimeError("LOCALAPPDATA is not set")
    return Path(local_app) / "MeeMeeScreener" / "data" / "stocks.duckdb"


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run long-horizon short-selling research and export CSVs.")
    parser.add_argument("--db-path", default="", help="DuckDB path. Default: LOCALAPPDATA/MeeMeeScreener/data/stocks.duckdb")
    parser.add_argument("--out-dir", default="tmp", help="Output directory for CSV files")
    parser.add_argument("--start-dt", type=int, default=0, help="Inclusive lower bound dt (unix or yyyymmdd int)")
    parser.add_argument("--end-dt", type=int, default=0, help="Inclusive upper bound dt (unix or yyyymmdd int)")
    parser.add_argument("--min-sample", type=int, default=120, help="Minimum sample count for rule outputs")
    parser.add_argument("--walkforward-train-months", type=int, default=18, help="Train window size (months)")
    return parser.parse_args()


def _build_panel_query(start_dt: int, end_dt: int) -> str:
    where_clauses: list[str] = ["f.ma60 IS NOT NULL", "bars.c > 0"]
    bars_where: list[str] = ["b.c > 0"]
    if start_dt > 0:
        where_clauses.append(f"bars.dt >= {int(start_dt)}")
        bars_where.append(f"b.date >= {int(start_dt)}")
    if end_dt > 0:
        where_clauses.append(f"bars.dt <= {int(end_dt)}")
        bars_where.append(f"b.date <= {int(end_dt) + max(HORIZONS)}")
    where_sql = " AND ".join(where_clauses)
    bars_where_sql = " AND ".join(bars_where)

    lead_cols = ",\n            ".join([f"LEAD(b.c, {h}) OVER w AS c_fwd{h}" for h in HORIZONS])
    min_cols = ",\n            ".join(
        [
            (
                f"MIN(b.l) OVER (PARTITION BY b.code ORDER BY b.date "
                f"ROWS BETWEEN 1 FOLLOWING AND {h} FOLLOWING) AS min_l_fwd{h}"
            )
            for h in HORIZONS
        ]
    )
    max_cols = ",\n            ".join(
        [
            (
                f"MAX(b.h) OVER (PARTITION BY b.code ORDER BY b.date "
                f"ROWS BETWEEN 1 FOLLOWING AND {h} FOLLOWING) AS max_h_fwd{h}"
            )
            for h in HORIZONS
        ]
    )

    return f"""
WITH bars AS (
    SELECT
        b.code,
        b.date AS dt,
        b.o,
        b.h,
        b.l,
        b.c,
        {lead_cols},
        {min_cols},
        {max_cols}
    FROM daily_bars b
    WHERE {bars_where_sql}
    WINDOW w AS (PARTITION BY b.code ORDER BY b.date)
),
joined AS (
    SELECT
        bars.dt,
        bars.code,
        bars.o AS open,
        bars.h AS high,
        bars.l AS low,
        bars.c AS close,
        bars.c_fwd3,
        bars.c_fwd5,
        bars.c_fwd7,
        bars.c_fwd10,
        bars.c_fwd15,
        bars.c_fwd20,
        bars.min_l_fwd3,
        bars.min_l_fwd5,
        bars.min_l_fwd7,
        bars.min_l_fwd10,
        bars.min_l_fwd15,
        bars.min_l_fwd20,
        bars.max_h_fwd3,
        bars.max_h_fwd5,
        bars.max_h_fwd7,
        bars.max_h_fwd10,
        bars.max_h_fwd15,
        bars.max_h_fwd20,
        f.ma20,
        f.ma60,
        f.close_prev1,
        f.ma20_prev1,
        f.close_ret20 AS mom20,
        f.atr14_pct,
        f.vol_ratio5_20,
        f.breakout20_up,
        f.breakout20_down,
        f.high20_dist,
        f.low20_dist,
        f.drawdown60,
        f.rebound60,
        p.p_down,
        p.p_turn_down,
        p.rank_down_20,
        p.p_up,
        p.p_turn_up
    FROM bars
    INNER JOIN ml_feature_daily f
        ON f.code = bars.code
       AND f.dt = bars.dt
    LEFT JOIN ml_pred_20d p
        ON p.code = bars.code
       AND p.dt = bars.dt
    WHERE {where_sql}
)
SELECT * FROM joined
ORDER BY code, dt
"""


def _parse_dt_series(dt_series: pd.Series) -> pd.Series:
    dt_num = pd.to_numeric(dt_series, errors="coerce")
    ts_mask = dt_num >= 1_000_000_000
    out = pd.Series(pd.NaT, index=dt_series.index, dtype="datetime64[ns]")
    if ts_mask.any():
        out.loc[ts_mask] = pd.to_datetime(dt_num.loc[ts_mask], unit="s", errors="coerce")
    if (~ts_mask).any():
        out.loc[~ts_mask] = pd.to_datetime(dt_num.loc[~ts_mask].astype("Int64").astype(str), format="%Y%m%d", errors="coerce")
    return out


def _load_panel(db_path: Path, start_dt: int, end_dt: int) -> pd.DataFrame:
    query = _build_panel_query(start_dt=start_dt, end_dt=end_dt)
    con = duckdb.connect(str(db_path), read_only=True)
    try:
        df = con.execute(query).df()
    finally:
        con.close()

    if df.empty:
        raise RuntimeError("analysis panel is empty")

    df = df.sort_values(["code", "dt"]).reset_index(drop=True)
    df["ext_ma20"] = (df["close"] / df["ma20"]) - 1.0
    df["above60"] = df["close"] > df["ma60"]
    first_in_code = df.groupby("code", sort=False).cumcount().eq(0)
    above60_prev = df.groupby("code", sort=False)["above60"].shift(1)
    group_switch = (df["above60"] != above60_prev) | first_in_code
    streak_group = group_switch.groupby(df["code"], sort=False).cumsum()
    cnt = df.groupby(["code", streak_group], sort=False).cumcount() + 1
    df["cnt_60_above"] = np.where(df["above60"], cnt, 0).astype("int32")
    df["ma20_breakdown"] = (df["close"] < df["ma20"]) & (df["close_prev1"] >= df["ma20_prev1"])

    for h in HORIZONS:
        df[f"min_ret{h}"] = (df[f"min_l_fwd{h}"] / df["close"]) - 1.0
        df[f"max_rise{h}"] = (df[f"max_h_fwd{h}"] / df["close"]) - 1.0
        df[f"close_ret{h}"] = (df[f"c_fwd{h}"] / df["close"]) - 1.0
        df[f"short_ret{h}"] = -df[f"close_ret{h}"]

    df["hit10_20d"] = df["min_ret20"] <= -0.10
    df["hit5_10d"] = df["min_ret10"] <= -0.05
    df["stop5_5d"] = df["max_rise5"] >= 0.05
    df["stop5_20d"] = df["max_rise20"] >= 0.05
    df["dt_date"] = _parse_dt_series(df["dt"])
    return df


def _cnt60_bin(series: pd.Series) -> pd.Series:
    values = pd.to_numeric(series, errors="coerce")
    bins = pd.Series("na", index=series.index, dtype="object")
    valid = values.notna()
    capped = values.clip(lower=0, upper=60)
    bins.loc[valid & (capped <= 9)] = "0-9"
    bins.loc[valid & (capped >= 10) & (capped <= 19)] = "10-19"
    bins.loc[valid & (capped >= 20) & (capped <= 29)] = "20-29"
    bins.loc[valid & (capped >= 30) & (capped <= 39)] = "30-39"
    bins.loc[valid & (capped >= 40) & (capped <= 49)] = "40-49"
    bins.loc[valid & (capped >= 50) & (capped <= 54)] = "50-54"
    bins.loc[valid & (capped >= 55) & (capped <= 59)] = "55-59"
    bins.loc[valid & (capped >= 60)] = "60"
    return bins


def _safe_mean(series: pd.Series) -> float:
    if series.empty:
        return float("nan")
    return float(pd.to_numeric(series, errors="coerce").mean())


def _rule_row(name: str, frame: pd.DataFrame, baseline_hit: float) -> RuleMetric:
    hit10 = float(frame["hit10_20d"].mean())
    avg_ret20 = float(frame["short_ret20"].mean())
    win20 = float((frame["short_ret20"] > 0).mean())
    lift = float(hit10 / baseline_hit) if baseline_hit > 0 else float("nan")
    return RuleMetric(name=name, n=int(len(frame)), hit10=hit10, lift=lift, avg_ret20=avg_ret20, win20=win20)


def _write_reversal_outputs(df: pd.DataFrame, pred_df: pd.DataFrame, out_dir: Path) -> list[Path]:
    outputs: list[Path] = []
    valid = df.dropna(subset=["min_ret5", "min_ret10", "min_ret20", "close_ret5", "close_ret20"]).copy()

    exact = (
        valid.groupby("cnt_60_above", as_index=False)
        .agg(
            n=("code", "size"),
            rev5_drop3=("min_ret5", lambda s: (s <= -0.03).mean()),
            rev10_drop5=("min_ret10", lambda s: (s <= -0.05).mean()),
            down20_ge10=("min_ret20", lambda s: (s <= -0.10).mean()),
            avg_ret5=("close_ret5", "mean"),
            avg_ret20=("close_ret20", "mean"),
        )
        .sort_values("cnt_60_above")
        .reset_index(drop=True)
    )
    p = out_dir / "reversal_by_cnt60_exact.csv"
    exact.to_csv(p, index=False)
    outputs.append(p)

    binned = valid.copy()
    binned["cnt60_bin"] = _cnt60_bin(binned["cnt_60_above"])
    by_bin = (
        binned.groupby("cnt60_bin", as_index=False)
        .agg(
            n=("code", "size"),
            rev5_drop3=("min_ret5", lambda s: (s <= -0.03).mean()),
            rev10_drop5=("min_ret10", lambda s: (s <= -0.05).mean()),
            down20_ge10=("min_ret20", lambda s: (s <= -0.10).mean()),
            avg_ret5=("close_ret5", "mean"),
            avg_ret20=("close_ret20", "mean"),
        )
        .sort_values("cnt60_bin")
        .reset_index(drop=True)
    )
    p = out_dir / "reversal_by_cnt60_bins.csv"
    by_bin.to_csv(p, index=False)
    outputs.append(p)

    ctx = valid[
        (valid["close"] > valid["ma60"])
        & (valid["mom20"] >= 0.10)
        & (valid["ext_ma20"] >= 0.03)
    ].copy()
    ctx_out = (
        ctx.groupby("cnt_60_above", as_index=False)
        .agg(
            n=("code", "size"),
            rev5_drop3=("min_ret5", lambda s: (s <= -0.03).mean()),
            rev10_drop5=("min_ret10", lambda s: (s <= -0.05).mean()),
            down20_ge10=("min_ret20", lambda s: (s <= -0.10).mean()),
            avg_ret20=("close_ret20", "mean"),
        )
        .sort_values("cnt_60_above")
        .reset_index(drop=True)
    )
    p = out_dir / "reversal_cnt60_uptrend_context.csv"
    ctx_out.to_csv(p, index=False)
    outputs.append(p)

    pred_work = pred_df.copy()
    pred_work["cnt60_bin"] = _cnt60_bin(pred_work["cnt_60_above"])
    baseline_hit = float(pred_work["hit10_20d"].mean())
    hit_by_bin = (
        pred_work.groupby("cnt60_bin", as_index=False)
        .agg(
            n=("code", "size"),
            hit10_20d=("hit10_20d", "mean"),
            hit5_10d=("hit5_10d", "mean"),
            avg_ret20=("short_ret20", "mean"),
            win20=("short_ret20", lambda s: (s > 0).mean()),
        )
        .sort_values("cnt60_bin")
        .reset_index(drop=True)
    )
    hit_by_bin["lift_vs_baseline"] = hit_by_bin["hit10_20d"] / baseline_hit if baseline_hit > 0 else np.nan
    p = out_dir / "short_hit_by_cnt60_bin.csv"
    hit_by_bin.to_csv(p, index=False)
    outputs.append(p)
    return outputs


def _write_rule_search_outputs(pred_df: pd.DataFrame, out_dir: Path, min_sample: int) -> list[Path]:
    outputs: list[Path] = []
    baseline_hit = float(pred_df["hit10_20d"].mean())
    rows: list[RuleMetric] = []

    for p_down_th in RULE_PDOWN_GRID:
        for p_turn_th in RULE_PTURN_GRID:
            base_mask = (pred_df["p_down"] >= p_down_th) & (pred_df["p_turn_down"] >= p_turn_th)
            base_subset = pred_df[base_mask]
            if len(base_subset) >= min_sample:
                rows.append(
                    _rule_row(
                        f"pdown>={p_down_th:.2f} & pturndown>={p_turn_th:.2f}",
                        frame=base_subset,
                        baseline_hit=baseline_hit,
                    )
                )

            for cnt_th in (40, 50, 60):
                subset = pred_df[base_mask & (pred_df["cnt_60_above"] >= cnt_th)]
                if len(subset) < min_sample:
                    continue
                rows.append(
                    _rule_row(
                        (
                            f"cnt60>={cnt_th} & pdown>={p_down_th:.2f} "
                            f"& pturndown>={p_turn_th:.2f}"
                        ),
                        frame=subset,
                        baseline_hit=baseline_hit,
                    )
                )

            subset_ext = pred_df[base_mask & (pred_df["ext_ma20"] >= 0.03) & (pred_df["cnt_60_above"] >= 30)]
            if len(subset_ext) >= min_sample:
                rows.append(
                    _rule_row(
                        (
                            f"ext_ma20>=0.03 & cnt60>=30 & pdown>={p_down_th:.2f} "
                            f"& pturndown>={p_turn_th:.2f}"
                        ),
                        frame=subset_ext,
                        baseline_hit=baseline_hit,
                    )
                )

            subset_break = pred_df[base_mask & (pred_df["breakout20_down"] == 1)]
            if len(subset_break) >= min_sample:
                rows.append(
                    _rule_row(
                        (
                            f"breakout20_down=1 & pdown>={p_down_th:.2f} "
                            f"& pturndown>={p_turn_th:.2f}"
                        ),
                        frame=subset_break,
                        baseline_hit=baseline_hit,
                    )
                )

    all_df = pd.DataFrame([r.__dict__ for r in rows]).drop_duplicates(subset=["name"])
    all_df = all_df.rename(columns={"name": "rule", "hit10": "hit10"})
    all_df = all_df.sort_values(["hit10", "n"], ascending=[False, False]).reset_index(drop=True)
    p = out_dir / "short_rule_search_all.csv"
    all_df.to_csv(p, index=False)
    outputs.append(p)

    practical = all_df[
        (all_df["n"] >= (min_sample * 2))
        & (all_df["hit10"] >= (baseline_hit * 1.5))
        & (all_df["win20"] >= 0.55)
    ].copy()
    practical = practical.sort_values(["lift", "hit10", "avg_ret20"], ascending=[False, False, False]).reset_index(drop=True)
    p = out_dir / "short_rule_search_practical.csv"
    practical.to_csv(p, index=False)
    outputs.append(p)
    return outputs


def _pattern_defs(pred_df: pd.DataFrame) -> dict[str, pd.Series]:
    r1 = (pred_df["p_down"] >= 0.58) & (pred_df["p_turn_down"] >= 0.60)
    return {
        "R1_model_consensus": r1,
        "R2_consensus_plus_extension": r1 & (pred_df["ext_ma20"] >= 0.05) & (pred_df["cnt_60_above"] >= 40),
        "R3_countertrend_upper_band": (pred_df["cnt_60_above"] >= 50) & (pred_df["ext_ma20"] >= 0.03) & (pred_df["p_down"] >= 0.58),
        "R4_breakdown_followthrough": (pred_df["breakout20_down"] == 1) & (pred_df["p_turn_down"] >= 0.55),
        "R5_break_strict": (pred_df["breakout20_down"] == 1) & (pred_df["p_down"] >= 0.62) & (pred_df["p_turn_down"] >= 0.62),
        "R6_overheat_then_turn": (pred_df["ext_ma20"] >= 0.08) & (pred_df["p_turn_down"] >= 0.55) & (pred_df["cnt_60_above"] >= 20),
    }


def _write_pattern_outputs(pred_df: pd.DataFrame, out_dir: Path, min_sample: int) -> list[Path]:
    outputs: list[Path] = []
    baseline_hit = float(pred_df["hit10_20d"].mean())
    pattern_masks = _pattern_defs(pred_df)

    rows = []
    for name, mask in pattern_masks.items():
        subset = pred_df[mask]
        if len(subset) < max(30, min_sample // 2):
            continue
        rows.append(
            {
                "pattern": name,
                "n": int(len(subset)),
                "hit10_20d": float(subset["hit10_20d"].mean()),
                "hit5_10d": float(subset["hit5_10d"].mean()),
                "avg_ret20": float(subset["short_ret20"].mean()),
                "win20": float((subset["short_ret20"] > 0).mean()),
                "p95_ret20": float(subset["short_ret20"].quantile(0.95)),
                "p05_ret20": float(subset["short_ret20"].quantile(0.05)),
                "lift_vs_baseline": float(subset["hit10_20d"].mean() / baseline_hit) if baseline_hit > 0 else np.nan,
            }
        )
    curated = pd.DataFrame(rows).sort_values(["lift_vs_baseline", "hit10_20d"], ascending=[False, False]).reset_index(drop=True)
    p = out_dir / "short_patterns_curated.csv"
    curated.to_csv(p, index=False)
    outputs.append(p)

    rr_rows = [
        {
            "pattern": "baseline",
            "n": int(len(pred_df)),
            "hit10_20d": float(pred_df["hit10_20d"].mean()),
            "hit5_10d": float(pred_df["hit5_10d"].mean()),
            "avg_ret20": float(pred_df["short_ret20"].mean()),
            "win20": float((pred_df["short_ret20"] > 0).mean()),
            "adverse5_high_ge_5pct": float((pred_df["max_rise5"] >= 0.05).mean()),
            "adverse10_high_ge_5pct": float((pred_df["max_rise10"] >= 0.05).mean()),
            "adverse20_high_ge_5pct": float((pred_df["max_rise20"] >= 0.05).mean()),
        }
    ]
    for name, mask in pattern_masks.items():
        subset = pred_df[mask]
        if len(subset) < max(30, min_sample // 2):
            continue
        rr_rows.append(
            {
                "pattern": name,
                "n": int(len(subset)),
                "hit10_20d": float(subset["hit10_20d"].mean()),
                "hit5_10d": float(subset["hit5_10d"].mean()),
                "avg_ret20": float(subset["short_ret20"].mean()),
                "win20": float((subset["short_ret20"] > 0).mean()),
                "adverse5_high_ge_5pct": float((subset["max_rise5"] >= 0.05).mean()),
                "adverse10_high_ge_5pct": float((subset["max_rise10"] >= 0.05).mean()),
                "adverse20_high_ge_5pct": float((subset["max_rise20"] >= 0.05).mean()),
            }
        )

    risk = pd.DataFrame(rr_rows).sort_values(["pattern"]).reset_index(drop=True)
    p = out_dir / "short_patterns_risk_reward.csv"
    risk.to_csv(p, index=False)
    outputs.append(p)
    return outputs


def _write_regime_calibration(pred_df: pd.DataFrame, out_dir: Path, min_sample: int) -> list[Path]:
    outputs: list[Path] = []
    work = pred_df.copy()
    work["regime"] = np.select(
        [
            work["breakout20_down"] == 1,
            (work["cnt_60_above"] >= 50) & (work["ext_ma20"] >= 0.03),
            work["cnt_60_above"] >= 50,
            work["close"] < work["ma20"],
        ],
        ["breakdown_follow", "countertrend_upper", "extended_uptrend", "below_ma20"],
        default="other",
    )

    bins = [0.0, 0.40, 0.50, 0.55, 0.60, 0.65, 0.70, 0.80, 1.01]
    labels = ["0.00-0.40", "0.40-0.50", "0.50-0.55", "0.55-0.60", "0.60-0.65", "0.65-0.70", "0.70-0.80", "0.80+"]
    work["pdown_bin"] = pd.cut(work["p_down"], bins=bins, labels=labels, include_lowest=True, right=False)
    grouped = (
        work.dropna(subset=["pdown_bin"])
        .groupby(["regime", "pdown_bin"], as_index=False, observed=False)
        .agg(
            n=("code", "size"),
            pred_mean=("p_down", "mean"),
            actual_hit10=("hit10_20d", "mean"),
            avg_ret20=("short_ret20", "mean"),
        )
    )
    grouped = grouped[grouped["n"] >= max(20, min_sample // 4)].copy()
    grouped["calibration_gap"] = grouped["actual_hit10"] - grouped["pred_mean"]
    grouped = grouped.sort_values(["regime", "pdown_bin"]).reset_index(drop=True)
    p = out_dir / "short_regime_calibration.csv"
    grouped.to_csv(p, index=False)
    outputs.append(p)

    reg_rows = []
    for regime, sub in work.groupby("regime"):
        if len(sub) < min_sample:
            continue
        binned = grouped[grouped["regime"] == regime]
        if binned.empty:
            continue
        ece = float((binned["calibration_gap"].abs() * binned["n"]).sum() / binned["n"].sum())
        brier = float(((sub["hit10_20d"].astype(float) - sub["p_down"]) ** 2).mean())
        reg_rows.append(
            {
                "regime": regime,
                "n": int(len(sub)),
                "ece_abs_gap": ece,
                "brier_score": brier,
                "hit10_20d": float(sub["hit10_20d"].mean()),
                "avg_ret20": float(sub["short_ret20"].mean()),
            }
        )
    reg_df = pd.DataFrame(reg_rows).sort_values(["ece_abs_gap", "n"], ascending=[True, False]).reset_index(drop=True)
    p = out_dir / "short_regime_calibration_summary.csv"
    reg_df.to_csv(p, index=False)
    outputs.append(p)
    return outputs


def _first_trigger_index(flags: np.ndarray, start: int, window: int) -> int:
    end = min(start + window + 1, len(flags))
    if start + 1 >= end:
        return -1
    rel = np.flatnonzero(flags[start + 1 : end])
    if rel.size == 0:
        return -1
    return int(start + 1 + rel[0])


def _strategy_metrics(records: Iterable[dict[str, float]]) -> dict[str, float]:
    frame = pd.DataFrame(list(records))
    if frame.empty:
        return {
            "n": 0,
            "add_rate": 0.0,
            "hit10_20d": np.nan,
            "avg_ret20": np.nan,
            "median_ret20": np.nan,
            "stop5_rate20": np.nan,
            "win20": np.nan,
        }
    return {
        "n": int(len(frame)),
        "add_rate": float(frame["is_added"].mean()),
        "hit10_20d": float(frame["hit10"].mean()),
        "avg_ret20": float(frame["ret20"].mean()),
        "median_ret20": float(frame["ret20"].median()),
        "stop5_rate20": float(frame["stop5"].mean()),
        "win20": float((frame["ret20"] > 0).mean()),
    }


def _write_two_stage_entry(pred_df: pd.DataFrame, out_dir: Path) -> list[Path]:
    outputs: list[Path] = []
    stage1_mask = (
        (pred_df["cnt_60_above"] >= 40)
        & (pred_df["ext_ma20"] >= 0.03)
        & (pred_df["p_down"] >= 0.55)
        & (pred_df["p_turn_down"] >= 0.55)
    )
    trigger = (pred_df["breakout20_down"] == 1) | pred_df["ma20_breakdown"]

    rows_stage1: list[dict[str, float]] = []
    rows_5: list[dict[str, float]] = []
    rows_10: list[dict[str, float]] = []

    work = pred_df[["code", "dt", "short_ret20", "min_ret20", "max_rise20"]].copy()
    work["stage1"] = stage1_mask.values
    work["trigger"] = trigger.values
    work = work.sort_values(["code", "dt"]).reset_index(drop=True)

    for _, g in work.groupby("code", sort=False):
        stage1_idx = np.flatnonzero(g["stage1"].to_numpy())
        if stage1_idx.size == 0:
            continue
        trig_flags = g["trigger"].to_numpy(dtype=bool)
        ret20 = g["short_ret20"].to_numpy(dtype=float)
        min_ret20 = g["min_ret20"].to_numpy(dtype=float)
        max_rise20 = g["max_rise20"].to_numpy(dtype=float)

        for i in stage1_idx:
            base_rec = {
                "ret20": float(ret20[i]),
                "hit10": float((-min_ret20[i]) >= 0.10),
                "stop5": float(max_rise20[i] >= 0.05),
                "is_added": 0.0,
            }
            rows_stage1.append(base_rec)

            for window, target_rows in ((5, rows_5), (10, rows_10)):
                j = _first_trigger_index(trig_flags, start=int(i), window=window)
                if j < 0:
                    target_rows.append(base_rec.copy())
                    continue
                rec = {
                    "ret20": float((ret20[i] + ret20[j]) / 2.0),
                    "hit10": float(((-min_ret20[i]) + (-min_ret20[j])) / 2.0 >= 0.10),
                    "stop5": float((max_rise20[i] + max_rise20[j]) / 2.0 >= 0.05),
                    "is_added": 1.0,
                }
                target_rows.append(rec)

    out = pd.DataFrame(
        [
            {"strategy": "stage1_only", **_strategy_metrics(rows_stage1)},
            {"strategy": "stage1_plus_break_5d", **_strategy_metrics(rows_5)},
            {"strategy": "stage1_plus_break_10d", **_strategy_metrics(rows_10)},
        ]
    )
    p = out_dir / "short_two_stage_entry.csv"
    out.to_csv(p, index=False)
    outputs.append(p)
    return outputs


def _write_failure_forensics(pred_df: pd.DataFrame, out_dir: Path) -> list[Path]:
    outputs: list[Path] = []
    r1 = pred_df[(pred_df["p_down"] >= 0.58) & (pred_df["p_turn_down"] >= 0.60)].copy()
    if r1.empty:
        raise RuntimeError("R1_model_consensus sample is empty; cannot run failure forensics")

    r1["failed"] = ~r1["hit10_20d"]
    fail = r1[r1["failed"]].copy()
    fail["reason"] = np.select(
        [
            fail["max_rise5"] >= 0.05,
            fail["close_ret20"] > 0,
            fail["min_ret10"] > -0.03,
            fail["ext_ma20"] < 0.02,
            fail["p_turn_down"] < 0.65,
        ],
        [
            "stop_hit_5pct",
            "trend_continued_up",
            "no_follow_through",
            "weak_extension",
            "turn_prob_not_high_enough",
        ],
        default="other",
    )
    summary = (
        fail.groupby("reason", as_index=False)
        .agg(
            n=("code", "size"),
            share_in_failures=("code", lambda s: len(s) / len(fail)),
            avg_ret20=("short_ret20", "mean"),
            avg_max_rise5=("max_rise5", "mean"),
            avg_min_ret10=("min_ret10", "mean"),
        )
        .sort_values(["n", "reason"], ascending=[False, True])
        .reset_index(drop=True)
    )
    p = out_dir / "short_failure_forensics.csv"
    summary.to_csv(p, index=False)
    outputs.append(p)

    cols = ["cnt_60_above", "ext_ma20", "p_down", "p_turn_down", "max_rise5", "min_ret10", "short_ret20"]
    delta_rows = []
    success = r1[~r1["failed"]]
    for col in cols:
        delta_rows.append(
            {
                "feature": col,
                "avg_success": _safe_mean(success[col]),
                "avg_failure": _safe_mean(fail[col]),
                "success_minus_failure": _safe_mean(success[col]) - _safe_mean(fail[col]),
            }
        )
    delta = pd.DataFrame(delta_rows)
    p = out_dir / "short_failure_feature_delta.csv"
    delta.to_csv(p, index=False)
    outputs.append(p)
    return outputs


def _write_holding_hazard(pred_df: pd.DataFrame, out_dir: Path) -> list[Path]:
    outputs: list[Path] = []
    patterns = _pattern_defs(pred_df)
    patterns = {"baseline": pd.Series(True, index=pred_df.index, dtype=bool), **patterns}

    rows = []
    for name, mask in patterns.items():
        subset = pred_df[mask].copy()
        if subset.empty:
            continue
        for h in HORIZONS:
            subset_h = subset.dropna(subset=[f"min_ret{h}", f"max_rise{h}", f"short_ret{h}"])
            if subset_h.empty:
                continue
            rows.append(
                {
                    "pattern": name,
                    "horizon_days": int(h),
                    "n": int(len(subset_h)),
                    "hit10_rate": float((subset_h[f"min_ret{h}"] <= -0.10).mean()),
                    "hit5_rate": float((subset_h[f"min_ret{h}"] <= -0.05).mean()),
                    "stop5_rate": float((subset_h[f"max_rise{h}"] >= 0.05).mean()),
                    "avg_ret_close_based": float(subset_h[f"short_ret{h}"].mean()),
                }
            )
    hazard = pd.DataFrame(rows).sort_values(["pattern", "horizon_days"]).reset_index(drop=True)
    p = out_dir / "short_holding_hazard.csv"
    hazard.to_csv(p, index=False)
    outputs.append(p)
    return outputs


def _write_walkforward(pred_df: pd.DataFrame, out_dir: Path, min_sample: int, train_months: int) -> list[Path]:
    outputs: list[Path] = []
    work = pred_df.dropna(subset=["dt_date"]).copy()
    work["month"] = work["dt_date"].dt.to_period("M")
    months = sorted(work["month"].dropna().unique())
    if len(months) <= train_months:
        raise RuntimeError("insufficient monthly history for walkforward analysis")

    rows = []
    for idx in range(train_months, len(months)):
        train_set = set(months[idx - train_months : idx])
        test_month = months[idx]
        train_mask = work["month"].isin(train_set)
        test_mask = work["month"] == test_month
        if int(test_mask.sum()) == 0:
            continue

        best = None
        for p_down_th in RULE_PDOWN_GRID:
            for p_turn_th in RULE_PTURN_GRID:
                m = train_mask & (work["p_down"] >= p_down_th) & (work["p_turn_down"] >= p_turn_th)
                n_train = int(m.sum())
                if n_train < min_sample:
                    continue
                train_sub = work[m]
                hit = float(train_sub["hit10_20d"].mean())
                stop = float((train_sub["max_rise20"] >= 0.05).mean())
                ret = float(train_sub["short_ret20"].mean())
                score = hit - (0.6 * stop) + (0.2 * ret)
                if best is None or score > best["score"]:
                    best = {
                        "p_down_th": p_down_th,
                        "p_turn_th": p_turn_th,
                        "score": score,
                        "train_n": n_train,
                        "train_hit10": hit,
                        "train_stop5": stop,
                        "train_avg_ret20": ret,
                    }
        if best is None:
            continue

        test_rule = test_mask & (work["p_down"] >= best["p_down_th"]) & (work["p_turn_down"] >= best["p_turn_th"])
        test_n = int(test_rule.sum())
        if test_n <= 0:
            continue
        test_sub = work[test_rule]
        test_hit = float(test_sub["hit10_20d"].mean())
        test_stop = float((test_sub["max_rise20"] >= 0.05).mean())
        test_ret = float(test_sub["short_ret20"].mean())

        base_sub = work[test_mask]
        base_hit = float(base_sub["hit10_20d"].mean())
        base_ret = float(base_sub["short_ret20"].mean())
        base_stop = float((base_sub["max_rise20"] >= 0.05).mean())

        rows.append(
            {
                "test_month": str(test_month),
                "p_down_th": best["p_down_th"],
                "p_turn_th": best["p_turn_th"],
                "train_n": best["train_n"],
                "train_hit10": best["train_hit10"],
                "train_stop5": best["train_stop5"],
                "train_avg_ret20": best["train_avg_ret20"],
                "test_n": test_n,
                "test_hit10": test_hit,
                "test_stop5": test_stop,
                "test_avg_ret20": test_ret,
                "test_baseline_hit10": base_hit,
                "test_baseline_stop5": base_stop,
                "test_baseline_avg_ret20": base_ret,
                "test_hit10_lift": (test_hit / base_hit) if base_hit > 0 else np.nan,
            }
        )

    wf = pd.DataFrame(rows).sort_values("test_month").reset_index(drop=True)
    p = out_dir / "short_walkforward_thresholds.csv"
    wf.to_csv(p, index=False)
    outputs.append(p)

    if wf.empty:
        summary = pd.DataFrame(
            [
                {
                    "folds": 0,
                    "avg_test_hit10": np.nan,
                    "avg_test_stop5": np.nan,
                    "avg_test_ret20": np.nan,
                    "avg_baseline_hit10": np.nan,
                    "avg_baseline_ret20": np.nan,
                    "avg_hit10_lift": np.nan,
                }
            ]
        )
    else:
        summary = pd.DataFrame(
            [
                {
                    "folds": int(len(wf)),
                    "avg_test_hit10": float(wf["test_hit10"].mean()),
                    "avg_test_stop5": float(wf["test_stop5"].mean()),
                    "avg_test_ret20": float(wf["test_avg_ret20"].mean()),
                    "avg_baseline_hit10": float(wf["test_baseline_hit10"].mean()),
                    "avg_baseline_ret20": float(wf["test_baseline_avg_ret20"].mean()),
                    "avg_hit10_lift": float(wf["test_hit10_lift"].mean()),
                }
            ]
        )
    p = out_dir / "short_walkforward_thresholds_summary.csv"
    summary.to_csv(p, index=False)
    outputs.append(p)
    return outputs


def run(
    db_path: Path,
    out_dir: Path,
    start_dt: int,
    end_dt: int,
    min_sample: int,
    walkforward_train_months: int,
) -> list[Path]:
    out_dir.mkdir(parents=True, exist_ok=True)
    panel = _load_panel(db_path=db_path, start_dt=start_dt, end_dt=end_dt)
    panel_valid = panel.dropna(subset=["min_ret20", "min_ret10", "min_ret5", "short_ret20"]).copy()
    pred_df = panel_valid.dropna(subset=["p_down", "p_turn_down"]).copy()
    if pred_df.empty:
        raise RuntimeError("prediction-backed sample is empty (p_down/p_turn_down missing)")

    outputs: list[Path] = []
    outputs.extend(_write_reversal_outputs(panel_valid, pred_df=pred_df, out_dir=out_dir))
    outputs.extend(_write_rule_search_outputs(pred_df=pred_df, out_dir=out_dir, min_sample=min_sample))
    outputs.extend(_write_pattern_outputs(pred_df=pred_df, out_dir=out_dir, min_sample=min_sample))
    outputs.extend(_write_regime_calibration(pred_df=pred_df, out_dir=out_dir, min_sample=min_sample))
    outputs.extend(_write_two_stage_entry(pred_df=pred_df, out_dir=out_dir))
    outputs.extend(_write_failure_forensics(pred_df=pred_df, out_dir=out_dir))
    outputs.extend(_write_holding_hazard(pred_df=pred_df, out_dir=out_dir))
    outputs.extend(
        _write_walkforward(
            pred_df=pred_df,
            out_dir=out_dir,
            min_sample=min_sample,
            train_months=walkforward_train_months,
        )
    )
    return outputs


def main() -> int:
    args = _parse_args()
    db_path = Path(args.db_path).resolve() if args.db_path else _default_db_path()
    out_dir = Path(args.out_dir).resolve()
    outputs = run(
        db_path=db_path,
        out_dir=out_dir,
        start_dt=int(args.start_dt),
        end_dt=int(args.end_dt),
        min_sample=int(args.min_sample),
        walkforward_train_months=int(args.walkforward_train_months),
    )
    for path in outputs:
        print(f"wrote: {path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
