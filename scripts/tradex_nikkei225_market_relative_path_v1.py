from __future__ import annotations

"""PIT market-relative path features for the Nikkei-225 daily assessor.

The builder derives its market series from the same-date eligible cross section.
It never reads outcome columns and it does not write MeeMee/runtime/ranking state.
"""

import argparse
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd


AXIS_ID = "tradex_nikkei225_market_relative_path_v1"
DEFAULT_INPUT = Path(
    r"G:\Tradex\tradex_nikkei225_daily_assessment_feature_ledger_v1\20260714T040047Z-tradex_nikkei225_daily_assessment_feature_ledger_v1\daily_assessment_features.parquet"
)
DEFAULT_OUTPUT = Path(r"G:\Tradex\mrp_v1")
MIN_CROSS_SECTION = 150
LAGS = tuple(range(20))
WINDOWS = (3, 5, 10, 20)
SOURCE_COLUMNS = [
    "code", "ymd", "c", "market_breadth_ma20", "dist_ma20_atr",
    "cross_ma20", "reclaim_ma20", "support_break", "oversold_risk",
    "upper_wick_ratio", "lower_wick_ratio",
]
CONTINUOUS_CLIPS = {
    "beta": (-5.0, 5.0), "return": (-0.30, 0.30),
    "cumulative": (-1.0, 1.0), "z": (-10.0, 10.0),
}
FIXED_INTERACTIONS = [
    "exret1*down_shock", "exret1*up_shock", "resid1*down_shock",
    "resid1*up_shock", "beta60_pre*mret1", "cum_exlog5*d_adv1",
    "dist_ma20_atr*down_shock", "cross_ma20*down_shock",
    "reclaim_ma20*up_shock", "support_break*down_shock",
    "oversold_risk*down_shock", "upper_wick_ratio*down_shock",
    "lower_wick_ratio*up_shock",
]


def _sha(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(8 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def _dump(path: Path, value: Any) -> None:
    def json_default(obj: Any) -> Any:
        # Audit predicates commonly arrive as numpy scalar booleans after
        # pandas comparisons.  Normalize at the artifact boundary so feature
        # calculations and their dtypes remain untouched.
        if isinstance(obj, np.generic):
            return obj.item()
        if isinstance(obj, Path):
            return str(obj)
        raise TypeError(f"Object of type {type(obj).__name__} is not JSON serializable")

    path.write_text(
        json.dumps(value, ensure_ascii=False, indent=2, default=json_default) + "\n",
        encoding="utf-8",
    )


def _clip(s: pd.Series, lo: float, hi: float) -> pd.Series:
    return s.replace([np.inf, -np.inf], np.nan).clip(lo, hi)


def _lag_columns(out: pd.DataFrame, group: pd.core.groupby.DataFrameGroupBy, source: str) -> None:
    for lag in LAGS:
        out[f"{source}_lag{lag}"] = group[source].shift(lag)


def _rolling_regression_pre(
    frame: pd.DataFrame, out: pd.DataFrame, window: int, min_periods: int
) -> None:
    """Regression lr~mlr using only observations through t-1."""
    beta = pd.Series(np.nan, index=frame.index, dtype=float)
    alpha = pd.Series(np.nan, index=frame.index, dtype=float)
    count = pd.Series(0, index=frame.index, dtype=np.int16)
    for _, idx in frame.groupby("code", sort=False).groups.items():
        ix = list(idx)
        x = out.loc[ix, "mlr1"].shift(1)
        y = out.loc[ix, "lr1"].shift(1)
        valid = x.notna() & y.notna()
        n = valid.astype(float).rolling(window, min_periods=1).sum()
        sx = x.where(valid).rolling(window, min_periods=1).sum()
        sy = y.where(valid).rolling(window, min_periods=1).sum()
        sxx = x.mul(x).where(valid).rolling(window, min_periods=1).sum()
        sxy = x.mul(y).where(valid).rolling(window, min_periods=1).sum()
        var_num = sxx - sx.mul(sx).div(n.where(n > 0))
        cov_num = sxy - sx.mul(sy).div(n.where(n > 0))
        ok = (n >= min_periods) & (var_num > 1e-10)
        b = cov_num.div(var_num).where(ok)
        a = sy.div(n).sub(b.mul(sx.div(n))).where(ok)
        beta.loc[ix] = b.to_numpy()
        alpha.loc[ix] = a.to_numpy()
        count.loc[ix] = n.fillna(0).astype(np.int16).to_numpy()
    out[f"beta{window}_pre"] = _clip(beta, *CONTINUOUS_CLIPS["beta"])
    out[f"alpha{window}_pre"] = _clip(alpha, *CONTINUOUS_CLIPS["return"])
    out[f"beta{window}_pre_n"] = count


def _relative_path_features(frame: pd.DataFrame, out: pd.DataFrame) -> None:
    rel_log = pd.Series(np.nan, index=frame.index, dtype=float)
    for _, idx in frame.groupby("code", sort=False).groups.items():
        ix = list(idx)
        x = out.loc[ix, "exlog1"]
        # Keep a missing current observation missing, but retain the prior path
        # level so the next valid observation continues from information known.
        level = x.fillna(0.0).cumsum().where(x.notna())
        rel_log.loc[ix] = level.to_numpy()
    out["rel_log_index"] = rel_log
    for w, minp in ((7, 5), (20, 15), (60, 40)):
        mean_exp = pd.Series(np.nan, index=frame.index, dtype=float)
        slope = pd.Series(np.nan, index=frame.index, dtype=float)
        for _, idx in frame.groupby("code", sort=False).groups.items():
            ix = list(idx)
            level = rel_log.loc[ix]
            # exp(L-logmeanexp(L)) is evaluated stably around the rolling max.
            def logmeanexp(a: np.ndarray) -> float:
                good = a[np.isfinite(a)]
                if len(good) < minp:
                    return np.nan
                m = float(np.max(good))
                return m + float(np.log(np.mean(np.exp(good - m))))
            lma = level.rolling(w, min_periods=minp).apply(logmeanexp, raw=True)
            mean_exp.loc[ix] = np.expm1(level - lma).to_numpy()
            slope.loc[ix] = lma.sub(lma.shift(5)).div(5.0).to_numpy()
        out[f"rel_dist_ma{w}"] = _clip(mean_exp, *CONTINUOUS_CLIPS["cumulative"])
        out[f"rel_ma{w}_slope5"] = _clip(slope, *CONTINUOUS_CLIPS["return"])
        out[f"rel_dist_ma{w}_lag1"] = out.groupby(frame["code"], sort=False)[f"rel_dist_ma{w}"].shift(1)
        out[f"rel_ma{w}_slope5_lag1"] = out.groupby(frame["code"], sort=False)[f"rel_ma{w}_slope5"].shift(1)
    highpos = pd.Series(np.nan, index=frame.index, dtype=float)
    drawdown = pd.Series(np.nan, index=frame.index, dtype=float)
    for _, idx in frame.groupby("code", sort=False).groups.items():
        ix = list(idx); level = rel_log.loc[ix]
        lo = level.rolling(20, min_periods=15).min()
        hi = level.rolling(20, min_periods=15).max()
        highpos.loc[ix] = level.sub(lo).div(hi.sub(lo).where(hi.sub(lo) > 1e-10)).to_numpy()
        drawdown.loc[ix] = np.expm1(level - hi).to_numpy()
    out["rel_highpos20"] = highpos.clip(0, 1)
    out["rel_drawdown20"] = _clip(drawdown, -1, 0)
    for name in ("rel_highpos20", "rel_drawdown20"):
        out[f"{name}_lag1"] = out.groupby(frame["code"], sort=False)[name].shift(1)


def build_features(frame: pd.DataFrame) -> pd.DataFrame:
    missing = sorted(set(SOURCE_COLUMNS) - set(frame.columns))
    if missing:
        raise ValueError(f"missing required columns: {missing}")
    src = frame[SOURCE_COLUMNS].copy().sort_values(["code", "ymd"]).reset_index(drop=True)
    if src.duplicated(["code", "ymd"]).any():
        raise ValueError("duplicate code/ymd")
    src["code"] = src["code"].astype(str)
    src["r1"] = src.groupby("code", sort=False)["c"].pct_change(fill_method=None)
    src["lr1"] = np.log(src["c"]).groupby(src["code"], sort=False).diff()

    cross = src.groupby("ymd", sort=True).agg(
        market_n=("r1", "count"), mret1=("r1", "mean"), mlr1=("lr1", "mean"),
        advancers_ratio=("r1", lambda s: float((s > 0).sum()) / s.notna().sum() if s.notna().sum() else np.nan),
        decliners_ratio=("r1", lambda s: float((s < 0).sum()) / s.notna().sum() if s.notna().sum() else np.nan),
    ).reset_index()
    invalid = cross["market_n"] < MIN_CROSS_SECTION
    cross.loc[invalid, ["mret1", "mlr1", "advancers_ratio", "decliners_ratio"]] = np.nan
    cross["market_valid"] = (~invalid).astype(np.int8)
    src = src.merge(cross, on="ymd", how="left", validate="many_to_one", sort=False)
    src = src.sort_values(["code", "ymd"]).reset_index(drop=True)
    out = src[["code", "ymd"]].copy()
    for name in ("r1", "lr1", "mret1", "mlr1", "advancers_ratio", "decliners_ratio"):
        out[name] = _clip(src[name], *CONTINUOUS_CLIPS["return"]) if name not in ("advancers_ratio", "decliners_ratio") else src[name]
    out["market_n"] = src["market_n"].astype(np.int16)
    out["market_valid"] = src["market_valid"].astype(np.int8)
    out["exret1"] = _clip(out["r1"] - out["mret1"], *CONTINUOUS_CLIPS["return"])
    out["exlog1"] = _clip(out["lr1"] - out["mlr1"], *CONTINUOUS_CLIPS["return"])
    grouped = out.groupby(src["code"], sort=False)
    _lag_columns(out, grouped, "exret1")
    _lag_columns(out, grouped, "exlog1")
    for w in WINDOWS:
        for name in ("exret1", "exlog1"):
            rolled = grouped[name].rolling(w, min_periods=w).sum().reset_index(level=0, drop=True).sort_index()
            stem = "exret_cum" if name == "exret1" else "cum_exlog"
            out[f"{stem}{w}"] = _clip(rolled, *CONTINUOUS_CLIPS["cumulative"])
            out[f"{stem}{w}_pre"] = grouped[name].shift(1).groupby(src["code"], sort=False).rolling(w, min_periods=w).sum().reset_index(level=0, drop=True).sort_index().clip(*CONTINUOUS_CLIPS["cumulative"])

    _rolling_regression_pre(src, out, 20, 15)
    _rolling_regression_pre(src, out, 60, 40)
    out["resid1"] = _clip(out["lr1"] - out["alpha60_pre"] - out["beta60_pre"] * out["mlr1"], *CONTINUOUS_CLIPS["return"])
    grouped = out.groupby(src["code"], sort=False)
    _lag_columns(out, grouped, "resid1")
    for w in WINDOWS:
        out[f"resid_cum{w}"] = _clip(grouped["resid1"].rolling(w, min_periods=w).sum().reset_index(level=0, drop=True).sort_index(), *CONTINUOUS_CLIPS["cumulative"])

    out = out.copy()
    _relative_path_features(src, out)
    out = out.copy()

    date = cross.set_index("ymd")
    date["d_adv1"] = date["advancers_ratio"].diff()
    date["breadth_trend5"] = date["advancers_ratio"] - date["advancers_ratio"].shift(5)
    breadth20 = src.groupby("ymd", sort=True)["market_breadth_ma20"].first().reindex(date.index)
    date["d_breadth20_1"] = breadth20.diff()
    for name in ("d_adv1", "breadth_trend5", "d_breadth20_1"):
        out[name] = src["ymd"].map(date[name])
        out[f"{name}_lag1"] = src["ymd"].map(date[name].shift(1))
    for w in WINDOWS:
        out[f"rel_breadth_divergence{w}"] = _clip(out[f"cum_exlog{w}"] - (out["advancers_ratio"] - src["ymd"].map(date["advancers_ratio"].shift(w))), *CONTINUOUS_CLIPS["cumulative"])
    raw_z = out["cum_exlog5"] - out["breadth_trend5"]
    grouped = out.groupby(src["code"], sort=False)
    zmean = grouped["cum_exlog5"].shift(1).sub(grouped["breadth_trend5"].shift(1)).groupby(src["code"], sort=False).rolling(20, min_periods=15).mean().reset_index(level=0, drop=True).sort_index()
    zstd = grouped["cum_exlog5"].shift(1).sub(grouped["breadth_trend5"].shift(1)).groupby(src["code"], sort=False).rolling(20, min_periods=15).std(ddof=0).reset_index(level=0, drop=True).sort_index()
    out["rel_vs_breadth_z20"] = _clip((raw_z - zmean) / zstd.where(zstd >= 1e-6), *CONTINUOUS_CLIPS["z"])

    sigma = pd.Series(np.nan, index=src.index, dtype=float)
    # This is a market-date series; calculate once and map back to avoid the
    # cross-sectional row count masquerading as time.
    date["sigma20_pre"] = date["mret1"].shift(1).rolling(20, min_periods=15).std(ddof=0)
    sigma = src["ymd"].map(date["sigma20_pre"])
    out["sigma20_pre"] = sigma
    threshold = np.maximum(0.01, 1.5 * sigma)
    out["down_shock"] = (out["mret1"] <= -threshold).where(threshold.notna()).astype("Int8")
    out["up_shock"] = (out["mret1"] >= threshold).where(threshold.notna()).astype("Int8")
    out["breadth_collapse"] = (out["d_adv1"] <= -0.20).where(out["d_adv1"].notna()).astype("Int8")
    out["breadth_surge"] = (out["d_adv1"] >= 0.20).where(out["d_adv1"].notna()).astype("Int8")

    out = out.copy()
    down = out["down_shock"].astype(float); up = out["up_shock"].astype(float)
    out["ix_exret1_down_shock"] = out["exret1"] * down
    out["ix_exret1_up_shock"] = out["exret1"] * up
    out["ix_resid1_down_shock"] = out["resid1"] * down
    out["ix_resid1_up_shock"] = out["resid1"] * up
    out["ix_beta60_mret1"] = out["beta60_pre"] * out["mret1"]
    out["ix_cum_exlog5_d_adv1"] = out["cum_exlog5"] * out["d_adv1"]
    for source, shock, suffix in (
        ("dist_ma20_atr", down, "down_shock"), ("cross_ma20", down, "down_shock"),
        ("reclaim_ma20", up, "up_shock"), ("support_break", down, "down_shock"),
        ("oversold_risk", down, "down_shock"), ("upper_wick_ratio", down, "down_shock"),
        ("lower_wick_ratio", up, "up_shock"),
    ):
        out[f"ix_{source}_{suffix}"] = pd.to_numeric(src[source], errors="coerce") * shock

    # Every nullable continuous feature gets an explicit missingness channel.
    flags = {"market_valid", "down_shock", "up_shock", "breadth_collapse", "breadth_surge", "market_n", "beta20_pre_n", "beta60_pre_n"}
    # Defragment before the wide, deterministic mask expansion.  The formal
    # ledger is intentionally wide, but building masks one column at a time
    # needlessly makes full-universe generation quadratic in block count.
    out = out.copy()
    continuous = [c for c in out.columns if c not in {"code", "ymd"} | flags]
    masks: dict[str, pd.Series] = {}
    for col in continuous:
        numeric = pd.to_numeric(out[col], errors="coerce").replace([np.inf, -np.inf], np.nan)
        out[col] = numeric
        masks[f"{col}_missing"] = numeric.isna().astype(np.int8)
    out = pd.concat([out, pd.DataFrame(masks, index=out.index)], axis=1)
    if np.isinf(out.select_dtypes(include=[np.number]).to_numpy(dtype=float, na_value=np.nan)).any():
        raise AssertionError("infinite feature value")
    return out


def _fixture_frame(n_codes: int = 152, n_dates: int = 85) -> pd.DataFrame:
    rows = []
    for ci in range(n_codes):
        for t in range(n_dates):
            market = 0.0015 * np.sin(t / 5) - (0.025 if t == 70 else 0)
            relative = (ci - n_codes / 2) * 0.000002 + 0.0008 * np.cos((t + ci % 7) / 6)
            close = 100 * np.exp(0.0004 * t + 0.002 * np.sin(t / 6) + relative * t + market)
            rows.append({"code": f"{ci:04d}", "ymd": 20200101 + t, "c": close,
                "market_breadth_ma20": 0.55 + 0.08 * np.sin(t / 9), "dist_ma20_atr": np.sin(t/8),
                "cross_ma20": int(t % 17 == 0), "reclaim_ma20": int(t % 19 == 0),
                "support_break": int(t % 23 == 0), "oversold_risk": max(0.0, -np.sin(t/8)),
                "upper_wick_ratio": 0.2 + 0.1*np.sin(t/4), "lower_wick_ratio": 0.2 + 0.1*np.cos(t/4)})
    return pd.DataFrame(rows)


def self_tests() -> dict[str, bool]:
    base = _fixture_frame(); full = build_features(base)
    dates = sorted(base.ymd.unique()); cutoff = dates[65]
    prefix = full[full.ymd <= cutoff].reset_index(drop=True)
    trunc = build_features(base[base.ymd <= cutoff]).reset_index(drop=True)
    mutated = base.copy(); future = mutated.ymd > cutoff
    mutated.loc[future, "c"] *= 2.7
    mut_prefix = build_features(mutated); mut_prefix = mut_prefix[mut_prefix.ymd <= cutoff].reset_index(drop=True)
    target_date = dates[50]; peer_mut = base.copy()
    peer = (peer_mut.code != "0000") & (peer_mut.ymd == target_date)
    peer_mut.loc[peer, "c"] *= np.linspace(0.98, 1.02, peer.sum())
    changed = build_features(peer_mut)
    a = full[(full.code == "0000") & (full.ymd == target_date)].iloc[0]
    b = changed[(changed.code == "0000") & (changed.ymd == target_date)].iloc[0]
    target = full[full.code == "0000"].reset_index(drop=True)
    lag1_ok = np.allclose(target.exret1_lag1.iloc[1:], target.exret1.iloc[:-1], equal_nan=True)
    checks = {
        "fixture_unique_key": not full.duplicated(["code", "ymd"]).any(),
        "fixture_market_valid": bool(full.market_valid.iloc[-1] == 1),
        "cutoff_regeneration": prefix.equals(trunc),
        "future_mutation": prefix.equals(mut_prefix),
        "cross_section_peer_sensitivity": bool(not np.isclose(a.mret1, b.mret1)),
        "cross_section_target_return_unchanged": bool(np.isclose(a.r1, b.r1)),
        "lag1_exact": bool(lag1_ok),
        "pre_beta_available": bool(target.beta60_pre.notna().any()),
        "shock_threshold_fixed": bool((full.down_shock.fillna(0).isin([0, 1])).all()),
        "no_infinite": not np.isinf(full.select_dtypes(include=[np.number]).to_numpy(dtype=float, na_value=np.nan)).any(),
    }
    if not all(checks.values()):
        raise AssertionError(checks)
    return checks


def _real_audits(frame: pd.DataFrame) -> dict[str, Any]:
    dates = sorted(frame.ymd.unique()); cutoff = int(dates[len(dates) * 3 // 4])
    # Use all names so the >=150 cross-sectional validity contract is exercised.
    small = frame[frame.ymd.isin(dates[max(0, len(dates)*3//4-80):len(dates)*3//4+5])].copy()
    full = build_features(small); prefix = full[full.ymd <= cutoff].reset_index(drop=True)
    trunc = build_features(small[small.ymd <= cutoff]).reset_index(drop=True)
    mutated = small.copy(); mutated.loc[mutated.ymd > cutoff, "c"] *= 1.91
    mut = build_features(mutated); mut = mut[mut.ymd <= cutoff].reset_index(drop=True)
    target_date = sorted(small.ymd.unique())[40]
    codes = sorted(small.code.astype(str).unique()); target_code = codes[0]
    changed = small.copy(); peers = (changed.ymd == target_date) & (changed.code.astype(str) != target_code)
    changed.loc[peers, "c"] *= np.linspace(.99, 1.01, peers.sum())
    alt = build_features(changed)
    before = full[(full.code == target_code) & (full.ymd == target_date)].iloc[0]
    after = alt[(alt.code == target_code) & (alt.ymd == target_date)].iloc[0]
    target = full[full.code == target_code].reset_index(drop=True)
    return {
        "cutoff": cutoff, "rows": len(small), "codes": len(codes),
        "cutoff_regeneration_passed": prefix.equals(trunc),
        "future_mutation_passed": prefix.equals(mut),
        "cross_section_peer_sensitivity_passed": not np.isclose(before.mret1, after.mret1),
        "cross_section_target_return_unchanged_passed": np.isclose(before.r1, after.r1),
        "lag1_passed": np.allclose(target.exret1_lag1.iloc[1:], target.exret1.iloc[:-1], equal_nan=True),
    }


def run(input_parquet: Path, output_root: Path) -> Path:
    tests = self_tests()
    frame = pd.read_parquet(input_parquet, columns=SOURCE_COLUMNS)
    audits = _real_audits(frame)
    features = build_features(frame)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    output = output_root / f"{stamp}-{AXIS_ID}"
    output.mkdir(parents=True, exist_ok=False)
    parquet = output / "market_relative_path_features.parquet"
    features.to_parquet(parquet, index=False, compression="zstd")
    manifest = {
        "schema_version": f"{AXIS_ID}.manifest.v1", "artifact_role": "feature_ledger",
        "source": {"path": str(input_parquet), "sha256": _sha(input_parquet), "columns_loaded": SOURCE_COLUMNS},
        "output": {"path": str(parquet), "sha256": _sha(parquet), "rows": len(features), "codes": int(features.code.nunique()), "columns": len(features.columns)},
        "contract": {
            "cross_section": "same-date equal-weight, valid only market_n>=150",
            "lags": list(LAGS), "cumulative_windows": list(WINDOWS),
            "regression": {"windows": [20, 60], "minimum_pairs": [15, 40], "pre_t_only": True, "variance_gate": 1e-10},
            "relative_ma": {"windows": [7, 20, 60], "minimums": [5, 15, 40], "lag1": True},
            "shock": "mret <=/>= max(1%,1.5*sigma20_pre); breadth shock is separate abs(d_adv1)>=.20",
            "fixed_interactions": FIXED_INTERACTIONS,
            "clips": CONTINUOUS_CLIPS, "outcome_columns_loaded": False,
        },
        "boundary": {"owner": "TRADEX", "meemee_changed": False, "runtime_db_write": False, "production_ranking_changed": False},
    }
    _dump(output / "manifest.json", manifest)
    audit = {
        "schema_version": f"{AXIS_ID}.audit.v1", "self_tests": tests,
        "cutoff_regeneration": audits["cutoff_regeneration_passed"],
        "future_mutation": audits["future_mutation_passed"],
        "cross_section": {"peer_sensitivity": audits["cross_section_peer_sensitivity_passed"], "target_return_unchanged": audits["cross_section_target_return_unchanged_passed"]},
        "lag1": audits["lag1_passed"], "sample": {k: audits[k] for k in ("cutoff", "rows", "codes")},
        "key_unique": not features.duplicated(["code", "ymd"]).any(),
        "finite_or_missing": not np.isinf(features.select_dtypes(include=[np.number]).to_numpy(dtype=float, na_value=np.nan)).any(),
        "artifact_sha256": _sha(parquet),
    }
    _dump(output / "audit.json", audit)
    complete = all(tests.values()) and all([
        audits["cutoff_regeneration_passed"], audits["future_mutation_passed"],
        audits["cross_section_peer_sensitivity_passed"], audits["cross_section_target_return_unchanged_passed"],
        audits["lag1_passed"], audit["key_unique"], audit["finite_or_missing"],
        _sha(parquet) == manifest["output"]["sha256"],
    ])
    _dump(output / "_ARTIFACT_COMPLETE.json", {
        "complete": complete, "manifest_sha256": _sha(output / "manifest.json"),
        "audit_sha256": _sha(output / "audit.json"), "parquet_sha256": _sha(parquet),
    })
    if not complete:
        raise RuntimeError("artifact verification failed")
    return output


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-parquet", type=Path, default=DEFAULT_INPUT)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--self-test", action="store_true")
    args = parser.parse_args()
    if args.self_test:
        print(json.dumps(self_tests(), indent=2)); return
    print(run(args.input_parquet, args.output_root))


if __name__ == "__main__":
    main()
