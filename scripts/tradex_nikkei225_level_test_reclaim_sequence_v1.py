from __future__ import annotations

"""PIT level/candle/box sequence features for the Nikkei-225 daily assessor.

This module deliberately reads only contemporaneous feature columns.  Outcome
columns in the source ledger are labels and are never loaded.
"""

import argparse
import hashlib
import json
import tempfile
from collections import deque
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd


AXIS_ID = "tradex_nikkei225_level_test_reclaim_sequence_v1"
DEFAULT_INPUT = Path(r"G:\Tradex\tradex_nikkei225_daily_assessment_feature_ledger_v1\20260714T040047Z-tradex_nikkei225_daily_assessment_feature_ledger_v1\daily_assessment_features.parquet")
DEFAULT_OUTPUT = Path(r"G:\Tradex\levelseq_v1")
LEVELS = {
    "support20_prior": ("support20", 1.0, True),
    "resistance20_prior": ("resistance20", -1.0, True),
    "ma7": ("ma7", 1.0, False),
    "ma20": ("ma20", 1.0, False),
    "ma60": ("ma60", 1.0, False),
}
SOURCE_COLUMNS = [
    "code", "ymd", "o", "h", "l", "c", "v", "atr14", "ma7", "ma20", "ma60",
    "support20", "resistance20", "body_ratio", "upper_wick_ratio", "lower_wick_ratio", "close_pos",
]
LABEL_PREFIXES = ("ret_close_", "down_exc_", "up_exc_")


def _sha(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(8 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def _dump(path: Path, value: Any) -> None:
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _slope(values: np.ndarray) -> float:
    ok = np.isfinite(values)
    if ok.sum() < 2:
        return np.nan
    x = np.arange(len(values), dtype=float)[ok]
    y = values[ok]
    return float(np.polyfit(x, y, 1)[0])


def _candle_features(part: pd.DataFrame) -> pd.DataFrame:
    out = pd.DataFrame(index=part.index)
    atr = part["atr14"].replace(0, np.nan)
    signed = (part["c"] - part["o"]) / atr
    absolute = (part["c"] - part["o"]).abs() / atr
    rng = (part["h"] - part["l"]) / atr
    lower_high = part["h"].diff().lt(0).astype(float)
    lower_low = part["l"].diff().lt(0).astype(float)
    close_to_high = (part["h"] - part["c"]) / atr
    close_to_low = (part["c"] - part["l"]) / atr
    for w in (3, 5, 10):
        roll = lambda s: s.rolling(w, min_periods=w)
        out[f"candle{w}_signed_body_sum_atr"] = roll(signed).sum()
        out[f"candle{w}_abs_body_sum_atr"] = roll(absolute).sum()
        out[f"candle{w}_signed_body_slope_atr"] = roll(signed).apply(_slope, raw=True)
        out[f"candle{w}_abs_body_slope_atr"] = roll(absolute).apply(_slope, raw=True)
        out[f"candle{w}_close_pos_slope"] = roll(part["close_pos"]).apply(_slope, raw=True)
        out[f"candle{w}_close_to_high_mean_atr"] = roll(close_to_high).mean()
        out[f"candle{w}_close_to_low_mean_atr"] = roll(close_to_low).mean()
        out[f"candle{w}_upper_wick_sum"] = roll(part["upper_wick_ratio"]).sum()
        out[f"candle{w}_lower_wick_sum"] = roll(part["lower_wick_ratio"]).sum()
        out[f"candle{w}_upper_wick_slope"] = roll(part["upper_wick_ratio"]).apply(_slope, raw=True)
        out[f"candle{w}_lower_wick_slope"] = roll(part["lower_wick_ratio"]).apply(_slope, raw=True)
        out[f"candle{w}_lower_high_count"] = roll(lower_high).sum()
        out[f"candle{w}_lower_low_count"] = roll(lower_low).sum()
        out[f"candle{w}_range_expansion"] = rng / roll(rng.shift(1)).mean()
    return out


def _level_features(part: pd.DataFrame, name: str, source: str, direction: float, freeze: bool) -> pd.DataFrame:
    n = len(part)
    closes = part["c"].to_numpy(dtype=float, copy=False)
    atrs = part["atr14"].to_numpy(dtype=float, copy=False)
    raw_levels = part[source].to_numpy(dtype=float, copy=False)
    out: list[dict[str, Any]] = []
    tests: list[bool] = []
    test_zs: list[float] = []
    test_ix: list[int] = []
    frozen: float | None = None
    break_ix: int | None = None
    reclaim_ix: int | None = None
    rebreak_ix: int | None = None
    breaks = reclaims = 0
    break_events: deque[int] = deque(); reclaim_events: deque[int] = deque()
    z_since: list[float] = []
    z_since_min = z_since_max = np.nan
    z_since_below_count = 0
    recent_test_events: deque[tuple[int, float]] = deque()
    consecutive_below = consecutive_above = 0
    previous_anchor: float | None = None
    prior_level = np.nan
    for i, row in enumerate(part.itertuples(index=False)):
        atr = float(getattr(row, "atr14"))
        raw_level = float(getattr(row, source))
        valid = np.isfinite(atr) and atr > 0 and np.isfinite(raw_level)
        level = frozen if frozen is not None else raw_level
        valid = valid and np.isfinite(level)
        if valid:
            close, low, high = float(row.c), float(row.l), float(row.h)
            z = direction * (close - level) / atr
            touched = low <= level + .25 * atr and high >= level - .25 * atr
        else:
            z, touched = np.nan, False
        tests.append(touched)
        if touched:
            # Positive depth means penetration through the holding side.
            test_ix.append(i); test_zs.append(float(-z))
            recent_test_events.append((i, float(-z)))
        while recent_test_events and recent_test_events[0][0] < i - 19:
            recent_test_events.popleft()

        broke_now = valid and z < -.25 and (break_ix is None)
        if broke_now:
            break_ix, reclaim_ix, rebreak_ix = i, None, None
            breaks += 1; break_events.append(i)
            z_since = [float(z)]
            z_since_min = z_since_max = float(z)
            z_since_below_count = int(z < -.10)
            if freeze:
                frozen = raw_level
                level = frozen
        elif break_ix is not None and valid:
            z_since.append(float(z))
            z_since_min = min(z_since_min, float(z))
            z_since_max = max(z_since_max, float(z))
            z_since_below_count += int(z < -.10)
            if reclaim_ix is None and z >= .10:
                reclaim_ix = i; reclaims += 1; reclaim_events.append(i)
            elif reclaim_ix is not None and rebreak_ix is None and z < -.25:
                rebreak_ix = i

        no_reset = False
        if break_ix is not None and valid:
            no_reset = len(z_since) >= 2 and z_since_below_count >= 2 and z_since_max < .10

        state = 0
        if break_ix is not None: state = 1
        if reclaim_ix is not None: state = 2
        if rebreak_ix is not None: state = 3
        recent_depth = [depth for _, depth in recent_test_events]
        last_age = i - test_ix[-1] if test_ix else 21
        while break_events and break_events[0] < i - 19:
            break_events.popleft()
        while reclaim_events and reclaim_events[0] < i - 19:
            reclaim_events.popleft()

        # Usually this is an O(1) continuation. Recompute only when the active
        # frozen anchor changes, because that change intentionally reinterprets
        # the immediately preceding closes against the new fixed level.
        active_anchor = frozen if frozen is not None else None
        if active_anchor != previous_anchor:
            consecutive_below = consecutive_above = 0
            for j in range(i, -1, -1):
                a = atrs[j]
                lev = active_anchor if active_anchor is not None else raw_levels[j]
                zz = direction * (closes[j] - lev) / a if a > 0 and np.isfinite(lev) else np.nan
                if np.isfinite(zz) and zz < -.10 and consecutive_above == 0:
                    consecutive_below += 1
                elif np.isfinite(zz) and zz >= .10 and consecutive_below == 0:
                    consecutive_above += 1
                else:
                    break
        else:
            consecutive_below = consecutive_below + 1 if np.isfinite(z) and z < -.10 else 0
            consecutive_above = consecutive_above + 1 if np.isfinite(z) and z >= .10 else 0
        previous_anchor = active_anchor
        window_start = max(0, i - 19)
        window_atr = atrs[window_start:i + 1]
        window_level = frozen if frozen is not None else raw_levels[window_start:i + 1]
        window_z = direction * (closes[window_start:i + 1] - window_level) / window_atr
        window_z[(window_atr <= 0) | ~np.isfinite(window_level)] = np.nan
        row_out = {
            f"{name}_level_z": z,
            f"{name}_test": int(touched),
            f"{name}_test_count5": int(sum(tests[max(0, i-4):i+1])),
            f"{name}_test_count20": int(sum(tests[max(0, i-19):i+1])),
            f"{name}_test_age": int(min(last_age, 21)),
            f"{name}_test_age_missing": int(not test_ix),
            f"{name}_last_test_depth_atr": test_zs[-1] if test_zs else np.nan,
            f"{name}_max_test_depth20_atr": max(recent_depth) if recent_depth else np.nan,
            f"{name}_test_depth_slope20": _slope(np.asarray(recent_depth)) if len(recent_depth) >= 2 else np.nan,
            f"{name}_test_depth_slope20_missing": int(len(recent_depth) < 2),
            f"{name}_hold_count20": int(np.sum(window_z >= .10)),
            f"{name}_break_count": breaks,
            f"{name}_break_count20":len(break_events),
            f"{name}_break_age": min(i - break_ix, 21) if break_ix is not None else 21,
            f"{name}_break_age_missing": int(break_ix is None),
            f"{name}_reclaim_count": reclaims,
            f"{name}_reclaim_count20":len(reclaim_events),
            f"{name}_reclaim_age": min(i - reclaim_ix, 21) if reclaim_ix is not None else 21,
            f"{name}_reclaim_age_missing": int(reclaim_ix is None),
            f"{name}_rebreak_flag": int(rebreak_ix is not None),
            f"{name}_no_reset_below": int(no_reset),
            f"{name}_lifecycle_state": state,
            f"{name}_break_to_reclaim_bars": reclaim_ix - break_ix if reclaim_ix is not None and break_ix is not None else np.nan,
            f"{name}_reclaim_to_rebreak_bars": rebreak_ix - reclaim_ix if rebreak_ix is not None and reclaim_ix is not None else np.nan,
            f"{name}_consecutive_below": consecutive_below,
            f"{name}_consecutive_above": consecutive_above,
            f"{name}_min_z_since_break": z_since_min if z_since else np.nan,
            f"{name}_max_z_since_break": z_since_max if z_since else np.nan,
            f"{name}_anchor_drift_atr": ((raw_level - prior_level) / atr) if (not freeze and valid and np.isfinite(prior_level)) else (0.0 if freeze and valid else np.nan),
        }
        out.append(row_out)
        prior_level = raw_level
        # A prior-range anchor lives at most 20 bars after break and resets on
        # a decisive return to the holding side. Terminal rebreak starts fresh.
        if break_ix is not None and freeze and ((i - break_ix >= 20) or (valid and z >= .25)):
            frozen = None; break_ix = reclaim_ix = rebreak_ix = None; z_since = []
            z_since_min = z_since_max = np.nan; z_since_below_count = 0
        elif rebreak_ix is not None and i > rebreak_ix:
            break_ix = reclaim_ix = rebreak_ix = None; z_since = []
            z_since_min = z_since_max = np.nan; z_since_below_count = 0
    return pd.DataFrame(out, index=part.index)


def _sideways_features(part: pd.DataFrame) -> pd.DataFrame:
    """PIT compression and fixed pre-run box lifecycle."""
    atr = part.atr14.replace(0, np.nan)
    pclose = part.c.shift(1)
    tr = pd.concat([(part.h-part.l), (part.h-pclose).abs(), (part.l-pclose).abs()], axis=1).max(axis=1)
    atr60 = tr.rolling(60, min_periods=60).mean()
    hi10, lo10 = part.h.rolling(10, min_periods=10).max(), part.l.rolling(10, min_periods=10).min()
    hi20, lo20 = part.h.rolling(20, min_periods=20).max(), part.l.rolling(20, min_periods=20).min()
    range10, range20 = hi10-lo10, hi20-lo20
    ma_spread = part[["ma7","ma20","ma60"]].max(axis=1)-part[["ma7","ma20","ma60"]].min(axis=1)
    compressed = (range10 <= 4*atr) & (ma_spread <= atr)
    vol5, vol20 = part.v.rolling(5,min_periods=5).mean(), part.v.rolling(20,min_periods=20).mean()
    rows: list[dict[str, Any]]=[]
    duration=0; upper=lower=None; box_start=None; outside_side=0; outside_run=0
    upper_events: list[tuple[int,float]]=[]; lower_events: list[tuple[int,float]]=[]
    last_order=0  # none/upper/lower/both-upper-last/both-lower-last = 0..4
    for i,row in enumerate(part.itertuples(index=False)):
        a=float(row.atr14); comp=bool(compressed.iloc[i])
        if comp:
            duration=min(20,duration+1)
            if duration==1:
                # The fixed box is the twenty bars strictly before run start.
                if i>=20:
                    upper=float(part.h.iloc[i-20:i].max()); lower=float(part.l.iloc[i-20:i].min())
                    box_start=i; upper_events=[]; lower_events=[]; last_order=0; outside_side=outside_run=0
                else:
                    upper=lower=None; box_start=None
        else:
            duration=0
        valid=upper is not None and lower is not None and np.isfinite(a) and a>0
        up_fail=down_fail=False; up_depth=down_depth=np.nan
        if valid:
            up_depth=(float(row.h)-upper)/a; down_depth=(lower-float(row.l))/a
            up_fail=up_depth>=.10 and float(row.c)<=upper+.10*a
            down_fail=down_depth>=.10 and float(row.c)>=lower-.10*a
            if up_fail: upper_events.append((i,float(up_depth)))
            if down_fail: lower_events.append((i,float(down_depth)))
            if up_fail and down_fail:
                # Daily OHLC cannot reveal intraday ordering. Preserve the last
                # identifiable ordering and expose an ambiguity flag below.
                pass
            elif up_fail:
                last_order=1 if not lower_events else 3
            elif down_fail:
                last_order=2 if not upper_events else 4
            side=1 if float(row.c)>upper+.25*a else (-1 if float(row.c)<lower-.25*a else 0)
            if side and side==outside_side: outside_run+=1
            elif side: outside_side=side; outside_run=1
            else: outside_side=outside_run=0
        recent_u=[e for e in upper_events if e[0]>=i-19]
        recent_l=[e for e in lower_events if e[0]>=i-19]
        all_events=recent_u+recent_l
        rows.append({
          "sideways_compressed":int(comp),"sideways_duration20":duration,
          "compression_ratio_10_20":range10.iloc[i]/range20.iloc[i] if range20.iloc[i]>0 else np.nan,
          "range_atr_10":range10.iloc[i]/a if a>0 else np.nan,"range_atr_20":range20.iloc[i]/a if a>0 else np.nan,
          "atr14_to_atr60":a/atr60.iloc[i] if atr60.iloc[i]>0 else np.nan,
          "volume_mean5_to20":vol5.iloc[i]/vol20.iloc[i] if vol20.iloc[i]>0 else np.nan,
          "box_age20":min(20,i-box_start+1) if box_start is not None else 0,
          "box_history_missing":int(not valid),
          "box_upper_dist_atr":(upper-float(row.c))/a if valid else np.nan,
          "box_lower_dist_atr":(float(row.c)-lower)/a if valid else np.nan,
          "box_upper_failed_excursion":int(up_fail),"box_lower_failed_excursion":int(down_fail),
          "box_same_day_both_failed_ambiguous":int(up_fail and down_fail),
          "box_upper_failed_excursion_count20":len(recent_u),"box_lower_failed_excursion_count20":len(recent_l),
          "box_upper_max_failed_depth20_atr":max((e[1] for e in recent_u),default=np.nan),
          "box_lower_max_failed_depth20_atr":max((e[1] for e in recent_l),default=np.nan),
          "box_last_failed_age20":min(20,i-max((e[0] for e in all_events),default=i-20)) if all_events else 20,
          "box_last_failed_age_missing":int(not all_events),
          "box_failed_balance20":(len(recent_u)-len(recent_l))/(len(recent_u)+len(recent_l)+1),
          "box_breakout_reentry_order":last_order,
          "box_clear_break_side":outside_side if outside_run else 0,
          "box_clear_break_confirmed2":int(outside_run>=2),
        })
        # Two consecutive decisive closes terminate the fixed-box episode.
        if outside_run>=2:
            upper=lower=None; box_start=None; upper_events=[]; lower_events=[]; last_order=0; outside_side=outside_run=0
    return pd.DataFrame(rows,index=part.index)


def build_features(frame: pd.DataFrame) -> pd.DataFrame:
    missing = sorted(set(SOURCE_COLUMNS) - set(frame.columns))
    if missing:
        raise ValueError(f"missing required columns: {missing}")
    source = frame[SOURCE_COLUMNS].copy().sort_values(["code", "ymd"]).reset_index(drop=True)
    if source.duplicated(["code", "ymd"]).any():
        raise ValueError("duplicate code/ymd")
    pieces: list[pd.DataFrame] = []
    for _, part in source.groupby("code", sort=False):
        part = part.reset_index(drop=True)
        feat = pd.concat([_candle_features(part), _sideways_features(part)], axis=1)
        for name, args in LEVELS.items():
            feat = pd.concat([feat, _level_features(part, name, *args)], axis=1)
        pieces.append(pd.concat([part[["code", "ymd"]], feat], axis=1))
    return pd.concat(pieces, ignore_index=True)


def _fixture_frame(n: int = 80) -> pd.DataFrame:
    x = np.arange(n, dtype=float); close = 100 + .08*x
    # deterministic break -> no-reset -> reclaim -> rebreak around MA-like levels
    close[55:58] -= np.array([2.0, 2.5, 2.8]); close[58] += .5; close[59] -= 2.5
    return pd.DataFrame({"code":"0001", "ymd":20200101+np.arange(n), "o":close-.1, "h":close+.5,
        "l":close-.5, "c":close, "v":1000+x, "atr14":np.ones(n), "ma7":100+.08*x,
        "ma20":100+.08*x, "ma60":100+.08*x, "support20":99+.08*x,
        "resistance20":101+.08*x, "body_ratio":.1, "upper_wick_ratio":.2,
        "lower_wick_ratio":.2, "close_pos":.5})


def self_tests() -> dict[str, bool]:
    base = _fixture_frame(); a = build_features(base)
    box_fixture=base.copy(); box_fixture.loc[:29,"ma7"]=90; box_fixture.loc[:29,"ma60"]=110
    box_fixture.loc[30:,["ma7","ma20","ma60"]]=np.repeat(box_fixture.loc[30:,"c"].to_numpy(),3).reshape(-1,3)
    box_features=build_features(box_fixture)
    cutoff = 60
    trunc = build_features(base.iloc[:cutoff].copy())
    mut = base.copy(); mut.loc[cutoff:, ["o","h","l","c","v"]] *= 9
    before = build_features(mut).iloc[:cutoff]
    fcols = [c for c in a if c not in ("code", "ymd")]
    with_labels=base.copy(); with_labels["ret_close_10"]=np.linspace(-9,9,len(base))
    label_a=build_features(with_labels); with_labels["ret_close_10"]=-with_labels["ret_close_10"]
    label_b=build_features(with_labels)
    checks = {
        "fixture_unique_key": not a.duplicated(["code","ymd"]).any(),
        "fixture_no_source_replication": set(a.columns).isdisjoint(set(SOURCE_COLUMNS)-{"code","ymd"}),
        "cutoff_regeneration": trunc.equals(a.iloc[:cutoff].reset_index(drop=True)),
        "future_mutation": np.allclose(a.iloc[:cutoff][fcols], before[fcols], equal_nan=True),
        "label_isolation_contract": not any(c.startswith(LABEL_PREFIXES) for c in SOURCE_COLUMNS) and label_a.equals(label_b),
        "fixture_break_seen": bool((a.ma20_lifecycle_state >= 1).any()),
        "fixture_box_anchor_seen": bool((box_features.box_history_missing == 0).any()),
        "fixture_feature_finite": bool(a.filter(regex="candle10_signed_body_sum").notna().any().all()),
    }
    if not all(checks.values()):
        raise AssertionError(checks)
    return checks


def _real_pit_audits(frame: pd.DataFrame) -> dict[str, Any]:
    codes=sorted(frame.code.astype(str).unique())[:2]
    small=frame[frame.code.astype(str).isin(codes)].sort_values(["code","ymd"]).reset_index(drop=True)
    dates=sorted(small.ymd.unique()); cutoff=int(dates[len(dates)*3//4])
    full=build_features(small); prefix=full[full.ymd<=cutoff].reset_index(drop=True)
    trunc=build_features(small[small.ymd<=cutoff]).reset_index(drop=True)
    mutated=small.copy(); future=mutated.ymd>cutoff
    mutated.loc[future,["o","h","l","c","v"]]*=[1.7,2.1,.4,1.8,9.0]
    mut=build_features(mutated); mut=mut[mut.ymd<=cutoff].reset_index(drop=True)
    labeled=small.copy(); labeled["ret_close_10"]=np.arange(len(labeled),dtype=float)
    label_a=build_features(labeled); labeled["ret_close_10"]*=-99; label_b=build_features(labeled)
    return {"codes":codes,"cutoff":cutoff,"cutoff_regeneration_passed":prefix.equals(trunc),
      "future_mutation_passed":prefix.equals(mut),"label_isolation_passed":label_a.equals(label_b)}


def run(input_parquet: Path, output_root: Path) -> Path:
    tests = self_tests()
    frame = pd.read_parquet(input_parquet, columns=SOURCE_COLUMNS)
    real_audits = _real_pit_audits(frame)
    features = build_features(frame)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    output = output_root / f"{stamp}-{AXIS_ID}"
    output.mkdir(parents=True, exist_ok=False)
    parquet = output / "level_test_reclaim_sequence_features.parquet"
    features.to_parquet(parquet, index=False, compression="zstd")
    manifest = {
        "schema_version": f"{AXIS_ID}.manifest.v1", "artifact_role":"feature_ledger",
        "source":{"path":str(input_parquet),"sha256":_sha(input_parquet),"columns_loaded":SOURCE_COLUMNS},
        "output":{"path":str(parquet),"sha256":_sha(parquet),"rows":len(features),"codes":features.code.nunique(),"columns":len(features.columns)},
        "contract":{"families":LEVELS,"level_epsilon_atr":.10,"test_band_atr":.25,
          "break_z_lt":-.25,"reclaim_z_ge":.10,"rebreak_z_lt":-.25,"prior_range_anchor_max_bars":20,
          "candle_windows":[3,5,10],"sideways":"range10<=4ATR14 and MA7/20/60 spread<=1ATR14",
          "box":"fixed prior-20 HH/LL at compressed-run start; .10ATR breach/reentry; .25ATR decisive close; terminate after two outside closes",
          "feature_time":"t or earlier","outcome_columns_loaded":False},
        "boundary":{"owner":"TRADEX","meemee_changed":False,"runtime_db_write":False,"production_ranking_changed":False},
    }
    _dump(output/"manifest.json", manifest)
    audit = {"schema_version":f"{AXIS_ID}.audit.v1","self_tests":tests,
      "cutoff_regeneration":{"fixture_passed":tests["cutoff_regeneration"],"real_sample_passed":real_audits["cutoff_regeneration_passed"],"codes":real_audits["codes"],"cutoff":real_audits["cutoff"]},
      "future_mutation":{"fixture_passed":tests["future_mutation"],"real_sample_passed":real_audits["future_mutation_passed"],"mutation":"all sampled OHLCV after cutoff"},
      "label_isolation":{"fixture_passed":tests["label_isolation_contract"],"real_sample_passed":real_audits["label_isolation_passed"],"loaded_columns":SOURCE_COLUMNS},
      "key_unique":not features.duplicated(["code","ymd"]).any(),"artifact_sha256":_sha(parquet)}
    _dump(output/"audit.json",audit)
    complete = all(tests.values()) and all(real_audits[k] for k in ("cutoff_regeneration_passed","future_mutation_passed","label_isolation_passed")) and audit["key_unique"] and _sha(parquet)==manifest["output"]["sha256"]
    _dump(output/"_ARTIFACT_COMPLETE.json",{"complete":complete,"manifest_sha256":_sha(output/"manifest.json"),"audit_sha256":_sha(output/"audit.json"),"parquet_sha256":_sha(parquet)})
    if not complete: raise RuntimeError("artifact verification failed")
    return output


def main() -> None:
    p=argparse.ArgumentParser(); p.add_argument("--input-parquet",type=Path,default=DEFAULT_INPUT)
    p.add_argument("--output-root",type=Path,default=DEFAULT_OUTPUT); p.add_argument("--self-test",action="store_true")
    a=p.parse_args()
    if a.self_test: print(json.dumps(self_tests(),indent=2)); return
    print(run(a.input_parquet,a.output_root))


if __name__ == "__main__": main()
