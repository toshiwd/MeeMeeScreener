#!/usr/bin/env python
"""Build a review-only PIT ledger for mature-top retry-failure geometry.

Only the retry sequence is new. Existing MA-duration, impulse-candle, gap and
MA-break columns are copied from the authoritative daily feature ledger and are
not combined into a selector here.
"""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path

import numpy as np
import pandas as pd


DEFAULT_SOURCE = Path(
    r"G:\Tradex\tradex_nikkei225_daily_assessment_feature_ledger_v1"
    r"\20260714T040047Z-tradex_nikkei225_daily_assessment_feature_ledger_v1"
    r"\daily_assessment_features.parquet"
)
LOOKBACK = 40
MIN_POST_PEAK_BARS = 4


def _sha(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for block in iter(lambda: f.read(1 << 20), b""):
            h.update(block)
    return h.hexdigest()


def _run_length(x: np.ndarray) -> np.ndarray:
    out = np.zeros(len(x), np.int32)
    n = 0
    for i, flag in enumerate(x):
        n = n + 1 if bool(flag) else 0
        out[i] = n
    return out


def build_code(g: pd.DataFrame) -> pd.DataFrame:
    """Generate features using only rows <= each output row."""
    g = g.sort_values("ymd").reset_index(drop=True).copy()
    n = len(g)
    hi = g.h.astype(float).to_numpy()
    lo = g.l.astype(float).to_numpy()
    cl = g.c.astype(float).to_numpy()
    op = g.o.astype(float).to_numpy()
    atr = g.atr14.astype(float).replace(0, np.nan).to_numpy()
    ma7 = g.ma7.astype(float).to_numpy()
    ma20 = g.ma20.astype(float).to_numpy()
    ma100 = g.ma100.astype(float).to_numpy()

    names = [
        "retry_first_high_age", "retry_pullback_age", "retry_second_high_age",
        "retry_first_to_pullback_bars", "retry_pullback_to_second_bars",
        "retry_first_to_second_bars", "retry_first_high_atr_above_pullback",
        "retry_second_recovery_fraction", "retry_second_shortfall_pct",
        "retry_second_shortfall_atr", "retry_post_second_shortfall_pct",
        "retry_post_second_shortfall_atr", "retry_lower_high_count",
        "retry_local_high_count", "retry_lower_high_fraction",
        "retry_local_high_slope_atr_per_bar", "retry_local_high_step_mean_atr",
        "retry_local_high_step_last_atr", "retry_sequence_span_bars",
    ]
    a = {x: np.full(n, np.nan) for x in names}
    a["retry_lower_high_count"] = np.zeros(n)
    a["retry_local_high_count"] = np.zeros(n)

    for i in range(n):
        start = max(0, i - LOOKBACK + 1)
        # Leave enough known bars after the first peak to form pullback/retry.
        first_end = i - MIN_POST_PEAK_BARS
        if first_end < start:
            continue
        j = start + int(np.nanargmax(hi[start : first_end + 1]))
        if j + 2 > i:
            continue
        # The deepest observed pullback after the first high; second retry is
        # the highest observed high strictly after that trough.
        k = j + 1 + int(np.nanargmin(lo[j + 1 : i])) if j + 1 < i else -1
        if k < 0 or k + 1 > i:
            continue
        m = k + 1 + int(np.nanargmax(hi[k + 1 : i + 1]))
        first, trough, second = hi[j], lo[k], hi[m]
        denom = first - trough
        av = atr[i]
        a["retry_first_high_age"][i] = i - j
        a["retry_pullback_age"][i] = i - k
        a["retry_second_high_age"][i] = i - m
        a["retry_first_to_pullback_bars"][i] = k - j
        a["retry_pullback_to_second_bars"][i] = m - k
        a["retry_first_to_second_bars"][i] = m - j
        a["retry_first_high_atr_above_pullback"][i] = denom / av if np.isfinite(av) else np.nan
        a["retry_second_recovery_fraction"][i] = (second - trough) / denom if denom > 0 else np.nan
        a["retry_second_shortfall_pct"][i] = (first - second) / first if first else np.nan
        a["retry_second_shortfall_atr"][i] = (first - second) / av if np.isfinite(av) else np.nan
        a["retry_post_second_shortfall_pct"][i] = (second - hi[i]) / second if second else np.nan
        a["retry_post_second_shortfall_atr"][i] = (second - hi[i]) / av if np.isfinite(av) else np.nan
        a["retry_sequence_span_bars"][i] = i - j

        # Confirmed local highs require only the following already-known bar.
        piv = [p for p in range(j, i) if p > 0 and hi[p] >= hi[p - 1] and hi[p] > hi[p + 1]]
        if i > 0 and hi[i] >= hi[i - 1]:
            piv.append(i)  # live, unconfirmed endpoint is still PIT-observable
        vals = np.asarray([hi[p] for p in piv], float)
        poss = np.asarray(piv, float)
        a["retry_local_high_count"][i] = len(vals)
        if len(vals) >= 2:
            steps = np.diff(vals)
            a["retry_lower_high_count"][i] = int((steps < 0).sum())
            a["retry_lower_high_fraction"][i] = float((steps < 0).mean())
            if np.isfinite(av):
                a["retry_local_high_slope_atr_per_bar"][i] = np.polyfit(poss, vals, 1)[0] / av
                a["retry_local_high_step_mean_atr"][i] = steps.mean() / av
                a["retry_local_high_step_last_atr"][i] = steps[-1] / av

    out = pd.DataFrame({"code": g.code.astype(str), "ymd": g.ymd.astype(str), **a})
    out["retry_sequence_available"] = out.retry_second_high_age.notna()
    # Existing context columns are copied/read-only; no composite score/flag.
    out["existing_above_ma100_run"] = _run_length((cl > ma100) & np.isfinite(ma100))
    out["existing_bull_body_atr"] = (cl - op) / atr
    out["existing_gap_pct"] = op / np.roll(cl, 1) - 1.0
    out.loc[0, "existing_gap_pct"] = np.nan
    out["existing_cross_ma7"] = g.cross_ma7.astype(float)
    out["existing_cross_ma20"] = g.cross_ma20.astype(float)
    out["existing_ma7_slope5_atr"] = g.ma7_slope5_atr.astype(float)
    return out


def _same(left: pd.DataFrame, right: pd.DataFrame) -> bool:
    left, right = left.reset_index(drop=True), right.reset_index(drop=True)
    if list(left.columns) != list(right.columns) or len(left) != len(right):
        return False
    for col in left:
        if pd.api.types.is_numeric_dtype(left[col]):
            if not np.allclose(left[col], right[col], equal_nan=True, rtol=0, atol=1e-12):
                return False
        elif not left[col].fillna("<NA>").equals(right[col].fillna("<NA>")):
            return False
    return True


def self_tests(raw: pd.DataFrame, full: pd.DataFrame) -> dict:
    probes = [("6326", "20260304"), ("9064", "20230907"), ("7203", "20240115")]
    cutoff_results = []
    mutation_results = []
    for code, cutoff in probes:
        src = raw[raw.code.astype(str).eq(code)].sort_values("ymd").reset_index(drop=True)
        if src.empty or not src.ymd.astype(str).le(cutoff).any():
            continue
        truncated = src[src.ymd.astype(str).le(cutoff)].copy()
        regen = build_code(truncated)
        expected = full[(full.code.eq(code)) & (full.ymd.le(cutoff))]
        cutoff_results.append({"code": code, "cutoff": cutoff, "pass": _same(expected, regen), "rows": len(regen)})
        mutated = src.copy()
        future = mutated.ymd.astype(str).gt(cutoff)
        mutated.loc[future, ["o", "h", "l", "c", "v"]] = mutated.loc[future, ["o", "h", "l", "c", "v"]] * 7.0 + 13.0
        mut_features = build_code(mutated)
        mutation_results.append({
            "code": code, "cutoff": cutoff,
            "pass": _same(expected, mut_features[mut_features.ymd.le(cutoff)]),
            "mutated_future_rows": int(future.sum()),
        })
    ref = full[(full.code.eq("6326")) & (full.ymd.eq("20260304"))]
    ref_ok = len(ref) == 1 and bool(ref.retry_sequence_available.iloc[0])
    return {
        "cutoff_regeneration": cutoff_results,
        "future_mutation": mutation_results,
        "kubota_6326_20260304": {"pass": ref_ok, "row": ref.to_dict("records")},
        "all_pass": bool(ref_ok and all(x["pass"] for x in cutoff_results + mutation_results)),
    }


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--source", type=Path, default=DEFAULT_SOURCE)
    ap.add_argument("--output", type=Path, required=True)
    args = ap.parse_args()
    args.output.mkdir(parents=True, exist_ok=True)
    raw = pd.read_parquet(args.source)
    raw["ymd"] = raw.ymd.astype(str)
    full = pd.concat([build_code(g) for _, g in raw.groupby("code", sort=False)], ignore_index=True)
    tests = self_tests(raw, full)
    if not tests["all_pass"]:
        raise RuntimeError(json.dumps(tests, default=str))
    ledger = args.output / "retry_failure_sequence_features.parquet"
    full.to_parquet(ledger, index=False)
    grid_columns = [
        "retry_first_high_age", "retry_first_to_pullback_bars",
        "retry_pullback_to_second_bars", "retry_first_to_second_bars",
        "retry_first_high_atr_above_pullback", "retry_second_recovery_fraction",
        "retry_second_shortfall_pct", "retry_second_shortfall_atr",
        "retry_lower_high_fraction", "retry_local_high_slope_atr_per_bar",
        "retry_local_high_step_mean_atr", "retry_sequence_span_bars",
    ]
    pre = full[full.ymd.le("20221230") & full.retry_sequence_available]
    quantiles = [0.10, 0.20, 0.30, 0.40, 0.50, 0.60, 0.70, 0.80, 0.90]
    grid = {
        "schema_version": "tradex_retry_failure_preperiod_grid_v1",
        "construction_period": {"start": full.ymd.min(), "end": "20221230"},
        "outcomes_used": False,
        "rule": "candidate cutpoints are marginal quantiles from construction period only; effectiveness selection must occur later under fixed OOS conditions",
        "rows": int(len(pre)),
        "quantiles": quantiles,
        "features": {
            col: {
                "non_missing": int(pre[col].notna().sum()),
                "cutpoints": [float(x) for x in pre[col].quantile(quantiles).drop_duplicates().to_list()],
            }
            for col in grid_columns
        },
    }
    grid_path = args.output / "preperiod_threshold_grid.json"
    grid_path.write_text(json.dumps(grid, ensure_ascii=False, indent=2), encoding="utf-8")
    audit = {
        "schema_version": "tradex_nikkei225_retry_failure_sequence_v1",
        "research_only": True,
        "new_axis": "retry_failure_sequence_only",
        "lookback": LOOKBACK,
        "rows": len(full), "codes": int(full.code.nunique()),
        "date_min": full.ymd.min(), "date_max": full.ymd.max(),
        "source": str(args.source), "source_sha256": _sha(args.source),
        "ledger": str(ledger), "ledger_sha256": _sha(ledger),
        "preperiod_threshold_grid": str(grid_path),
        "preperiod_threshold_grid_sha256": _sha(grid_path),
        "feature_columns": [c for c in full if c.startswith("retry_")],
        "existing_read_only_columns": [c for c in full if c.startswith("existing_")],
        "selection_or_score_emitted": False,
        "pit_contract": "each row uses only same-code bars with ymd <= row ymd",
        "tests": tests,
    }
    audit_path = args.output / "audit.json"
    audit_path.write_text(json.dumps(audit, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    complete = {
        "complete": True, "audit_sha256": _sha(audit_path),
        "ledger_sha256": _sha(ledger), "preperiod_threshold_grid_sha256": _sha(grid_path),
    }
    (args.output / "complete.json").write_text(json.dumps(complete, indent=2), encoding="utf-8")


if __name__ == "__main__":
    main()
