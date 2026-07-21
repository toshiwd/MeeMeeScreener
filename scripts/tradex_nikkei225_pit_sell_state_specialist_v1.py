from __future__ import annotations

import argparse
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd


AXIS_ID = "tradex_nikkei225_pit_sell_state_specialist_v1"
DEFAULT_DAILY = Path(r"G:\Tradex\tradex_nikkei225_daily_assessment_feature_ledger_v1\20260714T040047Z-tradex_nikkei225_daily_assessment_feature_ledger_v1\daily_assessment_features.parquet")
DEFAULT_RETRY = Path(r"G:\Tradex\retryseq_v1\20260715T-retry_failure_sequence_v1\retry_failure_sequence_features.parquet")
DEFAULT_CONTRACT = Path(r"G:\Tradex\sell_transition_annotation_v1\20260714T151842Z-tradex_sell_transition_annotation_v1\contract.json")
DEFAULT_OUT = Path(r"G:\Tradex\pit_sell_state_specialist_v1")
PRE_START, PRE_END = 20190101, 20211231


def sha(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for block in iter(lambda: f.read(8 << 20), b""):
            h.update(block)
    return h.hexdigest()


def dump(path: Path, payload: Any) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str) + "\n", encoding="utf-8")


def q(series: pd.Series, value: float) -> float:
    x = pd.to_numeric(series, errors="coerce").replace([np.inf, -np.inf], np.nan).dropna()
    if x.empty:
        raise ValueError(f"empty preperiod quantile input: {series.name}")
    return float(x.quantile(value))


def load_inputs(daily_path: Path, retry_path: Path) -> pd.DataFrame:
    daily = pd.read_parquet(daily_path).copy()
    retry = pd.read_parquet(retry_path).copy()
    daily["code"] = daily.code.astype(str).str.replace(r"\.0$", "", regex=True).str.zfill(4)
    retry["code"] = retry.code.astype(str).str.replace(r"\.0$", "", regex=True).str.zfill(4)
    daily["ymd"] = pd.to_numeric(daily.ymd, errors="raise").astype("int32")
    retry["ymd"] = pd.to_numeric(retry.ymd, errors="raise").astype("int32")
    retry_cols = [c for c in retry.columns if c not in {"code", "ymd"} and not c.startswith("existing_")]
    existing = ["existing_above_ma100_run", "existing_gap_pct"]
    retry_cols += [c for c in existing if c in retry]
    out = daily.merge(retry[["code", "ymd", *retry_cols]], on=["code", "ymd"], how="left", validate="one_to_one")
    return out.sort_values(["code", "ymd"]).reset_index(drop=True)


def preperiod_thresholds(frame: pd.DataFrame) -> dict[str, Any]:
    pre = frame.loc[frame.ymd.between(PRE_START, PRE_END)].copy()
    pre["dist_ma100_atr"] = (pre.c - pre.ma100) / pre.atr14.replace(0, np.nan)
    values = {
        "pos20_q75": q(pre.pos20, .75),
        "pre_ret10_q75": q(pre.pre_ret10, .75),
        "above_ma100_run_q75": q(pre.existing_above_ma100_run, .75),
        "dist_ma100_atr_q75": q(pre.dist_ma100_atr, .75),
        "dist_ma60_atr_q75": q(pre.dist_ma60_atr, .75),
        "retry_prominence_q50": q(pre.retry_first_high_atr_above_pullback, .50),
        "retry_recovery_q50": q(pre.retry_second_recovery_fraction, .50),
        "upper_supply_count5_q75": q(pre.upper_supply_count5, .75),
        "bear_count5_q75": q(pre.bear_count5, .75),
        "gap_pct_q25": q(pre.existing_gap_pct, .25),
        "strong_bull_body_ratio_q75": q(pre.body_ratio.where(pre.body_ratio > 0), .75),
        "close_pos_q75": q(pre.close_pos, .75),
        "upper_wick_ratio_q75": q(pre.upper_wick_ratio, .75),
        "lower_wick_ratio_q75": q(pre.lower_wick_ratio, .75),
        "volume_ratio20_q75": q(pre.volume_ratio20, .75),
        "bull_body_atr_q95": q(((pre.c - pre.o) / pre.atr14.replace(0, np.nan)).where(pre.c > pre.o), .95),
    }
    return {
        "selection_policy": "outcome_free_empirical_quantiles_on_2019_2021_only",
        "preperiod": [PRE_START, PRE_END],
        "row_count": int(len(pre)),
        "code_count": int(pre.code.nunique()),
        "values": values,
        "future_outcome_columns_used": [],
    }


def raw_bull_erasure_retry(frame: pd.DataFrame, thresholds: dict[str, Any]) -> pd.DataFrame:
    """PIT W-top proxy anchored on a large bullish body and a multi-bear retracement.

    Every emitted confirmation uses bars at or before that row.  Fixed semantic
    ratios are deliberately not selected against outcomes.
    """
    impulse_cut = float(thresholds["values"]["bull_body_atr_q95"])
    out: list[pd.DataFrame] = []
    for code, g in frame.groupby("code", sort=False):
        g = g.reset_index(drop=True)
        n = len(g); oo = g.o.to_numpy(float); hh = g.h.to_numpy(float); ll = g.l.to_numpy(float); cc = g.c.to_numpy(float)
        aa = g.atr14.to_numpy(float); yy = g.ymd.to_numpy(int); body_atr = (cc - oo) / np.where(aa == 0, np.nan, aa)
        candidate = np.zeros(n, dtype=bool); impulse_ymd = np.full(n, np.nan); valley_ymd = np.full(n, np.nan); retry_ymd = np.full(n, np.nan)
        retrace_arr = np.full(n, np.nan); recovery_arr = np.full(n, np.nan); shortfall_arr = np.full(n, np.nan); bear_arr = np.zeros(n, dtype=np.int16)
        for j in np.flatnonzero(np.isfinite(body_atr) & (body_atr >= impulse_cut)):
            impulse_body = cc[j] - oo[j]
            if impulse_body <= 0 or j + 4 >= n: continue
            peak_end = min(n - 1, j + 1); peak_pos = j + int(np.argmax(hh[j:peak_end + 1])); first_peak = hh[peak_pos]
            for k in range(max(j + 2, peak_pos + 1), min(n - 2, j + 13)):
                retrace = (cc[j] - cc[k]) / impulse_body; bear_count = int(np.sum(cc[j + 1:k + 1] < oo[j + 1:k + 1]))
                if retrace < .50 or bear_count < 2: continue
                for i in range(k + 2, min(n, j + 21)):
                    m = k + 1 + int(np.argmax(hh[k + 1:i])); second_high = hh[m]; decline = first_peak - ll[k]
                    recovery = np.nan if decline <= 0 else (second_high - ll[k]) / decline
                    if hh[i] < second_high and cc[i] < cc[m] and np.isfinite(recovery) and recovery >= .50 and second_high < first_peak:
                        candidate[i] = True; impulse_ymd[i] = yy[j]; valley_ymd[i] = yy[k]; retry_ymd[i] = yy[m]
                        retrace_arr[i] = retrace; recovery_arr[i] = recovery; shortfall_arr[i] = (first_peak - second_high) / aa[i]; bear_arr[i] = bear_count
        out.append(pd.DataFrame({"code": code, "ymd": yy, "bull_erasure_retry_candidate": candidate,
            "erasure_impulse_ymd": impulse_ymd, "erasure_valley_ymd": valley_ymd, "erasure_retry_ymd": retry_ymd,
            "erasure_retrace_fraction": retrace_arr, "erasure_retry_recovery_fraction": recovery_arr,
            "erasure_retry_shortfall_atr": shortfall_arr, "erasure_bear_count": bear_arr}))
    return pd.concat(out, ignore_index=True)


def build_state_ledger(frame: pd.DataFrame, thresholds: dict[str, Any]) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, Any]]:
    t = thresholds["values"]
    x = frame.copy()
    atr = x.atr14.replace(0, np.nan)
    x["dist_ma100_atr"] = (x.c - x.ma100) / atr
    position = (x.pos20 >= t["pos20_q75"]) | x.monthly_high_failure.fillna(False).astype(bool)
    maturity = x.existing_above_ma100_run >= t["above_ma100_run_q75"]
    extension = ((x.pre_ret10 >= t["pre_ret10_q75"]) | (x.dist_ma100_atr >= t["dist_ma100_atr_q75"]) | (x.dist_ma60_atr >= t["dist_ma60_atr_q75"]))
    x["s1_candidate"] = (position.astype(int) + maturity.astype(int) + extension.astype(int)) >= 2
    retry = (
        x.retry_sequence_available.fillna(False).astype(bool)
        & (x.retry_first_high_atr_above_pullback >= t["retry_prominence_q50"])
        & (x.retry_second_recovery_fraction >= t["retry_recovery_q50"])
        & (x.retry_second_shortfall_atr > 0)
        & (x.retry_local_high_slope_atr_per_bar <= 0)
    )
    x["retry_failure_candidate"] = retry
    erasure = raw_bull_erasure_retry(x, thresholds)
    x = x.merge(erasure, on=["code", "ymd"], validate="one_to_one")
    weakening = (
        x.cross_ma7.fillna(False).astype(bool)
        | (x.upper_supply_count5 >= t["upper_supply_count5_q75"])
        | (x.bear_count5 >= t["bear_count5_q75"])
    )
    x["weakening_candidate"] = weakening
    x["trigger_gap_down"] = x.existing_gap_pct <= min(0.0, t["gap_pct_q25"])
    x["trigger_ma20_break"] = x.cross_ma20.fillna(False).astype(bool)
    x["trigger_support_break"] = x.support_break.fillna(False).astype(bool)

    records: list[dict[str, Any]] = []
    for code, group in x.groupby("code", sort=False):
        recent_s1: list[int] = []
        recent_s2: list[int] = []
        recent_s3: list[int] = []
        recent_s4: list[int] = []
        last_event_pos = -999
        for pos, (_, row) in enumerate(group.iterrows()):
            recent_s1 = [p for p in recent_s1 if pos - p <= 20]
            recent_s2 = [p for p in recent_s2 if pos - p <= 10]
            recent_s3 = [p for p in recent_s3 if pos - p <= 10]
            recent_s4 = [p for p in recent_s4 if pos - p <= 20]
            prior_s1 = bool(recent_s1)
            prior_s2 = bool(recent_s2)
            s1_candidate = bool(row.s1_candidate)
            if s1_candidate:
                recent_s1.append(pos)
            s1 = bool(recent_s1)
            s2_candidate = bool(prior_s1 and (row.retry_failure_candidate or row.bull_erasure_retry_candidate))
            if s2_candidate:
                recent_s2.append(pos)
            s2 = bool(recent_s2)
            s3_candidate = bool(prior_s2 and row.weakening_candidate)
            if s3_candidate:
                recent_s3.append(pos)
            s3 = bool(recent_s3)
            trigger_count = int(bool(row.trigger_gap_down)) + int(bool(row.trigger_ma20_break)) + int(bool(row.trigger_support_break))
            s4_raw = bool(s3 and trigger_count >= 2)
            s4_event = bool(s4_raw and pos - last_event_pos > 10)
            if s4_event:
                last_event_pos = pos
                recent_s4.append(pos)
            state = "S4_SELL_TRIGGER" if s4_raw else "S3_WEAKENING" if s3 else "S2_TOP_FORMATION" if s2 else "S1_TOP_RISK" if s1 else "S0_NO_SETUP"
            records.append({
                "code": code, "ymd": int(row.ymd), "state": state,
                "s1_top_risk": s1, "s2_top_formation": s2, "s3_weakening": s3,
                "s1_candidate_today": s1_candidate, "s2_candidate_today": s2_candidate, "s3_candidate_today": s3_candidate,
                "bull_erasure_retry_candidate": bool(row.bull_erasure_retry_candidate),
                "erasure_impulse_ymd": row.erasure_impulse_ymd, "erasure_valley_ymd": row.erasure_valley_ymd,
                "erasure_retry_ymd": row.erasure_retry_ymd, "erasure_retrace_fraction": row.erasure_retrace_fraction,
                "erasure_retry_recovery_fraction": row.erasure_retry_recovery_fraction,
                "erasure_retry_shortfall_atr": row.erasure_retry_shortfall_atr, "erasure_bear_count": int(row.erasure_bear_count),
                "s4_sell_trigger_raw": s4_raw, "s4_sell_trigger_event": s4_event,
                "trigger_group_count": trigger_count,
                "trigger_gap_down": bool(row.trigger_gap_down),
                "trigger_ma20_break": bool(row.trigger_ma20_break),
                "trigger_support_break": bool(row.trigger_support_break),
                "prior_s1_within20": bool(recent_s1), "prior_s2_within10": bool(recent_s2),
                "sequence_incomplete": bool((row.retry_failure_candidate and not s1) or (row.weakening_candidate and not s2)),
            })
    states = pd.DataFrame(records)
    duplicated_state_columns = [c for c in states.columns if c in x.columns and c not in {"code", "ymd"}]
    z = x.drop(columns=duplicated_state_columns).merge(states, on=["code", "ymd"], validate="one_to_one")
    near60 = z.dist_ma60_atr.abs() <= .35
    near100 = ((z.c - z.ma100) / z.atr14.replace(0, np.nan)).abs() <= .35
    near_support = ((z.c - z.support20) / z.atr14.replace(0, np.nan)).between(0, .35)
    strong_bull = (z.body_ratio >= t["strong_bull_body_ratio_q75"]) & (z.close_pos >= t["close_pos_q75"])
    lanes = pd.DataFrame({
        "code": z.code, "ymd": z.ymd,
        "sell_add_retry_failure": z.retry_failure_candidate,
        "sell_add_bull_erasure_retry": z.bull_erasure_retry_candidate,
        "sell_add_gap_down": z.trigger_gap_down,
        "sell_add_ma20_break": z.trigger_ma20_break,
        "sell_add_support_break": z.trigger_support_break,
        "sell_deduct_ma7_rising": z.ma7_slope5_atr > 0,
        "sell_deduct_strong_bull_recovery": strong_bull,
        "sell_deduct_short_ma_reclaim": z.reclaim_ma7.fillna(False).astype(bool) | z.reclaim_ma20.fillna(False).astype(bool),
        "sell_deduct_downside_overextension": z.oversold_risk.fillna(False).astype(bool),
        "rebound_risk_ma60_touch": near60,
        "rebound_risk_ma100_touch": near100,
        "rebound_risk_prior_support_touch": near_support,
        "rebound_risk_long_lower_wick": z.lower_wick_ratio >= t["lower_wick_ratio_q75"],
        "rebound_risk_volume_bull_response": strong_bull & (z.volume_ratio20 >= t["volume_ratio20_q75"]),
        "rebound_risk_oversold": z.oversold_risk.fillna(False).astype(bool),
        "profit_take_boundary_not_instrumented": True,
    })
    boundary = {
        "support_to_resistance": "boundary_not_instrumented_broken_level_anchor_not_retained",
        "insufficient_rebound": "boundary_not_instrumented_rebound_leg_not_retained",
        "multi_bearish_erasure": "instrumented_raw_ohlc_pit_large_bull_retrace_retry_confirmation",
        "s9_invalidation": "boundary_not_instrumented_resistance_hold_anchor_not_retained",
        "profit_take": "boundary_not_instrumented_no_virtual_entry_or_position_lifecycle",
    }
    return states, lanes, boundary


def seed_replay(states: pd.DataFrame, lanes: pd.DataFrame) -> dict[str, Any]:
    joined = states.merge(lanes, on=["code", "ymd"], validate="one_to_one")
    expected = {
        "6326": {20260212: "S1_TOP_RISK", 20260302: "S2_TOP_FORMATION", 20260303: "S3_WEAKENING", 20260304: "S4_SELL_TRIGGER"},
    }
    rows = []
    for code, dates in expected.items():
        for ymd, user_state in dates.items():
            hit = joined[(joined.code == code) & (joined.ymd == ymd)]
            rows.append({
                "code": code, "ymd": ymd, "user_annotation": user_state,
                "available": bool(len(hit)),
                "engine_state": None if hit.empty else str(hit.iloc[0].state),
                "s4_event": None if hit.empty else bool(hit.iloc[0].s4_sell_trigger_event),
                "policy": "diagnostic_only_not_used_for_threshold_or_keep",
            })
    exact = all(r["available"] and r["engine_state"] == r["user_annotation"] for r in rows)
    return {"rows": rows, "excluded": [{"code": "9962", "reason": "out_of_nikkei225_universe_in_authoritative_daily_ledger", "future_research": "full_universe_only"}], "all_available": all(r["available"] for r in rows), "exact_state_fidelity": exact, "pass": exact}


def run(daily: Path, retry: Path, contract: Path, out_root: Path) -> Path:
    frame = load_inputs(daily, retry)
    thresholds = preperiod_thresholds(frame)
    states, lanes, boundary = build_state_ledger(frame, thresholds)
    seed = seed_replay(states, lanes)
    case = frame[frame.code.eq("6326")].copy().reset_index(drop=True)
    cutoff = 20260304
    cutoff_states, _, _ = build_state_ledger(case[case.ymd <= cutoff].copy(), thresholds)
    full_prefix = states[states.code.eq("6326") & states.ymd.le(cutoff)].reset_index(drop=True)
    compare_cols = ["code", "ymd", "state", "s1_top_risk", "s2_top_formation", "s3_weakening", "s4_sell_trigger_raw", "bull_erasure_retry_candidate"]
    cutoff_exact = full_prefix[compare_cols].equals(cutoff_states[compare_cols].reset_index(drop=True))
    mutated = case.copy()
    future = mutated.ymd > cutoff
    for col in ("o", "h", "l", "c"):
        mutated.loc[future, col] = mutated.loc[future, col] * (3.0 if col in {"h", "c"} else .2)
    mutated_states, _, _ = build_state_ledger(mutated, thresholds)
    mutation_exact = full_prefix[compare_cols].equals(mutated_states[mutated_states.ymd <= cutoff][compare_cols].reset_index(drop=True))
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    out = out_root / f"{stamp}-{AXIS_ID}-state"
    out.mkdir(parents=True, exist_ok=False)
    paths = {
        "state_ledger": out / "state_ledger.parquet",
        "lane_evidence_ledger": out / "lane_evidence_ledger.parquet",
        "thresholds": out / "state_thresholds_preperiod.json",
        "seed_replay": out / "seed_replay_2026.json",
        "contract": out / "contract.json",
    }
    states.to_parquet(paths["state_ledger"], index=False)
    lanes.to_parquet(paths["lane_evidence_ledger"], index=False)
    dump(paths["thresholds"], thresholds)
    dump(paths["seed_replay"], seed)
    payload = {
        "schema_version": AXIS_ID + ".state.v1", "artifact_role": "authoritative_state_engine_review_only",
        "source": {"daily": {"path": str(daily), "sha256": sha(daily)}, "retry": {"path": str(retry), "sha256": sha(retry)}, "sell_contract": {"path": str(contract), "sha256": sha(contract)}},
        "fixed_policy": {"state_order": ["S1_TOP_RISK", "S2_TOP_FORMATION", "S3_WEAKENING", "S4_SELL_TRIGGER"], "s1_lookback": 20, "s2_lookback": 10, "s4_independent_trigger_groups_required": 2, "event_dedup_bars": 10, "threshold_selection": thresholds["selection_policy"], "outcome_columns_used_for_state_or_thresholds": []},
        "boundary_instrumentation": boundary,
        "counts": {"rows": int(len(states)), "codes": int(states.code.nunique()), "states": states.state.value_counts().to_dict(), "s4_raw": int(states.s4_sell_trigger_raw.sum()), "s4_events": int(states.s4_sell_trigger_event.sum()), "sequence_incomplete": int(states.sequence_incomplete.sum())},
        "non_symmetry": ["sell_deduct_is_not_buy_add", "rebound_risk_is_not_buy_signal", "profit_take_is_not_buy_signal", "no_signed_combined_score"],
        "boundary": {"owner": "TRADEX", "meemee_changed": False, "runtime_db_write": False, "production_ranking_changed": False},
    }
    dump(paths["contract"], payload)
    artifacts = {name: {"path": str(path), "sha256": sha(path)} for name, path in paths.items()}
    audit = {"complete": True, "artifacts": artifacts, "checks": {"row_alignment": len(states) == len(frame) == len(lanes), "unique_keys": not states.duplicated(["code", "ymd"]).any(), "preperiod_ends_2021": thresholds["preperiod"][1] == 20211231, "no_outcome_thresholds": thresholds["future_outcome_columns_used"] == [], "s4_requires_s3": bool((~states.s4_sell_trigger_raw | states.s3_weakening).all()), "s4_two_groups": bool((~states.s4_sell_trigger_raw | (states.trigger_group_count >= 2)).all()), "cutoff_regeneration_exact": bool(cutoff_exact), "future_mutation_prefix_unchanged": bool(mutation_exact), "seed_replay_fidelity": bool(seed["pass"]), "s4_breadth_nonzero": int(states.s4_sell_trigger_event.sum()) > 0}}
    dump(out / "audit.json", audit)
    dump(out / "_ARTIFACT_COMPLETE.json", {"complete": all(audit["checks"].values()), "contract": str(paths["contract"]), "contract_sha256": sha(paths["contract"]), "audit": str(out / "audit.json"), "audit_sha256": sha(out / "audit.json")})
    return out


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--daily", type=Path, default=DEFAULT_DAILY)
    ap.add_argument("--retry", type=Path, default=DEFAULT_RETRY)
    ap.add_argument("--contract", type=Path, default=DEFAULT_CONTRACT)
    ap.add_argument("--output-root", type=Path, default=DEFAULT_OUT)
    args = ap.parse_args()
    print(run(args.daily, args.retry, args.contract, args.output_root))


if __name__ == "__main__":
    main()
