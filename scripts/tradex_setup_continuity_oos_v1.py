"""One-axis OOS challenger: require family-specific setup continuity for core/add."""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path

import numpy as np
import pandas as pd

import tradex_monthly_env_probe_add_oos_v1 as base


YEARS = (2023, 2024, 2025)


def sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def lifecycle_continuity(frame: pd.DataFrame) -> pd.DataFrame:
    out = []
    for _, g0 in frame.groupby("code", sort=False):
        g = g0.copy().reset_index(drop=True)
        stage = 0
        age = 999
        last_action = -99
        running_low = np.nan
        entry = np.nan
        current_family = "NONE"
        probe, add1, add2, status, families = [], [], [], [], []
        for i, row in g.iterrows():
            p = a1 = a2 = False
            if stage == 0 and bool(row.probe_allowed):
                p = True
                stage, age, last_action = 1, 0, i
                running_low, entry = float(row.l), float(row.c)
                current_family = str(row.probe_family)
            elif stage > 0:
                age += 1
                prior_low = running_low
                running_low = min(running_low, float(row.l))
                invalid = float(row.c) > max(float(row.ma20), entry + .8 * float(row.atr14)) or age > 20
                if invalid:
                    stage, age, last_action = 0, 999, -99
                    running_low, entry, current_family = np.nan, np.nan, "NONE"
                else:
                    new_low_close = float(row.c) < prior_low
                    gd_break = float(row.gap_pct) < -.005 and float(row.l) < prior_low
                    eligible = (not bool(row.room_veto)) and (not bool(row.rejection_veto))
                    box_core = (
                        current_family == "BOX_CEILING_ERASURE"
                        and age <= 2 and float(row.c) < entry and float(row.c) < float(row.ma20)
                    )
                    up_core = current_family == "UPTREND_TOP_FAILED_TRY" and (
                        bool(row.failed_try) or bool(row.strong_retry_failure)
                    )
                    down_core = current_family == "DOWNTREND_SUPPORT_BREAK" and bool(row.support_break)
                    core_continuity = box_core or up_core or down_core
                    original_core = box_core or (
                        eligible and (new_low_close or gd_break or bool(row.support_break))
                    )
                    if stage == 1 and i - last_action >= 1 and original_core and core_continuity:
                        a1, stage, last_action = True, 2, i
                    elif stage == 2 and i - last_action >= 2:
                        prior = g.iloc[i - 1]
                        box_add = (
                            current_family == "BOX_CEILING_ERASURE"
                            and float(prior.c) >= float(prior.ma7)
                            and float(row.c) < float(row.o)
                            and float(row.c) < float(row.ma7)
                            and float(row.upper_wick_ratio) >= .25
                        )
                        up_add = current_family == "UPTREND_TOP_FAILED_TRY" and (
                            bool(row.failed_try) or bool(row.strong_retry_failure)
                        ) and float(row.close_pos) <= .35
                        down_add = current_family == "DOWNTREND_SUPPORT_BREAK" and bool(row.support_break) and float(row.close_pos) <= .35
                        if eligible and (box_add or up_add or down_add):
                            a2, stage, last_action = True, 3, i
            probe.append(p)
            add1.append(a1)
            add2.append(a2)
            status.append(stage)
            families.append(current_family)
        g["probe_event"] = probe
        g["add1_event"] = add1
        g["add2_event"] = add2
        g["position_stage"] = status
        g["position_family"] = families
        out.append(g)
    return pd.concat(out, ignore_index=True)


def rates(x: pd.DataFrame, event_col: str) -> dict:
    out = {}
    for year in YEARS:
        z = x[x.dt.dt.year.eq(year) & x[event_col]].copy()
        out[str(year)] = {
            "events": int(len(z)),
            "codes": int(z.code.nunique()),
            "h5_down_first": None if z.empty else float(z.label_5.eq(0).mean()),
            "h5_rebound_first": None if z.empty else float(z.label_5.eq(1).mean()),
            "h5_neutral": None if z.empty else float((~z.label_5.isin([0, 1])).mean()),
        }
    return out


def anchor_rows(x: pd.DataFrame) -> list[dict]:
    anchors = {
        "6532": {20230623, 20230626, 20230704},
        "6526": {20250917, 20251002, 20251009, 20251014},
        "6702": {20250307, 20250310, 20250311, 20250313},
    }
    z = x[x.apply(lambda r: str(r.code).zfill(4) in anchors and int(r.ymd) in anchors[str(r.code).zfill(4)], axis=1)]
    cols = ["code", "ymd", "probe_raw", "probe_event", "add1_event", "add2_event", "position_stage", "position_family"]
    return z[cols].where(pd.notna(z[cols]), None).to_dict("records")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", type=Path, required=True)
    ap.add_argument("--retry-features", type=Path, required=True)
    ap.add_argument("--output", type=Path, required=True)
    args = ap.parse_args()
    args.output.mkdir(parents=True, exist_ok=False)

    raw = pd.read_parquet(args.input).sort_values(["code", "ymd"]).reset_index(drop=True)
    retry = pd.read_parquet(args.retry_features)
    retry["ymd"] = pd.to_numeric(retry.ymd, errors="raise").astype(int)
    retry["code"] = retry.code.astype(str)
    retry_cols = ["code", "ymd", "retry_sequence_available", "retry_second_recovery_fraction", "retry_second_shortfall_atr", "retry_local_high_slope_atr_per_bar", "existing_above_ma100_run"]
    raw = raw.merge(retry[retry_cols], on=["code", "ymd"], how="left", validate="one_to_one")
    monthly = base.monthly_environment(raw)
    joined = base.add_daily_features(raw, monthly)
    baseline = base.lifecycle(joined)
    challenger = lifecycle_continuity(joined)
    baseline_labeled, _ = base.evaluate(baseline)
    challenger_labeled, _ = base.evaluate(challenger)

    base_rates = {k: rates(baseline_labeled, c) for k, c in (("core", "add1_event"), ("add2", "add2_event"))}
    challenger_rates = {k: rates(challenger_labeled, c) for k, c in (("core", "add1_event"), ("add2", "add2_event"))}
    h5_all_years = all(
        challenger_rates["core"][str(y)]["events"] >= 30
        and challenger_rates["core"][str(y)]["h5_down_first"] > challenger_rates["core"][str(y)]["h5_rebound_first"]
        for y in YEARS
    )
    anchor_before = anchor_rows(baseline_labeled)
    anchor_after = anchor_rows(challenger_labeled)
    payload = {
        "schema_version": "tradex_setup_continuity_oos_v1.compare.v1",
        "artifact_role": "authoritative",
        "axis": "family-specific current setup continuity for core and add2",
        "fixed_conditions": {
            "universe": "same Nikkei225 feature ledger",
            "oos_years": list(YEARS),
            "probe": "unchanged",
            "environment": "unchanged",
            "position_invalidation": "unchanged",
            "outcome": "existing ATR first-passage order, t+1 through h5",
            "costs": "ignored per project rule",
        },
        "baseline": base_rates,
        "challenger": challenger_rates,
        "event_count_delta": {
            kind: {str(y): challenger_rates[kind][str(y)]["events"] - base_rates[kind][str(y)]["events"] for y in YEARS}
            for kind in ("core", "add2")
        },
        "human_anchors": {"baseline": anchor_before, "challenger": anchor_after},
        "observed_branching": {
            "changed_core_event_count": int(baseline_labeled.add1_event.sum() - challenger_labeled.add1_event.sum()),
            "changed_add2_event_count": int(baseline_labeled.add2_event.sum() - challenger_labeled.add2_event.sum()),
            "selection_divergence_reason": "core/add2 now require family-specific current structural evidence",
        },
        "judgment": {
            "decision": "keep" if h5_all_years else "drop",
            "h5_down_exceeds_rebound_all_years": h5_all_years,
            "anchor_6532_preserved": any(r["code"] == "6532" and r["ymd"] == 20230626 and r["add1_event"] for r in anchor_after),
            "false_core_6526_removed": not any(r["code"] == "6526" and r["ymd"] == 20251002 and r["add1_event"] for r in anchor_after),
            "false_add2_6702_removed": not any(r["code"] == "6702" and r["ymd"] == 20250313 and r["add2_event"] for r in anchor_after),
            "reason": "keep only if core h5 down-first exceeds rebound-first with >=30 events in every OOS year",
        },
        "not_changed": ["probe trigger", "monthly environment", "position invalidation", "MeeMee", "ranking", "runtime DB"],
    }
    compare = args.output / "compare.json"
    compare.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    event_cols = ["code", "ymd", "environment", "probe_family", "position_family", "probe_raw", "probe_event", "add1_event", "add2_event", "position_stage", "label_5"]
    challenger_labeled[event_cols].to_parquet(args.output / "challenger_event_ledger.parquet", index=False)
    audit = {
        "rows": int(len(challenger_labeled)), "codes": int(challenger_labeled.code.nunique()),
        "duplicate_code_ymd": int(challenger_labeled.duplicated(["code", "ymd"]).sum()),
        "input_sha256": sha(args.input), "retry_sha256": sha(args.retry_features),
        "future_used_for_selection": False, "review_only": True, "runtime_db_write": False,
    }
    (args.output / "audit.json").write_text(json.dumps(audit, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    (args.output / "_ARTIFACT_COMPLETE.json").write_text(json.dumps({"complete": True, "authoritative": "compare.json", "compare_sha256": sha(compare)}, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({"output": str(args.output), "judgment": payload["judgment"], "event_count_delta": payload["event_count_delta"]}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
