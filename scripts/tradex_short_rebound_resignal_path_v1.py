from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd


def summarize(group: pd.DataFrame) -> dict:
    return {
        "n": int(len(group)),
        "codes": int(group["code"].nunique()),
        "eventual_drop5_rate": float(group["eventual_drop5"].mean()),
        "upward_continuation_rate": float(group["upward_continuation"].mean()),
        "unresolved_rate": float(group["unresolved"].mean()),
        "core_resignal_rate": float(group["core_resignal"].mean()),
        "core_probe_resignal_rate": float(group["core_probe_resignal"].mean()),
        "median_core_probe_resignal_days": (
            None
            if not group["core_probe_resignal"].any()
            else float(group.loc[group["core_probe_resignal"], "core_probe_resignal_days"].median())
        ),
        "core_probe_resignal_drop5_in5_rate": (
            None
            if not group["core_probe_resignal"].any()
            else float(group.loc[group["core_probe_resignal"], "resignal_drop5_in5"].mean())
        ),
        "no_resignal_eventual_drop5_rate": (
            None
            if group["core_probe_resignal"].all()
            else float(group.loc[~group["core_probe_resignal"], "eventual_drop5"].mean())
        ),
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--daily", required=True)
    parser.add_argument("--signals", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    output = Path(args.output)
    output.mkdir(parents=True, exist_ok=False)

    daily = pd.read_parquet(args.daily).sort_values(["code", "bar_index"]).copy()
    signals = pd.read_parquet(args.signals).sort_values(["code", "bar_index"]).copy()
    daily["code"] = daily["code"].astype(str).str.zfill(4)
    signals["code"] = signals["code"].astype(str).str.zfill(4)
    signals = signals[signals["action_tier"].isin(["Core", "Probe"])].copy()

    rows: list[dict] = []
    for code, bars in daily.groupby("code", sort=False):
        bars = bars.reset_index(drop=True)
        index_to_pos = pd.Series(bars.index.values, index=bars["bar_index"]).to_dict()
        code_signals = signals[signals["code"].eq(code)].copy()
        if code_signals.empty:
            continue
        signal_by_pos = {
            int(index_to_pos[row.bar_index]): row
            for row in code_signals.itertuples()
            if row.bar_index in index_to_pos
        }
        core_positions = sorted(
            pos for pos, row in signal_by_pos.items() if row.action_tier == "Core"
        )
        last_anchor = -10_000
        for position in core_positions:
            if position <= last_anchor + 20:
                continue
            anchor = bars.iloc[position]
            rise_day = int(anchor["first_rise3_day"])
            drop_day = int(anchor["first_drop5_day"])
            if not (1 <= rise_day <= 5 and rise_day < drop_day):
                continue
            rebound_position = position + rise_day
            end_position = rebound_position + 20
            if end_position >= len(bars):
                continue
            last_anchor = position
            entry_open = float(anchor["entry_open"])
            future = bars.iloc[rebound_position + 1 : end_position + 1]
            eventual_drop5 = bool(float(future["l"].min()) <= entry_open * 0.95)
            close20_pct = 100 * (float(future.iloc[-1]["c"]) / entry_open - 1)
            upward = bool(not eventual_drop5 and close20_pct >= 3)
            unresolved = bool(not eventual_drop5 and not upward)

            later_signals = [
                (pos, row)
                for pos, row in signal_by_pos.items()
                if rebound_position < pos <= end_position
            ]
            later_signals.sort(key=lambda item: item[0])
            later_core = [(pos, row) for pos, row in later_signals if row.action_tier == "Core"]
            first_any = later_signals[0] if later_signals else None
            first_core = later_core[0] if later_core else None
            resignal_drop = False
            resignal_tier = None
            resignal_days = None
            if first_any is not None:
                resignal_pos, resignal_row = first_any
                resignal_tier = str(resignal_row.action_tier)
                resignal_days = int(resignal_pos - rebound_position)
                resignal_drop = bool(bars.iloc[resignal_pos]["drop5_in5"])
            rows.append(
                {
                    "code": code,
                    "signal_ymd": int(anchor["ymd"]),
                    "signal_bar_index": int(anchor["bar_index"]),
                    "period": "development" if int(anchor["ymd"]) < 20240101 else "validation",
                    "rebound_day": rise_day,
                    "rebound_ymd": int(bars.iloc[rebound_position]["ymd"]),
                    "entry_open": entry_open,
                    "close20_pct": close20_pct,
                    "eventual_drop5": eventual_drop5,
                    "upward_continuation": upward,
                    "unresolved": unresolved,
                    "path": "結局下落" if eventual_drop5 else "上昇継続" if upward else "未解決",
                    "core_resignal": first_core is not None,
                    "core_resignal_days": None if first_core is None else int(first_core[0] - rebound_position),
                    "core_probe_resignal": first_any is not None,
                    "core_probe_resignal_days": resignal_days,
                    "resignal_tier": resignal_tier,
                    "resignal_drop5_in5": resignal_drop,
                }
            )

    ledger = pd.DataFrame(rows)
    if ledger.empty:
        raise RuntimeError("no rebound episodes")
    overall = pd.DataFrame(
        [{"period": period, **summarize(group)} for period, group in ledger.groupby("period")]
    )
    yearly = pd.DataFrame(
        [
            {"year": int(year), **summarize(group)}
            for year, group in ledger.assign(year=ledger["signal_ymd"] // 10000).groupby("year")
        ]
    )
    by_resignal = pd.DataFrame(
        [
            {"period": period, "resignal_state": "再シグナルあり" if state else "再シグナルなし", **summarize(group)}
            for (period, state), group in ledger.groupby(["period", "core_probe_resignal"])
        ]
    )
    validation = overall[overall["period"].eq("validation")].iloc[0]
    validation_resignal = by_resignal[
        by_resignal["period"].eq("validation") & by_resignal["resignal_state"].eq("再シグナルあり")
    ].iloc[0]
    validation_no = by_resignal[
        by_resignal["period"].eq("validation") & by_resignal["resignal_state"].eq("再シグナルなし")
    ].iloc[0]
    checks = {
        "development_n_ge1000": bool(overall[overall["period"].eq("development")].iloc[0]["n"] >= 1000),
        "validation_n_ge500": bool(validation["n"] >= 500),
        "three_paths_present_validation": int(ledger[ledger["period"].eq("validation")]["path"].nunique()) == 3,
        "resignal_and_no_resignal_present": bool(validation_resignal["n"] > 0 and validation_no["n"] > 0),
        "resignal_typed_core_probe": True,
    }
    result = {
        "schema_version": "tradex_short_rebound_resignal_path_v1.compare.v1",
        "artifact_role": "authoritative_short_rebound_resignal_path",
        "review_only": True,
        "fixed_conditions": {
            "initial_signal": "Core", "episode_spacing": "same-code Core anchors at least 20 sessions apart",
            "rebound": "next-open basis +3% before -5%, within 5 sessions",
            "post_rebound_horizon": 20, "eventual_decline": "post-rebound low reaches -5% from initial next open",
            "upward_continuation": "no -5% decline and session-20 close >= +3%",
            "unresolved": "neither", "resignal": "Core separately and Core/Probe combined",
            "development": "2019-2023", "validation": "2024-2026", "costs": "ignored",
        },
        "authoritative_result": {
            "overall": json.loads(overall.to_json(orient="records", force_ascii=False)), "yearly": json.loads(yearly.to_json(orient="records", force_ascii=False)),
            "by_resignal": json.loads(by_resignal.to_json(orient="records", force_ascii=False)), "gate_checks": checks,
        },
        "observed_branching": {
            "changed_top5_members_count": None, "changed_top10_members_count": None,
            "changed_rank_count": 0, "selection_divergence_reason": "diagnostic path split only; signal membership unchanged",
        },
        "judgment": {
            "candidate_local_decision": "keep_diagnostic" if all(checks.values()) else "hold",
            "session_aggregate_decision": "keep_short_rebound_resignal_diagnostic",
            "authoritative_rollup_decision": "keep_short_rebound_resignal_path_v1_review_only" if all(checks.values()) else "hold_short_rebound_resignal_path_v1",
            "reason_type": "fixed_path_breadth_and_resignal_diagnostic_gates",
        },
        "not_changed": ["売りシグナル", "売り候補", "MeeMee", "ranking", "runtime DB", "production logic"],
    }
    ledger.to_parquet(output / "short_rebound_resignal_ledger.parquet", index=False)
    overall.to_parquet(output / "short_rebound_resignal_metrics.parquet", index=False)
    yearly.to_parquet(output / "short_rebound_resignal_yearly_metrics.parquet", index=False)
    by_resignal.to_parquet(output / "short_rebound_resignal_split_metrics.parquet", index=False)
    (output / "compare.json").write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    (output / "_ARTIFACT_COMPLETE.json").write_text(json.dumps({"complete": True, "authoritative": "compare.json"}), encoding="utf-8")
    print(json.dumps(result["authoritative_result"], ensure_ascii=False))


if __name__ == "__main__":
    main()
