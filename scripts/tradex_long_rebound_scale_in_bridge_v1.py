from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd

from tradex_long_rebound_scale_in_v1 import calculate, metric


BASELINE = "現行75%一括追加"
CHALLENGER = "50%追加後_継続確認で25%追加"


def bridge(source: pd.DataFrame) -> pd.DataFrame:
    frame = source.copy()
    honmei = frame["d1_high"].ge(3) & frame["d1_low"].gt(-3)
    stall = frame["d1_high"].lt(3) & (
        frame["d1_close"].le(-2) | frame["d1_low"].le(-3)
    )
    continuation = honmei & frame["c2"].gt(frame["c1"])
    ret = 0.25 * (frame["c3"] / frame["o1"] - 1)
    ret.loc[honmei] += 0.50 * (frame.loc[honmei, "c3"] / frame.loc[honmei, "o2"] - 1)
    ret.loc[continuation] += 0.25 * (
        frame.loc[continuation, "c3"] / frame.loc[continuation, "o3"] - 1
    )
    ret.loc[stall] = 0.25 * (frame.loc[stall, "o2"] / frame.loc[stall, "o1"] - 1)
    frame["variant"] = CHALLENGER
    frame["is_honmei"] = honmei
    frame["is_continuation"] = continuation
    frame["initial_size"] = 0.25
    frame["max_size"] = 0.25
    frame.loc[honmei, "max_size"] = 0.75
    frame.loc[continuation, "max_size"] = 1.0
    frame["return_pct"] = 100 * ret
    return frame


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    output = Path(args.output)
    output.mkdir(parents=True, exist_ok=False)
    source = pd.read_parquet(args.source)
    if "holding_days" in source.columns:
        source = source[source["holding_days"].eq(3)].copy()
    source = source[source["buy_family"].eq("急落反発")].copy()
    duplicates = int(source.duplicated(["code", "ymd", "bar_index"]).sum())
    if duplicates:
        raise RuntimeError(f"duplicate source events: {duplicates}")
    ledger = pd.concat([calculate(source, BASELINE), bridge(source)], ignore_index=True)
    overall = pd.DataFrame([
        {"variant": variant, "period": period, **metric(group)}
        for (variant, period), group in ledger.groupby(["variant", "period"], sort=False)
    ])
    yearly = pd.DataFrame([
        {"variant": variant, "year": int(year), **metric(group)}
        for (variant, year), group in ledger.assign(year=ledger["ymd"] // 10000).groupby(["variant", "year"], sort=False)
    ])
    dev = overall[overall["period"].eq("development")].set_index("variant")
    val = overall[overall["period"].eq("validation")].set_index("variant")
    years = yearly[yearly["variant"].eq(CHALLENGER) & yearly["year"].between(2024, 2026)]
    selected = bool(
        dev.loc[CHALLENGER, "mean_return_pct"] >= dev.loc[BASELINE, "mean_return_pct"]
        and dev.loc[CHALLENGER, "severe_loss5_rate"] < dev.loc[BASELINE, "severe_loss5_rate"]
    )
    checks = {
        "unique_source": duplicates == 0,
        "selected_on_development_only": selected,
        "candidate_count_maintained": val.loc[CHALLENGER, "candidate_retention_rate"] == 1,
        "initial_entry_rate_100": val.loc[CHALLENGER, "initial_entry_rate"] == 1,
        "validation_mean_ge_current_0403": val.loc[CHALLENGER, "mean_return_pct"] >= 0.4033005934607264,
        "validation_tail_below_current": val.loc[CHALLENGER, "severe_loss5_rate"] < val.loc[BASELINE, "severe_loss5_rate"],
        "honmei_size50_rate_ge60": val.loc[CHALLENGER, "honmei_size50_rate"] >= 0.60,
        "all_validation_years_positive": len(years) == 3 and years["mean_return_pct"].gt(0).all(),
    }
    keep = all(checks.values())
    result = {
        "schema_version": "tradex_long_rebound_scale_in_bridge_v1.compare.v1",
        "artifact_role": "authoritative_long_rebound_scale_in_bridge",
        "review_only": True,
        "fixed_conditions": {
            "family": "急落反発", "candidate_retention": "100%", "initial_entry": "翌日寄り25%を全件",
            "challenger": "本命は翌日寄り50%追加、2日目終値が1日目終値超なら3日目寄り25%追加",
            "stall": "既存失速なら翌日寄り撤退", "exit": "3日終値", "development": "2019-2023",
            "validation": "2024-2026", "costs": "ignored",
        },
        "authoritative_result": {"selected_variant": CHALLENGER if selected else None, "overall": overall.to_dict("records"), "validation_years": years.to_dict("records"), "gate_checks": checks},
        "observed_branching": {"changed_top5_members_count": 0, "changed_top10_members_count": 0, "changed_rank_count": 0, "selection_divergence_reason": "候補と入口を維持し増玉量のみ変更"},
        "judgment": {"candidate_local_decision": "keep" if keep else "drop", "session_aggregate_decision": "keep_scale_in_bridge" if keep else "drop_scale_in_bridge", "authoritative_rollup_decision": "keep_long_rebound_scale_in_bridge_v1_review_only" if keep else "drop_long_rebound_scale_in_bridge_v1", "reason_type": "development_selected_validation_profit_tail_scale_year_gates"},
        "not_changed": ["売り候補", "買い候補選定", "初回エントリー", "3日決済", "失速撤退", "MeeMee", "ranking", "runtime DB", "production logic"],
    }
    ledger.to_parquet(output / "scale_in_bridge_ledger.parquet", index=False)
    overall.to_parquet(output / "scale_in_bridge_metrics.parquet", index=False)
    yearly.to_parquet(output / "scale_in_bridge_yearly_metrics.parquet", index=False)
    (output / "compare.json").write_text(json.dumps(result, ensure_ascii=False, indent=2, default=lambda value: value.item()), encoding="utf-8")
    (output / "_ARTIFACT_COMPLETE.json").write_text(json.dumps({"complete": True, "authoritative": "compare.json"}), encoding="utf-8")
    print(json.dumps(result["authoritative_result"], ensure_ascii=False, default=lambda value: value.item()))


if __name__ == "__main__":
    main()
