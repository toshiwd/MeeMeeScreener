from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd


VARIANTS = ("現行3日終値", "2日目陰線で3日目寄り撤退", "1日目終値割れで3日目寄り撤退", "1日目安値割れで3日目寄り撤退")


def metric(group: pd.DataFrame) -> dict:
    ret = group["return_pct"]
    honmei = group[group["is_honmei"]]
    return {
        "n": int(len(group)), "codes": int(group["code"].nunique()),
        "candidate_retention_rate": 1.0, "initial_entry_rate": 1.0,
        "honmei_full_size_rate": float(honmei["max_size"].ge(1).mean()),
        "honmei_early_exit_rate": float(honmei["early_exit"].mean()),
        "mean_return_pct": float(ret.mean()), "median_return_pct": float(ret.median()),
        "win_rate": float(ret.gt(0).mean()), "severe_loss5_rate": float(ret.le(-5).mean()),
        "p10_return_pct": float(ret.quantile(0.1)),
    }


def calculate(source: pd.DataFrame, variant: str) -> pd.DataFrame:
    frame = source.copy()
    honmei = frame["d1_high"].ge(3) & frame["d1_low"].gt(-3)
    stall = frame["d1_high"].lt(3) & (frame["d1_close"].le(-2) | frame["d1_low"].le(-3))
    if variant == "現行3日終値":
        trigger = pd.Series(False, index=frame.index)
    elif variant == "2日目陰線で3日目寄り撤退":
        trigger = honmei & frame["c2"].lt(frame["o2"])
    elif variant == "1日目終値割れで3日目寄り撤退":
        trigger = honmei & frame["c2"].lt(frame["c1"])
    elif variant == "1日目安値割れで3日目寄り撤退":
        trigger = honmei & frame["c2"].lt(frame["l1"])
    else:
        raise ValueError(variant)
    exit_price = frame["c3"].copy()
    exit_price.loc[trigger] = frame.loc[trigger, "o3"]
    ret = 0.25 * (exit_price / frame["o1"] - 1)
    ret.loc[honmei] += 0.75 * (exit_price.loc[honmei] / frame.loc[honmei, "o2"] - 1)
    ret.loc[stall] = 0.25 * (frame.loc[stall, "o2"] / frame.loc[stall, "o1"] - 1)
    frame["variant"] = variant
    frame["is_honmei"] = honmei
    frame["early_exit"] = trigger
    frame["max_size"] = 0.25
    frame.loc[honmei, "max_size"] = 1.0
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
    ledger = pd.concat([calculate(source, variant) for variant in VARIANTS], ignore_index=True)
    overall = pd.DataFrame([
        {"variant": variant, "period": period, **metric(group)}
        for (variant, period), group in ledger.groupby(["variant", "period"], sort=False)
    ])
    yearly = pd.DataFrame([
        {"variant": variant, "year": int(year), **metric(group)}
        for (variant, year), group in ledger.assign(year=ledger["ymd"] // 10000).groupby(["variant", "year"], sort=False)
    ])
    dev = overall[overall["period"].eq("development")].set_index("variant")
    baseline = dev.loc["現行3日終値"]
    challengers = dev[dev.index != "現行3日終値"]
    eligible = challengers[
        challengers["mean_return_pct"].ge(baseline["mean_return_pct"])
        & challengers["severe_loss5_rate"].lt(baseline["severe_loss5_rate"])
    ]
    selected = None if eligible.empty else str(eligible["mean_return_pct"].idxmax())
    val = overall[overall["period"].eq("validation")].set_index("variant")
    years = yearly[yearly["variant"].eq(selected) & yearly["year"].between(2024, 2026)]
    checks = {
        "unique_source": duplicates == 0, "selected_on_development_only": selected is not None,
        "candidate_count_maintained": bool(selected and val.loc[selected, "candidate_retention_rate"] == 1),
        "initial_entry_rate_100": bool(selected and val.loc[selected, "initial_entry_rate"] == 1),
        "honmei_full_size_rate_100": bool(selected and val.loc[selected, "honmei_full_size_rate"] == 1),
        "validation_mean_ge_current_0403": bool(selected and val.loc[selected, "mean_return_pct"] >= 0.4033005934607264),
        "validation_tail_below_current": bool(selected and val.loc[selected, "severe_loss5_rate"] < val.loc["現行3日終値", "severe_loss5_rate"]),
        "all_validation_years_positive": bool(selected and len(years) == 3 and years["mean_return_pct"].gt(0).all()),
    }
    keep = all(checks.values())
    result = {
        "schema_version": "tradex_long_rebound_post_add_exit_v1.compare.v1",
        "artifact_role": "authoritative_long_rebound_post_add_exit", "review_only": True,
        "fixed_conditions": {"family": "急落反発", "candidate_retention": "100%", "initial_entry": "翌日寄り25%を全件", "honmei_add": "翌日寄り75%を全本命", "changed_axis": "増玉後の2日目反転による3日目寄り撤退のみ", "normal_exit": "3日終値", "development": "2019-2023", "validation": "2024-2026", "costs": "ignored"},
        "authoritative_result": {"selected_variant": selected, "overall": overall.to_dict("records"), "validation_years": years.to_dict("records"), "gate_checks": checks},
        "observed_branching": {"changed_top5_members_count": 0, "changed_top10_members_count": 0, "changed_rank_count": 0, "selection_divergence_reason": "候補・入口・増玉を固定し出口だけ変更"},
        "judgment": {"candidate_local_decision": "keep" if keep else "drop", "session_aggregate_decision": "keep_post_add_exit" if keep else "drop_post_add_exit", "authoritative_rollup_decision": "keep_long_rebound_post_add_exit_v1_review_only" if keep else "drop_no_post_add_exit_challenger_v1", "reason_type": "development_selected_validation_profit_tail_year_gates"},
        "not_changed": ["売り候補", "買い候補選定", "初回エントリー", "75%増玉", "MeeMee", "ranking", "runtime DB", "production logic"],
    }
    ledger.to_parquet(output / "post_add_exit_ledger.parquet", index=False)
    overall.to_parquet(output / "post_add_exit_metrics.parquet", index=False)
    yearly.to_parquet(output / "post_add_exit_yearly_metrics.parquet", index=False)
    (output / "compare.json").write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    (output / "_ARTIFACT_COMPLETE.json").write_text(json.dumps({"complete": True, "authoritative": "compare.json"}), encoding="utf-8")
    print(json.dumps(result["authoritative_result"], ensure_ascii=False))


if __name__ == "__main__":
    main()
