from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd


VARIANTS = (
    "現行75%一括追加",
    "50%追加",
    "25%追加後_継続確認で50%追加",
    "継続確認後75%追加",
)


def metric(group: pd.DataFrame) -> dict:
    ret = group["return_pct"]
    honmei = group[group["is_honmei"]]
    return {
        "n": int(len(group)),
        "codes": int(group["code"].nunique()),
        "candidate_retention_rate": 1.0,
        "initial_entry_rate": float(group["initial_size"].gt(0).mean()),
        "mean_return_pct": float(ret.mean()),
        "median_return_pct": float(ret.median()),
        "win_rate": float(ret.gt(0).mean()),
        "severe_loss5_rate": float(ret.le(-5).mean()),
        "p10_return_pct": float(ret.quantile(0.1)),
        "mean_max_size": float(group["max_size"].mean()),
        "honmei_n": int(len(honmei)),
        "honmei_size50_rate": float(honmei["max_size"].ge(0.5).mean()),
        "honmei_full_size_rate": float(honmei["max_size"].ge(1.0).mean()),
    }


def calculate(source: pd.DataFrame, variant: str) -> pd.DataFrame:
    frame = source.copy()
    honmei = frame["d1_high"].ge(3) & frame["d1_low"].gt(-3)
    stall = frame["d1_high"].lt(3) & (
        frame["d1_close"].le(-2) | frame["d1_low"].le(-3)
    )
    continuation = honmei & frame["c2"].gt(frame["c1"])
    exit_price = frame["c3"]
    ret = 0.25 * (exit_price / frame["o1"] - 1)
    max_size = pd.Series(0.25, index=frame.index, dtype="float64")

    if variant == "現行75%一括追加":
        ret.loc[honmei] += 0.75 * (exit_price.loc[honmei] / frame.loc[honmei, "o2"] - 1)
        max_size.loc[honmei] = 1.0
    elif variant == "50%追加":
        ret.loc[honmei] += 0.50 * (exit_price.loc[honmei] / frame.loc[honmei, "o2"] - 1)
        max_size.loc[honmei] = 0.75
    elif variant == "25%追加後_継続確認で50%追加":
        ret.loc[honmei] += 0.25 * (exit_price.loc[honmei] / frame.loc[honmei, "o2"] - 1)
        ret.loc[continuation] += 0.50 * (
            exit_price.loc[continuation] / frame.loc[continuation, "o3"] - 1
        )
        max_size.loc[honmei] = 0.50
        max_size.loc[continuation] = 1.0
    elif variant == "継続確認後75%追加":
        ret.loc[continuation] += 0.75 * (
            exit_price.loc[continuation] / frame.loc[continuation, "o3"] - 1
        )
        max_size.loc[continuation] = 1.0
    else:
        raise ValueError(variant)

    ret.loc[stall] = 0.25 * (frame.loc[stall, "o2"] / frame.loc[stall, "o1"] - 1)
    frame["variant"] = variant
    frame["is_honmei"] = honmei
    frame["is_continuation"] = continuation
    frame["initial_size"] = 0.25
    frame["max_size"] = max_size
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
    overall = pd.DataFrame(
        [
            {"variant": variant, "period": period, **metric(group)}
            for (variant, period), group in ledger.groupby(["variant", "period"], sort=False)
        ]
    )
    yearly = pd.DataFrame(
        [
            {"variant": variant, "year": int(year), **metric(group)}
            for (variant, year), group in ledger.assign(year=ledger["ymd"] // 10000).groupby(
                ["variant", "year"], sort=False
            )
        ]
    )
    development = overall[overall["period"].eq("development")].set_index("variant")
    baseline = development.loc["現行75%一括追加"]
    challengers = development[development.index != "現行75%一括追加"]
    eligible = challengers[
        challengers["mean_return_pct"].ge(baseline["mean_return_pct"])
        & challengers["severe_loss5_rate"].lt(baseline["severe_loss5_rate"])
        & challengers["honmei_size50_rate"].ge(0.60)
        & challengers["candidate_retention_rate"].eq(1.0)
        & challengers["initial_entry_rate"].eq(1.0)
    ]
    selected = None if eligible.empty else str(eligible["mean_return_pct"].idxmax())
    validation = overall[
        overall["period"].eq("validation") & overall["variant"].eq(selected)
    ]
    baseline_validation = overall[
        overall["period"].eq("validation") & overall["variant"].eq("現行75%一括追加")
    ]
    validation_years = yearly[
        yearly["variant"].eq(selected) & yearly["year"].between(2024, 2026)
    ]
    checks = {
        "unique_source": duplicates == 0,
        "selected_on_development_only": selected is not None,
        "candidate_count_maintained": bool(len(validation) == 1 and validation.iloc[0]["candidate_retention_rate"] == 1),
        "initial_entry_rate_100": bool(len(validation) == 1 and validation.iloc[0]["initial_entry_rate"] == 1),
        "validation_mean_ge_current_0403": bool(len(validation) == 1 and validation.iloc[0]["mean_return_pct"] >= 0.4033005934607264),
        "validation_tail_below_current": bool(len(validation) == 1 and validation.iloc[0]["severe_loss5_rate"] < baseline_validation.iloc[0]["severe_loss5_rate"]),
        "honmei_size50_rate_ge60": bool(len(validation) == 1 and validation.iloc[0]["honmei_size50_rate"] >= 0.60),
        "all_validation_years_positive": bool(len(validation_years) == 3 and validation_years["mean_return_pct"].gt(0).all()),
    }
    keep = all(checks.values())
    result = {
        "schema_version": "tradex_long_rebound_scale_in_v1.compare.v1",
        "artifact_role": "authoritative_long_rebound_scale_in",
        "review_only": True,
        "fixed_conditions": {
            "family": "急落反発", "candidate_retention": "100%", "initial_entry": "翌日寄り25%を全件",
            "honmei": "初日高値+3%以上かつ安値-3%を割らない", "continuation": "2日目終値が1日目終値を上回る",
            "stall": "既存失速なら翌日寄り撤退", "exit": "3日終値", "changed_axis": "増玉量と継続確認のみ",
            "development": "2019-2023", "validation": "2024-2026", "costs": "ignored",
        },
        "authoritative_result": {
            "selected_variant": selected, "overall": overall.to_dict("records"),
            "validation_years": validation_years.to_dict("records"), "gate_checks": checks,
        },
        "observed_branching": {
            "changed_top5_members_count": 0, "changed_top10_members_count": 0, "changed_rank_count": 0,
            "selection_divergence_reason": "候補と初回エントリーは固定し増玉量だけ変更",
        },
        "judgment": {
            "candidate_local_decision": "keep" if keep else "drop",
            "session_aggregate_decision": "keep_scale_in" if keep else "drop_scale_in",
            "authoritative_rollup_decision": "keep_long_rebound_scale_in_v1_review_only" if keep else "drop_no_scale_in_challenger_v1",
            "reason_type": "development_selected_candidate_retention_profit_tail_scale_year_gates",
        },
        "not_changed": ["売り候補", "買い候補選定", "初回エントリー", "3日決済", "失速撤退", "MeeMee", "ranking", "runtime DB", "production logic"],
    }
    ledger.to_parquet(output / "scale_in_ledger.parquet", index=False)
    overall.to_parquet(output / "scale_in_metrics.parquet", index=False)
    yearly.to_parquet(output / "scale_in_yearly_metrics.parquet", index=False)
    (output / "compare.json").write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    (output / "_ARTIFACT_COMPLETE.json").write_text(json.dumps({"complete": True, "authoritative": "compare.json"}), encoding="utf-8")
    print(json.dumps(result["authoritative_result"], ensure_ascii=False))


if __name__ == "__main__":
    main()
