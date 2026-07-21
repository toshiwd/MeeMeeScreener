from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd


VARIANTS = {
    "全件3日": (3, False),
    "本命4日": (4, False),
    "本命4日_前日安値割れ撤退": (4, True),
    "本命5日": (5, False),
    "本命5日_前日安値割れ撤退": (5, True),
}


def metric(group: pd.DataFrame) -> dict:
    ret = group["return_pct"]
    return {
        "n": int(len(group)),
        "codes": int(group["code"].nunique()),
        "mean_return_pct": float(ret.mean()),
        "median_return_pct": float(ret.median()),
        "win_rate": float(ret.gt(0).mean()),
        "severe_loss5_rate": float(ret.le(-5).mean()),
        "p10_return_pct": float(ret.quantile(0.1)),
    }


def calculate(source: pd.DataFrame, name: str, horizon: int, reversal: bool) -> pd.DataFrame:
    frame = source.copy()
    honmei = frame["d1_high"].ge(3) & frame["d1_low"].gt(-3)
    stall = frame["d1_high"].lt(3) & (
        frame["d1_close"].le(-2) | frame["d1_low"].le(-3)
    )
    exit_price = frame["c3"].copy()
    exit_reason = pd.Series("3日期限", index=frame.index, dtype="object")
    if horizon >= 4:
        exit_price.loc[honmei] = frame.loc[honmei, f"c{horizon}"]
        exit_reason.loc[honmei] = f"本命{horizon}日期限"
    if reversal:
        previous_low = {2: "l1", 3: "l2", 4: "l3"}
        next_open = {2: "o3", 3: "o4", 4: "o5"}
        still_holding = honmei.copy()
        for day in range(2, horizon):
            trigger = still_holding & frame[f"c{day}"].lt(frame[previous_low[day]])
            exit_price.loc[trigger] = frame.loc[trigger, next_open[day]]
            exit_reason.loc[trigger] = f"{day}日目反転_翌日寄り撤退"
            still_holding &= ~trigger
    frame["return_pct"] = 25 * (exit_price / frame["o1"] - 1)
    frame.loc[honmei, "return_pct"] = 100 * (
        0.25 * (exit_price.loc[honmei] / frame.loc[honmei, "o1"] - 1)
        + 0.75 * (exit_price.loc[honmei] / frame.loc[honmei, "o2"] - 1)
    )
    frame.loc[stall, "return_pct"] = 25 * (
        frame.loc[stall, "o2"] / frame.loc[stall, "o1"] - 1
    )
    exit_reason.loc[stall] = "失速_翌日寄り撤退"
    frame["variant"] = name
    frame["exit_reason"] = exit_reason
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

    ledger = pd.concat(
        [calculate(source, name, *settings) for name, settings in VARIANTS.items()],
        ignore_index=True,
    )
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
    baseline = development.loc["全件3日"]
    eligible = development[
        (development.index != "全件3日")
        & development["mean_return_pct"].gt(baseline["mean_return_pct"])
        & development["severe_loss5_rate"].le(0.05)
    ]
    selected = None if eligible.empty else str(eligible["mean_return_pct"].idxmax())
    validation = overall[
        overall["period"].eq("validation") & overall["variant"].eq(selected)
    ]
    baseline_validation = overall[
        overall["period"].eq("validation") & overall["variant"].eq("全件3日")
    ]
    validation_years = yearly[
        yearly["variant"].eq(selected) & yearly["year"].between(2024, 2026)
    ]
    checks = {
        "unique_source": duplicates == 0,
        "selected_on_development_only": selected is not None,
        "development_mean_above_baseline": bool(
            selected is not None
            and development.loc[selected, "mean_return_pct"] > baseline["mean_return_pct"]
        ),
        "validation_mean_above_baseline": bool(
            len(validation) == 1
            and validation.iloc[0]["mean_return_pct"]
            > baseline_validation.iloc[0]["mean_return_pct"]
        ),
        "validation_tail_le5": bool(
            len(validation) == 1 and validation.iloc[0]["severe_loss5_rate"] <= 0.05
        ),
        "all_validation_years_positive": bool(
            len(validation_years) == 3 and validation_years["mean_return_pct"].gt(0).all()
        ),
    }
    keep = all(checks.values())
    result = {
        "schema_version": "tradex_long_honmei_variable_holding_v2.compare.v1",
        "artifact_role": "authoritative_long_honmei_variable_holding",
        "review_only": True,
        "fixed_conditions": {
            "family": "急落反発",
            "entry": "翌日寄り25%",
            "add": "即上昇なら翌日寄り75%追加",
            "stall": "失速なら翌日寄り撤退",
            "other": "3日終値決済",
            "changed_axis": "本命の保有期限と前日安値割れ撤退のみ",
            "selection": "開発期で3日基準より平均損益が高く5%以上損失率5%以下",
            "development": "2019-2023",
            "validation": "2024-2026",
            "costs": "ignored",
        },
        "authoritative_result": {
            "selected_variant": selected,
            "overall": overall.to_dict("records"),
            "validation_years": validation_years.to_dict("records"),
            "gate_checks": checks,
        },
        "observed_branching": {
            "changed_top5_members_count": None,
            "changed_top10_members_count": None,
            "changed_rank_count": 0,
            "selection_divergence_reason": "候補と順位は固定し本命の出口だけ変更",
        },
        "judgment": {
            "candidate_local_decision": "keep" if keep else "drop",
            "session_aggregate_decision": "keep_variable_holding" if keep else "drop_variable_holding",
            "authoritative_rollup_decision": (
                "keep_long_honmei_variable_holding_v2_review_only"
                if keep
                else "drop_no_development_stable_variable_holding_v2"
            ),
            "reason_type": "development_first_profit_tail_then_validation_year_gates",
        },
        "not_changed": [
            "候補選定", "初期玉", "増玉条件", "失速撤退",
            "MeeMee", "ranking", "runtime DB", "production logic",
        ],
    }
    ledger.to_parquet(output / "variable_holding_ledger.parquet", index=False)
    overall.to_parquet(output / "variable_holding_metrics.parquet", index=False)
    yearly.to_parquet(output / "variable_holding_yearly_metrics.parquet", index=False)
    (output / "compare.json").write_text(
        json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    (output / "_ARTIFACT_COMPLETE.json").write_text(
        json.dumps({"complete": True, "authoritative": "compare.json"}), encoding="utf-8"
    )
    print(json.dumps(result["authoritative_result"], ensure_ascii=False))


if __name__ == "__main__":
    main()
