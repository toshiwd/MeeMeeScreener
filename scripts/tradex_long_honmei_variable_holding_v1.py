from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd


VARIANTS = ("全件3日", "本命5日", "本命5日_前日安値割れ撤退")


def metrics(group: pd.DataFrame) -> dict:
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


def apply_variant(source: pd.DataFrame, variant: str) -> pd.DataFrame:
    frame = source.copy()
    honmei = frame["entry_day_path"].eq("即上昇")
    stall = frame["entry_day_path"].eq("失速")

    exit_price = frame["c3"].copy()
    exit_reason = pd.Series("3日期限", index=frame.index, dtype="object")

    if variant != "全件3日":
        exit_price.loc[honmei] = frame.loc[honmei, "c5"]
        exit_reason.loc[honmei] = "本命5日期限"

    if variant == "本命5日_前日安値割れ撤退":
        reversal_d2 = honmei & frame["c2"].lt(frame["l1"])
        reversal_d3 = honmei & ~reversal_d2 & frame["c3"].lt(frame["l2"])
        reversal_d4 = honmei & ~reversal_d2 & ~reversal_d3 & frame["c4"].lt(frame["l3"])
        exit_price.loc[reversal_d2] = frame.loc[reversal_d2, "o3"]
        exit_price.loc[reversal_d3] = frame.loc[reversal_d3, "o4"]
        exit_price.loc[reversal_d4] = frame.loc[reversal_d4, "o5"]
        exit_reason.loc[reversal_d2] = "2日目反転_3日目寄り撤退"
        exit_reason.loc[reversal_d3] = "3日目反転_4日目寄り撤退"
        exit_reason.loc[reversal_d4] = "4日目反転_5日目寄り撤退"

    frame["return_pct"] = 25.0 * (exit_price / frame["o1"] - 1.0)
    frame.loc[honmei, "return_pct"] = 100.0 * (
        0.25 * (exit_price.loc[honmei] / frame.loc[honmei, "o1"] - 1.0)
        + 0.75 * (exit_price.loc[honmei] / frame.loc[honmei, "o2"] - 1.0)
    )
    frame.loc[stall, "return_pct"] = 25.0 * (
        frame.loc[stall, "o2"] / frame.loc[stall, "o1"] - 1.0
    )
    exit_reason.loc[stall] = "失速_翌日寄り撤退"
    frame["variant"] = variant
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
    source = source[source["buy_family"].eq("急落反発")].copy()

    ledgers = [apply_variant(source, variant) for variant in VARIANTS]
    ledger = pd.concat(ledgers, ignore_index=True)
    overall_rows = []
    yearly_rows = []
    tier_rows = []
    for (variant, period), group in ledger.groupby(["variant", "period"], sort=False):
        overall_rows.append({"variant": variant, "period": period, **metrics(group)})
    for (variant, year), group in ledger.assign(year=ledger["ymd"] // 10000).groupby(
        ["variant", "year"], sort=False
    ):
        yearly_rows.append({"variant": variant, "year": int(year), **metrics(group)})
    for (variant, period, tier), group in ledger.groupby(
        ["variant", "period", "entry_day_path"], sort=False
    ):
        tier_rows.append(
            {"variant": variant, "period": period, "entry_day_path": tier, **metrics(group)}
        )

    overall = pd.DataFrame(overall_rows)
    yearly = pd.DataFrame(yearly_rows)
    tiers = pd.DataFrame(tier_rows)
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
    validation_years = yearly[
        yearly["variant"].eq(selected) & yearly["year"].between(2024, 2026)
    ]
    baseline_validation = overall[
        overall["period"].eq("validation") & overall["variant"].eq("全件3日")
    ]
    checks = {
        "selected_on_development_only": selected is not None,
        "development_mean_above_baseline": bool(
            selected is not None
            and development.loc[selected, "mean_return_pct"] > baseline["mean_return_pct"]
        ),
        "validation_mean_above_baseline": bool(
            len(validation) == 1
            and len(baseline_validation) == 1
            and validation.iloc[0]["mean_return_pct"]
            > baseline_validation.iloc[0]["mean_return_pct"]
        ),
        "validation_tail_le5": bool(
            len(validation) == 1 and validation.iloc[0]["severe_loss5_rate"] <= 0.05
        ),
        "all_validation_years_positive": bool(
            len(validation_years) == 3
            and validation_years["mean_return_pct"].gt(0).all()
        ),
    }
    keep = all(checks.values())
    result = {
        "schema_version": "tradex_long_honmei_variable_holding_v1.compare.v1",
        "artifact_role": "authoritative_long_honmei_variable_holding",
        "review_only": True,
        "fixed_conditions": {
            "family": "急落反発",
            "entry": "翌日寄り25%",
            "add": "即上昇なら翌日寄り75%追加",
            "stall": "失速なら翌日寄り撤退",
            "other": "3日終値決済",
            "changed_axis": "本命の保有期限と反転撤退のみ",
            "reversal": "本命で終値が前日安値を割れば翌日寄り撤退",
            "development": "2019-2023",
            "validation": "2024-2026",
            "costs": "ignored",
        },
        "authoritative_result": {
            "selected_variant": selected,
            "overall": overall.to_dict("records"),
            "validation_years": validation_years.to_dict("records"),
            "validation_tiers": tiers[
                tiers["period"].eq("validation") & tiers["variant"].eq(selected)
            ].to_dict("records"),
            "gate_checks": checks,
        },
        "observed_branching": {
            "changed_top5_members_count": None,
            "changed_top10_members_count": None,
            "changed_rank_count": 0,
            "selection_divergence_reason": "候補と順位は固定し、本命の出口だけを変更",
        },
        "judgment": {
            "candidate_local_decision": "keep" if keep else "drop",
            "session_aggregate_decision": "keep_variable_holding" if keep else "drop_variable_holding",
            "authoritative_rollup_decision": (
                "keep_long_honmei_variable_holding_v1_review_only"
                if keep
                else "drop_long_honmei_variable_holding_v1"
            ),
            "reason_type": "development_selected_validation_profit_tail_year_gates",
        },
        "not_changed": [
            "候補選定",
            "初期玉",
            "増玉条件",
            "失速撤退",
            "MeeMee",
            "ranking",
            "runtime DB",
            "production logic",
        ],
    }

    ledger.to_parquet(output / "variable_holding_ledger.parquet", index=False)
    overall.to_parquet(output / "variable_holding_metrics.parquet", index=False)
    yearly.to_parquet(output / "variable_holding_yearly_metrics.parquet", index=False)
    tiers.to_parquet(output / "variable_holding_tier_metrics.parquet", index=False)
    (output / "compare.json").write_text(
        json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    (output / "_ARTIFACT_COMPLETE.json").write_text(
        json.dumps({"complete": True, "authoritative": "compare.json"}), encoding="utf-8"
    )
    print(json.dumps(result["authoritative_result"], ensure_ascii=False))


if __name__ == "__main__":
    main()
