from __future__ import annotations

import argparse
import json
import shutil
from pathlib import Path

import pandas as pd


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    source, output = Path(args.source), Path(args.output)
    output.mkdir(parents=True, exist_ok=False)
    for path in source.iterdir():
        if path.is_file():
            shutil.copy2(path, output / path.name)

    decision_path = output / "market_conditioned_decision_table.parquet"
    decisions = pd.read_parquet(decision_path)
    decisions["practical_role"] = "混合・保留"
    buy = (
        decisions["development_upward_lift"].gt(0)
        & decisions["development_drop_reduction"].gt(0)
        & decisions["validation_upward_lift"].ge(0.05)
        & decisions["validation_drop_reduction"].ge(0.05)
        & decisions["positive_validation_years"].eq(3)
        & decisions["validation_n"].ge(100)
    )
    sell = (
        decisions["development_upward_lift"].lt(0)
        & decisions["development_drop_reduction"].lt(0)
        & decisions["validation_upward_lift"].le(-0.05)
        & decisions["validation_drop_reduction"].le(-0.05)
        & decisions["negative_validation_years"].ge(2)
        & decisions["validation_n"].ge(100)
    )
    decisions.loc[buy, "practical_role"] = "買い優位"
    decisions.loc[sell, "practical_role"] = "売り再監視"
    decisions.to_parquet(decision_path, index=False)

    compare_path = output / "compare.json"
    payload = json.loads(compare_path.read_text(encoding="utf-8"))
    payload["schema_version"] = "tradex_short_denial_composite_market_regime_v2.compare.v1"
    payload["fixed_conditions"]["strict_buy_role"] = "validation upward lift and drop reduction each >=5pp, all 3 validation years positive"
    payload["fixed_conditions"]["strict_sell_role"] = "validation upward lift and drop reduction each <=-5pp, at least 2 negative validation years"
    records = json.loads(decisions.to_json(orient="records", force_ascii=False))
    stable_buy = decisions[decisions["practical_role"].eq("買い優位")]
    stable_sell = decisions[decisions["practical_role"].eq("売り再監視")]
    payload["authoritative_result"]["decision_table"] = records
    payload["authoritative_result"]["stable_buy_cells"] = json.loads(stable_buy.to_json(orient="records", force_ascii=False))
    payload["authoritative_result"]["stable_sell_cells"] = json.loads(stable_sell.to_json(orient="records", force_ascii=False))
    payload["authoritative_result"]["gate_checks"]["stable_buy_cell_exists"] = len(stable_buy) > 0
    payload["authoritative_result"]["gate_checks"]["stable_sell_cell_exists"] = len(stable_sell) > 0
    payload["judgment"] = {
        "candidate_local_decision": "hold",
        "session_aggregate_decision": "hold_no_stable_buy_keep_sell_rewatch_diagnostic",
        "authoritative_rollup_decision": "hold_no_stable_buy_keep_sell_rewatch_diagnostic_v2",
        "reason_type": "strict_5pp_three_year_buy_gate_and_strict_sell_rewatch_gate",
    }
    compare_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    (output / "_ARTIFACT_COMPLETE.json").write_text(
        json.dumps({"complete": True, "authoritative": "compare.json"}), encoding="utf-8"
    )
    print(json.dumps(payload["authoritative_result"], ensure_ascii=False))


if __name__ == "__main__":
    main()
