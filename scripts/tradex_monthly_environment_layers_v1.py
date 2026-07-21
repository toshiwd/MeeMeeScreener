"""Build a review-only multi-layer monthly environment ledger."""
from __future__ import annotations

import argparse
import json
from pathlib import Path

import numpy as np
import pandas as pd


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--monthly-ledger", type=Path, required=True)
    ap.add_argument("--output", type=Path, required=True)
    args = ap.parse_args()
    args.output.mkdir(parents=True, exist_ok=False)
    x = pd.read_parquet(args.monthly_ledger).copy()
    x["base_regime"] = x.environment
    local_span = (x.local_box_upper - x.local_box_lower).replace(0, np.nan)
    x["local_box_close_position"] = (x.c - x.local_box_lower) / local_span
    x["local_box_top_close_distance_atr"] = (x.local_box_upper - x.c) / x.matr6
    x["local_box_top_high_distance_atr"] = (x.local_box_upper - x.h) / x.matr6
    x["local_box_bottom_close_distance_atr"] = (x.c - x.local_box_lower) / x.matr6
    x["new_local_box_after_breakout_candidate"] = (
        x.post_box
        & x.local_box_mature
        & x.local_box_top_touch_count.ge(3)
        & x.breakout_age.ge(3)
    )
    x["local_structure"] = np.where(x.local_box_mature, "LOCAL_BOX_MATURE", "NO_MATURE_LOCAL_BOX")
    x["local_close_location"] = np.select(
        [
            x.local_box_mature & x.local_box_top_close_distance_atr.between(-0.30, 0.30),
            x.c.gt(x.local_box_upper),
            x.c.lt(x.local_box_lower),
        ],
        ["AT_LOCAL_CEILING", "ABOVE_LOCAL_BOX", "BELOW_LOCAL_BOX"],
        default="INSIDE_LOCAL_BOX",
    )
    x["local_top_touched"] = x.local_box_top_high_distance_atr.le(0.25)
    x["local_top_rejected"] = (
        x.new_local_box_after_breakout_candidate
        & x.local_top_touched
        & x.local_box_top_close_distance_atr.ge(0.30)
    )
    keep = [
        "code", "month", "source_month", "effective_month", "o", "h", "l", "c", "bars",
        "base_regime", "post_box", "box_reentry", "breakout_age", "box_pos",
        "local_box_mature", "local_box_top_touch_count", "local_box_upper", "local_box_lower",
        "new_local_box_after_breakout_candidate", "local_structure", "local_box_close_position",
        "local_close_location", "local_box_top_close_distance_atr", "local_box_top_high_distance_atr",
        "local_box_bottom_close_distance_atr", "local_top_touched", "local_top_rejected",
    ]
    ledger = x[keep].copy()
    ledger_path = args.output / "monthly_environment_layers.parquet"
    ledger.to_parquet(ledger_path, index=False)
    anchors = {}
    for code, effective, human_base, human_local in (
        ("6301", "2023-05", "POST_BOX_BREAKOUT_CONSOLIDATION", False),
        ("6532", "2023-06", "POST_BOX_BREAKOUT_CONSOLIDATION", True),
    ):
        hit = ledger[(ledger.code.astype(str).str.zfill(4) == code) & (ledger.effective_month.astype(str) == effective)]
        anchors[code] = None if hit.empty else {
            "base_regime": str(hit.iloc[-1].base_regime),
            "base_match": str(hit.iloc[-1].base_regime) == human_base,
            "new_local_box_after_breakout_candidate": bool(hit.iloc[-1].new_local_box_after_breakout_candidate),
            "local_structure_match": bool(hit.iloc[-1].new_local_box_after_breakout_candidate) == human_local,
            "touch_count": int(hit.iloc[-1].local_box_top_touch_count),
            "breakout_age": None if pd.isna(hit.iloc[-1].breakout_age) else float(hit.iloc[-1].breakout_age),
        }
    years = pd.PeriodIndex(ledger.month, freq="M").year
    summary = {}
    for year in (2023, 2024, 2025):
        z = ledger[years == year]
        summary[str(year)] = {
            "rows": len(z),
            "base_regime_counts": z.base_regime.value_counts().to_dict(),
            "new_local_box_after_breakout_candidate_rows": int(z.new_local_box_after_breakout_candidate.sum()),
            "new_local_box_after_breakout_candidate_codes": int(z.loc[z.new_local_box_after_breakout_candidate, "code"].nunique()),
            "local_top_rejected_rows": int(z.local_top_rejected.sum()),
        }
    compare = {
        "schema_version": "tradex_monthly_environment_layers_v1.compare.v1",
        "artifact_role": "authoritative_feature_contract",
        "decision": "hold_for_human_labels",
        "layers": ["base_regime", "local_structure", "local_close_location", "local_top_state"],
        "human_anchor_diagnostic": anchors,
        "year_summary": summary,
        "policy": {
            "base_regime_is_not_overwritten": True,
            "raw_distances_are_preserved": True,
            "new_local_box_candidate_is_diagnostic_not_a_trade_gate": True,
            "provisional_current_month_must_be_selected_by_month_not_effective_month": True,
        },
        "not_changed": ["probe", "core", "add", "profit take", "MeeMee ranking", "runtime DB"],
    }
    (args.output / "compare.json").write_text(json.dumps(compare, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    audit = {"rows": len(ledger), "duplicate_code_month": int(ledger.duplicated(["code", "month"]).sum()), "future_labels_used": False, "review_only": True}
    (args.output / "audit.json").write_text(json.dumps(audit, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    (args.output / "_ARTIFACT_COMPLETE.json").write_text(json.dumps({"complete": True, "authoritative": "compare.json", "ledger": str(ledger_path)}, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({"output": str(args.output), "anchors": anchors, "summary": summary}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
