"""Overlay month-to-date OHLC on prior-confirmed monthly environment layers."""
from __future__ import annotations

import argparse
import json
from pathlib import Path

import numpy as np
import pandas as pd


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--daily", type=Path, required=True)
    ap.add_argument("--monthly-layers", type=Path, required=True)
    ap.add_argument("--output", type=Path, required=True)
    args = ap.parse_args()
    args.output.mkdir(parents=True, exist_ok=False)
    d = pd.read_parquet(args.daily, columns=["code", "ymd", "o", "h", "l", "c"]).sort_values(["code", "ymd"]).reset_index(drop=True)
    d["decision_date"] = pd.to_datetime(d.ymd.astype(str), format="%Y%m%d")
    d["decision_month"] = d.decision_date.dt.to_period("M")
    group = d.groupby(["code", "decision_month"], sort=False)
    d["current_month_o"] = group.o.transform("first")
    d["current_month_h"] = group.h.cummax()
    d["current_month_l"] = group.l.cummin()
    d["current_month_c"] = d.c
    d["current_month_bars"] = group.c.cumcount() + 1
    layers = pd.read_parquet(args.monthly_layers)
    layers["effective_month"] = pd.PeriodIndex(layers.effective_month, freq="M")
    keep = [
        "code", "source_month", "effective_month", "base_regime", "post_box", "box_reentry", "breakout_age",
        "box_pos", "local_box_mature", "local_box_top_touch_count", "local_box_upper", "local_box_lower",
        "new_local_box_after_breakout_candidate", "local_structure",
    ]
    x = d.merge(layers[keep], left_on=["code", "decision_month"], right_on=["code", "effective_month"], how="left", validate="many_to_one")
    # Confirmed local boundaries never use the current partial month.
    width = (x.local_box_upper - x.local_box_lower).replace(0, np.nan)
    x["current_local_box_position"] = (x.current_month_c - x.local_box_lower) / width
    x["current_above_local_box"] = x.current_month_c > x.local_box_upper
    x["current_below_local_box"] = x.current_month_c < x.local_box_lower
    x["current_month_ceiling_try"] = x.current_month_h >= x.local_box_upper
    x["current_month_close_below_ceiling"] = x.current_month_c < x.local_box_upper
    x["current_month_rejection_from_ceiling_pct"] = np.where(
        x.current_month_ceiling_try,
        x.current_month_c / x.local_box_upper - 1.0,
        np.nan,
    )
    month_range = (x.current_month_h - x.current_month_l).replace(0, np.nan)
    x["current_month_close_position"] = (x.current_month_c - x.current_month_l) / month_range
    x["current_month_body_pct"] = (x.current_month_c - x.current_month_o) / x.current_month_o
    x["current_month_upper_wick_pct"] = (x.current_month_h - x[["current_month_o", "current_month_c"]].max(axis=1)) / x.current_month_o
    x["current_month_lower_wick_pct"] = (x[["current_month_o", "current_month_c"]].min(axis=1) - x.current_month_l) / x.current_month_o
    x["current_month_provisional"] = True
    out_cols = [
        "code", "ymd", "decision_month", "source_month", "base_regime", "post_box", "box_reentry", "breakout_age",
        "local_structure", "local_box_mature", "local_box_top_touch_count", "local_box_upper", "local_box_lower",
        "new_local_box_after_breakout_candidate", "current_month_o", "current_month_h", "current_month_l", "current_month_c",
        "current_month_bars", "current_local_box_position", "current_above_local_box", "current_below_local_box",
        "current_month_ceiling_try", "current_month_close_below_ceiling", "current_month_rejection_from_ceiling_pct",
        "current_month_close_position", "current_month_body_pct", "current_month_upper_wick_pct", "current_month_lower_wick_pct",
        "current_month_provisional",
    ]
    ledger = x[out_cols]
    path = args.output / "monthly_current_state_daily.parquet"
    ledger.to_parquet(path, index=False)
    anchors = {}
    for code, ymd in (("6301", 20230531), ("6532", 20230626)):
        hit = ledger[(ledger.code.astype(str).str.zfill(4) == code) & (ledger.ymd == ymd)]
        anchors[code] = None if hit.empty else {
            k: (bool(hit.iloc[-1][k]) if k in {"local_box_mature", "new_local_box_after_breakout_candidate", "current_month_ceiling_try", "current_month_close_below_ceiling"} else float(hit.iloc[-1][k]) if k in {"current_month_h", "current_month_c", "local_box_upper", "current_local_box_position", "current_month_rejection_from_ceiling_pct"} else str(hit.iloc[-1][k]))
            for k in ["base_regime", "local_box_mature", "new_local_box_after_breakout_candidate", "current_month_h", "current_month_c", "local_box_upper", "current_local_box_position", "current_month_ceiling_try", "current_month_close_below_ceiling", "current_month_rejection_from_ceiling_pct"]
        }
    compare = {
        "schema_version": "tradex_monthly_current_state_daily_v1.compare.v1",
        "artifact_role": "authoritative_feature_contract",
        "decision": "keep_as_review_only_evidence_layer",
        "anchors": anchors,
        "selection_policy": "prior-confirmed monthly boundaries plus current-month OHLC truncated at each decision ymd",
        "not_a_trade_gate": True,
        "not_changed": ["monthly base regime", "probe", "core", "add", "profit take", "MeeMee ranking", "runtime DB"],
    }
    (args.output / "compare.json").write_text(json.dumps(compare, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    audit = {"rows": len(ledger), "duplicate_code_ymd": int(ledger.duplicated(["code", "ymd"]).sum()), "future_daily_bars_used": False, "base_source_strictly_prior_month": bool((ledger.source_month.isna() | (pd.PeriodIndex(ledger.source_month, freq="M") < ledger.decision_month)).all()), "review_only": True}
    (args.output / "audit.json").write_text(json.dumps(audit, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    (args.output / "_ARTIFACT_COMPLETE.json").write_text(json.dumps({"complete": True, "authoritative": "compare.json", "ledger": str(path)}, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({"output": str(args.output), "anchors": anchors, "audit": audit}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
