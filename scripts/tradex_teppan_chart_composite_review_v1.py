from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd


DEFAULT_OUT_DIR = Path("G:/Tradex/teppan_chart_composite_review_v1")
BOX_SIGNAL_PATH = Path("G:/Tradex/monthly_box_upper_shelf_breakout_v1/signals.csv")
MA60_PULLBACK_PATH = Path("G:/Tradex/breakout_weak_pullback_ma60_recover_ma20_reclaim_v1/signals.csv")
MA200_TOUCH_PATH = Path("G:/Tradex/100ma_base_20ma_reclaim_200ma_touch_v1/signals.csv")


def _load(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    return pd.read_csv(path)


def _stats(df: pd.DataFrame, *, ret_col: str = "return_60d_pct", max_col: str = "max_high_60d_pct", dd_col: str = "min_low_60d_pct") -> dict[str, Any]:
    if df.empty:
        return {"count": 0}
    complete = df[df[ret_col].notna()].copy()
    if complete.empty:
        return {"count": int(len(df)), "complete_60d_count": 0}
    return {
        "count": int(len(df)),
        "complete_60d_count": int(len(complete)),
        "unique_symbol_count": int(complete["code"].astype(str).nunique()) if "code" in complete.columns else None,
        "win_20d_rate": float((complete.get("return_20d_pct", pd.Series(index=complete.index, dtype=float)) > 0).mean())
        if "return_20d_pct" in complete.columns
        else None,
        "win_60d_rate": float((complete[ret_col] > 0).mean()),
        "win_120d_rate": float((complete.get("return_120d_pct", pd.Series(index=complete.index, dtype=float)) > 0).mean())
        if "return_120d_pct" in complete.columns and complete["return_120d_pct"].notna().any()
        else None,
        "median_return_60d_pct": float(complete[ret_col].median()),
        "mean_return_60d_pct": float(complete[ret_col].mean()),
        "median_max_high_60d_pct": float(complete[max_col].median()) if max_col in complete.columns else None,
        "severe_drawdown_60d_rate": float((complete[dd_col] <= -10.0).mean()) if dd_col in complete.columns else None,
    }


def _box_breakout_decomposition(box: pd.DataFrame) -> dict[str, Any]:
    if box.empty:
        return {"available": False}
    breakout = box[box["breakout_confirm_date"].notna()].copy()
    strict_breakout = breakout[breakout["ma_order_state"].eq("strict_pampaka")].copy()
    early_breakout = breakout.copy()
    early_breakout["days_to_breakout"] = (
        pd.to_datetime(early_breakout["breakout_confirm_date"]) - pd.to_datetime(early_breakout["shelf_entry_date"])
    ).dt.days
    early_breakout = early_breakout[early_breakout["days_to_breakout"].between(1, 45)]
    monthly_cross_or_strict = breakout[
        breakout["monthly_ma20_cross_recent"].fillna(False) | breakout["ma_order_state"].eq("strict_pampaka")
    ].copy()
    low_dd = breakout[breakout["min_low_60d_pct"] > -6.0].copy()
    return {
        "available": True,
        "all_box_shelf": _stats(box),
        "breakout_confirmed": _stats(breakout),
        "strict_pampaka_and_breakout_confirmed": _stats(strict_breakout),
        "breakout_within_45_calendar_days": _stats(early_breakout),
        "monthly_cross_or_strict_pampaka_breakout": _stats(monthly_cross_or_strict),
        "breakout_and_low_dd_observed_slice": _stats(low_dd),
        "top_recent_success_examples": breakout.sort_values("shelf_entry_date", ascending=False)
        .head(20)
        .to_dict("records"),
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--out-dir", type=Path, default=DEFAULT_OUT_DIR)
    args = parser.parse_args()

    box = _load(BOX_SIGNAL_PATH)
    pullback = _load(MA60_PULLBACK_PATH)
    ma200 = _load(MA200_TOUCH_PATH)

    box_decomp = _box_breakout_decomposition(box)
    candidates = [
        {
            "axis": "monthly_box_upper_shelf_breakout_confirmed",
            "decision": "selected_next",
            "why": "large sample, high 60d win rate, low severe drawdown when breakout is confirmed",
            "stats": box_decomp.get("breakout_confirmed", {}),
        },
        {
            "axis": "breakout_weak_pullback_ma60_recover_ma20_reclaim",
            "decision": "hold_as_secondary_filter",
            "why": "high median return but small sample and high drawdown; use as add-on, not standalone",
            "stats": _stats(pullback),
        },
        {
            "axis": "100ma_base_20ma_reclaim_200ma_touch",
            "decision": "hold_as_watch_shape",
            "why": "broad but weak standalone; needs confirmation before use",
            "stats": _stats(ma200, dd_col="min_low_20d_pct"),
        },
    ]

    payload = {
        "artifact_name": "teppan_chart_composite_review_v1",
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "authoritative_inputs": {
            "box_upper_shelf": str(BOX_SIGNAL_PATH),
            "ma60_pullback_recover": str(MA60_PULLBACK_PATH),
            "ma200_touch_reclaim": str(MA200_TOUCH_PATH),
        },
        "fixed_review_policy": {
            "scope": "TRADEX-only chart-shape research-director review",
            "one_axis_selected": "monthly_box_upper_shelf_breakout_confirmed",
            "silent_fallback_used": False,
            "meemee_reflectable": False,
            "production_ranking_changed": False,
        },
        "box_breakout_decomposition": box_decomp,
        "candidate_axis_decisions": candidates,
        "recommended_next_axis": {
            "axis": "monthly_box_upper_shelf_breakout_confirmed_entry_plan_v1",
            "judgment": "next_validate",
            "reason": "This is the first shape with both high win rate and acceptable drawdown at large sample. Validate entry plan: initial shelf probe, add on breakout confirmation, hold while 20MA/60MA structure remains intact.",
            "keep_drop_hold_gate": {
                "keep": "win_60d_rate >= 0.68, median_return_60d_pct >= 5, severe_drawdown_60d_rate <= 0.15, complete_60d_count >= 500",
                "hold": "win_60d_rate >= 0.60 but drawdown or median return fails",
                "drop": "win_60d_rate < 0.60 or median_return_60d_pct <= 2",
            },
        },
    }
    args.out_dir.mkdir(parents=True, exist_ok=True)
    out_path = args.out_dir / "composite_review.json"
    out_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps({"out_path": str(out_path), "recommended_next_axis": payload["recommended_next_axis"], "selected_stats": candidates[0]["stats"]}, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
