from __future__ import annotations

import argparse
import csv
import json
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd


DEFAULT_OUT_DIR = Path("G:/Tradex/monthly_box_breakout_entry_plan_v1")
BOX_SIGNAL_PATH = Path("G:/Tradex/monthly_box_upper_shelf_breakout_v1/signals.csv")


@dataclass(frozen=True)
class PlanRow:
    code: str
    name: str
    shelf_entry_date: str
    breakout_confirm_date: str
    entry_plan: str
    shelf_entry_close: float
    breakout_confirm_close_proxy: float
    blended_entry_price: float
    return_20d_from_shelf_pct: float | None
    return_60d_from_shelf_pct: float | None
    return_120d_from_shelf_pct: float | None
    max_high_60d_from_shelf_pct: float | None
    min_low_60d_from_shelf_pct: float | None
    estimated_blended_return_60d_pct: float | None
    estimated_blended_max_high_60d_pct: float | None
    estimated_blended_min_low_60d_pct: float | None
    ma_order_state: str
    consecutive_green_10d_after_breakout: int | None
    success_blended_60d: bool
    win_blended_60d: bool
    severe_drawdown_blended_60d: bool


def _load() -> pd.DataFrame:
    return pd.read_csv(BOX_SIGNAL_PATH)


def _build_plan_rows(df: pd.DataFrame) -> list[PlanRow]:
    rows: list[PlanRow] = []
    breakout = df[df["breakout_confirm_date"].notna()].copy()
    for _, row in breakout.iterrows():
        shelf_entry = float(row["entry_close"])
        # The source artifact does not store breakout close; approximate add price from observed
        # shelf-to-breakout path by using the box upper breakout threshold. This is conservative
        # for many gap breakouts and avoids re-reading daily bars in this review layer.
        add_price = float(row["box_upper"]) * 1.05
        blended = (shelf_entry + add_price) / 2.0
        def scale_from_shelf(value: Any) -> float | None:
            if pd.isna(value):
                return None
            target = shelf_entry * (1.0 + float(value) / 100.0)
            return (target / blended - 1.0) * 100.0
        blended_60 = scale_from_shelf(row["return_60d_pct"])
        blended_max_60 = scale_from_shelf(row["max_high_60d_pct"])
        blended_min_60 = scale_from_shelf(row["min_low_60d_pct"])
        rows.append(
            PlanRow(
                code=str(row["code"]),
                name=str(row["name"]),
                shelf_entry_date=str(row["shelf_entry_date"]),
                breakout_confirm_date=str(row["breakout_confirm_date"]),
                entry_plan="half_at_shelf_upper_half_at_breakout_confirm",
                shelf_entry_close=shelf_entry,
                breakout_confirm_close_proxy=round(add_price, 4),
                blended_entry_price=round(blended, 4),
                return_20d_from_shelf_pct=None if pd.isna(row["return_20d_pct"]) else float(row["return_20d_pct"]),
                return_60d_from_shelf_pct=None if pd.isna(row["return_60d_pct"]) else float(row["return_60d_pct"]),
                return_120d_from_shelf_pct=None if pd.isna(row["return_120d_pct"]) else float(row["return_120d_pct"]),
                max_high_60d_from_shelf_pct=None if pd.isna(row["max_high_60d_pct"]) else float(row["max_high_60d_pct"]),
                min_low_60d_from_shelf_pct=None if pd.isna(row["min_low_60d_pct"]) else float(row["min_low_60d_pct"]),
                estimated_blended_return_60d_pct=None if blended_60 is None else round(blended_60, 4),
                estimated_blended_max_high_60d_pct=None if blended_max_60 is None else round(blended_max_60, 4),
                estimated_blended_min_low_60d_pct=None if blended_min_60 is None else round(blended_min_60, 4),
                ma_order_state=str(row["ma_order_state"]),
                consecutive_green_10d_after_breakout=None
                if pd.isna(row["consecutive_green_10d_after_breakout"])
                else int(row["consecutive_green_10d_after_breakout"]),
                success_blended_60d=bool(
                    blended_max_60 is not None
                    and blended_min_60 is not None
                    and blended_max_60 >= 8.0
                    and blended_min_60 > -10.0
                ),
                win_blended_60d=bool(blended_60 is not None and blended_60 > 0),
                severe_drawdown_blended_60d=bool(blended_min_60 is not None and blended_min_60 <= -10.0),
            )
        )
    return rows


def _summarize(rows: list[PlanRow]) -> dict[str, Any]:
    df = pd.DataFrame([asdict(row) for row in rows])
    complete = df[df["estimated_blended_return_60d_pct"].notna()].copy()
    if complete.empty:
        return {"signal_count": int(len(df)), "complete_60d_count": 0, "judgment": "hold", "reason": "no_complete_forward_window"}
    win = float(complete["win_blended_60d"].mean())
    success = float(complete["success_blended_60d"].mean())
    severe = float(complete["severe_drawdown_blended_60d"].mean())
    median_ret = float(complete["estimated_blended_return_60d_pct"].median())
    if win >= 0.68 and median_ret >= 5.0 and severe <= 0.15 and len(complete) >= 500:
        judgment = "keep"
        reason = "entry_plan_passes_teppan_gate"
    elif win >= 0.60 and median_ret > 2.0:
        judgment = "hold"
        reason = "positive_but_entry_plan_needs_filter"
    else:
        judgment = "drop"
        reason = "entry_plan_fails_win_or_return_gate"
    strict = complete[complete["ma_order_state"].eq("strict_pampaka")]
    return {
        "signal_count": int(len(df)),
        "complete_60d_count": int(len(complete)),
        "unique_symbol_count": int(complete["code"].astype(str).nunique()),
        "win_blended_60d_rate": win,
        "success_blended_60d_rate": success,
        "severe_drawdown_blended_60d_rate": severe,
        "median_blended_return_60d_pct": median_ret,
        "mean_blended_return_60d_pct": float(complete["estimated_blended_return_60d_pct"].mean()),
        "median_blended_max_high_60d_pct": float(complete["estimated_blended_max_high_60d_pct"].median()),
        "strict_pampaka_slice": {
            "count": int(len(strict)),
            "win_blended_60d_rate": float(strict["win_blended_60d"].mean()) if not strict.empty else None,
            "median_blended_return_60d_pct": float(strict["estimated_blended_return_60d_pct"].median()) if not strict.empty else None,
            "severe_drawdown_blended_60d_rate": float(strict["severe_drawdown_blended_60d"].mean()) if not strict.empty else None,
        },
        "judgment": judgment,
        "reason": reason,
    }


def _write_outputs(out_dir: Path, payload: dict[str, Any], rows: list[PlanRow]) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    payload_path = out_dir / "entry_plan_compare.json"
    payload_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    dict_rows = [asdict(row) for row in rows]
    with (out_dir / "entry_plan_signals.jsonl").open("w", encoding="utf-8", newline="\n") as fh:
        for row in dict_rows:
            fh.write(json.dumps(row, ensure_ascii=False) + "\n")
    if dict_rows:
        with (out_dir / "entry_plan_signals.csv").open("w", encoding="utf-8-sig", newline="") as fh:
            writer = csv.DictWriter(fh, fieldnames=list(dict_rows[0].keys()))
            writer.writeheader()
            writer.writerows(dict_rows)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--out-dir", type=Path, default=DEFAULT_OUT_DIR)
    args = parser.parse_args()
    source = _load()
    rows = _build_plan_rows(source)
    summary = _summarize(rows)
    payload = {
        "artifact_name": "monthly_box_breakout_entry_plan_v1",
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "authoritative_input": str(BOX_SIGNAL_PATH),
        "fixed_evaluation_conditions": {
            "scope": "TRADEX-only entry-plan validation on selected teppan chart axis",
            "entry_plan": "50% shelf upper probe + 50% add at breakout confirmation proxy",
            "breakout_confirmation_proxy": "box_upper * 1.05 because source artifact stores breakout date but not exact breakout close",
            "success": "estimated blended 60d max high >= +8% and drawdown > -10%",
            "silent_fallback_used": False,
            "meemee_reflectable": False,
        },
        "summary": summary,
        "sample_recent_rows": sorted([asdict(row) for row in rows], key=lambda item: item["shelf_entry_date"], reverse=True)[:20],
    }
    _write_outputs(args.out_dir, payload, rows)
    print(json.dumps({"out_dir": str(args.out_dir), "summary": summary}, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
