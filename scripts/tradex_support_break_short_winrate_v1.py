from __future__ import annotations

import argparse
import json
import math
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd


BATTLE_ZONE_LOOKBACK = 120
SUPPORT3_TOUCH_COUNT = 3


def _ymd(value: Any) -> int:
    ivalue = int(value)
    if ivalue > 100_000_000:
        return int(pd.to_datetime(ivalue, unit="s", utc=True).strftime("%Y%m%d"))
    return ivalue


def _pct(value: float | None) -> float | None:
    if value is None:
        return None
    if isinstance(value, float) and math.isnan(value):
        return None
    return round(float(value) * 100.0, 4)


def _load_bars(db_path: Path) -> tuple[pd.DataFrame, dict[str, Any]]:
    con = duckdb.connect(str(db_path), read_only=True)
    try:
        min_date, max_date, code_count, row_count = con.execute(
            """
            SELECT min(date), max(date), count(DISTINCT code), count(*)
            FROM daily_bars
            WHERE c IS NOT NULL AND o IS NOT NULL AND h IS NOT NULL AND l IS NOT NULL
            """
        ).fetchone()
        bars = con.execute(
            """
            SELECT
                b.code,
                b.date,
                b.o,
                b.h,
                b.l,
                b.c,
                b.v,
                m.ma20
            FROM daily_bars b
            LEFT JOIN daily_ma m ON m.code = b.code AND m.date = b.date
            WHERE b.c IS NOT NULL
              AND b.o IS NOT NULL
              AND b.h IS NOT NULL
              AND b.l IS NOT NULL
              AND b.v IS NOT NULL
            ORDER BY b.code, b.date
            """
        ).fetchdf()
    finally:
        con.close()

    metadata = {
        "daily_bars_min_date": _ymd(min_date),
        "daily_bars_max_date": _ymd(max_date),
        "daily_bars_code_count": int(code_count),
        "daily_bars_row_count": int(row_count),
    }
    return bars, metadata


def _support3_zones_for_code(group: pd.DataFrame) -> list[dict[str, Any]]:
    g = group.reset_index(drop=True)
    zones: list[dict[str, Any]] = []
    last_break_time = 0

    for idx in range(BATTLE_ZONE_LOOKBACK, len(g)):
        current = g.iloc[idx]
        ma20 = current.get("ma20")
        if pd.isna(ma20) or float(current["c"]) <= float(ma20):
            continue

        prior_volume = pd.to_numeric(g.loc[max(0, idx - 20) : idx - 1, "v"], errors="coerce").dropna()
        if prior_volume.empty or float(current["v"]) < float(prior_volume.mean()):
            continue

        prior = g.iloc[idx - BATTLE_ZONE_LOOKBACK : idx]
        resistance = float(prior["h"].max())
        if not math.isfinite(resistance) or resistance <= 0:
            continue

        lower = resistance * 0.97
        upper = resistance * 1.006
        touch_bars = prior[(prior["h"] >= lower) & (prior["h"] <= upper)]
        touch_count = int(len(touch_bars))
        if touch_count < 2:
            continue

        prev_close = float(g.at[idx - 1, "c"]) if idx > 0 else 0.0
        broke_resistance = (
            float(current["c"]) >= upper
            and float(current["h"]) >= upper
            and prev_close < upper
            and float(current["l"]) <= resistance * 1.04
        )
        if not broke_resistance:
            continue
        if int(current["date"]) - int(last_break_time) < 20 * 24 * 60 * 60:
            continue

        next_bars = g.iloc[idx + 1 : idx + 4]
        if len(next_bars) < 3:
            continue
        failed = next_bars[next_bars["c"] < resistance * 0.99]
        if not failed.empty:
            last_break_time = int(current["date"])
            continue

        if touch_count == SUPPORT3_TOUCH_COUNT:
            zones.append(
                {
                    "code": str(current["code"]),
                    "breakout_idx": idx,
                    "breakout_date": int(current["date"]),
                    "breakout_date_ymd": _ymd(current["date"]),
                    "support_established_idx": idx + 3,
                    "support_established_date": int(g.at[idx + 3, "date"]),
                    "support_established_date_ymd": _ymd(g.at[idx + 3, "date"]),
                    "resistance": resistance,
                    "upper": upper,
                    "lower": lower,
                    "touch_count": touch_count,
                    "start_date": int(touch_bars.iloc[0]["date"]),
                    "start_date_ymd": _ymd(touch_bars.iloc[0]["date"]),
                }
            )
        last_break_time = int(current["date"])
    return zones


def _event_for_zone(group: pd.DataFrame, zone: dict[str, Any]) -> dict[str, Any] | None:
    g = group.reset_index(drop=True)
    lower = float(zone["lower"])
    start_idx = int(zone["support_established_idx"]) + 1
    if start_idx >= len(g) - 20:
        return None

    for idx in range(start_idx, len(g) - 20):
        close = float(g.at[idx, "c"])
        prev_close = float(g.at[idx - 1, "c"]) if idx > 0 else close
        if close < lower and prev_close >= lower:
            future = g.loc[idx + 1 : idx + 20]
            event: dict[str, Any] = {
                **zone,
                "event_rule": "support3_zone_lower_close_break",
                "break_idx": idx,
                "break_date": int(g.at[idx, "date"]),
                "break_date_ymd": _ymd(g.at[idx, "date"]),
                "break_close": close,
                "entry_next_open": float(g.at[idx + 1, "o"]),
                "support_break_depth_pct": close / lower - 1.0,
                "future_low_5": float(g.loc[idx + 1 : idx + 5, "l"].min()),
                "future_low_10": float(g.loc[idx + 1 : idx + 10, "l"].min()),
                "future_low_20": float(future["l"].min()),
                "future_high_20": float(future["h"].max()),
                "future_c_5": float(g.at[idx + 5, "c"]),
                "future_c_10": float(g.at[idx + 10, "c"]),
                "future_c_20": float(g.at[idx + 20, "c"]),
            }
            for horizon in (5, 10, 20):
                event[f"downside_from_break_close_{horizon}d"] = 1.0 - (
                    float(event[f"future_low_{horizon}"]) / close
                )
                event[f"close_return_after_break_{horizon}d"] = float(event[f"future_c_{horizon}"]) / close - 1.0
                event[f"short_return_next_open_{horizon}d"] = (
                    float(event["entry_next_open"]) / float(event[f"future_c_{horizon}"]) - 1.0
                )
            event["adverse_rebound_20d_from_next_open"] = float(event["entry_next_open"]) / float(event["future_high_20"]) - 1.0
            return event
    return None


def build_events(db_path: Path) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, Any]]:
    bars, metadata = _load_bars(db_path)
    zones: list[dict[str, Any]] = []
    events: list[dict[str, Any]] = []

    for _code, group in bars.groupby("code", sort=False):
        code_zones = _support3_zones_for_code(group)
        zones.extend(code_zones)
        for zone in code_zones:
            event = _event_for_zone(group, zone)
            if event is not None:
                events.append(event)

    return pd.DataFrame(events), pd.DataFrame(zones), metadata


def _summarize(rows: pd.DataFrame) -> dict[str, Any]:
    out: dict[str, Any] = {
        "event_count": int(len(rows)),
        "code_count": int(rows["code"].nunique()) if len(rows) else 0,
        "break_date_min": int(rows["break_date_ymd"].min()) if len(rows) else None,
        "break_date_max": int(rows["break_date_ymd"].max()) if len(rows) else None,
    }
    for horizon in (5, 10, 20):
        downside = rows[f"downside_from_break_close_{horizon}d"].dropna()
        close_ret = rows[f"close_return_after_break_{horizon}d"].dropna()
        short_ret = rows[f"short_return_next_open_{horizon}d"].dropna()
        out[f"downside_{horizon}d_sample_count"] = int(len(downside))
        out[f"downside_{horizon}d_mean"] = _pct(float(downside.mean())) if len(downside) else None
        out[f"downside_{horizon}d_median"] = _pct(float(downside.median())) if len(downside) else None
        out[f"downside_{horizon}d_p75"] = _pct(float(downside.quantile(0.75))) if len(downside) else None
        out[f"downside_{horizon}d_ge_3pct_rate"] = _pct(float((downside >= 0.03).mean())) if len(downside) else None
        out[f"downside_{horizon}d_ge_5pct_rate"] = _pct(float((downside >= 0.05).mean())) if len(downside) else None
        out[f"downside_{horizon}d_ge_10pct_rate"] = _pct(float((downside >= 0.10).mean())) if len(downside) else None
        out[f"close_return_{horizon}d_mean"] = _pct(float(close_ret.mean())) if len(close_ret) else None
        out[f"close_return_{horizon}d_median"] = _pct(float(close_ret.median())) if len(close_ret) else None
        out[f"short_next_open_win_rate_{horizon}d"] = _pct(float((short_ret > 0).mean())) if len(short_ret) else None
        out[f"short_next_open_mean_{horizon}d"] = _pct(float(short_ret.mean())) if len(short_ret) else None

    adverse = rows["adverse_rebound_20d_from_next_open"].dropna()
    out["adverse_rebound_20d_ge_5pct_rate"] = _pct(float((adverse <= -0.05).mean())) if len(adverse) else None
    out["adverse_rebound_20d_p10"] = _pct(float(adverse.quantile(0.10))) if len(adverse) else None
    return out


def run(db_path: Path, output_root: Path) -> Path:
    output_dir = output_root / (datetime.now().strftime("%Y%m%dT%H%M%S") + "-meemee-support3-break-v1")
    output_dir.mkdir(parents=True, exist_ok=True)

    events, zones, metadata = build_events(db_path)
    overall = _summarize(events)
    recent = events[events["break_date_ymd"] >= 20240101] if not events.empty else events
    recent_2024_plus = _summarize(recent)

    compare = {
        "schema_version": "meemee_support3_break_downside_v1",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "source_db": str(db_path),
        **metadata,
        "definition": {
            "source_logic": "app/frontend/src/components/DetailChart.tsx buildBattleZones",
            "battle_zone_lookback": BATTLE_ZONE_LOOKBACK,
            "support3": "state support with touchCount == 3; support means the breakout did not close back below resistance * 0.99 in the next 3 bars",
            "support_zone": "lower = resistance * 0.97; upper = resistance * 1.006",
            "break_event": "first later close below support zone lower after support state is established",
            "downside": "1 - future low / break close; positive values mean subsequent downside after the support3 break",
            "display_cap_note": "DetailChart displays only the latest two zones; historical validation intentionally scans all historical zones.",
            "scope": "TRADEX review-only; no MeeMee reflection",
        },
        "zone_count": int(len(zones)),
        "event_count": int(len(events)),
        "overall": overall,
        "recent_2024_plus": recent_2024_plus,
        "decision": {
            "candidate_local_decision": "hold",
            "reason": "support3 break has meaningful downside follow-through, but sample size is small and the detector was originally a display overlay rather than a validated trading entry.",
        },
    }
    (output_dir / "compare.json").write_text(json.dumps(compare, ensure_ascii=False, indent=2), encoding="utf-8")
    events.to_csv(output_dir / "events.csv", index=False)
    zones.to_csv(output_dir / "zones.csv", index=False)
    return output_dir


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db-path", type=Path, default=Path(r"C:\work\meemee-screener\stocks.duckdb"))
    parser.add_argument("--output-root", type=Path, default=Path(r"G:\Tradex\support3_break_downside_v1"))
    args = parser.parse_args()
    out = run(args.db_path, args.output_root)
    print(out)
    print((out / "compare.json").read_text(encoding="utf-8"))


if __name__ == "__main__":
    main()
