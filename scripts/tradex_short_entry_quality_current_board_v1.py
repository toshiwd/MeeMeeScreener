"""Materialize the current review-only board for the validated entry-quality short challenger."""
import argparse
import hashlib
import json
import sys
from pathlib import Path

import duckdb
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.tradex_pre_crash_short_exit_profit_take_v1 import _load_daily
from scripts.tradex_pre_crash_shape_false_positive_escape_v1 import _add_shape_features
from scripts.tradex_short_episode_timing_axis_v1 import add_episode_features, timing_state


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", type=Path, required=True)
    ap.add_argument("--rollup-compare", type=Path, required=True)
    ap.add_argument("--output", type=Path, required=True)
    a = ap.parse_args()
    a.output.mkdir(parents=True, exist_ok=False)

    rollup = json.loads(a.rollup_compare.read_text(encoding="utf-8"))
    if rollup["judgment"]["authoritative_rollup_decision"] != "keep_short_entry_quality_v1_review_only":
        raise RuntimeError("entry-quality rollup is not kept")
    daily = _load_daily(a.db, None)
    latest_ymd = int(daily.ymd.max())
    con = duckdb.connect(str(a.db), read_only=True)
    names = con.execute("select code,name from industry_master").fetchdf()
    earnings = con.execute(
        "select code,strftime(planned_date,'%Y%m%d')::integer event_ymd,kind from earnings_planned"
    ).fetchdf()
    con.close()
    name_map = dict(zip(names.code.astype(str).str.zfill(4), names.name))
    earnings.code = earnings.code.astype(str).str.zfill(4)

    rows = []
    for code, group in daily.groupby("code", sort=False):
        g = add_episode_features(_add_shape_features(group))
        g["ma20_current"] = g.c.rolling(20, min_periods=20).mean()
        g["ma20_slope5_pct"] = (g.ma20_current / g.ma20_current.shift(5) - 1) * 100
        current = g.iloc[-1]
        if int(current.ymd) != latest_ymd:
            continue
        state = timing_state(current.episode_age)
        price_ok = 900 <= float(current.c) < 5000
        ma_above = bool(current.c >= current.ma20_current)
        ma_rising = bool(current.ma20_slope5_pct > 0)
        if state == "LateChase":
            review_state = "LateChase"
        elif state == "Continuation":
            review_state = "Continuation"
        elif state == "Early" and not bool(current.episode_onset):
            review_state = "EarlyMonitor"
        elif state == "Early" and not price_ok:
            review_state = "PriceBlock"
        elif state == "Early" and ma_above and ma_rising:
            review_state = "TrendPullbackBlock"
        elif state == "Early" and ma_above and not ma_rising:
            review_state = "EntryReady"
        elif state == "Early":
            review_state = "EarlyCrashReview"
        else:
            continue
        event_rows = earnings.loc[earnings.code.eq(str(code))].copy()
        event_annotation = None
        if not event_rows.empty:
            signal = pd.Timestamp(str(latest_ymd))
            event_rows["delta"] = event_rows.event_ymd.map(
                lambda value: (pd.Timestamp(str(int(value))) - signal).days
            )
            near = event_rows.loc[event_rows.delta.abs().le(3)].sort_values("delta", key=lambda values: values.abs())
            if not near.empty:
                event = near.iloc[0]
                event_annotation = {
                    "event_ymd": int(event.event_ymd),
                    "calendar_delta": int(event.delta),
                    "kind": str(event.kind),
                    "policy": "annotation_only",
                }
        rows.append({
            "code": str(code),
            "name": name_map.get(str(code)),
            "signal_ymd": latest_ymd,
            "signal_source": "confirmed_pan",
            "entry_timing_status": "NextOpenPending",
            "review_state": review_state,
            "signal_close": float(current.c),
            "ret1_pct": float(current.ret1 * 100),
            "episode_start_ymd": None if pd.isna(current.episode_start_ymd) else int(current.episode_start_ymd),
            "episode_age": None if pd.isna(current.episode_age) else int(current.episode_age),
            "episode_drop_pct": None if pd.isna(current.episode_drop) else float(current.episode_drop * 100),
            "ma20": float(current.ma20_current),
            "ma20_slope5_pct": float(current.ma20_slope5_pct),
            "close_vs_ma20_pct": float((current.c / current.ma20_current - 1) * 100),
            "price_band_ok": price_ok,
            "event_annotation": event_annotation,
        })
    priority = {"EntryReady": 0, "EarlyCrashReview": 1, "EarlyMonitor": 2, "Continuation": 3, "TrendPullbackBlock": 4, "LateChase": 5, "PriceBlock": 6}
    rows.sort(key=lambda row: (priority[row["review_state"]], row["ret1_pct"]))
    counts = pd.Series([row["review_state"] for row in rows]).value_counts().to_dict()
    result = {
        "schema_version": "tradex_short_entry_quality_current_board_v1.board.v1",
        "artifact_role": "authoritative_current_short_entry_quality_board",
        "review_only": True,
        "latest_as_of": latest_ymd,
        "source_status": "confirmed_pan_only",
        "fixed_conditions": rollup["fixed_conditions"],
        "counts": {str(key): int(value) for key, value in counts.items()},
        "candidates": rows,
        "authoritative_decision": "ready_current_entry_quality_board_review_only",
        "runtime_db_write": False,
        "meemee_modified": False,
        "production_ranking_modified": False,
    }
    board = a.output / "current_board.json"
    board.write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    audit = {
        "sources": {
            "db": {"path": str(a.db.resolve()), "read_only": True},
            "rollup_compare": {"path": str(a.rollup_compare.resolve()), "sha256": sha(a.rollup_compare)},
        },
        "latest_ymd": latest_ymd,
        "candidate_count": len(rows),
        "entry_ready_count": int(counts.get("EntryReady", 0)),
        "board_sha256": sha(board),
    }
    (a.output / "audit.json").write_text(json.dumps(audit, indent=2) + "\n", encoding="utf-8")
    (a.output / "_ARTIFACT_COMPLETE.json").write_text(
        json.dumps({"complete": True, "authoritative": "current_board.json", "sha256": sha(board)}, indent=2) + "\n",
        encoding="utf-8",
    )
    print(json.dumps({"output": str(a.output), "latest_ymd": latest_ymd, "counts": counts, "entry_ready": [row for row in rows if row["review_state"] == "EntryReady"]}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
