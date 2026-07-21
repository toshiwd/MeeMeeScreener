from __future__ import annotations

import argparse
import csv
import json
import math
import sys
from datetime import datetime, timezone
from pathlib import Path
from statistics import mean, median
from typing import Any

import duckdb

if __package__ in {None, ""}:
    sys.path.append(str(Path(__file__).resolve().parents[1]))

from shared.runtime_stock_db_contract import inspect_runtime_stock_db, resolve_runtime_stock_db_path


AXIS_ID = "short_ma60_to_ma100_breakdown_v1"
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\short_ma60_to_ma100_breakdown_v1")


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _tag() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str) + "\n", encoding="utf-8")


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fields: list[str] = []
    for row in rows:
        for key in row:
            if key not in fields:
                fields.append(key)
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def _clean(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
        return None
    if hasattr(value, "item"):
        return _clean(value.item())
    return value


def _round(value: float | None, digits: int = 6) -> float | None:
    if value is None or not math.isfinite(value):
        return None
    return round(value, digits)


def _pct(values: list[float], q: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    idx = (len(ordered) - 1) * q
    lo = int(math.floor(idx))
    hi = int(math.ceil(idx))
    if lo == hi:
        return ordered[lo]
    return ordered[lo] * (hi - idx) + ordered[hi] * (idx - lo)


FEATURE_SQL = r"""
WITH normalized AS (
  SELECT
    code,
    CASE
      WHEN date > 30000000 THEN CAST(strftime(to_timestamp(date), '%Y%m%d') AS INTEGER)
      ELSE CAST(date AS INTEGER)
    END AS ymd,
    o, h, l, c, v
  FROM daily_bars
  WHERE COALESCE(source, 'pan') <> 'yahoo'
    AND o > 0 AND h > 0 AND l > 0 AND c > 0
),
base AS (
  SELECT
    *,
    avg(c) OVER w20 AS ma20,
    avg(c) OVER w60 AS ma60,
    avg(c) OVER w100 AS ma100,
    avg(c) OVER w200 AS ma200,
    avg(v) OVER w20 AS vol20,
    lag(c, 1) OVER w AS prev_c,
    avg(c) OVER w60_prev AS prev_ma60,
    lag(c, 20) OVER w AS c_lag20,
    lag(c, 60) OVER w AS c_lag60,
    max(h) OVER (PARTITION BY code ORDER BY ymd ROWS BETWEEN 59 PRECEDING AND CURRENT ROW) AS high60,
    min(l) OVER (PARTITION BY code ORDER BY ymd ROWS BETWEEN 59 PRECEDING AND CURRENT ROW) AS low60,
    min(l) OVER (PARTITION BY code ORDER BY ymd ROWS BETWEEN 99 PRECEDING AND CURRENT ROW) AS low100
  FROM normalized
  WINDOW
    w AS (PARTITION BY code ORDER BY ymd),
    w20 AS (PARTITION BY code ORDER BY ymd ROWS BETWEEN 19 PRECEDING AND CURRENT ROW),
    w60 AS (PARTITION BY code ORDER BY ymd ROWS BETWEEN 59 PRECEDING AND CURRENT ROW),
    w100 AS (PARTITION BY code ORDER BY ymd ROWS BETWEEN 99 PRECEDING AND CURRENT ROW),
    w200 AS (PARTITION BY code ORDER BY ymd ROWS BETWEEN 199 PRECEDING AND CURRENT ROW),
    w60_prev AS (PARTITION BY code ORDER BY ymd ROWS BETWEEN 60 PRECEDING AND 1 PRECEDING)
),
projected AS (
  SELECT
    *,
    lag(ma60, 10) OVER w AS ma60_lag10,
    lead(ymd, 1) OVER w AS f1_ymd,
    lead(h, 1) OVER w AS f1_h,
    lead(l, 1) OVER w AS f1_l,
    lead(c, 1) OVER w AS f1_c,
    lead(ma60, 1) OVER w AS f1_ma60,
    lead(ma100, 1) OVER w AS f1_ma100,
    lead(low100, 1) OVER w AS f1_low100,
    lead(ymd, 2) OVER w AS f2_ymd,
    lead(h, 2) OVER w AS f2_h,
    lead(l, 2) OVER w AS f2_l,
    lead(c, 2) OVER w AS f2_c,
    lead(ma60, 2) OVER w AS f2_ma60,
    lead(ma100, 2) OVER w AS f2_ma100,
    lead(low100, 2) OVER w AS f2_low100,
    lead(ymd, 3) OVER w AS f3_ymd,
    lead(h, 3) OVER w AS f3_h,
    lead(l, 3) OVER w AS f3_l,
    lead(c, 3) OVER w AS f3_c,
    lead(ma60, 3) OVER w AS f3_ma60,
    lead(ma100, 3) OVER w AS f3_ma100,
    lead(low100, 3) OVER w AS f3_low100,
    lead(ymd, 4) OVER w AS f4_ymd,
    lead(h, 4) OVER w AS f4_h,
    lead(l, 4) OVER w AS f4_l,
    lead(c, 4) OVER w AS f4_c,
    lead(ma60, 4) OVER w AS f4_ma60,
    lead(ma100, 4) OVER w AS f4_ma100,
    lead(low100, 4) OVER w AS f4_low100,
    lead(ymd, 5) OVER w AS f5_ymd,
    lead(h, 5) OVER w AS f5_h,
    lead(l, 5) OVER w AS f5_l,
    lead(c, 5) OVER w AS f5_c,
    lead(ma60, 5) OVER w AS f5_ma60,
    lead(ma100, 5) OVER w AS f5_ma100,
    lead(low100, 5) OVER w AS f5_low100,
    lead(ymd, 6) OVER w AS f6_ymd,
    lead(h, 6) OVER w AS f6_h,
    lead(l, 6) OVER w AS f6_l,
    lead(c, 6) OVER w AS f6_c,
    lead(ma60, 6) OVER w AS f6_ma60,
    lead(ma100, 6) OVER w AS f6_ma100,
    lead(low100, 6) OVER w AS f6_low100,
    lead(ymd, 7) OVER w AS f7_ymd,
    lead(h, 7) OVER w AS f7_h,
    lead(l, 7) OVER w AS f7_l,
    lead(c, 7) OVER w AS f7_c,
    lead(ma60, 7) OVER w AS f7_ma60,
    lead(ma100, 7) OVER w AS f7_ma100,
    lead(low100, 7) OVER w AS f7_low100,
    lead(ymd, 8) OVER w AS f8_ymd,
    lead(h, 8) OVER w AS f8_h,
    lead(l, 8) OVER w AS f8_l,
    lead(c, 8) OVER w AS f8_c,
    lead(ma60, 8) OVER w AS f8_ma60,
    lead(ma100, 8) OVER w AS f8_ma100,
    lead(low100, 8) OVER w AS f8_low100,
    lead(ymd, 9) OVER w AS f9_ymd,
    lead(h, 9) OVER w AS f9_h,
    lead(l, 9) OVER w AS f9_l,
    lead(c, 9) OVER w AS f9_c,
    lead(ma60, 9) OVER w AS f9_ma60,
    lead(ma100, 9) OVER w AS f9_ma100,
    lead(low100, 9) OVER w AS f9_low100,
    lead(ymd, 10) OVER w AS f10_ymd,
    lead(h, 10) OVER w AS f10_h,
    lead(l, 10) OVER w AS f10_l,
    lead(c, 10) OVER w AS f10_c,
    lead(ma60, 10) OVER w AS f10_ma60,
    lead(ma100, 10) OVER w AS f10_ma100,
    lead(low100, 10) OVER w AS f10_low100
  FROM base
  WINDOW w AS (PARTITION BY code ORDER BY ymd)
),
events AS (
  SELECT
    *,
    CASE WHEN h > l THEN (c - l) / (h - l) ELSE NULL END AS close_pos,
    CASE WHEN h > l THEN abs(c - o) / (h - l) ELSE NULL END AS body_ratio,
    CASE WHEN vol20 > 0 THEN v / vol20 ELSE NULL END AS volume_vs20,
    c / NULLIF(ma60, 0) - 1 AS dist_ma60,
    c / NULLIF(ma100, 0) - 1 AS dist_ma100,
    ma60 / NULLIF(ma100, 0) - 1 AS ma60_vs_ma100,
    c / NULLIF(c_lag20, 0) - 1 AS ret20,
    c / NULLIF(c_lag60, 0) - 1 AS ret60,
    (c - low60) / NULLIF(high60 - low60, 0) AS range60_pos,
    ma60 / NULLIF(ma60_lag10, 0) - 1 AS ma60_slope10
  FROM projected
  WHERE ma60 IS NOT NULL
    AND ma100 IS NOT NULL
    AND prev_ma60 IS NOT NULL
    AND f10_c IS NOT NULL
    AND prev_c >= prev_ma60
    AND c < ma60
)
SELECT *
FROM events
ORDER BY ymd, code
"""


def _exit_metrics(row: dict[str, Any], horizon: int) -> dict[str, Any]:
    entry = float(row["c"])
    target_day: int | None = None
    stop_day: int | None = None
    target_kind: str | None = None
    target_ret: float | None = None
    best_mfe = 0.0
    worst_mae = 0.0
    close_h = float(row.get(f"f{horizon}_c") or entry)
    for day in range(1, horizon + 1):
        low = row.get(f"f{day}_l")
        high = row.get(f"f{day}_h")
        close = row.get(f"f{day}_c")
        ma60 = row.get(f"f{day}_ma60")
        ma100 = row.get(f"f{day}_ma100")
        low100 = row.get(f"f{day}_low100")
        if low is None or high is None or close is None:
            continue
        best_mfe = max(best_mfe, (entry - float(low)) / entry)
        worst_mae = min(worst_mae, (entry - float(high)) / entry)
        support = None
        kind = None
        if ma100 is not None:
            support = float(ma100)
            kind = "ma100"
        if low100 is not None:
            low100_f = float(low100)
            if support is None or low100_f > support:
                support = low100_f
                kind = "low100_support"
        if target_day is None and support is not None and float(low) <= support:
            target_day = day
            target_kind = kind
            target_ret = (entry - support) / entry
        if stop_day is None and ma60 is not None and float(close) >= float(ma60):
            stop_day = day
    first_event = "time"
    first_day = horizon
    if target_day is not None and (stop_day is None or target_day <= stop_day):
        first_event = f"target_{target_kind}"
        first_day = target_day
    elif stop_day is not None:
        first_event = "ma60_reclaim_stop"
        first_day = stop_day
    return {
        "target_hit_10d": target_day is not None,
        "target_day": target_day,
        "target_kind": target_kind,
        "ma60_reclaim_stop_10d": stop_day is not None,
        "stop_day": stop_day,
        "first_event": first_event,
        "first_event_day": first_day,
        "target_return_if_hit": target_ret,
        "close10_short_ret": (entry - close_h) / entry,
        "mfe10_short": best_mfe,
        "mae10_short": worst_mae,
    }


def _classify(row: dict[str, Any]) -> list[str]:
    tags = ["baseline_ma60_break_to_ma100_support"]
    close_pos = row.get("close_pos")
    body_ratio = row.get("body_ratio")
    volume_vs20 = row.get("volume_vs20")
    ret20 = row.get("ret20")
    ret60 = row.get("ret60")
    dist100 = row.get("dist_ma100")
    ma60_slope10 = row.get("ma60_slope10")
    if close_pos is not None and float(close_pos) <= 0.35:
        tags.append("weak_close")
    if float(row["c"]) < float(row["o"]) and body_ratio is not None and float(body_ratio) >= 0.45:
        tags.append("bearish_body")
    if volume_vs20 is not None and float(volume_vs20) >= 1.25:
        tags.append("volume_confirmed")
    if ret20 is not None and float(ret20) <= -0.08:
        tags.append("already_fast_drop20")
    if ret60 is not None and float(ret60) >= 0.15:
        tags.append("post_60d_rise")
    if dist100 is not None and float(dist100) >= 0.03:
        tags.append("room_to_ma100")
    if ma60_slope10 is not None and float(ma60_slope10) <= 0:
        tags.append("ma60_flat_or_down")
    if "weak_close" in tags and "room_to_ma100" in tags:
        tags.append("weak_close_room_to_ma100")
    if "bearish_body" in tags and "volume_confirmed" in tags:
        tags.append("bearish_volume_break")
    return tags


def _summary(rows: list[dict[str, Any]]) -> dict[str, Any]:
    if not rows:
        return {"event_count": 0}
    close10 = [float(row["close10_short_ret"]) for row in rows]
    mfe10 = [float(row["mfe10_short"]) for row in rows]
    mae10 = [float(row["mae10_short"]) for row in rows]
    target_returns = [float(row["target_return_if_hit"]) for row in rows if row.get("target_return_if_hit") is not None]
    months: dict[str, list[dict[str, Any]]] = {}
    years: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        month = str(row["as_of"])[:6]
        year = str(row["as_of"])[:4]
        months.setdefault(month, []).append(row)
        years.setdefault(year, []).append(row)
    target_hit_count = sum(1 for row in rows if row["target_hit_10d"])
    target_first_count = sum(1 for row in rows if str(row["first_event"]).startswith("target_"))
    stop_first_count = sum(1 for row in rows if row["first_event"] == "ma60_reclaim_stop")
    return {
        "event_count": len(rows),
        "unique_code_count": len({row["code"] for row in rows}),
        "target_hit_10d_rate": _round(target_hit_count / len(rows)),
        "target_first_rate": _round(target_first_count / len(rows)),
        "ma60_reclaim_stop_first_rate": _round(stop_first_count / len(rows)),
        "close10_short_ret_mean": _round(mean(close10)),
        "close10_short_ret_median": _round(median(close10)),
        "close10_short_positive_rate": _round(sum(1 for value in close10 if value > 0) / len(close10)),
        "mfe10_short_mean": _round(mean(mfe10)),
        "mfe10_short_p75": _round(_pct(mfe10, 0.75)),
        "mae10_short_mean": _round(mean(mae10)),
        "mae10_short_p10": _round(_pct(mae10, 0.10)),
        "target_return_if_hit_mean": _round(mean(target_returns)) if target_returns else None,
        "month_count": len(months),
        "year_count": len(years),
        "positive_month_rate": _round(
            sum(1 for group in months.values() if mean(float(row["close10_short_ret"]) for row in group) > 0) / len(months)
        ) if months else None,
        "target_hit_by_year": {
            year: _round(sum(1 for row in group if row["target_hit_10d"]) / len(group))
            for year, group in sorted(years.items())
        },
    }


def _decision(summary: dict[str, Any], baseline: dict[str, Any]) -> tuple[str, str]:
    n = int(summary.get("event_count") or 0)
    hit = float(summary.get("target_hit_10d_rate") or 0.0)
    first = float(summary.get("target_first_rate") or 0.0)
    stop_first = float(summary.get("ma60_reclaim_stop_first_rate") or 0.0)
    mean_ret = float(summary.get("close10_short_ret_mean") or 0.0)
    positive_month = float(summary.get("positive_month_rate") or 0.0)
    baseline_hit = float(baseline.get("target_hit_10d_rate") or 0.0)
    if n < 120:
        return "hold", "insufficient_breadth"
    if hit >= 0.55 and first >= 0.45 and stop_first <= 0.40 and mean_ret > 0 and positive_month >= 0.55 and hit >= baseline_hit + 0.03:
        return "keep", "target_and_return_profile_improved"
    if hit <= baseline_hit + 0.01 or mean_ret <= 0:
        return "drop", "no_same_condition_improvement"
    return "hold", "some_improvement_but_not_enough_for_keep"


def run(*, db_path: Path, output_root: Path, start_ymd: int, end_ymd: int | None, limit_events: int | None) -> Path:
    run_dir = output_root / f"{_tag()}-{AXIS_ID}"
    run_dir.mkdir(parents=True, exist_ok=False)
    with duckdb.connect(str(db_path), read_only=True) as conn:
        raw = conn.execute(FEATURE_SQL).fetchdf().to_dict("records")
    rows = [{key: _clean(value) for key, value in row.items()} for row in raw]
    rows = [row for row in rows if int(row["ymd"]) >= start_ymd and (end_ymd is None or int(row["ymd"]) <= end_ymd)]
    if limit_events:
        rows = rows[-int(limit_events):]

    events: list[dict[str, Any]] = []
    for row in rows:
        metrics = _exit_metrics(row, 10)
        tags = _classify(row)
        events.append({
            "code": str(row["code"]),
            "as_of": int(row["ymd"]),
            "entry_close": _round(float(row["c"])),
            "ma60": _round(float(row["ma60"])),
            "ma100": _round(float(row["ma100"])),
            "low100": _round(float(row["low100"])),
            "dist_ma100": _round(float(row["dist_ma100"])),
            "ma60_vs_ma100": _round(float(row["ma60_vs_ma100"])),
            "ret20": _round(float(row["ret20"])) if row.get("ret20") is not None else None,
            "ret60": _round(float(row["ret60"])) if row.get("ret60") is not None else None,
            "volume_vs20": _round(float(row["volume_vs20"])) if row.get("volume_vs20") is not None else None,
            "close_pos": _round(float(row["close_pos"])) if row.get("close_pos") is not None else None,
            "ma60_slope10": _round(float(row["ma60_slope10"])) if row.get("ma60_slope10") is not None else None,
            "tags": "|".join(tags),
            **{key: _round(value) if isinstance(value, float) else value for key, value in metrics.items()},
        })

    tag_rows: dict[str, list[dict[str, Any]]] = {}
    for event in events:
        for tag in str(event["tags"]).split("|"):
            tag_rows.setdefault(tag, []).append(event)
    baseline = _summary(tag_rows.get("baseline_ma60_break_to_ma100_support", []))
    leaderboard = []
    for tag, group in sorted(tag_rows.items()):
        summary = _summary(group)
        decision, reason = _decision(summary, baseline)
        leaderboard.append({"pattern_tag": tag, "decision": decision, "reason": reason, **summary})
    leaderboard.sort(
        key=lambda row: (
            {"keep": 2, "hold": 1, "drop": 0}.get(str(row["decision"]), 0),
            float(row.get("target_first_rate") or 0.0),
            float(row.get("target_hit_10d_rate") or 0.0),
            float(row.get("close10_short_ret_mean") or -999),
        ),
        reverse=True,
    )
    keep = [row for row in leaderboard if row["decision"] == "keep"]
    hold = [row for row in leaderboard if row["decision"] == "hold"]
    authoritative_decision = "keep" if keep else ("hold" if hold else "drop")
    authoritative_reason = (
        "at_least_one_filter_improved_target_first_under_fixed_conditions"
        if keep
        else "no_filter_met_keep_gate_but_some_need_review" if hold else "no_pattern_improved_enough"
    )
    compare = {
        "schema_version": f"{AXIS_ID}_compare_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "research_phase": "effectiveness_judgment",
        "boundary_owner": "TRADEX",
        "fixed_evaluation_conditions": {
            "universe": "runtime daily_bars confirmed non-yahoo rows",
            "period_start_ymd": start_ymd,
            "period_end_ymd": end_ymd,
            "event": "prev close >= prev 60MA and current close < current 60MA",
            "entry": "signal day close",
            "target": "daily low touches same-day 100MA or rolling 100-day low support within next 10 trading bars",
            "stop_observation": "daily close reclaims same-day 60MA within next 10 trading bars",
            "cost_slippage": "gross only",
            "provisional_intraday_used": False,
        },
        "baseline": baseline,
        "leaderboard": leaderboard,
        "candidate_local_decision": authoritative_decision,
        "authoritative_rollup_decision": authoritative_decision,
        "reason": authoritative_reason,
        "runtime_db_write": False,
        "meemee_unchanged": True,
        "production_ranking_changed": False,
    }
    rollup = {
        "schema_version": f"{AXIS_ID}_session_rollup_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "authoritative_rollup_decision": authoritative_decision,
        "reason": authoritative_reason,
        "top_patterns": leaderboard[:10],
        "artifact_refs": {
            "compare_json": str(run_dir / "compare.json"),
            "family_leaderboard_json": str(run_dir / "family_leaderboard.json"),
            "session_leaderboard_rollup_json": str(run_dir / "session_leaderboard_rollup.json"),
            "event_sample_csv": str(run_dir / "event_sample.csv"),
        },
    }
    audit = {
        "schema_version": f"{AXIS_ID}_audit_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "db_path": str(db_path),
        "runtime_stock_db_status": inspect_runtime_stock_db(runtime_db_path=db_path),
        "event_count": len(events),
        "tag_count": len(leaderboard),
        "no_runtime_mutation": True,
        "no_meemee_reflection": True,
        "no_provisional_intraday_mix": True,
    }
    _write_json(run_dir / "compare.json", compare)
    _write_json(run_dir / "family_leaderboard.json", {"schema_version": f"{AXIS_ID}_family_leaderboard_v1", "rows": leaderboard})
    _write_json(run_dir / "session_leaderboard_rollup.json", rollup)
    _write_json(run_dir / "audit.json", audit)
    _write_csv(run_dir / "event_sample.csv", events[:5000])
    complete = {
        "schema_version": f"{AXIS_ID}_artifact_complete_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "run_root": str(run_dir),
        "required_artifacts": ["compare.json", "family_leaderboard.json", "session_leaderboard_rollup.json", "audit.json", "event_sample.csv"],
        "all_present": all((run_dir / name).exists() for name in ["compare.json", "family_leaderboard.json", "session_leaderboard_rollup.json", "audit.json", "event_sample.csv"]),
        "authoritative_rollup_decision": authoritative_decision,
    }
    _write_json(run_dir / "_ARTIFACT_COMPLETE.json", complete)
    _write_json(output_root / "latest_session_leaderboard_rollup.json", {"run_root": str(run_dir), **rollup})
    return run_dir


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db-path", type=Path, default=None)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--start-ymd", type=int, default=20200101)
    parser.add_argument("--end-ymd", type=int, default=None)
    parser.add_argument("--limit-events", type=int, default=None)
    args = parser.parse_args()
    run_dir = run(
        db_path=args.db_path or resolve_runtime_stock_db_path(),
        output_root=args.output_root,
        start_ymd=args.start_ymd,
        end_ymd=args.end_ymd,
        limit_events=args.limit_events,
    )
    print(run_dir)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
