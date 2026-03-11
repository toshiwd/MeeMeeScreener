from __future__ import annotations

import argparse
import json
from dataclasses import dataclass, field
from pathlib import Path
import sys
from typing import Any

import duckdb
import numpy as np
import pandas as pd

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from app.core.config import config
from scripts.month_end_shape_study import (
    ROUND_TRIP_COST_DEFAULT,
    _bucket_cnt100,
    _bucket_cnt60,
    _bucket_dist_ma20,
    _box_state,
    _calc_ma_count_up,
    _detect_body_box,
    _rolling_sma,
    _safe_float,
    _summary_from_returns,
    _trend_bucket,
)


LONG_SIDE = "long"
SHORT_SIDE = "short"
SIDES = (LONG_SIDE, SHORT_SIDE)
TRIGGERS = ("stop3", "stop5", "ma20", "box", "negation_fast")
ACTIONS = ("exit", "doten_remainder", "doten_opt")


@dataclass(frozen=True)
class PatternSpec:
    pattern_id: str
    side: str
    horizon_days: int
    box_state: str
    trend_bucket: str
    dist_bucket: str
    cnt60_bucket: str
    cnt100_bucket: str
    entry_offset: str
    source_n: int
    source_mean: float
    source_pf: float
    source_quality: float

    @property
    def feature_key(self) -> tuple[str, str, str, str, str, str]:
        return (
            str(self.box_state),
            str(self.trend_bucket),
            str(self.dist_bucket),
            str(self.cnt60_bucket),
            str(self.cnt100_bucket),
            str(self.entry_offset),
        )

    @property
    def opposite_opt_days(self) -> int:
        return 10 if self.side == LONG_SIDE else 25

    @property
    def required_lookahead_days(self) -> int:
        return max(int(self.horizon_days), int(self.horizon_days) + int(self.opposite_opt_days))

    def to_dict(self) -> dict[str, Any]:
        return {
            "pattern_id": self.pattern_id,
            "side": self.side,
            "horizon_days": int(self.horizon_days),
            "source_n": int(self.source_n),
            "source_mean": float(self.source_mean),
            "source_pf": float(self.source_pf),
            "source_quality": float(self.source_quality),
            "shape": {
                "box_state": self.box_state,
                "trend_bucket": self.trend_bucket,
                "dist_bucket": self.dist_bucket,
                "cnt60_bucket": self.cnt60_bucket,
                "cnt100_bucket": self.cnt100_bucket,
                "entry_offset": self.entry_offset,
            },
        }


@dataclass
class PolicyAccumulator:
    seen: int = 0
    valid: int = 0
    triggered_valid: int = 0
    returns: list[float] = field(default_factory=list)
    hold_returns: list[float] = field(default_factory=list)

    def add(self, *, hold_ret: float, policy_ret: float | None, triggered: bool) -> None:
        self.seen += 1
        if policy_ret is None or not np.isfinite(policy_ret):
            return
        self.valid += 1
        if triggered:
            self.triggered_valid += 1
        self.returns.append(float(policy_ret))
        self.hold_returns.append(float(hold_ret))

    def summary(self) -> dict[str, Any]:
        arr = np.array(self.returns, dtype=np.float64) if self.returns else np.array([], dtype=np.float64)
        s = _summary_from_returns(arr)
        delta_mean = None
        improve_rate = None
        if self.returns and self.hold_returns:
            diff = np.array(self.returns, dtype=np.float64) - np.array(self.hold_returns, dtype=np.float64)
            delta_mean = float(np.mean(diff))
            improve_rate = float(np.mean(diff > 0.0))
        return {
            **s,
            "seen": int(self.seen),
            "valid": int(self.valid),
            "coverage": float(self.valid / self.seen) if self.seen > 0 else None,
            "trigger_rate": float(self.triggered_valid / self.valid) if self.valid > 0 else None,
            "delta_mean_vs_hold": delta_mean,
            "improve_rate_vs_hold": improve_rate,
        }


@dataclass
class PatternAccumulator:
    spec: PatternSpec
    baseline: PolicyAccumulator = field(default_factory=PolicyAccumulator)
    policies: dict[tuple[str, str], PolicyAccumulator] = field(default_factory=dict)

    def __post_init__(self) -> None:
        for trig in TRIGGERS:
            for action in ACTIONS:
                self.policies[(trig, action)] = PolicyAccumulator()


def _parse_float(v: Any) -> float:
    try:
        return float(v)
    except Exception:
        return float("nan")


def _load_pattern_specs(
    path: Path,
    *,
    per_side: int,
    min_n: int,
    min_pf: float,
    min_mean: float,
) -> list[PatternSpec]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    specs: list[PatternSpec] = []
    seen: set[tuple[str, int, str, str, str, str, str, str]] = set()
    for side in SIDES:
        rows = payload.get(side, {}).get("top_patterns_box_focus", [])
        rank = 0
        for row in rows:
            n = int(row.get("n") or 0)
            mean = _parse_float(row.get("mean"))
            pf = _parse_float(row.get("pf"))
            if n < int(min_n):
                continue
            if not np.isfinite(mean) or mean < float(min_mean):
                continue
            if not np.isfinite(pf) or pf < float(min_pf):
                continue
            shape = row.get("shape") or {}
            key = (
                str(side),
                int(row.get("horizon_days") or 0),
                str(shape.get("box_state")),
                str(shape.get("trend_bucket")),
                str(shape.get("dist_bucket")),
                str(shape.get("cnt60_bucket")),
                str(shape.get("cnt100_bucket")),
                str(shape.get("entry_offset")),
            )
            if key in seen:
                continue
            seen.add(key)
            rank += 1
            spec = PatternSpec(
                pattern_id=f"{side}_p{rank:02d}",
                side=str(side),
                horizon_days=int(row.get("horizon_days") or 0),
                box_state=str(shape.get("box_state")),
                trend_bucket=str(shape.get("trend_bucket")),
                dist_bucket=str(shape.get("dist_bucket")),
                cnt60_bucket=str(shape.get("cnt60_bucket")),
                cnt100_bucket=str(shape.get("cnt100_bucket")),
                entry_offset=str(shape.get("entry_offset")),
                source_n=n,
                source_mean=float(mean),
                source_pf=float(pf),
                source_quality=float(_parse_float(row.get("quality"))),
            )
            specs.append(spec)
            if rank >= int(per_side):
                break
    return specs


def _calc_side_ret(side: str, entry: float, exit_: float, *, round_trip_cost: float) -> float:
    if side == LONG_SIDE:
        return float((exit_ / entry) - 1.0 - round_trip_cost)
    return float(-((exit_ / entry) - 1.0) - round_trip_cost)


def _check_trigger(
    side: str,
    trigger: str,
    *,
    price: float,
    ma20: float | None,
    box_lower: float | None,
    box_upper: float | None,
    entry: float,
) -> bool:
    ret = (price / entry) - 1.0
    if side == LONG_SIDE:
        stop3 = ret <= -0.03
        stop5 = ret <= -0.05
        ma20_break = (ma20 is not None and np.isfinite(ma20) and price < ma20)
        box_break = (box_lower is not None and np.isfinite(box_lower) and price < box_lower)
        if trigger == "stop3":
            return bool(stop3)
        if trigger == "stop5":
            return bool(stop5)
        if trigger == "ma20":
            return bool(ma20_break)
        if trigger == "box":
            return bool(box_break)
        if trigger == "negation_fast":
            return bool(stop3 or ma20_break or box_break)
        return False

    stop3 = ret >= 0.03
    stop5 = ret >= 0.05
    ma20_retake = (ma20 is not None and np.isfinite(ma20) and price > ma20)
    box_reclaim = (box_upper is not None and np.isfinite(box_upper) and price > box_upper)
    if trigger == "stop3":
        return bool(stop3)
    if trigger == "stop5":
        return bool(stop5)
    if trigger == "ma20":
        return bool(ma20_retake)
    if trigger == "box":
        return bool(box_reclaim)
    if trigger == "negation_fast":
        return bool(stop3 or ma20_retake or box_reclaim)
    return False


def _first_trigger_day(
    side: str,
    trigger: str,
    *,
    close_path: np.ndarray,
    ma20_path: np.ndarray,
    horizon_days: int,
    entry: float,
    box_lower: float | None,
    box_upper: float | None,
) -> int | None:
    max_d = min(int(horizon_days), int(len(close_path) - 1))
    for d in range(1, max_d + 1):
        price = _safe_float(close_path[d])
        if price is None or price <= 0:
            continue
        ma20 = _safe_float(ma20_path[d]) if d < len(ma20_path) else None
        if _check_trigger(
            side,
            trigger,
            price=price,
            ma20=ma20,
            box_lower=box_lower,
            box_upper=box_upper,
            entry=entry,
        ):
            return int(d)
    return None


def _policy_return(
    spec: PatternSpec,
    *,
    close_path: np.ndarray,
    trigger_day: int | None,
    action: str,
    round_trip_cost: float,
) -> tuple[float | None, bool]:
    h = int(spec.horizon_days)
    if h >= len(close_path):
        return None, False
    entry = _safe_float(close_path[0])
    if entry is None or entry <= 0:
        return None, False
    hold_exit = _safe_float(close_path[h])
    if hold_exit is None or hold_exit <= 0:
        return None, False
    hold_ret = _calc_side_ret(spec.side, entry, hold_exit, round_trip_cost=round_trip_cost)
    if trigger_day is None:
        return hold_ret, False

    d = int(trigger_day)
    if d <= 0 or d >= len(close_path):
        return None, True
    stop_exit = _safe_float(close_path[d])
    if stop_exit is None or stop_exit <= 0:
        return None, True
    first_leg = _calc_side_ret(spec.side, entry, stop_exit, round_trip_cost=round_trip_cost)
    if action == "exit":
        return first_leg, True

    opp_side = SHORT_SIDE if spec.side == LONG_SIDE else LONG_SIDE
    if action == "doten_remainder":
        if d >= h:
            return first_leg, True
        exit_remain = _safe_float(close_path[h])
        if exit_remain is None or exit_remain <= 0:
            return None, True
        second_leg = _calc_side_ret(opp_side, stop_exit, exit_remain, round_trip_cost=round_trip_cost)
        return float(first_leg + second_leg), True

    if action == "doten_opt":
        target = d + int(spec.opposite_opt_days)
        if target >= len(close_path):
            return None, True
        exit_opt = _safe_float(close_path[target])
        if exit_opt is None or exit_opt <= 0:
            return None, True
        second_leg = _calc_side_ret(opp_side, stop_exit, exit_opt, round_trip_cost=round_trip_cost)
        return float(first_leg + second_leg), True

    return hold_ret, True


def _finalize_pattern_result(acc: PatternAccumulator) -> dict[str, Any]:
    baseline = acc.baseline.summary()
    rows: list[dict[str, Any]] = []
    for trig in TRIGGERS:
        for action in ACTIONS:
            s = acc.policies[(trig, action)].summary()
            rows.append({"trigger": trig, "action": action, **s})
    rows.sort(
        key=lambda r: (
            -(r.get("delta_mean_vs_hold") or -999.0),
            -(r.get("quality") or -999.0),
            -(r.get("mean") or -999.0),
        )
    )
    return {
        "spec": acc.spec.to_dict(),
        "baseline_hold": baseline,
        "policies": rows,
        "best_policy_by_delta": rows[0] if rows else None,
    }


def run_study(
    *,
    pattern_source: Path,
    patterns_per_side: int,
    min_source_n: int,
    min_source_pf: float,
    min_source_mean: float,
    round_trip_cost: float,
) -> dict[str, Any]:
    specs = _load_pattern_specs(
        pattern_source,
        per_side=patterns_per_side,
        min_n=min_source_n,
        min_pf=min_source_pf,
        min_mean=min_source_mean,
    )
    if not specs:
        return {
            "meta": {
                "pattern_source": str(pattern_source),
                "patterns_per_side": int(patterns_per_side),
                "round_trip_cost": float(round_trip_cost),
            },
            "error": "no_pattern_specs",
        }

    specs_by_key: dict[tuple[str, str, str, str, str, str], list[PatternSpec]] = {}
    max_lookahead = 1
    for spec in specs:
        specs_by_key.setdefault(spec.feature_key, []).append(spec)
        max_lookahead = max(max_lookahead, int(spec.required_lookahead_days))

    pattern_acc: dict[str, PatternAccumulator] = {spec.pattern_id: PatternAccumulator(spec=spec) for spec in specs}
    side_acc: dict[str, dict[tuple[str, str], PolicyAccumulator]] = {LONG_SIDE: {}, SHORT_SIDE: {}}
    for side in SIDES:
        for trig in TRIGGERS:
            for action in ACTIONS:
                side_acc[side][(trig, action)] = PolicyAccumulator()
    side_base: dict[str, PolicyAccumulator] = {LONG_SIDE: PolicyAccumulator(), SHORT_SIDE: PolicyAccumulator()}

    with duckdb.connect(str(config.DB_PATH)) as con:
        daily = con.execute(
            """
            SELECT
                b.code,
                b.date,
                CAST(b.c AS DOUBLE) AS close,
                CAST(m.ma20 AS DOUBLE) AS ma20,
                CAST(m.ma60 AS DOUBLE) AS ma60
            FROM daily_bars b
            LEFT JOIN daily_ma m ON m.code = b.code AND m.date = b.date
            ORDER BY b.code, b.date
            """
        ).fetchdf()
        monthly = con.execute(
            """
            SELECT
                code,
                month,
                CAST(o AS DOUBLE) AS o,
                CAST(h AS DOUBLE) AS h,
                CAST(l AS DOUBLE) AS l,
                CAST(c AS DOUBLE) AS c
            FROM monthly_bars
            ORDER BY code, month
            """
        ).fetchdf()

    daily["code"] = daily["code"].astype(str)
    daily["dt"] = pd.to_datetime(daily["date"], unit="s", utc=True).dt.tz_localize(None)
    daily["month"] = daily["dt"].dt.to_period("M")
    monthly["code"] = monthly["code"].astype(str)
    monthly["dt"] = pd.to_datetime(monthly["month"], unit="s", utc=True).dt.tz_localize(None)
    monthly["period"] = monthly["dt"].dt.to_period("M")

    daily_groups = {str(code): group.copy() for code, group in daily.groupby("code", sort=False)}
    monthly_groups = {str(code): group.copy() for code, group in monthly.groupby("code", sort=False)}
    code_list = sorted(daily_groups.keys())

    events_matched = 0
    events_skipped_lookahead = 0

    for code in code_list:
        d = daily_groups.get(code)
        if d.empty or len(d) < max(260, max_lookahead + 40):
            continue
        closes = d["close"].to_numpy(dtype=np.float64, copy=False)
        ma20 = d["ma20"].to_numpy(dtype=np.float64, copy=False)
        ma60 = d["ma60"].to_numpy(dtype=np.float64, copy=False)
        ma100 = _rolling_sma(closes, 100)
        cnt60 = _calc_ma_count_up(closes, _rolling_sma(closes, 60))
        cnt100 = _calc_ma_count_up(closes, ma100)

        month_to_indices: dict[pd.Period, list[int]] = {}
        month_seq = d["month"].tolist()
        for i, month_key in enumerate(month_seq):
            month_to_indices.setdefault(month_key, []).append(i)
        ordered_months = sorted(month_to_indices.keys())
        if len(ordered_months) < 2:
            continue

        m = monthly_groups.get(code)
        m_box_by_period: dict[pd.Period, dict[str, Any] | None] = {}
        if not m.empty:
            m = m.sort_values("period")
            monthly_rows = list(
                zip(
                    m["month"].astype(int).tolist(),
                    m["o"].astype(float).tolist(),
                    m["h"].astype(float).tolist(),
                    m["l"].astype(float).tolist(),
                    m["c"].astype(float).tolist(),
                )
            )
            periods = m["period"].tolist()
            for j, period in enumerate(periods):
                m_box_by_period[period] = _detect_body_box(monthly_rows[: j + 1])

        for mi in range(len(ordered_months) - 1):
            month_key = ordered_months[mi]
            idxs = month_to_indices.get(month_key, [])
            if not idxs:
                continue
            prev_month = month_key - 1
            box = m_box_by_period.get(prev_month)
            box_lower = _safe_float(box.get("lower")) if box else None
            box_upper = _safe_float(box.get("upper")) if box else None

            for offset in (2, 1, 0):
                if len(idxs) <= offset:
                    continue
                entry_idx = idxs[-(offset + 1)]
                entry_close = _safe_float(closes[entry_idx])
                if entry_close is None or entry_close <= 0:
                    continue
                ma20_i = _safe_float(ma20[entry_idx])
                ma60_i = _safe_float(ma60[entry_idx])
                ma100_i = _safe_float(ma100[entry_idx])
                dist_ma20 = None
                if ma20_i is not None and ma20_i > 0:
                    dist_ma20 = (entry_close - ma20_i) / ma20_i
                trend = _trend_bucket(entry_close, ma20_i, ma60_i, ma100_i)
                state, _ = _box_state(entry_close, box)
                key = (
                    state,
                    trend,
                    _bucket_dist_ma20(dist_ma20),
                    _bucket_cnt60(float(cnt60[entry_idx])),
                    _bucket_cnt100(float(cnt100[entry_idx])),
                    f"M-{3 - offset}",
                )
                matched_specs = specs_by_key.get(key, [])
                if not matched_specs:
                    continue
                for spec in matched_specs:
                    req = int(spec.required_lookahead_days)
                    if (entry_idx + req) >= len(closes):
                        events_skipped_lookahead += 1
                        continue

                    path = closes[entry_idx : entry_idx + req + 1]
                    path_ma20 = ma20[entry_idx : entry_idx + req + 1]
                    hold_h = int(spec.horizon_days)
                    hold_exit = _safe_float(path[hold_h])
                    if hold_exit is None or hold_exit <= 0:
                        continue
                    hold_ret = _calc_side_ret(spec.side, entry_close, hold_exit, round_trip_cost=round_trip_cost)

                    events_matched += 1
                    pa = pattern_acc[spec.pattern_id]
                    pa.baseline.add(hold_ret=hold_ret, policy_ret=hold_ret, triggered=False)
                    side_base[spec.side].add(hold_ret=hold_ret, policy_ret=hold_ret, triggered=False)

                    trigger_days: dict[str, int | None] = {}
                    for trig in TRIGGERS:
                        trigger_days[trig] = _first_trigger_day(
                            spec.side,
                            trig,
                            close_path=path,
                            ma20_path=path_ma20,
                            horizon_days=spec.horizon_days,
                            entry=entry_close,
                            box_lower=box_lower,
                            box_upper=box_upper,
                        )
                    for trig in TRIGGERS:
                        t_day = trigger_days[trig]
                        for action in ACTIONS:
                            pol_ret, triggered = _policy_return(
                                spec,
                                close_path=path,
                                trigger_day=t_day,
                                action=action,
                                round_trip_cost=round_trip_cost,
                            )
                            pa.policies[(trig, action)].add(
                                hold_ret=hold_ret,
                                policy_ret=pol_ret,
                                triggered=triggered,
                            )
                            side_acc[spec.side][(trig, action)].add(
                                hold_ret=hold_ret,
                                policy_ret=pol_ret,
                                triggered=triggered,
                            )

    pattern_rows = [_finalize_pattern_result(acc) for acc in pattern_acc.values()]
    pattern_rows.sort(
        key=lambda r: (
            -int((r.get("baseline_hold") or {}).get("n") or 0),
            -float((r.get("baseline_hold") or {}).get("quality") or -999.0),
        )
    )

    side_rows: dict[str, Any] = {}
    for side in SIDES:
        rows: list[dict[str, Any]] = []
        for trig in TRIGGERS:
            for action in ACTIONS:
                s = side_acc[side][(trig, action)].summary()
                rows.append({"trigger": trig, "action": action, **s})
        rows.sort(
            key=lambda r: (
                -(r.get("delta_mean_vs_hold") or -999.0),
                -(r.get("quality") or -999.0),
                -(r.get("mean") or -999.0),
            )
        )
        side_rows[side] = {
            "baseline_hold": side_base[side].summary(),
            "policy_rank": rows,
            "best_policy_by_delta": rows[0] if rows else None,
        }

    return {
        "meta": {
            "pattern_source": str(pattern_source),
            "patterns_per_side": int(patterns_per_side),
            "round_trip_cost": float(round_trip_cost),
            "code_count": int(len(code_list)),
            "events_matched": int(events_matched),
            "events_skipped_lookahead": int(events_skipped_lookahead),
            "max_required_lookahead_days": int(max_lookahead),
        },
        "patterns": pattern_rows,
        "side_summary": side_rows,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Entry invalidation and doten study for month-end setups")
    parser.add_argument(
        "--pattern-source",
        type=Path,
        default=Path("tmp/short_long_holding_recommendation_20260227.json"),
    )
    parser.add_argument("--patterns-per-side", type=int, default=8)
    parser.add_argument("--min-source-n", type=int, default=1200)
    parser.add_argument("--min-source-pf", type=float, default=1.0)
    parser.add_argument("--min-source-mean", type=float, default=0.0)
    parser.add_argument("--round-trip-cost", type=float, default=ROUND_TRIP_COST_DEFAULT)
    parser.add_argument("--output", type=Path, default=Path("tmp/entry_invalidation_doten_study.json"))
    args = parser.parse_args()

    result = run_study(
        pattern_source=Path(args.pattern_source),
        patterns_per_side=int(args.patterns_per_side),
        min_source_n=int(args.min_source_n),
        min_source_pf=float(args.min_source_pf),
        min_source_mean=float(args.min_source_mean),
        round_trip_cost=float(args.round_trip_cost),
    )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"[entry_invalidation_doten_study] wrote {args.output}")
    print(json.dumps(result.get("meta", {}), ensure_ascii=False))
    if "error" in result:
        print("[entry_invalidation_doten_study] error=", result.get("error"))
        return
    side_summary = result.get("side_summary", {})
    print(
        "[entry_invalidation_doten_study] side_best=",
        {
            side: (side_summary.get(side, {}) or {}).get("best_policy_by_delta")
            for side in SIDES
        },
    )


if __name__ == "__main__":
    main()
