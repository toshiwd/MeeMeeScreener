from __future__ import annotations

import argparse
import itertools
import json
import math
import sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from statistics import mean, median
from typing import Any

import duckdb

if __package__ in {None, ""}:
    sys.path.append(str(Path(__file__).resolve().parents[1]))

from shared.runtime_stock_db_contract import inspect_runtime_stock_db, resolve_runtime_stock_db_path


AXIS_ID = "failed_high_retest_short_backtest_v1"
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\failed_high_retest_short_backtest_v1")


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _tag() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _write_json(path: Path, payload: Any) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str) + "\n", encoding="utf-8")


def _write_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    path.write_text("".join(json.dumps(row, ensure_ascii=False, default=str) + "\n" for row in rows), encoding="utf-8")


def _round(value: float | None, digits: int = 6) -> float | None:
    return None if value is None or not math.isfinite(value) else round(value, digits)


def _date_expr() -> str:
    return """
    CASE
        WHEN length(CAST(abs(date) AS VARCHAR)) = 8 THEN CAST(date AS INTEGER)
        ELSE CAST(strftime(to_timestamp(CAST(date AS BIGINT)), '%Y%m%d') AS INTEGER)
    END
    """


def _split(ymd: int, *, train_end_ymd: int, validation_end_ymd: int) -> str:
    if ymd <= train_end_ymd:
        return "train"
    if ymd <= validation_end_ymd:
        return "validation"
    return "test"


def _sma(values: list[float], end_idx: int, period: int) -> float | None:
    if end_idx + 1 < period:
        return None
    window = values[end_idx - period + 1:end_idx + 1]
    return sum(window) / len(window) if window else None


def _unit_score(value: float | None, *, lower: float, upper: float, default: float = 0.0) -> float:
    if value is None or not math.isfinite(value) or upper <= lower:
        return default
    return max(0.0, min(1.0, (value - lower) / (upper - lower)))


def _signal_for_bars(bars: list[dict[str, Any]], idx: int) -> dict[str, Any] | None:
    if idx < 119:
        return None
    anchor = bars[idx]
    prior_start = max(0, idx - 252)
    prior = bars[prior_start:idx]
    if len(prior) < 40:
        return None
    prior_high_local_idx = max(range(len(prior)), key=lambda pos: float(prior[pos]["high"]))
    prior_high_idx = prior_start + prior_high_local_idx
    peak_age = idx - prior_high_idx
    if peak_age < 20:
        return None
    prior_high = float(bars[prior_high_idx]["high"])
    if prior_high <= 0.0 or float(anchor["high"]) <= 0.0:
        return None
    after_peak = bars[prior_high_idx + 1:idx]
    if len(after_peak) < 10:
        return None
    pullback_low = min(float(row["low"]) for row in after_peak)
    pullback_depth = (prior_high - pullback_low) / prior_high
    if pullback_depth < 0.12:
        return None
    pre_anchor = bars[max(prior_high_idx + 1, idx - 12):idx]
    if max((float(row["high"]) for row in pre_anchor), default=0.0) >= prior_high * 1.01:
        return None

    closes = [float(row["close"]) for row in bars[:idx + 1]]
    ma7 = _sma(closes, idx, 7)
    ma20 = _sma(closes, idx, 20)
    ma20_prev_20 = _sma(closes, idx - 20, 20) if idx >= 20 else None
    ma20_slope_20 = (ma20 / ma20_prev_20 - 1.0) if ma20 and ma20_prev_20 and ma20_prev_20 > 0 else None
    if ma20_slope_20 is not None and ma20_slope_20 > 0.05:
        return None

    retest_ratio = float(anchor["high"]) / prior_high
    if retest_ratio < 0.88 or retest_ratio >= 1.01:
        return None
    range_size = float(anchor["high"]) - float(anchor["low"])
    if range_size <= 0.0 or float(anchor["open"]) <= 0.0:
        return None
    body = float(anchor["close"]) - float(anchor["open"])
    anchor_drop_pct = (float(anchor["open"]) - float(anchor["close"])) / float(anchor["open"])
    close_from_high = float(anchor["close"]) / float(anchor["high"])
    close_pos = (float(anchor["close"]) - float(anchor["low"])) / range_size
    bearish_body_ratio = (float(anchor["open"]) - float(anchor["close"])) / range_size

    left_peak_window = bars[max(0, prior_high_idx - 20):prior_high_idx]
    right_peak_window = bars[prior_high_idx + 1:min(idx, prior_high_idx + 21)]
    prior_peak_local_prominence = 0.0
    if len(left_peak_window) >= 5 and len(right_peak_window) >= 5:
        left_high = max(float(row["high"]) for row in left_peak_window)
        right_high = max(float(row["high"]) for row in right_peak_window)
        prior_peak_local_prominence = min((prior_high - left_high) / prior_high, (prior_high - right_high) / prior_high)

    near_retest = retest_ratio >= 0.92
    anchor_below_ma7 = bool(ma7 is not None and float(anchor["close"]) < ma7)
    bearish_rejection = bool(body < 0 and bearish_body_ratio >= 0.35 and close_pos <= 0.45 and close_from_high <= 0.95)
    if not near_retest and not (retest_ratio >= 0.90 and bearish_rejection):
        return None

    stage = "forming"
    stage_score = 0.58
    if bearish_rejection and anchor_below_ma7 and near_retest:
        stage = "confirmed"
        stage_score = 0.78
    if stage == "confirmed" and anchor_drop_pct >= 0.05:
        stage = "extended_drop"
        stage_score = 0.90
    if stage == "forming" and float(anchor["close"]) < float(anchor["open"]):
        stage_score += 0.06
    if prior_peak_local_prominence >= 0.01:
        stage_score += 0.04
    volume = float(anchor.get("volume") or 0.0)
    prev20 = bars[max(0, idx - 20):idx]
    avg_volume20 = sum(float(row.get("volume") or 0.0) for row in prev20) / len(prev20) if prev20 else None
    volume_ratio20 = volume / avg_volume20 if avg_volume20 and avg_volume20 > 0 else None
    score = max(0.0, min(1.0, stage_score + 0.10 * _unit_score(retest_ratio, lower=0.90, upper=1.0) + 0.06 * _unit_score(volume_ratio20, lower=1.2, upper=3.0, default=0.5)))
    return {
        "stage": stage,
        "score": score,
        "retest_ratio": retest_ratio,
        "anchor_drop_pct": anchor_drop_pct,
        "ma20_slope_20": ma20_slope_20,
        "peak_age": peak_age,
        "pullback_depth": pullback_depth,
        "peak_prominence": prior_peak_local_prominence,
        "volume_ratio20": volume_ratio20,
        "close_pos": close_pos,
        "bearish_body_ratio": bearish_body_ratio,
        "anchor_below_ma7": anchor_below_ma7,
        "bearish_rejection": bearish_rejection,
    }


def _outcome(bars: list[dict[str, Any]], idx: int, horizon: int) -> dict[str, float] | None:
    if idx + horizon >= len(bars):
        return None
    entry = float(bars[idx]["close"])
    future = bars[idx + 1:idx + horizon + 1]
    close_h = float(bars[idx + horizon]["close"])
    low = min(float(row["low"]) for row in future)
    high = max(float(row["high"]) for row in future)
    return {"ret": entry / close_h - 1.0, "mfe": entry / low - 1.0, "mae": entry / high - 1.0}


def _exit_outcome(
    bars: list[dict[str, Any]],
    idx: int,
    *,
    horizon: int,
    take_profit: float,
    stop_loss: float,
) -> dict[str, Any] | None:
    if idx + horizon >= len(bars):
        return None
    entry = float(bars[idx]["close"])
    if entry <= 0:
        return None
    take_price = entry * (1.0 - take_profit)
    stop_price = entry * (1.0 + stop_loss)
    for offset, row in enumerate(bars[idx + 1:idx + horizon + 1], start=1):
        hit_stop = float(row["high"]) >= stop_price
        hit_take = float(row["low"]) <= take_price
        if hit_stop and hit_take:
            return {"ret": -stop_loss, "win": False, "exit_reason": "same_day_stop_first", "exit_days": offset}
        if hit_stop:
            return {"ret": -stop_loss, "win": False, "exit_reason": "stop_loss", "exit_days": offset}
        if hit_take:
            return {"ret": take_profit, "win": True, "exit_reason": "take_profit", "exit_days": offset}
    close_h = float(bars[idx + horizon]["close"])
    ret = entry / close_h - 1.0
    return {"ret": ret, "win": ret > 0, "exit_reason": "time_exit", "exit_days": horizon}


def _atoms(row: dict[str, Any]) -> set[str]:
    s = row["signal"]
    atoms = {f"stage={s['stage']}"}
    if s["anchor_below_ma7"]:
        atoms.add("anchor_below_ma7=true")
    if s["bearish_rejection"]:
        atoms.add("bearish_rejection=true")
    for threshold in (0.62, 0.70, 0.78, 0.86):
        if s["score"] >= threshold:
            atoms.add(f"score>={threshold}")
    for threshold in (0.92, 0.95, 0.98):
        if s["retest_ratio"] >= threshold:
            atoms.add(f"retest_ratio>={threshold}")
    for threshold in (0.0, 0.006, 0.012, 0.02, 0.05):
        if s["anchor_drop_pct"] >= threshold:
            atoms.add(f"anchor_drop_pct>={threshold}")
    for threshold in (0.0, 0.02):
        value = s.get("ma20_slope_20")
        if value is not None and value <= threshold:
            atoms.add(f"ma20_slope_20<={threshold}")
    for threshold in (40, 80, 120):
        if s["peak_age"] >= threshold:
            atoms.add(f"peak_age>={threshold}")
    for threshold in (0.15, 0.20, 0.30):
        if s["pullback_depth"] >= threshold:
            atoms.add(f"pullback_depth>={threshold}")
    for threshold in (0.01, 0.03):
        if s["peak_prominence"] >= threshold:
            atoms.add(f"peak_prominence>={threshold}")
    for threshold in (1.0, 1.5, 2.0):
        value = s.get("volume_ratio20")
        if value is not None and value >= threshold:
            atoms.add(f"volume_ratio20>={threshold}")
    return atoms


def _empty() -> dict[str, Any]:
    return {"count": 0, "codes": set(), "ret": [], "wins": [], "mfe": [], "mae": []}


def _add(acc: dict[str, Any], row: dict[str, Any]) -> None:
    acc["count"] += 1
    acc["codes"].add(row["code"])
    acc["ret"].append(row["exit_ret_short"] if row.get("exit_ret_short") is not None else row["ret20_short"])
    acc["wins"].append(bool(row["exit_win"]) if row.get("exit_win") is not None else row["ret20_short"] > 0)
    acc["mfe"].append(row["mfe20_short"])
    acc["mae"].append(row["mae20_short"])


def _summary(acc: dict[str, Any]) -> dict[str, Any]:
    count = int(acc["count"])
    if count == 0:
        return {"count": 0}
    ret = acc["ret"]
    mfe = acc["mfe"]
    mae = acc["mae"]
    return {
        "count": count,
        "unique_code_count": len(acc["codes"]),
        "ret20_short_mean": _round(mean(ret)),
        "ret20_short_median": _round(median(ret)),
        "ret20_short_positive_rate": _round(sum(1 for value in acc["wins"] if value) / count),
        "mfe20_short_ge_8pct_rate": _round(sum(1 for value in mfe if value >= 0.08) / count),
        "mae20_short_le_minus5pct_rate": _round(sum(1 for value in mae if value <= -0.05) / count),
    }


def _load_codes(conn: duckdb.DuckDBPyConnection, start_ymd: int, end_ymd: int) -> list[tuple[str, str]]:
    return [(str(code), str(name or "")) for code, name in conn.execute(
        f"""
        SELECT b.code, any_value(t.name)
        FROM (SELECT code, {_date_expr()} AS ymd FROM daily_bars WHERE COALESCE(source, 'pan') = 'pan') b
        LEFT JOIN tickers t ON t.code = b.code
        WHERE b.ymd BETWEEN ? AND ?
          AND COALESCE(t.name, '') NOT ILIKE '%ETF%'
          AND COALESCE(t.name, '') NOT ILIKE '%ETN%'
          AND COALESCE(t.name, '') NOT ILIKE '%REIT%'
          AND COALESCE(t.name, '') NOT ILIKE '%(投)%'
          AND COALESCE(t.name, '') NOT ILIKE '%投資法人%'
          AND COALESCE(t.name, '') NOT ILIKE '%NEXT%'
        GROUP BY b.code
        HAVING COUNT(*) >= 260
        ORDER BY b.code
        """,
        [start_ymd, end_ymd],
    ).fetchall()]


def _load_bars(conn: duckdb.DuckDBPyConnection, code: str, start_ymd: int, end_ymd: int) -> list[dict[str, Any]]:
    return [
        {"date": int(d), "open": float(o), "high": float(h), "low": float(l), "close": float(c), "volume": float(v or 0)}
        for d, o, h, l, c, v in conn.execute(
            f"""
            WITH normalized AS (
                SELECT {_date_expr()} AS ymd, o, h, l, c, v
                FROM daily_bars
                WHERE code = ? AND COALESCE(source, 'pan') = 'pan'
            )
            SELECT ymd, o, h, l, c, v
            FROM normalized
            WHERE ymd BETWEEN ? AND ?
              AND o IS NOT NULL AND h IS NOT NULL AND l IS NOT NULL AND c IS NOT NULL
            ORDER BY ymd
            """,
            [code, start_ymd, end_ymd],
        ).fetchall()
    ]


def _candidate_key(candidate: dict[str, Any]) -> tuple[float, float, float, int]:
    return (
        float(candidate["by_split"].get("validation", {}).get("ret20_short_positive_rate") or 0),
        float(candidate["by_split"].get("train", {}).get("ret20_short_positive_rate") or 0),
        float(candidate["by_split"].get("test", {}).get("ret20_short_positive_rate") or 0),
        int(candidate["by_split"].get("validation", {}).get("count") or 0),
    )


def run(
    *,
    db_path: Path,
    output_root: Path,
    start_ymd: int,
    end_ymd: int,
    target_win_rate: float,
    min_train_count: int,
    min_validation_count: int,
    min_test_count: int,
    max_rule_size: int,
    train_end_ymd: int,
    validation_end_ymd: int,
    exit_take_profit: float | None,
    exit_stop_loss: float | None,
    exit_horizon: int,
) -> Path:
    output_dir = output_root / f"{_tag()}-{AXIS_ID}"
    output_dir.mkdir(parents=True, exist_ok=False)
    rows: list[dict[str, Any]] = []
    conn = duckdb.connect(str(db_path), read_only=True)
    try:
        code_names = _load_codes(conn, start_ymd, end_ymd)
        for code, name in code_names:
            bars = _load_bars(conn, code, start_ymd - 10000, end_ymd)
            for idx in range(119, len(bars) - 21):
                ymd = int(bars[idx]["date"])
                if not (start_ymd <= ymd <= end_ymd):
                    continue
                signal = _signal_for_bars(bars, idx)
                outcome20 = _outcome(bars, idx, 20) if signal else None
                if not signal or not outcome20:
                    continue
                exit20 = (
                    _exit_outcome(
                        bars,
                        idx,
                        horizon=exit_horizon,
                        take_profit=exit_take_profit,
                        stop_loss=exit_stop_loss,
                    )
                    if exit_take_profit is not None and exit_stop_loss is not None
                    else None
                )
                row = {
                    "code": code,
                    "name": name,
                    "as_of": ymd,
                    "split": _split(ymd, train_end_ymd=train_end_ymd, validation_end_ymd=validation_end_ymd),
                    "close": _round(float(bars[idx]["close"])),
                    "ret20_short": _round(outcome20["ret"]),
                    "mfe20_short": _round(outcome20["mfe"]),
                    "mae20_short": _round(outcome20["mae"]),
                    "exit_ret_short": _round(float(exit20["ret"])) if exit20 else None,
                    "exit_win": bool(exit20["win"]) if exit20 else None,
                    "exit_reason": str(exit20["exit_reason"]) if exit20 else None,
                    "exit_days": int(exit20["exit_days"]) if exit20 else None,
                    "signal": {key: _round(value) if isinstance(value, float) else value for key, value in signal.items()},
                }
                row["atoms"] = sorted(_atoms(row))
                rows.append(row)
        runtime_status = inspect_runtime_stock_db(runtime_db_path=db_path)
    finally:
        conn.close()

    aggregate: dict[tuple[str, ...], dict[str, dict[str, Any]]] = defaultdict(lambda: defaultdict(_empty))
    for row in rows:
        atoms = row["atoms"]
        for size in range(1, min(max_rule_size, len(atoms)) + 1):
            for combo in itertools.combinations(atoms, size):
                _add(aggregate[combo][row["split"]], row)
    candidates: list[dict[str, Any]] = []
    for rule, by_split_acc in aggregate.items():
        by_split = {split: _summary(acc) for split, acc in sorted(by_split_acc.items())}
        if int(by_split.get("train", {}).get("count") or 0) < min_train_count:
            continue
        if int(by_split.get("validation", {}).get("count") or 0) < min_validation_count:
            continue
        if int(by_split.get("test", {}).get("count") or 0) < min_test_count:
            continue
        all_acc = _empty()
        for acc in by_split_acc.values():
            all_acc["count"] += acc["count"]
            all_acc["codes"].update(acc["codes"])
            all_acc["ret"].extend(acc["ret"])
            all_acc["wins"].extend(acc["wins"])
            all_acc["mfe"].extend(acc["mfe"])
            all_acc["mae"].extend(acc["mae"])
        candidates.append({
            "rule_atoms": list(rule),
            "rule_size": len(rule),
            "by_split": by_split,
            "all": _summary(all_acc),
            "passes_target_all_splits": all(
                (by_split.get(split, {}).get("ret20_short_positive_rate") or 0) >= target_win_rate
                for split in ("train", "validation", "test")
            ),
        })
    candidates.sort(key=_candidate_key, reverse=True)
    stable = [candidate for candidate in candidates if candidate["passes_target_all_splits"]]
    _write_jsonl(output_dir / "failed_high_retest_events.jsonl", rows)
    _write_jsonl(output_dir / "failed_high_retest_rule_candidates_top.jsonl", candidates[:500])
    _write_jsonl(output_dir / "failed_high_retest_rule_candidates_stable_70.jsonl", stable[:200])
    audit = {
        "schema_version": "tradex_failed_high_retest_short_backtest_v1_audit",
        "generated_at": _utc_now(),
        "boundary_owner": "TRADEX",
        "axis_id": AXIS_ID,
        "db_path": str(db_path),
        "runtime_stock_db_status": runtime_status,
        "fixed_evaluation_conditions": {
            "source_logic": "code-verified rankings_cache._failed_high_retest_short_signal_for_bars replicated for historical bars",
            "instrument_filter": "exclude ETF/ETN/REIT/investment-corporation/NEXT-like names",
            "start_ymd": start_ymd,
            "end_ymd": end_ymd,
            "target": "exit_win_rate" if exit_take_profit is not None and exit_stop_loss is not None else "ret20_short_positive_rate",
            "target_win_rate": target_win_rate,
            "exit_policy": {
                "enabled": exit_take_profit is not None and exit_stop_loss is not None,
                "take_profit": exit_take_profit,
                "stop_loss": exit_stop_loss,
                "horizon": exit_horizon,
                "same_day_both_hit": "stop_first_conservative",
            },
            "train": f"as_of <= {train_end_ymd}",
            "validation": f"{train_end_ymd + 1} <= as_of <= {validation_end_ymd}",
            "test": f"as_of >= {validation_end_ymd + 1}",
            "min_train_count": min_train_count,
            "min_validation_count": min_validation_count,
            "min_test_count": min_test_count,
            "cost_slippage": "not applied",
        },
        "event_count": len(rows),
        "top_candidate": candidates[0] if candidates else None,
        "stable_70_candidate_count": len(stable),
        "authoritative_rollup_decision": "keep" if stable else "hold_no_stable_70pct_rule",
        "runtime_db_write": False,
        "production_ranking_changed": False,
        "non_scope": ["MeeMee reflection", "production ranking mutation", "runtime DB write"],
    }
    _write_json(output_dir / "failed_high_retest_backtest_audit.json", audit)
    _write_json(output_root / "latest_failed_high_retest_backtest_audit.json", {"run_root": str(output_dir), **audit})
    return output_dir


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db-path", type=Path, default=None)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--start-ymd", type=int, default=20200101)
    parser.add_argument("--end-ymd", type=int, default=20260629)
    parser.add_argument("--target-win-rate", type=float, default=0.70)
    parser.add_argument("--min-train-count", type=int, default=80)
    parser.add_argument("--min-validation-count", type=int, default=40)
    parser.add_argument("--min-test-count", type=int, default=15)
    parser.add_argument("--max-rule-size", type=int, default=3)
    parser.add_argument("--train-end-ymd", type=int, default=20241230)
    parser.add_argument("--validation-end-ymd", type=int, default=20251230)
    parser.add_argument("--exit-take-profit", type=float, default=None)
    parser.add_argument("--exit-stop-loss", type=float, default=None)
    parser.add_argument("--exit-horizon", type=int, default=20)
    args = parser.parse_args()
    print(run(
        db_path=args.db_path or resolve_runtime_stock_db_path(),
        output_root=args.output_root,
        start_ymd=args.start_ymd,
        end_ymd=args.end_ymd,
        target_win_rate=args.target_win_rate,
        min_train_count=args.min_train_count,
        min_validation_count=args.min_validation_count,
        min_test_count=args.min_test_count,
        max_rule_size=args.max_rule_size,
        train_end_ymd=args.train_end_ymd,
        validation_end_ymd=args.validation_end_ymd,
        exit_take_profit=args.exit_take_profit,
        exit_stop_loss=args.exit_stop_loss,
        exit_horizon=args.exit_horizon,
    ))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
