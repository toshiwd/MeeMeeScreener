from __future__ import annotations

import argparse
import json
import random
import time
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from app.backend.services import strategy_backtest_service


def _utc_now_iso() -> str:
    return datetime.now(tz=timezone.utc).isoformat()


def _json_default(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()
    return str(value)


def _append_jsonl(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload, ensure_ascii=False, default=_json_default))
        handle.write("\n")


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=_json_default), encoding="utf-8")


def _load_last_iteration(path: Path) -> int:
    if not path.exists():
        return 0
    last_iteration = 0
    with path.open("r", encoding="utf-8", errors="replace") as handle:
        for raw_line in handle:
            line = raw_line.strip()
            if not line:
                continue
            try:
                payload = json.loads(line)
            except Exception:
                continue
            if str(payload.get("event")) != "run_complete":
                continue
            try:
                last_iteration = max(last_iteration, int(payload.get("iteration") or 0))
            except Exception:
                continue
    return int(last_iteration)


def _load_best_score(path: Path) -> float | None:
    if not path.exists():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None
    try:
        return float(((payload.get("evaluation") or {}).get("score")))
    except Exception:
        return None


def _choose_optional_tuple(rng: random.Random, options: list[tuple[str, ...] | None]) -> tuple[str, ...] | None:
    value = rng.choice(options)
    return tuple(value) if value else None


def _sample_config(rng: random.Random) -> strategy_backtest_service.StrategyBacktestConfig:
    allowed_sides = rng.choice(["both", "long", "short"])
    long_setups = _choose_optional_tuple(
        rng,
        [
            None,
            ("long_breakout_p2",),
            ("long_reversal_p1",),
            ("long_breakout_p2", "long_reversal_p1"),
        ],
    )
    short_setups = _choose_optional_tuple(
        rng,
        [
            None,
            ("short_crash_top_p3", "short_failed_high_p1", "short_box_fail_p2"),
            ("short_box_fail_p2", "short_downtrend_p4"),
            ("short_crash_top_p3",),
            ("short_failed_high_p1",),
        ],
    )
    min_long_score = rng.choice([1.0, 1.25, 1.5, 2.0])
    min_short_score = rng.choice([1.0, 1.5, 2.0])
    if allowed_sides == "long":
        min_short_score = 99.0
        short_setups = None
    elif allowed_sides == "short":
        min_long_score = 99.0
        long_setups = None
    return strategy_backtest_service.StrategyBacktestConfig(
        max_positions=3,
        initial_units=1,
        add1_units=1,
        add2_units=1,
        hedge_units=1,
        min_hedge_ratio=0.2,
        cost_bps=20.0,
        min_history_bars=220,
        prefer_net_short_ratio=2.0,
        event_lookback_days=2,
        event_lookahead_days=1,
        min_long_score=float(min_long_score),
        min_short_score=float(min_short_score),
        max_new_entries_per_day=int(rng.choice([1, 2, 3])),
        max_new_entries_per_month=None,
        allowed_sides=allowed_sides,
        require_decision_for_long=False,
        require_ma_bull_stack_long=False,
        max_dist_ma20_long=None,
        min_volume_ratio_long=0.0,
        max_atr_pct_long=None,
        min_ml_p_up_long=None,
        allowed_long_setups=long_setups,
        allowed_short_setups=short_setups,
        use_regime_filter=bool(rng.choice([False, True])),
        regime_breadth_lookback_days=20,
        regime_long_min_breadth_above60=0.52,
        regime_short_max_breadth_above60=0.48,
        range_bias_width_min=float(rng.choice([0.06, 0.08, 0.10])),
        range_bias_long_pos_min=float(rng.choice([0.55, 0.60, 0.65])),
        range_bias_short_pos_max=float(rng.choice([0.35, 0.40, 0.45])),
        ma20_count20_min_long=int(rng.choice([10, 12, 15])),
        ma20_count20_min_short=12,
        ma60_count60_min_long=int(rng.choice([24, 30, 36])),
        ma60_count60_min_short=30,
    )


def _build_hedge_profile(report: dict[str, Any]) -> dict[str, float]:
    rows = (
        (((report.get("attribution") or {}).get("hedge") or {}).get("rows"))
        if isinstance(report.get("attribution"), dict)
        else None
    )
    core_trades = 0.0
    hedge_trades = 0.0
    core_ret = 0.0
    hedge_ret = 0.0
    if isinstance(rows, list):
        for row in rows:
            if not isinstance(row, dict):
                continue
            key = str(row.get("key") or "").lower()
            trades = float(row.get("trades") or 0.0)
            ret_sum = float(row.get("ret_net_sum") or 0.0)
            if key == "hedge":
                hedge_trades += trades
                hedge_ret += ret_sum
            else:
                core_trades += trades
                core_ret += ret_sum
    total_trades = float(core_trades + hedge_trades)
    total_ret = float(core_ret + hedge_ret)
    hedge_trade_ratio = float(hedge_trades / total_trades) if total_trades > 0 else 0.0
    hedge_ret_share = float(hedge_ret / total_ret) if total_ret != 0 else 0.0
    return {
        "core_trades": float(core_trades),
        "hedge_trades": float(hedge_trades),
        "total_trades": float(total_trades),
        "hedge_trade_ratio": float(hedge_trade_ratio),
        "core_ret_sum": float(core_ret),
        "hedge_ret_sum": float(hedge_ret),
        "hedge_ret_share": float(hedge_ret_share),
    }


def _evaluate_report(
    report: dict[str, Any],
    *,
    gate_result: dict[str, Any],
    config: strategy_backtest_service.StrategyBacktestConfig,
) -> dict[str, Any]:
    summary = report.get("summary") if isinstance(report.get("summary"), dict) else {}
    pnl = float(summary.get("oos_total_realized_unit_pnl") or 0.0)
    pf = float(summary.get("oos_mean_profit_factor") or 0.0)
    win_window_ratio = float(summary.get("oos_positive_window_ratio") or 0.0)
    worst_dd = float(summary.get("oos_worst_max_drawdown_unit") or 0.0)
    trades = int(summary.get("oos_trade_events") or 0)
    weighted_win_rate = float(summary.get("oos_weighted_win_rate") or 0.0)
    hedge_profile = _build_hedge_profile(report)

    entry_score = 0.0
    if config.allowed_sides in {"both", "long"}:
        entry_score += min(2.0, max(0.0, float(config.min_long_score)))
    if config.allowed_sides in {"both", "short"}:
        short_score = 0.0 if float(config.min_short_score) >= 99.0 else max(0.0, float(config.min_short_score))
        entry_score += min(2.0, short_score)
    entry_score = min(2.0, entry_score)

    hold_score = float((pf - 1.0) * 8.0 + (win_window_ratio - 0.4) * 6.0 + (weighted_win_rate - 0.5) * 4.0)
    hedge_score = float(
        hedge_profile["hedge_ret_sum"] * 4.0
        + hedge_profile["hedge_trade_ratio"] * 5.0
        + max(0.0, hedge_profile["hedge_ret_share"]) * 2.0
    )
    take_profit_score = float(pnl / max(1.0, trades / 1000.0))
    score = float(
        pnl
        + (pf - 1.0) * 10.0
        + (win_window_ratio - 0.4) * 8.0
        + worst_dd * 3.0
        + hedge_score
        + take_profit_score
    )

    return {
        "pnl": float(pnl),
        "pf": float(pf),
        "win_window_ratio": float(win_window_ratio),
        "worst_dd": float(worst_dd),
        "gate_pass": bool(gate_result.get("passed")),
        "score": float(score),
        "trades": int(trades),
        "weighted_win_rate": float(weighted_win_rate),
        "entry_score": float(entry_score),
        "hold_score": float(hold_score),
        "hedge_score": float(hedge_score),
        "take_profit_score": float(take_profit_score),
        "hedge_profile": hedge_profile,
    }


def _should_prune_probe(
    probe_report: dict[str, Any],
    *,
    worst_dd_threshold: float,
) -> tuple[bool, str | None]:
    execution = probe_report.get("execution") if isinstance(probe_report.get("execution"), dict) else {}
    summary = probe_report.get("summary") if isinstance(probe_report.get("summary"), dict) else {}
    if str(execution.get("truncated_reason")) == "oos_worst_max_drawdown_below_threshold":
        return True, "oos_worst_max_drawdown_below_threshold"
    worst_dd = summary.get("oos_worst_max_drawdown_unit")
    try:
        if worst_dd is not None and float(worst_dd) < float(worst_dd_threshold):
            return True, "oos_worst_max_drawdown_below_threshold"
    except Exception:
        pass
    return False, None


def _done_reached(
    payload: dict[str, Any],
    *,
    done_require_max_codes: int,
    done_min_trades: int,
    done_min_hedge_trades: int,
) -> bool:
    evaluation = payload.get("evaluation") if isinstance(payload.get("evaluation"), dict) else {}
    hedge_profile = evaluation.get("hedge_profile") if isinstance(evaluation, dict) else {}
    return bool(
        int(payload.get("max_codes") or 0) >= int(done_require_max_codes)
        and bool(evaluation.get("gate_pass"))
        and int(evaluation.get("trades") or 0) >= int(done_min_trades)
        and int((hedge_profile or {}).get("hedge_trades") or 0) >= int(done_min_hedge_trades)
    )


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Continuous walkforward research worker")
    parser.add_argument("--max-runs", type=int, default=0)
    parser.add_argument("--sleep-seconds", type=float, default=10.0)
    parser.add_argument("--max-codes-choices", default="150,250,350,500")
    parser.add_argument("--step-months", type=int, default=12)
    parser.add_argument("--progress-jsonl", required=True)
    parser.add_argument("--best-json", required=True)
    parser.add_argument("--stop-file", required=True)
    parser.add_argument("--done-file", required=True)
    parser.add_argument("--done-require-max-codes", type=int, default=500)
    parser.add_argument("--done-min-trades", type=int, default=1200)
    parser.add_argument("--done-min-hedge-trades", type=int, default=80)
    parser.add_argument("--seed", type=int, default=None)
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    progress_path = Path(args.progress_jsonl)
    best_path = Path(args.best_json)
    stop_path = Path(args.stop_file)
    done_path = Path(args.done_file)
    max_codes_choices = [int(part.strip()) for part in str(args.max_codes_choices).split(",") if part.strip()]
    if not max_codes_choices:
        raise ValueError("--max-codes-choices must contain at least one value")

    rng = random.Random(args.seed)
    iteration = _load_last_iteration(progress_path)
    best_score = _load_best_score(best_path)

    _append_jsonl(
        progress_path,
        {
            "ts": _utc_now_iso(),
            "event": "worker_start",
            "iteration": int(iteration),
            "max_runs": int(args.max_runs),
            "step_months": int(args.step_months),
            "max_codes_choices": max_codes_choices,
        },
    )

    run_count = 0
    while True:
        if stop_path.exists():
            _append_jsonl(
                progress_path,
                {
                    "ts": _utc_now_iso(),
                    "event": "stop_file_detected",
                    "path": str(stop_path),
                },
            )
            break
        if int(args.max_runs) > 0 and run_count >= int(args.max_runs):
            break

        started_at = time.perf_counter()
        iteration += 1
        max_codes = int(rng.choice(max_codes_choices))
        config = _sample_config(rng)

        try:
            probe_report = strategy_backtest_service.run_strategy_walkforward(
                max_codes=max_codes,
                dry_run=True,
                config=config,
                train_months=24,
                test_months=3,
                step_months=int(args.step_months),
                min_windows=1,
                max_windows=2,
                stop_on_oos_worst_max_drawdown_below=-0.12,
            )
            prune, prune_reason = _should_prune_probe(
                probe_report,
                worst_dd_threshold=-0.12,
            )
            if prune:
                _append_jsonl(
                    progress_path,
                    {
                        "ts": _utc_now_iso(),
                        "event": "run_pruned",
                        "iteration": int(iteration),
                        "elapsed_sec": round(time.perf_counter() - started_at, 3),
                        "max_codes": int(max_codes),
                        "config": asdict(config),
                        "probe": {
                            "execution": probe_report.get("execution") or {},
                            "summary": probe_report.get("summary") or {},
                        },
                        "reason": str(prune_reason or "probe_rejected"),
                    },
                )
                run_count += 1
                if float(args.sleep_seconds) > 0:
                    time.sleep(float(args.sleep_seconds))
                continue

            report = strategy_backtest_service.run_strategy_walkforward(
                max_codes=max_codes,
                dry_run=False,
                config=config,
                train_months=24,
                test_months=3,
                step_months=int(args.step_months),
                min_windows=1,
            )
            gate_result = strategy_backtest_service.run_strategy_walkforward_gate(
                min_oos_total_realized_unit_pnl=0.0,
                min_oos_mean_profit_factor=1.05,
                min_oos_positive_window_ratio=0.40,
                min_oos_worst_max_drawdown_unit=-0.12,
                dry_run=False,
                note="research_worker_standard_gate",
            )
            snapshot = strategy_backtest_service.save_daily_walkforward_research_snapshot()
            evaluation = _evaluate_report(report, gate_result=gate_result, config=config)
            payload = {
                "ts": _utc_now_iso(),
                "event": "run_complete",
                "iteration": int(iteration),
                "elapsed_sec": round(time.perf_counter() - started_at, 3),
                "run_id": report.get("run_id"),
                "max_codes": int(max_codes),
                "windowing": report.get("windowing") or {},
                "config": asdict(config),
                "summary": report.get("summary") or {},
                "evaluation": evaluation,
                "gate_result": {
                    "gate_id": gate_result.get("gate_id"),
                    "status": gate_result.get("status"),
                    "passed": bool(gate_result.get("passed")),
                },
                "snapshot": {
                    "saved": bool(snapshot.get("saved")),
                    "snapshot_date": snapshot.get("snapshot_date"),
                    "source_run_id": snapshot.get("source_run_id"),
                },
            }
            _append_jsonl(progress_path, payload)

            current_score = float(evaluation.get("score") or 0.0)
            if best_score is None or current_score > float(best_score):
                _write_json(best_path, payload)
                best_score = float(current_score)

            if _done_reached(
                payload,
                done_require_max_codes=int(args.done_require_max_codes),
                done_min_trades=int(args.done_min_trades),
                done_min_hedge_trades=int(args.done_min_hedge_trades),
            ):
                _write_json(done_path, payload)
                break
        except Exception as exc:
            _append_jsonl(
                progress_path,
                {
                    "ts": _utc_now_iso(),
                    "event": "run_error",
                    "iteration": int(iteration),
                    "elapsed_sec": round(time.perf_counter() - started_at, 3),
                    "max_codes": int(max_codes),
                    "config": asdict(config),
                    "error": str(exc),
                },
            )

        run_count += 1
        if float(args.sleep_seconds) > 0:
            time.sleep(float(args.sleep_seconds))

    _append_jsonl(
        progress_path,
        {
            "ts": _utc_now_iso(),
            "event": "worker_stop",
            "iteration": int(iteration),
            "run_count": int(run_count),
        },
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
