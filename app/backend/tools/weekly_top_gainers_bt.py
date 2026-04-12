from __future__ import annotations

import argparse
import json
import math
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable

import numpy as np
import pandas as pd

from app.backend.tools.weekly_top_gainers_study import _load_daily_frame, build_weekly_top_gainers_study_frame
from app.db.session import get_conn

DEFAULT_LOOKBACK_DAYS = 365 * 30
DEFAULT_INITIAL_CAPITAL = 10_000_000.0
DEFAULT_TRAIN_END_YMD = 20161230
DEFAULT_COST = 0.001
DEFAULT_REPORT_DIR = Path("G:/Tradex/reports")


@dataclass(frozen=True)
class BacktestConfig:
    lookback_days: int = DEFAULT_LOOKBACK_DAYS
    initial_capital: float = DEFAULT_INITIAL_CAPITAL
    train_end_ymd: int = DEFAULT_TRAIN_END_YMD
    transaction_cost_rate: float = DEFAULT_COST
    report_dir: Path = DEFAULT_REPORT_DIR


@dataclass(frozen=True)
class CodeHistory:
    code: str
    ymds: np.ndarray
    opens: np.ndarray
    highs: np.ndarray
    lows: np.ndarray
    closes: np.ndarray


def _fmt_pct(value: Any) -> str:
    if value is None or pd.isna(value):
        return "-"
    return f"{float(value):.2%}"


def _fmt_num(value: Any, digits: int = 0) -> str:
    if value is None or pd.isna(value):
        return "-"
    return f"{float(value):.{digits}f}"


def _build_histories(daily: pd.DataFrame) -> dict[str, CodeHistory]:
    out: dict[str, CodeHistory] = {}
    for code, group in daily.groupby("code", sort=False):
        g = group.sort_values("date_dt")
        out[str(code)] = CodeHistory(
            code=str(code),
            ymds=g["ymd"].astype(int).to_numpy(),
            opens=g["o"].astype(float).to_numpy(),
            highs=g["h"].astype(float).to_numpy(),
            lows=g["l"].astype(float).to_numpy(),
            closes=g["c"].astype(float).to_numpy(),
        )
    return out


def _next_entry_idx(history: CodeHistory, signal_ymd: int) -> int | None:
    idx = int(np.searchsorted(history.ymds, int(signal_ymd) + 1, side="left"))
    return None if idx >= history.ymds.size else idx


def _simulate_trade(
    history: CodeHistory,
    entry_idx: int,
    *,
    tp: float,
    sl: float,
    max_hold_days: int,
    cost: float,
) -> dict[str, Any] | None:
    if entry_idx < 0 or entry_idx >= history.ymds.size:
        return None
    entry_open = float(history.opens[entry_idx])
    if not math.isfinite(entry_open) or entry_open <= 0:
        return None
    tp_price = entry_open * (1.0 + float(tp))
    sl_price = entry_open * (1.0 - float(sl))
    end_idx = min(entry_idx + max(1, int(max_hold_days)) - 1, history.ymds.size - 1)
    exit_idx = end_idx
    exit_reason = "max_hold"
    exit_price = float(history.closes[end_idx])
    for idx in range(entry_idx, end_idx + 1):
        high = float(history.highs[idx])
        low = float(history.lows[idx])
        if low <= sl_price and high >= tp_price:
            exit_idx = idx
            exit_price = sl_price
            exit_reason = "stop_loss_first_when_both_hit"
            break
        if low <= sl_price:
            exit_idx = idx
            exit_price = sl_price
            exit_reason = "stop_loss"
            break
        if high >= tp_price:
            exit_idx = idx
            exit_price = tp_price
            exit_reason = "take_profit"
            break
    entry_fill = entry_open * (1.0 + float(cost))
    exit_fill = exit_price * (1.0 - float(cost))
    return {
        "entry_ymd": int(history.ymds[entry_idx]),
        "exit_ymd": int(history.ymds[exit_idx]),
        "entry_open": entry_open,
        "entry_fill": entry_fill,
        "exit_fill": exit_fill,
        "gross_ret": float(exit_price / entry_open - 1.0),
        "net_ret": float(exit_fill / entry_fill - 1.0),
        "exit_reason": exit_reason,
        "hold_days": int(exit_idx - entry_idx + 1),
    }


def _build_candidates(study: pd.DataFrame, histories: dict[str, CodeHistory], threshold: int) -> list[dict[str, Any]]:
    rows = study.loc[study["candidate_score"] >= int(threshold)].copy()
    if rows.empty:
        return []
    rows.sort_values(["week_last_ymd", "candidate_score", "code"], ascending=[True, False, True], inplace=True)
    candidates: list[dict[str, Any]] = []
    for _, row in rows.iterrows():
        code = str(row["code"])
        history = histories.get(code)
        if history is None:
            continue
        entry_idx = _next_entry_idx(history, int(row["week_last_ymd"]))
        if entry_idx is None:
            continue
        candidates.append(
            {
                "code": code,
                "signal_week_last_ymd": int(row["week_last_ymd"]),
                "entry_ymd": int(history.ymds[entry_idx]),
                "entry_idx": int(entry_idx),
                "candidate_score": float(row["candidate_score"]),
                "trend_4w": None if pd.isna(row.get("trend_4w")) else float(row.get("trend_4w")),
                "trend_12w": None if pd.isna(row.get("trend_12w")) else float(row.get("trend_12w")),
            }
        )
    return candidates


def _evaluate_period(
    study: pd.DataFrame,
    histories: dict[str, CodeHistory],
    *,
    initial_capital: float,
    threshold: int,
    tp: float,
    sl: float,
    max_hold_days: int,
    max_positions: int,
    cost: float,
    start_ymd: int,
    end_ymd: int,
) -> dict[str, Any]:
    candidates = [row for row in _build_candidates(study, histories, threshold) if start_ymd <= row["entry_ymd"] <= end_ymd]
    if not candidates:
        return {"ok": False, "reason": "no_candidates", "initial_capital": initial_capital, "final_capital": initial_capital}

    plans: list[dict[str, Any]] = []
    for row in candidates:
        trade = _simulate_trade(
            histories[row["code"]],
            int(row["entry_idx"]),
            tp=tp,
            sl=sl,
            max_hold_days=max_hold_days,
            cost=cost,
        )
        if trade is None or trade["exit_ymd"] > end_ymd:
            continue
        trade.update(row)
        plans.append(trade)
    if not plans:
        return {"ok": False, "reason": "no_trade_plans", "initial_capital": initial_capital, "final_capital": initial_capital}

    plans_by_entry: dict[int, list[dict[str, Any]]] = {}
    for plan in plans:
        plans_by_entry.setdefault(int(plan["entry_ymd"]), []).append(plan)

    trading_days = sorted(
        int(day)
        for day in pd.unique(pd.concat([pd.Series(h.ymds) for h in histories.values()], ignore_index=True))
        if start_ymd <= int(day) <= end_ymd
    )
    cash = float(initial_capital)
    open_positions: dict[str, dict[str, Any]] = {}
    equity_curve: list[dict[str, Any]] = []
    realized: list[dict[str, Any]] = []

    def position_value(plan: dict[str, Any], day: int) -> float:
        history = histories[str(plan["code"])]
        idx = int(np.searchsorted(history.ymds, int(day), side="right") - 1)
        if idx < 0:
            return 0.0
        return float(plan["allocation"] * (float(history.closes[idx]) / float(plan["entry_open"])))

    for day in trading_days:
        for code, pos in list(open_positions.items()):
            if int(pos["exit_ymd"]) == int(day):
                cash += float(pos["allocation"]) * (1.0 + float(pos["net_ret"]))
                realized.append(pos)
                open_positions.pop(code, None)

        todays = [plan for plan in plans_by_entry.get(int(day), []) if plan["code"] not in open_positions]
        todays.sort(key=lambda row: (float(row["candidate_score"]), float(row["trend_4w"] or 0.0)), reverse=True)
        slots = max(0, int(max_positions) - len(open_positions))
        selected = todays[:slots]
        if selected:
            allocation = float(cash / len(selected))
            for plan in selected:
                plan = dict(plan)
                plan["allocation"] = allocation
                open_positions[str(plan["code"])] = plan
                cash -= allocation

        open_value = sum(position_value(plan, int(day)) for plan in open_positions.values())
        equity_curve.append({"ymd": int(day), "cash": cash, "open_value": open_value, "equity": cash + open_value})

    eq = pd.Series([float(row["equity"]) for row in equity_curve], dtype=float)
    max_drawdown = float((eq / eq.cummax() - 1.0).min()) if not eq.empty else 0.0
    trade_returns = pd.Series([float(plan["net_ret"]) for plan in realized], dtype=float)
    final_capital = float(eq.iloc[-1]) if not eq.empty else float(cash)

    return {
        "ok": True,
        "initial_capital": float(initial_capital),
        "final_capital": final_capital,
        "total_return": float(final_capital / initial_capital - 1.0),
        "annualized_return": None if len(eq) == 0 else float((final_capital / initial_capital) ** (252.0 / len(eq)) - 1.0),
        "max_drawdown": max_drawdown,
        "trade_count": int(len(realized)),
        "win_rate": None if trade_returns.empty else float((trade_returns > 0).mean()),
        "avg_trade_net_ret": None if trade_returns.empty else float(trade_returns.mean()),
        "median_trade_net_ret": None if trade_returns.empty else float(trade_returns.median()),
        "equity_curve": equity_curve,
        "trades": realized,
    }


def _parameter_grid(
    score_thresholds: Iterable[int],
    take_profit_pcts: Iterable[float],
    stop_loss_pcts: Iterable[float],
    max_hold_days: Iterable[int],
    max_positions: Iterable[int],
) -> list[dict[str, Any]]:
    grid: list[dict[str, Any]] = []
    for threshold in score_thresholds:
        for tp in take_profit_pcts:
            for sl in stop_loss_pcts:
                for hold in max_hold_days:
                    for pos in max_positions:
                        grid.append(
                            {
                                "score_threshold": int(threshold),
                                "take_profit_pct": float(tp),
                                "stop_loss_pct": float(sl),
                                "max_hold_days": int(hold),
                                "max_positions": int(pos),
                            }
                        )
    return grid


def _best_row(rows: list[dict[str, Any]]) -> dict[str, Any] | None:
    if not rows:
        return None
    return max(rows, key=lambda row: (float(row.get("final_capital") or 0.0), float(row.get("total_return") or 0.0), -float(row.get("max_drawdown") or 0.0)))


def run_weekly_top_gainers_portfolio_backtest(
    *,
    config: BacktestConfig = BacktestConfig(),
    score_thresholds: Iterable[int] = (6, 7, 8, 9),
    take_profit_pcts: Iterable[float] = (0.08, 0.10, 0.12, 0.15),
    stop_loss_pcts: Iterable[float] = (0.04, 0.05, 0.07),
    max_hold_days: Iterable[int] = (10, 20, 30),
    max_positions: Iterable[int] = (5, 10),
) -> dict[str, Any]:
    with get_conn() as conn:
        if not conn.execute("SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'daily_bars'").fetchone()[0]:
            return {"ok": False, "reason": "daily_bars_missing"}
        daily = _load_daily_frame(conn, lookback_days=config.lookback_days)
    if daily.empty:
        return {"ok": False, "reason": "daily_frame_empty"}

    study = build_weekly_top_gainers_study_frame(daily, top_n=10)
    if study.empty:
        return {"ok": False, "reason": "weekly_study_empty"}

    histories = _build_histories(daily)
    start_ymd = int(daily["ymd"].min())
    end_ymd = int(daily["ymd"].max())
    train_end_ymd = int(config.train_end_ymd)
    grid = _parameter_grid(score_thresholds, take_profit_pcts, stop_loss_pcts, max_hold_days, max_positions)

    train_rows: list[dict[str, Any]] = []
    test_rows: list[dict[str, Any]] = []
    for params in grid:
        train = _evaluate_period(
            study,
            histories,
            initial_capital=config.initial_capital,
            threshold=params["score_threshold"],
            tp=params["take_profit_pct"],
            sl=params["stop_loss_pct"],
            max_hold_days=params["max_hold_days"],
            max_positions=params["max_positions"],
            cost=config.transaction_cost_rate,
            start_ymd=start_ymd,
            end_ymd=train_end_ymd,
        )
        train.update(params)
        train_rows.append(train)

        test = _evaluate_period(
            study,
            histories,
            initial_capital=config.initial_capital,
            threshold=params["score_threshold"],
            tp=params["take_profit_pct"],
            sl=params["stop_loss_pct"],
            max_hold_days=params["max_hold_days"],
            max_positions=params["max_positions"],
            cost=config.transaction_cost_rate,
            start_ymd=train_end_ymd + 1,
            end_ymd=end_ymd,
        )
        test.update(params)
        test_rows.append(test)

    best_train = _best_row(train_rows)
    best_test = None
    if best_train is not None:
        for row in test_rows:
            if all(row.get(key) == best_train.get(key) for key in ("score_threshold", "take_profit_pct", "stop_loss_pct", "max_hold_days", "max_positions")):
                best_test = row
                break

    return {
        "ok": True,
        "as_of_ymd": end_ymd,
        "period_start_ymd": start_ymd,
        "train_end_ymd": train_end_ymd,
        "period_end_ymd": end_ymd,
        "lookback_days": int(config.lookback_days),
        "initial_capital": float(config.initial_capital),
        "transaction_cost_rate": float(config.transaction_cost_rate),
        "grid_size": int(len(grid)),
        "study_summary": {
            "codes": int(study["code"].nunique()),
            "weeks": int(study["target_week_start_ymd"].nunique()),
            "rows": int(len(study)),
            "top10_rate": float(study["is_top_n"].mean()),
        },
        "train_rows": train_rows,
        "test_rows": test_rows,
        "best_train": best_train,
        "best_test": best_test,
    }


def _jsonable(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(k): _jsonable(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_jsonable(v) for v in value]
    if isinstance(value, tuple):
        return [_jsonable(v) for v in value]
    if isinstance(value, (pd.Timestamp, datetime)):
        return value.isoformat()
    if isinstance(value, np.generic):
        return value.item()
    return value


def _write_reports(result: dict[str, Any], report_dir: Path, prefix: str) -> tuple[Path, Path]:
    report_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d%H%M%S")
    json_path = report_dir / f"{prefix}_{stamp}.json"
    md_path = report_dir / f"{prefix}_{stamp}.md"
    json_path.write_text(json.dumps(_jsonable(result), ensure_ascii=False, indent=2), encoding="utf-8")

    lines = [
        "# Weekly Top Gainers Portfolio Backtest",
        "",
        f"- ok: `{result.get('ok')}`",
        f"- as_of_ymd: `{result.get('as_of_ymd')}`",
        f"- period_start_ymd: `{result.get('period_start_ymd')}`",
        f"- train_end_ymd: `{result.get('train_end_ymd')}`",
        f"- period_end_ymd: `{result.get('period_end_ymd')}`",
        f"- initial_capital: `{_fmt_num(result.get('initial_capital'))}`",
        f"- transaction_cost_rate: `{_fmt_pct(result.get('transaction_cost_rate'))}`",
        "",
        "## Best Train",
        "",
    ]
    best_train = result.get("best_train") or {}
    lines += [
        f"- score_threshold: `{best_train.get('score_threshold')}`",
        f"- take_profit_pct: `{_fmt_pct(best_train.get('take_profit_pct'))}`",
        f"- stop_loss_pct: `{_fmt_pct(best_train.get('stop_loss_pct'))}`",
        f"- max_hold_days: `{best_train.get('max_hold_days')}`",
        f"- max_positions: `{best_train.get('max_positions')}`",
        f"- final_capital: `{_fmt_num(best_train.get('final_capital'))}`",
        f"- total_return: `{_fmt_pct(best_train.get('total_return'))}`",
        f"- annualized_return: `{_fmt_pct(best_train.get('annualized_return'))}`",
        f"- max_drawdown: `{_fmt_pct(best_train.get('max_drawdown'))}`",
        f"- trade_count: `{best_train.get('trade_count')}`",
        f"- win_rate: `{_fmt_pct(best_train.get('win_rate'))}`",
        "",
        "## Best Holdout",
        "",
    ]
    best_test = result.get("best_test") or {}
    lines += [
        f"- score_threshold: `{best_test.get('score_threshold')}`",
        f"- take_profit_pct: `{_fmt_pct(best_test.get('take_profit_pct'))}`",
        f"- stop_loss_pct: `{_fmt_pct(best_test.get('stop_loss_pct'))}`",
        f"- max_hold_days: `{best_test.get('max_hold_days')}`",
        f"- max_positions: `{best_test.get('max_positions')}`",
        f"- final_capital: `{_fmt_num(best_test.get('final_capital'))}`",
        f"- total_return: `{_fmt_pct(best_test.get('total_return'))}`",
        f"- annualized_return: `{_fmt_pct(best_test.get('annualized_return'))}`",
        f"- max_drawdown: `{_fmt_pct(best_test.get('max_drawdown'))}`",
        f"- trade_count: `{best_test.get('trade_count')}`",
        f"- win_rate: `{_fmt_pct(best_test.get('win_rate'))}`",
        "",
        "## Top Train Rows",
        "",
        "| threshold | tp | sl | hold | pos | final_capital | total_return | max_drawdown | trade_count |",
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for row in sorted(result.get("train_rows") or [], key=lambda item: float(item.get("final_capital") or 0.0), reverse=True)[:10]:
        lines.append(
            "| {score_threshold} | {tp} | {sl} | {hold} | {pos} | {final} | {ret} | {dd} | {count} |".format(
                score_threshold=row.get("score_threshold"),
                tp=_fmt_pct(row.get("take_profit_pct")),
                sl=_fmt_pct(row.get("stop_loss_pct")),
                hold=row.get("max_hold_days"),
                pos=row.get("max_positions"),
                final=_fmt_num(row.get("final_capital")),
                ret=_fmt_pct(row.get("total_return")),
                dd=_fmt_pct(row.get("max_drawdown")),
                count=row.get("trade_count"),
            )
        )
    md_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return json_path, md_path


def _parse_ints(value: str) -> list[int]:
    return [int(item.strip()) for item in value.split(",") if item.strip()]


def _parse_floats(value: str) -> list[float]:
    return [float(item.strip()) for item in value.split(",") if item.strip()]


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Weekly top gainers portfolio backtest")
    parser.add_argument("--lookback-days", type=int, default=DEFAULT_LOOKBACK_DAYS)
    parser.add_argument("--initial-capital", type=float, default=DEFAULT_INITIAL_CAPITAL)
    parser.add_argument("--train-end-ymd", type=int, default=DEFAULT_TRAIN_END_YMD)
    parser.add_argument("--transaction-cost-rate", type=float, default=DEFAULT_COST)
    parser.add_argument("--report-dir", type=Path, default=DEFAULT_REPORT_DIR)
    parser.add_argument("--prefix", default="weekly_top_gainers_portfolio_backtest")
    parser.add_argument("--score-thresholds", default="6,7,8,9")
    parser.add_argument("--take-profit-pcts", default="0.08,0.10,0.12,0.15")
    parser.add_argument("--stop-loss-pcts", default="0.04,0.05,0.07")
    parser.add_argument("--max-hold-days", default="10,20,30")
    parser.add_argument("--max-positions", default="5,10")
    args = parser.parse_args(argv)

    result = run_weekly_top_gainers_portfolio_backtest(
        config=BacktestConfig(
            lookback_days=int(args.lookback_days),
            initial_capital=float(args.initial_capital),
            train_end_ymd=int(args.train_end_ymd),
            transaction_cost_rate=float(args.transaction_cost_rate),
            report_dir=Path(args.report_dir),
        ),
        score_thresholds=_parse_ints(str(args.score_thresholds)),
        take_profit_pcts=_parse_floats(str(args.take_profit_pcts)),
        stop_loss_pcts=_parse_floats(str(args.stop_loss_pcts)),
        max_hold_days=_parse_ints(str(args.max_hold_days)),
        max_positions=_parse_ints(str(args.max_positions)),
    )
    json_path, md_path = _write_reports(result, Path(args.report_dir), str(args.prefix))
    print(json.dumps({"ok": result.get("ok"), "json": str(json_path), "markdown": str(md_path)}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
