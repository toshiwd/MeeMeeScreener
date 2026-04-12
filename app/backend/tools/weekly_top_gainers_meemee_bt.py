from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable

import duckdb
import numpy as np
import pandas as pd

from app.backend.tools.weekly_top_gainers_bt import (
    BacktestConfig,
    _best_row,
    _build_candidates,
    _build_histories,
    _evaluate_period,
    _fmt_num,
    _fmt_pct,
    _jsonable,
    _parameter_grid,
    _simulate_trade,
)
from app.backend.tools.weekly_top_gainers_study import _load_daily_frame, build_weekly_top_gainers_study_frame
from external_analysis.contracts.paths import resolve_result_db_path
from app.db.session import get_conn


DEFAULT_REPORT_DIR = Path("G:/Tradex/reports")
DEFAULT_RESULT_DB_PATH = resolve_result_db_path()


@dataclass(frozen=True)
class MeeMeeBlendConfig(BacktestConfig):
    result_db_path: Path = DEFAULT_RESULT_DB_PATH
    surface_min_opportunity_score: float = 0.0
    surface_min_direction_prob: float = 0.55


def _load_latest_surface_frame(
    result_db_path: Path,
    *,
    start_ymd: int,
    end_ymd: int,
) -> pd.DataFrame:
    if not result_db_path.exists():
        return pd.DataFrame()
    conn = duckdb.connect(str(result_db_path), read_only=True)
    try:
        if not conn.execute("SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'forecast_surface_daily'").fetchone()[0]:
            return pd.DataFrame()
        frame = conn.execute(
            """
            SELECT
                CAST(as_of_date AS VARCHAR) AS as_of_date,
                code,
                side,
                action_state,
                direction_prob,
                expected_ret_20,
                expected_mfe_20,
                expected_mae_20,
                invalidation_price,
                opportunity_score,
                freshness_state
            FROM forecast_surface_daily
            WHERE CAST(REPLACE(CAST(as_of_date AS VARCHAR), '-', '') AS INTEGER) BETWEEN ? AND ?
              AND side = 'long'
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY as_of_date, code, side
                ORDER BY created_at DESC, publish_id DESC
            ) = 1
            """,
            [int(start_ymd), int(end_ymd)],
        ).df()
    finally:
        conn.close()
    if frame.empty:
        return frame
    frame["as_of_ymd"] = frame["as_of_date"].astype(str).str.replace("-", "", regex=False).astype(int)
    frame["code"] = frame["code"].astype(str).str.strip()
    frame["expected_upside"] = pd.to_numeric(frame["expected_mfe_20"], errors="coerce")
    frame["expected_downside"] = pd.to_numeric(frame["expected_mae_20"], errors="coerce")
    return frame


def _build_blended_candidates(
    study: pd.DataFrame,
    histories: dict[str, Any],
    surface: pd.DataFrame,
    *,
    threshold: int,
    surface_min_opportunity_score: float,
    surface_min_direction_prob: float,
) -> list[dict[str, Any]]:
    base_candidates = _build_candidates(study, histories, threshold)
    if not base_candidates:
        return []
    if surface.empty:
        return []
    candidates_df = pd.DataFrame(base_candidates)
    if candidates_df.empty:
        return []
    merged = candidates_df.merge(
        surface,
        left_on=["signal_week_last_ymd", "code"],
        right_on=["as_of_ymd", "code"],
        how="left",
        suffixes=("", "_surface"),
    )
    if merged.empty:
        return []
    merged["surface_action_state"] = merged["action_state"].astype(str).str.strip().str.lower()
    merged["surface_direction_prob"] = pd.to_numeric(merged["direction_prob"], errors="coerce")
    merged["surface_opportunity_score"] = pd.to_numeric(merged["opportunity_score"], errors="coerce")
    merged["surface_expected_upside"] = pd.to_numeric(merged["expected_upside"], errors="coerce")
    merged["surface_expected_downside"] = pd.to_numeric(merged["expected_downside"], errors="coerce")
    merged["surface_invalidation_price"] = pd.to_numeric(merged["invalidation_price"], errors="coerce")

    has_surface = merged["as_of_ymd"].notna()
    action_state = merged["surface_action_state"]
    surface_ok = has_surface & action_state.isin({"enter", "wait"})
    surface_ok &= merged["surface_direction_prob"].fillna(0.0) >= float(surface_min_direction_prob)
    surface_ok &= merged["surface_opportunity_score"].fillna(-1e9) >= float(surface_min_opportunity_score)

    fallback = ~has_surface
    merged = merged.loc[surface_ok | fallback].copy()
    if merged.empty:
        return []
    merged["blended_score"] = pd.to_numeric(merged["candidate_score"], errors="coerce").fillna(-1e9)
    boosted = merged["surface_opportunity_score"].notna()
    merged.loc[boosted, "blended_score"] = (
        merged.loc[boosted, "blended_score"]
        + merged.loc[boosted, "surface_opportunity_score"].fillna(0.0) * 5.0
        + merged.loc[boosted, "surface_direction_prob"].fillna(0.0) * 2.0
    )
    merged.loc[fallback, "surface_action_state"] = "fallback"
    merged.loc[fallback, "surface_direction_prob"] = np.nan
    merged.loc[fallback, "surface_opportunity_score"] = np.nan
    merged.loc[fallback, "surface_expected_upside"] = np.nan
    merged.loc[fallback, "surface_expected_downside"] = np.nan
    merged.loc[fallback, "surface_invalidation_price"] = np.nan
    return merged.to_dict(orient="records")


def _evaluate_blended_period(
    study: pd.DataFrame,
    histories: dict[str, Any],
    surface: pd.DataFrame,
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
    surface_min_opportunity_score: float,
    surface_min_direction_prob: float,
) -> dict[str, Any]:
    candidates = [
        row
        for row in _build_blended_candidates(
            study,
            histories,
            surface,
            threshold=threshold,
            surface_min_opportunity_score=surface_min_opportunity_score,
            surface_min_direction_prob=surface_min_direction_prob,
        )
        if start_ymd <= row["entry_ymd"] <= end_ymd
    ]
    if not candidates:
        return {
            "ok": False,
            "reason": "no_candidates",
            "initial_capital": initial_capital,
            "final_capital": initial_capital,
        }
    return _evaluate_candidate_rows(
        candidates,
        histories,
        initial_capital=initial_capital,
        tp=tp,
        sl=sl,
        max_hold_days=max_hold_days,
        max_positions=max_positions,
        cost=cost,
        start_ymd=start_ymd,
        end_ymd=end_ymd,
    )


def _evaluate_candidate_rows(
    candidates: list[dict[str, Any]],
    histories: dict[str, Any],
    *,
    initial_capital: float,
    tp: float,
    sl: float,
    max_hold_days: int,
    max_positions: int,
    cost: float,
    start_ymd: int,
    end_ymd: int,
) -> dict[str, Any]:
    candidates = [row for row in candidates if start_ymd <= int(row["entry_ymd"]) <= end_ymd]
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


def run_weekly_top_gainers_meemee_backtest(
    *,
    config: MeeMeeBlendConfig = MeeMeeBlendConfig(),
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
    surface = _load_latest_surface_frame(Path(config.result_db_path), start_ymd=start_ymd, end_ymd=end_ymd)

    grid = _parameter_grid(score_thresholds, take_profit_pcts, stop_loss_pcts, max_hold_days, max_positions)
    train_rows: list[dict[str, Any]] = []
    test_rows: list[dict[str, Any]] = []
    for params in grid:
        train = _evaluate_blended_period(
            study,
            histories,
            surface,
            initial_capital=config.initial_capital,
            threshold=params["score_threshold"],
            tp=params["take_profit_pct"],
            sl=params["stop_loss_pct"],
            max_hold_days=params["max_hold_days"],
            max_positions=params["max_positions"],
            cost=config.transaction_cost_rate,
            start_ymd=start_ymd,
            end_ymd=train_end_ymd,
            surface_min_opportunity_score=config.surface_min_opportunity_score,
            surface_min_direction_prob=config.surface_min_direction_prob,
        )
        train.update(params)
        train_rows.append(train)

        test = _evaluate_blended_period(
            study,
            histories,
            surface,
            initial_capital=config.initial_capital,
            threshold=params["score_threshold"],
            tp=params["take_profit_pct"],
            sl=params["stop_loss_pct"],
            max_hold_days=params["max_hold_days"],
            max_positions=params["max_positions"],
            cost=config.transaction_cost_rate,
            start_ymd=train_end_ymd + 1,
            end_ymd=end_ymd,
            surface_min_opportunity_score=config.surface_min_opportunity_score,
            surface_min_direction_prob=config.surface_min_direction_prob,
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
        "initial_capital": float(config.initial_capital),
        "transaction_cost_rate": float(config.transaction_cost_rate),
        "surface_rows": int(len(surface)),
        "surface_date_count": int(surface["as_of_ymd"].nunique()) if not surface.empty else 0,
        "surface_min_opportunity_score": float(config.surface_min_opportunity_score),
        "surface_min_direction_prob": float(config.surface_min_direction_prob),
        "train_rows": train_rows,
        "test_rows": test_rows,
        "best_train": best_train,
        "best_test": best_test,
        "study_summary": {
            "codes": int(study["code"].nunique()),
            "weeks": int(study["target_week_start_ymd"].nunique()),
            "rows": int(len(study)),
            "top10_rate": float(study["is_top_n"].mean()),
        },
    }


def _write_reports(result: dict[str, Any], report_dir: Path, prefix: str) -> tuple[Path, Path]:
    report_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d%H%M%S")
    json_path = report_dir / f"{prefix}_{stamp}.json"
    md_path = report_dir / f"{prefix}_{stamp}.md"
    json_path.write_text(json.dumps(_jsonable(result), ensure_ascii=False, indent=2), encoding="utf-8")
    lines = [
        "# Weekly Top Gainers + MeeMee Surface Backtest",
        "",
        f"- ok: `{result.get('ok')}`",
        f"- as_of_ymd: `{result.get('as_of_ymd')}`",
        f"- period_start_ymd: `{result.get('period_start_ymd')}`",
        f"- train_end_ymd: `{result.get('train_end_ymd')}`",
        f"- period_end_ymd: `{result.get('period_end_ymd')}`",
        f"- surface_rows: `{result.get('surface_rows')}`",
        f"- surface_date_count: `{result.get('surface_date_count')}`",
        f"- initial_capital: `{_fmt_num(result.get('initial_capital'))}`",
        f"- surface_min_opportunity_score: `{_fmt_num(result.get('surface_min_opportunity_score'), digits=3)}`",
        f"- surface_min_direction_prob: `{_fmt_pct(result.get('surface_min_direction_prob'))}`",
        "",
    ]
    best_train = result.get("best_train") or {}
    best_test = result.get("best_test") or {}
    lines += [
        "## Best Train",
        "",
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
    ]
    md_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return json_path, md_path


def _parse_ints(value: str) -> list[int]:
    return [int(item.strip()) for item in value.split(",") if item.strip()]


def _parse_floats(value: str) -> list[float]:
    return [float(item.strip()) for item in value.split(",") if item.strip()]


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Weekly top gainers + MeeMee surface portfolio backtest")
    parser.add_argument("--lookback-days", type=int, default=365 * 30)
    parser.add_argument("--initial-capital", type=float, default=10_000_000.0)
    parser.add_argument("--train-end-ymd", type=int, default=20161230)
    parser.add_argument("--transaction-cost-rate", type=float, default=0.001)
    parser.add_argument("--result-db-path", type=Path, default=DEFAULT_RESULT_DB_PATH)
    parser.add_argument("--surface-min-opportunity-score", type=float, default=0.0)
    parser.add_argument("--surface-min-direction-prob", type=float, default=0.55)
    parser.add_argument("--report-dir", type=Path, default=DEFAULT_REPORT_DIR)
    parser.add_argument("--prefix", default="weekly_top_gainers_meemee_backtest")
    parser.add_argument("--score-thresholds", default="6,7,8,9")
    parser.add_argument("--take-profit-pcts", default="0.08,0.10,0.12,0.15")
    parser.add_argument("--stop-loss-pcts", default="0.04,0.05,0.07")
    parser.add_argument("--max-hold-days", default="10,20,30")
    parser.add_argument("--max-positions", default="5,10")
    args = parser.parse_args(argv)

    result = run_weekly_top_gainers_meemee_backtest(
        config=MeeMeeBlendConfig(
            lookback_days=int(args.lookback_days),
            initial_capital=float(args.initial_capital),
            train_end_ymd=int(args.train_end_ymd),
            transaction_cost_rate=float(args.transaction_cost_rate),
            report_dir=Path(args.report_dir),
            result_db_path=Path(args.result_db_path),
            surface_min_opportunity_score=float(args.surface_min_opportunity_score),
            surface_min_direction_prob=float(args.surface_min_direction_prob),
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
