from __future__ import annotations

from datetime import datetime

from app.backend.core.jobs import job_manager
from app.backend.services import strategy_backtest_service


def _to_int(value: object) -> int | None:
    try:
        if value is None:
            return None
        return int(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None


def _to_float(value: object) -> float | None:
    try:
        if value is None:
            return None
        return float(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None


def _to_bool(value: object) -> bool | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    if text in {"1", "true", "yes", "y", "on"}:
        return True
    if text in {"0", "false", "no", "n", "off"}:
        return False
    return None


def _to_setup_tuple(value: object) -> tuple[str, ...] | None:
    if value is None:
        return None
    items: list[str] = []
    if isinstance(value, (list, tuple, set)):
        for raw in value:
            text = str(raw).strip()
            if text:
                items.append(text)
    else:
        text = str(value).strip()
        if not text:
            return None
        items = [s.strip() for s in text.split(",") if s.strip()]
    return tuple(items) if items else None


def _build_config(payload: dict) -> strategy_backtest_service.StrategyBacktestConfig:
    raw = payload.get("config")
    conf = raw if isinstance(raw, dict) else {}
    kwargs: dict[str, object] = {}

    int_fields = (
        "max_positions",
        "initial_units",
        "add1_units",
        "add2_units",
        "hedge_units",
        "min_history_bars",
        "event_lookback_days",
        "event_lookahead_days",
        "max_new_entries_per_day",
        "max_new_entries_per_month",
        "regime_breadth_lookback_days",
    )
    float_fields = (
        "min_hedge_ratio",
        "cost_bps",
        "prefer_net_short_ratio",
        "min_long_score",
        "min_short_score",
        "max_dist_ma20_long",
        "min_volume_ratio_long",
        "max_atr_pct_long",
        "min_ml_p_up_long",
        "regime_long_min_breadth_above60",
        "regime_short_max_breadth_above60",
    )
    bool_fields = ("require_decision_for_long", "require_ma_bull_stack_long", "use_regime_filter")

    for name in int_fields:
        parsed = _to_int(conf.get(name))
        if parsed is not None:
            kwargs[name] = parsed
    for name in float_fields:
        parsed = _to_float(conf.get(name))
        if parsed is not None:
            kwargs[name] = parsed
    for name in bool_fields:
        parsed = _to_bool(conf.get(name))
        if parsed is not None:
            kwargs[name] = parsed

    allowed_sides = conf.get("allowed_sides")
    if allowed_sides is not None:
        side = str(allowed_sides).strip().lower()
        if side in {"both", "long", "short"}:
            kwargs["allowed_sides"] = side
    for name in ("allowed_long_setups", "allowed_short_setups"):
        parsed = _to_setup_tuple(conf.get(name))
        if parsed:
            kwargs[name] = parsed

    return strategy_backtest_service.StrategyBacktestConfig(**kwargs)


def handle_strategy_backtest(job_id: str, payload: dict) -> None:
    start_dt = _to_int(payload.get("start_dt"))
    end_dt = _to_int(payload.get("end_dt"))
    max_codes = _to_int(payload.get("max_codes"))
    dry_run = bool(payload.get("dry_run", False))
    config = _build_config(payload)

    job_manager._update_db(
        job_id,
        "strategy_backtest",
        "running",
        progress=10,
        message="Running strategy backtest...",
    )
    result = strategy_backtest_service.run_strategy_backtest(
        start_dt=start_dt,
        end_dt=end_dt,
        max_codes=max_codes,
        dry_run=dry_run,
        config=config,
    )
    metrics = result.get("metrics") or {}
    win_rate = metrics.get("win_rate")
    trade_events = metrics.get("trade_events")
    message = (
        f"Strategy backtest completed (dry_run={dry_run}, win_rate={win_rate}, trades={trade_events})"
    )
    job_manager._update_db(
        job_id,
        "strategy_backtest",
        "success",
        progress=100,
        message=message,
        finished_at=datetime.now(),
    )


def handle_strategy_walkforward(job_id: str, payload: dict) -> None:
    start_dt = _to_int(payload.get("start_dt"))
    end_dt = _to_int(payload.get("end_dt"))
    max_codes = _to_int(payload.get("max_codes"))
    dry_run = bool(payload.get("dry_run", False))
    train_months = _to_int(payload.get("train_months")) or 24
    test_months = _to_int(payload.get("test_months")) or 3
    step_months = _to_int(payload.get("step_months")) or 1
    min_windows = _to_int(payload.get("min_windows")) or 1
    config = _build_config(payload)

    job_manager._update_db(
        job_id,
        "strategy_walkforward",
        "running",
        progress=10,
        message="Running walkforward validation...",
    )
    result = strategy_backtest_service.run_strategy_walkforward(
        start_dt=start_dt,
        end_dt=end_dt,
        max_codes=max_codes,
        dry_run=dry_run,
        config=config,
        train_months=train_months,
        test_months=test_months,
        step_months=step_months,
        min_windows=min_windows,
    )
    summary = result.get("summary") or {}
    windows = summary.get("executed_windows")
    win_rate = summary.get("oos_weighted_win_rate")
    trades = summary.get("oos_trade_events")
    message = (
        "Walkforward completed "
        f"(dry_run={dry_run}, windows={windows}, oos_win_rate={win_rate}, oos_trades={trades})"
    )
    job_manager._update_db(
        job_id,
        "strategy_walkforward",
        "success",
        progress=100,
        message=message,
        finished_at=datetime.now(),
    )
