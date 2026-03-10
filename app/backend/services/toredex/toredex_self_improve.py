from __future__ import annotations

import calendar
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import date
import json
import os
from pathlib import Path
import random
import subprocess
import sys
import tempfile
import time
import uuid
from typing import Any

from .toredex_config import load_toredex_config
from .toredex_repository import ToredexRepository
from .toredex_runner import run_backtest


@dataclass(frozen=True)
class StageSpec:
    name: str
    order: int
    months: int


_DEFAULT_SCORE_WEIGHTS: dict[str, float] = {
    "maxDrawdown": 0.45,
    "worstMonth": 0.30,
    "turnover": 0.01,
    "netExposure": 0.20,
    "costDrag": 0.40,
    "top1Concentration": 0.10,
    "gateNgExitShare": 0.10,
    "cutLossExitShare": 0.15,
    "tradeShortfall": 1.25,
    "riskGateFailPenalty": 1000.0,
}

_DEFAULT_MIN_TRADES: dict[str, int] = {
    "stage0": 1,
    "stage1": 3,
    "stage2": 8,
}

_DEFAULT_TOP1_ENTRY_SHARE_MAX_PCT: dict[str, float] = {
    "stage1": 95.0,
    "stage2": 90.0,
}


def _as_int(value: Any, default: int) -> int:
    try:
        return int(value)
    except Exception:
        return int(default)


def _as_float(value: Any, default: float) -> float:
    try:
        return float(value)
    except Exception:
        return float(default)


def _as_optional_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except Exception:
        return None


def _as_bool(value: Any, default: bool) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        text = value.strip().lower()
        if text in {"1", "true", "yes", "y", "on"}:
            return True
        if text in {"0", "false", "no", "n", "off"}:
            return False
    return bool(default)


def _normalize_score_weights(opt_raw: dict[str, Any]) -> dict[str, float]:
    out = dict(_DEFAULT_SCORE_WEIGHTS)
    raw = opt_raw.get("scoreWeights")
    if not isinstance(raw, dict):
        return out
    for key in out.keys():
        if key not in raw:
            continue
        out[key] = _as_float(raw.get(key), out[key])
    return out


def _resolve_min_trades(opt_raw: dict[str, Any]) -> dict[str, int]:
    out = dict(_DEFAULT_MIN_TRADES)
    key_map = {
        "stage0": "minTradesStage0",
        "stage1": "minTradesStage1",
        "stage2": "minTradesStage2",
    }
    for stage_name, cfg_key in key_map.items():
        if cfg_key in opt_raw:
            out[stage_name] = max(0, _as_int(opt_raw.get(cfg_key), out[stage_name]))
    return out


def _resolve_top1_entry_share_caps_pct(opt_raw: dict[str, Any]) -> dict[str, float]:
    out = dict(_DEFAULT_TOP1_ENTRY_SHARE_MAX_PCT)
    key_map = {
        "stage1": "top1EntryShareMaxStage1Pct",
        "stage2": "top1EntryShareMaxStage2Pct",
    }
    for stage_name, cfg_key in key_map.items():
        if cfg_key not in opt_raw:
            continue
        raw_value = _as_float(opt_raw.get(cfg_key), out[stage_name])
        out[stage_name] = min(100.0, max(0.0, raw_value))
    return out


def _compute_top1_entry_share(reason_counts: dict[str, int] | None) -> float:
    reason_map = reason_counts if isinstance(reason_counts, dict) else {}
    entry_top1 = max(0, _as_int(reason_map.get("E_NEW_TOP1_GATE_OK"), 0))
    entry_topk = max(0, _as_int(reason_map.get("E_NEW_TOPK_GATE_OK"), 0))
    entry_switch = max(0, _as_int(reason_map.get("E_NEW_SWITCH_IN"), 0))
    entry_total = float(max(0, entry_top1 + entry_topk + entry_switch))
    return (float(entry_top1) / entry_total) if entry_total > 0 else 1.0


def _compute_gate_ng_exit_share(reason_counts: dict[str, int] | None) -> float:
    reason_map = reason_counts if isinstance(reason_counts, dict) else {}
    gate_ng = max(0, _as_int(reason_map.get("X_EXIT_GATE_NG"), 0))
    total_close = 0
    for reason_id, raw_count in reason_map.items():
        key = str(reason_id or "").strip()
        if not key:
            continue
        if key.startswith(("X_", "R_", "T_", "S_")):
            total_close += max(0, _as_int(raw_count, 0))
    if total_close <= 0:
        return 0.0
    return float(gate_ng) / float(total_close)


def _compute_cut_loss_exit_share(reason_counts: dict[str, int] | None) -> float:
    reason_map = reason_counts if isinstance(reason_counts, dict) else {}
    cut_warn = max(0, _as_int(reason_map.get("R_CUT_LOSS_WARN"), 0))
    cut_hard = max(0, _as_int(reason_map.get("R_CUT_LOSS_HARD"), 0))
    cut_total = cut_warn + cut_hard
    total_close = 0
    for reason_id, raw_count in reason_map.items():
        key = str(reason_id or "").strip()
        if not key:
            continue
        if key.startswith(("X_", "R_", "T_", "S_")):
            total_close += max(0, _as_int(raw_count, 0))
    if total_close <= 0:
        return 0.0
    return float(cut_total) / float(total_close)


def _score_sort_value(item: dict[str, Any]) -> float:
    score_objective = item.get("score_objective")
    if score_objective is not None:
        return _as_float(score_objective, -1_000_000_000.0)
    return _as_float(item.get("score_net_return_pct"), -1_000_000_000.0)


def _count_trade_events(result: dict[str, Any]) -> int:
    days = result.get("days")
    if not isinstance(days, list):
        return 0
    total = 0
    for day in days:
        if not isinstance(day, dict):
            continue
        total += _as_int(day.get("trade_count"), 0)
    return max(0, total)


def _build_stage_score(
    *,
    stage_name: str,
    passed: bool,
    final_metrics: dict[str, Any],
    score_net_return_pct: float,
    worst_month: float | None,
    max_turnover: float | None,
    max_abs_net_units: float | None,
    trade_count: int,
    reason_counts: dict[str, int] | None,
    opt_raw: dict[str, Any],
) -> tuple[float, dict[str, float]]:
    weights = _normalize_score_weights(opt_raw)
    min_trades_map = _resolve_min_trades(opt_raw)
    min_required_trades = max(0, int(min_trades_map.get(stage_name, 0)))

    max_drawdown_pct = _as_float(final_metrics.get("max_drawdown_pct"), 0.0)
    gross_return_pct = _as_float(final_metrics.get("gross_cum_return_pct"), score_net_return_pct)
    cost_drag_pct = max(0.0, gross_return_pct - score_net_return_pct)

    dd_abs = max(0.0, -max_drawdown_pct)
    worst_month_abs = max(0.0, -_as_float(worst_month, 0.0))
    turnover_pct = max(0.0, _as_float(max_turnover, 0.0))
    net_exposure_units = max(0.0, abs(_as_float(max_abs_net_units, 0.0)))
    trade_shortfall = max(0.0, float(min_required_trades - max(0, trade_count)))
    top1_entry_share = _compute_top1_entry_share(reason_counts)
    top1_concentration_excess = max(0.0, top1_entry_share - 0.60)
    gate_ng_exit_share = _compute_gate_ng_exit_share(reason_counts)
    gate_ng_exit_share_excess = max(0.0, gate_ng_exit_share - 0.25)
    cut_loss_exit_share = _compute_cut_loss_exit_share(reason_counts)
    cut_loss_exit_share_excess = max(0.0, cut_loss_exit_share - 0.30)
    top1_share_caps_pct = _resolve_top1_entry_share_caps_pct(opt_raw)
    stage_top1_cap_pct = top1_share_caps_pct.get(stage_name)
    top1_gate_failed = bool(
        (stage_top1_cap_pct is not None) and ((top1_entry_share * 100.0) > (float(stage_top1_cap_pct) + 1e-9))
    )

    score = float(score_net_return_pct)
    score -= weights["maxDrawdown"] * dd_abs
    score -= weights["worstMonth"] * worst_month_abs
    score -= weights["turnover"] * turnover_pct
    score -= weights["netExposure"] * net_exposure_units
    score -= weights["costDrag"] * cost_drag_pct
    score -= weights["top1Concentration"] * (top1_concentration_excess * 100.0)
    score -= weights["gateNgExitShare"] * (gate_ng_exit_share_excess * 100.0)
    score -= weights["cutLossExitShare"] * (cut_loss_exit_share_excess * 100.0)
    score -= weights["tradeShortfall"] * trade_shortfall
    if not passed:
        score -= weights["riskGateFailPenalty"]

    components = {
        "scoreNetReturnPct": float(score_net_return_pct),
        "maxDrawdownAbsPct": dd_abs,
        "worstMonthAbsPct": worst_month_abs,
        "turnoverPctPerMonthMax": turnover_pct,
        "netExposureUnitsMaxAbs": net_exposure_units,
        "costDragPct": cost_drag_pct,
        "tradeCount": float(max(0, trade_count)),
        "minRequiredTrades": float(min_required_trades),
        "tradeShortfall": trade_shortfall,
        "top1EntrySharePct": float(round(top1_entry_share * 100.0, 6)),
        "top1EntryShareCapPct": float(stage_top1_cap_pct) if stage_top1_cap_pct is not None else 100.0,
        "top1EntryShareGateFailed": 1.0 if top1_gate_failed else 0.0,
        "top1ConcentrationExcessPct": float(round(top1_concentration_excess * 100.0, 6)),
        "gateNgExitSharePct": float(round(gate_ng_exit_share * 100.0, 6)),
        "gateNgExitShareExcessPct": float(round(gate_ng_exit_share_excess * 100.0, 6)),
        "cutLossExitSharePct": float(round(cut_loss_exit_share * 100.0, 6)),
        "cutLossExitShareExcessPct": float(round(cut_loss_exit_share_excess * 100.0, 6)),
        "passed": 1.0 if passed else 0.0,
        "scoreObjective": float(score),
    }
    return float(score), components


def _add_months(value: date, months: int) -> date:
    y = value.year + ((value.month - 1 + months) // 12)
    m = (value.month - 1 + months) % 12 + 1
    d = min(value.day, calendar.monthrange(y, m)[1])
    return date(y, m, d)


def _range_months_ending_at(end_date: date, months: int) -> tuple[date, date]:
    start = _add_months(end_date, -(max(1, int(months)) - 1))
    return start, end_date


def _safe_git_commit() -> str:
    try:
        out = subprocess.check_output(["git", "rev-parse", "HEAD"], text=True)
        return str(out).strip()
    except Exception:
        return "unknown"


def _repo_call_with_retry(func, *, retries: int = 4) -> Any:
    last_exc: Exception | None = None
    for attempt in range(max(1, int(retries))):
        try:
            return func()
        except Exception as exc:
            last_exc = exc
            msg = str(exc).lower()
            retryable = ("already open" in msg) or ("used by" in msg) or ("cannot open file" in msg)
            if (not retryable) or attempt >= retries - 1:
                raise
            time.sleep(0.15 * float(attempt + 1))
    if last_exc is not None:
        raise last_exc
    raise RuntimeError("repository call failed")


def _is_duckdb_lock_error(exc: Exception) -> bool:
    msg = str(exc).lower()
    return (
        ("already open" in msg)
        or ("used by" in msg)
        or ("cannot open file" in msg)
        or ("file is already open" in msg)
    )


def _normalize_parallel_db_paths(raw: Any) -> list[str]:
    if not isinstance(raw, list):
        return []
    out: list[str] = []
    seen: set[str] = set()
    for item in raw:
        text = str(item or "").strip()
        if not text:
            continue
        resolved = str(Path(text).resolve())
        if resolved in seen:
            continue
        if not Path(resolved).exists():
            continue
        seen.add(resolved)
        out.append(resolved)
    return out


def _run_backtest_via_subprocess(
    *,
    season_id: str,
    start_date: str,
    end_date: str,
    config_override: dict[str, Any],
    db_path: str,
) -> dict[str, Any]:
    command = [
        sys.executable,
        "-m",
        "toredex",
        "run-backtest",
        "--season-id",
        season_id,
        "--start-date",
        start_date,
        "--end-date",
        end_date,
    ]

    temp_override_path: str | None = None
    if isinstance(config_override, dict) and config_override:
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            suffix=".json",
            delete=False,
        ) as handle:
            json.dump(config_override, handle, ensure_ascii=False, sort_keys=True)
            temp_override_path = str(Path(handle.name).resolve())
        command.extend(["--config-override-json", temp_override_path])

    env = dict(os.environ)
    env["STOCKS_DB_PATH"] = str(db_path)
    repo_root = Path(__file__).resolve().parents[3]
    try:
        completed = subprocess.run(
            command,
            cwd=str(repo_root),
            env=env,
            capture_output=True,
            text=True,
            check=False,
        )
    finally:
        if temp_override_path:
            try:
                Path(temp_override_path).unlink(missing_ok=True)
            except Exception:
                pass

    if completed.returncode != 0:
        stderr_text = str(completed.stderr or "").strip()
        stdout_text = str(completed.stdout or "").strip()
        detail = stderr_text or stdout_text or "unknown subprocess error"
        raise RuntimeError(f"run-backtest subprocess failed ({completed.returncode}): {detail}")

    try:
        parsed = json.loads(str(completed.stdout or "{}"))
    except Exception as exc:
        raise RuntimeError(f"run-backtest subprocess JSON parse failed: {exc}") from exc

    if not isinstance(parsed, dict):
        raise RuntimeError("run-backtest subprocess returned non-object payload")
    if parsed.get("ok") is False:
        raise RuntimeError(f"run-backtest subprocess returned failure payload: {parsed}")
    return parsed


def _sample_candidate_override(
    *,
    rng: random.Random,
    mode: str,
    base: dict[str, Any],
    include_short_bias: bool,
    optimize_cost_model: bool,
) -> dict[str, Any]:
    short_enabled = rng.random() < (0.8 if include_short_bias else 0.4)
    if mode == "champion":
        short_enabled = rng.random() < 0.5

    optimization_block = base.get("optimization") if isinstance(base.get("optimization"), dict) else {}
    top1_cap_stage1 = _as_float(optimization_block.get("top1EntryShareMaxStage1Pct"), 100.0)
    top1_cap_stage2 = _as_float(optimization_block.get("top1EntryShareMaxStage2Pct"), 100.0)
    top1_cap_enabled = (top1_cap_stage1 < 100.0) or (top1_cap_stage2 < 100.0)
    if top1_cap_enabled and mode != "champion":
        short_enabled = rng.random() < (0.25 if include_short_bias else 0.15)

    if top1_cap_enabled:
        max_holdings = 10
        max_new_entries_per_day = 2.0
        new_entry_max_rank = rng.choice([5.0, 10.0])
    else:
        max_holdings = rng.choice([3, 5, 7, 10])
        max_new_entries_per_day = rng.choice([1.0, 2.0])
        new_entry_max_rank = rng.choice([1.0, 2.0, 3.0, 5.0])

    risk_gates = base.get("riskGates") if isinstance(base.get("riskGates"), dict) else {}
    risk_mode = risk_gates.get(mode) if isinstance(risk_gates.get(mode), dict) else {}
    risk_gate_max_net_units = max(1, int(_as_float(risk_mode.get("maxNetExposureUnits"), 4.0)))
    if top1_cap_enabled:
        max_net_units = risk_gate_max_net_units
    else:
        default_choices = [1, 2, 3, 4]
        allowed_choices = [u for u in default_choices if u <= risk_gate_max_net_units]
        if not allowed_choices:
            allowed_choices = [risk_gate_max_net_units]
        max_net_units = rng.choice(allowed_choices)
    if top1_cap_enabled:
        max_units_per_ticker = 2
        max_per_sector = 3
        min_liquidity20d = rng.choice([0.0, 50_000_000.0])
    else:
        max_units_per_ticker = rng.choice([1, 2, 3])
        max_per_sector = rng.choice([2, 3])
        min_liquidity20d = rng.choice([0.0, 50_000_000.0, 100_000_000.0])

    if top1_cap_enabled:
        entry_min_up = rng.choice([0.58, 0.60, 0.62])
        entry_min_ev = rng.choice([0.02, 0.03])
        switch_min_ev_gap = rng.choice([0.0, 0.01, 0.02])
        exit_if_unranked = rng.choice([0.0, 0.0, 0.0, 1.0])
        exit_min_up = rng.choice([0.45, 0.48, 0.50])
        exit_min_ev = rng.choice([-0.02, -0.01, 0.0])
        rev_risk_warn = rng.choice([0.50, 0.55])
        rev_risk_high = min(0.90, rev_risk_warn + rng.choice([0.08, 0.10, 0.12]))
        cut_loss_warn = rng.choice([-5.5, -7.0, -8.5])
        cut_loss_hard = cut_loss_warn - rng.choice([2.5, 3.5, 4.5])
        take_profit_hint = rng.choice([8.0, 10.0, 12.0])
    else:
        entry_min_up = rng.choice([0.55, 0.58, 0.60, 0.62])
        entry_min_ev = rng.choice([0.0, 0.01, 0.02, 0.03])
        switch_min_ev_gap = rng.choice([0.01, 0.03])
        exit_if_unranked = rng.choice([0.0, 0.0, 1.0])
        exit_min_up = rng.choice([0.40, 0.45, 0.50, 0.55])
        exit_min_ev = rng.choice([-0.03, -0.02, -0.01, 0.0])
        rev_risk_warn = rng.choice([0.50, 0.55, 0.60])
        rev_risk_high = min(0.90, rev_risk_warn + rng.choice([0.06, 0.08, 0.10, 0.12]))
        cut_loss_warn = rng.choice([-5.5, -7.0, -8.5, -10.0])
        cut_loss_hard = cut_loss_warn - rng.choice([2.0, 3.0, 4.0])
        take_profit_hint = rng.choice([8.0, 10.0, 12.0, 15.0])
    if top1_cap_enabled:
        add_min_ev = rng.choice([0.01, 0.02, 0.03])
        exit_gate_ng_min_holding_days = rng.choice([5.0, 8.0, 13.0])
        exit_gate_ng_min_pnl_pct = rng.choice([-1.0, 0.0, 0.5])
    else:
        add_min_ev = rng.choice([0.0, 0.01, 0.02, 0.03])
        exit_gate_ng_min_holding_days = rng.choice([3.0, 5.0, 8.0, 13.0, 21.0])
        exit_gate_ng_min_pnl_pct = rng.choice([-2.0, -1.0, 0.0, 1.0])

    base_cost = base.get("costModel") if isinstance(base.get("costModel"), dict) else {}
    fees_bps = _as_float(base_cost.get("feesBps"), 0.0)
    slippage_bps = _as_float(base_cost.get("slippageBps"), 0.0)
    slippage_liq_factor = _as_float(base_cost.get("slippageLiquidityFactorBps"), 0.0)
    borrow_bps = _as_float(base_cost.get("borrowShortBpsAnnual"), 0.0)
    if optimize_cost_model:
        fees_bps = rng.choice([0.0, 5.0, 10.0, 15.0])
        slippage_bps = rng.choice([0.0, 1.0, 2.0, 3.0])
        slippage_liq_factor = rng.choice([0.0, 1.0, 2.0])
        borrow_bps = rng.choice([0.0, 50.0, 100.0])

    sensitivity = base_cost.get("sensitivityBps")
    sensitivity_bps: list[float] = [5.0, 10.0, 15.0]
    if isinstance(sensitivity, list):
        parsed = [_as_float(x, 0.0) for x in sensitivity]
        parsed = [x for x in parsed if x >= 0.0]
        if parsed:
            sensitivity_bps = parsed

    return {
        "operatingMode": mode,
        "maxHoldings": max_holdings,
        "sides": {
            "longEnabled": True,
            "shortEnabled": bool(short_enabled),
        },
        "thresholds": {
            "entryMinUpProb": entry_min_up,
            "entryMinEv": entry_min_ev,
            "addMinEv": add_min_ev,
            "exitMinUpProb": exit_min_up,
            "exitMinEv": exit_min_ev,
            "revRiskWarn": rev_risk_warn,
            "revRiskHigh": rev_risk_high,
            "cutLossWarnPct": cut_loss_warn,
            "cutLossHardPct": cut_loss_hard,
            "takeProfitHintPct": take_profit_hint,
            "switchMinEvGap": switch_min_ev_gap,
            "maxNewEntriesPerDay": max_new_entries_per_day,
            "newEntryMaxRank": new_entry_max_rank,
            "exitIfUnranked": exit_if_unranked,
            "exitGateNgMinHoldingDays": exit_gate_ng_min_holding_days,
            "exitGateNgMinPnlPct": exit_gate_ng_min_pnl_pct,
        },
        "costModel": {
            "feesBps": fees_bps,
            "slippageBps": slippage_bps,
            "slippageLiquidityFactorBps": slippage_liq_factor,
            "borrowShortBpsAnnual": borrow_bps,
            "sensitivityBps": sensitivity_bps,
        },
        "portfolioConstraints": {
            "grossUnitsCap": 10,
            "maxNetUnits": max_net_units,
            "maxUnitsPerTicker": max_units_per_ticker,
            "maxPerSector": max_per_sector,
            "minLiquidity20d": min_liquidity20d,
            "shortBlacklist": base.get("portfolioConstraints", {}).get("shortBlacklist", []),
        },
    }


def _stage_specs_from_config(cfg: dict[str, Any]) -> tuple[StageSpec, StageSpec, StageSpec]:
    opt = cfg.get("optimization") if isinstance(cfg.get("optimization"), dict) else {}
    s0 = StageSpec(name="stage0", order=0, months=max(1, int(opt.get("stage0Months", 2))))
    s1 = StageSpec(name="stage1", order=1, months=max(1, int(opt.get("stage1Months", 12))))
    s2 = StageSpec(name="stage2", order=2, months=max(1, int(opt.get("stage2Months", 36))))
    return s0, s1, s2


def _evaluate_stage(
    *,
    repo: ToredexRepository,
    stage: StageSpec,
    mode: str,
    config_override: dict[str, Any],
    start_date: date,
    end_date: date,
    git_commit: str,
    optimization_raw: dict[str, Any],
    db_path: str | None = None,
) -> dict[str, Any]:
    cfg = load_toredex_config(override=config_override)
    config_hash = cfg.config_hash

    cached = _repo_call_with_retry(
        lambda: repo.get_optimization_result(
            config_hash=config_hash,
            stage=stage.name,
            start_date=start_date,
            end_date=end_date,
            operating_mode=mode,
        )
    )
    if isinstance(cached, dict):
        metrics_json = str(cached.get("metrics_json") or "")
        payload: dict[str, Any] = {}
        if metrics_json:
            try:
                parsed = json.loads(metrics_json)
                if isinstance(parsed, dict):
                    payload = parsed
            except Exception:
                payload = {}
        payload_pass = payload.get("stage_pass")
        if isinstance(payload_pass, bool):
            pass_value = payload_pass
        else:
            pass_value = str(cached.get("status") or "").strip().lower() == "pass"
        return {
            "cached": True,
            "config_hash": config_hash,
            "stage": stage.name,
            "stage_order": stage.order,
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "season_id": str(cached.get("season_id") or ""),
            "pass": pass_value,
            "score_net_return_pct": cached.get("score_net_return_pct"),
            "score_objective": payload.get("score_objective") if isinstance(payload.get("score_objective"), (int, float)) else cached.get("score_net_return_pct"),
            "trade_count": payload.get("trade_count") if isinstance(payload.get("trade_count"), (int, float)) else None,
            "pass_reason": str(payload.get("stage_pass_reason") or ""),
            "result": payload,
        }

    season_id_base = f"toredex_opt_{mode}_{stage.name}_{config_hash[:12]}_{start_date.isoformat()}_{end_date.isoformat()}"
    season_id = season_id_base
    if db_path:
        # Avoid idempotent no-op reuse on shared worker DBs when the same config hash is re-evaluated.
        season_id = f"{season_id_base}_{uuid.uuid4().hex[:8]}"
    if db_path:
        result = _run_backtest_via_subprocess(
            season_id=season_id,
            start_date=start_date.isoformat(),
            end_date=end_date.isoformat(),
            config_override=config_override,
            db_path=db_path,
        )
    else:
        result = run_backtest(
            season_id=season_id,
            start_date=start_date.isoformat(),
            end_date=end_date.isoformat(),
            dry_run=False,
            config_override=config_override,
        )

    final_metrics = result.get("final_metrics") if isinstance(result.get("final_metrics"), dict) else {}
    risk_gate = result.get("risk_gate") if isinstance(result.get("risk_gate"), dict) else {}
    trade_count = _count_trade_events(result)
    fail_reasons: list[str] = []
    if not bool(risk_gate.get("pass", True)):
        fail_reasons.append(str(risk_gate.get("reason") or "RISK_GATE"))
    if bool(final_metrics.get("game_over")):
        fail_reasons.append("R_GAME_OVER")
    min_trades = _resolve_min_trades(optimization_raw).get(stage.name, 0)
    if trade_count < int(min_trades):
        fail_reasons.append(f"MIN_TRADES({trade_count}<{int(min_trades)})")

    score_net_return_pct = float(final_metrics.get("net_cum_return_pct") or final_metrics.get("cum_return_pct") or 0.0)

    rollup = result.get("rollup") if isinstance(result.get("rollup"), dict) else {}
    worst_month = _as_optional_float(rollup.get("worst_month_pct"))
    max_turnover = _as_optional_float(rollup.get("max_turnover_pct_per_month"))
    max_abs_net_units = _as_optional_float(rollup.get("max_abs_net_units"))
    reason_counts_raw = result.get("reason_counts")
    reason_counts: dict[str, int] = {}
    if isinstance(reason_counts_raw, dict):
        for reason_id, count_value in reason_counts_raw.items():
            reason_key = str(reason_id or "").strip()
            if not reason_key:
                continue
            reason_counts[reason_key] = max(0, _as_int(count_value, 0))
    elif isinstance(reason_counts_raw, list):
        for row in reason_counts_raw:
            if not isinstance(row, dict):
                continue
            reason_key = str(row.get("reason_id") or "").strip()
            if not reason_key:
                continue
            reason_counts[reason_key] = max(0, _as_int(row.get("count"), 0))
    if db_path is None:
        if worst_month is None:
            worst_month = _repo_call_with_retry(
                lambda: repo.get_worst_month_return_pct(season_id, before_or_equal=end_date)
            )
        if max_turnover is None:
            max_turnover = _repo_call_with_retry(
                lambda: repo.get_max_turnover_pct_per_month(season_id, before_or_equal=end_date)
            )
        if max_abs_net_units is None:
            max_abs_net_units = _repo_call_with_retry(
                lambda: repo.get_max_abs_net_units(season_id, before_or_equal=end_date)
            )
        if not reason_counts:
            try:
                rows = _repo_call_with_retry(lambda: repo.get_trade_reason_counts(season_id))
            except Exception:
                rows = []
            if isinstance(rows, list):
                for row in rows:
                    if not isinstance(row, dict):
                        continue
                    reason_key = str(row.get("reason_id") or "").strip()
                    if not reason_key:
                        continue
                    reason_counts[reason_key] = max(0, _as_int(row.get("count"), 0))
    top1_entry_share = _compute_top1_entry_share(reason_counts)
    top1_share_caps_pct = _resolve_top1_entry_share_caps_pct(optimization_raw)
    stage_top1_cap_pct = top1_share_caps_pct.get(stage.name)
    if stage_top1_cap_pct is not None and (top1_entry_share * 100.0) > (float(stage_top1_cap_pct) + 1e-9):
        fail_reasons.append(f"TOP1_ENTRY_SHARE({top1_entry_share * 100.0:.3f}>{float(stage_top1_cap_pct):.3f})")
    passed = len(fail_reasons) == 0
    score_objective, score_components = _build_stage_score(
        stage_name=stage.name,
        passed=passed,
        final_metrics=final_metrics,
        score_net_return_pct=score_net_return_pct,
        worst_month=worst_month,
        max_turnover=max_turnover,
        max_abs_net_units=max_abs_net_units,
        trade_count=trade_count,
        reason_counts=reason_counts,
        opt_raw=optimization_raw,
    )

    result["score_objective"] = float(score_objective)
    result["score_components"] = score_components
    result["trade_count"] = int(trade_count)
    result["stage_pass"] = bool(passed)
    result["stage_pass_reason"] = ";".join([x for x in fail_reasons if x])

    _repo_call_with_retry(
        lambda: repo.save_optimization_result(
            {
                "run_id": str(uuid.uuid4()),
                "config_hash": config_hash,
                "git_commit": git_commit,
                "operating_mode": mode,
                "season_id": season_id,
                "stage": stage.name,
                "stage_order": stage.order,
                "start_date": start_date,
                "end_date": end_date,
                "status": "pass" if passed else "fail",
                "score_net_return_pct": score_net_return_pct,
                "max_drawdown_pct": final_metrics.get("max_drawdown_pct"),
                "worst_month_pct": worst_month,
                "turnover_pct_avg": max_turnover,
                "net_exposure_units_max": max_abs_net_units,
                "metrics_json": json.dumps(result, ensure_ascii=False, sort_keys=True),
                "artifact_path": f"runs/{season_id}",
            }
        )
    )

    return {
        "cached": False,
        "config_hash": config_hash,
        "stage": stage.name,
        "stage_order": stage.order,
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
        "season_id": season_id,
        "pass": passed,
        "score_net_return_pct": score_net_return_pct,
        "score_objective": score_objective,
        "trade_count": trade_count,
        "pass_reason": ";".join([x for x in fail_reasons if x]),
        "worker_db_path": str(db_path or ""),
        "result": result,
    }


def _evaluate_stage_batch(
    *,
    repo: ToredexRepository,
    stage: StageSpec,
    mode: str,
    overrides: list[dict[str, Any]],
    start_date: date,
    end_date: date,
    git_commit: str,
    optimization_raw: dict[str, Any],
    parallel_workers: int,
    parallel_db_paths: list[str],
) -> list[dict[str, Any]]:
    if not overrides:
        return []

    worker_limit = max(1, int(parallel_workers))
    candidate_paths = [p for p in parallel_db_paths if str(p).strip()]
    def _build_fail_payload(
        *,
        override: dict[str, Any],
        preferred_db_path: str | None,
        reason_code: str,
        error_text: str,
    ) -> dict[str, Any]:
        cfg_hash = load_toredex_config(override=override).config_hash
        score_floor = -1_000_000_000.0
        result_payload = {
            "ok": False,
            "error": str(error_text),
            "stage_pass": False,
            "stage_pass_reason": str(reason_code),
            "score_objective": score_floor,
            "trade_count": 0,
        }
        return {
            "cached": False,
            "config_hash": cfg_hash,
            "stage": stage.name,
            "stage_order": stage.order,
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "season_id": "",
            "pass": False,
            "score_net_return_pct": -999.0,
            "score_objective": score_floor,
            "trade_count": 0,
            "pass_reason": str(reason_code),
            "worker_db_path": str(preferred_db_path or ""),
            "result": result_payload,
        }

    def _evaluate_one_with_fallback(
        *,
        override: dict[str, Any],
        preferred_db_path: str | None,
    ) -> dict[str, Any]:
        try_paths: list[str] = []
        if preferred_db_path:
            try_paths.append(preferred_db_path)
        for path in candidate_paths:
            if preferred_db_path and path == preferred_db_path:
                continue
            try_paths.append(path)

        last_lock_error: Exception | None = None
        if not try_paths:
            return _evaluate_stage(
                repo=repo,
                stage=stage,
                mode=mode,
                config_override=override,
                start_date=start_date,
                end_date=end_date,
                git_commit=git_commit,
                optimization_raw=optimization_raw,
                db_path=None,
            )

        for db_path in try_paths:
            try:
                return _evaluate_stage(
                    repo=repo,
                    stage=stage,
                    mode=mode,
                    config_override=override,
                    start_date=start_date,
                    end_date=end_date,
                    git_commit=git_commit,
                    optimization_raw=optimization_raw,
                    db_path=db_path,
                )
            except Exception as exc:
                if _is_duckdb_lock_error(exc):
                    last_lock_error = exc
                    continue
                return _build_fail_payload(
                    override=override,
                    preferred_db_path=db_path,
                    reason_code="EVAL_ERROR",
                    error_text=str(exc),
                )

        if last_lock_error is not None:
            return _build_fail_payload(
                override=override,
                preferred_db_path=preferred_db_path,
                reason_code="DB_LOCK",
                error_text=str(last_lock_error),
            )
        return _build_fail_payload(
            override=override,
            preferred_db_path=preferred_db_path,
            reason_code="DB_LOCK",
            error_text="DB_LOCK",
        )

    if not candidate_paths or worker_limit <= 1:
        preferred_path = candidate_paths[0] if candidate_paths else None
        out: list[dict[str, Any]] = []
        for override in overrides:
            out.append(
                _evaluate_one_with_fallback(
                    override=override,
                    preferred_db_path=preferred_path,
                )
            )
        return out

    active_paths = candidate_paths[: min(worker_limit, len(candidate_paths))]
    buckets: list[list[dict[str, Any]]] = [[] for _ in active_paths]
    for idx, override in enumerate(overrides):
        buckets[idx % len(active_paths)].append(override)

    out: list[dict[str, Any]] = []

    def _run_bucket(path: str, payloads: list[dict[str, Any]]) -> list[dict[str, Any]]:
        bucket_results: list[dict[str, Any]] = []
        for override in payloads:
            bucket_results.append(_evaluate_one_with_fallback(override=override, preferred_db_path=path))
        return bucket_results

    with ThreadPoolExecutor(max_workers=len(active_paths)) as executor:
        futures = []
        for idx, payloads in enumerate(buckets):
            if not payloads:
                continue
            futures.append(executor.submit(_run_bucket, active_paths[idx], payloads))
        for future in as_completed(futures):
            out.extend(future.result())
    return out


def run_self_improve(
    *,
    mode: str = "challenger",
    iterations: int | None = None,
    stage2_topk: int | None = None,
    seed: int | None = None,
    stage0_months: int | None = None,
    stage1_months: int | None = None,
    stage2_months: int | None = None,
    parallel_workers: int | None = None,
    parallel_db_paths: list[str] | None = None,
) -> dict[str, Any]:
    mode_norm = str(mode or "challenger").strip().lower()
    if mode_norm not in {"champion", "challenger"}:
        mode_norm = "challenger"

    repo = ToredexRepository()
    base_cfg = load_toredex_config()
    base_data = dict(base_cfg.data)

    opt = base_cfg.optimization
    opt_raw = base_data.get("optimization") if isinstance(base_data.get("optimization"), dict) else {}
    total_iters = int(iterations) if iterations is not None else int(opt.get("iterationsPerRun", 12))
    total_iters = max(1, total_iters)
    topk = int(stage2_topk) if stage2_topk is not None else int(opt.get("stage2TopK", 3))
    topk = max(1, topk)
    optimize_cost_model = _as_bool(opt_raw.get("optimizeCostModel"), False)
    stage1_budget_default = max(topk * 4, min(total_iters, 8))
    stage1_max_candidates = max(1, _as_int(opt_raw.get("stage1MaxCandidates"), stage1_budget_default))
    raw_parallel_paths: Any = parallel_db_paths if parallel_db_paths is not None else opt_raw.get("parallelDbPaths")
    resolved_parallel_paths = _normalize_parallel_db_paths(raw_parallel_paths)
    parallel_workers_default = _as_int(opt_raw.get("parallelWorkers"), len(resolved_parallel_paths) or 1)
    parallel_workers_value = (
        max(1, int(parallel_workers)) if parallel_workers is not None else max(1, int(parallel_workers_default))
    )
    if resolved_parallel_paths:
        parallel_workers_value = min(parallel_workers_value, len(resolved_parallel_paths))
    else:
        parallel_workers_value = 1

    latest = repo.get_latest_available_asof()
    if latest is None:
        raise RuntimeError("K_NO_SNAPSHOT: latest asOf not found")
    end_date = date(int(str(latest)[:4]), int(str(latest)[4:6]), int(str(latest)[6:8]))

    stage0_spec, stage1_spec, stage2_spec = _stage_specs_from_config(base_data)
    if stage0_months is not None:
        stage0_spec = StageSpec(name=stage0_spec.name, order=stage0_spec.order, months=max(1, int(stage0_months)))
    if stage1_months is not None:
        stage1_spec = StageSpec(name=stage1_spec.name, order=stage1_spec.order, months=max(1, int(stage1_months)))
    if stage2_months is not None:
        stage2_spec = StageSpec(name=stage2_spec.name, order=stage2_spec.order, months=max(1, int(stage2_months)))
    stage0_range = _range_months_ending_at(end_date, stage0_spec.months)
    stage1_range = _range_months_ending_at(end_date, stage1_spec.months)
    stage2_range = _range_months_ending_at(end_date, stage2_spec.months)

    if seed is None:
        seed = int(end_date.strftime("%Y%m%d"))
    rng = random.Random(seed)
    git_commit = _safe_git_commit()

    stage0_results: list[dict[str, Any]] = []
    stage1_results: list[dict[str, Any]] = []
    stage2_results: list[dict[str, Any]] = []

    base_override = {
        "operatingMode": mode_norm,
        "costModel": {
            "sensitivityBps": [5.0, 10.0, 15.0],
        },
    }

    candidates: list[dict[str, Any]] = []
    candidate_by_hash: dict[str, dict[str, Any]] = {}
    stage0_cache_exists_by_hash: dict[str, bool] = {}

    def _has_cached_stage0(cfg_hash: str) -> bool:
        key = str(cfg_hash or "")
        if not key:
            return False
        cached = stage0_cache_exists_by_hash.get(key)
        if cached is not None:
            return bool(cached)
        row = _repo_call_with_retry(
            lambda: repo.get_optimization_result(
                config_hash=key,
                stage=stage0_spec.name,
                start_date=stage0_range[0],
                end_date=stage0_range[1],
                operating_mode=mode_norm,
            )
        )
        exists = isinstance(row, dict)
        stage0_cache_exists_by_hash[key] = exists
        return exists

    def _register_candidate(override: dict[str, Any], *, skip_cached_stage0: bool) -> bool:
        cfg_hash = load_toredex_config(override=override).config_hash
        if cfg_hash in candidate_by_hash:
            return False
        if skip_cached_stage0 and _has_cached_stage0(cfg_hash):
            return False
        candidate_by_hash[cfg_hash] = override
        candidates.append(override)
        return True

    _register_candidate(base_override, skip_cached_stage0=True)
    attempts = 0
    max_attempts = max(60, total_iters * 20)
    while len(candidates) < total_iters and attempts < max_attempts:
        attempts += 1
        _register_candidate(
            _sample_candidate_override(
                rng=rng,
                mode=mode_norm,
                base=base_data,
                include_short_bias=True,
                optimize_cost_model=optimize_cost_model,
            ),
            skip_cached_stage0=True,
        )

    if not candidates:
        _register_candidate(base_override, skip_cached_stage0=False)

    stage0_results.extend(
        _evaluate_stage_batch(
            repo=repo,
            stage=stage0_spec,
            mode=mode_norm,
            overrides=candidates,
            start_date=stage0_range[0],
            end_date=stage0_range[1],
            git_commit=git_commit,
            optimization_raw=opt_raw,
            parallel_workers=parallel_workers_value,
            parallel_db_paths=resolved_parallel_paths,
        )
    )

    stage0_passed = [x for x in stage0_results if bool(x.get("pass"))]
    stage0_passed.sort(key=_score_sort_value, reverse=True)

    stage1_overrides: list[dict[str, Any]] = []
    for seed_stage0 in stage0_passed[:stage1_max_candidates]:
        cfg_hash = str(seed_stage0.get("config_hash") or "")
        matched_override = candidate_by_hash.get(cfg_hash)
        if matched_override is None:
            continue
        stage1_overrides.append(matched_override)

    stage1_results.extend(
        _evaluate_stage_batch(
            repo=repo,
            stage=stage1_spec,
            mode=mode_norm,
            overrides=stage1_overrides,
            start_date=stage1_range[0],
            end_date=stage1_range[1],
            git_commit=git_commit,
            optimization_raw=opt_raw,
            parallel_workers=parallel_workers_value,
            parallel_db_paths=resolved_parallel_paths,
        )
    )

    stage1_passed = [x for x in stage1_results if bool(x.get("pass"))]
    stage1_passed.sort(key=_score_sort_value, reverse=True)

    stage2_overrides: list[dict[str, Any]] = []
    for pick in stage1_passed[:topk]:
        cfg_hash = str(pick.get("config_hash") or "")
        matched_override = candidate_by_hash.get(cfg_hash)
        if matched_override is None:
            continue
        stage2_overrides.append(matched_override)

    stage2_results.extend(
        _evaluate_stage_batch(
            repo=repo,
            stage=stage2_spec,
            mode=mode_norm,
            overrides=stage2_overrides,
            start_date=stage2_range[0],
            end_date=stage2_range[1],
            git_commit=git_commit,
            optimization_raw=opt_raw,
            parallel_workers=parallel_workers_value,
            parallel_db_paths=resolved_parallel_paths,
        )
    )

    best_stage2 = None
    if stage2_results:
        ordered = sorted(stage2_results, key=_score_sort_value, reverse=True)
        best_stage2 = ordered[0]

    return {
        "ok": True,
        "mode": mode_norm,
        "seed": seed,
        "git_commit": git_commit,
        "iterations": total_iters,
        "stage2_topk": topk,
        "candidate_count": len(candidates),
        "stage1_max_candidates": stage1_max_candidates,
        "optimize_cost_model": bool(optimize_cost_model),
        "parallel": {
            "workers": int(parallel_workers_value),
            "db_paths": list(resolved_parallel_paths),
        },
        "ranges": {
            "stage0": {
                "start": stage0_range[0].isoformat(),
                "end": stage0_range[1].isoformat(),
            },
            "stage1": {
                "start": stage1_range[0].isoformat(),
                "end": stage1_range[1].isoformat(),
            },
            "stage2": {
                "start": stage2_range[0].isoformat(),
                "end": stage2_range[1].isoformat(),
            },
        },
        "counts": {
            "stage0": len(stage0_results),
            "stage0_pass": len(stage0_passed),
            "stage1": len(stage1_results),
            "stage1_pass": len(stage1_passed),
            "stage2": len(stage2_results),
        },
        "stage0": stage0_results,
        "stage1": stage1_results,
        "stage2": stage2_results,
        "best_stage2": best_stage2,
    }


def _coerce_optional_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except Exception:
        return None


def _coerce_optional_bool(value: Any) -> bool | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        text = value.strip().lower()
        if text in {"1", "true", "yes", "y", "on"}:
            return True
        if text in {"0", "false", "no", "n", "off"}:
            return False
    return None


def _best_stage2_from_result(result: dict[str, Any]) -> dict[str, Any] | None:
    best = result.get("best_stage2")
    return best if isinstance(best, dict) else None


def _best_stage2_summary(best: dict[str, Any] | None) -> dict[str, Any]:
    if not isinstance(best, dict):
        return {
            "exists": False,
            "pass": False,
            "config_hash": "",
            "season_id": "",
            "score_net_return_pct": None,
            "score_objective": None,
            "trade_count": None,
            "pass_reason": "",
        }
    return {
        "exists": True,
        "pass": bool(best.get("pass")),
        "config_hash": str(best.get("config_hash") or ""),
        "season_id": str(best.get("season_id") or ""),
        "score_net_return_pct": _coerce_optional_float(best.get("score_net_return_pct")),
        "score_objective": _coerce_optional_float(best.get("score_objective")),
        "trade_count": _as_int(best.get("trade_count"), 0) if best.get("trade_count") is not None else None,
        "pass_reason": str(best.get("pass_reason") or ""),
    }


def _is_better_best(left: dict[str, Any] | None, right: dict[str, Any] | None) -> bool:
    if not isinstance(right, dict):
        return False
    if not isinstance(left, dict):
        return True
    return _score_sort_value(right) > _score_sort_value(left)


def _target_reached(
    *,
    best: dict[str, Any] | None,
    target_net_return_pct: float | None,
    target_score_objective: float | None,
    require_stage2_pass: bool,
) -> tuple[bool, str]:
    if not isinstance(best, dict):
        return False, "NO_STAGE2_RESULT"

    if require_stage2_pass and not bool(best.get("pass")):
        return False, "STAGE2_NOT_PASS"

    if target_net_return_pct is not None:
        net_ret = _coerce_optional_float(best.get("score_net_return_pct"))
        if net_ret is None or net_ret < target_net_return_pct:
            return False, "TARGET_NET_RETURN_NOT_REACHED"

    if target_score_objective is not None:
        score_obj = _coerce_optional_float(best.get("score_objective"))
        if score_obj is None or score_obj < target_score_objective:
            return False, "TARGET_SCORE_OBJECTIVE_NOT_REACHED"

    return True, "TARGET_REACHED"


def run_self_improve_loop(
    *,
    mode: str = "challenger",
    iterations: int | None = None,
    stage2_topk: int | None = None,
    seed: int | None = None,
    stage0_months: int | None = None,
    stage1_months: int | None = None,
    stage2_months: int | None = None,
    parallel_workers: int | None = None,
    parallel_db_paths: list[str] | None = None,
    max_cycles: int | None = None,
    target_net_return_pct: float | None = None,
    target_score_objective: float | None = None,
    require_stage2_pass: bool | None = None,
) -> dict[str, Any]:
    base_cfg = load_toredex_config()
    opt_raw = base_cfg.data.get("optimization") if isinstance(base_cfg.data.get("optimization"), dict) else {}

    max_cycles_value = _as_int(max_cycles if max_cycles is not None else opt_raw.get("loopMaxCycles"), 10)
    max_cycles_value = max(1, max_cycles_value)

    target_net = _coerce_optional_float(
        target_net_return_pct if target_net_return_pct is not None else opt_raw.get("loopTargetNetReturnPct")
    )
    target_score = _coerce_optional_float(
        target_score_objective if target_score_objective is not None else opt_raw.get("loopTargetScoreObjective")
    )
    require_pass_cfg = _coerce_optional_bool(opt_raw.get("loopRequireStage2Pass"))
    require_pass = (
        _coerce_optional_bool(require_stage2_pass)
        if require_stage2_pass is not None
        else (require_pass_cfg if require_pass_cfg is not None else True)
    )

    repo = ToredexRepository()
    latest = repo.get_latest_available_asof()
    if seed is None:
        if latest is not None:
            base_seed = _as_int(latest, 0)
        else:
            base_seed = int(date.today().strftime("%Y%m%d"))
    else:
        base_seed = _as_int(seed, int(date.today().strftime("%Y%m%d")))

    cycle_summaries: list[dict[str, Any]] = []
    best_overall: dict[str, Any] | None = None
    best_overall_cycle: int | None = None

    loop_started = time.monotonic()
    stop_reason = "MAX_CYCLES_REACHED"
    reached = False
    completed_cycles = 0

    for cycle_idx in range(max_cycles_value):
        cycle_no = cycle_idx + 1
        cycle_seed = base_seed + cycle_idx
        cycle_started = time.monotonic()

        run_result = run_self_improve(
            mode=mode,
            iterations=iterations,
            stage2_topk=stage2_topk,
            seed=cycle_seed,
            stage0_months=stage0_months,
            stage1_months=stage1_months,
            stage2_months=stage2_months,
            parallel_workers=parallel_workers,
            parallel_db_paths=parallel_db_paths,
        )

        best_stage2 = _best_stage2_from_result(run_result)
        best_summary = _best_stage2_summary(best_stage2)
        cycle_elapsed = round(time.monotonic() - cycle_started, 3)
        cycle_summaries.append(
            {
                "cycle": cycle_no,
                "seed": cycle_seed,
                "elapsed_sec": cycle_elapsed,
                "counts": dict(run_result.get("counts") or {}),
                "best_stage2": best_summary,
            }
        )

        completed_cycles = cycle_no
        if _is_better_best(best_overall, best_stage2):
            best_overall = best_stage2
            best_overall_cycle = cycle_no

        hit, reason = _target_reached(
            best=best_stage2,
            target_net_return_pct=target_net,
            target_score_objective=target_score,
            require_stage2_pass=bool(require_pass),
        )
        if hit:
            reached = True
            stop_reason = reason
            break
        stop_reason = reason

    total_elapsed = round(time.monotonic() - loop_started, 3)
    best_overall_summary = _best_stage2_summary(best_overall)
    if best_overall_cycle is not None:
        best_overall_summary["cycle"] = int(best_overall_cycle)

    return {
        "ok": True,
        "mode": str(mode or "challenger").strip().lower(),
        "max_cycles": max_cycles_value,
        "completed_cycles": completed_cycles,
        "base_seed": base_seed,
        "targets": {
            "net_return_pct": target_net,
            "score_objective": target_score,
            "require_stage2_pass": bool(require_pass),
        },
        "reached": bool(reached),
        "stop_reason": stop_reason,
        "elapsed_sec": total_elapsed,
        "best_overall": best_overall_summary,
        "cycles": cycle_summaries,
    }
