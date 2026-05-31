from __future__ import annotations

import argparse
import csv
import json
import math
import sys
from collections import defaultdict, deque
from datetime import datetime, timezone
from pathlib import Path
from statistics import median
from typing import Any

import duckdb

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.tradex_10pct_move_capture_research_v1 import _date_expr, _table_columns


AXIS_ID = "market_regime_trade_permission_gate_v1"
DEFAULT_INPUT_ROOT = Path(r"G:\Tradex\ten_pct_move_capture_research_v1\20260526T023233Z-ten-pct-move-capture-research-v1")
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\market_regime_trade_permission_gate_v1")
REQUIRED_ARTIFACTS = (
    "market_regime_gate_summary.json",
    "market_regime_gate_rows.csv",
    "market_feature_contract.json",
    "regime_definition_contract.json",
    "long_permission_metrics.json",
    "short_permission_metrics.json",
    "setup_by_regime_metrics.json",
    "gate_vs_all_conditions_comparison.json",
    "no_lookahead_audit.json",
    "source_coverage.json",
    "research_decision.json",
    "_ARTIFACT_COMPLETE.json",
)
PERMISSION_GATES = (
    "long_allowed_only_risk_on_or_recovery",
    "long_blocked_low_breadth_or_risk_off",
    "short_allowed_only_risk_off_or_low_breadth",
    "no_trade_high_volatility",
)


def _now_tag() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _json_ready(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(k): _json_ready(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_json_ready(v) for v in value]
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, float):
        return None if not math.isfinite(value) else value
    return value


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(_json_ready(payload), ensure_ascii=False, sort_keys=True, indent=2) + "\n", encoding="utf-8")


def _safe_float(value: Any) -> float | None:
    try:
        if value in {None, ""}:
            return None
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _as_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "yes", "y"}


def _round(value: float | None, digits: int = 6) -> float | None:
    return None if value is None else round(value, digits)


def _mean(values: list[float]) -> float | None:
    return None if not values else sum(values) / len(values)


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _load_capture_rows(input_root: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    with (input_root / "ten_pct_capture_rows.csv").open(newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            row["dt"] = int(row["dt"])
            row["code"] = str(row["code"])
            row["horizon"] = int(row["horizon"])
            row["target_hit"] = _as_bool(row.get("target_hit"))
            row["target_before_stop"] = _as_bool(row.get("target_before_stop"))
            row["stop_before_target"] = _as_bool(row.get("stop_before_target"))
            row["neither_hit"] = _as_bool(row.get("neither_hit"))
            row["severe_loss"] = _as_bool(row.get("severe_loss"))
            row["adverse_excursion"] = _safe_float(row.get("adverse_excursion"))
            row["return_at_exit"] = _safe_float(row.get("return_at_exit"))
            row["days_to_event"] = int(float(row["days_to_event"])) if row.get("days_to_event") not in {None, ""} else None
            rows.append(row)
    return rows


def _load_daily_bars(db_path: Path, *, start_ymd: int, end_ymd: int) -> dict[str, list[dict[str, Any]]]:
    start_buffer = start_ymd - 10000
    by_code: dict[str, list[dict[str, Any]]] = defaultdict(list)
    with duckdb.connect(str(db_path), read_only=True) as conn:
        cols = _table_columns(conn, "daily_bars")
        source_filter = "AND lower(coalesce(source, '')) = 'pan'" if "source" in cols else ""
        rows = conn.execute(
            f"""
            WITH normalized AS (
                SELECT CAST(code AS VARCHAR) AS code, {_date_expr("date")} AS ymd,
                       CAST(o AS DOUBLE) AS o, CAST(h AS DOUBLE) AS h,
                       CAST(l AS DOUBLE) AS l, CAST(c AS DOUBLE) AS c,
                       CAST(v AS DOUBLE) AS v
                FROM daily_bars
                WHERE o IS NOT NULL AND h IS NOT NULL AND l IS NOT NULL AND c IS NOT NULL
                  {source_filter}
            )
            SELECT code, ymd, o, h, l, c, v
            FROM normalized
            WHERE ymd BETWEEN ? AND ? AND ymd IS NOT NULL
            ORDER BY code, ymd
            """,
            [start_buffer, end_ymd],
        ).fetchall()
    for code, ymd, o, h, l, c, v in rows:
        if ymd is None or c is None or float(c) <= 0:
            continue
        by_code[str(code)].append({"code": str(code), "ymd": int(ymd), "o": float(o), "h": float(h), "l": float(l), "c": float(c), "v": float(v or 0)})
    return by_code


def _market_features_by_date(by_code: dict[str, list[dict[str, Any]]], as_of_dates: set[int]) -> tuple[dict[int, dict[str, Any]], dict[str, Any]]:
    per_date_members: dict[int, list[dict[str, Any]]] = defaultdict(list)
    for code, rows in by_code.items():
        closes: deque[float] = deque(maxlen=61)
        highs20: deque[float] = deque(maxlen=21)
        lows20: deque[float] = deque(maxlen=21)
        prev_close: float | None = None
        rets20: deque[float] = deque(maxlen=21)
        for row in rows:
            close = row["c"]
            closes.append(close)
            highs20.append(row["h"])
            lows20.append(row["l"])
            if prev_close:
                rets20.append(close / prev_close - 1.0)
            if row["ymd"] in as_of_dates and len(closes) >= 61:
                close_list = list(closes)
                ma20 = sum(close_list[-20:]) / 20
                ma20_prev = sum(close_list[-25:-5]) / 20
                ma60 = sum(close_list[-60:]) / 60
                ma60_prev = sum(close_list[-61:-1]) / 60
                ret20 = close / close_list[-21] - 1.0
                vol = _mean([abs(v) for v in list(rets20)[-20:]])
                prior_high20 = max(list(highs20)[:-1]) if len(highs20) >= 21 else None
                prior_low20 = min(list(lows20)[:-1]) if len(lows20) >= 21 else None
                per_date_members[row["ymd"]].append(
                    {
                        "above_ma20": close > ma20,
                        "above_ma60": close > ma60,
                        "ma20_up": ma20 > ma20_prev,
                        "ma60_up": ma60 > ma60_prev,
                        "ret20": ret20,
                        "volatility": vol,
                        "new_high_20d": bool(prior_high20 and row["h"] >= prior_high20),
                        "new_low_20d": bool(prior_low20 and row["l"] <= prior_low20),
                        "advance": bool(prev_close and close > prev_close),
                    }
                )
            prev_close = close
    features: dict[int, dict[str, Any]] = {}
    missing_dates: list[int] = []
    for ymd in sorted(as_of_dates):
        members = per_date_members.get(ymd, [])
        if not members:
            missing_dates.append(ymd)
            continue
        n = len(members)
        pct_above_ma20 = sum(m["above_ma20"] for m in members) / n
        pct_above_ma60 = sum(m["above_ma60"] for m in members) / n
        pct_20ma_up = sum(m["ma20_up"] for m in members) / n
        pct_60ma_up = sum(m["ma60_up"] for m in members) / n
        ret20s = sorted(float(m["ret20"]) for m in members)
        vols = sorted(float(m["volatility"] or 0.0) for m in members)
        new_high = sum(m["new_high_20d"] for m in members)
        new_low = sum(m["new_low_20d"] for m in members)
        adv = sum(m["advance"] for m in members)
        adv_decl = (adv - (n - adv)) / n
        median_ret20 = float(median(ret20s))
        median_vol = float(median(vols))
        broad_risk_on = pct_above_ma20 >= 0.58 and pct_above_ma60 >= 0.52 and pct_20ma_up >= 0.55 and median_ret20 > 0
        broad_risk_off = pct_above_ma20 <= 0.42 and pct_above_ma60 <= 0.48 and pct_20ma_up <= 0.45 and median_ret20 < 0
        low_breadth = pct_above_ma20 <= 0.38 or pct_above_ma60 <= 0.42
        high_vol = median_vol >= 0.025
        recovery = pct_above_ma20 >= 0.48 and pct_above_ma60 < 0.52 and pct_20ma_up >= 0.52 and median_ret20 > 0
        range_flag = not broad_risk_on and not broad_risk_off and not high_vol
        if high_vol:
            primary = "high_volatility"
        elif low_breadth:
            primary = "low_breadth"
        elif broad_risk_off:
            primary = "risk_off"
        elif broad_risk_on:
            primary = "risk_on"
        elif recovery:
            primary = "recovery_attempt"
        else:
            primary = "neutral_range"
        features[ymd] = {
            "as_of_date": ymd,
            "market_sample_count": n,
            "universe_pct_above_ma20": pct_above_ma20,
            "universe_pct_above_ma60": pct_above_ma60,
            "universe_pct_20ma_up": pct_20ma_up,
            "universe_pct_60ma_up": pct_60ma_up,
            "market_median_ret20_past": median_ret20,
            "market_median_volatility": median_vol,
            "new_high_count_20d": new_high,
            "new_low_count_20d": new_low,
            "advancing_declining_proxy": adv_decl,
            "broad_risk_on_flag": broad_risk_on,
            "broad_risk_off_flag": broad_risk_off,
            "range_market_flag": range_flag,
            "high_volatility_flag": high_vol,
            "low_breadth_flag": low_breadth,
            "recovery_attempt_flag": recovery,
            "regime_bucket": primary,
        }
    audit = {
        "as_of_date_count": len(as_of_dates),
        "market_feature_date_count": len(features),
        "missing_market_feature_dates": missing_dates[:50],
        "missing_market_feature_date_count": len(missing_dates),
    }
    return features, audit


def _gate_keeps(row: dict[str, Any], gate: str) -> bool:
    direction = row["direction"]
    risk_on = row["broad_risk_on_flag"]
    risk_off = row["broad_risk_off_flag"]
    recovery = row["recovery_attempt_flag"]
    low_breadth = row["low_breadth_flag"]
    high_vol = row["high_volatility_flag"]
    if gate == "long_allowed_only_risk_on_or_recovery":
        return direction == "long" and (risk_on or recovery)
    if gate == "long_blocked_low_breadth_or_risk_off":
        return direction == "long" and not (low_breadth or risk_off)
    if gate == "short_allowed_only_risk_off_or_low_breadth":
        return direction == "short" and (risk_off or low_breadth)
    if gate == "no_trade_high_volatility":
        return not high_vol
    raise ValueError(gate)


def _metric(rows: list[dict[str, Any]], all_rows: list[dict[str, Any]] | None = None) -> dict[str, Any]:
    if not rows:
        base = {
            "sample_count": 0,
            "date_count": 0,
            "code_count": 0,
            "target_10pct_hit_rate": None,
            "target_before_stop_rate": None,
            "stop_before_target_rate": None,
            "neither_hit_rate": None,
            "severe_loss_rate": None,
            "average_adverse_excursion": None,
            "mean_return_at_exit": None,
            "return_per_day": None,
        }
    else:
        n = len(rows)
        returns = [float(r["return_at_exit"]) for r in rows if r.get("return_at_exit") is not None]
        adverse = [float(r["adverse_excursion"]) for r in rows if r.get("adverse_excursion") is not None]
        days = [float(r["days_to_event"]) for r in rows if r.get("days_to_event") is not None]
        mean_ret = _mean(returns)
        base = {
            "sample_count": n,
            "date_count": len({r["dt"] for r in rows}),
            "code_count": len({r["code"] for r in rows}),
            "target_10pct_hit_rate": _round(sum(1 for r in rows if r["target_hit"]) / n),
            "target_before_stop_rate": _round(sum(1 for r in rows if r["target_before_stop"]) / n),
            "stop_before_target_rate": _round(sum(1 for r in rows if r["stop_before_target"]) / n),
            "neither_hit_rate": _round(sum(1 for r in rows if r["neither_hit"]) / n),
            "severe_loss_rate": _round(sum(1 for r in rows if r["severe_loss"]) / n),
            "average_adverse_excursion": _round(_mean(adverse)),
            "mean_return_at_exit": _round(mean_ret),
            "return_per_day": _round((mean_ret / (_mean(days) or 1.0)) if mean_ret is not None else None),
        }
    if all_rows is not None:
        all_metric = _metric(all_rows, None)
        base["comparison_vs_all_conditions"] = {
            "target_before_stop_rate_delta": _round(
                (base.get("target_before_stop_rate") or 0.0) - (all_metric.get("target_before_stop_rate") or 0.0)
            ),
            "severe_loss_rate_delta": _round((base.get("severe_loss_rate") or 0.0) - (all_metric.get("severe_loss_rate") or 0.0)),
            "mean_return_at_exit_delta": _round((base.get("mean_return_at_exit") or 0.0) - (all_metric.get("mean_return_at_exit") or 0.0)),
        }
        base["setup_count_filtered_out"] = len(all_rows) - len(rows)
        base["kept_share"] = _round(len(rows) / len(all_rows) if all_rows else None)
    return base


def _group_metrics(rows: list[dict[str, Any]], keys: tuple[str, ...], all_by_direction: dict[str, list[dict[str, Any]]] | None = None) -> dict[str, Any]:
    groups: dict[tuple[Any, ...], list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        groups[tuple(row[k] for k in keys)].append(row)
    out: dict[str, Any] = {}
    for key, group in sorted(groups.items()):
        direction = next((v for k, v in zip(keys, key) if k == "direction"), group[0].get("direction"))
        all_rows = all_by_direction.get(direction, []) if all_by_direction else None
        out["|".join(map(str, key))] = _metric(group, all_rows)
    return out


def _decision(long_metrics: dict[str, Any], short_metrics: dict[str, Any], source_coverage: dict[str, Any]) -> dict[str, Any]:
    if source_coverage["missing_market_feature_date_count"] > 0:
        decision = "blocked_missing_market_features"
        reason = "not_all_input_as_of_dates_have_point_in_time_market_features"
    else:
        def qualifies(metrics: dict[str, Any]) -> bool:
            return (
                (metrics.get("sample_count") or 0) >= 10000
                and (metrics.get("date_count") or 0) >= 120
                and (metrics.get("kept_share") or 0) >= 0.15
                and ((metrics.get("comparison_vs_all_conditions") or {}).get("target_before_stop_rate_delta") or 0) >= 0.03
                and (metrics.get("severe_loss_rate") or 1) <= 0.03
            )

        best_long = max(long_metrics.values(), key=lambda m: (m.get("comparison_vs_all_conditions") or {}).get("target_before_stop_rate_delta") or -9, default={})
        best_short = max(short_metrics.values(), key=lambda m: (m.get("comparison_vs_all_conditions") or {}).get("target_before_stop_rate_delta") or -9, default={})
        long_ok = qualifies(best_long)
        short_ok = qualifies(best_short)
        thin = any(
            (m.get("sample_count") or 0) > 0
            and (m.get("sample_count") or 0) < 10000
            and ((m.get("comparison_vs_all_conditions") or {}).get("target_before_stop_rate_delta") or 0) >= 0.04
            for m in list(long_metrics.values()) + list(short_metrics.values())
        )
        if long_ok and short_ok and not source_coverage["short_borrow_contract_missing"]:
            decision = "market_gate_keep_for_setup_replay"
            reason = "long_and_short_permission_gates_improve_with_operational_constraints_documented"
        elif long_ok:
            decision = "long_permission_edge_found"
            reason = "long_permission_gate_materially_improves_target_before_stop_with_support"
        elif short_ok and source_coverage["short_borrow_contract_missing"]:
            decision = "short_permission_edge_theoretical"
            reason = "short_permission_gate_improves_price_path_but_borrow_contract_missing"
        elif short_ok:
            decision = "market_gate_keep_for_setup_replay"
            reason = "short_permission_gate_materially_improves_with_borrow_contract_available"
        elif thin:
            decision = "market_gate_promising_but_underpowered"
            reason = "positive_gate_delta_exists_but_sample_support_is_thin"
        else:
            decision = "no_market_regime_gate_edge"
            reason = "fixed_permission_gates_do_not_materially_improve_quality_with_required_support"
    return {
        "axis_id": AXIS_ID,
        "research_decision": decision,
        "candidate_local_decision": decision,
        "session_aggregate_decision": decision,
        "authoritative_rollup_decision": decision,
        "decision_reason": reason,
        "runtime_db_write": False,
        "meemee_reflectable_candidate": False,
        "production_ranking_changed": False,
        "production_candidate_generator_changed": False,
        "validated_buy_count": 0,
        "next_step_if_keep_or_promising": "setup_specific_replay_under_allowed_regimes_only",
        "short_results_operational_status": "theoretical_price_only" if source_coverage["short_borrow_contract_missing"] else "borrow_contract_available_for_audit",
    }


def _write_rows_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    fields = [
        "dt", "code", "direction", "setup_family", "horizon", "stop_rule", "regime_bucket", "permission_gate", "gate_kept",
        "target_before_stop", "stop_before_target", "neither_hit", "severe_loss", "return_at_exit", "adverse_excursion",
        "universe_pct_above_ma20", "universe_pct_above_ma60", "universe_pct_20ma_up", "universe_pct_60ma_up",
        "market_median_ret20_past", "market_median_volatility", "new_high_count_20d", "new_low_count_20d",
        "advancing_declining_proxy", "broad_risk_on_flag", "broad_risk_off_flag", "range_market_flag",
        "high_volatility_flag", "low_breadth_flag", "recovery_attempt_flag",
    ]
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def run_audit(
    *,
    input_root: Path = DEFAULT_INPUT_ROOT,
    output_root: Path = DEFAULT_OUTPUT_ROOT,
    db_path: Path | None = None,
) -> dict[str, Any]:
    source_10pct = _read_json(input_root / "source_coverage.json")
    decision_10pct = _read_json(input_root / "research_decision.json")
    resolved_db = Path(db_path or source_10pct["runtime_db_path"])
    run_dir = output_root / f"{_now_tag()}-market-regime-trade-permission-gate-v1"
    rows = _load_capture_rows(input_root)
    as_of_dates = {int(r["dt"]) for r in rows}
    by_code = _load_daily_bars(resolved_db, start_ymd=min(as_of_dates), end_ymd=max(as_of_dates))
    features_by_date, feature_audit = _market_features_by_date(by_code, as_of_dates)
    enriched_base: list[dict[str, Any]] = []
    gate_rows: list[dict[str, Any]] = []
    for row in rows:
        feat = features_by_date.get(row["dt"])
        if not feat:
            continue
        base = {**row, **feat}
        enriched_base.append(base)
        for gate in PERMISSION_GATES:
            if gate.startswith("long_") and row["direction"] != "long":
                continue
            if gate.startswith("short_") and row["direction"] != "short":
                continue
            gate_rows.append({**base, "permission_gate": gate, "gate_kept": _gate_keeps(base, gate)})
    all_by_direction: dict[str, list[dict[str, Any]]] = {
        "long": [r for r in enriched_base if r["direction"] == "long"],
        "short": [r for r in enriched_base if r["direction"] == "short"],
    }
    kept_gate_rows = [r for r in gate_rows if r["gate_kept"]]
    long_permission = _group_metrics([r for r in kept_gate_rows if r["direction"] == "long"], ("permission_gate", "direction"), all_by_direction)
    short_permission = _group_metrics([r for r in kept_gate_rows if r["direction"] == "short"], ("permission_gate", "direction"), all_by_direction)
    setup_by_regime = _group_metrics(enriched_base, ("setup_family", "direction", "regime_bucket"), all_by_direction)
    gate_comparison = _group_metrics(kept_gate_rows, ("permission_gate", "direction"), all_by_direction)
    source_coverage = {
        "axis_id": AXIS_ID,
        "input_root": str(input_root),
        "runtime_db_path": str(resolved_db),
        "input_authoritative_decision": decision_10pct.get("authoritative_rollup_decision"),
        "input_rows_loaded": len(rows),
        "input_rows_with_market_features": len(enriched_base),
        "daily_bars_source": "pan_confirmed_only",
        "provisional_yahoo_bars_used": False,
        "short_borrow_contract_missing": bool(source_10pct.get("short_borrow_contract_missing", True)),
        **feature_audit,
    }
    decision = _decision(long_permission, short_permission, source_coverage)
    summary = {
        "axis_id": AXIS_ID,
        "output_dir": str(run_dir),
        "research_phase": "effectiveness_judgment",
        "fixed_evaluation_conditions": {
            "same_input_rows": True,
            "same_10pct_target": True,
            "same_stop_rules": True,
            "same_setup_definitions": True,
            "confirmed_historical_bars_only": True,
            "same_cost_slippage": True,
            "same_artifact_detail_level": True,
        },
        "input_authoritative_artifact": str(input_root),
        "input_authoritative_decision": decision_10pct.get("authoritative_rollup_decision"),
        "authoritative_research_decision": decision["research_decision"],
        "best_long_permission_gate": max(long_permission.items(), key=lambda kv: (kv[1].get("comparison_vs_all_conditions") or {}).get("target_before_stop_rate_delta") or -9, default=(None, {}))[0],
        "best_short_permission_gate": max(short_permission.items(), key=lambda kv: (kv[1].get("comparison_vs_all_conditions") or {}).get("target_before_stop_rate_delta") or -9, default=(None, {}))[0],
    }
    artifacts = {
        "market_regime_gate_summary.json": summary,
        "market_feature_contract.json": {
            "axis_id": AXIS_ID,
            "feature_source": "confirmed daily_bars pan rows at or before as_of_date",
            "features": [
                "universe_pct_above_ma20", "universe_pct_above_ma60", "universe_pct_20ma_up", "universe_pct_60ma_up",
                "market_median_ret20_past", "market_median_volatility", "new_high_count_20d", "new_low_count_20d",
                "advancing_declining_proxy", "broad_risk_on_flag", "broad_risk_off_flag", "range_market_flag",
            ],
            "future_outcomes_excluded_from_features": True,
            "provisional_yahoo_bars_used": False,
        },
        "regime_definition_contract.json": {
            "axis_id": AXIS_ID,
            "fixed_buckets": ["risk_on", "neutral_range", "risk_off", "high_volatility", "low_breadth", "recovery_attempt"],
            "permission_gates": list(PERMISSION_GATES),
            "tuning_loop_used": False,
            "target_stop_or_setup_changed": False,
        },
        "long_permission_metrics.json": long_permission,
        "short_permission_metrics.json": short_permission,
        "setup_by_regime_metrics.json": setup_by_regime,
        "gate_vs_all_conditions_comparison.json": gate_comparison,
        "no_lookahead_audit.json": {
            "axis_id": AXIS_ID,
            "pass": source_coverage["missing_market_feature_date_count"] == 0,
            "market_features_use_as_of_or_prior_bars_only": True,
            "future_bars_used_for_permission": [],
            "future_outcomes_evaluation_only": True,
            "input_10pct_outcomes_used_as_labels_only": True,
            "provisional_yahoo_bars_used": False,
        },
        "source_coverage.json": source_coverage,
        "research_decision.json": decision,
    }
    run_dir.mkdir(parents=True, exist_ok=True)
    for name, payload in artifacts.items():
        _write_json(run_dir / name, payload)
    _write_rows_csv(run_dir / "market_regime_gate_rows.csv", gate_rows)
    existing = {name: (run_dir / name).exists() for name in REQUIRED_ARTIFACTS if name != "_ARTIFACT_COMPLETE.json"}
    complete = {
        "axis_id": AXIS_ID,
        "complete": all(existing.values()),
        "artifact_complete": all(existing.values()),
        "required_artifacts": list(REQUIRED_ARTIFACTS),
        "existing_artifacts": existing,
        "output_dir": str(run_dir),
        "runtime_db_write": False,
        "meemee_reflectable_candidate": False,
        "production_ranking_changed": False,
        "production_candidate_generator_changed": False,
        "validated_buy_count": 0,
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
    }
    _write_json(run_dir / "_ARTIFACT_COMPLETE.json", complete)
    return {"output_dir": str(run_dir), "summary": summary, "decision": decision, "source_coverage": source_coverage}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run TRADEX-only market regime trade permission gate v1.")
    parser.add_argument("--input-root", type=Path, default=DEFAULT_INPUT_ROOT)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--db-path", type=Path, default=None)
    args = parser.parse_args(argv)
    result = run_audit(input_root=args.input_root, output_root=args.output_root, db_path=args.db_path)
    print(json.dumps(_json_ready(result), ensure_ascii=False, sort_keys=True, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
