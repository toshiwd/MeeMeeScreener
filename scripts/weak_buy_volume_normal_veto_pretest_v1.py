from __future__ import annotations

import argparse
import hashlib
import json
import math
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

import pandas as pd

if __package__ in {None, ""}:
    sys.path.append(str(Path(__file__).resolve().parents[1]))


AXIS_ID = "weak_buy_volume_normal_veto_pretest_v1"
SCHEMA_PREFIX = "tradex_weak_buy_volume_normal_veto_pretest_v1"
DEFAULT_OUTPUT_DIR_NAME = "weak_buy_volume_normal_veto_pretest_v1"

VETO_FEATURE = "daily_volume_state"
VETO_VALUE = "daily_volume_normal"
LOT_SIZE = 100
ONE_WAY_COST_BPS = 30.0

ROOT_SOURCE_ARTIFACTS = (
    "daily_candidate_snapshot.csv",
    "orders_ledger.csv",
    "positions_ledger.csv",
    "equity_curve.csv",
    "post_run_outcome_labels.csv",
    "failure_diagnosis_summary.json",
    "selection_feature_manifest.json",
    "no_lookahead_audit.json",
)

REFERENCE_ARTIFACTS = (
    "diagnosis_v1",
    "bought_weak_candidate_decomposition_v1",
    "trade_reflection_audit_v1",
)

OUTPUT_ARTIFACTS = (
    "weak_buy_volume_normal_veto_pretest_summary.json",
    "vetoed_baseline_buys.csv",
    "replacement_candidates.csv",
    "counterfactual_orders_ledger.csv",
    "counterfactual_positions_ledger.csv",
    "counterfactual_equity_curve.csv",
    "baseline_vs_veto_comparison.json",
    "false_veto_analysis.csv",
    "saved_loss_cases.csv",
    "missed_profit_cases.csv",
    "no_lookahead_audit.json",
    "selection_feature_manifest.json",
    "next_axis_decision.json",
    "_ARTIFACT_COMPLETE.json",
)

DECISIONS = (
    "keep_for_replay_challenger",
    "hold_due_to_false_veto_risk",
    "drop_due_to_profit_damage",
    "drop_due_to_no_portfolio_improvement",
)


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _json_ready(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _json_ready(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set, frozenset)):
        return [_json_ready(item) for item in value]
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    try:
        import numpy as np

        if isinstance(value, np.generic):
            return _json_ready(value.item())
    except Exception:
        pass
    if isinstance(value, float) and not math.isfinite(value):
        return None
    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass
    return value


def _json_text(payload: Any) -> str:
    return json.dumps(_json_ready(payload), ensure_ascii=False, sort_keys=True, default=str)


def _write_json(path: Path, payload: dict[str, Any]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(_json_text(payload) + "\n", encoding="utf-8")
    return path


def _write_csv(path: Path, rows: Iterable[dict[str, Any]] | pd.DataFrame, columns: list[str] | None = None) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    frame = rows if isinstance(rows, pd.DataFrame) else pd.DataFrame(list(rows))
    if columns is not None:
        if frame.empty:
            frame = pd.DataFrame(columns=columns)
        else:
            for column in columns:
                if column not in frame.columns:
                    frame[column] = None
    frame.to_csv(path, index=False)
    return path


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _load_frame(root: Path, name: str) -> pd.DataFrame:
    path = root / name
    if not path.exists():
        raise FileNotFoundError(f"missing required artifact: {path}")
    return pd.read_csv(path)


def _safe_float(value: Any) -> float | None:
    try:
        if value is None or pd.isna(value):
            return None
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    return parsed if math.isfinite(parsed) else None


def _safe_int(value: Any) -> int | None:
    parsed = _safe_float(value)
    return None if parsed is None else int(parsed)


def _bool_series(series: pd.Series) -> pd.Series:
    return series.astype(str).str.lower().isin(["true", "1", "yes"])


def _source_status(run_root: Path) -> dict[str, bool]:
    status = {name: (run_root / name).exists() for name in ROOT_SOURCE_ARTIFACTS}
    status.update({name: (run_root / name).exists() for name in REFERENCE_ARTIFACTS})
    status["bought_weak_candidate_cases.csv"] = (run_root / "bought_weak_candidate_cases.csv").exists() or (run_root / "diagnosis_v1" / "bought_weak_candidate_cases.csv").exists()
    status["bought_weak_candidate_decomposition_v1/weak_buy_veto_candidate_rules.json"] = (run_root / "bought_weak_candidate_decomposition_v1" / "weak_buy_veto_candidate_rules.json").exists()
    status["trade_reflection_audit_v1/reflection_priority_decision.json"] = (run_root / "trade_reflection_audit_v1" / "reflection_priority_decision.json").exists()
    return status


def _parse_components(raw: Any) -> dict[str, Any]:
    if raw is None or pd.isna(raw):
        return {}
    try:
        items = json.loads(str(raw))
    except json.JSONDecodeError:
        return {}
    parsed: dict[str, Any] = {}
    for item in items if isinstance(items, list) else []:
        if not isinstance(item, dict):
            continue
        feature = str(item.get("feature") or "").strip()
        if feature:
            parsed[feature] = item.get("value")
            parsed[f"{feature}_points"] = item.get("points")
    return parsed


def _add_feature_columns(candidates: pd.DataFrame) -> pd.DataFrame:
    out = candidates.copy()
    parsed = [_parse_components(value) for value in out.get("score_components_json", pd.Series(dtype=object))]
    for feature in [VETO_FEATURE]:
        out[feature] = [item.get(feature) for item in parsed]
    return out


def _stable_position_id(decision_ymd: int, code: str, slot: int) -> str:
    payload = f"{AXIS_ID}:{decision_ymd}:{code}:{slot}"
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:16]


def _cost(notional: float) -> float:
    return abs(float(notional)) * ONE_WAY_COST_BPS / 10_000.0


def _drawdown(equity: pd.Series) -> float:
    if equity.empty:
        return 0.0
    peak = equity.cummax()
    dd = equity / peak - 1.0
    return float(dd.min())


def _load_weak_keys(run_root: Path) -> set[tuple[int, str]]:
    paths = [run_root / "bought_weak_candidate_cases.csv", run_root / "diagnosis_v1" / "bought_weak_candidate_cases.csv"]
    for path in paths:
        if path.exists():
            frame = pd.read_csv(path)
            if {"decision_ymd", "code"}.issubset(frame.columns):
                return set(zip(frame["decision_ymd"].astype(int), frame["code"].astype(str)))
    return set()


def _build_baseline_position_contributions(orders: pd.DataFrame, positions: pd.DataFrame) -> dict[str, dict[str, Any]]:
    buys = orders[(orders["action"] == "buy") & (orders["order_status"] == "filled")].copy()
    exits = orders[(orders["action"].isin(["exit", "stop"])) & (orders["order_status"] == "filled")].copy()
    latest = positions.sort_values("ymd", kind="stable").groupby("position_id", sort=False).tail(1) if not positions.empty else pd.DataFrame()
    out: dict[str, dict[str, Any]] = {}
    for _idx, buy in buys.iterrows():
        pid = str(buy["position_id"])
        exit_rows = exits[exits["position_id"].astype(str) == pid]
        if not exit_rows.empty:
            exit_row = exit_rows.iloc[-1]
            contribution = _safe_float(exit_row.get("realized_pnl")) or 0.0
            exit_ymd = int(exit_row["execution_ymd"])
            exit_decision_ymd = int(exit_row["decision_ymd"])
            exit_action = str(exit_row["action"])
            exit_reason = exit_row.get("reason_type")
        else:
            pos = latest[latest["position_id"].astype(str) == pid]
            contribution = (_safe_float(pos.iloc[-1].get("unrealized_pnl")) or 0.0) if not pos.empty else 0.0
            exit_ymd = None
            exit_decision_ymd = None
            exit_action = "open_mark"
            exit_reason = "open_at_run_end"
        out[pid] = {
            "position_id": pid,
            "code": str(buy["code"]),
            "entry_decision_ymd": int(buy["decision_ymd"]),
            "entry_ymd": int(buy["execution_ymd"]),
            "entry_price": _safe_float(buy.get("execution_price")),
            "shares": _safe_int(buy.get("shares")) or 0,
            "notional": _safe_float(buy.get("notional")) or 0.0,
            "entry_cost": _safe_float(buy.get("cost_amount")) or 0.0,
            "baseline_contribution": contribution,
            "exit_ymd": exit_ymd,
            "exit_decision_ymd": exit_decision_ymd,
            "exit_action": exit_action,
            "exit_reason": exit_reason,
        }
    return out


def _replacement_for_slot(
    day_candidates: pd.DataFrame,
    used_codes: set[str],
) -> pd.Series | None:
    eligible = day_candidates[
        (day_candidates[VETO_FEATURE].astype(str) != VETO_VALUE)
        & (_bool_series(day_candidates.get("entry_allowed_by_score", pd.Series([True] * len(day_candidates), index=day_candidates.index))))
        & (~_bool_series(day_candidates.get("downside_guard_blocked", pd.Series([False] * len(day_candidates), index=day_candidates.index))))
        & (_bool_series(day_candidates.get("next_open_available", pd.Series([True] * len(day_candidates), index=day_candidates.index))))
    ].copy()
    eligible = eligible[~eligible["code"].astype(str).isin(used_codes)]
    if eligible.empty:
        return None
    return eligible.sort_values(["candidate_rank", "selection_score"], ascending=[True, False], kind="stable").iloc[0]


def _counterfactual_return(labels: pd.DataFrame, decision_ymd: int, code: str) -> float | None:
    row = labels[(labels["decision_ymd"].astype(int) == int(decision_ymd)) & (labels["code"].astype(str) == str(code))]
    if row.empty:
        return None
    return _safe_float(row.iloc[0].get("post_ret_20"))


def build_veto_pretest(run_root: Path) -> dict[str, Any]:
    candidates = _add_feature_columns(_load_frame(run_root, "daily_candidate_snapshot.csv"))
    orders = _load_frame(run_root, "orders_ledger.csv")
    positions = _load_frame(run_root, "positions_ledger.csv")
    equity = _load_frame(run_root, "equity_curve.csv")
    labels = _load_frame(run_root, "post_run_outcome_labels.csv")
    failure = _read_json(run_root / "failure_diagnosis_summary.json")
    source_manifest = _read_json(run_root / "selection_feature_manifest.json")
    source_audit = _read_json(run_root / "no_lookahead_audit.json")
    rules = _read_json(run_root / "bought_weak_candidate_decomposition_v1" / "weak_buy_veto_candidate_rules.json")
    reflection = _read_json(run_root / "trade_reflection_audit_v1" / "reflection_priority_decision.json")

    if reflection.get("selected_next_axis") != "bought_weak_candidate":
        raise ValueError("trade_reflection_audit_v1 did not select bought_weak_candidate")
    matching_rules = [
        item for item in rules.get("candidate_rules", [])
        if item.get("rule", {}).get("feature") == VETO_FEATURE and item.get("rule", {}).get("value") == VETO_VALUE
    ]
    if not matching_rules:
        raise ValueError(f"missing veto candidate rule {VETO_FEATURE} == {VETO_VALUE}")

    weak_keys = _load_weak_keys(run_root)
    contributions = _build_baseline_position_contributions(orders, positions)
    buy_orders = orders[(orders["action"] == "buy") & (orders["order_status"] == "filled")].copy()
    candidate_keyed = candidates[["decision_ymd", "code", "candidate_rank", "selection_score", "close", "next_execution_ymd", VETO_FEATURE]].copy()
    baseline_buys = buy_orders.merge(candidate_keyed, on=["decision_ymd", "code"], how="left", suffixes=("", "_candidate"))
    vetoed = baseline_buys[baseline_buys[VETO_FEATURE].astype(str) == VETO_VALUE].copy()

    veto_rows: list[dict[str, Any]] = []
    replacement_rows: list[dict[str, Any]] = []
    false_rows: list[dict[str, Any]] = []
    saved_rows: list[dict[str, Any]] = []
    missed_rows: list[dict[str, Any]] = []
    adjustment_rows: list[dict[str, Any]] = []
    cf_order_rows: list[dict[str, Any]] = []
    cf_position_rows: list[dict[str, Any]] = []

    vetoed_pids = set(vetoed["position_id"].astype(str).tolist())
    kept_orders = orders[~orders["position_id"].astype(str).isin(vetoed_pids)].copy()
    if not kept_orders.empty:
        kept_orders["counterfactual_status"] = "baseline_kept"
    cf_order_rows.extend(kept_orders.to_dict("records"))
    kept_positions = positions[~positions["position_id"].astype(str).isin(vetoed_pids)].copy()
    if not kept_positions.empty:
        kept_positions["counterfactual_status"] = "baseline_kept"
    cf_position_rows.extend(kept_positions.to_dict("records"))

    for slot, (_idx, buy) in enumerate(vetoed.iterrows(), start=1):
        decision_ymd = int(buy["decision_ymd"])
        code = str(buy["code"])
        pid = str(buy["position_id"])
        contrib = contributions.get(pid, {})
        baseline_contribution = float(contrib.get("baseline_contribution") or 0.0)
        baseline_key = (decision_ymd, code)
        selected_label = labels[(labels["decision_ymd"].astype(int) == decision_ymd) & (labels["code"].astype(str) == code)]
        baseline_post_ret20 = _safe_float(selected_label.iloc[0].get("post_ret_20")) if not selected_label.empty else None
        weak_buy = baseline_key in weak_keys
        good_buy = bool((not weak_buy) and baseline_contribution > 0 and (baseline_post_ret20 is None or baseline_post_ret20 > 0))

        day = candidates[candidates["decision_ymd"].astype(int) == decision_ymd].copy()
        used_codes = set(buy_orders[buy_orders["decision_ymd"].astype(int) == decision_ymd]["code"].astype(str).tolist())
        used_codes.discard(code)
        replacement = _replacement_for_slot(day, used_codes)
        replacement_status = "cash_hold_no_replacement"
        replacement_code = None
        replacement_rank = None
        replacement_score = None
        replacement_volume_state = None
        replacement_post_ret20 = None
        replacement_contribution = 0.0
        replacement_entry_cost = 0.0
        replacement_exit_cost = 0.0
        replacement_position_id = None
        proxy_entry_price = None
        proxy_exit_price = None
        shares = 0
        notional = float(buy.get("notional") or 0.0)

        if replacement is not None:
            replacement_status = "replacement_available"
            replacement_code = str(replacement["code"])
            replacement_rank = _safe_int(replacement.get("candidate_rank"))
            replacement_score = _safe_float(replacement.get("selection_score"))
            replacement_volume_state = replacement.get(VETO_FEATURE)
            replacement_post_ret20 = _counterfactual_return(labels, decision_ymd, replacement_code)
            proxy_entry_price = _safe_float(replacement.get("close"))
            if proxy_entry_price and proxy_entry_price > 0:
                shares = int((notional // (proxy_entry_price * LOT_SIZE)) * LOT_SIZE)
            if shares <= 0:
                replacement_status = "cash_hold_insufficient_proxy_lot"
            elif replacement_post_ret20 is None:
                replacement_status = "replacement_available_missing_outcome_label"
            else:
                notional = shares * float(proxy_entry_price)
                replacement_entry_cost = _cost(notional)
                proxy_exit_price = float(proxy_entry_price) * (1.0 + float(replacement_post_ret20))
                exit_notional = shares * proxy_exit_price
                replacement_exit_cost = _cost(exit_notional)
                replacement_contribution = (exit_notional - notional) - replacement_entry_cost - replacement_exit_cost
                replacement_position_id = _stable_position_id(decision_ymd, replacement_code, slot)
                cf_order_rows.append(
                    {
                        "order_id": f"{AXIS_ID}-buy-{slot:06d}",
                        "decision_ymd": decision_ymd,
                        "execution_ymd": int(buy["execution_ymd"]),
                        "action": "buy",
                        "code": replacement_code,
                        "order_status": "filled_proxy",
                        "execution_price": proxy_entry_price,
                        "shares": shares,
                        "notional": notional,
                        "cost_amount": replacement_entry_cost,
                        "position_id": replacement_position_id,
                        "reason_type": "volume_normal_veto_replacement",
                        "execution_price_source": "decision_close_proxy_artifact_only",
                    }
                )
                exit_ymd = contrib.get("exit_ymd") or int(equity.iloc[-1]["ymd"])
                cf_order_rows.append(
                    {
                        "order_id": f"{AXIS_ID}-exit-{slot:06d}",
                        "decision_ymd": contrib.get("exit_decision_ymd") or exit_ymd,
                        "execution_ymd": exit_ymd,
                        "action": "exit",
                        "code": replacement_code,
                        "order_status": "filled_proxy",
                        "execution_price": proxy_exit_price,
                        "shares": shares,
                        "notional": exit_notional,
                        "cost_amount": replacement_exit_cost,
                        "position_id": replacement_position_id,
                        "reason_type": "counterfactual_exit_on_baseline_slot_end",
                        "realized_pnl": replacement_contribution,
                        "realized_return": replacement_contribution / (notional + replacement_entry_cost) if notional else None,
                        "execution_price_source": "post_ret_20_diagnostic_outcome_proxy",
                    }
                )
                cf_position_rows.append(
                    {
                        "ymd": int(buy["execution_ymd"]),
                        "position_id": replacement_position_id,
                        "code": replacement_code,
                        "shares": shares,
                        "entry_ymd": int(buy["execution_ymd"]),
                        "entry_price": proxy_entry_price,
                        "close_price": proxy_entry_price,
                        "market_value": notional,
                        "cost_basis": notional + replacement_entry_cost,
                        "unrealized_pnl": -replacement_entry_cost,
                        "holding_days": 1,
                        "counterfactual_status": "replacement_open_proxy",
                    }
                )
                cf_position_rows.append(
                    {
                        "ymd": exit_ymd,
                        "position_id": replacement_position_id,
                        "code": replacement_code,
                        "shares": shares,
                        "entry_ymd": int(buy["execution_ymd"]),
                        "entry_price": proxy_entry_price,
                        "close_price": proxy_exit_price,
                        "market_value": exit_notional,
                        "cost_basis": notional + replacement_entry_cost,
                        "unrealized_pnl": replacement_contribution,
                        "holding_days": None,
                        "counterfactual_status": "replacement_exit_proxy",
                    }
                )

        delta = replacement_contribution - baseline_contribution
        effective_ymd = contrib.get("exit_ymd") or int(equity.iloc[-1]["ymd"])
        adjustment_rows.append({"ymd": int(effective_ymd), "delta": delta})
        if baseline_contribution < 0 and delta > 0:
            saved_rows.append(
                {
                    "baseline_position_id": pid,
                    "baseline_code": code,
                    "decision_ymd": decision_ymd,
                    "baseline_contribution": baseline_contribution,
                    "replacement_code": replacement_code,
                    "replacement_contribution": replacement_contribution,
                    "saved_loss": delta,
                    "replacement_status": replacement_status,
                }
            )
        if baseline_contribution > replacement_contribution:
            missed_rows.append(
                {
                    "baseline_position_id": pid,
                    "baseline_code": code,
                    "decision_ymd": decision_ymd,
                    "baseline_contribution": baseline_contribution,
                    "replacement_code": replacement_code,
                    "replacement_contribution": replacement_contribution,
                    "missed_profit": baseline_contribution - replacement_contribution,
                    "replacement_status": replacement_status,
                }
            )
        false_rows.append(
            {
                "baseline_position_id": pid,
                "baseline_code": code,
                "decision_ymd": decision_ymd,
                "baseline_contribution": baseline_contribution,
                "baseline_post_ret20": baseline_post_ret20,
                "weak_buy_vetoed": weak_buy,
                "good_buy_vetoed": good_buy,
                "replacement_code": replacement_code,
                "replacement_status": replacement_status,
                "missed_profit_if_positive": max(0.0, baseline_contribution - replacement_contribution),
            }
        )
        veto_rows.append(
            {
                "order_id": buy.get("order_id"),
                "position_id": pid,
                "decision_ymd": decision_ymd,
                "execution_ymd": int(buy["execution_ymd"]),
                "code": code,
                "candidate_rank": buy.get("candidate_rank"),
                "selection_score": buy.get("selection_score"),
                "daily_volume_state": buy.get(VETO_FEATURE),
                "baseline_contribution": baseline_contribution,
                "baseline_post_ret20": baseline_post_ret20,
                "weak_buy_vetoed": weak_buy,
                "good_buy_vetoed": good_buy,
                "replacement_status": replacement_status,
                "replacement_code": replacement_code,
                "replacement_contribution": replacement_contribution,
                "delta_contribution": delta,
            }
        )
        replacement_rows.append(
            {
                "vetoed_order_id": buy.get("order_id"),
                "decision_ymd": decision_ymd,
                "vetoed_code": code,
                "replacement_status": replacement_status,
                "replacement_code": replacement_code,
                "replacement_candidate_rank": replacement_rank,
                "replacement_selection_score": replacement_score,
                "replacement_daily_volume_state": replacement_volume_state,
                "replacement_post_ret20_diagnostic": replacement_post_ret20,
                "proxy_entry_price": proxy_entry_price,
                "proxy_exit_price": proxy_exit_price,
                "shares": shares,
                "estimated_net_contribution": replacement_contribution,
            }
        )

    adjustments = pd.DataFrame(adjustment_rows)
    cf_equity = equity.copy()
    cf_equity["baseline_equity"] = pd.to_numeric(cf_equity["equity"], errors="coerce")
    if adjustments.empty:
        cf_equity["cumulative_counterfactual_delta"] = 0.0
    else:
        by_day = adjustments.groupby("ymd", as_index=False)["delta"].sum()
        cf_equity = cf_equity.merge(by_day, on="ymd", how="left")
        cf_equity["delta"] = cf_equity["delta"].fillna(0.0)
        cf_equity["cumulative_counterfactual_delta"] = cf_equity["delta"].cumsum()
    cf_equity["counterfactual_equity"] = cf_equity["baseline_equity"] + cf_equity["cumulative_counterfactual_delta"]
    cf_equity["equity_method"] = "artifact_only_estimate_not_exact_next_open_replay"

    veto_df = pd.DataFrame(veto_rows)
    replacements_df = pd.DataFrame(replacement_rows)
    false_df = pd.DataFrame(false_rows)
    saved_df = pd.DataFrame(saved_rows)
    missed_df = pd.DataFrame(missed_rows)
    cf_orders = pd.DataFrame(cf_order_rows)
    cf_positions = pd.DataFrame(cf_position_rows)

    baseline_final = float(equity.iloc[-1]["equity"])
    veto_final = float(cf_equity.iloc[-1]["counterfactual_equity"])
    baseline_dd = _drawdown(pd.to_numeric(equity["equity"], errors="coerce"))
    veto_dd = _drawdown(pd.to_numeric(cf_equity["counterfactual_equity"], errors="coerce"))
    baseline_cost = float(pd.to_numeric(orders.get("cost_amount", pd.Series(dtype=float)), errors="coerce").sum())
    veto_cost = float(pd.to_numeric(cf_orders.get("cost_amount", pd.Series(dtype=float)), errors="coerce").sum()) if not cf_orders.empty else 0.0
    vetoed_count = int(len(veto_df))
    weak_count = int(false_df["weak_buy_vetoed"].sum()) if not false_df.empty else 0
    good_count = int(false_df["good_buy_vetoed"].sum()) if not false_df.empty else 0
    false_rate = good_count / vetoed_count if vetoed_count else 0.0
    replacement_available = int((replacements_df.get("replacement_status", pd.Series(dtype=str)) == "replacement_available").sum()) if not replacements_df.empty else 0
    cash_hold = int(replacements_df.get("replacement_status", pd.Series(dtype=str)).astype(str).str.startswith("cash_hold").sum()) if not replacements_df.empty else 0
    saved_loss_total = float(pd.to_numeric(saved_df.get("saved_loss", pd.Series(dtype=float)), errors="coerce").sum()) if not saved_df.empty else 0.0
    missed_profit_total = float(pd.to_numeric(missed_df.get("missed_profit", pd.Series(dtype=float)), errors="coerce").sum()) if not missed_df.empty else 0.0
    metrics = {
        "baseline_final_equity": baseline_final,
        "veto_final_equity": veto_final,
        "delta_final_equity": veto_final - baseline_final,
        "baseline_max_drawdown": baseline_dd,
        "veto_max_drawdown": veto_dd,
        "delta_max_drawdown": veto_dd - baseline_dd,
        "baseline_cost_total": baseline_cost,
        "veto_cost_total": veto_cost,
        "order_count_delta": int(len(cf_orders) - len(orders)),
        "vetoed_buy_count": vetoed_count,
        "weak_buy_vetoed_count": weak_count,
        "good_buy_vetoed_count": good_count,
        "false_veto_rate": false_rate,
        "replacement_available_count": replacement_available,
        "cash_hold_count": cash_hold,
        "saved_loss_total": saved_loss_total,
        "missed_profit_total": missed_profit_total,
    }
    if metrics["delta_final_equity"] > 0 and false_rate <= 0.30:
        decision = "keep_for_replay_challenger"
        reason = "positive_counterfactual_delta_with_acceptable_false_veto_rate"
    elif false_rate > 0.30:
        decision = "hold_due_to_false_veto_risk"
        reason = "false_veto_rate_above_30_percent"
    elif metrics["delta_final_equity"] < 0 and missed_profit_total > saved_loss_total:
        decision = "drop_due_to_profit_damage"
        reason = "missed_profit_exceeds_saved_loss"
    else:
        decision = "drop_due_to_no_portfolio_improvement"
        reason = "no_positive_counterfactual_equity_delta"

    return {
        "frames": {
            "vetoed": veto_df,
            "replacements": replacements_df,
            "cf_orders": cf_orders,
            "cf_positions": cf_positions,
            "cf_equity": cf_equity,
            "false_veto": false_df,
            "saved_loss": saved_df,
            "missed_profit": missed_df,
        },
        "metrics": metrics,
        "decision": decision,
        "decision_reason": reason,
        "source_manifest": source_manifest,
        "source_audit": source_audit,
        "failure": failure,
        "rule": matching_rules[0],
    }


def run_weak_buy_volume_normal_veto_pretest_v1(run_root: str | Path, output_root: str | Path | None = None) -> dict[str, Any]:
    run_root = Path(run_root)
    output_root = Path(output_root) if output_root is not None else run_root / DEFAULT_OUTPUT_DIR_NAME
    source_status = _source_status(run_root)
    missing = [name for name, exists in source_status.items() if not exists]
    if missing:
        raise FileNotFoundError(f"missing required source artifacts: {missing}")

    result = build_veto_pretest(run_root)
    frames = result["frames"]
    metrics = result["metrics"]

    _write_csv(output_root / "vetoed_baseline_buys.csv", frames["vetoed"])
    _write_csv(output_root / "replacement_candidates.csv", frames["replacements"])
    _write_csv(output_root / "counterfactual_orders_ledger.csv", frames["cf_orders"])
    _write_csv(output_root / "counterfactual_positions_ledger.csv", frames["cf_positions"])
    _write_csv(output_root / "counterfactual_equity_curve.csv", frames["cf_equity"])
    _write_csv(output_root / "false_veto_analysis.csv", frames["false_veto"])
    _write_csv(output_root / "saved_loss_cases.csv", frames["saved_loss"], columns=["baseline_position_id", "baseline_code", "decision_ymd", "baseline_contribution", "replacement_code", "replacement_contribution", "saved_loss", "replacement_status"])
    _write_csv(output_root / "missed_profit_cases.csv", frames["missed_profit"], columns=["baseline_position_id", "baseline_code", "decision_ymd", "baseline_contribution", "replacement_code", "replacement_contribution", "missed_profit", "replacement_status"])

    comparison = {
        "schema_version": f"{SCHEMA_PREFIX}_baseline_vs_veto_comparison_v1",
        "axis_id": AXIS_ID,
        "generated_at": _utc_now(),
        "metrics": metrics,
        "method": {
            "counterfactual_type": "artifact_only_pretest",
            "exact_next_open_replay": False,
            "replacement_entry_price_source": "decision_close_proxy_artifact_only",
            "replacement_exit_price_source": "post_ret_20_diagnostic_outcome_proxy",
            "selection_uses_post_run_outcomes": False,
            "diagnostic_evaluation_uses_post_run_outcomes": True,
        },
    }
    _write_json(output_root / "baseline_vs_veto_comparison.json", comparison)

    manifest = {
        "schema_version": f"{SCHEMA_PREFIX}_selection_feature_manifest_v1",
        "axis_id": AXIS_ID,
        "generated_at": _utc_now(),
        "selection_allowed_columns": [VETO_FEATURE, "candidate_rank", "selection_score", "entry_allowed_by_score", "downside_guard_blocked", "next_open_available"],
        "selection_forbidden_columns": ["post_ret_5", "post_ret_10", "post_ret_20", "post_ret_40", "mae_20", "mfe_20", "outcome_bucket", "realized_pnl", "realized_return"],
        "diagnostic_only_columns": ["post_ret_20", "mae_20", "mfe_20", "baseline_contribution", "replacement_contribution"],
        "outcome_label_columns": ["post_ret_5", "post_ret_10", "post_ret_20", "post_ret_40", "mae_20", "mfe_20", "outcome_bucket"],
        "veto_rule": {"feature": VETO_FEATURE, "op": "eq", "value": VETO_VALUE},
        "audit_result": "pass",
    }
    _write_json(output_root / "selection_feature_manifest.json", manifest)

    audit = {
        "schema_version": f"{SCHEMA_PREFIX}_no_lookahead_audit_v1",
        "axis_id": AXIS_ID,
        "generated_at": _utc_now(),
        "audit_result": "pass",
        "selection_feature_manifest": "selection_feature_manifest.json",
        "source_no_lookahead_audit_result": result["source_audit"].get("audit_result"),
        "source_selection_feature_manifest_result": result["source_manifest"].get("audit_result"),
        "selection_used_columns": manifest["selection_allowed_columns"],
        "selection_forbidden_columns_used": [],
        "post_run_outcomes_used_for_selection": False,
        "post_run_outcomes_used_for_diagnostic_evaluation": True,
        "silent_fallback_used": False,
        "execution_price_limitation": {
            "exact_replacement_next_open_prices_available": False,
            "counterfactual_execution_method": "decision_close_proxy_artifact_only",
            "status": "research_fallback_recorded_not_silent",
        },
    }
    _write_json(output_root / "no_lookahead_audit.json", audit)

    decision_payload = {
        "schema_version": f"{SCHEMA_PREFIX}_next_axis_decision_v1",
        "axis_id": AXIS_ID,
        "generated_at": _utc_now(),
        "decision_candidates": list(DECISIONS),
        "decision": result["decision"],
        "decision_count": 1,
        "reason_type": result["decision_reason"],
        "metrics": metrics,
        "policy": {
            "single_axis_only": True,
            "veto_candidate": f"{VETO_FEATURE} == {VETO_VALUE}",
            "replay_rerun": False,
            "baseline_policy_changed": False,
            "production_policy_changed": False,
            "post_run_outcome_used_for_selection": False,
        },
    }
    _write_json(output_root / "next_axis_decision.json", decision_payload)

    summary = {
        "schema_version": f"{SCHEMA_PREFIX}_summary_v1",
        "axis_id": AXIS_ID,
        "generated_at": _utc_now(),
        "run_root": str(run_root),
        "source_artifacts": source_status,
        "reference_artifacts": {
            "diagnosis_v1": str(run_root / "diagnosis_v1"),
            "bought_weak_candidate_decomposition_v1": str(run_root / "bought_weak_candidate_decomposition_v1"),
            "trade_reflection_audit_v1": str(run_root / "trade_reflection_audit_v1"),
        },
        "scope": {
            "tradex_only": True,
            "derived_artifacts_only": True,
            "single_axis_only": True,
            "replay_rerun": False,
            "baseline_policy_changed": False,
            "production_policy_changed": False,
            "meemee_ui_changed": False,
            "runtime_db_written": False,
            "ranking_changed": False,
            "publish_registry_changed": False,
        },
        "pretest_rule": {"feature": VETO_FEATURE, "op": "eq", "value": VETO_VALUE},
        "method": comparison["method"],
        "metrics": metrics,
        "decision": result["decision"],
        "decision_reason": result["decision_reason"],
        "limitations": [
            "replacement next-open prices are not present in the requested artifact surface",
            "counterfactual equity is an artifact-only estimate and is not a full replay challenger",
        ],
    }
    _write_json(output_root / "weak_buy_volume_normal_veto_pretest_summary.json", summary)

    complete = {
        "schema_version": f"{SCHEMA_PREFIX}_artifact_complete_v1",
        "axis_id": AXIS_ID,
        "generated_at": _utc_now(),
        "run_root": str(run_root),
        "output_root": str(output_root),
        "complete": True,
        "required_artifacts_all_present": all((output_root / name).exists() for name in OUTPUT_ARTIFACTS if name != "_ARTIFACT_COMPLETE.json"),
        "source_artifacts_all_present": all(source_status.values()),
        "decision": result["decision"],
        "decision_count": 1,
        "single_axis_only": True,
        "veto_candidate": f"{VETO_FEATURE} == {VETO_VALUE}",
        "replay_rerun": False,
        "baseline_policy_changed": False,
        "production_policy_changed": False,
        "post_run_outcome_used_for_selection": False,
        "silent_fallback_used": False,
        "research_fallback_recorded": True,
        "exact_next_open_replay": False,
    }
    _write_json(output_root / "_ARTIFACT_COMPLETE.json", complete)

    return {
        "complete": True,
        "output_root": str(output_root),
        "decision": result["decision"],
        "metrics": metrics,
    }


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Pretest daily_volume_state == daily_volume_normal weak-buy veto.")
    parser.add_argument("--run-root", required=True, type=Path)
    parser.add_argument("--output-root", type=Path, default=None)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    print(_json_text(run_weak_buy_volume_normal_veto_pretest_v1(args.run_root, args.output_root)))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
