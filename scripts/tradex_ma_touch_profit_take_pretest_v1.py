from __future__ import annotations

import argparse
import json
import math
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd


AXIS_ID = "ma_touch_profit_take_pretest_v1"
DEFAULT_FEATURE_ROOT = Path("G:/Tradex/ma_phase_feature_base_v1/20260603T121215Z-ma-phase-feature-base-v1")
DEFAULT_INPUT_PARQUET = DEFAULT_FEATURE_ROOT / "ma_phase_features.parquet"
DEFAULT_INPUT_AUDIT = DEFAULT_FEATURE_ROOT / "input_audit.json"
DEFAULT_OBSERVABLE_ROOT = Path("G:/Tradex/ma_touch_observable_signal_v1/20260603T152852Z-ma-touch-observable-signal-v1")
DEFAULT_OBSERVABLE_EVENTS = DEFAULT_OBSERVABLE_ROOT / "ma_touch_observable_signal_events.csv"
DEFAULT_OUT_ROOT = Path("G:/Tradex/ma_touch_profit_take_pretest_v1")
TARGET_MAS = ("MA60", "MA100", "MA200")
SIGNALS = (
    "all_touch_events",
    "high_touch_only_weak",
    "gap_touch_fade",
    "close_break_above_weak",
    "close_break_above_strong",
    "touch_with_lower_support",
    "touch_without_lower_support",
)
EXIT_ACTIONS = {
    "exit_on_touch_close",
    "exit_next_day_if_weak_confirmed",
    "weak_touch_exit_strong_break_hold",
    "pullback_wait_variant",
}
REQUIRED_ARTIFACTS = (
    "input_audit.json",
    "profit_take_definition.json",
    "ma_touch_profit_take_events.csv",
    "action_summary.csv",
    "signal_action_contrast.csv",
    "yearly_stability_summary.csv",
    "candidate_examples.csv",
    "research_decision.json",
    "_ARTIFACT_COMPLETE.json",
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


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(_json_ready(payload), ensure_ascii=False, sort_keys=True, indent=2) + "\n", encoding="utf-8")


def _load_next_day_features(path: Path) -> pd.DataFrame:
    cols = ["code", "ymd", "c", "ma60", "ma100", "ma200", "close_above_ma20"]
    base = pd.read_parquet(path, columns=cols).sort_values(["code", "ymd"], kind="stable")
    base["code"] = base["code"].astype(str)
    for col in ["ymd", "c", "ma60", "ma100", "ma200", "close_above_ma20"]:
        base[f"next_{col}"] = base.groupby("code", sort=False)[col].shift(-1)
    return base[["code", "ymd", "next_ymd", "next_c", "next_ma60", "next_ma100", "next_ma200", "next_close_above_ma20"]]


def _load_events(events_path: Path, feature_path: Path) -> pd.DataFrame:
    frame = pd.read_csv(events_path)
    frame["code"] = frame["code"].astype(str)
    next_features = _load_next_day_features(feature_path)
    frame = frame.merge(next_features, on=["code", "ymd"], how="left", validate="many_to_one")
    frame["event_year"] = frame["ymd"].astype(str).str.slice(0, 4).astype(int)
    for col in [c for c in frame.columns if c.startswith("signal_")]:
        frame[col] = frame[col].fillna(False).astype(bool)
    for col in ["target_ma_rebreak_20b", "rebreak_ma20_20b", "pullback_occurred_5b", "recovered_after_pullback_20b", "severe_loss_flag_20b", "higher_high_made_20b", "lower_low_made_20b"]:
        frame[col] = frame[col].fillna(False).astype(bool)
    next_target = []
    for row in frame.itertuples(index=False):
        next_target.append(getattr(row, f"next_{str(row.target_ma).lower()}", float("nan")))
    frame["next_target_ma_value"] = pd.to_numeric(pd.Series(next_target, index=frame.index), errors="coerce")
    frame["next_day_weak_confirmed"] = (
        frame["next_c"].lt(frame["next_target_ma_value"])
        | frame["next_c"].lt(frame["c"])
        | ~frame["next_close_above_ma20"].fillna(True).astype(bool)
    )
    frame["pullback_wait_exit_trigger"] = frame["pullback_occurred_5b"] & frame["rebreak_ma20_20b"]
    frame["target_ma_slope_context"] = frame["target_ma_slope_context"].fillna("unknown")
    frame["ma20_phase_context"] = frame["ma20_phase_context"].fillna("other")
    return frame


def _signal_mask(frame: pd.DataFrame, signal: str) -> pd.Series:
    if signal == "all_touch_events":
        return pd.Series(True, index=frame.index)
    return frame[f"signal_{signal}"].fillna(False)


def _action_exit_mask(frame: pd.DataFrame, action: str) -> pd.Series:
    if action == "hold_after_touch":
        return pd.Series(False, index=frame.index)
    if action == "exit_on_touch_close":
        return pd.Series(True, index=frame.index)
    if action == "exit_next_day_if_weak_confirmed":
        return frame["next_day_weak_confirmed"].fillna(False)
    if action == "hold_if_strong_break":
        return ~frame["signal_close_break_above_strong"].fillna(False)
    if action == "weak_touch_exit_strong_break_hold":
        return frame["signal_high_touch_only_weak"] | frame["signal_gap_touch_fade"] | frame["signal_close_break_above_weak"]
    if action == "pullback_wait_variant":
        return frame["pullback_wait_exit_trigger"].fillna(False)
    raise ValueError(action)


def _mean(series: pd.Series) -> float | None:
    valid = pd.to_numeric(series, errors="coerce").dropna()
    return None if valid.empty else float(valid.mean())


def _median(series: pd.Series) -> float | None:
    valid = pd.to_numeric(series, errors="coerce").dropna()
    return None if valid.empty else float(valid.median())


def _rate(series: pd.Series) -> float | None:
    valid = series.dropna()
    return None if valid.empty else float(valid.astype(bool).mean())


def _row(group: pd.DataFrame, *, target_ma: str, signal: str, action: str, context_name: str | None = None, context_value: str | None = None) -> dict[str, Any]:
    valid = group[group["ret_20b"].notna()].copy()
    exit_mask = _action_exit_mask(valid, action)
    ret20 = pd.to_numeric(valid["ret_20b"], errors="coerce")
    avoided = (-ret20.clip(upper=0)).where(exit_mask, 0.0)
    opportunity = ret20.clip(lower=0).where(exit_mask, 0.0)
    net = (-ret20).where(exit_mask, 0.0)
    severe = valid["severe_loss_flag_20b"].fillna(False)
    big_winner = ret20.ge(10.0)
    row = {
        "target_ma": target_ma,
        "observable_signal": signal,
        "action": action,
        "event_count": int(len(valid)),
        "unique_symbol_count": int(valid["code"].nunique()) if not valid.empty else 0,
        "exit_rate": _rate(exit_mask) if not valid.empty else None,
        "mean_forward_ret5": _mean(valid["ret_5b"]),
        "mean_forward_ret10": _mean(valid["ret_10b"]),
        "mean_forward_ret20": _mean(valid["ret_20b"]),
        "median_forward_ret20": _median(valid["ret_20b"]),
        "hit_rate20": _rate(ret20 > 0) if not valid.empty else None,
        "severe_loss_rate20": _rate(severe) if not valid.empty else None,
        "mean_max_drawdown20": _mean(valid["max_drawdown_20b"]),
        "median_max_drawdown20": _median(valid["max_drawdown_20b"]),
        "ma20_rebreak_rate20": _rate(valid["rebreak_ma20_20b"]) if not valid.empty else None,
        "target_ma_rebreak_rate20": _rate(valid["target_ma_rebreak_20b"]) if not valid.empty else None,
        "avoided_loss_mean": _mean(avoided),
        "opportunity_cost_mean": _mean(opportunity),
        "net_exit_advantage_vs_hold": _mean(net),
        "tail_loss_reduction": _rate(severe & exit_mask) if not valid.empty else None,
        "missed_big_winner_rate": _rate(big_winner & exit_mask) if not valid.empty else None,
    }
    if context_name is not None:
        row["context_name"] = context_name
        row["context_value"] = context_value
    return row


def _action_summary(frame: pd.DataFrame) -> pd.DataFrame:
    actions = ["hold_after_touch", "exit_on_touch_close", "exit_next_day_if_weak_confirmed", "hold_if_strong_break", "weak_touch_exit_strong_break_hold", "pullback_wait_variant"]
    rows: list[dict[str, Any]] = []
    for target_ma, target_group in frame.groupby("target_ma", sort=False):
        for signal in SIGNALS:
            group = target_group[_signal_mask(target_group, signal)]
            for action in actions:
                rows.append(_row(group, target_ma=target_ma, signal=signal, action=action))
    return pd.DataFrame(rows)


def _context_summary(frame: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for context in ["target_ma_slope_context", "ma20_phase_context", "lower_support_bucket"]:
        for (target_ma, value), group in frame.groupby(["target_ma", context], dropna=False, sort=False):
            for action in ["hold_after_touch", "exit_on_touch_close", "weak_touch_exit_strong_break_hold"]:
                rows.append(_row(group, target_ma=target_ma, signal=f"context_{context}", action=action, context_name=context, context_value=str(value)))
    return pd.DataFrame(rows)


def _summary_lookup(summary: pd.DataFrame, target_ma: str, signal: str, action: str) -> dict[str, Any] | None:
    row = summary[(summary["target_ma"] == target_ma) & (summary["observable_signal"] == signal) & (summary["action"] == action)]
    return None if row.empty else row.iloc[0].to_dict()


def _compare(summary: pd.DataFrame, target_ma: str, signal: str, first_action: str, second_action: str, name: str) -> dict[str, Any]:
    a = _summary_lookup(summary, target_ma, signal, first_action)
    b = _summary_lookup(summary, target_ma, signal, second_action)
    payload = {"name": f"{target_ma}_{name}", "target_ma": target_ma, "signal": signal, "first_action": first_action, "second_action": second_action}
    if not a or not b:
        payload["status"] = "missing_group"
        return payload
    deltas = {}
    for metric in ["net_exit_advantage_vs_hold", "tail_loss_reduction", "missed_big_winner_rate", "opportunity_cost_mean", "avoided_loss_mean", "severe_loss_rate20", "mean_max_drawdown20", "ma20_rebreak_rate20", "target_ma_rebreak_rate20"]:
        if a.get(metric) is not None and b.get(metric) is not None:
            deltas[f"{metric}_delta"] = a[metric] - b[metric]
    payload.update({"status": "ready" if a["event_count"] >= 200 and b["event_count"] >= 200 else "insufficient_sample", "first_metrics": a, "second_metrics": b, "deltas": deltas})
    return payload


def _contrasts(summary: pd.DataFrame, context: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for target_ma in TARGET_MAS:
        rows.extend(
            [
                _compare(summary, target_ma, "high_touch_only_weak", "exit_on_touch_close", "hold_after_touch", "exit_vs_hold_high_touch_only_weak"),
                _compare(summary, target_ma, "gap_touch_fade", "exit_on_touch_close", "hold_after_touch", "exit_vs_hold_gap_touch_fade"),
                _compare(summary, target_ma, "all_touch_events", "hold_if_strong_break", "hold_after_touch", "hold_if_strong_break_vs_hold"),
                _compare(summary, target_ma, "all_touch_events", "weak_touch_exit_strong_break_hold", "hold_after_touch", "weak_exit_strong_hold_vs_hold"),
                _compare(summary, target_ma, "all_touch_events", "pullback_wait_variant", "exit_on_touch_close", "pullback_wait_vs_exit_on_touch"),
            ]
        )
    for target_ma in TARGET_MAS:
        for context_name, first, second in [
            ("lower_support_bucket", "none_near", "light_support"),
            ("target_ma_slope_context", "up", "down"),
            ("ma20_phase_context", "1-10", "15-20"),
            ("ma20_phase_context", "15-20", "21-30"),
        ]:
            first_rows = context[(context["target_ma"] == target_ma) & (context["context_name"] == context_name) & (context["context_value"] == first) & (context["action"] == "exit_on_touch_close")]
            second_rows = context[(context["target_ma"] == target_ma) & (context["context_name"] == context_name) & (context["context_value"] == second) & (context["action"] == "exit_on_touch_close")]
            if not first_rows.empty and not second_rows.empty:
                a = first_rows.iloc[0].to_dict()
                b = second_rows.iloc[0].to_dict()
                rows.append({"name": f"{target_ma}_{context_name}_{first}_vs_{second}", "target_ma": target_ma, "signal": f"context_{context_name}", "first_action": "exit_on_touch_close", "second_action": "exit_on_touch_close", "status": "ready" if a["event_count"] >= 200 and b["event_count"] >= 200 else "insufficient_sample", "first_metrics": a, "second_metrics": b, "deltas": {"net_exit_advantage_vs_hold_delta": a["net_exit_advantage_vs_hold"] - b["net_exit_advantage_vs_hold"], "tail_loss_reduction_delta": a["tail_loss_reduction"] - b["tail_loss_reduction"], "missed_big_winner_rate_delta": a["missed_big_winner_rate"] - b["missed_big_winner_rate"]}})
    return pd.DataFrame(rows)


def _yearly(frame: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for (year, target_ma), group in frame.groupby(["event_year", "target_ma"], sort=False):
        for signal in ["high_touch_only_weak", "gap_touch_fade", "close_break_above_weak", "all_touch_events"]:
            subset = group[_signal_mask(group, signal)]
            for action in ["hold_after_touch", "exit_on_touch_close", "weak_touch_exit_strong_break_hold"]:
                row = _row(subset, target_ma=target_ma, signal=signal, action=action)
                row["event_year"] = int(year)
                row["sample_status"] = "sufficient" if row["event_count"] >= 100 else "insufficient_sample"
                rows.append(row)
    return pd.DataFrame(rows).sort_values(["target_ma", "observable_signal", "action", "event_year"], kind="stable")


def _decision(contrast: pd.DataFrame, yearly: pd.DataFrame) -> dict[str, Any]:
    ready = contrast[contrast["status"].eq("ready")]
    keep_rule: list[dict[str, Any]] = []
    context: list[dict[str, Any]] = []
    entry_guard: list[dict[str, Any]] = []
    for row in ready.to_dict("records"):
        name = row["name"]
        d = row["deltas"]
        if "exit_vs_hold_high_touch_only_weak" in name or "exit_vs_hold_gap_touch_fade" in name:
            net = d.get("net_exit_advantage_vs_hold_delta", 0)
            tail = d.get("tail_loss_reduction_delta", 0)
            missed = d.get("missed_big_winner_rate_delta", 1)
            if (net > 0 or tail >= 0.08) and missed <= 0.12:
                keep_rule.append({"typed_reason": "weak_touch_exit_has_positive_or_tail_risk_advantage", "contrast": name, "deltas": d})
            elif tail > 0.03:
                context.append({"typed_reason": "weak_touch_exit_reduces_tail_risk_but_opportunity_cost_is_material", "contrast": name, "deltas": d})
            else:
                entry_guard.append({"typed_reason": "weak_touch_is_more_entry_guard_than_profit_take", "contrast": name, "deltas": d})
    stable_years = _stable_years(yearly)
    if keep_rule and stable_years >= 4:
        decision = "keep_for_profit_take_rule_pretest_next"
        reason = "weak_touch_exit_has_profit_take_pretest_edge_with_yearly_stability"
    elif context:
        decision = "keep_as_profit_take_context"
        reason = "weak_touch_exit_reduces_tail_or_rebreak_risk_but_opportunity_cost_remains"
    elif entry_guard:
        decision = "keep_as_entry_guard_only"
        reason = "weak_touch_exit_is_not_enough_for_profit_taking_but_supports_entry_guard"
    else:
        decision = "drop"
        reason = "touch_exit_does_not_improve_loss_avoidance_enough"
    return {
        "axis_id": AXIS_ID,
        "candidate_local_decision": decision,
        "session_aggregate_decision": decision,
        "authoritative_rollup_decision": decision,
        "reason": reason,
        "profit_take_rule_reasons": keep_rule,
        "profit_take_context_reasons": context,
        "entry_guard_only_reasons": entry_guard,
        "stable_years": stable_years,
        "future_defined_reaction_type_used_as_input_signal": False,
        "non_scope": [
            "no MeeMee reflection",
            "no runtime DB write",
            "no ranking change",
            "no publish",
            "no candidate generation change",
            "no live sell rule",
            "no bad-pick removal",
            "no entry guard implementation",
            "no score tuning",
            "no threshold optimization",
        ],
    }


def _stable_years(yearly: pd.DataFrame) -> int:
    subset = yearly[
        yearly["observable_signal"].isin(["high_touch_only_weak", "gap_touch_fade"])
        & yearly["action"].eq("exit_on_touch_close")
        & yearly["sample_status"].eq("sufficient")
    ]
    return int(subset[subset["net_exit_advantage_vs_hold"].gt(0) | subset["tail_loss_reduction"].ge(0.08)]["event_year"].nunique())


def _definition() -> dict[str, Any]:
    return {
        "axis_id": AXIS_ID,
        "target_events": "upper MA touch events from ma_touch_observable_signal_v1",
        "actions": {
            "hold_after_touch": "continue holding after touch day close; raw forward metrics are hold outcomes",
            "exit_on_touch_close": "exit at touch day close; positive forward return is opportunity cost and negative forward return is avoided loss",
            "exit_next_day_if_weak_confirmed": "exit only when next day close is below target MA, below touch close, or below MA20",
            "hold_if_strong_break": "hold only on close_break_above_strong, otherwise exit",
            "weak_touch_exit_strong_break_hold": "exit on high_touch_only_weak/gap_touch_fade/close_break_above_weak and hold on close_break_above_strong",
            "pullback_wait_variant": "diagnostic: wait up to 5 bars; exit when pullback occurs and MA20 rebreaks",
        },
        "no_future_defined_reaction_type_used_as_input_signal": True,
        "authoritative_input": str(DEFAULT_INPUT_PARQUET),
        "observable_signal_events": str(DEFAULT_OBSERVABLE_EVENTS),
    }


def run(args: argparse.Namespace) -> Path:
    out_dir = args.out_root / f"{_now_tag()}-{AXIS_ID.replace('_', '-')}"
    out_dir.mkdir(parents=True, exist_ok=False)
    source_audit = json.loads(args.input_audit.read_text(encoding="utf-8"))
    frame = _load_events(args.observable_events, args.input_parquet)
    summary = _action_summary(frame)
    context = _context_summary(frame)
    contrast = _contrasts(summary, context)
    yearly = _yearly(frame)
    decision = _decision(contrast, yearly)
    examples = frame[frame["signal_high_touch_only_weak"] | frame["signal_gap_touch_fade"] | frame["signal_close_break_above_weak"]].head(5000)
    input_audit = {
        "axis_id": AXIS_ID,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "input_parquet": str(args.input_parquet),
        "input_audit": str(args.input_audit),
        "observable_events": str(args.observable_events),
        "source_axis_id": source_audit.get("axis_id"),
        "confirmed_bars_only_inherited": bool(source_audit.get("confirmed_bars_only")),
        "runtime_db_write": False,
        "meemee_reflection": False,
        "ranking_change": False,
        "publish": False,
        "events_loaded": int(len(frame)),
        "unique_symbol_count": int(frame["code"].nunique()),
        "min_ymd": int(frame["ymd"].min()),
        "max_ymd": int(frame["ymd"].max()),
        "target_ma_counts": frame["target_ma"].value_counts().to_dict(),
        "future_defined_reaction_type_used_as_input_signal": False,
    }
    _write_json(out_dir / "input_audit.json", input_audit)
    _write_json(out_dir / "profit_take_definition.json", _definition())
    frame.to_csv(out_dir / "ma_touch_profit_take_events.csv", index=False, encoding="utf-8")
    summary.to_csv(out_dir / "action_summary.csv", index=False, encoding="utf-8")
    contrast.to_csv(out_dir / "signal_action_contrast.csv", index=False, encoding="utf-8")
    yearly.to_csv(out_dir / "yearly_stability_summary.csv", index=False, encoding="utf-8")
    examples.to_csv(out_dir / "candidate_examples.csv", index=False, encoding="utf-8")
    _write_json(out_dir / "research_decision.json", decision)
    missing = [name for name in REQUIRED_ARTIFACTS if name != "_ARTIFACT_COMPLETE.json" and not (out_dir / name).exists()]
    _write_json(out_dir / "_ARTIFACT_COMPLETE.json", {"axis_id": AXIS_ID, "status": "complete" if not missing else "incomplete", "missing_artifacts": missing, "authoritative_result": str(out_dir / "research_decision.json"), "generated_at_utc": datetime.now(timezone.utc).isoformat()})
    if missing:
        raise RuntimeError(f"missing artifacts: {missing}")
    return out_dir


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="TRADEX read-only upper MA touch profit-taking pretest.")
    parser.add_argument("--input-parquet", type=Path, default=DEFAULT_INPUT_PARQUET)
    parser.add_argument("--input-audit", type=Path, default=DEFAULT_INPUT_AUDIT)
    parser.add_argument("--observable-events", type=Path, default=DEFAULT_OBSERVABLE_EVENTS)
    parser.add_argument("--out-root", type=Path, default=DEFAULT_OUT_ROOT)
    return parser.parse_args()


def main() -> None:
    print(run(parse_args()))


if __name__ == "__main__":
    main()
