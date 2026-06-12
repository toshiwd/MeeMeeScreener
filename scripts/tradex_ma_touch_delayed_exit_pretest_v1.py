from __future__ import annotations

import argparse
import json
import math
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd


AXIS_ID = "ma_touch_delayed_exit_pretest_v1"
DEFAULT_FEATURE_ROOT = Path("G:/Tradex/ma_phase_feature_base_v1/20260603T121215Z-ma-phase-feature-base-v1")
DEFAULT_INPUT_PARQUET = DEFAULT_FEATURE_ROOT / "ma_phase_features.parquet"
DEFAULT_INPUT_AUDIT = DEFAULT_FEATURE_ROOT / "input_audit.json"
DEFAULT_OBSERVABLE_EVENTS = Path("G:/Tradex/ma_touch_observable_signal_v1/20260603T152852Z-ma-touch-observable-signal-v1/ma_touch_observable_signal_events.csv")
DEFAULT_PROFIT_EVENTS = Path("G:/Tradex/ma_touch_profit_take_pretest_v1/20260603T153425Z-ma-touch-profit-take-pretest-v1/ma_touch_profit_take_events.csv")
DEFAULT_OUT_ROOT = Path("G:/Tradex/ma_touch_delayed_exit_pretest_v1")
TARGET_MAS = ("MA60", "MA100", "MA200")
SIGNALS = ("high_touch_only_weak", "gap_touch_fade", "close_break_above_weak", "close_break_above_strong")
ACTIONS = (
    "hold_after_touch",
    "exit_on_touch_close",
    "exit_next_day_weak_confirm",
    "exit_on_ma20_rebreak_3b",
    "exit_on_ma20_rebreak_5b",
    "exit_on_ma20_rebreak_10b",
    "exit_on_target_ma_failed_hold_3b",
    "exit_on_target_ma_failed_hold_5b",
    "exit_on_5bar_failed_reacceleration",
    "conditional_hold_if_support_holds",
    "weak_exit_strong_hold_control",
)
REQUIRED_ARTIFACTS = (
    "input_audit.json",
    "delayed_exit_definition.json",
    "ma_touch_delayed_exit_events.csv",
    "action_summary.csv",
    "signal_action_contrast.csv",
    "support_context_summary.csv",
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
    cols = ["code", "ymd", "c", "ma20", "ma60", "ma100", "ma200", "is_upper_shadow_long", "is_small_body", "is_doji_like", "h", "l"]
    base = pd.read_parquet(path, columns=cols).sort_values(["code", "ymd"], kind="stable")
    base["code"] = base["code"].astype(str)
    body_range = pd.to_numeric(base["h"] - base["l"], errors="coerce").replace(0, float("nan"))
    base["close_position_in_range"] = pd.to_numeric((base["c"] - base["l"]) / body_range, errors="coerce")
    base["weak_close_position"] = base["close_position_in_range"].lt(0.4).fillna(False)
    for col in ["ymd", "c", "ma20", "ma60", "ma100", "ma200", "is_upper_shadow_long", "is_small_body", "is_doji_like", "weak_close_position"]:
        base[f"next_{col}"] = base.groupby("code", sort=False)[col].shift(-1)
    return base[["code", "ymd", "next_ymd", "next_c", "next_ma20", "next_ma60", "next_ma100", "next_ma200", "next_is_upper_shadow_long", "next_is_small_body", "next_is_doji_like", "next_weak_close_position"]]


def _load_events(profit_events: Path, feature_path: Path) -> pd.DataFrame:
    frame = pd.read_csv(profit_events)
    frame["code"] = frame["code"].astype(str)
    frame = frame.merge(_load_next_day_features(feature_path), on=["code", "ymd"], how="left", validate="many_to_one", suffixes=("", "_fresh"))
    frame["event_year"] = frame["ymd"].astype(str).str.slice(0, 4).astype(int)
    for col in [c for c in frame.columns if c.startswith("signal_")]:
        frame[col] = frame[col].fillna(False).astype(bool)
    for col in [
        "rebreak_ma20_5b",
        "rebreak_ma20_10b",
        "rebreak_ma20_20b",
        "target_ma_rebreak_5b",
        "target_ma_rebreak_10b",
        "target_ma_rebreak_20b",
        "higher_high_made_5b",
        "pullback_occurred_5b",
        "severe_loss_flag_20b",
    ]:
        frame[col] = frame[col].fillna(False).astype(bool)
    next_target = []
    for row in frame.itertuples(index=False):
        next_target.append(getattr(row, f"next_{str(row.target_ma).lower()}", float("nan")))
    frame["next_target_ma_value"] = pd.to_numeric(pd.Series(next_target, index=frame.index), errors="coerce")
    frame["next_day_weak_confirm"] = (
        pd.to_numeric(frame["next_c"], errors="coerce").lt(pd.to_numeric(frame["c"], errors="coerce"))
        | pd.to_numeric(frame["next_c"], errors="coerce").lt(frame["next_target_ma_value"])
        | pd.to_numeric(frame["next_c"], errors="coerce").lt(pd.to_numeric(frame["next_ma20"], errors="coerce"))
        | frame["next_is_upper_shadow_long"].fillna(False).astype(bool)
        | frame["next_is_small_body"].fillna(False).astype(bool)
        | frame["next_is_doji_like"].fillna(False).astype(bool)
        | frame["next_weak_close_position"].fillna(False).astype(bool)
    )
    frame["target_failed_hold_3b"] = frame["target_ma_rebreak_5b"] | frame["touch_method"].eq("high_touch_only")
    frame["target_failed_hold_5b"] = frame["target_ma_rebreak_5b"] | frame["touch_method"].eq("high_touch_only")
    frame["failed_reacceleration_5b"] = (~frame["higher_high_made_5b"]) & (pd.to_numeric(frame["ret_5b"], errors="coerce") <= 0)
    frame["has_lower_support"] = frame["signal_touch_with_lower_support"].fillna(False).astype(bool)
    frame["lower_support_absent"] = frame["signal_touch_without_lower_support"].fillna(False).astype(bool)
    return frame


def _signal_mask(frame: pd.DataFrame, signal: str) -> pd.Series:
    return frame[f"signal_{signal}"].fillna(False)


def _exit_mask(frame: pd.DataFrame, action: str) -> tuple[pd.Series, pd.Series]:
    if action == "hold_after_touch":
        return pd.Series(False, index=frame.index), pd.Series(0, index=frame.index)
    if action == "exit_on_touch_close":
        return pd.Series(True, index=frame.index), pd.Series(0, index=frame.index)
    if action == "exit_next_day_weak_confirm":
        mask = frame["next_day_weak_confirm"].fillna(False)
        return mask, pd.Series(1, index=frame.index).where(mask, 0)
    if action == "exit_on_ma20_rebreak_3b":
        mask = frame["rebreak_ma20_5b"]
        return mask, pd.Series(3, index=frame.index).where(mask, 0)
    if action == "exit_on_ma20_rebreak_5b":
        mask = frame["rebreak_ma20_5b"]
        return mask, pd.Series(5, index=frame.index).where(mask, 0)
    if action == "exit_on_ma20_rebreak_10b":
        mask = frame["rebreak_ma20_10b"]
        return mask, pd.Series(10, index=frame.index).where(mask, 0)
    if action == "exit_on_target_ma_failed_hold_3b":
        mask = frame["target_failed_hold_3b"]
        return mask, pd.Series(3, index=frame.index).where(mask, 0)
    if action == "exit_on_target_ma_failed_hold_5b":
        mask = frame["target_failed_hold_5b"]
        return mask, pd.Series(5, index=frame.index).where(mask, 0)
    if action == "exit_on_5bar_failed_reacceleration":
        mask = frame["failed_reacceleration_5b"] | frame["rebreak_ma20_5b"]
        return mask, pd.Series(5, index=frame.index).where(mask, 0)
    if action == "conditional_hold_if_support_holds":
        weak = frame["signal_high_touch_only_weak"] | frame["signal_gap_touch_fade"] | frame["signal_close_break_above_weak"]
        mask = weak & frame["lower_support_absent"] & frame["next_day_weak_confirm"]
        return mask, pd.Series(1, index=frame.index).where(mask, 0)
    if action == "weak_exit_strong_hold_control":
        mask = frame["signal_high_touch_only_weak"] | frame["signal_gap_touch_fade"] | frame["signal_close_break_above_weak"]
        return mask, pd.Series(0, index=frame.index).where(mask, 0)
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
    exit_mask, delay = _exit_mask(valid, action)
    ret20 = pd.to_numeric(valid["ret_20b"], errors="coerce")
    avoided = (-ret20.clip(upper=0)).where(exit_mask, 0.0)
    opportunity = ret20.clip(lower=0).where(exit_mask, 0.0)
    net = (-ret20).where(exit_mask, 0.0)
    big_winner = ret20.ge(10)
    row = {
        "target_ma": target_ma,
        "observable_signal": signal,
        "action": action,
        "event_count": int(len(valid)),
        "unique_symbol_count": int(valid["code"].nunique()) if not valid.empty else 0,
        "mean_ret5_after_action": _mean(valid["ret_5b"]),
        "mean_ret10_after_action": _mean(valid["ret_10b"]),
        "mean_ret20_after_action": _mean(valid["ret_20b"]),
        "median_ret20_after_action": _median(valid["ret_20b"]),
        "hit_rate20": _rate(ret20 > 0) if not valid.empty else None,
        "severe_loss_rate20": _rate(valid["severe_loss_flag_20b"]) if not valid.empty else None,
        "mean_max_drawdown20": _mean(valid["max_drawdown_20b"]),
        "median_max_drawdown20": _median(valid["max_drawdown_20b"]),
        "ma20_rebreak_rate20": _rate(valid["rebreak_ma20_20b"]) if not valid.empty else None,
        "target_ma_rebreak_rate20": _rate(valid["target_ma_rebreak_20b"]) if not valid.empty else None,
        "avoided_loss_mean": _mean(avoided),
        "opportunity_cost_mean": _mean(opportunity),
        "net_exit_advantage_vs_hold": _mean(net),
        "tail_loss_reduction": _rate(valid["severe_loss_flag_20b"] & exit_mask) if not valid.empty else None,
        "missed_big_winner_rate": _rate(big_winner & exit_mask) if not valid.empty else None,
        "winner_retention_rate": _rate((~exit_mask) & big_winner) if not valid.empty else None,
        "delayed_exit_trigger_rate": _rate(exit_mask) if not valid.empty else None,
        "average_exit_delay_bars": _mean(delay.where(exit_mask)),
    }
    if context_name is not None:
        row["context_name"] = context_name
        row["context_value"] = context_value
    return row


def _summary(frame: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for target_ma, target_group in frame.groupby("target_ma", sort=False):
        for signal in SIGNALS:
            group = target_group[_signal_mask(target_group, signal)]
            for action in ACTIONS:
                rows.append(_row(group, target_ma=target_ma, signal=signal, action=action))
    return pd.DataFrame(rows)


def _support_context(frame: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for context in ["lower_support_bucket", "target_ma_slope_context", "ma20_phase_context"]:
        for (target_ma, value), group in frame.groupby(["target_ma", context], dropna=False, sort=False):
            for action in ["exit_next_day_weak_confirm", "exit_on_ma20_rebreak_5b", "conditional_hold_if_support_holds"]:
                rows.append(_row(group, target_ma=target_ma, signal=f"context_{context}", action=action, context_name=context, context_value=str(value)))
    return pd.DataFrame(rows)


def _lookup(summary: pd.DataFrame, target_ma: str, signal: str, action: str) -> dict[str, Any] | None:
    row = summary[(summary["target_ma"] == target_ma) & (summary["observable_signal"] == signal) & (summary["action"] == action)]
    return None if row.empty else row.iloc[0].to_dict()


def _compare(summary: pd.DataFrame, target_ma: str, signal: str, first: str, second: str, name: str) -> dict[str, Any]:
    a = _lookup(summary, target_ma, signal, first)
    b = _lookup(summary, target_ma, signal, second)
    payload = {"name": f"{target_ma}_{signal}_{name}", "target_ma": target_ma, "signal": signal, "first_action": first, "second_action": second}
    if not a or not b:
        payload["status"] = "missing_group"
        return payload
    deltas = {}
    for metric in ["net_exit_advantage_vs_hold", "tail_loss_reduction", "missed_big_winner_rate", "winner_retention_rate", "opportunity_cost_mean", "avoided_loss_mean", "delayed_exit_trigger_rate", "average_exit_delay_bars"]:
        if a.get(metric) is not None and b.get(metric) is not None:
            deltas[f"{metric}_delta"] = a[metric] - b[metric]
    payload.update({"status": "ready" if a["event_count"] >= 200 and b["event_count"] >= 200 else "insufficient_sample", "first_metrics": a, "second_metrics": b, "deltas": deltas})
    return payload


def _contrasts(summary: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for target_ma in TARGET_MAS:
        for signal in ("high_touch_only_weak", "gap_touch_fade", "close_break_above_weak"):
            rows.extend(
                [
                    _compare(summary, target_ma, signal, "exit_next_day_weak_confirm", "exit_on_touch_close", "next_day_vs_touch_exit"),
                    _compare(summary, target_ma, signal, "exit_next_day_weak_confirm", "hold_after_touch", "next_day_vs_hold"),
                    _compare(summary, target_ma, signal, "exit_on_ma20_rebreak_5b", "exit_on_touch_close", "ma20_rebreak5_vs_touch_exit"),
                    _compare(summary, target_ma, signal, "exit_on_ma20_rebreak_5b", "hold_after_touch", "ma20_rebreak5_vs_hold"),
                    _compare(summary, target_ma, signal, "exit_on_5bar_failed_reacceleration", "hold_after_touch", "failed_reaccel5_vs_hold"),
                    _compare(summary, target_ma, signal, "conditional_hold_if_support_holds", "exit_on_touch_close", "conditional_support_vs_touch_exit"),
                    _compare(summary, target_ma, signal, "weak_exit_strong_hold_control", "exit_next_day_weak_confirm", "weak_control_vs_next_day"),
                ]
            )
    return pd.DataFrame(rows)


def _yearly(frame: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for (year, target_ma), group in frame.groupby(["event_year", "target_ma"], sort=False):
        for signal in ("high_touch_only_weak", "gap_touch_fade", "close_break_above_weak"):
            subset = group[_signal_mask(group, signal)]
            for action in ("hold_after_touch", "exit_on_touch_close", "exit_next_day_weak_confirm", "exit_on_ma20_rebreak_5b", "exit_on_5bar_failed_reacceleration"):
                row = _row(subset, target_ma=target_ma, signal=signal, action=action)
                row["event_year"] = int(year)
                row["sample_status"] = "sufficient" if row["event_count"] >= 100 else "insufficient_sample"
                rows.append(row)
    return pd.DataFrame(rows).sort_values(["target_ma", "observable_signal", "action", "event_year"], kind="stable")


def _stable_years(yearly: pd.DataFrame) -> int:
    subset = yearly[
        yearly["action"].isin(["exit_next_day_weak_confirm", "exit_on_ma20_rebreak_5b", "exit_on_5bar_failed_reacceleration"])
        & yearly["sample_status"].eq("sufficient")
    ]
    support = subset[(subset["net_exit_advantage_vs_hold"] > 0) | ((subset["tail_loss_reduction"] >= 0.05) & (subset["missed_big_winner_rate"] <= 0.08))]
    return int(support["event_year"].nunique())


def _decision(contrast: pd.DataFrame, yearly: pd.DataFrame) -> dict[str, Any]:
    rule = []
    context = []
    entry = []
    for row in contrast[contrast["status"].eq("ready")].to_dict("records"):
        d = row["deltas"]
        if any(key in row["name"] for key in ["next_day_vs_hold", "ma20_rebreak5_vs_hold", "failed_reaccel5_vs_hold"]):
            net = d.get("net_exit_advantage_vs_hold_delta", 0)
            tail = d.get("tail_loss_reduction_delta", 0)
            missed = d.get("missed_big_winner_rate_delta", 1)
            opp = d.get("opportunity_cost_mean_delta", 0)
            if net > 0 and tail >= 0.03 and missed <= 0.08:
                rule.append({"typed_reason": "delayed_exit_improves_net_and_tail_with_lower_missed_winners", "contrast": row["name"], "deltas": d})
            elif tail >= 0.03 and opp < 3.0:
                context.append({"typed_reason": "delayed_exit_reduces_tail_loss_but_net_edge_is_weak", "contrast": row["name"], "deltas": d})
            else:
                entry.append({"typed_reason": "delayed_exit_more_suitable_as_entry_guard_than_position_exit", "contrast": row["name"], "deltas": d})
    stable = _stable_years(yearly)
    if rule and stable >= 4:
        decision = "keep_for_profit_take_rule_pretest_next"
        reason = "delayed_exit_passes_profit_take_pretest_gates"
    elif context:
        decision = "keep_as_profit_take_context"
        reason = "delayed_exit_reduces_tail_loss_but_net_advantage_is_weak"
    elif entry:
        decision = "keep_as_entry_guard_only"
        reason = "delayed_exit_does_not_justify_existing_position_exit"
    else:
        decision = "drop"
        reason = "delayed_exit_does_not_improve_over_hold_or_immediate_exit"
    return {
        "axis_id": AXIS_ID,
        "candidate_local_decision": decision,
        "session_aggregate_decision": decision,
        "authoritative_rollup_decision": decision,
        "reason": reason,
        "profit_take_rule_reasons": rule,
        "profit_take_context_reasons": context,
        "entry_guard_only_reasons": entry,
        "stable_years": stable,
        "future_reaction_type_used_as_input": False,
        "no_future_reaction_type_used_as_input": True,
        "non_scope": ["no MeeMee reflection", "no runtime DB write", "no ranking change", "no publish", "no candidate generation change", "no live sell rule", "no bad-pick removal", "no entry guard implementation", "no score tuning", "no threshold optimization"],
    }


def _definition() -> dict[str, Any]:
    return {
        "axis_id": AXIS_ID,
        "target_events": "upper MA touch observable events for MA60/MA100/MA200",
        "action_return_model": "read-only event approximation: exits are evaluated by avoided/opportunity forward ret20 from touch close; delayed exit trigger uses observable next-day or horizon flags, not future reaction_type labels",
        "research_fallback": "3-bar MA20/target rebreak flags are not present in the profit event input, so 3-bar variants use available 5-bar rebreak flags while retaining a 3-bar nominal delay for conservative comparison",
        "actions": list(ACTIONS),
        "future_reaction_type_used_as_input": False,
        "no_future_reaction_type_used_as_input": True,
        "authoritative_input": str(DEFAULT_INPUT_PARQUET),
        "observable_events": str(DEFAULT_OBSERVABLE_EVENTS),
        "profit_events": str(DEFAULT_PROFIT_EVENTS),
    }


def run(args: argparse.Namespace) -> Path:
    out_dir = args.out_root / f"{_now_tag()}-{AXIS_ID.replace('_', '-')}"
    out_dir.mkdir(parents=True, exist_ok=False)
    source_audit = json.loads(args.input_audit.read_text(encoding="utf-8"))
    frame = _load_events(args.profit_events, args.input_parquet)
    summary = _summary(frame)
    support_context = _support_context(frame)
    contrast = _contrasts(summary)
    yearly = _yearly(frame)
    decision = _decision(contrast, yearly)
    examples = frame[frame["signal_high_touch_only_weak"] | frame["signal_gap_touch_fade"] | frame["signal_close_break_above_weak"]].head(5000)
    input_audit = {
        "axis_id": AXIS_ID,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "input_parquet": str(args.input_parquet),
        "input_audit": str(args.input_audit),
        "observable_events": str(args.observable_events),
        "profit_events": str(args.profit_events),
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
        "future_reaction_type_used_as_input": False,
        "no_future_reaction_type_used_as_input": True,
    }
    _write_json(out_dir / "input_audit.json", input_audit)
    _write_json(out_dir / "delayed_exit_definition.json", _definition())
    frame.to_csv(out_dir / "ma_touch_delayed_exit_events.csv", index=False, encoding="utf-8")
    summary.to_csv(out_dir / "action_summary.csv", index=False, encoding="utf-8")
    contrast.to_csv(out_dir / "signal_action_contrast.csv", index=False, encoding="utf-8")
    support_context.to_csv(out_dir / "support_context_summary.csv", index=False, encoding="utf-8")
    yearly.to_csv(out_dir / "yearly_stability_summary.csv", index=False, encoding="utf-8")
    examples.to_csv(out_dir / "candidate_examples.csv", index=False, encoding="utf-8")
    _write_json(out_dir / "research_decision.json", decision)
    missing = [name for name in REQUIRED_ARTIFACTS if name != "_ARTIFACT_COMPLETE.json" and not (out_dir / name).exists()]
    _write_json(out_dir / "_ARTIFACT_COMPLETE.json", {"axis_id": AXIS_ID, "status": "complete" if not missing else "incomplete", "missing_artifacts": missing, "authoritative_result": str(out_dir / "research_decision.json"), "generated_at_utc": datetime.now(timezone.utc).isoformat()})
    if missing:
        raise RuntimeError(f"missing artifacts: {missing}")
    return out_dir


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="TRADEX read-only upper MA touch delayed exit pretest.")
    parser.add_argument("--input-parquet", type=Path, default=DEFAULT_INPUT_PARQUET)
    parser.add_argument("--input-audit", type=Path, default=DEFAULT_INPUT_AUDIT)
    parser.add_argument("--observable-events", type=Path, default=DEFAULT_OBSERVABLE_EVENTS)
    parser.add_argument("--profit-events", type=Path, default=DEFAULT_PROFIT_EVENTS)
    parser.add_argument("--out-root", type=Path, default=DEFAULT_OUT_ROOT)
    return parser.parse_args()


def main() -> None:
    print(run(parse_args()))


if __name__ == "__main__":
    main()
