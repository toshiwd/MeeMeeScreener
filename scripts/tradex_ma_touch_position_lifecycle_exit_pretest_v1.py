from __future__ import annotations

import argparse
import json
import math
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd


AXIS_ID = "ma_touch_position_lifecycle_exit_pretest_v1"
DEFAULT_FEATURE_ROOT = Path("G:/Tradex/ma_phase_feature_base_v1/20260603T121215Z-ma-phase-feature-base-v1")
DEFAULT_INPUT_PARQUET = DEFAULT_FEATURE_ROOT / "ma_phase_features.parquet"
DEFAULT_INPUT_AUDIT = DEFAULT_FEATURE_ROOT / "input_audit.json"
DEFAULT_OBSERVABLE_EVENTS = Path("G:/Tradex/ma_touch_observable_signal_v1/20260603T152852Z-ma-touch-observable-signal-v1/ma_touch_observable_signal_events.csv")
DEFAULT_DELAYED_EVENTS = Path("G:/Tradex/ma_touch_delayed_exit_pretest_v1/20260603T154539Z-ma-touch-delayed-exit-pretest-v1/ma_touch_delayed_exit_events.csv")
DEFAULT_OUT_ROOT = Path("G:/Tradex/ma_touch_position_lifecycle_exit_pretest_v1")
KNOWN_LIFECYCLE_DIRS = [
    "G:/Tradex/position_lifecycle_state_machine_v1",
    "G:/Tradex/position_lifecycle_current_board_v1",
    "G:/Tradex/position_management_policy_pretest_v1",
    "G:/Tradex/practical_decision_support_bundle_v1",
]
TARGET_MAS = ("MA60", "MA100", "MA200")
SIGNALS = ("high_touch_only_weak", "gap_touch_fade", "close_break_above_weak", "close_break_above_strong")
POLICIES = (
    "hold_baseline",
    "exit_on_touch_close",
    "delayed_exit_ma20_rebreak_5",
    "delayed_exit_failed_reacceleration_5",
    "delayed_exit_next_day_weak_confirm",
    "support_aware_delayed_exit",
    "strong_break_hold_weak_exit_control",
)
REQUIRED_ARTIFACTS = (
    "input_audit.json",
    "position_lifecycle_definition.json",
    "exit_policy_definition.json",
    "position_touch_exit_events.csv",
    "policy_summary.csv",
    "profit_bucket_summary.csv",
    "holding_age_summary.csv",
    "signal_policy_contrast.csv",
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
        return [_json_ready(v) for k in []] if False else [_json_ready(v) for v in value]
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


def _load_feature_index(path: Path) -> pd.DataFrame:
    cols = ["code", "ymd", "c"]
    frame = pd.read_parquet(path, columns=cols).sort_values(["code", "ymd"], kind="stable")
    frame["code"] = frame["code"].astype(str)
    frame["row_idx"] = frame.groupby("code", sort=False).cumcount()
    return frame[["code", "ymd", "c", "row_idx"]]


def _load_events(delayed_events: Path, feature_path: Path) -> pd.DataFrame:
    events = pd.read_csv(delayed_events)
    events["code"] = events["code"].astype(str)
    features = _load_feature_index(feature_path)
    events = events.merge(features.rename(columns={"c": "feature_touch_close", "row_idx": "touch_row_idx"}), on=["code", "ymd"], how="left", validate="many_to_one")
    events["holding_age_bars"] = pd.to_numeric(events["consecutive_bars_above_ma20"], errors="coerce").fillna(0).clip(lower=1).astype(int)
    entry = features.rename(columns={"ymd": "synthetic_entry_ymd", "c": "synthetic_entry_price", "row_idx": "entry_row_idx"})
    events["entry_row_idx"] = events["touch_row_idx"] - events["holding_age_bars"] + 1
    events = events.merge(entry, on=["code", "entry_row_idx"], how="left", validate="many_to_one")
    events["entry_price"] = pd.to_numeric(events["synthetic_entry_price"], errors="coerce")
    events["touch_close"] = pd.to_numeric(events["c"], errors="coerce")
    events["unrealized_ret_at_touch"] = (events["touch_close"] / events["entry_price"] - 1.0) * 100.0
    events = events[events["entry_price"].notna() & events["touch_close"].notna() & events["unrealized_ret_at_touch"].notna()].copy()
    events["event_year"] = events["ymd"].astype(str).str.slice(0, 4).astype(int)
    events["profit_bucket"] = pd.cut(
        events["unrealized_ret_at_touch"],
        bins=[-float("inf"), 0, 3, 7, 15, float("inf")],
        labels=["<=0%", "0-3%", "3-7%", "7-15%", "15%+"],
        right=True,
    ).astype("string")
    events["holding_age_bucket"] = pd.cut(
        events["holding_age_bars"],
        bins=[0, 5, 10, 20, 40, float("inf")],
        labels=["1-5", "6-10", "11-20", "21-40", "41+"],
        right=True,
    ).astype("string")
    for col in [c for c in events.columns if c.startswith("signal_")]:
        events[col] = events[col].fillna(False).astype(bool)
    for col in ["rebreak_ma20_5b", "rebreak_ma20_20b", "higher_high_made_5b", "pullback_occurred_5b", "severe_loss_flag_20b", "next_day_weak_confirm", "lower_support_absent"]:
        if col in events.columns:
            events[col] = events[col].fillna(False).astype(bool)
    return events


def _signal_mask(frame: pd.DataFrame, signal: str) -> pd.Series:
    return frame[f"signal_{signal}"].fillna(False)


def _exit_mask(frame: pd.DataFrame, policy: str) -> tuple[pd.Series, pd.Series]:
    if policy == "hold_baseline":
        return pd.Series(False, index=frame.index), pd.Series(0, index=frame.index)
    if policy == "exit_on_touch_close":
        return pd.Series(True, index=frame.index), pd.Series(0, index=frame.index)
    if policy == "delayed_exit_ma20_rebreak_5":
        mask = frame["rebreak_ma20_5b"]
        return mask, pd.Series(5, index=frame.index).where(mask, 0)
    if policy == "delayed_exit_failed_reacceleration_5":
        mask = ((~frame["higher_high_made_5b"]) & (pd.to_numeric(frame["ret_5b"], errors="coerce") <= 0)) | frame["rebreak_ma20_5b"]
        return mask, pd.Series(5, index=frame.index).where(mask, 0)
    if policy == "delayed_exit_next_day_weak_confirm":
        mask = frame["next_day_weak_confirm"]
        return mask, pd.Series(1, index=frame.index).where(mask, 0)
    if policy == "support_aware_delayed_exit":
        weak = frame["signal_high_touch_only_weak"] | frame["signal_gap_touch_fade"] | frame["signal_close_break_above_weak"]
        mask = weak & frame["lower_support_absent"].fillna(False) & frame["next_day_weak_confirm"].fillna(False)
        return mask, pd.Series(1, index=frame.index).where(mask, 0)
    if policy == "strong_break_hold_weak_exit_control":
        mask = frame["signal_high_touch_only_weak"] | frame["signal_gap_touch_fade"] | frame["signal_close_break_above_weak"]
        return mask, pd.Series(0, index=frame.index).where(mask, 0)
    raise ValueError(policy)


def _mean(series: pd.Series) -> float | None:
    valid = pd.to_numeric(series, errors="coerce").dropna()
    return None if valid.empty else float(valid.mean())


def _median(series: pd.Series) -> float | None:
    valid = pd.to_numeric(series, errors="coerce").dropna()
    return None if valid.empty else float(valid.median())


def _rate(series: pd.Series) -> float | None:
    valid = series.dropna()
    return None if valid.empty else float(valid.astype(bool).mean())


def _row(group: pd.DataFrame, *, policy: str, target_ma: str, signal: str, profit_bucket: str = "ALL", holding_age_bucket: str = "ALL") -> dict[str, Any]:
    valid = group[group["ret_20b"].notna()].copy()
    exit_mask, delay = _exit_mask(valid, policy)
    ret20 = pd.to_numeric(valid["ret_20b"], errors="coerce")
    unreal = pd.to_numeric(valid["unrealized_ret_at_touch"], errors="coerce")
    avoided = (-ret20.clip(upper=0)).where(exit_mask, 0.0)
    opportunity = ret20.clip(lower=0).where(exit_mask, 0.0)
    net = (-ret20).where(exit_mask, 0.0)
    big_winner = ret20.ge(10)
    return {
        "policy": policy,
        "target_ma": target_ma,
        "observable_signal": signal,
        "profit_bucket": profit_bucket,
        "holding_age_bucket": holding_age_bucket,
        "event_count": int(len(valid)),
        "unique_symbol_count": int(valid["code"].nunique()) if not valid.empty else 0,
        "mean_incremental_ret20_vs_exit": _mean(ret20),
        "median_incremental_ret20_vs_exit": _median(ret20),
        "net_exit_advantage_vs_hold": _mean(net),
        "realized_profit_protected": _mean(unreal.where(exit_mask, 0.0)),
        "avoided_loss_mean": _mean(avoided),
        "opportunity_cost_mean": _mean(opportunity),
        "tail_loss_reduction": _rate(valid["severe_loss_flag_20b"] & exit_mask) if not valid.empty else None,
        "missed_big_winner_rate": _rate(big_winner & exit_mask) if not valid.empty else None,
        "winner_retention_rate": _rate((~exit_mask) & big_winner) if not valid.empty else None,
        "max_drawdown_after_touch": _mean(valid["max_drawdown_20b"]),
        "MA20_rebreak_rate": _rate(valid["rebreak_ma20_20b"]) if not valid.empty else None,
        "average_exit_delay_bars": _mean(delay.where(exit_mask)),
        "exit_trigger_rate": _rate(exit_mask) if not valid.empty else None,
        "mean_unrealized_ret_at_touch": _mean(unreal),
        "mean_holding_age_bars": _mean(valid["holding_age_bars"]),
    }


def _policy_summary(frame: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for target_ma, tg in frame.groupby("target_ma", sort=False):
        for signal in SIGNALS:
            sg = tg[_signal_mask(tg, signal)]
            for profit_bucket, pg in sg.groupby("profit_bucket", dropna=False, sort=False):
                for age_bucket, ag in pg.groupby("holding_age_bucket", dropna=False, sort=False):
                    for policy in POLICIES:
                        rows.append(_row(ag, policy=policy, target_ma=target_ma, signal=signal, profit_bucket=str(profit_bucket), holding_age_bucket=str(age_bucket)))
    return pd.DataFrame(rows)


def _bucket_summary(frame: pd.DataFrame, bucket_col: str) -> pd.DataFrame:
    rows = []
    for (target_ma, bucket), group in frame.groupby(["target_ma", bucket_col], dropna=False, sort=False):
        for policy in ["hold_baseline", "exit_on_touch_close", "delayed_exit_ma20_rebreak_5", "delayed_exit_failed_reacceleration_5", "delayed_exit_next_day_weak_confirm"]:
            rows.append(_row(group, policy=policy, target_ma=target_ma, signal=f"ALL_BY_{bucket_col}", profit_bucket=str(bucket) if bucket_col == "profit_bucket" else "ALL", holding_age_bucket=str(bucket) if bucket_col == "holding_age_bucket" else "ALL"))
    return pd.DataFrame(rows)


def _aggregate_summary(frame: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for target_ma, tg in frame.groupby("target_ma", sort=False):
        for signal in SIGNALS:
            sg = tg[_signal_mask(tg, signal)]
            for policy in POLICIES:
                rows.append(_row(sg, policy=policy, target_ma=target_ma, signal=signal))
    return pd.DataFrame(rows)


def _lookup(summary: pd.DataFrame, target_ma: str, signal: str, policy: str) -> dict[str, Any] | None:
    row = summary[(summary["target_ma"] == target_ma) & (summary["observable_signal"] == signal) & (summary["policy"] == policy)]
    return None if row.empty else row.iloc[0].to_dict()


def _compare(summary: pd.DataFrame, target_ma: str, signal: str, first: str, second: str, name: str) -> dict[str, Any]:
    a = _lookup(summary, target_ma, signal, first)
    b = _lookup(summary, target_ma, signal, second)
    payload = {"name": f"{target_ma}_{signal}_{name}", "target_ma": target_ma, "signal": signal, "first_policy": first, "second_policy": second}
    if not a or not b:
        payload["status"] = "missing_group"
        return payload
    deltas = {}
    for metric in ["net_exit_advantage_vs_hold", "realized_profit_protected", "tail_loss_reduction", "missed_big_winner_rate", "winner_retention_rate", "opportunity_cost_mean", "avoided_loss_mean", "exit_trigger_rate"]:
        if a.get(metric) is not None and b.get(metric) is not None:
            deltas[f"{metric}_delta"] = a[metric] - b[metric]
    payload.update({"status": "ready" if a["event_count"] >= 100 and b["event_count"] >= 100 else "insufficient_sample", "first_metrics": a, "second_metrics": b, "deltas": deltas})
    return payload


def _contrasts(frame: pd.DataFrame, aggregate: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for target_ma in TARGET_MAS:
        for signal in ("high_touch_only_weak", "gap_touch_fade", "close_break_above_weak"):
            rows.extend(
                [
                    _compare(aggregate, target_ma, signal, "delayed_exit_failed_reacceleration_5", "hold_baseline", "failed_reaccel5_vs_hold"),
                    _compare(aggregate, target_ma, signal, "delayed_exit_failed_reacceleration_5", "exit_on_touch_close", "failed_reaccel5_vs_touch_exit"),
                    _compare(aggregate, target_ma, signal, "delayed_exit_ma20_rebreak_5", "hold_baseline", "ma20_rebreak5_vs_hold"),
                    _compare(aggregate, target_ma, signal, "support_aware_delayed_exit", "delayed_exit_failed_reacceleration_5", "support_aware_vs_failed_reaccel5"),
                    _compare(aggregate, target_ma, signal, "strong_break_hold_weak_exit_control", "hold_baseline", "strong_break_hold_weak_exit_vs_hold"),
                ]
            )
    for scope_name, mask in [
        ("profitable_only", frame["unrealized_ret_at_touch"] > 0),
        ("profit_3pct_plus", frame["unrealized_ret_at_touch"] >= 3),
        ("profit_lte_3pct", frame["unrealized_ret_at_touch"] <= 3),
        ("holding_age_11plus", frame["holding_age_bars"] >= 11),
        ("holding_age_lte10", frame["holding_age_bars"] <= 10),
    ]:
        scoped = _aggregate_summary(frame[mask])
        for target_ma in TARGET_MAS:
            for signal in ("high_touch_only_weak", "gap_touch_fade"):
                rows.append(_compare(scoped, target_ma, signal, "delayed_exit_failed_reacceleration_5", "hold_baseline", f"{scope_name}_failed_reaccel5_vs_hold"))
    return pd.DataFrame(rows)


def _yearly(frame: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for (year, target_ma), group in frame.groupby(["event_year", "target_ma"], sort=False):
        for signal in ("high_touch_only_weak", "gap_touch_fade", "close_break_above_weak"):
            sg = group[_signal_mask(group, signal)]
            for policy in ("hold_baseline", "exit_on_touch_close", "delayed_exit_ma20_rebreak_5", "delayed_exit_failed_reacceleration_5"):
                row = _row(sg, policy=policy, target_ma=target_ma, signal=signal)
                row["event_year"] = int(year)
                row["sample_status"] = "sufficient" if row["event_count"] >= 50 else "insufficient_sample"
                rows.append(row)
    return pd.DataFrame(rows).sort_values(["target_ma", "observable_signal", "policy", "event_year"], kind="stable")


def _stable_years(yearly: pd.DataFrame) -> int:
    subset = yearly[
        yearly["policy"].isin(["delayed_exit_ma20_rebreak_5", "delayed_exit_failed_reacceleration_5"])
        & yearly["observable_signal"].isin(["high_touch_only_weak", "gap_touch_fade"])
        & yearly["sample_status"].eq("sufficient")
    ]
    support = subset[(subset["net_exit_advantage_vs_hold"] > 0) & (subset["missed_big_winner_rate"] <= 0.08)]
    return int(support["event_year"].nunique())


def _decision(contrast: pd.DataFrame, yearly: pd.DataFrame, fallback_used: bool) -> dict[str, Any]:
    rule = []
    context = []
    monitor = []
    for row in contrast[contrast["status"].eq("ready")].to_dict("records"):
        d = row["deltas"]
        if "failed_reaccel5_vs_hold" in row["name"] or "ma20_rebreak5_vs_hold" in row["name"]:
            net = d.get("net_exit_advantage_vs_hold_delta", 0)
            missed = d.get("missed_big_winner_rate_delta", 1)
            tail = d.get("tail_loss_reduction_delta", 0)
            if net > 0 and missed <= 0.08 and tail >= 0.03:
                rule.append({"typed_reason": "delayed_exit_improves_profitable_lifecycle_proxy", "contrast": row["name"], "deltas": d})
            elif tail >= 0.03:
                context.append({"typed_reason": "delayed_exit_reduces_tail_but_lifecycle_proxy_net_is_weaker", "contrast": row["name"], "deltas": d})
            else:
                monitor.append({"typed_reason": "delayed_exit_has_monitor_value_only_in_lifecycle_proxy", "contrast": row["name"], "deltas": d})
    stable = _stable_years(yearly)
    if rule and stable >= 4 and not fallback_used:
        decision = "keep_for_sell_rule_candidate_next"
        reason = "real_lifecycle_delayed_exit_passes_sell_rule_candidate_gates"
    elif rule and stable >= 4 and fallback_used:
        decision = "hold"
        reason = "direction_passes_under_synthetic_lifecycle_but_real_lifecycle_source_is_missing"
    elif context:
        decision = "keep_as_profit_take_context"
        reason = "delayed_exit_reduces_tail_loss_but_sell_rule_evidence_is_incomplete"
    elif monitor:
        decision = "keep_as_position_monitor_only"
        reason = "position_lifecycle_proxy_supports_monitoring_not_exit_policy"
    else:
        decision = "drop"
        reason = "delayed_exit_does_not_improve_realistic_position_states"
    return {
        "axis_id": AXIS_ID,
        "candidate_local_decision": decision,
        "session_aggregate_decision": decision,
        "authoritative_rollup_decision": decision,
        "reason": reason,
        "sell_rule_candidate_reasons": rule,
        "profit_take_context_reasons": context,
        "position_monitor_only_reasons": monitor,
        "stable_years": stable,
        "research_fallback_used": fallback_used,
        "future_reaction_type_used_as_input": False,
        "no_future_reaction_type_used_as_input": True,
        "non_scope": ["no MeeMee reflection", "no runtime DB write", "no ranking change", "no publish", "no candidate generation change", "no live sell rule implementation", "no bad-pick removal", "no entry guard implementation", "no score tuning", "no threshold optimization"],
    }


def _definition() -> dict[str, Any]:
    return {
        "axis_id": AXIS_ID,
        "position_lifecycle_source": "synthetic_ma20_above_run_lifecycle",
        "synthetic_entry_rule": "entry_ymd/entry_price is reconstructed as the first bar in the current consecutive close-above-MA20 run before the upper MA touch",
        "is_real_position_lifecycle": False,
        "is_synthetic_lifecycle": True,
        "research_fallback_used": True,
        "profit_buckets": ["<=0%", "0-3%", "3-7%", "7-15%", "15%+"],
        "holding_age_buckets": ["1-5", "6-10", "11-20", "21-40", "41+"],
    }


def _exit_definition() -> dict[str, Any]:
    return {
        "axis_id": AXIS_ID,
        "policies": list(POLICIES),
        "future_reaction_type_used_as_input": False,
        "no_future_reaction_type_used_as_input": True,
        "policy_note": "event-level exit approximation uses forward ret20 from touch close for avoided/opportunity accounting; no live sell rule is changed",
    }


def run(args: argparse.Namespace) -> Path:
    out_dir = args.out_root / f"{_now_tag()}-{AXIS_ID.replace('_', '-')}"
    out_dir.mkdir(parents=True, exist_ok=False)
    source_audit = json.loads(args.input_audit.read_text(encoding="utf-8"))
    frame = _load_events(args.delayed_events, args.input_parquet)
    policy = _policy_summary(frame)
    profit_bucket = _bucket_summary(frame, "profit_bucket")
    holding_age = _bucket_summary(frame, "holding_age_bucket")
    aggregate = _aggregate_summary(frame)
    contrast = _contrasts(frame, aggregate)
    yearly = _yearly(frame)
    decision = _decision(contrast, yearly, True)
    examples = frame[frame["signal_high_touch_only_weak"] | frame["signal_gap_touch_fade"] | frame["signal_close_break_above_weak"]].head(5000)
    input_audit = {
        "axis_id": AXIS_ID,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "input_parquet": str(args.input_parquet),
        "input_audit": str(args.input_audit),
        "observable_events": str(args.observable_events),
        "delayed_events": str(args.delayed_events),
        "source_axis_id": source_audit.get("axis_id"),
        "confirmed_bars_only_inherited": bool(source_audit.get("confirmed_bars_only")),
        "position_lifecycle_source": "synthetic_ma20_above_run_lifecycle",
        "entry_source": "ma_phase_feature_base current MA20 above run reconstructed entry",
        "is_real_position_lifecycle": False,
        "is_synthetic_lifecycle": True,
        "research_fallback_used": True,
        "searched_lifecycle_artifact_paths": KNOWN_LIFECYCLE_DIRS,
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
    _write_json(out_dir / "position_lifecycle_definition.json", _definition())
    _write_json(out_dir / "exit_policy_definition.json", _exit_definition())
    frame.to_csv(out_dir / "position_touch_exit_events.csv", index=False, encoding="utf-8")
    policy.to_csv(out_dir / "policy_summary.csv", index=False, encoding="utf-8")
    profit_bucket.to_csv(out_dir / "profit_bucket_summary.csv", index=False, encoding="utf-8")
    holding_age.to_csv(out_dir / "holding_age_summary.csv", index=False, encoding="utf-8")
    contrast.to_csv(out_dir / "signal_policy_contrast.csv", index=False, encoding="utf-8")
    yearly.to_csv(out_dir / "yearly_stability_summary.csv", index=False, encoding="utf-8")
    examples.to_csv(out_dir / "candidate_examples.csv", index=False, encoding="utf-8")
    _write_json(out_dir / "research_decision.json", decision)
    missing = [name for name in REQUIRED_ARTIFACTS if name != "_ARTIFACT_COMPLETE.json" and not (out_dir / name).exists()]
    _write_json(out_dir / "_ARTIFACT_COMPLETE.json", {"axis_id": AXIS_ID, "status": "complete" if not missing else "incomplete", "missing_artifacts": missing, "authoritative_result": str(out_dir / "research_decision.json"), "generated_at_utc": datetime.now(timezone.utc).isoformat()})
    if missing:
        raise RuntimeError(f"missing artifacts: {missing}")
    return out_dir


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="TRADEX read-only MA touch position lifecycle exit pretest.")
    parser.add_argument("--input-parquet", type=Path, default=DEFAULT_INPUT_PARQUET)
    parser.add_argument("--input-audit", type=Path, default=DEFAULT_INPUT_AUDIT)
    parser.add_argument("--observable-events", type=Path, default=DEFAULT_OBSERVABLE_EVENTS)
    parser.add_argument("--delayed-events", type=Path, default=DEFAULT_DELAYED_EVENTS)
    parser.add_argument("--out-root", type=Path, default=DEFAULT_OUT_ROOT)
    return parser.parse_args()


def main() -> None:
    print(run(parse_args()))


if __name__ == "__main__":
    main()
