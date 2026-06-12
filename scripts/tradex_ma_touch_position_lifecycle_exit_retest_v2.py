from __future__ import annotations

import argparse
import json
import math
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd


AXIS_ID = "ma_touch_position_lifecycle_exit_retest_v2"
DEFAULT_OUT_ROOT = Path("G:/Tradex/ma_touch_position_lifecycle_exit_retest_v2")
DEFAULT_LEDGER_ROOT = Path("G:/Tradex/canonical_position_lifecycle_ledger_v1/20260604T001714Z-canonical-position-lifecycle-ledger-v1")
DEFAULT_LEDGER = DEFAULT_LEDGER_ROOT / "lifecycle_ledger.parquet"
DEFAULT_LEDGER_AUDIT = DEFAULT_LEDGER_ROOT / "input_audit.json"
DEFAULT_TOUCH_EVENTS = Path("G:/Tradex/ma_touch_delayed_exit_pretest_v1/20260603T154539Z-ma-touch-delayed-exit-pretest-v1/ma_touch_delayed_exit_events.csv")
CURRENT_POLICY = "policy_a_loss_control"
HOLD_POLICY = "baseline_hold20"
TARGET_MAS = ("MA60", "MA100", "MA200")
CHALLENGERS = ("delayed_exit_ma20_rebreak_5", "delayed_exit_failed_reacceleration_5")
WEAK_SIGNALS = ("high_touch_only_weak", "gap_touch_fade", "close_break_above_weak")
REQUIRED = (
    "input_audit.json",
    "retest_definition.json",
    "position_touch_retest_events.csv",
    "policy_comparison_summary.csv",
    "challenger_contrast.csv",
    "symbol_concentration_summary.csv",
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


def _load_joined(ledger_path: Path, touch_path: Path) -> pd.DataFrame:
    ledger = pd.read_parquet(ledger_path)
    ledger["code"] = ledger["code"].astype(str)
    touch_cols = [
        "code",
        "ymd",
        "target_ma",
        "ret_5b",
        "ret_20b",
        "max_drawdown_20b",
        "rebreak_ma20_5b",
        "rebreak_ma20_20b",
        "higher_high_made_5b",
        "severe_loss_flag_20b",
        "signal_high_touch_only_weak",
        "signal_gap_touch_fade",
        "signal_close_break_above_weak",
        "signal_close_break_above_strong",
    ]
    touch = pd.read_csv(touch_path, usecols=touch_cols)
    touch["code"] = touch["code"].astype(str)
    for col in [c for c in touch.columns if c.startswith("signal_") or c in {"rebreak_ma20_5b", "rebreak_ma20_20b", "higher_high_made_5b", "severe_loss_flag_20b"}]:
        touch[col] = touch[col].fillna(False).astype(bool)
    merged = ledger.merge(touch, left_on=["code", "bar_date"], right_on=["code", "ymd"], how="inner", validate="many_to_many")
    merged = merged[merged["is_open_at_bar"].fillna(False)].copy()
    merged["event_year"] = merged["bar_date"].astype(str).str.slice(0, 4).astype(int)
    merged["touch_event_id"] = merged["code"] + ":" + merged["bar_date"].astype(str) + ":" + merged["target_ma"]
    merged["signal_group"] = "other"
    merged.loc[merged["signal_close_break_above_strong"], "signal_group"] = "close_break_above_strong"
    merged.loc[merged["signal_close_break_above_weak"], "signal_group"] = "close_break_above_weak"
    merged.loc[merged["signal_gap_touch_fade"], "signal_group"] = "gap_touch_fade"
    merged.loc[merged["signal_high_touch_only_weak"], "signal_group"] = "high_touch_only_weak"
    merged["weak_signal"] = merged["signal_group"].isin(WEAK_SIGNALS)
    return merged


def _entry_final_returns(ledger: pd.DataFrame) -> pd.DataFrame:
    final = ledger.sort_values(["position_id", "bar_date"], kind="stable").groupby("position_id", as_index=False).tail(1)
    return final[["position_id", "close_price", "bar_date", "unrealized_return_pct"]].rename(
        columns={"close_price": "policy_final_close", "bar_date": "policy_final_bar_date", "unrealized_return_pct": "policy_final_unrealized_pct"}
    )


def _event_panel(joined: pd.DataFrame, ledger_path: Path) -> pd.DataFrame:
    ledger = pd.read_parquet(ledger_path)
    final = _entry_final_returns(ledger)
    panel = joined.merge(final, on="position_id", how="left", validate="many_to_one")
    panel["hold_incremental_ret"] = (pd.to_numeric(panel["policy_final_close"], errors="coerce") / pd.to_numeric(panel["close_price"], errors="coerce") - 1.0) * 100.0
    panel["current_policy_incremental_ret"] = panel["hold_incremental_ret"]
    panel.loc[panel["source_policy"].eq(HOLD_POLICY), "policy_role"] = "hold_baseline"
    panel.loc[panel["source_policy"].eq(CURRENT_POLICY), "policy_role"] = "current_replay_exit_policy"
    panel.loc[~panel["source_policy"].isin([HOLD_POLICY, CURRENT_POLICY]), "policy_role"] = "other_replay_policy"
    return panel


def _challenger_exit_mask(frame: pd.DataFrame, challenger: str) -> pd.Series:
    if challenger == "delayed_exit_ma20_rebreak_5":
        return frame["weak_signal"] & frame["rebreak_ma20_5b"]
    if challenger == "delayed_exit_failed_reacceleration_5":
        return frame["weak_signal"] & (((~frame["higher_high_made_5b"]) & (pd.to_numeric(frame["ret_5b"], errors="coerce") <= 0)) | frame["rebreak_ma20_5b"])
    raise ValueError(challenger)


def _metric(frame: pd.DataFrame, *, policy: str, target_ma: str = "ALL", signal_group: str = "ALL") -> dict[str, Any]:
    if policy == "hold_baseline":
        base = frame[frame["policy_role"].eq("hold_baseline")].copy()
        exit_mask = pd.Series(False, index=base.index)
        incremental = base["hold_incremental_ret"]
    elif policy == "current_replay_exit_policy":
        base = frame[frame["policy_role"].eq("current_replay_exit_policy")].copy()
        exit_mask = pd.Series(False, index=base.index)
        incremental = base["current_policy_incremental_ret"]
    else:
        base = frame[frame["policy_role"].eq("hold_baseline")].copy()
        exit_mask = _challenger_exit_mask(base, policy)
        incremental = base["hold_incremental_ret"].where(~exit_mask, 0.0)
    if target_ma != "ALL":
        base = base[base["target_ma"].eq(target_ma)].copy()
        incremental = incremental.loc[base.index]
        exit_mask = exit_mask.loc[base.index]
    if signal_group != "ALL":
        base = base[base["signal_group"].eq(signal_group)].copy()
        incremental = incremental.loc[base.index]
        exit_mask = exit_mask.loc[base.index]
    ret = pd.to_numeric(incremental, errors="coerce")
    hold_ret = pd.to_numeric(base["hold_incremental_ret"], errors="coerce")
    avoided = (-hold_ret.clip(upper=0)).where(exit_mask, 0.0)
    opportunity = hold_ret.clip(lower=0).where(exit_mask, 0.0)
    net_adv = ret - hold_ret
    big_winner = hold_ret.ge(10.0)
    severe = hold_ret.le(-10.0)
    return {
        "policy": policy,
        "target_ma": target_ma,
        "signal_group": signal_group,
        "event_count": int(len(base)),
        "unique_symbol_count": int(base["code"].nunique()) if not base.empty else 0,
        "mean_incremental_ret": None if ret.dropna().empty else float(ret.mean()),
        "median_incremental_ret": None if ret.dropna().empty else float(ret.median()),
        "hit_rate": None if ret.dropna().empty else float(ret.gt(0).mean()),
        "net_exit_advantage_vs_hold": None if net_adv.dropna().empty else float(net_adv.mean()),
        "avoided_loss_mean": None if avoided.dropna().empty else float(avoided.mean()),
        "opportunity_cost_mean": None if opportunity.dropna().empty else float(opportunity.mean()),
        "tail_loss_reduction": None if severe.dropna().empty else float((severe & exit_mask).mean()),
        "missed_big_winner_rate": None if big_winner.dropna().empty else float((big_winner & exit_mask).mean()),
        "winner_retention_rate": None if big_winner.dropna().empty else float((big_winner & ~exit_mask).mean()),
        "exit_trigger_rate": None if base.empty else float(exit_mask.mean()),
        "max_symbol_share": None if base.empty else float(base["code"].value_counts(normalize=True).iloc[0]),
        "max_month_share": None if base.empty else float(base["bar_date"].astype(str).str.slice(0, 6).value_counts(normalize=True).iloc[0]),
    }


def _summary(panel: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    policies = ("hold_baseline", "current_replay_exit_policy", *CHALLENGERS)
    for policy in policies:
        rows.append(_metric(panel, policy=policy))
        for target in TARGET_MAS:
            rows.append(_metric(panel, policy=policy, target_ma=target))
        for signal in (*WEAK_SIGNALS, "close_break_above_strong"):
            rows.append(_metric(panel, policy=policy, signal_group=signal))
    return pd.DataFrame(rows)


def _compare(summary: pd.DataFrame, first: str, second: str, name: str) -> dict[str, Any]:
    a = summary[(summary["policy"] == first) & (summary["target_ma"] == "ALL") & (summary["signal_group"] == "ALL")]
    b = summary[(summary["policy"] == second) & (summary["target_ma"] == "ALL") & (summary["signal_group"] == "ALL")]
    payload = {"name": name, "first_policy": first, "second_policy": second}
    if a.empty or b.empty:
        payload["status"] = "missing_group"
        return payload
    ar = a.iloc[0].to_dict()
    br = b.iloc[0].to_dict()
    deltas = {}
    for metric in ["mean_incremental_ret", "hit_rate", "net_exit_advantage_vs_hold", "tail_loss_reduction", "missed_big_winner_rate", "winner_retention_rate", "opportunity_cost_mean", "avoided_loss_mean", "max_symbol_share", "max_month_share"]:
        if ar.get(metric) is not None and br.get(metric) is not None:
            deltas[f"{metric}_delta"] = ar[metric] - br[metric]
    payload.update({"status": "ready" if ar["event_count"] >= 20 and br["event_count"] >= 20 else "insufficient_sample", "first_metrics": ar, "second_metrics": br, "deltas": deltas})
    return payload


def _contrasts(summary: pd.DataFrame) -> dict[str, Any]:
    rows = []
    for challenger in CHALLENGERS:
        rows.append(_compare(summary, challenger, "hold_baseline", f"{challenger}_vs_hold_baseline"))
        rows.append(_compare(summary, challenger, "current_replay_exit_policy", f"{challenger}_vs_current_replay_exit_policy"))
    rows.append(_compare(summary, "current_replay_exit_policy", "hold_baseline", "current_replay_exit_policy_vs_hold_baseline"))
    return {"axis_id": AXIS_ID, "required_contrasts": rows}


def _contrast_frame(contrast: dict[str, Any]) -> pd.DataFrame:
    rows = []
    for row in contrast["required_contrasts"]:
        flat = {k: v for k, v in row.items() if k not in {"first_metrics", "second_metrics", "deltas"}}
        flat.update({f"delta_{k}": v for k, v in row.get("deltas", {}).items()})
        rows.append(flat)
    return pd.DataFrame(rows)


def _yearly(panel: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for year, group in panel.groupby("event_year", sort=False):
        for policy in ("hold_baseline", "current_replay_exit_policy", *CHALLENGERS):
            row = _metric(group, policy=policy)
            row["event_year"] = int(year)
            row["sample_status"] = "sufficient" if row["event_count"] >= 10 else "insufficient_sample"
            rows.append(row)
    return pd.DataFrame(rows)


def _concentration(panel: pd.DataFrame) -> pd.DataFrame:
    rows = []
    base = panel[panel["policy_role"].eq("hold_baseline")]
    for key in ["code", "target_ma", "signal_group"]:
        counts = base[key].value_counts().reset_index()
        counts.columns = [key, "event_count"]
        counts["share"] = counts["event_count"] / max(1, len(base))
        counts["dimension"] = key
        rows.extend(counts.to_dict("records"))
    return pd.DataFrame(rows)


def _decision(contrast: dict[str, Any], summary: pd.DataFrame, yearly: pd.DataFrame, ledger_audit: dict[str, Any]) -> dict[str, Any]:
    ready = {row["name"]: row for row in contrast["required_contrasts"] if row.get("status") == "ready"}
    best = None
    reasons = []
    for challenger in CHALLENGERS:
        hold = ready.get(f"{challenger}_vs_hold_baseline")
        current = ready.get(f"{challenger}_vs_current_replay_exit_policy")
        row = summary[(summary["policy"] == challenger) & (summary["target_ma"] == "ALL") & (summary["signal_group"] == "ALL")]
        if hold and current and not row.empty:
            metrics = row.iloc[0].to_dict()
            beats_hold = hold["deltas"].get("mean_incremental_ret_delta", 0) > 0
            beats_current = current["deltas"].get("mean_incremental_ret_delta", 0) > 0
            net_positive = metrics.get("net_exit_advantage_vs_hold", 0) > 0
            tail_meaningful = metrics.get("tail_loss_reduction", 0) >= 0.03
            missed_ok = metrics.get("missed_big_winner_rate", 1) <= 0.08
            retention_ok = metrics.get("winner_retention_rate", 0) >= 0.05
            concentration_ok = (metrics.get("max_symbol_share") or 1) <= 0.25 and (metrics.get("max_month_share") or 1) <= 0.35
            yearly_ok = _yearly_ok(yearly, challenger)
            score = metrics.get("mean_incremental_ret") or -999
            payload = {
                "challenger": challenger,
                "beats_hold": beats_hold,
                "beats_current": beats_current,
                "net_positive": net_positive,
                "tail_meaningful": tail_meaningful,
                "missed_ok": missed_ok,
                "retention_ok": retention_ok,
                "concentration_ok": concentration_ok,
                "yearly_ok": yearly_ok,
                "metrics": metrics,
            }
            if beats_hold and beats_current and net_positive and tail_meaningful and missed_ok and retention_ok and concentration_ok and yearly_ok:
                reasons.append(payload)
                if best is None or score > best["metrics"]["mean_incremental_ret"]:
                    best = payload
    source_replay_specific = bool(ledger_audit.get("is_replay_specific_lifecycle"))
    if best and source_replay_specific:
        decision = "promote_to_replay_specific_exit_champion_candidate"
        reason = "delayed_exit_challenger_beats_hold_and_current_replay_policy_same_condition"
    elif reasons or source_replay_specific:
        decision = "keep_for_real_lifecycle_validation_next"
        reason = "replay_specific_result_or_source_scope_justifies_real_lifecycle_validation"
    else:
        decision = "drop"
        reason = "challenger_does_not_beat_hold_or_current_policy"
    return {
        "axis_id": AXIS_ID,
        "candidate_local_decision": decision,
        "session_aggregate_decision": decision,
        "authoritative_rollup_decision": decision,
        "reason": reason,
        "best_challenger": best,
        "champion_candidate_reasons": reasons,
        "source_lifecycle_type": "research_replay_specific" if source_replay_specific else "unknown",
        "same_condition": {
            "same_lifecycle": True,
            "same_period": True,
            "same_position_set": True,
            "same_entry_source_condition": True,
        },
        "sell_rule_promotion_allowed": False,
        "live_sell_rule_promotion_allowed": False,
        "meemee_reflection": False,
        "production_ranking_or_publish_change": False,
        "actual_position_sell_rule_allowed": False,
        "synthetic_lifecycle_promotion_allowed": False,
        "allowed_promotion_boundary": [
            "promote_to_replay_specific_exit_champion_candidate",
            "keep_for_replay_specific_champion",
            "keep_for_real_lifecycle_validation_next",
        ],
        "non_scope": ["no live sell rule promotion", "no MeeMee reflection", "no production ranking/publish", "no actual-position sell rule", "no synthetic lifecycle promotion"],
    }


def _yearly_ok(yearly: pd.DataFrame, challenger: str) -> bool:
    subset = yearly[(yearly["policy"] == challenger) & (yearly["sample_status"] == "sufficient")]
    if subset.empty:
        return False
    return int((subset["mean_incremental_ret"] > 0).sum()) >= max(1, len(subset) - 1)


def _definition(ledger_audit: dict[str, Any]) -> dict[str, Any]:
    return {
        "axis_id": AXIS_ID,
        "comparison_type": "same-condition replay-specific lifecycle champion challenger comparison",
        "hold_baseline": HOLD_POLICY,
        "current_replay_exit_policy": CURRENT_POLICY,
        "challengers": list(CHALLENGERS),
        "source_lifecycle_type": "research_replay_specific" if ledger_audit.get("is_replay_specific_lifecycle") else "unknown",
        "sell_rule_promotion_allowed": False,
        "replay_specific_champion_candidate_promotion_allowed": True,
    }


def run(args: argparse.Namespace) -> Path:
    out_dir = args.out_root / f"{_now_tag()}-{AXIS_ID.replace('_', '-')}"
    out_dir.mkdir(parents=True, exist_ok=False)
    ledger_audit = json.loads(args.ledger_audit.read_text(encoding="utf-8"))
    joined = _load_joined(args.ledger, args.touch_events)
    panel = _event_panel(joined, args.ledger)
    summary = _summary(panel)
    contrast = _contrasts(summary)
    yearly = _yearly(panel)
    concentration = _concentration(panel)
    decision = _decision(contrast, summary, yearly, ledger_audit)
    input_audit = {
        "axis_id": AXIS_ID,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "ledger": str(args.ledger),
        "ledger_audit": str(args.ledger_audit),
        "touch_events": str(args.touch_events),
        "joined_rows": int(len(panel)),
        "hold_baseline_touch_rows": int((panel["policy_role"] == "hold_baseline").sum()),
        "current_policy_touch_rows": int((panel["policy_role"] == "current_replay_exit_policy").sum()),
        "unique_touch_events": int(panel[["code", "bar_date", "target_ma"]].drop_duplicates().shape[0]),
        "unique_positions": int(panel["position_id"].nunique()),
        "source_lifecycle_type": "research_replay_specific",
        "same_lifecycle": True,
        "same_period": True,
        "same_position_set": True,
        "same_entry_source_condition": True,
        "sell_rule_promotion_allowed": False,
        "runtime_db_write": False,
        "meemee_reflection": False,
        "ranking_change": False,
        "publish": False,
    }
    _write_json(out_dir / "input_audit.json", input_audit)
    _write_json(out_dir / "retest_definition.json", _definition(ledger_audit))
    panel.to_csv(out_dir / "position_touch_retest_events.csv", index=False, encoding="utf-8")
    summary.to_csv(out_dir / "policy_comparison_summary.csv", index=False, encoding="utf-8")
    _write_json(out_dir / "challenger_contrast.json", contrast)
    _contrast_frame(contrast).to_csv(out_dir / "challenger_contrast.csv", index=False, encoding="utf-8")
    concentration.to_csv(out_dir / "symbol_concentration_summary.csv", index=False, encoding="utf-8")
    yearly.to_csv(out_dir / "yearly_stability_summary.csv", index=False, encoding="utf-8")
    panel.head(5000).to_csv(out_dir / "candidate_examples.csv", index=False, encoding="utf-8")
    _write_json(out_dir / "research_decision.json", decision)
    missing = [name for name in REQUIRED if name != "_ARTIFACT_COMPLETE.json" and not (out_dir / name).exists()]
    _write_json(out_dir / "_ARTIFACT_COMPLETE.json", {"axis_id": AXIS_ID, "status": "complete" if not missing else "incomplete", "missing_artifacts": missing, "authoritative_result": str(out_dir / "research_decision.json"), "generated_at_utc": datetime.now(timezone.utc).isoformat()})
    if missing:
        raise RuntimeError(f"missing artifacts: {missing}")
    return out_dir


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="TRADEX replay-specific MA touch position lifecycle exit retest v2.")
    parser.add_argument("--out-root", type=Path, default=DEFAULT_OUT_ROOT)
    parser.add_argument("--ledger", type=Path, default=DEFAULT_LEDGER)
    parser.add_argument("--ledger-audit", type=Path, default=DEFAULT_LEDGER_AUDIT)
    parser.add_argument("--touch-events", type=Path, default=DEFAULT_TOUCH_EVENTS)
    return parser.parse_args()


def main() -> None:
    print(run(parse_args()))


if __name__ == "__main__":
    main()
