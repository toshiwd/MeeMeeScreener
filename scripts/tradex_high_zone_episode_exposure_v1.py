from __future__ import annotations

import argparse
import json
import math
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

try:
    from scripts.tradex_high_zone_initial_exposure_v1 import HORIZONS, load_events, metrics, price_band, stability
except ModuleNotFoundError:
    from tradex_high_zone_initial_exposure_v1 import HORIZONS, load_events, metrics, price_band, stability


AXIS_ID = "tradex_high_zone_episode_exposure_v1"
FAMILY = "high_zone_climax"
POLICIES = ("all100_reference", "high_price25_champion", "low_price_episode25", "combined_episode25", "combined_episode50")


def _value(row: pd.Series, name: str) -> float | None:
    value = row.get(name)
    return None if value is None or pd.isna(value) else float(value)


def annotate_episode(events: pd.DataFrame) -> pd.DataFrame:
    result = events.sort_values(["code", "signal_ymd"]).copy()
    result["signal_date"] = pd.to_datetime(result.signal_ymd.astype(str))
    result["prior_signal_date"] = result.groupby("code").signal_date.shift()
    result["repeat_within_35d"] = (result.signal_date - result.prior_signal_date).dt.days.le(35)
    result["gap1"] = result.o1 / result.c - 1.0
    result["low_price_episode"] = (result.c < 900) & (result.repeat_within_35d | (result.gap1 >= 0.15))
    return result


def exposure(row: pd.Series, policy: str) -> float:
    high_price = float(row["c"]) >= 10000
    episode = bool(row["low_price_episode"])
    if policy == "all100_reference": return 1.0
    if policy == "high_price25_champion": return 0.25 if high_price else 1.0
    if policy == "low_price_episode25": return 0.25 if episode else 1.0
    if policy == "combined_episode25": return 0.25 if high_price or episode else 1.0
    if high_price: return 0.25
    return 0.50 if episode else 1.0


def replay(events: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for _, source in annotate_episode(events).iterrows():
        open1 = _value(source, "o1")
        for policy in POLICIES:
            weight = exposure(source, policy)
            record: dict[str, Any] = {
                "family": FAMILY, "code": str(source["code"]), "signal_ymd": int(source["signal_ymd"]),
                "signal_close": float(source["c"]), "price_band": price_band(float(source["c"])),
                "continuation_risk_high": bool(source["low_price_episode"]),
                "repeat_within_35d": bool(source["repeat_within_35d"]), "gap1": float(source["gap1"]),
                "low_price_episode": bool(source["low_price_episode"]), "policy": policy,
                "state": "entry" if open1 else "unavailable", "exposure": weight,
                "entry_offset": 1 if open1 else None, "entry_price": open1, "wait_days": 1 if open1 else None,
            }
            for horizon in HORIZONS:
                exit_close = _value(source, f"c{1 + horizon}")
                highs = [_value(source, f"h{i}") for i in range(1, 2 + horizon)]
                highs = [x for x in highs if x is not None]
                record[f"ret{horizon}"] = None if open1 is None or exit_close is None else weight * (1 - exit_close / open1)
                record[f"mae{horizon}"] = None if open1 is None or not highs else weight * (1 - max(highs) / open1)
            rows.append(record)
    return pd.DataFrame(rows)


def _json_ready(value: Any) -> Any:
    if isinstance(value, dict): return {str(k): _json_ready(v) for k,v in value.items()}
    if isinstance(value, list): return [_json_ready(v) for v in value]
    if isinstance(value, Path): return str(value)
    if hasattr(value, "item"): return _json_ready(value.item())
    if isinstance(value, float) and (math.isnan(value) or math.isinf(value)): return None
    return value


def run(db_path: Path, output_root: Path, start_ymd: int, end_ymd: int) -> Path:
    events = load_events(db_path, start_ymd, end_ymd); events = events[events.family == FAMILY].copy(); ledger = replay(events)
    results = {p: metrics(ledger[ledger.policy == p]) for p in POLICIES}; stable = {p: stability(ledger[ledger.policy == p]) for p in POLICIES}
    reference, champion = results["all100_reference"], results["high_price25_champion"]
    challengers = {}
    for policy in POLICIES[2:]:
        item = results[policy]
        checks = {
            "participation_capture_100pct": item["participation_capture_rate"] == 1.0,
            "mean_ret10_not_worse_than_champion": item["h10"]["mean"] >= champion["h10"]["mean"],
            "pf10_not_worse_than_champion": (item["h10"]["profit_factor"] or 0) >= (champion["h10"]["profit_factor"] or 0),
            "loss10_rate_not_worse_than_champion": item["h10"]["loss_le_minus10_rate"] <= champion["h10"]["loss_le_minus10_rate"],
            "positive_year_rate_at_least_75pct": stable[policy]["positive_mean_ret10_year_rate"] >= 0.75,
            "positive_price_band_rate_at_least_75pct": stable[policy]["positive_mean_ret10_price_band_rate"] >= 0.75,
        }
        decision = "keep" if all(checks.values()) else ("hold" if checks["participation_capture_100pct"] and checks["pf10_not_worse_than_champion"] and checks["loss10_rate_not_worse_than_champion"] else "drop")
        challengers[policy] = {"candidate_local_decision": decision, "checks": checks, "metrics": item, "stability": stable[policy]}
    keepers = [p for p,x in challengers.items() if x["candidate_local_decision"] == "keep"]
    holds = [p for p,x in challengers.items() if x["candidate_local_decision"] == "hold"]
    pool = keepers or holds; leader = max(pool, key=lambda p: results[p]["h10"]["mean"]) if pool else None; decision = "keep" if keepers else ("hold" if holds else "drop")
    episode_rows = ledger[(ledger.policy == "all100_reference") & ledger.low_price_episode]
    payload = {
        "schema_version": f"{AXIS_ID}.compare.v1", "artifact_role": "authoritative", "research_phase": "effectiveness_judgment",
        "fixed_evaluation_conditions": {"universe": "same high_zone_climax top5/day signals", "period": {"start_ymd": start_ymd, "end_ymd": end_ymd}, "changed_axis": "initial exposure for low-price continuation episodes only", "episode_definition": "signal close under 900 AND (same-code high-zone signal within 35 calendar days OR next-open gap at least 15pct)", "horizons": list(HORIZONS), "costs": "ignored_by_user_request", "runtime_db_write": False, "meemee_reflection": False},
        "source": {"db_path": str(db_path), "event_count": int(len(events)), "ledger_count": int(len(ledger)), "episode_count": int(len(episode_rows)), "episode_tail_loss_count_at_full_exposure": int((episode_rows.ret10 <= -0.10).sum())},
        "reference": {"policy": "all100_reference", "metrics": reference, "stability": stable["all100_reference"]},
        "champion": {"policy": "high_price25_champion", "metrics": champion, "stability": stable["high_price25_champion"]},
        "challengers": challengers,
        "observed_branching": {"changed_top5_members_count": 0, "changed_top10_members_count": 0, "changed_rank_count": 0, "selection_divergence_reason": "membership and timing fixed; only initial exposure changes for typed episodes"},
        "decision": {"candidate_local_decision": decision, "session_aggregate_decision": decision, "authoritative_rollup_decision": f"{decision}_high_zone_episode_exposure", "selected_policy": leader if decision == "keep" else None, "research_leader": leader, "reason_type": "episode_sizing_beats_price_champion" if decision == "keep" else ("risk_quality_improves_but_mean_gate_fails" if decision == "hold" else "episode_sizing_does_not_improve_champion")},
        "runtime_db_write": False, "production_ranking_changed": False, "meemee_changed": False,
    }
    run_dir = output_root / f"{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}-{AXIS_ID}"; run_dir.mkdir(parents=True, exist_ok=False)
    ledger.to_parquet(run_dir / "episode_exposure_ledger.parquet", index=False); (run_dir / "compare.json").write_text(json.dumps(_json_ready(payload), ensure_ascii=False, indent=2) + "\n", encoding="utf-8"); (run_dir / "_ARTIFACT_COMPLETE.json").write_text(json.dumps({"status":"complete","required_files":["compare.json","episode_exposure_ledger.parquet","_ARTIFACT_COMPLETE.json"]}, indent=2) + "\n", encoding="utf-8"); return run_dir


def main() -> int:
    parser=argparse.ArgumentParser(); parser.add_argument("--db-path",type=Path,required=True); parser.add_argument("--output-root",type=Path,default=Path(r"G:\Tradex\tradex_high_zone_episode_exposure_v1")); parser.add_argument("--start-ymd",type=int,default=20160101); parser.add_argument("--end-ymd",type=int,default=20260617); args=parser.parse_args(); print(run(args.db_path,args.output_root,args.start_ymd,args.end_ymd)); return 0


if __name__ == "__main__": raise SystemExit(main())
