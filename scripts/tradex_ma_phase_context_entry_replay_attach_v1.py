from __future__ import annotations

import argparse
import json
import math
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd


AXIS_ID = "ma_phase_context_entry_replay_attach_v1"
DEFAULT_REPLAY_ROOT = Path("G:/Tradex/current_buyable_historical_operational_replay_v1/20260526T014356Z-current-buyable-historical-operational-replay-v1")
DEFAULT_REPLAY_ROWS = DEFAULT_REPLAY_ROOT / "historical_operational_replay_rows.csv"
DEFAULT_PHASE_ROOT = Path("G:/Tradex/ma_phase_feature_base_v1/20260603T121215Z-ma-phase-feature-base-v1")
DEFAULT_PHASE_FEATURES = DEFAULT_PHASE_ROOT / "ma_phase_features.parquet"
DEFAULT_DISCOVERY_DECISION = Path("G:/Tradex/ma_phase_candidate_discovery_v1/20260604T011602Z-ma-phase-candidate-discovery-v1/final_research_decision.json")
DEFAULT_OUT_ROOT = Path("G:/Tradex/ma_phase_context_entry_replay_attach_v1")
REQUIRED_OUTPUTS = (
    "final_research_decision.json",
    "compare.json",
    "feature_join_audit.json",
    "candidate_branching_summary.json",
    "replacement_quality_summary.json",
    "context_attached_replay_rows.csv",
    "report.md",
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


def _rate(series: pd.Series) -> float | None:
    valid = series.dropna()
    if valid.empty:
        return None
    return float(valid.astype(bool).mean())


def _mean(series: pd.Series) -> float | None:
    valid = pd.to_numeric(series, errors="coerce").dropna()
    if valid.empty:
        return None
    return float(valid.mean())


def _median(series: pd.Series) -> float | None:
    valid = pd.to_numeric(series, errors="coerce").dropna()
    if valid.empty:
        return None
    return float(valid.median())


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _contract_check(replay_rows: Path, phase_features: Path, discovery_decision: Path) -> tuple[bool, list[str], dict[str, Any]]:
    checked: dict[str, Any] = {
        "files_checked": [str(replay_rows), str(phase_features), str(discovery_decision)],
        "required_replay_columns": ["as_of_date", "code", "fresh_runtime_research_watch_rank", "ret20"],
        "required_phase_columns": [
            "code",
            "ymd",
            "bars_since_cross_above_ma7",
            "upper_resistance_bucket",
            "lower_support_bucket",
            "consecutive_bars_above_ma60",
            "rebreak_ma7_7b",
            "rebreak_ma60_20b",
            "max_drawdown_20b",
            "ret_20b",
        ],
    }
    missing: list[str] = []
    for path in [replay_rows, phase_features, discovery_decision]:
        if not path.exists():
            missing.append(f"missing_file:{path}")
    if missing:
        return False, missing, checked

    replay_head = pd.read_csv(replay_rows, nrows=5)
    replay_cols = set(replay_head.columns)
    missing.extend([f"missing_replay_column:{c}" for c in checked["required_replay_columns"] if c not in replay_cols])
    phase_cols = set(pd.read_parquet(phase_features, columns=None).columns)
    missing.extend([f"missing_phase_column:{c}" for c in checked["required_phase_columns"] if c not in phase_cols])
    decision = _read_json(discovery_decision)
    checked["discovery_decision"] = decision.get("authoritative_rollup_decision")
    checked["discovery_best_candidate"] = decision.get("best_candidate", {}).get("best_candidate")
    if decision.get("authoritative_rollup_decision") != "keep_as_context_feature":
        missing.append("unexpected_discovery_decision")
    return not missing, missing, checked


def _load_joined(replay_rows: Path, phase_features: Path) -> tuple[pd.DataFrame, dict[str, Any]]:
    replay = pd.read_csv(replay_rows)
    replay["code"] = replay["code"].astype(str)
    replay["as_of_date"] = replay["as_of_date"].astype(str)
    replay["baseline_rank"] = pd.to_numeric(replay["fresh_runtime_research_watch_rank"], errors="coerce")
    replay["ret20"] = pd.to_numeric(replay["ret20"], errors="coerce")

    phase_cols = [
        "code",
        "ymd",
        "close_above_ma7",
        "cross_above_ma7_today",
        "bars_since_cross_above_ma7",
        "bars_since_cross_below_ma7",
        "close_above_ma20",
        "upper_resistance_bucket",
        "lower_support_bucket",
        "is_lower_shadow_long",
        "is_hammer_like",
        "is_large_bull_body",
        "is_engulfing_bull",
        "close_above_ma60",
        "consecutive_bars_above_ma60",
        "is_large_bear_body",
        "is_upper_shadow_long",
        "is_shooting_star_like",
        "rebreak_ma7_7b",
        "rebreak_ma60_20b",
        "ret_20b",
        "max_drawdown_20b",
    ]
    phase = pd.read_parquet(phase_features, columns=phase_cols)
    phase["code"] = phase["code"].astype(str)
    phase["ymd"] = phase["ymd"].astype(str)
    phase = phase.drop_duplicates(["code", "ymd"], keep="last")
    joined = replay.merge(phase, left_on=["code", "as_of_date"], right_on=["code", "ymd"], how="left", validate="many_to_one")
    joined["feature_joined"] = joined["ymd"].notna()

    joined["ma7_phase_3_5_no_light_upper"] = (
        joined["close_above_ma7"].fillna(False)
        & pd.to_numeric(joined["bars_since_cross_above_ma7"], errors="coerce").between(3, 5, inclusive="both")
        & joined["upper_resistance_bucket"].isin(["none_near", "light_resistance"])
    )
    joined["ma7_pullback_reclaim_support_bull"] = (
        joined["cross_above_ma7_today"].fillna(False)
        & pd.to_numeric(joined["bars_since_cross_below_ma7"], errors="coerce").between(1, 5, inclusive="both")
        & joined["close_above_ma20"].fillna(False)
        & joined["upper_resistance_bucket"].isin(["none_near", "light_resistance"])
        & joined["lower_support_bucket"].isin(["light_support", "medium_support", "heavy_support"])
        & (
            joined["is_lower_shadow_long"].fillna(False)
            | joined["is_hammer_like"].fillna(False)
            | joined["is_large_bull_body"].fillna(False)
            | joined["is_engulfing_bull"].fillna(False)
        )
    )
    joined["ma60_run_20_plus_no_light_upper"] = (
        joined["close_above_ma60"].fillna(False)
        & (pd.to_numeric(joined["consecutive_bars_above_ma60"], errors="coerce") >= 20)
        & joined["upper_resistance_bucket"].isin(["none_near", "light_resistance"])
    )
    joined["ma60_failure_guard_heavy_weak_candle"] = (
        (pd.to_numeric(joined["consecutive_bars_above_ma60"], errors="coerce") >= 20)
        & joined["upper_resistance_bucket"].isin(["medium_resistance", "heavy_resistance"])
        & (
            joined["is_large_bear_body"].fillna(False)
            | joined["is_upper_shadow_long"].fillna(False)
            | joined["is_shooting_star_like"].fillna(False)
        )
    )
    positive_cols = ["ma7_phase_3_5_no_light_upper", "ma7_pullback_reclaim_support_bull", "ma60_run_20_plus_no_light_upper"]
    joined["ma_phase_positive_context_count"] = joined[positive_cols].sum(axis=1)
    joined["ma_phase_bad_pick_guard"] = joined["ma60_failure_guard_heavy_weak_candle"].fillna(False)
    joined["context_rank"] = (
        joined.sort_values(
            ["as_of_date", "ma_phase_bad_pick_guard", "ma_phase_positive_context_count", "baseline_rank", "code"],
            ascending=[True, True, False, True, True],
            kind="stable",
        )
        .groupby("as_of_date")
        .cumcount()
        + 1
    )
    audit = {
        "input_replay_rows": int(len(replay)),
        "input_phase_rows": int(len(phase)),
        "joined_rows": int(len(joined)),
        "feature_joined_rows": int(joined["feature_joined"].sum()),
        "feature_join_coverage": float(joined["feature_joined"].mean()) if len(joined) else None,
        "unique_dates": int(joined["as_of_date"].nunique()),
        "unique_symbols": int(joined["code"].nunique()),
        "join_keys": {"replay": ["code", "as_of_date"], "phase": ["code", "ymd"]},
        "baseline_rank_column": "fresh_runtime_research_watch_rank",
        "context_feature_only": True,
        "baseline_semantics_changed": False,
    }
    return joined, audit


def _topk_metrics(rows: pd.DataFrame, rank_col: str, topk: int, label: str) -> dict[str, Any]:
    top = rows[rows[rank_col] <= topk].copy()
    return {
        "label": label,
        "topk": topk,
        "row_count": int(len(top)),
        "unique_date_count": int(top["as_of_date"].nunique()),
        "unique_symbol_count": int(top["code"].nunique()),
        "mean_ret20": _mean(top["ret20"]),
        "median_ret20": _median(top["ret20"]),
        "hit_rate": _rate(top["ret20"] > 0),
        "bottom_loss_rate_20d": _rate(top["ret20"] <= -0.05),
        "severe_loss_rate_20d": _rate(top["ret20"] <= -0.10),
        "mean_max_drawdown_20d": _mean(top["max_drawdown_20b"]),
        "median_max_drawdown_20d": _median(top["max_drawdown_20b"]),
        "rebreak_ma7_7b_rate": _rate(top["rebreak_ma7_7b"]),
        "rebreak_ma60_20b_rate": _rate(top["rebreak_ma60_20b"]),
        "phase_positive_context_rate": _rate(top["ma_phase_positive_context_count"] > 0),
        "bad_pick_guard_rate": _rate(top["ma_phase_bad_pick_guard"]),
    }


def _compare(rows: pd.DataFrame) -> dict[str, Any]:
    rows = rows.copy()
    rows["baseline_top_rank"] = rows.groupby("as_of_date")["baseline_rank"].rank(method="first")
    metrics = []
    deltas = []
    for topk in [5, 10, 20]:
        baseline = _topk_metrics(rows, "baseline_top_rank", topk, "baseline_current_replay_rank")
        challenger = _topk_metrics(rows, "context_rank", topk, "ma_phase_context_challenger")
        metrics.extend([baseline, challenger])
        delta = {"topk": topk}
        for key in [
            "mean_ret20",
            "median_ret20",
            "hit_rate",
            "bottom_loss_rate_20d",
            "severe_loss_rate_20d",
            "mean_max_drawdown_20d",
            "rebreak_ma7_7b_rate",
            "rebreak_ma60_20b_rate",
            "phase_positive_context_rate",
            "bad_pick_guard_rate",
        ]:
            if challenger.get(key) is not None and baseline.get(key) is not None:
                delta[f"{key}_delta"] = challenger[key] - baseline[key]
        deltas.append(delta)
    return {"metrics": metrics, "deltas": deltas}


def _branching(rows: pd.DataFrame) -> tuple[dict[str, Any], dict[str, Any]]:
    rows = rows.copy()
    rows["baseline_top_rank"] = rows.groupby("as_of_date")["baseline_rank"].rank(method="first")
    rows["baseline_in_top5"] = rows["baseline_top_rank"] <= 5
    rows["baseline_in_top10"] = rows["baseline_top_rank"] <= 10
    rows["baseline_in_top20"] = rows["baseline_top_rank"] <= 20
    rows["challenger_in_top5"] = rows["context_rank"] <= 5
    rows["challenger_in_top10"] = rows["context_rank"] <= 10
    rows["challenger_in_top20"] = rows["context_rank"] <= 20
    rows["rank_changed"] = rows["baseline_top_rank"] != rows["context_rank"]

    date_rows = []
    replacement_rows = []
    for date, g in rows.groupby("as_of_date", sort=True):
        row = {"as_of_date": date, "candidate_count": int(len(g)), "rank_changed_count": int(g["rank_changed"].sum())}
        for topk in [5, 10, 20]:
            b = set(g.loc[g[f"baseline_in_top{topk}"], "code"])
            c = set(g.loc[g[f"challenger_in_top{topk}"], "code"])
            removed = b - c
            added = c - b
            row[f"changed_top{topk}_members_count"] = len(removed | added)
            removed_rows = g[g["code"].isin(removed)].copy()
            added_rows = g[g["code"].isin(added)].copy()
            row[f"bad_pick_removed_top{topk}_count"] = int((removed_rows["ret20"] <= -0.05).sum())
            if len(removed_rows) or len(added_rows):
                replacement_rows.append(
                    {
                        "as_of_date": date,
                        "topk": topk,
                        "removed_count": int(len(removed_rows)),
                        "added_count": int(len(added_rows)),
                        "removed_mean_ret20": _mean(removed_rows["ret20"]),
                        "added_mean_ret20": _mean(added_rows["ret20"]),
                        "replacement_ret20_advantage": (
                            _mean(added_rows["ret20"]) - _mean(removed_rows["ret20"])
                            if _mean(added_rows["ret20"]) is not None and _mean(removed_rows["ret20"]) is not None
                            else None
                        ),
                        "removed_bad_pick_count": int((removed_rows["ret20"] <= -0.05).sum()),
                        "added_bad_pick_count": int((added_rows["ret20"] <= -0.05).sum()),
                    }
                )
        date_rows.append(row)
    branch_frame = pd.DataFrame(date_rows)
    repl_frame = pd.DataFrame(replacement_rows)
    summary = {
        "changed_top5_members_count": int(branch_frame["changed_top5_members_count"].sum()) if not branch_frame.empty else 0,
        "changed_top10_members_count": int(branch_frame["changed_top10_members_count"].sum()) if not branch_frame.empty else 0,
        "changed_top20_members_count": int(branch_frame["changed_top20_members_count"].sum()) if not branch_frame.empty else 0,
        "changed_rank_count": int(rows["rank_changed"].sum()),
        "dates_with_top5_branching": int((branch_frame["changed_top5_members_count"] > 0).sum()) if not branch_frame.empty else 0,
        "dates_with_top10_branching": int((branch_frame["changed_top10_members_count"] > 0).sum()) if not branch_frame.empty else 0,
        "bad_pick_removal_count_top5": int(branch_frame["bad_pick_removed_top5_count"].sum()) if not branch_frame.empty else 0,
        "bad_pick_removal_count_top10": int(branch_frame["bad_pick_removed_top10_count"].sum()) if not branch_frame.empty else 0,
        "bad_pick_removal_count_top20": int(branch_frame["bad_pick_removed_top20_count"].sum()) if not branch_frame.empty else 0,
        "branching_by_date": date_rows,
    }
    replacement = {
        "replacement_row_count": int(len(repl_frame)),
        "topk_summary": [],
    }
    for topk in [5, 10, 20]:
        part = repl_frame[repl_frame["topk"] == topk] if not repl_frame.empty else pd.DataFrame()
        replacement["topk_summary"].append(
            {
                "topk": topk,
                "replacement_event_count": int(len(part)),
                "mean_replacement_ret20_advantage": _mean(part["replacement_ret20_advantage"]) if not part.empty else None,
                "median_replacement_ret20_advantage": _median(part["replacement_ret20_advantage"]) if not part.empty else None,
                "removed_bad_pick_count": int(part["removed_bad_pick_count"].sum()) if not part.empty else 0,
                "added_bad_pick_count": int(part["added_bad_pick_count"].sum()) if not part.empty else 0,
            }
        )
    return summary, replacement


def _affected_slices(rows: pd.DataFrame) -> dict[str, Any]:
    slices = {}
    for col in [
        "ma7_phase_3_5_no_light_upper",
        "ma7_pullback_reclaim_support_bull",
        "ma60_run_20_plus_no_light_upper",
        "ma60_failure_guard_heavy_weak_candle",
    ]:
        part = rows[rows[col].fillna(False)]
        slices[col] = {
            "row_count": int(len(part)),
            "unique_symbol_count": int(part["code"].nunique()),
            "mean_ret20": _mean(part["ret20"]),
            "hit_rate": _rate(part["ret20"] > 0),
            "bottom_loss_rate_20d": _rate(part["ret20"] <= -0.05),
            "severe_loss_rate_20d": _rate(part["ret20"] <= -0.10),
            "mean_max_drawdown_20d": _mean(part["max_drawdown_20b"]),
            "rebreak_ma7_7b_rate": _rate(part["rebreak_ma7_7b"]),
            "rebreak_ma60_20b_rate": _rate(part["rebreak_ma60_20b"]),
        }
    return slices


def _decision(compare: dict[str, Any], branching: dict[str, Any], replacement: dict[str, Any], audit: dict[str, Any]) -> tuple[str, str]:
    if audit.get("feature_join_coverage", 0) < 0.95:
        return "hold", "insufficient_feature_join_coverage"
    top10_delta = next((d for d in compare["deltas"] if d["topk"] == 10), {})
    top20_delta = next((d for d in compare["deltas"] if d["topk"] == 20), {})
    branching_happened = branching["changed_top10_members_count"] > 0 or branching["changed_top20_members_count"] > 0
    if not branching_happened:
        return "drop", "no_meaningful_branching"
    ret_improved = (top10_delta.get("mean_ret20_delta", 0) > 0) and (top20_delta.get("mean_ret20_delta", 0) >= 0)
    risk_not_worse = top10_delta.get("severe_loss_rate_20d_delta", 0) <= 0 and top10_delta.get("mean_max_drawdown_20d_delta", 0) <= 0
    rebreak_improved = top10_delta.get("rebreak_ma7_7b_rate_delta", 0) < 0 or top10_delta.get("rebreak_ma60_20b_rate_delta", 0) < 0
    replacement_ok = any(
        item["topk"] == 10 and item.get("mean_replacement_ret20_advantage") is not None and item["mean_replacement_ret20_advantage"] > 0
        for item in replacement["topk_summary"]
    )
    if ret_improved and risk_not_worse and replacement_ok:
        return "keep_for_candidate_pretest_next", "topk_quality_improved_with_branching_and_no_unacceptable_risk_worsening"
    if rebreak_improved or branching["bad_pick_removal_count_top10"] > 0:
        return "keep_as_context_feature", "context_improves_rebreak_or_bad_pick_diagnostics_but_not_enough_for_candidate_promotion"
    return "drop", "return_or_risk_quality_did_not_improve"


def _write_blocked(out_dir: Path, checked: dict[str, Any], missing: list[str]) -> None:
    out_dir.mkdir(parents=True, exist_ok=False)
    payload = {
        "axis_id": AXIS_ID,
        "authoritative_rollup_decision": "hold",
        "reason": "existing_pipeline_cannot_safely_accept_ma_phase_context",
        "checked": checked,
        "missing_contract": missing,
        "minimal_required_contract_change": [
            "candidate rows must expose stable code/date join keys",
            "candidate rows must expose a baseline rank suitable for same-condition top-K comparison",
            "candidate rows must expose ret20 or equivalent same-horizon outcome",
        ],
        "non_scope": [
            "no MeeMee reflection",
            "no runtime DB write",
            "no ranking change",
            "no publish",
            "no production candidate generation change",
            "no live buy/sell rule",
            "no frozen exit champion change",
        ],
    }
    _write_json(out_dir / "final_research_decision.json", payload)
    _write_json(out_dir / "feature_join_audit.json", payload)
    _write_json(out_dir / "_ARTIFACT_COMPLETE.json", {"axis_id": AXIS_ID, "status": "blocked_confirmation_artifact", "required_files_present": False})


def run(args: argparse.Namespace) -> Path:
    out_dir = args.out_root / f"{_now_tag()}-{AXIS_ID.replace('_', '-')}"
    ok, missing, checked = _contract_check(args.replay_rows, args.phase_features, args.discovery_decision)
    if not ok:
        _write_blocked(out_dir, checked, missing)
        return out_dir

    out_dir.mkdir(parents=True, exist_ok=False)
    joined, audit = _load_joined(args.replay_rows, args.phase_features)
    compare = _compare(joined)
    branching, replacement = _branching(joined)
    slices = _affected_slices(joined)
    decision, reason = _decision(compare, branching, replacement, audit)

    joined_out = joined[
        [
            "as_of_date",
            "code",
            "baseline_rank",
            "context_rank",
            "ret20",
            "max_drawdown_20b",
            "rebreak_ma7_7b",
            "rebreak_ma60_20b",
            "ma7_phase_3_5_no_light_upper",
            "ma7_pullback_reclaim_support_bull",
            "ma60_run_20_plus_no_light_upper",
            "ma60_failure_guard_heavy_weak_candle",
            "ma_phase_positive_context_count",
            "ma_phase_bad_pick_guard",
        ]
    ].copy()
    joined_out.to_csv(out_dir / "context_attached_replay_rows.csv", index=False, encoding="utf-8")

    _write_json(out_dir / "feature_join_audit.json", {"axis_id": AXIS_ID, **checked, **audit})
    _write_json(out_dir / "compare.json", {"axis_id": AXIS_ID, "same_condition": True, "compare": compare, "affected_candidate_slices": slices})
    _write_json(out_dir / "candidate_branching_summary.json", {"axis_id": AXIS_ID, **branching})
    _write_json(out_dir / "replacement_quality_summary.json", {"axis_id": AXIS_ID, **replacement})

    decision_payload = {
        "axis_id": AXIS_ID,
        "authoritative_rollup_decision": decision,
        "reason": reason,
        "candidate_family": "ma_phase_context_attached_to_existing_entry_replay_v1",
        "what_was_attached": [
            "ma7_phase_3_5_no_light_upper",
            "ma7_pullback_reclaim_support_bull",
            "ma60_run_20_plus_no_light_upper",
            "ma60_failure_guard_heavy_weak_candle",
        ],
        "what_was_not_changed": [
            "existing replay rows",
            "baseline fresh_runtime_research_watch_rank semantics",
            "universe",
            "period",
            "top-K evaluation horizons",
            "regime condition",
            "cost/slippage assumptions",
            "MeeMee",
            "runtime DB",
            "ranking",
            "publish",
            "production candidate generation",
            "live buy/sell rules",
            "frozen replay-specific exit champion",
        ],
        "authoritative_sources": {
            "replay_rows": str(args.replay_rows),
            "phase_features": str(args.phase_features),
            "discovery_decision": str(args.discovery_decision),
        },
        "feature_join_audit": audit,
        "topk_compare_deltas": compare["deltas"],
        "branching_summary": {k: v for k, v in branching.items() if k != "branching_by_date"},
        "replacement_quality_summary": replacement["topk_summary"],
        "affected_candidate_slices": slices,
        "boundary_flags": {
            "tradex_only": True,
            "runtime_db_write": False,
            "meemee_reflection": False,
            "ranking_change": False,
            "publish": False,
            "production_candidate_generation_change": False,
            "live_rule_promotion_allowed": False,
            "frozen_exit_champion_changed": False,
        },
    }
    _write_json(out_dir / "final_research_decision.json", decision_payload)
    report = [
        f"# {AXIS_ID}",
        "",
        f"- authoritative_rollup_decision: `{decision}`",
        f"- reason: `{reason}`",
        f"- replay rows: `{len(joined)}`",
        f"- feature join coverage: `{audit['feature_join_coverage']}`",
        f"- changed_top10_members_count: `{branching['changed_top10_members_count']}`",
        f"- changed_rank_count: `{branching['changed_rank_count']}`",
        "",
        "Markdown is derived only. JSON files are authoritative.",
        "",
    ]
    (out_dir / "report.md").write_text("\n".join(report), encoding="utf-8")
    _write_json(
        out_dir / "_ARTIFACT_COMPLETE.json",
        {
            "axis_id": AXIS_ID,
            "status": "complete",
            "generated_at_utc": datetime.now(timezone.utc).isoformat(),
            "required_files": list(REQUIRED_OUTPUTS),
            "required_files_present": all((out_dir / name).exists() for name in REQUIRED_OUTPUTS if name != "_ARTIFACT_COMPLETE.json"),
        },
    )
    return out_dir


def main() -> None:
    parser = argparse.ArgumentParser(description="Attach MA phase context to existing TRADEX entry replay candidates as feature-only challenger.")
    parser.add_argument("--replay-rows", type=Path, default=DEFAULT_REPLAY_ROWS)
    parser.add_argument("--phase-features", type=Path, default=DEFAULT_PHASE_FEATURES)
    parser.add_argument("--discovery-decision", type=Path, default=DEFAULT_DISCOVERY_DECISION)
    parser.add_argument("--out-root", type=Path, default=DEFAULT_OUT_ROOT)
    args = parser.parse_args()
    out_dir = run(args)
    print(out_dir)


if __name__ == "__main__":
    main()
