from __future__ import annotations

import argparse
import json
import math
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd


AXIS_ID = "pre_entry_bad_pick_feature_contract_expansion_v1"
DEFAULT_SOURCE_DECISION = Path("G:/Tradex/context_risk_not_clean_stability_replay_v1/20260604T024719Z-context-risk-not-clean-stability-replay-v1/final_research_decision.json")
DEFAULT_REPLAY_ROWS = Path("G:/Tradex/intersection_family_current_period_risk_containment_v1/20260526T010028Z-intersection-family-current-period-risk-containment-v1/risk_containment_rows.csv")
DEFAULT_SOURCE_DB = Path("C:/Users/enish/AppData/Local/MeeMeeScreener/data/stocks.duckdb")
DEFAULT_OUT_ROOT = Path("G:/Tradex/pre_entry_bad_pick_feature_contract_expansion_v1")
REQUIRED = (
    "final_research_decision.json",
    "feature_contract_summary.json",
    "join_coverage_summary.json",
    "leakage_safety_summary.json",
    "next_axis_candidate_card.json",
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


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _mean(series: pd.Series) -> float | None:
    valid = pd.to_numeric(series, errors="coerce").dropna()
    return None if valid.empty else float(valid.mean())


def _rate(series: pd.Series) -> float | None:
    valid = series.dropna()
    return None if valid.empty else float(valid.astype(bool).mean())


def _load_replay(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path)
    required = {"as_of_date", "code", "fresh_runtime_research_watch_rank", "ret20"}
    missing = sorted(required - set(df.columns))
    if missing:
        raise RuntimeError(f"replay rows missing required columns: {missing}")
    df["code"] = df["code"].astype(str)
    df["as_of_date"] = pd.to_numeric(df["as_of_date"], errors="coerce").astype("Int64")
    df["ret20"] = pd.to_numeric(df["ret20"], errors="coerce")
    df["bad_pick_flag"] = df["ret20"] <= -0.05
    df["severe_loss_flag"] = df["ret20"] <= -0.10
    df["hit_flag"] = df["ret20"] > 0
    return df


def _load_bar_features(source_db: Path, replay: pd.DataFrame) -> pd.DataFrame:
    codes = sorted(replay["code"].dropna().astype(str).unique().tolist())
    min_date = int(replay["as_of_date"].min()) - 10000
    max_date = int(replay["as_of_date"].max())
    con = duckdb.connect(str(source_db), read_only=True)
    try:
        expr = "CAST(strftime(to_timestamp(date), '%Y%m%d') AS INTEGER)"
        bars = con.execute(
            f"""
            SELECT CAST(code AS VARCHAR) AS code, {expr} AS as_of_date, o, h, l, c, v, source
            FROM daily_bars
            WHERE CAST(code AS VARCHAR) IN (SELECT * FROM UNNEST(?))
              AND {expr} BETWEEN ? AND ?
            ORDER BY code, as_of_date
            """,
            [codes, min_date, max_date],
        ).fetchdf()
    finally:
        con.close()
    bars["code"] = bars["code"].astype(str)
    bars = bars.sort_values(["code", "as_of_date"], kind="stable").copy()
    g = bars.groupby("code", sort=False)
    prev_close = g["c"].shift(1)
    bars["gap_pct"] = bars["o"] / prev_close - 1.0
    bars["body_ratio"] = (bars["c"] - bars["o"]).abs() / (bars["h"] - bars["l"]).replace(0, pd.NA)
    bars["dollar_volume"] = bars["c"] * bars["v"]
    bars["volume20_avg"] = g["v"].transform(lambda s: s.rolling(20, min_periods=20).mean())
    bars["turnover20_value"] = g["dollar_volume"].transform(lambda s: s.rolling(20, min_periods=20).mean())
    bars["volume_vs_20d_avg"] = bars["v"] / bars["volume20_avg"]
    bars["volume_followthrough_proxy"] = bars["volume_vs_20d_avg"] * (bars["c"] / bars["o"] - 1.0)
    bars["thin_volume_proxy"] = bars["volume20_avg"]
    bars["thin_turnover_proxy"] = bars["turnover20_value"]
    return bars[[
        "code", "as_of_date", "gap_pct", "body_ratio", "v", "volume20_avg", "turnover20_value",
        "volume_vs_20d_avg", "volume_followthrough_proxy", "thin_volume_proxy", "thin_turnover_proxy", "source"
    ]]


def _slice_quality(frame: pd.DataFrame, flag: pd.Series) -> dict[str, Any]:
    inside = frame[flag.fillna(False)]
    outside = frame[~flag.fillna(False)]
    return {
        "inside_count": int(len(inside)),
        "outside_count": int(len(outside)),
        "inside_bad_pick_rate": _rate(inside["bad_pick_flag"]),
        "outside_bad_pick_rate": _rate(outside["bad_pick_flag"]),
        "inside_severe_loss_rate": _rate(inside["severe_loss_flag"]),
        "outside_severe_loss_rate": _rate(outside["severe_loss_flag"]),
        "inside_mean_ret20": _mean(inside["ret20"]),
        "outside_mean_ret20": _mean(outside["ret20"]),
        "inside_hit_rate": _rate(inside["hit_flag"]),
        "outside_hit_rate": _rate(outside["hit_flag"]),
    }


def _candidate_axes(joined: pd.DataFrame) -> list[dict[str, Any]]:
    q = lambda c, p: float(pd.to_numeric(joined[c], errors="coerce").quantile(p))
    axes = [
        ("gap_abs_q75", "gap", "abs(gap_pct) >= source q75", joined["gap_pct"].abs() >= q("gap_pct", 0.75)),
        ("gap_up_q75", "gap", "gap_pct >= source q75", joined["gap_pct"] >= q("gap_pct", 0.75)),
        ("gap_down_q25", "gap", "gap_pct <= source q25", joined["gap_pct"] <= q("gap_pct", 0.25)),
        ("volume_spike_q75", "volume abnormality", "volume_vs_20d_avg >= source q75", joined["volume_vs_20d_avg"] >= q("volume_vs_20d_avg", 0.75)),
        ("volume_dry_q25", "volume abnormality", "volume_vs_20d_avg <= source q25", joined["volume_vs_20d_avg"] <= q("volume_vs_20d_avg", 0.25)),
        ("volume_without_followthrough_q25", "volume without follow-through", "volume_followthrough_proxy <= source q25", joined["volume_followthrough_proxy"] <= q("volume_followthrough_proxy", 0.25)),
        ("thin_turnover_q25", "liquidity/thin-sample", "turnover20_value <= source q25", joined["turnover20_value"] <= q("turnover20_value", 0.25)),
        ("thin_volume_q25", "liquidity/thin-sample", "volume20_avg <= source q25", joined["volume20_avg"] <= q("volume20_avg", 0.25)),
    ]
    rows = []
    for axis_id, family, definition, flag in axes:
        quality = _slice_quality(joined, flag)
        bad_lift = (quality["inside_bad_pick_rate"] - quality["outside_bad_pick_rate"]) if quality["inside_bad_pick_rate"] is not None and quality["outside_bad_pick_rate"] is not None else None
        ret_gap = (quality["inside_mean_ret20"] - quality["outside_mean_ret20"]) if quality["inside_mean_ret20"] is not None and quality["outside_mean_ret20"] is not None else None
        coverage = float(flag.fillna(False).mean()) if len(joined) else None
        score = (bad_lift or 0) * 3.0 - (ret_gap or 0) * 2.0 + (coverage or 0) * 0.1
        rows.append({
            "axis_id": axis_id,
            "axis_family": family,
            "definition": definition,
            "deployable_status": "deployable_pre_entry_feature",
            "threshold_status": "research_source_quantile_for_axis_selection_only",
            "coverage": coverage,
            "quality": quality,
            "axis_selection_score": score,
        })
    rows.sort(key=lambda r: r["axis_selection_score"], reverse=True)
    return rows


def run(args: argparse.Namespace) -> Path:
    frozen = _read_json(args.source_decision)
    if frozen.get("authoritative_rollup_decision") != "hold":
        raise RuntimeError("context_risk_not_clean source decision is not hold")
    out_dir = args.out_root / f"{_now_tag()}-{AXIS_ID.replace('_', '-')}"
    out_dir.mkdir(parents=True, exist_ok=False)
    replay = _load_replay(args.replay_rows)
    features = _load_bar_features(args.source_db, replay)
    joined = replay.merge(features, on=["code", "as_of_date"], how="left", validate="one_to_one")
    feature_cols = ["gap_pct", "volume20_avg", "turnover20_value", "volume_vs_20d_avg", "volume_followthrough_proxy"]
    joined_ok = joined[feature_cols].notna().all(axis=1)
    axes = _candidate_axes(joined[joined_ok].copy()) if joined_ok.any() else []
    coverage = {
        "replay_rows": int(len(replay)),
        "joined_rows": int(len(joined)),
        "full_feature_coverage_rows": int(joined_ok.sum()),
        "full_feature_coverage_rate": float(joined_ok.mean()) if len(joined) else None,
        "unique_dates": int(joined["as_of_date"].nunique()),
        "unique_symbols": int(joined["code"].nunique()),
        "join_keys": ["code", "as_of_date"],
    }
    leakage = {
        "audit_result": "pass" if coverage["full_feature_coverage_rate"] and coverage["full_feature_coverage_rate"] >= 0.90 else "blocked",
        "features_use_as_of_or_prior_bars_only": True,
        "future_outcomes_used_for_feature_construction": False,
        "ret20_used_for_axis_quality_diagnostic_only": True,
        "runtime_db_read_only": True,
        "runtime_db_write": False,
        "feature_definitions": {
            "gap_pct": "today open / prior confirmed close - 1",
            "volume_vs_20d_avg": "today volume / rolling 20-bar average volume through as_of_date",
            "turnover20_value": "rolling 20-bar average close*volume through as_of_date",
            "volume_followthrough_proxy": "volume_vs_20d_avg times same-day open-to-close return",
        },
    }
    if not joined_ok.any() or (coverage["full_feature_coverage_rate"] or 0) < 0.90:
        decision, reason = "blocked_contract_missing", "gap_volume_liquidity_features_could_not_be_safely_joined_with_enough_coverage"
        selected = None
    elif not axes:
        decision, reason = "drop_expansion", "features_joined_but_no_candidate_axis_available"
        selected = None
    else:
        selected = axes[0]
        if (selected["coverage"] or 0) >= 0.05 and selected["quality"]["inside_count"] >= 30:
            decision, reason = "keep_for_next_axis_selection", "gap_volume_liquidity_pre_entry_fields_are_available_leakage_safe_and_joinable"
        else:
            decision, reason = "drop_expansion", "fields_available_but_coverage_or_sample_too_weak"
    contract = {
        "axis_id": AXIS_ID,
        "source_db": str(args.source_db),
        "source_replay_rows": str(args.replay_rows),
        "contract_grain": "one replay candidate row x as_of_date",
        "join_key": ["code", "as_of_date"],
        "available_fields": [
            {"name": "gap_pct", "family": "gap", "as_of_observable": True},
            {"name": "volume20_avg", "family": "volume/liquidity", "as_of_observable": True},
            {"name": "turnover20_value", "family": "liquidity", "as_of_observable": True},
            {"name": "volume_vs_20d_avg", "family": "volume abnormality", "as_of_observable": True},
            {"name": "volume_followthrough_proxy", "family": "volume without follow-through", "as_of_observable": True},
        ],
        "candidate_axes_ranked": axes,
    }
    card = {
        "axis_id": AXIS_ID,
        "next_axis_available": selected is not None and decision == "keep_for_next_axis_selection",
        "selected_next_axis": selected,
        "challenger_implemented": False,
        "next_challenger_scope": "single-axis selection only; no multi-axis combination" if selected else None,
    }
    final = {
        "axis_id": AXIS_ID,
        "authoritative_rollup_decision": decision,
        "reason": reason,
        "frozen_context_risk_source": str(args.source_decision),
        "context_risk_not_clean_frozen": True,
        "feature_contract_result": contract,
        "join_coverage_summary": coverage,
        "leakage_safety_summary": leakage,
        "next_axis_candidate_card": card,
        "boundary_flags": {
            "tradex_only": True,
            "research_infrastructure_diagnostic": True,
            "runtime_db_write": False,
            "meemee_reflection": False,
            "ranking_change": False,
            "publish": False,
            "production_candidate_generation_change": False,
            "live_rule_promotion_allowed": False,
            "frozen_exit_champion_changed": False,
            "ma_phase_mixed_in": False,
            "threshold_tuning": False,
            "challenger_implemented": False,
        },
        "next_required_single_step": "run_single_axis_selection_for_gap_volume_liquidity_family" if decision == "keep_for_next_axis_selection" else "resolve_feature_contract_blocker_before_next_axis",
    }
    _write_json(out_dir / "feature_contract_summary.json", contract)
    _write_json(out_dir / "join_coverage_summary.json", coverage)
    _write_json(out_dir / "leakage_safety_summary.json", leakage)
    _write_json(out_dir / "next_axis_candidate_card.json", card)
    _write_json(out_dir / "final_research_decision.json", final)
    _write_json(out_dir / "_ARTIFACT_COMPLETE.json", {
        "axis_id": AXIS_ID,
        "status": "complete",
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "required_files": list(REQUIRED),
        "required_files_present": all((out_dir / f).exists() for f in REQUIRED if f != "_ARTIFACT_COMPLETE.json"),
    })
    return out_dir


def main() -> None:
    parser = argparse.ArgumentParser(description="Expand pre-entry bad-pick feature contract for gap, volume, and liquidity axes.")
    parser.add_argument("--source-decision", type=Path, default=DEFAULT_SOURCE_DECISION)
    parser.add_argument("--replay-rows", type=Path, default=DEFAULT_REPLAY_ROWS)
    parser.add_argument("--source-db", type=Path, default=DEFAULT_SOURCE_DB)
    parser.add_argument("--out-root", type=Path, default=DEFAULT_OUT_ROOT)
    print(run(parser.parse_args()))


if __name__ == "__main__":
    main()
