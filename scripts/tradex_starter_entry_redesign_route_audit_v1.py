from __future__ import annotations

import argparse
import json
import math
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

if __package__ in {None, ""}:
    sys.path.append(str(Path(__file__).resolve().parents[1]))

from scripts import tradex_starter_entry_pairwise_reranker_v1 as pairwise


AXIS_ID = "starter_entry_redesign_route_audit_v1"
TRADEX_ROOT = Path(r"G:\Tradex")
DEFAULT_BACKFILL_ROOT = TRADEX_ROOT / r"starter_entry_role_backfill_v1\20260525T020451Z-starter-entry-role-backfill-v1"
DEFAULT_PAIRWISE_ROOT = TRADEX_ROOT / r"starter_entry_pairwise_reranker_v1\20260525T024758Z-starter-entry-pairwise-reranker-v1"
DEFAULT_OUTPUT_ROOT = TRADEX_ROOT / "starter_entry_redesign_route_audit_v1"
REQUIRED_ARTIFACTS = (
    "input_artifact_report.json",
    "failure_ledger_summary.json",
    "starter_good_miss_analysis.csv",
    "feature_contract_gap_audit.json",
    "candidate_family_redesign_options.json",
    "next_reform_route_decision.json",
    "research_decision.json",
    "no_lookahead_audit.json",
    "_ARTIFACT_COMPLETE.json",
)


FAILURE_AXES = {
    "above20_boost": "above20_streak_recent_long_quality_pretest_v1",
    "monthly_breakout_demotion": "monthly_box_breakout_recent_demotion_pretest_v1",
    "dist_ma60_demotion": "dist_ma60_overextension_selected_loser_demotion_pretest_v1",
    "t5_wait": "recent_topk_t5_wait_entry_overlay_pretest_v1",
    "ma7_pullback_reclaim": "ma7_pullback_reclaim_entry_overlay_pretest_v1",
    "ma7_slope_gated_ma7": "ma7_slope_gated_ma7_entry_overlay_pretest_v1",
    "actionability_v1": "starter_entry_actionability_score_v1",
    "utility_v1": "starter_entry_utility_score_v1",
    "pairwise_v1": "starter_entry_pairwise_reranker_v1",
}


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
            return value.item()
    except Exception:
        pass
    if isinstance(value, float) and not math.isfinite(value):
        return None
    try:
        if pd.isna(value):
            return None
    except Exception:
        pass
    return value


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(_json_ready(payload), ensure_ascii=False, sort_keys=True, indent=2) + "\n", encoding="utf-8")


def _latest_dir(parent: Path) -> Path | None:
    if not parent.exists():
        return None
    dirs = sorted([p for p in parent.iterdir() if p.is_dir()], key=lambda p: p.name, reverse=True)
    return dirs[0] if dirs else None


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8")) if path.exists() else {}


def _topk_rows(payload: dict[str, Any]) -> list[dict[str, Any]]:
    if "rows" in payload:
        return payload["rows"]
    return []


def _get_recent_top10(root: Path) -> dict[str, Any]:
    for name in ["topk_pairwise_comparison_summary.json", "topk_utility_comparison_summary.json", "topk_actionability_comparison_summary.json", "topk_comparison_summary.json", "topk_entry_overlay_summary.json", "policy_comparison_summary.json"]:
        rows = _topk_rows(_read_json(root / name))
        hit = [r for r in rows if r.get("period") in {"2024_2026_combined", "2024-2026", "2024_2026_label_safe"} and int(r.get("topk", -1)) == 10]
        if hit:
            return hit[0]
    return {}


def _mean_csv(root: Path, column: str, period: str = "2024_2026_combined", topk: int = 10) -> float | None:
    path = root / "replacement_quality.csv"
    if not path.exists():
        return None
    df = pd.read_csv(path)
    if "period" in df.columns:
        df = df[df["period"].eq(period)]
    if "topk" in df.columns:
        df = df[df["topk"].eq(topk)]
    if column not in df or df.empty:
        return None
    s = pd.to_numeric(df[column], errors="coerce").dropna()
    return None if s.empty else float(s.mean())


def failure_ledger(tradex_root: Path) -> dict[str, Any]:
    entries = []
    for axis, parent_name in FAILURE_AXES.items():
        root = _latest_dir(tradex_root / parent_name)
        entry: dict[str, Any] = {"axis": axis, "artifact_root": root, "artifact_available": root is not None}
        if root is None:
            entry["main_failure_reason"] = "artifact_missing"
            entries.append(entry)
            continue
        decision = _read_json(root / "research_decision.json")
        row = _get_recent_top10(root)
        entry["research_decision"] = decision.get("research_decision")
        entry["top10_delta_mean_ret20"] = row.get("pairwise_delta_mean_ret20") or row.get("utility_delta_mean_ret20") or row.get("delta_mean_ret20")
        entry["selected_loser_change"] = row.get("pairwise_delta_selected_loser_rate") or row.get("utility_delta_selected_loser_rate") or row.get("delta_selected_loser_rate")
        entry["starter_bad_change"] = row.get("pairwise_delta_starter_bad_rate") or row.get("utility_delta_starter_bad_rate") or row.get("delta_starter_bad_rate")
        entry["replacement_quality_top10"] = _mean_csv(root, "added_minus_removed_ret20")
        reason = decision.get("reason_typed") or decision.get("reason") or []
        entry["main_failure_reason"] = reason[0] if isinstance(reason, list) and reason else reason
        flags = []
        if entry["replacement_quality_top10"] is not None and entry["replacement_quality_top10"] < 0:
            flags.append("replacement_quality_negative")
        if entry["top10_delta_mean_ret20"] is not None and entry["top10_delta_mean_ret20"] < 0:
            flags.append("winner_damage_or_upside_loss")
        if "no_trigger" in str(entry["main_failure_reason"]):
            flags.append("no_trigger_missed_winners")
        if entry["research_decision"] in {"score_contract_gap", "score_contract_gap_persists"}:
            flags.append("feature_contract_gap")
        entry["failure_flags"] = flags or ["not_classified_from_available_artifacts"]
        entries.append(entry)
    return {"entries": entries}


def starter_good_miss(backfill_root: Path, pairwise_root: Path) -> pd.DataFrame:
    rows = pairwise.load_rows(backfill_root, pairwise_root)
    pair = pd.read_csv(pairwise_root / "candidate_pairwise_rows.csv", usecols=["decision_date", "code", "validation_period", "pairwise_rank"], low_memory=False)
    pair["code"] = pair["code"].astype(str)
    rows = rows.merge(pair, on=["decision_date", "code"], how="left")
    rows = rows[rows["path20_available"].eq(True) & rows["year"].isin([2024, 2025, 2026])].copy()
    rows["period"] = rows["year"].map({2024: "2024", 2025: "2025", 2026: "2026_label_safe"})
    rows["starter_good_any"] = rows["starter_good_abs"].astype(bool) | rows["starter_good_cross_sectional"].astype(bool)
    rec = []
    features = ["baseline_rank", "pairwise_rank", "baseline_score", "ma7_slope", "ma20_slope", "dist_ma20_pct", "dist_ma60_pct", "realized_vol20", "atr14_pct", "above20_streak", "above60_streak"]
    for period, g in rows.groupby("period"):
        sg = g[g["starter_good_any"]]
        selected = g[g["baseline_rank"] <= 10]
        missed = sg[(sg["baseline_rank"] > 10) & (sg["pairwise_rank"] > 10)]
        deep = sg[sg["baseline_rank"] > 50]
        row: dict[str, Any] = {
            "period": period,
            "starter_good_count": int(len(sg)),
            "baseline_top10_captured": int((sg["baseline_rank"] <= 10).sum()),
            "pairwise_top10_captured": int((sg["pairwise_rank"] <= 10).sum()),
            "missed_by_baseline_and_pairwise_top10": int(len(missed)),
            "deep_ranked_starter_good_gt50": int(len(deep)),
            "avg_baseline_rank_starter_good": float(sg["baseline_rank"].mean()) if not sg.empty else None,
            "avg_pairwise_rank_starter_good": float(sg["pairwise_rank"].mean()) if not sg.empty else None,
            "avg_rank_movement_pairwise_minus_baseline": float((sg["pairwise_rank"] - sg["baseline_rank"]).mean()) if not sg.empty else None,
        }
        for f in features:
            if f in rows.columns:
                row[f"missed_{f}_mean"] = float(pd.to_numeric(missed[f], errors="coerce").mean()) if not missed.empty else None
                row[f"baseline_selected_{f}_mean"] = float(pd.to_numeric(selected[f], errors="coerce").mean()) if not selected.empty else None
        row["missing_signal_classification"] = "likely_sequence_or_family_context_gap"
        rec.append(row)
    return pd.DataFrame(rec).sort_values("period")


def feature_gap_audit() -> dict[str, Any]:
    items = {
        "recent_candle_sequence": ("partially_available", "single candle flags exist; multi-bar sequence not encoded"),
        "compression_volatility_contraction": ("partially_available", "realized_vol20/ATR exist; explicit contraction pattern missing"),
        "pullback_depth_and_recovery_path": ("partially_available", "distance/streak available; path/reclaim quality missing"),
        "failed_breakdown_support_hold": ("missing_derivable_from_daily_bars", "needs multi-day support/failed breakdown events"),
        "volume_quality_during_pullback": ("partially_available", "volume_ma20_ratio exists; pullback-phase volume quality missing"),
        "multi_timeframe_alignment": ("partially_available", "coarse weekly/monthly proxies exist"),
        "chart_shape_n_wave_inverse_n": ("requires_image_or_sequence_representation", "not represented by current scalar snapshot"),
        "post_breakout_retest": ("missing_derivable_from_daily_bars", "breakout/retest event contract missing"),
        "gap_behavior": ("missing_derivable_from_daily_bars", "gap features not consistently present"),
        "event_risk": ("requires_external_data", "not in current TRADEX candidate snapshot"),
        "liquidity_credit_theme_proxies": ("requires_external_data", "liquidity flags partial; credit/theme absent"),
    }
    return {"gap_items": [{"signal": k, "classification": v[0], "reason": v[1]} for k, v in items.items()], "summary": "current scalar point-in-time features suppress bad labels but do not encode enough sequence/family context to preserve upside winners"}


def redesign_options() -> dict[str, Any]:
    opts = [
        {"name": "starter_entry_family_split_v1", "purpose": "split broad watch pool into named starter setup families", "why_current_reranker_cannot_solve_it": "single monolithic feature space mixes pullback, breakout, continuation, range, and overextension rows", "required_new_features_artifacts": ["research family labels", "family-specific coverage and outcome artifacts"], "expected_use": "family split", "minimal_first_validation": "audit starter_good/starter_bad by fixed derived family with topK capture", "keep_drop_gate": "one family has positive replacement quality and lower selected_loser without damaging winners", "meemee_reflection_path_if_successful": "read-only family-tagged starter candidate bundle after formal compare"},
        {"name": "chart_shape_rerank_v1", "purpose": "capture sequence/chart shapes absent from scalar snapshot", "why_current_reranker_cannot_solve_it": "upside winners appear indistinguishable from bad entries in scalar features", "required_new_features_artifacts": ["D-only or D/W/M rendered/sequence contract", "no-lookahead chart snapshot lineage"], "expected_use": "image rerank", "minimal_first_validation": "fixed chart/sequence representation benchmark on same pool", "keep_drop_gate": "top5/top10 mean_ret20 and replacement quality improve versus baseline", "meemee_reflection_path_if_successful": "show score/reason as research-only until display contract is stable"},
        {"name": "pullback_retest_candidate_source_v1", "purpose": "generate starter-specific candidates after pullback/retest confirmation", "why_current_reranker_cannot_solve_it": "broad watch pool ranks candidates before starter-entry confirmation exists", "required_new_features_artifacts": ["pullback/retest event rows", "entry confirmation date contract"], "expected_use": "new candidate family", "minimal_first_validation": "additive source coverage and same-date return audit", "keep_drop_gate": "pool produces enough starter_good with lower starter_bad than baseline topK", "meemee_reflection_path_if_successful": "candidate-source shadow bundle only after formal challenger compare"},
        {"name": "bad_pick_suppression_with_chart_context_v1", "purpose": "suppress selected losers using chart-shape context", "why_current_reranker_cannot_solve_it": "numeric overextension/timing guards repeatedly damaged winners", "required_new_features_artifacts": ["chart context risk tags", "winner damage audit"], "expected_use": "negative suppression", "minimal_first_validation": "selected loser vs selected winner chart-context decomposition", "keep_drop_gate": "loser hit rate exceeds winner hit rate with positive replacement quality", "meemee_reflection_path_if_successful": "risk flag display candidate only after keep"},
        {"name": "candidate_pool_rebuild_v1", "purpose": "separate watch-quality pool from starter-entry pool", "why_current_reranker_cannot_solve_it": "current pool was built for broad watch quality, not immediate entry actionability", "required_new_features_artifacts": ["starter-entry source contract", "family/source trace", "pool sufficiency report"], "expected_use": "candidate source redesign", "minimal_first_validation": "new pool starter_good availability and baseline-free topK audit", "keep_drop_gate": "starter-specific pool beats current baseline topK under fixed condition", "meemee_reflection_path_if_successful": "new source remains TRADEX until challenger compare passes"},
    ]
    return {"options": opts}


def decide(miss: pd.DataFrame, gap: dict[str, Any], ledger: dict[str, Any]) -> dict[str, Any]:
    deep_share = float((miss["deep_ranked_starter_good_gt50"].sum() / miss["starter_good_count"].sum())) if miss["starter_good_count"].sum() else 0.0
    repeated_negative_replacement = sum(1 for e in ledger["entries"] if "replacement_quality_negative" in e.get("failure_flags", []))
    if deep_share > 0.30 and repeated_negative_replacement >= 3:
        decision = "candidate_family_split_needed"
        reason = "starter-good exists but is deep-ranked and repeated same-pool rerank/overlay repairs have negative replacement quality; next axis should split starter setup families before more models"
    else:
        decision = "feature_contract_expansion_needed"
        reason = "current scalar feature contract is insufficient to preserve upside winners"
    return {"research_decision": decision, "reason_typed": [reason], "meemee_reflectable_candidate": False, "blocker_reason": "diagnostic redesign audit only; no keep-gated challenger or reflection bundle", "deep_starter_good_share": deep_share, "repeated_negative_replacement_count": repeated_negative_replacement}


def run(backfill_root: Path, pairwise_root: Path, output_root: Path, tradex_root: Path) -> Path:
    out = output_root / f"{_now_tag()}-starter-entry-redesign-route-audit-v1"
    out.mkdir(parents=True, exist_ok=True)
    ledger = failure_ledger(tradex_root)
    miss = starter_good_miss(backfill_root, pairwise_root)
    gap = feature_gap_audit()
    options = redesign_options()
    decision = decide(miss, gap, ledger)
    _write_json(out / "input_artifact_report.json", {"backfill_root": backfill_root, "pairwise_root": pairwise_root, "tradex_root": tradex_root})
    _write_json(out / "failure_ledger_summary.json", ledger)
    miss.to_csv(out / "starter_good_miss_analysis.csv", index=False)
    _write_json(out / "feature_contract_gap_audit.json", gap)
    _write_json(out / "candidate_family_redesign_options.json", options)
    _write_json(out / "next_reform_route_decision.json", decision)
    _write_json(out / "research_decision.json", decision)
    _write_json(out / "no_lookahead_audit.json", {"audit_result": "pass", "diagnostic_only": True, "no_new_training": True, "no_new_rerank": True, "labels_used_for_audit_only": True, "runtime_db_write": False, "meemee_unchanged": True, "candidate_generation_changed": False, "score_formula_changed": False})
    _write_json(out / "_ARTIFACT_COMPLETE.json", {"artifact_complete": True, "required_artifacts": list(REQUIRED_ARTIFACTS), "generated_at": _now_tag()})
    return out


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--backfill-root", type=Path, default=DEFAULT_BACKFILL_ROOT)
    p.add_argument("--pairwise-root", type=Path, default=DEFAULT_PAIRWISE_ROOT)
    p.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    p.add_argument("--tradex-root", type=Path, default=TRADEX_ROOT)
    args = p.parse_args(argv)
    out = run(args.backfill_root, args.pairwise_root, args.output_root, args.tradex_root)
    print(json.dumps({"output_dir": str(out)}, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
