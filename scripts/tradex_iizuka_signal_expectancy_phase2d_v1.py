from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts import tradex_iizuka_signal_expectancy_v1 as base

SCRIPT_NAME = "tradex_iizuka_signal_expectancy_phase2d_v1"
SCHEMA_VERSION = "tradex_iizuka_signal_expectancy_phase2d_v1"
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\iizuka_signal_expectancy_v1_phase2d")
PHASE1C_ROOT = Path(r"G:\Tradex\iizuka_signal_expectancy_v1_phase1c\20260509T050355Z-080636")
PHASE2C3_ROOT = Path(r"G:\Tradex\point_in_time_candidate_pool_contract_v1_phase2c3\20260509T071034Z-875642")
SIGNAL_ROWS = PHASE1C_ROOT / "phase1c_mixed_signal_rows.parquet"
POOL_ROWS = PHASE2C3_ROOT / "full_candidate_pool_rows_context_lineage.parquet"
PHASE1C_DECISION = PHASE1C_ROOT / "phase1c_signal_decision.json"
PHASE2C3_DECISION = PHASE2C3_ROOT / "phase2c3_decision.json"
BOOST_VARIANTS = {
    "mixed_signal_boost_light": 0.02,
    "mixed_signal_boost_medium": 0.05,
}
TOPK_VALUES = [5, 10, 20]
REQUIRED_ARTIFACTS = [
    "run_manifest.json",
    "input_resolution.json",
    "phase2d_key_alignment.json",
    "phase2d_candidate_pool_snapshot.json",
    "phase2d_champion_baseline_metrics.json",
    "phase2d_boost_test_results.json",
    "phase2d_branching_analysis.json",
    "phase2d_bad_pick_removal_analysis.json",
    "phase2d_decision.json",
    "_ARTIFACT_COMPLETE.json",
]


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _session_id() -> str:
    now = datetime.now(timezone.utc)
    return f"{now.strftime('%Y%m%dT%H%M%SZ')}-{now.microsecond:06d}"


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    base._write_json(path, payload)


def _safe_path(value: str | Path | None, default: Path) -> Path:
    return base._safe_path(value, default)


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _date(value: pd.Series) -> pd.Series:
    return pd.to_datetime(value, errors="coerce").dt.date.astype("string")


def _prepare_signal(rows: pd.DataFrame) -> pd.DataFrame:
    out = rows.copy()
    out["symbol_norm"] = out["symbol"].astype(str).str.strip()
    for column in ["decision_date", "execution_date"]:
        out[f"{column}_norm"] = _date(out[column])
    return out


def _prepare_pool(rows: pd.DataFrame) -> pd.DataFrame:
    out = rows.copy()
    out["symbol_norm"] = out["symbol"].astype(str).str.strip()
    out["candidate_date_norm"] = _date(out["candidate_date"])
    out["anchor_date_norm2"] = _date(out["anchor_date"])
    out["side_norm"] = out["side"].astype(str).str.lower()
    out["champion_score"] = pd.to_numeric(out["champion_score"], errors="coerce")
    out["champion_rank"] = pd.to_numeric(out["champion_rank"], errors="coerce")
    return out


def _pool_key_set(pool: pd.DataFrame, date_col: str) -> set[str]:
    return set(pool["symbol_norm"].astype(str) + "|" + pool[date_col].astype(str))


def _align_exact(signal: pd.DataFrame, pool: pd.DataFrame, signal_date_col: str, policy: str) -> pd.DataFrame:
    matched = signal.merge(
        pool[["symbol_norm", "candidate_date_norm", "side_norm", "champion_rank", "top5_membership", "top10_membership", "top20_membership", "top50_membership"]],
        left_on=["symbol_norm", signal_date_col],
        right_on=["symbol_norm", "candidate_date_norm"],
        how="inner",
    )
    matched["key_policy"] = policy
    return matched


def _align_nearest_prior(signal: pd.DataFrame, pool: pd.DataFrame) -> pd.DataFrame:
    parts: list[pd.DataFrame] = []
    pool_dates = pool[["symbol_norm", "candidate_date_norm", "side_norm", "champion_rank", "top5_membership", "top10_membership", "top20_membership", "top50_membership"]].copy()
    pool_dates["candidate_dt"] = pd.to_datetime(pool_dates["candidate_date_norm"], errors="coerce")
    sig = signal.copy()
    sig["execution_dt"] = pd.to_datetime(sig["execution_date_norm"], errors="coerce")
    for symbol, group in sig.groupby("symbol_norm", sort=False):
        right = pool_dates.loc[pool_dates["symbol_norm"].eq(symbol)].dropna(subset=["candidate_dt"]).sort_values("candidate_dt", kind="mergesort")
        if right.empty:
            continue
        left = group.dropna(subset=["execution_dt"]).sort_values("execution_dt", kind="mergesort")
        if left.empty:
            continue
        joined = pd.merge_asof(left, right, left_on="execution_dt", right_on="candidate_dt", direction="backward", suffixes=("", "_pool"))
        joined = joined.loc[joined["candidate_dt"].notna()].copy()
        parts.append(joined)
    matched = pd.concat(parts, ignore_index=True) if parts else signal.head(0).copy()
    if len(matched):
        matched["candidate_date_norm"] = matched["candidate_dt"].dt.date.astype("string")
        matched["key_policy"] = "code+nearest_prior_candidate_date"
    return matched


def _key_alignment(signal: pd.DataFrame, pool: pd.DataFrame) -> tuple[dict[str, Any], str | None, pd.DataFrame]:
    alignments: dict[str, pd.DataFrame] = {
        "code+decision_date": _align_exact(signal, pool, "decision_date_norm", "code+decision_date"),
        "code+execution_date": _align_exact(signal, pool, "execution_date_norm", "code+execution_date"),
        "code+nearest_prior_candidate_date": _align_nearest_prior(signal, pool),
    }
    summaries: dict[str, Any] = {}
    for policy, rows in alignments.items():
        summaries[policy] = {
            "matched_signal_rows": int(rows["candidate_id"].nunique()) if "candidate_id" in rows.columns and len(rows) else int(len(rows)),
            "candidate_pool_overlap": int(len(rows)),
            "top50_overlap": int(rows["top50_membership"].fillna(False).astype(bool).sum()) if len(rows) else 0,
            "top20_overlap": int(rows["top20_membership"].fillna(False).astype(bool).sum()) if len(rows) else 0,
            "top10_overlap": int(rows["top10_membership"].fillna(False).astype(bool).sum()) if len(rows) else 0,
            "top5_overlap": int(rows["top5_membership"].fillna(False).astype(bool).sum()) if len(rows) else 0,
            "no_lookahead_status": "safe_no_future_candidate_date",
        }
    selected_policy = None
    selected = signal.head(0).copy()
    for policy in ["code+execution_date", "code+decision_date", "code+nearest_prior_candidate_date"]:
        if summaries[policy]["matched_signal_rows"] > 0:
            selected_policy = policy
            selected = alignments[policy]
            break
    return {
        "schema_version": f"{SCHEMA_VERSION}_key_alignment_v1",
        "generated_at_utc": _utc_now(),
        "forbidden_key_policies": ["nearest_next"],
        "policy_summaries": summaries,
        "selected_key_policy": selected_policy,
        "selected_reason": "preferred exact safe policy with overlap, else nearest-prior" if selected_policy else "no_safe_policy_has_overlap",
    }, selected_policy, selected


def _topk_members(frame: pd.DataFrame, score_col: str, k: int) -> pd.DataFrame:
    parts = []
    for _, group in frame.groupby(["candidate_date_norm", "side_norm"], sort=False):
        ordered = group.sort_values([score_col, "champion_rank", "symbol_norm"], ascending=[False, True, True], kind="mergesort")
        parts.append(ordered.head(k).copy())
    return pd.concat(parts, ignore_index=True) if parts else frame.head(0).copy()


def _metrics(frame: pd.DataFrame) -> dict[str, Any]:
    result: dict[str, Any] = {"count": int(len(frame))}
    for horizon in [5, 10, 20]:
        col = f"ret{horizon}"
        if col in frame.columns:
            vals = pd.to_numeric(frame[col], errors="coerce")
        else:
            vals = pd.to_numeric(frame.get(f"forward_ret_{horizon}d"), errors="coerce")
        result[f"ret{horizon}_mean"] = None if vals.dropna().empty else float(vals.mean())
        result[f"ret{horizon}_median"] = None if vals.dropna().empty else float(vals.median())
    ret20 = pd.to_numeric(frame.get("ret20", frame.get("forward_ret_20d")), errors="coerce")
    result["win_rate_20"] = None if ret20.dropna().empty else float((ret20 > 0).mean())
    mae = pd.to_numeric(frame.get("mae20", frame.get("mae_20d", frame.get("max_adverse_excursion"))), errors="coerce")
    result["mae_mean"] = None if mae.dropna().empty else float(mae.mean())
    result["mae_median"] = None if mae.dropna().empty else float(mae.median())
    result["bad_pick_count"] = int((ret20 < 0).sum()) if not ret20.dropna().empty else 0
    return result


def _baseline_metrics(pool: pd.DataFrame) -> dict[str, Any]:
    topk = {}
    for k in TOPK_VALUES:
        topk[f"top{k}"] = _metrics(_topk_members(pool, "champion_score", k))
    return {
        "schema_version": f"{SCHEMA_VERSION}_champion_baseline_metrics_v1",
        "generated_at_utc": _utc_now(),
        "evaluation_labels_used_after_candidate_construction": True,
        "topk": topk,
    }


def _apply_boost(pool: pd.DataFrame, matched: pd.DataFrame, boost: float) -> pd.DataFrame:
    out = pool.copy()
    keys = set(matched["symbol_norm"].astype(str) + "|" + matched["candidate_date_norm"].astype(str))
    out["phase2d_match_key"] = out["symbol_norm"].astype(str) + "|" + out["candidate_date_norm"].astype(str)
    out["iizuka_mixed_signal_matched"] = out["phase2d_match_key"].isin(keys)
    out["boosted_score"] = out["champion_score"] + out["iizuka_mixed_signal_matched"].astype(float) * boost
    out["boost_formula"] = f"boosted_score = champion_score + {boost} if monthly_C_mixed_pullback_end_reclaim7_v1 matched else champion_score"
    return out


def _variant_results(pool: pd.DataFrame, matched: pd.DataFrame, baseline: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    boost_results: dict[str, Any] = {
        "schema_version": f"{SCHEMA_VERSION}_boost_test_results_v1",
        "generated_at_utc": _utc_now(),
        "boost_variants": {},
    }
    branching: dict[str, Any] = {
        "schema_version": f"{SCHEMA_VERSION}_branching_analysis_v1",
        "generated_at_utc": _utc_now(),
        "variants": {},
    }
    bad_pick: dict[str, Any] = {
        "schema_version": f"{SCHEMA_VERSION}_bad_pick_removal_v1",
        "generated_at_utc": _utc_now(),
        "variants": {},
    }
    champion_sets = {}
    for k in TOPK_VALUES:
        frame = _topk_members(pool, "champion_score", k)
        champion_sets[k] = set(frame["symbol_norm"].astype(str) + "|" + frame["candidate_date_norm"].astype(str) + "|" + frame["side_norm"].astype(str))
    for name, boost in BOOST_VARIANTS.items():
        boosted = _apply_boost(pool, matched, boost)
        top_metrics = {}
        branch = {"boost_value": boost, "changed_rank_count": int(boosted["iizuka_mixed_signal_matched"].sum())}
        removal = {"boost_value": boost}
        for k in TOPK_VALUES:
            top = _topk_members(boosted, "boosted_score", k)
            metrics = _metrics(top)
            top_metrics[f"top{k}"] = metrics
            variant_set = set(top["symbol_norm"].astype(str) + "|" + top["candidate_date_norm"].astype(str) + "|" + top["side_norm"].astype(str))
            champ_set = champion_sets[k]
            added_keys = variant_set - champ_set
            removed_keys = champ_set - variant_set
            branch[f"changed_top{k}_members_count"] = int(len(added_keys))
            branch[f"top{k}_overlap_with_champion"] = int(len(variant_set & champ_set))
            branch[f"top{k}_churn_proxy"] = float(len(added_keys) / max(1, len(champ_set)))
            champ_metric = baseline["topk"][f"top{k}"]
            top_metrics[f"top{k}"]["ret20_mean_delta"] = None if metrics["ret20_mean"] is None or champ_metric["ret20_mean"] is None else float(metrics["ret20_mean"] - champ_metric["ret20_mean"])
            top_metrics[f"top{k}"]["ret20_median_delta"] = None if metrics["ret20_median"] is None or champ_metric["ret20_median"] is None else float(metrics["ret20_median"] - champ_metric["ret20_median"])
            top_metrics[f"top{k}"]["win_rate_20_delta"] = None if metrics["win_rate_20"] is None or champ_metric["win_rate_20"] is None else float(metrics["win_rate_20"] - champ_metric["win_rate_20"])
            removal[f"top{k}_bad_pick_removal_count"] = int(champ_metric["bad_pick_count"] - metrics["bad_pick_count"])
            removal[f"top{k}_newly_added_count"] = int(len(added_keys))
            removal[f"top{k}_removed_count"] = int(len(removed_keys))
        boost_results["boost_variants"][name] = {
            "boost_value": boost,
            "boost_formula": f"boosted_score = champion_score + {boost} for matched signal rows",
            "topk": top_metrics,
        }
        branching["variants"][name] = branch
        bad_pick["variants"][name] = removal
    return boost_results, branching, bad_pick


def _decide(alignment: dict[str, Any], boost_results: dict[str, Any], branching: dict[str, Any]) -> tuple[str, str, bool]:
    if not alignment["selected_key_policy"]:
        return "blocked", "no_safe_key_policy_has_overlap", False
    best = None
    for name, payload in boost_results["boost_variants"].items():
        top10 = payload["topk"]["top10"]
        top20 = payload["topk"]["top20"]
        churn10 = branching["variants"][name]["top10_churn_proxy"]
        improves = (
            (top10["ret20_mean_delta"] is not None and top10["ret20_mean_delta"] > 0)
            or (top10["ret20_median_delta"] is not None and top10["ret20_median_delta"] > 0)
            or (top20["ret20_mean_delta"] is not None and top20["ret20_mean_delta"] > 0)
            or (top20["ret20_median_delta"] is not None and top20["ret20_median_delta"] > 0)
        )
        if improves and churn10 <= 0.30:
            best = name
            break
    if best:
        return "proceed_to_ranking_challenger_candidate", f"{best}_improves_topk_under_fixed_conditions", True
    any_changed = any(v["changed_top10_members_count"] > 0 or v["changed_top20_members_count"] > 0 for v in branching["variants"].values())
    if any_changed:
        return "hold_for_boost_design", "safe_overlap_exists_but_fixed_boost_did_not_clear_challenger_gate", False
    return "explain_only", "signal_overlaps_pool_but_no_material_rank_movement", False


def run_phase2d(*, output_root: Path) -> dict[str, Any]:
    session_root = output_root / _session_id()
    session_root.mkdir(parents=True, exist_ok=True)
    signal_decision = _read_json(PHASE1C_DECISION)
    pool_decision = _read_json(PHASE2C3_DECISION)
    signal = _prepare_signal(pd.read_parquet(SIGNAL_ROWS))
    pool = _prepare_pool(pd.read_parquet(POOL_ROWS))
    alignment, selected_policy, matched = _key_alignment(signal, pool)
    pool_snapshot = {
        "schema_version": f"{SCHEMA_VERSION}_candidate_pool_snapshot_v1",
        "generated_at_utc": _utc_now(),
        "pool_path": str(POOL_ROWS),
        "row_count": int(len(pool)),
        "candidate_date_min": str(pool["candidate_date_norm"].min()),
        "candidate_date_max": str(pool["candidate_date_norm"].max()),
        "point_in_time_phase2c3_decision": pool_decision.get("authoritative_decision"),
        "full_no_lookahead_verified": pool_decision.get("full_no_lookahead_verified"),
        "evaluation_labels_present": [c for c in ["ret5", "ret10", "ret20", "mae63", "forward_ret_5d", "forward_ret_10d", "forward_ret_20d", "mae_20d"] if c in pool.columns],
    }
    baseline = _baseline_metrics(pool)
    boost_results, branching, bad_pick = _variant_results(pool, matched, baseline) if selected_policy else (
        {"schema_version": f"{SCHEMA_VERSION}_boost_test_results_v1", "generated_at_utc": _utc_now(), "boost_variants": {}, "blocked_reason": "no_safe_key_policy_has_overlap"},
        {"schema_version": f"{SCHEMA_VERSION}_branching_analysis_v1", "generated_at_utc": _utc_now(), "variants": {}, "blocked_reason": "no_safe_key_policy_has_overlap"},
        {"schema_version": f"{SCHEMA_VERSION}_bad_pick_removal_v1", "generated_at_utc": _utc_now(), "variants": {}, "blocked_reason": "no_safe_key_policy_has_overlap"},
    )
    decision, reason, allow_next = _decide(alignment, boost_results, branching)
    selected_summary = alignment["policy_summaries"].get(selected_policy or "", {})
    decision_payload = {
        "schema_version": f"{SCHEMA_VERSION}_decision_v1",
        "generated_at_utc": _utc_now(),
        "authoritative_decision": decision,
        "decision_reason": reason,
        "selected_key_policy": selected_policy,
        "matched_signal_rows": selected_summary.get("matched_signal_rows", 0),
        "top50_overlap": selected_summary.get("top50_overlap", 0),
        "top20_overlap": selected_summary.get("top20_overlap", 0),
        "top10_overlap": selected_summary.get("top10_overlap", 0),
        "top5_overlap": selected_summary.get("top5_overlap", 0),
        "boost_variants_tested": list(BOOST_VARIANTS.keys()) if selected_policy else [],
        "future_ranking_challenger_candidate_allowed": allow_next,
        "meemee_remains_blocked": True,
        "meemee_changed": False,
        "production_ranking_changed": False,
        "publish_changed": False,
        "signal_definition_changed": False,
        "thresholds_changed": False,
        "champion_scoring_logic_changed": False,
    }
    artifacts = {
        "run_manifest.json": {
            "schema_version": f"{SCHEMA_VERSION}_manifest_v1",
            "generated_at_utc": _utc_now(),
            "script_name": SCRIPT_NAME,
            "session_root": str(session_root),
            "boundary": "TRADEX-only",
            "boost_variants": BOOST_VARIANTS,
        },
        "input_resolution.json": {
            "schema_version": f"{SCHEMA_VERSION}_input_resolution_v1",
            "generated_at_utc": _utc_now(),
            "phase1c_root": str(PHASE1C_ROOT),
            "phase2c3_root": str(PHASE2C3_ROOT),
            "signal_rows": str(SIGNAL_ROWS),
            "candidate_pool_rows": str(POOL_ROWS),
            "phase1c_decision": signal_decision.get("authoritative_rollup_decision"),
            "phase2c3_decision": pool_decision.get("authoritative_decision"),
        },
        "phase2d_key_alignment.json": alignment,
        "phase2d_candidate_pool_snapshot.json": pool_snapshot,
        "phase2d_champion_baseline_metrics.json": baseline,
        "phase2d_boost_test_results.json": boost_results,
        "phase2d_branching_analysis.json": branching,
        "phase2d_bad_pick_removal_analysis.json": bad_pick,
        "phase2d_decision.json": decision_payload,
    }
    for name, body in artifacts.items():
        _write_json(session_root / name, body)
    complete = {
        "schema_version": f"{SCHEMA_VERSION}_artifact_complete_v1",
        "generated_at_utc": _utc_now(),
        "session_root": str(session_root),
        "required_artifacts": REQUIRED_ARTIFACTS,
        "all_present": all((session_root / artifact).exists() for artifact in REQUIRED_ARTIFACTS if artifact != "_ARTIFACT_COMPLETE.json"),
    }
    _write_json(session_root / "_ARTIFACT_COMPLETE.json", complete)
    return {"session_root": str(session_root), "decision": decision, "selected_key_policy": selected_policy}


def main() -> None:
    parser = argparse.ArgumentParser(description="TRADEX Phase 2d Iizuka mixed signal fixed ranking pretest")
    parser.add_argument("--output-root", default=str(DEFAULT_OUTPUT_ROOT))
    args = parser.parse_args()
    result = run_phase2d(output_root=_safe_path(args.output_root, DEFAULT_OUTPUT_ROOT))
    print(json.dumps(base._json_ready(result), ensure_ascii=False, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
