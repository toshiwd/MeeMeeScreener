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

SCRIPT_NAME = "tradex_iizuka_signal_expectancy_phase3_candidate_generation_v1"
SCHEMA_VERSION = "tradex_iizuka_signal_expectancy_phase3_candidate_generation_v1"
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\iizuka_signal_expectancy_v1_phase3_candidate_generation")
PHASE1C_ROOT = Path(r"G:\Tradex\iizuka_signal_expectancy_v1_phase1c\20260509T050355Z-080636")
PHASE2C3_ROOT = Path(r"G:\Tradex\point_in_time_candidate_pool_contract_v1_phase2c3\20260509T071034Z-875642")
PHASE2D_ROOT = Path(r"G:\Tradex\iizuka_signal_expectancy_v1_phase2d\20260509T072605Z-379660")
SIGNAL_ROWS = PHASE1C_ROOT / "phase1c_mixed_signal_rows.parquet"
POOL_ROWS = PHASE2C3_ROOT / "full_candidate_pool_rows_context_lineage.parquet"
PHASE1C_COMPARE = PHASE1C_ROOT / "phase1c_mixed_expectancy_compare.json"
PHASE1C_DECISION = PHASE1C_ROOT / "phase1c_signal_decision.json"
PHASE2D_DECISION = PHASE2D_ROOT / "phase2d_decision.json"
REQUIRED_ARTIFACTS = [
    "run_manifest.json",
    "input_resolution.json",
    "phase3_signal_pool_membership.json",
    "phase3_signal_only_expectancy.json",
    "phase3_reference_comparison.json",
    "phase3_candidate_generation_value_test.json",
    "phase3_decision.json",
    "_ARTIFACT_COMPLETE.json",
]


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _session_id() -> str:
    now = datetime.now(timezone.utc)
    return f"{now.strftime('%Y%m%dT%H%M%SZ')}-{now.microsecond:06d}"


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    base._write_json(path, payload)


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _safe_path(value: str | Path | None, default: Path) -> Path:
    return base._safe_path(value, default)


def _date(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, errors="coerce").dt.date.astype("string")


def _prep_signal(frame: pd.DataFrame) -> pd.DataFrame:
    out = frame.copy()
    out["symbol_norm"] = out["symbol"].astype(str).str.strip()
    out["decision_date_norm"] = _date(out["decision_date"])
    out["execution_date_norm"] = _date(out["execution_date"])
    out["signal_row_id"] = range(len(out))
    return out


def _prep_pool(frame: pd.DataFrame) -> pd.DataFrame:
    out = frame.copy()
    out["symbol_norm"] = out["symbol"].astype(str).str.strip()
    out["candidate_date_norm"] = _date(out["candidate_date"])
    out["candidate_dt"] = pd.to_datetime(out["candidate_date_norm"], errors="coerce")
    out["top50_membership"] = out.get("top50_membership", False).fillna(False).astype(bool)
    out["top20_membership"] = out.get("top20_membership", False).fillna(False).astype(bool)
    out["top10_membership"] = out.get("top10_membership", False).fillna(False).astype(bool)
    out["top5_membership"] = out.get("top5_membership", False).fillna(False).astype(bool)
    return out


def _exact_ids(signal: pd.DataFrame, pool: pd.DataFrame, signal_col: str) -> set[int]:
    matched = signal[["signal_row_id", "symbol_norm", signal_col]].merge(
        pool[["symbol_norm", "candidate_date_norm"]],
        left_on=["symbol_norm", signal_col],
        right_on=["symbol_norm", "candidate_date_norm"],
        how="inner",
    )
    return set(matched["signal_row_id"].astype(int))


def _nearest_prior_ids(signal: pd.DataFrame, pool: pd.DataFrame) -> set[int]:
    ids: set[int] = set()
    sig = signal.copy()
    sig["execution_dt"] = pd.to_datetime(sig["execution_date_norm"], errors="coerce")
    for symbol, group in sig.groupby("symbol_norm", sort=False):
        right = pool.loc[pool["symbol_norm"].eq(symbol), ["candidate_dt"]].dropna().sort_values("candidate_dt", kind="mergesort")
        if right.empty:
            continue
        left = group.dropna(subset=["execution_dt"]).sort_values("execution_dt", kind="mergesort")
        if left.empty:
            continue
        joined = pd.merge_asof(left[["signal_row_id", "execution_dt"]], right, left_on="execution_dt", right_on="candidate_dt", direction="backward")
        ids.update(joined.loc[joined["candidate_dt"].notna(), "signal_row_id"].astype(int).tolist())
    return ids


def _classify(signal: pd.DataFrame, pool: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, Any]]:
    pool_min = str(pool["candidate_date_norm"].min())
    pool_max = str(pool["candidate_date_norm"].max())
    in_decision = _exact_ids(signal, pool, "decision_date_norm")
    in_execution = _exact_ids(signal, pool, "execution_date_norm")
    in_prior = _nearest_prior_ids(signal, pool)
    in_pool = in_decision | in_execution | in_prior
    within_period = (signal["execution_date_norm"] >= pool_min) & (signal["execution_date_norm"] <= pool_max)
    classified = signal.copy()
    classified["pool_membership_class"] = "absent_from_candidate_pool"
    classified.loc[classified["signal_row_id"].isin(in_pool), "pool_membership_class"] = "already_in_candidate_pool"
    classified.loc[~within_period, "pool_membership_class"] = "unsafe_to_classify"
    absent = classified["pool_membership_class"].eq("absent_from_candidate_pool")
    summary = {
        "schema_version": f"{SCHEMA_VERSION}_membership_v1",
        "generated_at_utc": _utc_now(),
        "safe_key_policies": ["code+decision_date", "code+execution_date", "code+nearest_prior_candidate_date"],
        "forbidden_key_policies": ["nearest_next"],
        "pool_candidate_date_min": pool_min,
        "pool_candidate_date_max": pool_max,
        "already_in_candidate_pool_count": int(classified["pool_membership_class"].eq("already_in_candidate_pool").sum()),
        "absent_from_candidate_pool_count": int(absent.sum()),
        "unsafe_to_classify_count": int(classified["pool_membership_class"].eq("unsafe_to_classify").sum()),
        "exact_decision_match_count": int(len(in_decision)),
        "exact_execution_match_count": int(len(in_execution)),
        "nearest_prior_match_count": int(len(in_prior)),
        "classification_notes": "signals outside verified pool execution-date range are unsafe_to_classify; nearest_next is not used",
    }
    return classified, summary


def _metrics(frame: pd.DataFrame) -> dict[str, Any]:
    result: dict[str, Any] = {"count": int(len(frame))}
    for horizon in [5, 10, 20]:
        vals = pd.to_numeric(frame.get(f"ret{horizon}"), errors="coerce")
        result[f"ret{horizon}_mean"] = None if vals.dropna().empty else float(vals.mean())
        result[f"ret{horizon}_median"] = None if vals.dropna().empty else float(vals.median())
    ret20 = pd.to_numeric(frame.get("ret20"), errors="coerce")
    mae = pd.to_numeric(frame.get("mae20", frame.get("mae_20d", frame.get("max_adverse_excursion"))), errors="coerce")
    result["win_rate_20"] = None if ret20.dropna().empty else float((ret20 > 0).mean())
    result["mae20_mean"] = None if mae.dropna().empty else float(mae.mean())
    result["mae20_median"] = None if mae.dropna().empty else float(mae.median())
    result["severe_loser_share"] = None if ret20.dropna().empty else float(((ret20 < 0) & (mae <= -0.08)).mean())
    result["failed_followthrough_share"] = None if ret20.dropna().empty else float(((pd.to_numeric(frame.get("ret5"), errors="coerce") <= 0) & (ret20 <= 0)).mean())
    return result


def _concentration(frame: pd.DataFrame) -> dict[str, Any]:
    def share(column: str) -> float | None:
        if column not in frame.columns or frame.empty:
            return None
        counts = frame[column].fillna("unknown").astype(str).value_counts()
        return None if counts.empty else float(counts.iloc[0] / len(frame))
    year = pd.to_datetime(frame.get("decision_date"), errors="coerce").dt.year.astype("string") if "decision_date" in frame.columns else pd.Series(dtype="string")
    tmp = frame.copy()
    tmp["decision_year"] = year
    return {
        "largest_year_share": share("decision_year") if len(tmp) else None,
        "largest_sector_share": share("sector"),
        "largest_liquidity_share": share("liquidity_bucket"),
    }


def _pool_metrics(pool: pd.DataFrame, top50_only: bool = False) -> dict[str, Any]:
    frame = pool.loc[pool["top50_membership"]] if top50_only else pool
    normalized = frame.rename(columns={"mae_20d": "mae20"}).copy()
    return _metrics(normalized)


def _delta(a: Any, b: Any) -> float | None:
    if a is None or b is None:
        return None
    return float(a - b)


def run_phase3(*, output_root: Path) -> dict[str, Any]:
    session_root = output_root / _session_id()
    session_root.mkdir(parents=True, exist_ok=True)
    signal_decision = _read_json(PHASE1C_DECISION)
    phase2d_decision = _read_json(PHASE2D_DECISION)
    signal = _prep_signal(pd.read_parquet(SIGNAL_ROWS))
    pool = _prep_pool(pd.read_parquet(POOL_ROWS))
    classified, membership = _classify(signal, pool)
    absent = classified.loc[classified["pool_membership_class"].eq("absent_from_candidate_pool")].copy()
    already = classified.loc[classified["pool_membership_class"].eq("already_in_candidate_pool")].copy()
    signal_only_metrics = _metrics(absent)
    signal_only = {
        "schema_version": f"{SCHEMA_VERSION}_signal_only_expectancy_v1",
        "generated_at_utc": _utc_now(),
        "signal_only_count": int(len(absent)),
        "metrics": signal_only_metrics,
        "concentration": _concentration(absent),
    }
    pool_ref = _pool_metrics(pool)
    top50_ref = _pool_metrics(pool, top50_only=True)
    phase1c_full = signal_decision.get("signal", {})
    baseline_2_deltas = signal_decision.get("baseline_2_deltas", {})
    references = {
        "schema_version": f"{SCHEMA_VERSION}_reference_comparison_v1",
        "generated_at_utc": _utc_now(),
        "existing_candidate_pool": pool_ref,
        "top50_pool_rows": top50_ref,
        "phase1c_mixed_full_set": phase1c_full,
        "phase1c_baseline_2_deltas": baseline_2_deltas,
        "signal_only_vs_candidate_pool": {
            "ret20_mean_delta": _delta(signal_only_metrics.get("ret20_mean"), pool_ref.get("ret20_mean")),
            "ret20_median_delta": _delta(signal_only_metrics.get("ret20_median"), pool_ref.get("ret20_median")),
            "win_rate20_delta": _delta(signal_only_metrics.get("win_rate_20"), pool_ref.get("win_rate_20")),
            "severe_loser_delta": _delta(signal_only_metrics.get("severe_loser_share"), pool_ref.get("severe_loser_share")),
        },
        "signal_only_vs_top50_pool": {
            "ret20_mean_delta": _delta(signal_only_metrics.get("ret20_mean"), top50_ref.get("ret20_mean")),
            "ret20_median_delta": _delta(signal_only_metrics.get("ret20_median"), top50_ref.get("ret20_median")),
            "win_rate20_delta": _delta(signal_only_metrics.get("win_rate_20"), top50_ref.get("win_rate_20")),
        },
        "signal_only_vs_phase1c_full": {
            "ret20_mean_delta": _delta(signal_only_metrics.get("ret20_mean"), phase1c_full.get("ret20_mean")),
            "ret20_median_delta": _delta(signal_only_metrics.get("ret20_median"), phase1c_full.get("ret20_median")),
            "win_rate20_delta": _delta(signal_only_metrics.get("win_rate_20"), phase1c_full.get("win_rate_20")),
        },
    }
    concentration = _concentration(absent)
    value_test = {
        "schema_version": f"{SCHEMA_VERSION}_candidate_generation_value_test_v1",
        "generated_at_utc": _utc_now(),
        "signal_only_count": int(len(absent)),
        "already_in_candidate_pool_count": int(len(already)),
        "unique_candidate_contribution": int(len(absent)),
        "signal_only_ret20_mean_delta_vs_candidate_pool": references["signal_only_vs_candidate_pool"]["ret20_mean_delta"],
        "signal_only_ret20_median_delta_vs_candidate_pool": references["signal_only_vs_candidate_pool"]["ret20_median_delta"],
        "signal_only_win_rate20_delta_vs_candidate_pool": references["signal_only_vs_candidate_pool"]["win_rate20_delta"],
        "severe_loser_delta_vs_candidate_pool": references["signal_only_vs_candidate_pool"]["severe_loser_delta"],
        "concentration": concentration,
        "risk_gates": {
            "count_ge_30": int(len(absent)) >= 30,
            "largest_sector_share_le_0_40": concentration.get("largest_sector_share") is not None and concentration["largest_sector_share"] <= 0.40,
            "largest_liquidity_share_le_0_50": concentration.get("largest_liquidity_share") is not None and concentration["largest_liquidity_share"] <= 0.50,
        },
    }
    beats_pool = (
        (references["signal_only_vs_candidate_pool"]["ret20_mean_delta"] or -999) > 0
        and (references["signal_only_vs_candidate_pool"]["ret20_median_delta"] or -999) > 0
        and (references["signal_only_vs_candidate_pool"]["win_rate20_delta"] or -999) > 0
    )
    gates_pass = all(value_test["risk_gates"].values())
    if membership["unsafe_to_classify_count"] == len(classified):
        decision = "blocked"
        reason = "no_signal_rows_within_verified_pool_period"
        allow_next = False
    elif beats_pool and gates_pass:
        decision = "proceed_to_candidate_generation_challenger"
        reason = "signal_only_candidates_beat_candidate_pool_with_acceptable_risk"
        allow_next = True
    elif int(len(absent)) >= 10 and (
        (references["signal_only_vs_candidate_pool"]["ret20_median_delta"] or -999) > 0
        or (references["signal_only_vs_candidate_pool"]["win_rate20_delta"] or -999) > 0
    ):
        decision = "hold_for_candidate_generation_design"
        reason = "directional_signal_only_value_but_not_full_gate"
        allow_next = False
    elif int(len(absent)) > 0:
        decision = "explain_only"
        reason = "signal_only_rows_do_not_beat_candidate_pool_references"
        allow_next = False
    else:
        decision = "blocked"
        reason = "no_absent_signal_only_rows_to_evaluate"
        allow_next = False
    decision_payload = {
        "schema_version": f"{SCHEMA_VERSION}_decision_v1",
        "generated_at_utc": _utc_now(),
        "authoritative_decision": decision,
        "decision_reason": reason,
        "signal_only_count": int(len(absent)),
        "already_in_candidate_pool_count": int(len(already)),
        "unsafe_to_classify_count": membership["unsafe_to_classify_count"],
        "candidate_generation_challenger_allowed_next": allow_next,
        "meemee_changed": False,
        "production_ranking_changed": False,
        "publish_changed": False,
        "ranking_challenger_created": False,
        "signal_definition_changed": False,
        "thresholds_changed": False,
    }
    artifacts = {
        "run_manifest.json": {
            "schema_version": f"{SCHEMA_VERSION}_manifest_v1",
            "generated_at_utc": _utc_now(),
            "script_name": SCRIPT_NAME,
            "session_root": str(session_root),
            "boundary": "TRADEX-only",
        },
        "input_resolution.json": {
            "schema_version": f"{SCHEMA_VERSION}_input_resolution_v1",
            "generated_at_utc": _utc_now(),
            "phase1c_root": str(PHASE1C_ROOT),
            "phase2c3_root": str(PHASE2C3_ROOT),
            "phase2d_root": str(PHASE2D_ROOT),
            "signal_rows": str(SIGNAL_ROWS),
            "candidate_pool_rows": str(POOL_ROWS),
            "phase1c_decision": signal_decision.get("authoritative_rollup_decision"),
            "phase2d_decision": phase2d_decision.get("authoritative_decision"),
        },
        "phase3_signal_pool_membership.json": membership,
        "phase3_signal_only_expectancy.json": signal_only,
        "phase3_reference_comparison.json": references,
        "phase3_candidate_generation_value_test.json": value_test,
        "phase3_decision.json": decision_payload,
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
    return {"session_root": str(session_root), "decision": decision, "signal_only_count": int(len(absent))}


def main() -> None:
    parser = argparse.ArgumentParser(description="TRADEX Phase 3 Iizuka mixed candidate-generation pretest")
    parser.add_argument("--output-root", default=str(DEFAULT_OUTPUT_ROOT))
    args = parser.parse_args()
    result = run_phase3(output_root=_safe_path(args.output_root, DEFAULT_OUTPUT_ROOT))
    print(json.dumps(base._json_ready(result), ensure_ascii=False, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
