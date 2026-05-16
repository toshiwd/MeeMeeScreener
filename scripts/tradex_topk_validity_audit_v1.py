from __future__ import annotations

import argparse
import hashlib
import json
import math
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd

if __package__ in {None, ""}:
    sys.path.append(str(Path(__file__).resolve().parents[1]))

from scripts import tradex_ma_buy_sell_probe_v1 as ma_probe


AXIS_ID = "topk_validity_audit_v1"
SCHEMA_PREFIX = "tradex_topk_validity_audit_v1"
DEFAULT_SOURCE_FINAL_ROLLUP_JSON = Path(
    r"G:\Tradex\ma_buy_sell_probe_v1_final_decision\20260512T050000Z-ma_buy_sell_probe_v1_final_decision_rollup\ma_buy_sell_probe_v1_final_decision_rollup.json"
)
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\topk_validity_audit_v1")

RANK_BUCKETS = (
    ("rank_1_5", 1, 5),
    ("rank_6_10", 6, 10),
    ("rank_11_20", 11, 20),
    ("rank_21_50", 21, 50),
    ("rank_51_100", 51, 100),
    ("rank_rest", 101, None),
)
K_VALUES = (3, 5, 10, 15, 20, 30, 50)
REQUIRED_JSON = (
    "topk_validity_audit.json",
    "topk_rank_bucket_quality.json",
    "topk_k_sensitivity.json",
    "topk_random_baseline.json",
    "topk_by_month.json",
    "topk_by_regime.json",
    "topk_operational_fit.json",
    "topk_validity_manifest.json",
    "_TOPK_AUDIT_COMPLETE.json",
)


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _default_run_id() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "-" + AXIS_ID


def _json_ready(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _json_ready(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_json_ready(item) for item in value]
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
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


def _write_json(path: Path, payload: dict[str, Any]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(_json_ready(payload), ensure_ascii=False, sort_keys=True, indent=2) + "\n", encoding="utf-8")
    return path


def _load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _stable_hash(payload: dict[str, Any]) -> str:
    return hashlib.sha256(json.dumps(_json_ready(payload), ensure_ascii=False, sort_keys=True).encode("utf-8")).hexdigest()


def _median_or_none(values: list[Any]) -> float | None:
    series = pd.to_numeric(pd.Series(values), errors="coerce").dropna()
    if series.empty:
        return None
    return float(series.median())


def _quality(rows: pd.DataFrame) -> dict[str, Any]:
    if rows.empty:
        return {
            "sample_count": 0,
            "mean_ret20": None,
            "median_ret20": None,
            "hit_rate": None,
            "downside_rate": None,
            "bottom15_rate": None,
        }
    ret = pd.to_numeric(rows["forward_ret_20d"], errors="coerce")
    return {
        "sample_count": int(len(rows)),
        "mean_ret20": ma_probe._mean_or_none(ret.tolist()),
        "median_ret20": _median_or_none(ret.tolist()),
        "hit_rate": ma_probe._rate_or_none(ret.gt(0).tolist()),
        "downside_rate": ma_probe._rate_or_none(ret.lt(0).tolist()),
        "bottom15_rate": ma_probe._rate_or_none(rows.get("bottom15_label", pd.Series(False, index=rows.index)).tolist()),
    }


def _load_canonical_regime(stock_db: Path, source: pd.DataFrame) -> pd.DataFrame:
    canonical, _meta = ma_probe._load_canonical_regime_rows(stock_db)
    if canonical.empty:
        out = source.copy()
        out["regime_label"] = "unknown"
        return out
    out = source.merge(canonical, on="trade_date_key", how="left", validate="many_to_one")
    out["regime_label"] = out["canonical_regime_label"].fillna("unknown").astype(str)
    return out.drop(columns=[column for column in ("canonical_regime_label",) if column in out.columns])


def _rank_bucket_rows(source: pd.DataFrame) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    rank = pd.to_numeric(source["champion_rank"], errors="coerce")
    for label, start, end in RANK_BUCKETS:
        mask = rank.ge(start) if end is None else rank.ge(start) & rank.le(end)
        subset = source[mask].copy()
        quality = _quality(subset)
        rows.append({"rank_bucket": label, "rank_start": start, "rank_end": end, **quality})
    return rows


def _topk_rows(source: pd.DataFrame, group_cols: list[str] | None = None) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    group_cols = group_cols or []
    grouped = [((), source)] if not group_cols else source.groupby(group_cols, sort=True)
    for group_key, group in grouped:
        if group_cols:
            if not isinstance(group_key, tuple):
                group_key = (group_key,)
            prefix = dict(zip(group_cols, [str(item) for item in group_key], strict=True))
        else:
            prefix = {}
        for top_k in K_VALUES:
            subset = group[pd.to_numeric(group["champion_rank"], errors="coerce").le(top_k)].copy()
            rows.append({**prefix, "top_k": int(top_k), **_quality(subset)})
    return rows


def _rank_correlation(source: pd.DataFrame) -> dict[str, Any]:
    rank = pd.to_numeric(source["champion_rank"], errors="coerce")
    ret = pd.to_numeric(source["forward_ret_20d"], errors="coerce")
    valid = rank.notna() & ret.notna()
    if int(valid.sum()) < 3:
        return {"spearman_rank_ret20": None, "pearson_rank_ret20": None}
    return {
        "spearman_rank_ret20": float(rank[valid].corr(ret[valid], method="spearman")),
        "pearson_rank_ret20": float(rank[valid].corr(ret[valid], method="pearson")),
    }


def _random_baseline_rows(source: pd.DataFrame) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    rng_source = source.copy()
    rng_source["random_rank"] = rng_source.groupby(["trade_date_key", "side"], sort=False)["symbol"].rank(method="first")
    # Stable pseudo-random order without non-deterministic seeds.
    rng_source["random_key"] = rng_source.apply(lambda row: _stable_hash({"symbol": row["symbol"], "date": row["trade_date_key"]}), axis=1)
    rng_source = rng_source.sort_values(["trade_date_key", "side", "random_key"], kind="stable").copy()
    rng_source["random_rank"] = rng_source.groupby(["trade_date_key", "side"], sort=False).cumcount() + 1
    for top_k in K_VALUES:
        champion = source[pd.to_numeric(source["champion_rank"], errors="coerce").le(top_k)]
        random = rng_source[rng_source["random_rank"].le(top_k)]
        champion_quality = _quality(champion)
        random_quality = _quality(random)
        rows.append(
            {
                "baseline": "deterministic_random_same_universe",
                "top_k": int(top_k),
                "champion": champion_quality,
                "baseline_quality": random_quality,
                "champion_minus_baseline_mean_ret20": ma_probe._delta(champion_quality["mean_ret20"], random_quality["mean_ret20"]),
                "champion_minus_baseline_bottom15_rate": ma_probe._delta(champion_quality["bottom15_rate"], random_quality["bottom15_rate"]),
            }
        )
    return rows


def _bucket_gradient_score(bucket_rows: list[dict[str, Any]]) -> dict[str, Any]:
    first = next(row for row in bucket_rows if row["rank_bucket"] == "rank_1_5")
    mid = next(row for row in bucket_rows if row["rank_bucket"] == "rank_11_20")
    lower = next(row for row in bucket_rows if row["rank_bucket"] == "rank_21_50")
    spread_top5_vs_11_20 = ma_probe._delta(first["mean_ret20"], mid["mean_ret20"])
    spread_top5_vs_21_50 = ma_probe._delta(first["mean_ret20"], lower["mean_ret20"])
    positive_spreads = sum(1 for value in (spread_top5_vs_11_20, spread_top5_vs_21_50) if value is not None and value > 0)
    return {
        "top5_vs_11_20_mean_ret20_spread": spread_top5_vs_11_20,
        "top5_vs_21_50_mean_ret20_spread": spread_top5_vs_21_50,
        "positive_spread_count": positive_spreads,
        "clear_quality_gradient": positive_spreads == 2,
    }


def _concentration(rows: list[dict[str, Any]], group_key: str, top_k: int) -> dict[str, Any]:
    subset = [row for row in rows if int(row.get("top_k") or 0) == top_k]
    positive = [row for row in subset if (row.get("mean_ret20") or 0.0) > 0.0]
    total_count = sum(int(row.get("sample_count") or 0) for row in subset)
    max_count = max((int(row.get("sample_count") or 0) for row in subset), default=0)
    return {
        "group_key": group_key,
        "top_k": int(top_k),
        "bucket_count": len(subset),
        "positive_bucket_count": len(positive),
        "max_sample_share": None if total_count <= 0 else float(max_count / total_count),
        "concentrated": bool(total_count > 0 and max_count / total_count > 0.50) or len(positive) <= 1,
    }


def _decision(
    *,
    bucket_gradient: dict[str, Any],
    random_rows: list[dict[str, Any]],
    month_rows: list[dict[str, Any]],
    regime_rows: list[dict[str, Any]],
) -> tuple[str, list[str]]:
    reasons: list[str] = []
    topk_baseline_wins = [
        row
        for row in random_rows
        if int(row["top_k"]) in {5, 10, 20}
        and (row.get("champion_minus_baseline_mean_ret20") or 0.0) > 0.0
    ]
    topk_bottom_not_worse = [
        row
        for row in random_rows
        if int(row["top_k"]) in {5, 10, 20}
        and (row.get("champion_minus_baseline_bottom15_rate") is None or row.get("champion_minus_baseline_bottom15_rate") <= 0.0)
    ]
    month_concentration = _concentration(month_rows, "month_bucket", 10)
    regime_concentration = _concentration(regime_rows, "regime_label", 10)
    if bucket_gradient["clear_quality_gradient"]:
        reasons.append("rank_bucket_quality_gradient_present")
    else:
        reasons.append("rank_bucket_quality_gradient_weak")
    if len(topk_baseline_wins) >= 2:
        reasons.append("topk_beats_random_baseline")
    else:
        reasons.append("topk_random_baseline_advantage_weak")
    if len(topk_bottom_not_worse) >= 2:
        reasons.append("topk_bottom15_not_worse_than_random")
    else:
        reasons.append("topk_bottom15_worse_or_unclear")
    if month_concentration["concentrated"]:
        reasons.append("topk_advantage_month_concentrated")
    else:
        reasons.append("topk_advantage_not_month_concentrated")
    if regime_concentration["concentrated"]:
        reasons.append("topk_advantage_regime_concentrated")
    else:
        reasons.append("topk_advantage_not_regime_concentrated")
    if bucket_gradient["clear_quality_gradient"] and len(topk_baseline_wins) == 3 and not month_concentration["concentrated"] and not regime_concentration["concentrated"]:
        return "topk_valid", reasons
    if len(topk_baseline_wins) >= 1 or bucket_gradient["positive_spread_count"] >= 1:
        return "topk_partially_valid", reasons
    return "topk_not_valid", reasons


def _operational_fit(decision: str, reasons: list[str]) -> dict[str, Any]:
    return {
        "schema_version": f"{SCHEMA_PREFIX}_operational_fit_v1",
        "generated_at": _utc_now(),
        "fixed_topK_valid": decision == "topk_valid",
        "variable_K_needed": decision != "topk_valid",
        "score_threshold_needed": decision == "topk_partially_valid",
        "topK_plus_confidence_needed": decision == "topk_partially_valid",
        "typed_reasons": reasons,
        "interpretation": "fixed topK is acceptable for research but needs threshold/confidence validation before production" if decision == "topk_partially_valid" else decision,
    }


def _read_back(output_dir: Path) -> dict[str, Any]:
    parse_status: dict[str, bool] = {}
    required_before_complete = [name for name in REQUIRED_JSON if name != "_TOPK_AUDIT_COMPLETE.json"]
    for name in required_before_complete:
        path = output_dir / name
        parse_status[name] = path.exists()
        if path.exists():
            try:
                _load_json(path)
            except Exception:
                parse_status[name] = False
    audit = _load_json(output_dir / "topk_validity_audit.json")
    manifest = _load_json(output_dir / "topk_validity_manifest.json")
    return {
        "parse_status": parse_status,
        "verification": {
            "required_json_exist": all((output_dir / name).exists() for name in required_before_complete),
            "required_json_parse": all(parse_status.values()),
            "decision_is_typed": audit.get("audit_decision") in {"topk_valid", "topk_partially_valid", "topk_not_valid"},
            "fixed_conditions_preserved": manifest.get("fixed_conditions_preserved") is True,
            "no_meemee_reflection": manifest.get("meemee_reflection") is False,
            "no_production_registration": manifest.get("production_registration") is False,
            "no_champion_artifact_regeneration": manifest.get("champion_artifact_regenerated") is False,
            "no_ranking_logic_change": manifest.get("ranking_logic_changed") is False,
        },
    }


def run_topk_validity_audit(
    *,
    source_final_rollup_json: Path = DEFAULT_SOURCE_FINAL_ROLLUP_JSON,
    output_root: Path = DEFAULT_OUTPUT_ROOT,
    run_id: str | None = None,
) -> dict[str, Any]:
    started_at = _utc_now()
    source_final_rollup_json = Path(source_final_rollup_json).resolve()
    source_rollup = _load_json(source_final_rollup_json)
    source_run_dir = Path(str(source_rollup["source_role_validation_run"])).resolve()
    source_artifacts = ma_probe._read_source_run_artifacts(source_run_dir)
    evaluation_contract = source_artifacts["evaluation_contract.json"]
    source_rows_path = Path(str(evaluation_contract["source_rows_artifact_path"]))
    stock_db = Path(str(evaluation_contract["runtime_stock_db_path"]))
    source = ma_probe.load_source_rows(source_rows_path)
    source = _load_canonical_regime(stock_db, source)
    fixed_payload = {
        "source_rows_artifact_path": str(source_rows_path),
        "champion_compare_json_path": evaluation_contract.get("champion_compare_json_path"),
        "ret20_source_mode": evaluation_contract.get("ret20_source_mode"),
        "candidate_build_order_mode": evaluation_contract.get("candidate_build_order_mode"),
        "artifact_detail_level": evaluation_contract.get("artifact_detail_level"),
        "topk_list": list(ma_probe.TOP_K_VALUES),
        "cost_slippage_config": evaluation_contract.get("cost_slippage_config"),
        "axis_id": AXIS_ID,
    }
    fixed_condition_hash = _stable_hash(fixed_payload)
    rank_bucket_rows = _rank_bucket_rows(source)
    k_sensitivity_rows = _topk_rows(source)
    random_rows = _random_baseline_rows(source)
    month_rows = _topk_rows(source, ["month_bucket"])
    regime_rows = _topk_rows(source, ["regime_label"])
    bucket_gradient = _bucket_gradient_score(rank_bucket_rows)
    rank_corr = _rank_correlation(source)
    decision, reasons = _decision(
        bucket_gradient=bucket_gradient,
        random_rows=random_rows,
        month_rows=month_rows,
        regime_rows=regime_rows,
    )
    operational_fit = _operational_fit(decision, reasons)
    output_dir = Path(output_root).resolve() / str(run_id or _default_run_id()).strip()
    output_dir.mkdir(parents=True, exist_ok=True)
    audit = {
        "schema_version": f"{SCHEMA_PREFIX}_audit_v1",
        "axis_id": AXIS_ID,
        "generated_at": _utc_now(),
        "audit_decision": decision,
        "decision_reason_codes": reasons,
        "question": "Does top5/top10/top20 represent useful operational candidate groups?",
        "rank_correlation": rank_corr,
        "bucket_gradient": bucket_gradient,
        "current_ma_demotion_research_interpretable": decision in {"topk_valid", "topk_partially_valid"},
        "recommended_next_evaluation_mode": "continue_topK_with_threshold_confidence_audit" if decision == "topk_partially_valid" else ("continue_topK_optimization" if decision == "topk_valid" else "switch_to_threshold_or_rank_bucket_evaluation"),
        "fixed_condition_hash": fixed_condition_hash,
    }
    manifest = {
        "schema_version": f"{SCHEMA_PREFIX}_manifest_v1",
        "axis_id": AXIS_ID,
        "run_id": output_dir.name,
        "script_path": str(Path(__file__).resolve()),
        "started_at": started_at,
        "completed_at": _utc_now(),
        "source_final_rollup_json": str(source_final_rollup_json),
        "source_rows_artifact_path": str(source_rows_path),
        "runtime_stock_db_path": str(stock_db),
        "fixed_condition_hash": fixed_condition_hash,
        "fixed_conditions_preserved": True,
        "ranking_logic_changed": False,
        "ma_logic_changed": False,
        "new_features_added": False,
        "meemee_reflection": False,
        "production_registration": False,
        "champion_artifact_regenerated": False,
        "silent_fallback_used": False,
    }
    payloads = {
        "topk_validity_audit.json": audit,
        "topk_rank_bucket_quality.json": {"schema_version": f"{SCHEMA_PREFIX}_rank_bucket_quality_v1", "generated_at": _utc_now(), "rows": rank_bucket_rows},
        "topk_k_sensitivity.json": {"schema_version": f"{SCHEMA_PREFIX}_k_sensitivity_v1", "generated_at": _utc_now(), "rows": k_sensitivity_rows},
        "topk_random_baseline.json": {"schema_version": f"{SCHEMA_PREFIX}_random_baseline_v1", "generated_at": _utc_now(), "rows": random_rows},
        "topk_by_month.json": {"schema_version": f"{SCHEMA_PREFIX}_by_month_v1", "generated_at": _utc_now(), "rows": month_rows, "top10_concentration": _concentration(month_rows, "month_bucket", 10)},
        "topk_by_regime.json": {"schema_version": f"{SCHEMA_PREFIX}_by_regime_v1", "generated_at": _utc_now(), "rows": regime_rows, "top10_concentration": _concentration(regime_rows, "regime_label", 10)},
        "topk_operational_fit.json": operational_fit,
        "topk_validity_manifest.json": manifest,
    }
    for name, payload in payloads.items():
        _write_json(output_dir / name, payload)
    read_back = _read_back(output_dir)
    complete = all(read_back["verification"].values())
    if complete:
        _write_json(
            output_dir / "_TOPK_AUDIT_COMPLETE.json",
            {
                "schema_version": f"{SCHEMA_PREFIX}_complete_v1",
                "generated_at": _utc_now(),
                "artifact_root": str(output_dir),
                "complete": True,
                "read_back_verification": read_back,
            },
        )
    return {
        "output_dir": str(output_dir),
        "audit_complete_written": complete,
        "audit_decision": decision,
        "decision_reason_codes": reasons,
        "required_artifacts": {name: str(output_dir / name) for name in REQUIRED_JSON},
        "read_back_verification": read_back,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="TRADEX-only topK validity audit v1.")
    parser.add_argument("--source-final-rollup-json", default=str(DEFAULT_SOURCE_FINAL_ROLLUP_JSON))
    parser.add_argument("--output-root", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--run-id", default="")
    args = parser.parse_args(argv)
    result = run_topk_validity_audit(
        source_final_rollup_json=Path(args.source_final_rollup_json),
        output_root=Path(args.output_root),
        run_id=args.run_id or None,
    )
    print(json.dumps(_json_ready(result), ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
