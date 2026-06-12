from __future__ import annotations

import argparse
import json
import math
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd


AXIS_ID = "watch_to_entry_confirmation_pretest_v1"
DEFAULT_SOURCE = Path(
    r"G:\Tradex\practical_decision_support_bundle_v1"
    r"\20260602T105354Z-practical_decision_support_bundle_v1"
    r"\decision_support_surface.parquet"
)
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\watch_to_entry_confirmation_pretest_v1")


def _tag() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _json_ready(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(k): _json_ready(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_ready(v) for v in value]
    if isinstance(value, Path):
        return str(value)
    if hasattr(value, "item"):
        return _json_ready(value.item())
    if isinstance(value, float) and not math.isfinite(value):
        return None
    try:
        if pd.isna(value):
            return None
    except Exception:
        pass
    return value


def _write_json(path: Path, payload: Any) -> None:
    path.write_text(json.dumps(_json_ready(payload), ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _metrics(rows: pd.DataFrame) -> dict[str, Any]:
    ret20 = pd.to_numeric(rows.get("ret20"), errors="coerce").dropna()
    ret5 = pd.to_numeric(rows.get("ret5"), errors="coerce").dropna()
    return {
        "row_count": int(len(rows)),
        "date_count": int(rows["as_of_date"].nunique()) if "as_of_date" in rows else 0,
        "code_count": int(rows["code"].astype(str).nunique()) if "code" in rows else 0,
        "ret5_mean": float(ret5.mean()) if not ret5.empty else None,
        "ret20_mean": float(ret20.mean()) if not ret20.empty else None,
        "ret20_median": float(ret20.median()) if not ret20.empty else None,
        "winner_rate_ret20_gt_10pct": float((ret20 > 0.10).mean()) if not ret20.empty else None,
        "positive_rate_ret20_gt_0": float((ret20 > 0).mean()) if not ret20.empty else None,
        "bad_rate_ret20_lt_minus_5pct": float((ret20 < -0.05).mean()) if not ret20.empty else None,
        "severe_rate_ret20_lt_minus_10pct": float((ret20 < -0.10).mean()) if not ret20.empty else None,
    }


def _compare(left: pd.DataFrame, right: pd.DataFrame) -> dict[str, Any]:
    lm = _metrics(left)
    rm = _metrics(right)
    return {
        "left": lm,
        "right": rm,
        "ret20_mean_delta": None if lm["ret20_mean"] is None or rm["ret20_mean"] is None else lm["ret20_mean"] - rm["ret20_mean"],
        "winner_rate_delta": None
        if lm["winner_rate_ret20_gt_10pct"] is None or rm["winner_rate_ret20_gt_10pct"] is None
        else lm["winner_rate_ret20_gt_10pct"] - rm["winner_rate_ret20_gt_10pct"],
        "bad_rate_delta": None
        if lm["bad_rate_ret20_lt_minus_5pct"] is None or rm["bad_rate_ret20_lt_minus_5pct"] is None
        else lm["bad_rate_ret20_lt_minus_5pct"] - rm["bad_rate_ret20_lt_minus_5pct"],
        "severe_rate_delta": None
        if lm["severe_rate_ret20_lt_minus_10pct"] is None or rm["severe_rate_ret20_lt_minus_10pct"] is None
        else lm["severe_rate_ret20_lt_minus_10pct"] - rm["severe_rate_ret20_lt_minus_10pct"],
        "sample_allows_comparison": len(left) >= 30 and len(right) >= 30,
    }


def _variants(watch: pd.DataFrame) -> dict[str, pd.Series]:
    clean_high = (~watch["failed_high_flag"].astype(bool)) & (~watch["bearish_body_flag"].astype(bool)) & (watch["upper_wick_ratio"] <= 0.35)
    ma20_reclaim = (watch["close_above_ma20"].astype(bool)) & (watch["ma20_slope_10d"] >= 0)
    shallow_pullback = watch["close_vs_ma20_pct"].between(-0.02, 0.06, inclusive="both")
    support_reclaim = ma20_reclaim & shallow_pullback & clean_high
    bullish_confirmation = support_reclaim & (watch["bullish_body_flag"].astype(bool) | (watch["lower_wick_ratio"] >= 0.35))
    volume_confirmation = bullish_confirmation & watch["volume_vs_20d_avg"].between(0.8, 2.5, inclusive="both")
    weekly_alignment = bullish_confirmation & watch["weekly_supportive_flag"].astype(bool)
    monthly_weekly_alignment = weekly_alignment & watch["monthly_supportive_flag"].astype(bool)
    probability_tight = (
        bullish_confirmation
        & (watch["upside_probability_20d"] >= 0.56)
        & (watch["downside_risk_probability_20d"] <= 0.49)
        & (watch["entry_actionability_score"] >= 0.07)
    )
    return {
        "ma20_reclaim_clean": ma20_reclaim & clean_high,
        "support_reclaim_clean": support_reclaim,
        "bullish_support_reclaim": bullish_confirmation,
        "bullish_support_reclaim_volume_ok": volume_confirmation,
        "bullish_support_reclaim_weekly_supportive": weekly_alignment,
        "bullish_support_reclaim_monthly_weekly_supportive": monthly_weekly_alignment,
        "bullish_support_reclaim_probability_tight": probability_tight,
    }


def _decision(results: dict[str, Any]) -> dict[str, Any]:
    keep: list[str] = []
    hold: list[str] = []
    drop: list[str] = []
    for name, item in results["variants"].items():
        comp = item["comparison_vs_all_watch"]
        n = item["metrics"]["row_count"]
        ret_delta = comp["ret20_mean_delta"]
        bad_delta = comp["bad_rate_delta"]
        severe_delta = comp["severe_rate_delta"]
        winner_delta = comp["winner_rate_delta"]
        if n < 100:
            hold.append(name)
            continue
        improves_return = ret_delta is not None and ret_delta > 0.005
        improves_bad = bad_delta is not None and bad_delta <= -0.01
        not_worse_severe = severe_delta is not None and severe_delta <= 0.0
        improves_winner = winner_delta is not None and winner_delta >= 0.0
        if improves_return and improves_bad and not_worse_severe and improves_winner:
            keep.append(name)
        else:
            drop.append(name)
    if keep:
        return {
            "candidate_local_decision": "keep",
            "kept_variants": keep,
            "held_variants": hold,
            "dropped_variants": drop,
            "reason": "watch promotion condition improved return and reduced bad-rate under same review surface",
        }
    if hold:
        return {
            "candidate_local_decision": "hold",
            "kept_variants": [],
            "held_variants": hold,
            "dropped_variants": drop,
            "reason": "some variants were underpowered but no same-condition keep threshold passed",
        }
    return {
        "candidate_local_decision": "drop",
        "kept_variants": [],
        "held_variants": [],
        "dropped_variants": drop,
        "reason": "no watch promotion condition improved return and downside together under same conditions",
    }


def run(source: Path, output_root: Path) -> Path:
    rows = pd.read_parquet(source)
    rows["as_of_date"] = rows["as_of_date"].astype(int)
    rows["code"] = rows["code"].astype(str)
    watch = rows.loc[rows["review_bucket"].eq("Watch")].copy()
    if watch.empty:
        raise ValueError("Watch cohort is empty")
    out = output_root / f"{_tag()}-{AXIS_ID}"
    out.mkdir(parents=True, exist_ok=False)

    all_watch_metrics = _metrics(watch)
    variant_masks = _variants(watch)
    variants: dict[str, Any] = {}
    selected_parts: list[pd.DataFrame] = []
    for name, mask in variant_masks.items():
        selected = watch.loc[mask].copy()
        rejected = watch.loc[~mask].copy()
        selected["promotion_variant"] = name
        selected_parts.append(selected)
        yearly = {
            str(year): _compare(group, watch.loc[watch["as_of_date"].astype(str).str[:4].astype(int).eq(year)])
            for year, group in selected.groupby(selected["as_of_date"].astype(str).str[:4].astype(int), sort=True)
        }
        variants[name] = {
            "metrics": _metrics(selected),
            "comparison_vs_all_watch": _compare(selected, watch),
            "comparison_vs_rejected_watch": _compare(selected, rejected),
            "yearly_comparison_vs_same_year_watch": yearly,
            "rule": {
                "source_bucket": "Watch",
                "uses_future_outcomes_for_selection": False,
                "labels_used_only_for_evaluation": ["ret5", "ret20"],
            },
        }

    promoted = pd.concat(selected_parts, ignore_index=True) if selected_parts else pd.DataFrame()
    promoted.to_csv(out / "watch_promotion_rows.csv", index=False)
    results = {
        "axis_id": AXIS_ID,
        "source": str(source),
        "fixed_evaluation_conditions": {
            "source_surface": "practical_decision_support_bundle_v1 decision_support_surface.parquet",
            "review_start": 20240101,
            "source_bucket": "Watch only",
            "labels": ["ret5", "ret20", "winner_ret20_gt_10pct", "bad_ret20_lt_minus_5pct", "severe_ret20_lt_minus_10pct"],
            "candidate_generation_changed": False,
            "model_changed": False,
            "meemee_unchanged": True,
            "runtime_db_write": False,
        },
        "all_watch_metrics": all_watch_metrics,
        "variants": variants,
    }
    results["decision"] = _decision(results)
    _write_json(out / "watch_to_entry_confirmation_compare.json", results)
    _write_json(
        out / "research_decision.json",
        {
            "decision_class": "READY_REVIEW_ONLY",
            "candidate_local_decision": results["decision"]["candidate_local_decision"],
            "research_decision": "watch_to_entry_confirmation_pretest_ready_for_manual_review",
            "automatic_trade_action": False,
            "validated_buy_count": 0,
            "runtime_db_write": False,
            "meemee_unchanged": True,
            "production_ranking_changed": False,
            "candidate_generation_changed": False,
        },
    )
    _write_json(output_root / "latest_research_decision.json", {"run_root": str(out), "decision_class": "READY_REVIEW_ONLY"})
    return out


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", type=Path, default=DEFAULT_SOURCE)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    args = parser.parse_args()
    print(run(args.source, args.output_root))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
