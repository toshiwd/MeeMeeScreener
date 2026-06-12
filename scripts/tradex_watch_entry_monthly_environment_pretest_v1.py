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
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts import tradex_watch_to_entry_confirmation_pretest_v1 as pretest


AXIS_ID = "watch_entry_monthly_environment_pretest_v1"
DEFAULT_SOURCE = pretest.DEFAULT_SOURCE
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\watch_entry_monthly_environment_pretest_v1")
TOP_KS = (5, 10, 20)


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
    return {
        "row_count": int(len(rows)),
        "date_count": int(rows["as_of_date"].nunique()) if "as_of_date" in rows else 0,
        "code_count": int(rows["code"].astype(str).nunique()) if "code" in rows else 0,
        "ret20_mean": float(ret20.mean()) if not ret20.empty else None,
        "ret20_median": float(ret20.median()) if not ret20.empty else None,
        "winner_rate_ret20_gt_10pct": float((ret20 > 0.10).mean()) if not ret20.empty else None,
        "positive_rate_ret20_gt_0": float((ret20 > 0).mean()) if not ret20.empty else None,
        "bad_rate_ret20_lt_minus_5pct": float((ret20 < -0.05).mean()) if not ret20.empty else None,
        "severe_rate_ret20_lt_minus_10pct": float((ret20 < -0.10).mean()) if not ret20.empty else None,
    }


def _delta(left: dict[str, Any], right: dict[str, Any]) -> dict[str, Any]:
    keys = [
        "ret20_mean",
        "winner_rate_ret20_gt_10pct",
        "positive_rate_ret20_gt_0",
        "bad_rate_ret20_lt_minus_5pct",
        "severe_rate_ret20_lt_minus_10pct",
    ]
    return {key: None if left.get(key) is None or right.get(key) is None else left[key] - right[key] for key in keys}


def _topk(rows: pd.DataFrame, k: int) -> pd.DataFrame:
    return (
        rows.sort_values(["as_of_date", "entry_actionability_score"], ascending=[True, False])
        .groupby("as_of_date", group_keys=False)
        .head(k)
        .copy()
    )


def _monthly_masks(rows: pd.DataFrame) -> dict[str, pd.Series]:
    box = pd.to_numeric(rows["monthly_box_position"], errors="coerce")
    width = pd.to_numeric(rows["monthly_box_width_pct"], errors="coerce")
    monthly_vs_ma20 = pd.to_numeric(rows["monthly_close_vs_ma20_pct"], errors="coerce")
    monthly_slope = pd.to_numeric(rows["monthly_ma20_slope"], errors="coerce")
    supportive = rows["monthly_supportive_flag"].astype(bool)
    return {
        "monthly_supportive": supportive,
        "monthly_above_ma20": monthly_vs_ma20 >= 0,
        "monthly_supportive_or_above_ma20": supportive | (monthly_vs_ma20 >= 0),
        "monthly_not_overextended_box_high": box <= 0.85,
        "monthly_mid_to_high_not_extreme": box.between(0.35, 0.85, inclusive="both"),
        "monthly_constructive_pullback_zone": box.between(0.35, 0.70, inclusive="both") & (monthly_slope >= 0),
        "monthly_tight_box_not_high": (width <= 0.35) & (box <= 0.85),
        "monthly_supportive_and_not_extreme": supportive & (box <= 0.85),
    }


def _topk_compare(base: pd.DataFrame, gated: pd.DataFrame) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for k in TOP_KS:
        base_top = _topk(base, k)
        gated_top = _topk(gated, k)
        shared_dates = sorted(set(base_top["as_of_date"].astype(int)) & set(gated_top["as_of_date"].astype(int)))
        base_shared = base_top.loc[base_top["as_of_date"].astype(int).isin(shared_dates)].copy()
        gated_shared = gated_top.loc[gated_top["as_of_date"].astype(int).isin(shared_dates)].copy()
        bm = _metrics(base_shared)
        gm = _metrics(gated_shared)
        base_keys = set(zip(base_shared["as_of_date"].astype(int), base_shared["code"].astype(str)))
        gated_keys = set(zip(gated_shared["as_of_date"].astype(int), gated_shared["code"].astype(str)))
        out[str(k)] = {
            "shared_date_count": len(shared_dates),
            "base_promoted_topk": bm,
            "monthly_gated_topk": gm,
            "delta_monthly_gated_minus_base": _delta(gm, bm),
            "changed_members_count": len(base_keys.symmetric_difference(gated_keys)),
            "selection_divergence_reason": "same promoted Watch rule plus one monthly-environment gate",
        }
    return out


def _monthly_bucket_rows(rows: pd.DataFrame) -> list[dict[str, Any]]:
    work = rows.copy()
    work["box_bucket"] = pd.cut(
        pd.to_numeric(work["monthly_box_position"], errors="coerce"),
        bins=[-math.inf, 0.35, 0.70, 0.85, math.inf],
        labels=["low", "mid", "high", "extreme_high"],
    ).astype(str)
    work["monthly_supportive_bucket"] = work["monthly_supportive_flag"].astype(bool).map({True: "supportive", False: "not_supportive"})
    out = []
    for (support, box), group in work.groupby(["monthly_supportive_bucket", "box_bucket"], dropna=False, sort=True):
        out.append({"monthly_supportive_bucket": str(support), "box_bucket": str(box), **_metrics(group)})
    return out


def _decision(gates: dict[str, Any]) -> dict[str, Any]:
    keep: list[str] = []
    hold: list[str] = []
    drop: list[str] = []
    for name, item in gates.items():
        top10 = item["topk_comparison"]["10"]
        n = int(top10["monthly_gated_topk"]["row_count"] or 0)
        dates = int(top10["shared_date_count"] or 0)
        delta = top10["delta_monthly_gated_minus_base"]
        ret_delta = delta.get("ret20_mean")
        bad_delta = delta.get("bad_rate_ret20_lt_minus_5pct")
        severe_delta = delta.get("severe_rate_ret20_lt_minus_10pct")
        if n < 100 or dates < 50:
            hold.append(name)
        elif ret_delta is not None and ret_delta > 0.003 and bad_delta is not None and bad_delta <= -0.02 and severe_delta is not None and severe_delta <= -0.01:
            keep.append(name)
        else:
            drop.append(name)
    if keep:
        return {
            "candidate_local_decision": "keep",
            "kept_gates": keep,
            "held_gates": hold,
            "dropped_gates": drop,
            "reason": "monthly environment gate improved top10 return and downside under fixed promoted Watch conditions",
        }
    if hold:
        return {
            "candidate_local_decision": "hold",
            "kept_gates": [],
            "held_gates": hold,
            "dropped_gates": drop,
            "reason": "some monthly gates were underpowered but no keep threshold passed",
        }
    return {
        "candidate_local_decision": "drop",
        "kept_gates": [],
        "held_gates": [],
        "dropped_gates": drop,
        "reason": "no monthly environment gate improved return and downside together enough",
    }


def run(source: Path, output_root: Path) -> Path:
    rows = pd.read_parquet(source)
    rows["as_of_date"] = rows["as_of_date"].astype(int)
    rows["code"] = rows["code"].astype(str)
    watch = rows.loc[rows["review_bucket"].eq("Watch")].copy()
    promoted = watch.loc[pretest._variants(watch)["bullish_support_reclaim_volume_ok"]].copy()
    output = output_root / f"{_tag()}-{AXIS_ID}"
    output.mkdir(parents=True, exist_ok=False)
    gates: dict[str, Any] = {}
    for name, mask in _monthly_masks(promoted).items():
        gated = promoted.loc[mask.fillna(False)].copy()
        gated["monthly_environment_gate"] = name
        gates[name] = {
            "metrics": _metrics(gated),
            "comparison_vs_base_promoted": {
                "base": _metrics(promoted),
                "monthly_gated": _metrics(gated),
                "delta_monthly_gated_minus_base": _delta(_metrics(gated), _metrics(promoted)),
            },
            "topk_comparison": _topk_compare(promoted, gated),
            "monthly_bucket_rows": _monthly_bucket_rows(gated),
        }
    payload = {
        "axis_id": AXIS_ID,
        "source": str(source),
        "fixed_evaluation_conditions": {
            "source_surface": "practical_decision_support_bundle_v1 decision_support_surface.parquet",
            "source_bucket": "Watch only",
            "promotion_variant": "bullish_support_reclaim_volume_ok",
            "single_axis_changed": "monthly_environment_gate",
            "model_changed": False,
            "candidate_generation_changed": False,
            "runtime_db_write": False,
            "meemee_unchanged": True,
        },
        "base_promoted_metrics": _metrics(promoted),
        "base_monthly_bucket_rows": _monthly_bucket_rows(promoted),
        "gates": gates,
    }
    payload["decision"] = _decision(gates)
    promoted.to_csv(output / "base_promoted_rows.csv", index=False)
    _write_json(output / "monthly_environment_compare.json", payload)
    _write_json(
        output / "research_decision.json",
        {
            "decision_class": "READY_REVIEW_ONLY",
            "candidate_local_decision": payload["decision"]["candidate_local_decision"],
            "research_decision": "watch_entry_monthly_environment_pretest_ready_for_manual_review",
            "automatic_trade_action": False,
            "validated_buy_count": 0,
            "runtime_db_write": False,
            "meemee_unchanged": True,
            "production_ranking_changed": False,
            "candidate_generation_changed": False,
        },
    )
    _write_json(output_root / "latest_research_decision.json", {"run_root": str(output), "decision_class": "READY_REVIEW_ONLY"})
    return output


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", type=Path, default=DEFAULT_SOURCE)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    args = parser.parse_args()
    print(run(args.source, args.output_root))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
