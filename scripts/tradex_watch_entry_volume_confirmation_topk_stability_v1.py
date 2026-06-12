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


AXIS_ID = "watch_entry_volume_confirmation_topk_stability_v1"
DEFAULT_SOURCE = pretest.DEFAULT_SOURCE
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\watch_entry_volume_confirmation_topk_stability_v1")
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
    if rows.empty:
        return rows.copy()
    return (
        rows.sort_values(["as_of_date", "entry_actionability_score"], ascending=[True, False])
        .groupby("as_of_date", group_keys=False)
        .head(k)
        .copy()
    )


def _per_month(rows: pd.DataFrame) -> dict[str, Any]:
    out: dict[str, Any] = {}
    if rows.empty:
        return out
    ym = rows["as_of_date"].astype(str).str[:6]
    for month, group in rows.groupby(ym, sort=True):
        out[str(month)] = _metrics(group)
    return out


def _topk_compare(watch: pd.DataFrame, promoted: pd.DataFrame) -> dict[str, Any]:
    out: dict[str, Any] = {}
    promoted_keys = set(zip(promoted["as_of_date"].astype(int), promoted["code"].astype(str)))
    for k in TOP_KS:
        champion = _topk(watch, k)
        challenger = _topk(promoted, k)
        champion_keys = set(zip(champion["as_of_date"].astype(int), champion["code"].astype(str)))
        challenger_keys = set(zip(challenger["as_of_date"].astype(int), challenger["code"].astype(str)))
        shared_dates = sorted(set(champion["as_of_date"].astype(int)) & set(challenger["as_of_date"].astype(int)))
        champion_on_shared = champion.loc[champion["as_of_date"].astype(int).isin(shared_dates)].copy()
        challenger_on_shared = challenger.loc[challenger["as_of_date"].astype(int).isin(shared_dates)].copy()
        cm = _metrics(champion_on_shared)
        chm = _metrics(challenger_on_shared)
        out[str(k)] = {
            "shared_date_count": len(shared_dates),
            "champion_all_watch_topk": cm,
            "challenger_promoted_topk": chm,
            "delta_challenger_minus_champion": _delta(chm, cm),
            "changed_members_count": len(challenger_keys.symmetric_difference(champion_keys)),
            "promoted_members_in_champion_count": len(champion_keys & promoted_keys),
            "challenger_unique_member_count": len(challenger_keys - champion_keys),
            "champion_unique_member_count": len(champion_keys - challenger_keys),
            "selection_divergence_reason": "challenger ranks only Watch rows passing bullish support reclaim with non-overheated volume",
        }
    return out


def _decision(topk: dict[str, Any], promoted: pd.DataFrame) -> dict[str, Any]:
    k10 = topk.get("10", {})
    delta = k10.get("delta_challenger_minus_champion", {})
    n = int(k10.get("challenger_promoted_topk", {}).get("row_count") or 0)
    ret_delta = delta.get("ret20_mean")
    bad_delta = delta.get("bad_rate_ret20_lt_minus_5pct")
    severe_delta = delta.get("severe_rate_ret20_lt_minus_10pct")
    monthly = _per_month(promoted)
    month_count = len(monthly)
    negative_months = sum(1 for item in monthly.values() if (item.get("ret20_mean") or 0) < 0)
    if n < 100 or month_count < 12:
        return {
            "candidate_local_decision": "hold",
            "reason": "topK or month coverage is underpowered",
            "negative_months": negative_months,
            "month_count": month_count,
        }
    if ret_delta is not None and ret_delta > 0 and bad_delta is not None and bad_delta < 0 and severe_delta is not None and severe_delta <= 0:
        return {
            "candidate_local_decision": "keep",
            "reason": "promoted top10 improved mean return and reduced bad/severe rates versus all-Watch top10 on shared dates",
            "negative_months": negative_months,
            "month_count": month_count,
        }
    return {
        "candidate_local_decision": "drop",
        "reason": "promoted top10 did not improve return and downside together versus all-Watch top10",
        "negative_months": negative_months,
        "month_count": month_count,
    }


def run(source: Path, output_root: Path) -> Path:
    rows = pd.read_parquet(source)
    rows["as_of_date"] = rows["as_of_date"].astype(int)
    rows["code"] = rows["code"].astype(str)
    watch = rows.loc[rows["review_bucket"].eq("Watch")].copy()
    promoted = watch.loc[pretest._variants(watch)["bullish_support_reclaim_volume_ok"]].copy()
    promoted["promotion_variant"] = "bullish_support_reclaim_volume_ok"
    output = output_root / f"{_tag()}-{AXIS_ID}"
    output.mkdir(parents=True, exist_ok=False)

    topk = _topk_compare(watch, promoted)
    payload = {
        "axis_id": AXIS_ID,
        "source": str(source),
        "fixed_evaluation_conditions": {
            "source_surface": "practical_decision_support_bundle_v1 decision_support_surface.parquet",
            "review_start": 20240101,
            "source_bucket": "Watch only",
            "promotion_variant": "bullish_support_reclaim_volume_ok",
            "model_changed": False,
            "candidate_generation_changed": False,
            "runtime_db_write": False,
            "meemee_unchanged": True,
            "labels_used_only_for_evaluation": ["ret20", "winner_ret20_gt_10pct", "bad_ret20_lt_minus_5pct", "severe_ret20_lt_minus_10pct"],
        },
        "all_watch_metrics": _metrics(watch),
        "promoted_metrics": _metrics(promoted),
        "monthly_promoted_metrics": _per_month(promoted),
        "topk_comparison": topk,
    }
    payload["decision"] = _decision(topk, promoted)
    promoted.to_csv(output / "promoted_watch_rows.csv", index=False)
    _write_json(output / "topk_stability_compare.json", payload)
    _write_json(
        output / "research_decision.json",
        {
            "decision_class": "READY_REVIEW_ONLY",
            "candidate_local_decision": payload["decision"]["candidate_local_decision"],
            "research_decision": "watch_entry_volume_confirmation_topk_stability_ready_for_manual_review",
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
