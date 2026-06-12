from __future__ import annotations

import argparse
import json
import math
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd


AXIS_ID = "context_risk_not_clean_bad_pick_challenger_v1"
DEFAULT_SOURCE_DECISION = Path("G:/Tradex/bad_pick_single_axis_goal_selection_v1/20260604T023300Z-bad-pick-single-axis-goal-selection-v1/final_research_decision.json")
DEFAULT_REPLAY_ROWS = Path("G:/Tradex/current_buyable_historical_operational_replay_v1/20260526T014356Z-current-buyable-historical-operational-replay-v1/historical_operational_replay_rows.csv")
DEFAULT_OUT_ROOT = Path("G:/Tradex/context_risk_not_clean_bad_pick_challenger_v1")
REQUIRED = (
    "final_research_decision.json",
    "compare.json",
    "challenger_effect_summary.json",
    "replacement_quality_summary.json",
    "_ARTIFACT_COMPLETE.json",
)
TOPKS = (5, 10, 20)


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


def _median(series: pd.Series) -> float | None:
    valid = pd.to_numeric(series, errors="coerce").dropna()
    return None if valid.empty else float(valid.median())


def _rate(series: pd.Series) -> float | None:
    valid = series.dropna()
    return None if valid.empty else float(valid.astype(bool).mean())


def _load_rows(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path)
    df["code"] = df["code"].astype(str)
    df["as_of_date"] = df["as_of_date"].astype(str)
    df["baseline_rank_source"] = pd.to_numeric(df["fresh_runtime_research_watch_rank"], errors="coerce")
    df["baseline_top_rank"] = df.groupby("as_of_date")["baseline_rank_source"].rank(method="first")
    df["ret20"] = pd.to_numeric(df["ret20"], errors="coerce")
    df["hit_flag"] = df["ret20"] > 0
    df["bad_pick_flag"] = df["ret20"] <= -0.05
    df["severe_loss_flag"] = df["ret20"] <= -0.10
    df["context_risk_not_clean"] = ~df["variant_c_combined_context_risk_clean"].fillna(False).astype(bool)
    # Single-axis demotion: keep same rows and date groups, move only flagged rows behind clean rows.
    df = df.sort_values(["as_of_date", "context_risk_not_clean", "baseline_rank_source", "code"], ascending=[True, True, True, True], kind="stable").copy()
    df["challenger_rank"] = df.groupby("as_of_date").cumcount() + 1
    return df


def _metrics(part: pd.DataFrame, label: str, topk: int) -> dict[str, Any]:
    return {
        "label": label,
        "topk": topk,
        "row_count": int(len(part)),
        "unique_date_count": int(part["as_of_date"].nunique()),
        "unique_symbol_count": int(part["code"].nunique()),
        "mean_ret20": _mean(part["ret20"]),
        "median_ret20": _median(part["ret20"]),
        "hit_rate": _rate(part["hit_flag"]),
        "bad_pick_rate": _rate(part["bad_pick_flag"]),
        "severe_loss_rate": _rate(part["severe_loss_flag"]),
        "context_risk_not_clean_rate": _rate(part["context_risk_not_clean"]),
    }


def _compare(df: pd.DataFrame) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    metrics = []
    deltas = []
    replacement_rows = []
    effect_rows = []
    for topk in TOPKS:
        b = df[df["baseline_top_rank"] <= topk]
        c = df[df["challenger_rank"] <= topk]
        bm = _metrics(b, "baseline", topk)
        cm = _metrics(c, "context_risk_not_clean_demotion_challenger", topk)
        metrics.extend([bm, cm])
        delta = {"topk": topk}
        for key in ["mean_ret20", "median_ret20", "hit_rate", "bad_pick_rate", "severe_loss_rate", "context_risk_not_clean_rate"]:
            delta[f"{key}_delta"] = (cm[key] - bm[key]) if cm[key] is not None and bm[key] is not None else None
        deltas.append(delta)
    for date, g in df.groupby("as_of_date", sort=True):
        for topk in TOPKS:
            bset = set(g.loc[g["baseline_top_rank"] <= topk, "code"])
            cset = set(g.loc[g["challenger_rank"] <= topk, "code"])
            removed = g[g["code"].isin(bset - cset)]
            added = g[g["code"].isin(cset - bset)]
            common = g[g["code"].isin(bset & cset)]
            effect_rows.append({
                "as_of_date": date,
                "topk": topk,
                "changed_members_count": int(len(bset ^ cset)),
                "removed_count": int(len(removed)),
                "added_count": int(len(added)),
                "within_topk_reordered_count": int((common["baseline_top_rank"] != common["challenger_rank"]).sum()),
                "bad_pick_removed_count": int(removed["bad_pick_flag"].sum()),
                "severe_loss_removed_count": int(removed["severe_loss_flag"].sum()),
                "good_pick_false_removed_count": int((removed["ret20"] > 0).sum()),
                "bad_pick_added_count": int(added["bad_pick_flag"].sum()),
                "good_pick_added_count": int((added["ret20"] > 0).sum()),
                "removed_mean_ret20": _mean(removed["ret20"]),
                "added_mean_ret20": _mean(added["ret20"]),
                "replacement_ret20_advantage": (_mean(added["ret20"]) - _mean(removed["ret20"])) if _mean(added["ret20"]) is not None and _mean(removed["ret20"]) is not None else None,
            })
            if len(removed) or len(added):
                replacement_rows.append(effect_rows[-1])
    effect = pd.DataFrame(effect_rows)
    replacement = pd.DataFrame(replacement_rows)
    effect_summary = {
        "topk": [],
        "changed_rank_count": int((df["baseline_top_rank"] != df["challenger_rank"]).sum()),
        "flagged_rows_total": int(df["context_risk_not_clean"].sum()),
    }
    replacement_summary = {"topk": []}
    for topk in TOPKS:
        ep = effect[effect["topk"] == topk]
        rp = replacement[replacement["topk"] == topk] if not replacement.empty else pd.DataFrame()
        effect_summary["topk"].append({
            "topk": topk,
            "changed_members_count": int(ep["changed_members_count"].sum()),
            "dates_with_member_branching": int((ep["changed_members_count"] > 0).sum()),
            "within_topk_reordered_count": int(ep["within_topk_reordered_count"].sum()),
            "bad_pick_removed_count": int(ep["bad_pick_removed_count"].sum()),
            "severe_loss_removed_count": int(ep["severe_loss_removed_count"].sum()),
            "good_pick_false_removed_count": int(ep["good_pick_false_removed_count"].sum()),
            "bad_pick_added_count": int(ep["bad_pick_added_count"].sum()),
        })
        replacement_summary["topk"].append({
            "topk": topk,
            "replacement_event_count": int(len(rp)),
            "mean_replacement_ret20_advantage": _mean(rp["replacement_ret20_advantage"]) if not rp.empty else None,
            "median_replacement_ret20_advantage": _median(rp["replacement_ret20_advantage"]) if not rp.empty else None,
            "removed_bad_pick_count": int(rp["bad_pick_removed_count"].sum()) if not rp.empty else 0,
            "removed_severe_loss_count": int(rp["severe_loss_removed_count"].sum()) if not rp.empty else 0,
            "false_removed_good_pick_count": int(rp["good_pick_false_removed_count"].sum()) if not rp.empty else 0,
            "added_bad_pick_count": int(rp["bad_pick_added_count"].sum()) if not rp.empty else 0,
        })
    return {"metrics": metrics, "deltas": deltas}, effect_summary, replacement_summary


def _decision(compare: dict[str, Any], effect: dict[str, Any]) -> tuple[str, str]:
    top5 = next(d for d in compare["deltas"] if d["topk"] == 5)
    top10 = next(d for d in compare["deltas"] if d["topk"] == 10)
    e10 = next(x for x in effect["topk"] if x["topk"] == 10)
    e5 = next(x for x in effect["topk"] if x["topk"] == 5)
    enough_branching = e10["changed_members_count"] >= 10 or e5["changed_members_count"] >= 8
    risk_improved = (top10["severe_loss_rate_delta"] or 0) < 0 or (top10["bad_pick_rate_delta"] or 0) < 0
    return_ok = (top10["mean_ret20_delta"] or 0) >= -0.002 and (top10["hit_rate_delta"] or 0) >= -0.01
    top5_not_bad = (top5["mean_ret20_delta"] or 0) >= -0.003 and (top5["hit_rate_delta"] or 0) >= -0.015
    false_removal_ok = e10["good_pick_false_removed_count"] <= max(10, e10["bad_pick_removed_count"] * 4)
    if enough_branching and risk_improved and return_ok and top5_not_bad and false_removal_ok:
        return "keep_for_candidate_pretest_next", "single_axis_demotion_improves_bad_pick_or_severe_loss_without_unacceptable_topk_quality_cost"
    if risk_improved or e10["bad_pick_removed_count"] > 0:
        return "keep_diagnostic_only", "context_risk_axis_removes_some_bad_picks_but_topk_quality_or_branching_is_not_strong_enough"
    return "drop", "single_axis_challenger_does_not_improve_bad_pick_or_topk_quality"


def run(args: argparse.Namespace) -> Path:
    source = _read_json(args.source_decision)
    if source.get("selected_axis_name") != "context_risk_not_clean":
        raise RuntimeError("source selected axis is not context_risk_not_clean")
    out_dir = args.out_root / f"{_now_tag()}-{AXIS_ID.replace('_', '-')}"
    out_dir.mkdir(parents=True, exist_ok=False)
    df = _load_rows(args.replay_rows)
    compare, effect, replacement = _compare(df)
    decision, reason = _decision(compare, effect)
    design = {
        "selected_axis": "context_risk_not_clean",
        "definition": "variant_c_combined_context_risk_clean == false",
        "challenger_form": "demotion",
        "design_reason": "keeps the same replay candidate set and baseline source intact while testing the single deployable bad-pick flag; avoids hard veto overfitting and threshold tuning",
        "ranking_rule": "within each as_of_date, clean rows first, then context_risk_not_clean rows, preserving baseline rank within each group",
        "threshold_tuning": False,
    }
    _write_json(out_dir / "compare.json", {"axis_id": AXIS_ID, "same_condition": True, "challenger_design": design, "compare": compare})
    _write_json(out_dir / "challenger_effect_summary.json", {"axis_id": AXIS_ID, **effect})
    _write_json(out_dir / "replacement_quality_summary.json", {"axis_id": AXIS_ID, **replacement})
    final = {
        "axis_id": AXIS_ID,
        "authoritative_rollup_decision": decision,
        "reason": reason,
        "source_artifact": str(args.source_decision),
        "challenger_design": design,
        "top5_top10_top20_comparison": compare,
        "bad_pick_removal_result": effect["topk"],
        "good_pick_false_removal_result": [
            {"topk": x["topk"], "good_pick_false_removed_count": x["good_pick_false_removed_count"]} for x in effect["topk"]
        ],
        "branching_count": {
            "changed_rank_count": effect["changed_rank_count"],
            "topk": [
                {"topk": x["topk"], "changed_members_count": x["changed_members_count"], "dates_with_member_branching": x["dates_with_member_branching"]}
                for x in effect["topk"]
            ],
        },
        "why_keep_drop_diagnostic": reason,
        "next_required_single_step": "run_candidate_pretest_for_context_risk_not_clean_under_same_condition" if decision == "keep_for_candidate_pretest_next" else "do_not_promote_context_risk_challenger_without_more_evidence",
        "boundary_flags": {
            "tradex_only": True,
            "read_only_challenger_comparison": True,
            "runtime_db_write": False,
            "meemee_reflection": False,
            "ranking_change": False,
            "publish": False,
            "production_candidate_generation_change": False,
            "live_rule_promotion_allowed": False,
            "ma_phase_mixed_in": False,
            "threshold_tuning": False,
            "baseline_rank_source_changed": False,
            "replay_rows_changed": False,
        },
    }
    _write_json(out_dir / "final_research_decision.json", final)
    _write_json(out_dir / "_ARTIFACT_COMPLETE.json", {"axis_id": AXIS_ID, "status": "complete", "generated_at_utc": datetime.now(timezone.utc).isoformat(), "required_files": list(REQUIRED), "required_files_present": all((out_dir / f).exists() for f in REQUIRED if f != "_ARTIFACT_COMPLETE.json")})
    return out_dir


def main() -> None:
    parser = argparse.ArgumentParser(description="Compare a context_risk_not_clean single-axis bad-pick removal challenger.")
    parser.add_argument("--source-decision", type=Path, default=DEFAULT_SOURCE_DECISION)
    parser.add_argument("--replay-rows", type=Path, default=DEFAULT_REPLAY_ROWS)
    parser.add_argument("--out-root", type=Path, default=DEFAULT_OUT_ROOT)
    print(run(parser.parse_args()))


if __name__ == "__main__":
    main()
