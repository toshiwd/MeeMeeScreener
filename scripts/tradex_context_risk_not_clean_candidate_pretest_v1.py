from __future__ import annotations

import argparse
import json
import math
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd


AXIS_ID = "context_risk_not_clean_candidate_pretest_v1"
DEFAULT_SOURCE = Path("G:/Tradex/context_risk_not_clean_bad_pick_challenger_v1/20260604T023543Z-context-risk-not-clean-bad-pick-challenger-v1/final_research_decision.json")
DEFAULT_REPLAY_ROWS = Path("G:/Tradex/current_buyable_historical_operational_replay_v1/20260526T014356Z-current-buyable-historical-operational-replay-v1/historical_operational_replay_rows.csv")
DEFAULT_OUT_ROOT = Path("G:/Tradex/context_risk_not_clean_candidate_pretest_v1")
REQUIRED = (
    "final_research_decision.json",
    "candidate_pretest_compare.json",
    "candidate_pretest_effect_summary.json",
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
    df["period_bucket"] = df["period_bucket"].astype(str)
    df["baseline_rank_source"] = pd.to_numeric(df["fresh_runtime_research_watch_rank"], errors="coerce")
    df["baseline_top_rank"] = df.groupby("as_of_date")["baseline_rank_source"].rank(method="first")
    df["ret20"] = pd.to_numeric(df["ret20"], errors="coerce")
    df["hit_flag"] = df["ret20"] > 0
    df["bad_pick_flag"] = df["ret20"] <= -0.05
    df["severe_loss_flag"] = df["ret20"] <= -0.10
    df["context_risk_not_clean"] = ~df["variant_c_combined_context_risk_clean"].fillna(False).astype(bool)
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


def _compare(df: pd.DataFrame) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any], list[dict[str, Any]]]:
    metrics, deltas, effect_rows, repl_rows = [], [], [], []
    for topk in TOPKS:
        b = df[df["baseline_top_rank"] <= topk]
        c = df[df["challenger_rank"] <= topk]
        bm, cm = _metrics(b, "baseline", topk), _metrics(c, "challenger", topk)
        metrics.extend([bm, cm])
        delta = {"topk": topk}
        for key in ["mean_ret20", "median_ret20", "hit_rate", "bad_pick_rate", "severe_loss_rate", "context_risk_not_clean_rate"]:
            delta[f"{key}_delta"] = cm[key] - bm[key] if cm[key] is not None and bm[key] is not None else None
        deltas.append(delta)
    for date, g in df.groupby("as_of_date", sort=True):
        for topk in TOPKS:
            bset = set(g.loc[g["baseline_top_rank"] <= topk, "code"])
            cset = set(g.loc[g["challenger_rank"] <= topk, "code"])
            removed = g[g["code"].isin(bset - cset)]
            added = g[g["code"].isin(cset - bset)]
            common = g[g["code"].isin(bset & cset)]
            row = {
                "as_of_date": date,
                "topk": topk,
                "changed_members_count": int(len(bset ^ cset)),
                "within_topk_reordered_count": int((common["baseline_top_rank"] != common["challenger_rank"]).sum()),
                "bad_pick_removed_count": int(removed["bad_pick_flag"].sum()),
                "severe_loss_removed_count": int(removed["severe_loss_flag"].sum()),
                "good_pick_false_removed_count": int((removed["ret20"] > 0).sum()),
                "bad_pick_added_count": int(added["bad_pick_flag"].sum()),
                "removed_mean_ret20": _mean(removed["ret20"]),
                "added_mean_ret20": _mean(added["ret20"]),
                "replacement_ret20_advantage": (_mean(added["ret20"]) - _mean(removed["ret20"])) if _mean(added["ret20"]) is not None and _mean(removed["ret20"]) is not None else None,
            }
            effect_rows.append(row)
            if len(removed) or len(added):
                repl_rows.append(row)
    effect = pd.DataFrame(effect_rows)
    repl = pd.DataFrame(repl_rows)
    effect_summary = {"changed_rank_count": int((df["baseline_top_rank"] != df["challenger_rank"]).sum()), "flagged_rows_total": int(df["context_risk_not_clean"].sum()), "topk": []}
    replacement_summary = {"topk": []}
    for topk in TOPKS:
        ep = effect[effect["topk"] == topk]
        rp = repl[repl["topk"] == topk] if not repl.empty else pd.DataFrame()
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
    stability = []
    for period, part in df.groupby("period_bucket"):
        for topk in (5, 10):
            b = part[part["baseline_top_rank"] <= topk]
            c = part[part["challenger_rank"] <= topk]
            stability.append({
                "period_bucket": period,
                "topk": topk,
                "baseline_mean_ret20": _mean(b["ret20"]),
                "challenger_mean_ret20": _mean(c["ret20"]),
                "mean_ret20_delta": (_mean(c["ret20"]) - _mean(b["ret20"])) if _mean(c["ret20"]) is not None and _mean(b["ret20"]) is not None else None,
                "baseline_bad_pick_rate": _rate(b["bad_pick_flag"]),
                "challenger_bad_pick_rate": _rate(c["bad_pick_flag"]),
                "bad_pick_rate_delta": (_rate(c["bad_pick_flag"]) - _rate(b["bad_pick_flag"])) if _rate(c["bad_pick_flag"]) is not None and _rate(b["bad_pick_flag"]) is not None else None,
                "row_count": int(len(c)),
            })
    return {"metrics": metrics, "deltas": deltas, "period_stability": stability}, effect_summary, replacement_summary, repl_rows


def _decision(compare: dict[str, Any], effect: dict[str, Any]) -> tuple[str, str, bool]:
    top5 = next(d for d in compare["deltas"] if d["topk"] == 5)
    top10 = next(d for d in compare["deltas"] if d["topk"] == 10)
    e5 = next(x for x in effect["topk"] if x["topk"] == 5)
    e10 = next(x for x in effect["topk"] if x["topk"] == 10)
    positive = (top5["bad_pick_rate_delta"] or 0) < 0 and (top10["severe_loss_rate_delta"] or 0) < 0
    no_cost = (top5["mean_ret20_delta"] or 0) >= 0 and (top10["mean_ret20_delta"] or 0) >= 0 and (top5["hit_rate_delta"] or 0) >= 0 and (top10["hit_rate_delta"] or 0) >= -0.005
    branching_small = e10["dates_with_member_branching"] < 5 or e10["changed_members_count"] < 10
    if positive and no_cost and not branching_small:
        return "keep", "improvement_reproduced_with_sufficient_branching", True
    if positive and no_cost:
        return "hold", "direction_reproduced_but_branching_or_sample_stability_is_insufficient", True
    if (top5["mean_ret20_delta"] or 0) < 0 or (top10["mean_ret20_delta"] or 0) < 0:
        return "drop", "topk_return_quality_worsened", False
    return "hold", "mixed_or_underpowered_candidate_pretest_result", False


def run(args: argparse.Namespace) -> Path:
    source = _read_json(args.source_artifact)
    if source.get("authoritative_rollup_decision") != "keep_for_candidate_pretest_next":
        raise RuntimeError("source challenger is not keep_for_candidate_pretest_next")
    out_dir = args.out_root / f"{_now_tag()}-{AXIS_ID.replace('_', '-')}"
    out_dir.mkdir(parents=True, exist_ok=False)
    df = _load_rows(args.replay_rows)
    compare, effect, replacement, _ = _compare(df)
    decision, reason, reproduced = _decision(compare, effect)
    tested = {
        "axis": "context_risk_not_clean",
        "definition": "variant_c_combined_context_risk_clean == false",
        "form": "demotion",
        "rule": "within each as_of_date, clean rows first, then context_risk_not_clean rows, preserving baseline rank within each group",
        "threshold_tuning": False,
    }
    _write_json(out_dir / "candidate_pretest_compare.json", {"axis_id": AXIS_ID, "tested_challenger": tested, "compare": compare})
    _write_json(out_dir / "candidate_pretest_effect_summary.json", {"axis_id": AXIS_ID, **effect})
    _write_json(out_dir / "replacement_quality_summary.json", {"axis_id": AXIS_ID, **replacement})
    final = {
        "axis_id": AXIS_ID,
        "authoritative_rollup_decision": decision,
        "tested_challenger": tested,
        "source_artifact": str(args.source_artifact),
        "prior_improvement_reproduced": reproduced,
        "top5_top10_top20_result": compare,
        "branching_result": effect,
        "false_removal_replacement_quality": replacement,
        "keep_drop_hold_reason": reason,
        "next_required_single_step": "stability_expand_or_out_of_sample_replay_before_any_promotion" if decision == "hold" else ("prepare_research_candidate_for_next_same_condition_comparison" if decision == "keep" else "drop_context_risk_not_clean_candidate_line"),
        "boundary_flags": {
            "tradex_only": True,
            "candidate_pretest_only": True,
            "runtime_db_write": False,
            "meemee_reflection": False,
            "ranking_change": False,
            "publish": False,
            "production_candidate_generation_change": False,
            "live_rule_promotion_allowed": False,
            "frozen_exit_champion_changed": False,
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
    parser = argparse.ArgumentParser(description="Candidate pretest for fixed context_risk_not_clean demotion challenger.")
    parser.add_argument("--source-artifact", type=Path, default=DEFAULT_SOURCE)
    parser.add_argument("--replay-rows", type=Path, default=DEFAULT_REPLAY_ROWS)
    parser.add_argument("--out-root", type=Path, default=DEFAULT_OUT_ROOT)
    print(run(parser.parse_args()))


if __name__ == "__main__":
    main()
