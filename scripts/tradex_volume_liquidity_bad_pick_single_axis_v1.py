from __future__ import annotations

import argparse
import json
import math
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from scripts import tradex_gap_down_q25_bad_pick_challenger_v1 as gap_challenger


AXIS_ID = "volume_liquidity_bad_pick_single_axis_v1"
DEFAULT_GAP_HOLD = Path("G:/Tradex/gap_down_q25_candidate_pretest_v1/20260604T032713Z-gap-down-q25-candidate-pretest-v1/final_research_decision.json")
DEFAULT_CONTRACT = Path("G:/Tradex/pre_entry_bad_pick_feature_contract_expansion_v1/20260604T025739Z-pre-entry-bad-pick-feature-contract-expansion-v1/final_research_decision.json")
DEFAULT_REPLAY_ROWS = gap_challenger.DEFAULT_REPLAY_ROWS
DEFAULT_SOURCE_DB = gap_challenger.DEFAULT_SOURCE_DB
DEFAULT_OUT_ROOT = Path("G:/Tradex/volume_liquidity_bad_pick_single_axis_v1")
TOPKS = (5, 10, 20)
REQUIRED = (
    "final_research_decision.json",
    "selected_axis_candidate_card.json",
    "effect_summary.json",
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
    return value


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(_json_ready(payload), ensure_ascii=False, sort_keys=True, indent=2) + "\n", encoding="utf-8")


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _mean(series):
    valid = series.dropna()
    return None if valid.empty else float(valid.mean())


def _rate(series):
    valid = series.dropna()
    return None if valid.empty else float(valid.astype(bool).mean())


def _prepare_rows(replay_rows: Path, source_db: Path):
    replay = gap_challenger._load_replay(replay_rows)
    rows, _ = gap_challenger._join_gap(replay, source_db)
    # _join_gap already joins bars only with gap; reload richer feature contract via expansion helper.
    from scripts import tradex_pre_entry_bad_pick_feature_contract_expansion_v1 as contract
    features = contract._load_bar_features(source_db, replay)
    rows = replay.merge(features, on=["code", "as_of_date"], how="left", validate="one_to_one")
    rows["baseline_top_rank"] = rows.groupby("as_of_date")["baseline_rank_source"].rank(method="first")
    return rows


def _candidate_masks(rows):
    q = lambda c, p: float(rows[c].quantile(p))
    return [
        {
            "axis_id": "volume_spike_q75",
            "axis_family": "volume abnormality",
            "definition": "volume_vs_20d_avg >= source q75",
            "mask": rows["volume_vs_20d_avg"] >= q("volume_vs_20d_avg", 0.75),
        },
        {
            "axis_id": "volume_dry_q25",
            "axis_family": "volume abnormality",
            "definition": "volume_vs_20d_avg <= source q25",
            "mask": rows["volume_vs_20d_avg"] <= q("volume_vs_20d_avg", 0.25),
        },
        {
            "axis_id": "volume_without_followthrough_q25",
            "axis_family": "volume without follow-through",
            "definition": "volume_followthrough_proxy <= source q25",
            "mask": rows["volume_followthrough_proxy"] <= q("volume_followthrough_proxy", 0.25),
        },
        {
            "axis_id": "thin_turnover_q25",
            "axis_family": "liquidity/thin-sample",
            "definition": "turnover20_value <= source q25",
            "mask": rows["turnover20_value"] <= q("turnover20_value", 0.25),
        },
        {
            "axis_id": "thin_volume_q25",
            "axis_family": "liquidity/thin-sample",
            "definition": "volume20_avg <= source q25",
            "mask": rows["volume20_avg"] <= q("volume20_avg", 0.25),
        },
    ]


def _quality(rows, mask):
    inside = rows[mask.fillna(False)]
    outside = rows[~mask.fillna(False)]
    return {
        "inside_count": int(len(inside)),
        "coverage": float(mask.fillna(False).mean()) if len(rows) else None,
        "inside_bad_pick_rate": _rate(inside["bad_pick_flag"]),
        "outside_bad_pick_rate": _rate(outside["bad_pick_flag"]),
        "inside_severe_loss_rate": _rate(inside["severe_loss_flag"]),
        "outside_severe_loss_rate": _rate(outside["severe_loss_flag"]),
        "inside_mean_ret20": _mean(inside["ret20"]),
        "outside_mean_ret20": _mean(outside["ret20"]),
        "inside_hit_rate": _rate(inside["hit_flag"]),
        "outside_hit_rate": _rate(outside["hit_flag"]),
    }


def _select_axis(rows):
    candidates = []
    for c in _candidate_masks(rows):
        q = _quality(rows, c["mask"])
        bad_lift = (q["inside_bad_pick_rate"] or 0) - (q["outside_bad_pick_rate"] or 0)
        severe_lift = (q["inside_severe_loss_rate"] or 0) - (q["outside_severe_loss_rate"] or 0)
        ret_penalty = (q["outside_mean_ret20"] or 0) - (q["inside_mean_ret20"] or 0)
        score = bad_lift * 3 + severe_lift * 2 + ret_penalty
        row = {k: v for k, v in c.items() if k != "mask"}
        row.update(q)
        row["axis_selection_score"] = score
        row["_mask"] = c["mask"]
        candidates.append(row)
    candidates.sort(key=lambda x: x["axis_selection_score"], reverse=True)
    best = candidates[0] if candidates else None
    selected = None
    if best and best["inside_count"] >= 30 and best["axis_selection_score"] > 0.08 and (best["inside_bad_pick_rate"] or 0) > (best["outside_bad_pick_rate"] or 0):
        selected = best
    return selected, candidates


def _metrics(part, label, topk, flag_col):
    return {
        "label": label,
        "topk": topk,
        "row_count": int(len(part)),
        "mean_ret20": _mean(part["ret20"]),
        "hit_rate": _rate(part["hit_flag"]),
        "bad_pick_rate": _rate(part["bad_pick_flag"]),
        "severe_loss_rate": _rate(part["severe_loss_flag"]),
        "axis_flag_rate": _rate(part[flag_col]),
    }


def _run_challenger(rows, selected):
    flag_col = selected["axis_id"]
    rows = rows.copy()
    rows[flag_col] = selected["_mask"].fillna(False).astype(bool)
    rows = rows.sort_values(["as_of_date", flag_col, "baseline_rank_source", "code"], ascending=[True, True, True, True], kind="stable").copy()
    rows["challenger_rank"] = rows.groupby("as_of_date").cumcount() + 1
    metrics, deltas, effects, repls = [], [], [], []
    for topk in TOPKS:
        b = rows[rows["baseline_top_rank"] <= topk]
        ch = rows[rows["challenger_rank"] <= topk]
        bm, cm = _metrics(b, "baseline", topk, flag_col), _metrics(ch, "challenger", topk, flag_col)
        metrics += [bm, cm]
        delta = {"topk": topk}
        for key in ["mean_ret20", "hit_rate", "bad_pick_rate", "severe_loss_rate", "axis_flag_rate"]:
            delta[f"{key}_delta"] = cm[key] - bm[key] if cm[key] is not None and bm[key] is not None else None
        deltas.append(delta)
    for date, g in rows.groupby("as_of_date", sort=True):
        for topk in TOPKS:
            bset = set(g.loc[g["baseline_top_rank"] <= topk, "code"])
            cset = set(g.loc[g["challenger_rank"] <= topk, "code"])
            removed = g[g["code"].isin(bset - cset)]
            added = g[g["code"].isin(cset - bset)]
            common = g[g["code"].isin(bset & cset)]
            row = {
                "topk": topk,
                "changed_members_count": int(len(bset ^ cset)),
                "within_topk_reordered_count": int((common["baseline_top_rank"] != common["challenger_rank"]).sum()),
                "bad_pick_removed_count": int(removed["bad_pick_flag"].sum()),
                "severe_loss_removed_count": int(removed["severe_loss_flag"].sum()),
                "good_pick_false_removed_count": int((removed["ret20"] > 0).sum()),
                "bad_pick_added_count": int(added["bad_pick_flag"].sum()),
                "replacement_ret20_advantage": (_mean(added["ret20"]) - _mean(removed["ret20"])) if _mean(added["ret20"]) is not None and _mean(removed["ret20"]) is not None else None,
            }
            effects.append(row)
            if len(removed) or len(added):
                repls.append(row)
    effect_summary = {"changed_rank_count": int((rows["baseline_top_rank"] != rows["challenger_rank"]).sum()), "topk": []}
    repl_summary = {"topk": []}
    import pandas as pd
    ef = pd.DataFrame(effects)
    rf = pd.DataFrame(repls)
    for topk in TOPKS:
        ep = ef[ef["topk"] == topk]
        rp = rf[rf["topk"] == topk] if not rf.empty else pd.DataFrame()
        effect_summary["topk"].append({
            "topk": topk,
            "changed_members_count": int(ep["changed_members_count"].sum()),
            "dates_with_member_branching": int((ep["changed_members_count"] > 0).sum()),
            "bad_pick_removed_count": int(ep["bad_pick_removed_count"].sum()),
            "severe_loss_removed_count": int(ep["severe_loss_removed_count"].sum()),
            "good_pick_false_removed_count": int(ep["good_pick_false_removed_count"].sum()),
            "bad_pick_added_count": int(ep["bad_pick_added_count"].sum()),
            "within_topk_reordered_count": int(ep["within_topk_reordered_count"].sum()),
        })
        repl_summary["topk"].append({
            "topk": topk,
            "replacement_event_count": int(len(rp)),
            "mean_replacement_ret20_advantage": _mean(rp["replacement_ret20_advantage"]) if not rp.empty else None,
            "removed_bad_pick_count": int(rp["bad_pick_removed_count"].sum()) if not rp.empty else 0,
            "removed_severe_loss_count": int(rp["severe_loss_removed_count"].sum()) if not rp.empty else 0,
            "false_removed_good_pick_count": int(rp["good_pick_false_removed_count"].sum()) if not rp.empty else 0,
            "added_bad_pick_count": int(rp["bad_pick_added_count"].sum()) if not rp.empty else 0,
        })
    return {"metrics": metrics, "deltas": deltas}, effect_summary, repl_summary


def _decision(compare, effect, repl, ran):
    if not ran:
        return "keep_diagnostic_only", "no_volume_liquidity_axis_strong_enough_for_challenger", False
    t5 = next(d for d in compare["deltas"] if d["topk"] == 5)
    t10 = next(d for d in compare["deltas"] if d["topk"] == 10)
    e5 = next(x for x in effect["topk"] if x["topk"] == 5)
    e10 = next(x for x in effect["topk"] if x["topk"] == 10)
    r5 = next(x for x in repl["topk"] if x["topk"] == 5)
    direction = ((t5["bad_pick_rate_delta"] or 0) < 0 or (t10["bad_pick_rate_delta"] or 0) < 0 or (t10["severe_loss_rate_delta"] or 0) < 0) and (t5["mean_ret20_delta"] or 0) >= -0.002 and (t10["mean_ret20_delta"] or 0) >= -0.002 and (t5["hit_rate_delta"] or 0) >= -0.01
    branching = e5["changed_members_count"] >= 20 or e10["changed_members_count"] >= 10
    false_heavy = r5["false_removed_good_pick_count"] > max(10, r5["removed_bad_pick_count"] * 4)
    if direction and branching and not false_heavy:
        return "keep", "volume_liquidity_axis_improved_topk_quality_with_acceptable_false_removal_and_branching", True
    if direction:
        return "hold", "direction_positive_but_sample_branching_or_false_removal_cost_insufficient", True
    return "drop", "volume_liquidity_axis_failed_topk_quality_or_bad_pick_reduction", False


def run(args):
    gap_hold = _read_json(args.gap_hold)
    contract = _read_json(args.contract)
    if gap_hold.get("authoritative_rollup_decision") != "hold":
        raise RuntimeError("gap_down_q25 is not frozen hold")
    if contract.get("authoritative_rollup_decision") != "keep_for_next_axis_selection":
        raise RuntimeError("feature contract is not ready")
    out = args.out_root / f"{_now_tag()}-{AXIS_ID.replace('_','-')}"
    out.mkdir(parents=True, exist_ok=False)
    rows = _prepare_rows(args.replay_rows, args.source_db)
    selected, candidates = _select_axis(rows)
    challenger_run = selected is not None
    compare = effect = repl = None
    pretest_run = False
    if challenger_run:
        compare, effect, repl = _run_challenger(rows, selected)
        pretest_run = True
        _write_json(out / "challenger_compare.json", {"axis_id": AXIS_ID, "selected_axis": {k: v for k, v in selected.items() if k != "_mask"}, "compare": compare})
        _write_json(out / "candidate_pretest_compare.json", {"axis_id": AXIS_ID, "selected_axis": {k: v for k, v in selected.items() if k != "_mask"}, "compare": compare})
    decision, reason, reproduced = _decision(compare, effect, repl, challenger_run)
    card = {"selected_axis_name": selected["axis_id"] if selected else None, "challenger_was_run": challenger_run, "candidate_pretest_was_run": pretest_run, "ranked_volume_liquidity_axes": [{k: v for k, v in c.items() if k != "_mask"} for c in candidates]}
    effect_payload = {"axis_id": AXIS_ID, "challenger_was_run": challenger_run, "effect_summary": effect, "replacement_quality": repl}
    _write_json(out / "selected_axis_candidate_card.json", card)
    _write_json(out / "effect_summary.json", effect_payload)
    final = {
        "axis_id": AXIS_ID,
        "authoritative_rollup_decision": decision,
        "selected_axis_name": selected["axis_id"] if selected else None,
        "challenger_was_run": challenger_run,
        "candidate_pretest_was_run": pretest_run,
        "top5_top10_top20_result": compare,
        "false_removal_replacement_quality": repl,
        "keep_hold_drop_reason": reason,
        "prior_direction_reproduced": reproduced,
        "next_required_single_step": "run_stability_replay_for_selected_volume_liquidity_axis" if decision == "keep" else ("freeze_volume_liquidity_axis_as_hold_or_diagnostic" if decision == "hold" else "drop_volume_liquidity_family_or_try_new_independent_family"),
        "boundary_flags": {
            "tradex_only": True,
            "runtime_db_write": False,
            "meemee_reflection": False,
            "ranking_change": False,
            "publish": False,
            "production_candidate_generation_change": False,
            "live_rule_promotion_allowed": False,
            "ma_phase_mixed_in": False,
            "context_risk_not_clean_mixed_in": False,
            "gap_down_q25_mixed_in": False,
            "threshold_tuning": False,
            "multiple_axes_combined": False,
        },
    }
    _write_json(out / "final_research_decision.json", final)
    required = list(REQUIRED)
    if challenger_run:
        required += ["challenger_compare.json", "candidate_pretest_compare.json"]
    _write_json(out / "_ARTIFACT_COMPLETE.json", {"axis_id": AXIS_ID, "status": "complete", "generated_at_utc": datetime.now(timezone.utc).isoformat(), "required_files": required, "required_files_present": all((out / f).exists() for f in required if f != "_ARTIFACT_COMPLETE.json")})
    return out


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--gap-hold", type=Path, default=DEFAULT_GAP_HOLD)
    parser.add_argument("--contract", type=Path, default=DEFAULT_CONTRACT)
    parser.add_argument("--replay-rows", type=Path, default=DEFAULT_REPLAY_ROWS)
    parser.add_argument("--source-db", type=Path, default=DEFAULT_SOURCE_DB)
    parser.add_argument("--out-root", type=Path, default=DEFAULT_OUT_ROOT)
    print(run(parser.parse_args()))


if __name__ == "__main__":
    main()
