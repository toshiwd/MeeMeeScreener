from __future__ import annotations

import argparse
import json
import math
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd


AXIS_ID = "gap_down_q25_bad_pick_challenger_v1"
DEFAULT_SOURCE = Path("G:/Tradex/pre_entry_bad_pick_feature_contract_expansion_v1/20260604T025739Z-pre-entry-bad-pick-feature-contract-expansion-v1/final_research_decision.json")
DEFAULT_REPLAY_ROWS = Path("G:/Tradex/intersection_family_current_period_risk_containment_v1/20260526T010028Z-intersection-family-current-period-risk-containment-v1/risk_containment_rows.csv")
DEFAULT_SOURCE_DB = Path("C:/Users/enish/AppData/Local/MeeMeeScreener/data/stocks.duckdb")
DEFAULT_OUT_ROOT = Path("G:/Tradex/gap_down_q25_bad_pick_challenger_v1")
TOPKS = (5, 10, 20)
REQUIRED = ("final_research_decision.json", "compare.json", "challenger_effect_summary.json", "replacement_quality_summary.json", "_ARTIFACT_COMPLETE.json")


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


def _load_replay(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path)
    df["code"] = df["code"].astype(str)
    df["as_of_date"] = pd.to_numeric(df["as_of_date"], errors="coerce").astype("Int64")
    df["baseline_rank_source"] = pd.to_numeric(df["fresh_runtime_research_watch_rank"], errors="coerce")
    df["baseline_top_rank"] = df.groupby("as_of_date")["baseline_rank_source"].rank(method="first")
    df["ret20"] = pd.to_numeric(df["ret20"], errors="coerce")
    df["hit_flag"] = df["ret20"] > 0
    df["bad_pick_flag"] = df["ret20"] <= -0.05
    df["severe_loss_flag"] = df["ret20"] <= -0.10
    return df


def _join_gap(df: pd.DataFrame, source_db: Path) -> tuple[pd.DataFrame, float]:
    codes = sorted(df["code"].astype(str).unique().tolist())
    min_date, max_date = int(df["as_of_date"].min()) - 10000, int(df["as_of_date"].max())
    con = duckdb.connect(str(source_db), read_only=True)
    try:
        expr = "CAST(strftime(to_timestamp(date), '%Y%m%d') AS INTEGER)"
        bars = con.execute(
            f"""
            SELECT CAST(code AS VARCHAR) AS code, {expr} AS as_of_date, o, c
            FROM daily_bars
            WHERE CAST(code AS VARCHAR) IN (SELECT * FROM UNNEST(?))
              AND {expr} BETWEEN ? AND ?
            ORDER BY code, as_of_date
            """,
            [codes, min_date, max_date],
        ).fetchdf()
    finally:
        con.close()
    bars["code"] = bars["code"].astype(str)
    bars = bars.sort_values(["code", "as_of_date"], kind="stable").copy()
    bars["prev_close"] = bars.groupby("code", sort=False)["c"].shift(1)
    bars["gap_pct"] = bars["o"] / bars["prev_close"] - 1.0
    joined = df.merge(bars[["code", "as_of_date", "gap_pct"]], on=["code", "as_of_date"], how="left", validate="one_to_one")
    q25 = float(joined["gap_pct"].quantile(0.25))
    joined["gap_down_q25"] = joined["gap_pct"] <= q25
    joined = joined.sort_values(["as_of_date", "gap_down_q25", "baseline_rank_source", "code"], ascending=[True, True, True, True], kind="stable").copy()
    joined["challenger_rank"] = joined.groupby("as_of_date").cumcount() + 1
    return joined, q25


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
        "gap_down_q25_rate": _rate(part["gap_down_q25"]),
    }


def _compare(df: pd.DataFrame) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    metrics, deltas, effect_rows, repl_rows = [], [], [], []
    for topk in TOPKS:
        b, c = df[df["baseline_top_rank"] <= topk], df[df["challenger_rank"] <= topk]
        bm, cm = _metrics(b, "baseline", topk), _metrics(c, "challenger", topk)
        metrics.extend([bm, cm])
        delta = {"topk": topk}
        for key in ["mean_ret20", "median_ret20", "hit_rate", "bad_pick_rate", "severe_loss_rate", "gap_down_q25_rate"]:
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
    effect_summary = {"changed_rank_count": int((df["baseline_top_rank"] != df["challenger_rank"]).sum()), "flagged_rows_total": int(df["gap_down_q25"].sum()), "topk": []}
    repl_summary = {"topk": []}
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
        repl_summary["topk"].append({
            "topk": topk,
            "replacement_event_count": int(len(rp)),
            "mean_replacement_ret20_advantage": _mean(rp["replacement_ret20_advantage"]) if not rp.empty else None,
            "median_replacement_ret20_advantage": _median(rp["replacement_ret20_advantage"]) if not rp.empty else None,
            "removed_bad_pick_count": int(rp["bad_pick_removed_count"].sum()) if not rp.empty else 0,
            "removed_severe_loss_count": int(rp["severe_loss_removed_count"].sum()) if not rp.empty else 0,
            "false_removed_good_pick_count": int(rp["good_pick_false_removed_count"].sum()) if not rp.empty else 0,
            "added_bad_pick_count": int(rp["bad_pick_added_count"].sum()) if not rp.empty else 0,
        })
    return {"metrics": metrics, "deltas": deltas}, effect_summary, repl_summary


def _decision(compare: dict[str, Any], effect: dict[str, Any]) -> tuple[str, str]:
    t5 = next(d for d in compare["deltas"] if d["topk"] == 5)
    t10 = next(d for d in compare["deltas"] if d["topk"] == 10)
    e10 = next(x for x in effect["topk"] if x["topk"] == 10)
    e5 = next(x for x in effect["topk"] if x["topk"] == 5)
    improves = (t5["bad_pick_rate_delta"] or 0) < 0 or (t10["bad_pick_rate_delta"] or 0) < 0 or (t10["severe_loss_rate_delta"] or 0) < 0
    cost_ok = (t5["mean_ret20_delta"] or 0) >= -0.002 and (t10["mean_ret20_delta"] or 0) >= -0.002 and (t5["hit_rate_delta"] or 0) >= -0.01 and (t10["hit_rate_delta"] or 0) >= -0.01
    meaningful = e10["changed_members_count"] >= 10 or e5["changed_members_count"] >= 20
    if improves and cost_ok and meaningful:
        return "keep_for_candidate_pretest_next", "gap_down_q25_demotion_improves_bad_pick_or_severe_loss_with_acceptable_topk_cost_and_branching"
    if improves:
        return "keep_diagnostic_only", "gap_down_q25_has_bad_pick_signal_but_topk_improvement_or_branching_is_too_weak"
    return "drop", "gap_down_q25_demotion_does_not_improve_topk_quality"


def run(args: argparse.Namespace) -> Path:
    source = _read_json(args.source_artifact)
    selected = source.get("next_axis_candidate_card", {}).get("selected_next_axis", {})
    if selected.get("axis_id") != "gap_down_q25":
        raise RuntimeError("source selected next axis is not gap_down_q25")
    out_dir = args.out_root / f"{_now_tag()}-{AXIS_ID.replace('_', '-')}"
    out_dir.mkdir(parents=True, exist_ok=False)
    replay = _load_replay(args.replay_rows)
    df, q25 = _join_gap(replay, args.source_db)
    compare, effect, repl = _compare(df)
    decision, reason = _decision(compare, effect)
    design = {
        "axis": "gap_down_q25",
        "family": "gap",
        "definition": "gap_pct <= source q25",
        "q25_value": q25,
        "form": "demotion",
        "rule": "within each as_of_date, non-gap_down_q25 rows first, then gap_down_q25 rows, preserving baseline rank inside each group",
        "threshold_tuning": False,
    }
    _write_json(out_dir / "compare.json", {"axis_id": AXIS_ID, "same_condition": True, "challenger_design": design, "compare": compare})
    _write_json(out_dir / "challenger_effect_summary.json", {"axis_id": AXIS_ID, **effect})
    _write_json(out_dir / "replacement_quality_summary.json", {"axis_id": AXIS_ID, **repl})
    final = {
        "axis_id": AXIS_ID,
        "authoritative_rollup_decision": decision,
        "reason": reason,
        "source_artifact": str(args.source_artifact),
        "challenger_design": design,
        "top5_top10_top20_comparison": compare,
        "branching_result": effect,
        "false_removal_replacement_quality": repl,
        "next_required_single_step": "run_candidate_pretest_for_gap_down_q25" if decision == "keep_for_candidate_pretest_next" else ("park_gap_down_q25_as_diagnostic_context" if decision == "keep_diagnostic_only" else "drop_gap_down_q25_axis"),
        "boundary_flags": {
            "tradex_only": True,
            "read_only_challenger_comparison": True,
            "runtime_db_write": False,
            "meemee_reflection": False,
            "ranking_change": False,
            "publish": False,
            "production_candidate_generation_change": False,
            "live_rule_promotion_allowed": False,
            "frozen_exit_champion_changed": False,
            "ma_phase_mixed_in": False,
            "context_risk_not_clean_mixed_in": False,
            "threshold_tuning": False,
            "baseline_rank_source_changed": False,
            "replay_rows_changed": False,
        },
    }
    _write_json(out_dir / "final_research_decision.json", final)
    _write_json(out_dir / "_ARTIFACT_COMPLETE.json", {"axis_id": AXIS_ID, "status": "complete", "generated_at_utc": datetime.now(timezone.utc).isoformat(), "required_files": list(REQUIRED), "required_files_present": all((out_dir / f).exists() for f in REQUIRED if f != "_ARTIFACT_COMPLETE.json")})
    return out_dir


def main() -> None:
    parser = argparse.ArgumentParser(description="Single-axis gap_down_q25 bad-pick challenger comparison.")
    parser.add_argument("--source-artifact", type=Path, default=DEFAULT_SOURCE)
    parser.add_argument("--replay-rows", type=Path, default=DEFAULT_REPLAY_ROWS)
    parser.add_argument("--source-db", type=Path, default=DEFAULT_SOURCE_DB)
    parser.add_argument("--out-root", type=Path, default=DEFAULT_OUT_ROOT)
    print(run(parser.parse_args()))


if __name__ == "__main__":
    main()
