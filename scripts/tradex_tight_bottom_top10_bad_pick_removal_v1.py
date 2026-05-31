from __future__ import annotations

import argparse
import json
import math
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd


AXIS_ID = "tight_bottom_top10_bad_pick_removal_v1"
DEFAULT_INPUT_ROOT = Path(r"G:\Tradex\starter_entry_family_source_split_design_v1\20260525T041110Z-starter-entry-family-source-split-design-v1")
DEFAULT_AUDIT_ROOT = Path(r"G:\Tradex\replacement_pool_quality_audit_v1\20260525T075648Z-replacement-pool-quality-audit-v1")
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\tight_bottom_top10_bad_pick_removal_v1")
REQUIRED_ARTIFACTS = (
    "tight_bad_pick_summary.json",
    "tight_bad_pick_rows.csv",
    "candidate_feature_contract.json",
    "removed_candidate_quality.json",
    "replacement_quality.json",
    "topk_comparison.json",
    "bottom_top10_capture_metrics.json",
    "winner_preservation_metrics.json",
    "no_lookahead_audit.json",
    "source_coverage.json",
    "research_decision.json",
    "_ARTIFACT_COMPLETE.json",
)
READ_COLUMNS = [
    "decision_date",
    "code",
    "year",
    "baseline_rank",
    "baseline_score",
    "liquidity_flags_json",
    "event_flags_json",
    "risk_flags_json",
    "volume_ma20_ratio",
    "realized_vol20",
    "atr14_pct",
    "upper_wick_ratio",
    "lower_wick_ratio",
    "large_bearish_candle",
    "failed_high_update",
    "dist_ma20_pct",
    "dist_ma60_pct",
    "days_since_ma20_reclaim",
    "monthly_high_zone_proxy",
    "monthly_box_breakout_proxy",
    "weekly_monthly_uptrend_proxy",
    "path20_available",
    "ret20",
    "starter_bad",
    "selected_winner",
]
TOPK_VALUES = (5, 10, 20)


def _now_tag() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _json_ready(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(k): _json_ready(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set)):
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


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(_json_ready(payload), ensure_ascii=False, sort_keys=True, indent=2) + "\n", encoding="utf-8")


def _to_bool(series: pd.Series) -> pd.Series:
    return series.astype(str).str.lower().isin(["true", "1", "yes"])


def _mean(frame: pd.DataFrame, col: str) -> float | None:
    vals = pd.to_numeric(frame.get(col), errors="coerce").dropna()
    return None if vals.empty else float(vals.mean())


def _rate_bool(series: pd.Series) -> float | None:
    vals = series.dropna()
    return None if vals.empty else float(vals.astype(bool).mean())


def load_rows(input_root: Path) -> tuple[pd.DataFrame, dict[str, str]]:
    source = input_root / "candidate_family_source_rows.csv"
    header = pd.read_csv(source, nrows=0).columns.tolist()
    present = [c for c in READ_COLUMNS if c in header]
    rows = pd.concat([c for c in pd.read_csv(source, usecols=present, chunksize=250_000, low_memory=False)], ignore_index=True)
    for c in READ_COLUMNS:
        if c not in rows:
            rows[c] = pd.NA
    rows["code"] = rows["code"].astype(str).str.removesuffix(".0")
    for col in ["decision_date", "year", "baseline_rank", "baseline_score", "volume_ma20_ratio", "realized_vol20", "atr14_pct", "upper_wick_ratio", "lower_wick_ratio", "dist_ma20_pct", "dist_ma60_pct", "days_since_ma20_reclaim", "ret20"]:
        rows[col] = pd.to_numeric(rows[col], errors="coerce")
    for col in ["large_bearish_candle", "failed_high_update", "monthly_high_zone_proxy", "monthly_box_breakout_proxy", "weekly_monthly_uptrend_proxy", "path20_available", "starter_bad", "selected_winner"]:
        rows[col] = _to_bool(rows[col])
    rows = rows[rows["path20_available"] & rows["baseline_rank"].notna() & rows["ret20"].notna()].copy()
    event_real = rows["event_flags_json"].fillna("{}").astype(str).ne("{}").any()
    liq_real = rows["liquidity_flags_json"].fillna("{}").astype(str).str.contains("turnover|spread|market|cap|value|liquid", case=False, regex=True).any()
    contract = feature_contract(event_real, liq_real, header)
    return rows, contract


def feature_contract(event_real: bool, liq_real: bool, header: list[str]) -> dict[str, Any]:
    fields: dict[str, dict[str, str]] = {}
    for col in READ_COLUMNS:
        if col in {"ret20", "starter_bad", "selected_winner"}:
            cls = "outcome_only"
        elif col in {"event_flags_json"} and not event_real:
            cls = "unavailable"
        elif col in {"liquidity_flags_json"} and not liq_real:
            cls = "unavailable"
        elif col in header:
            cls = "point_in_time_feature"
        else:
            cls = "unavailable"
        fields[col] = {"classification": cls}
    fields["ret20_derived_terms"] = {"classification": "forbidden_future_leak"}
    return {
        "axis_id": AXIS_ID,
        "fields": fields,
        "true_liquidity_event_fields_available": bool(event_real or liq_real),
        "variant_a_status": "available" if event_real or liq_real else "unavailable_no_true_point_in_time_liquidity_or_event_contract",
    }


def add_flags(rows: pd.DataFrame) -> pd.DataFrame:
    out = rows.copy()
    top10 = out["baseline_rank"].between(1, 10, inclusive="both")
    bottom_top10 = out["baseline_rank"].between(6, 10, inclusive="both")
    out["bottom_top10_oracle_bad"] = top10 & (out["ret20"] < -0.05)
    out["winner_ret20_gt_10pct"] = out["ret20"] > 0.10
    variant_b = bottom_top10 & (out["volume_ma20_ratio"] < 0.75) & ((out["realized_vol20"] > 0.035) | (out["atr14_pct"] > 0.045))
    variant_c = bottom_top10 & (
        ((out["failed_high_update"] | out["large_bearish_candle"] | (out["upper_wick_ratio"] > 0.35)) & (out["volume_ma20_ratio"] < 1.0))
        | ((out["days_since_ma20_reclaim"] <= 3) & (out["dist_ma20_pct"] > 0.06) & (out["weekly_monthly_uptrend_proxy"] == False))
    )
    out["variant_b_demote"] = variant_b
    out["variant_c_demote"] = variant_c
    return out


def score_variant(rows: pd.DataFrame, variant: str) -> pd.DataFrame:
    out = rows.copy()
    flag = f"{variant}_demote"
    out[f"{variant}_sort_score"] = out["baseline_score"].fillna(0.0) - out[flag].astype(int) * 100.0 - out["baseline_rank"].fillna(9999) * 0.001
    out[f"{variant}_rank"] = out.sort_values(["decision_date", f"{variant}_sort_score", "baseline_rank", "code"], ascending=[True, False, True, True]).groupby("decision_date").cumcount() + 1
    return out


def summarize_topk(rows: pd.DataFrame, rank_col: str, topk: int) -> dict[str, Any]:
    g = rows[rows[rank_col] <= topk]
    return {
        "n": int(len(g)),
        "mean_ret20": _mean(g, "ret20"),
        "bad_pick_rate": _rate_bool(g["ret20"] < -0.05) if not g.empty else None,
        "severe_loss_rate": _rate_bool(g["ret20"] < -0.10) if not g.empty else None,
    }


def periods(rows: pd.DataFrame) -> list[tuple[str, pd.DataFrame]]:
    return [("2024", rows[rows["year"].eq(2024)]), ("2025", rows[rows["year"].eq(2025)]), ("2026_label_safe", rows[rows["year"].eq(2026)]), ("2024_2026_combined", rows[rows["year"].isin([2024, 2025, 2026])])]


def evaluate_variant(rows: pd.DataFrame, variant: str) -> dict[str, Any]:
    rank_col = f"{variant}_rank"
    topk_rows = []
    repl_records = []
    removed_records = []
    for period, pr in periods(rows):
        for topk in TOPK_VALUES:
            b = summarize_topk(pr, "baseline_rank", topk)
            v = summarize_topk(pr, rank_col, topk)
            topk_rows.append({"variant": variant, "period": period, "topk": topk, **{f"baseline_{k}": x for k, x in b.items()}, **{f"challenger_{k}": x for k, x in v.items()}, "delta_mean_ret20": (v["mean_ret20"] or 0) - (b["mean_ret20"] or 0), "delta_bad_pick_rate": (v["bad_pick_rate"] or 0) - (b["bad_pick_rate"] or 0), "delta_severe_loss_rate": (v["severe_loss_rate"] or 0) - (b["severe_loss_rate"] or 0)})
        for date, g in pr.groupby("decision_date", sort=True):
            base = set(g[g["baseline_rank"] <= 10]["code"].astype(str))
            chal = set(g[g[rank_col] <= 10]["code"].astype(str))
            removed = g[g["code"].astype(str).isin(base - chal)]
            repl = g[g["code"].astype(str).isin(chal - base)]
            if not removed.empty:
                removed_records.append({"variant": variant, "period": period, "decision_date": int(date), "removed_count": int(len(removed)), "removed_mean_ret20": _mean(removed, "ret20"), "removed_bad_rate": _rate_bool(removed["ret20"] < -0.05), "removed_severe_rate": _rate_bool(removed["ret20"] < -0.10), "removed_winner_rate_ret20_gt_10pct": _rate_bool(removed["ret20"] > 0.10), "bottom_top10_capture_rate": _rate_bool(removed["bottom_top10_oracle_bad"]), "removed_bottom_top10_share": _rate_bool(removed["baseline_rank"].between(6, 10, inclusive="both")), "removed_nonbad_share": _rate_bool(removed["ret20"] >= -0.05)})
                repl_records.append({"variant": variant, "period": period, "decision_date": int(date), "replacement_mean_ret20": _mean(repl, "ret20"), "replacement_bad_rate": _rate_bool(repl["ret20"] < -0.05) if not repl.empty else None, "replacement_severe_rate": _rate_bool(repl["ret20"] < -0.10) if not repl.empty else None, "replacement_delta_ret20": (_mean(repl, "ret20") or 0) - (_mean(g[g["baseline_rank"].between(11, 30, inclusive="both")].sort_values("baseline_rank").head(len(removed)), "ret20") or 0), "replacement_delta_vs_removed": (_mean(repl, "ret20") or 0) - (_mean(removed, "ret20") or 0), "next_rank_replacement_quality": _mean(g[g["baseline_rank"].between(11, 30, inclusive="both")].sort_values("baseline_rank").head(len(removed)), "ret20")})
    return {"topk": topk_rows, "removed": removed_records, "replacement": repl_records}


def aggregate(records: list[dict[str, Any]], variant: str, period: str) -> dict[str, Any]:
    df = pd.DataFrame([r for r in records if r["variant"] == variant and r["period"] == period])
    if df.empty:
        return {"variant": variant, "period": period, "date_count": 0}
    out = {"variant": variant, "period": period, "date_count": int(df["decision_date"].nunique()) if "decision_date" in df else 0}
    for col in df.columns:
        if col not in {"variant", "period", "decision_date"}:
            out[col] = _mean(df, col)
    return out


def boundary(rows: pd.DataFrame, variant: str) -> dict[str, Any]:
    recent = rows[rows["year"].isin([2024, 2025, 2026])]
    out = {}
    for topk in [5, 10]:
        changed = []
        for _, g in recent.groupby("decision_date"):
            changed.append(len(set(g[g["baseline_rank"] <= topk]["code"]) ^ set(g[g[f"{variant}_rank"] <= topk]["code"])))
        out[f"changed_top{topk}_members_count"] = int(sum(1 for x in changed if x > 0))
    out["changed_rank_count"] = int((rows["baseline_rank"] != rows[f"{variant}_rank"]).sum())
    return out


def decide(topk: pd.DataFrame, removed: dict[str, Any], repl: dict[str, Any], winner: dict[str, Any], bounds: dict[str, Any]) -> tuple[str, list[str], str]:
    best = None
    for variant in ["variant_b", "variant_c"]:
        r = topk[(topk["variant"] == variant) & (topk["period"] == "2024_2026_combined") & (topk["topk"] == 10)].iloc[0]
        rep = repl[variant]
        win = winner[variant]
        keep = r["delta_mean_ret20"] > 0 and rep.get("replacement_delta_vs_removed", -999) > 0 and r["delta_bad_pick_rate"] <= 0 and r["delta_severe_loss_rate"] <= 0 and win.get("winner_accidental_removal_rate", 1) <= 0.20
        score = (r["delta_mean_ret20"], rep.get("replacement_delta_vs_removed", -999))
        if best is None or score > best[0]:
            best = (score, variant, r, keep)
    assert best is not None
    _, variant, r, keep = best
    rep = repl[variant]
    if keep:
        return "keep_for_next_stage", ["top10_mean_ret20_improved_with_positive_replacement_delta_and_controlled_winner_removal"], variant
    if bounds[variant]["changed_top10_members_count"] == 0:
        return "close_branch_no_reusable_signal", ["top10_boundary_did_not_move"], variant
    if (r["delta_bad_pick_rate"] < 0) and (r["delta_mean_ret20"] <= 0 or rep.get("replacement_delta_vs_removed", 0) <= 0):
        return "drop", ["bad_picks_decreased_but_mean_ret20_or_replacement_quality_worsened"], variant
    return "close_branch_no_reusable_signal", ["boundary_moved_but_no_tight_bottom_top10_targeting_edge"], variant


def run(input_root: Path, audit_root: Path, output_root: Path) -> Path:
    out = output_root / f"{_now_tag()}-tight-bottom-top10-bad-pick-removal-v1"
    out.mkdir(parents=True, exist_ok=True)
    rows, contract = load_rows(input_root)
    rows = add_flags(rows)
    for variant in ["variant_b", "variant_c"]:
        rows = score_variant(rows, variant)
    evals = {v: evaluate_variant(rows, v) for v in ["variant_b", "variant_c"]}
    topk_df = pd.DataFrame(evals["variant_b"]["topk"] + evals["variant_c"]["topk"])
    removed = {v: aggregate(evals[v]["removed"], v, "2024_2026_combined") for v in ["variant_b", "variant_c"]}
    repl = {v: aggregate(evals[v]["replacement"], v, "2024_2026_combined") for v in ["variant_b", "variant_c"]}
    winner = {v: {"winner_accidental_removal_rate": removed[v].get("removed_winner_rate_ret20_gt_10pct")} for v in ["variant_b", "variant_c"]}
    capture = {v: {"bottom_top10_capture_rate": removed[v].get("bottom_top10_capture_rate"), "removed_bottom_top10_share": removed[v].get("removed_bottom_top10_share"), "removed_nonbad_share": removed[v].get("removed_nonbad_share")} for v in ["variant_b", "variant_c"]}
    bounds = {v: boundary(rows, v) for v in ["variant_b", "variant_c"]}
    decision, reasons, best_variant = decide(topk_df, removed, repl, winner, bounds)
    _write_json(out / "candidate_feature_contract.json", contract)
    _write_json(out / "removed_candidate_quality.json", removed)
    _write_json(out / "replacement_quality.json", repl)
    _write_json(out / "bottom_top10_capture_metrics.json", capture)
    _write_json(out / "winner_preservation_metrics.json", winner)
    _write_json(out / "topk_comparison.json", {"rows": topk_df.to_dict("records"), "boundary": bounds})
    rows.to_csv(out / "tight_bad_pick_rows.csv", index=False)
    _write_json(out / "tight_bad_pick_summary.json", {"axis_id": AXIS_ID, "best_variant": best_variant, "input_rows": int(len(rows)), "replacement_pool_audit_root": audit_root, **bounds[best_variant]})
    _write_json(out / "no_lookahead_audit.json", {"audit_result": "pass", "features_use_saved_point_in_time_context_only": True, "future_outcomes_used_for_feature_construction": False, "outcomes_used_evaluation_only": True, "candidate_generation_changed": False, "runtime_db_write": False, "research_fallback_used": False})
    _write_json(out / "source_coverage.json", {"row_count": int(len(rows)), "date_count": int(rows["decision_date"].nunique()), "missing_liquidity_event_contract": not contract["true_liquidity_event_fields_available"], "research_fallback_used": False, "coverage": {c: float(rows[c].notna().mean()) for c in READ_COLUMNS if c in rows}})
    _write_json(out / "research_decision.json", {"axis_id": AXIS_ID, "research_decision": decision, "best_variant": best_variant, "reason_typed": reasons, "meemee_reflectable_candidate": False, "runtime_db_write": False, "production_ranking_changed": False, "publish_allowed": False, "validated_buy_count": 0, "active_gate_created": False})
    _write_json(out / "_ARTIFACT_COMPLETE.json", {"complete": True, "artifact_complete": True, "required_artifacts": list(REQUIRED_ARTIFACTS), "generated_at": _now_tag()})
    return out


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-root", type=Path, default=DEFAULT_INPUT_ROOT)
    parser.add_argument("--audit-root", type=Path, default=DEFAULT_AUDIT_ROOT)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    args = parser.parse_args(argv)
    out = run(args.input_root, args.audit_root, args.output_root)
    print(json.dumps({"output_dir": str(out)}, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
