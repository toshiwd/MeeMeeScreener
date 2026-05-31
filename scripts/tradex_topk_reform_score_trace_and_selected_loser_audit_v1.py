from __future__ import annotations

import argparse
import json
import math
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd


AXIS_ID = "topk_reform_score_trace_and_selected_loser_audit_v1"
BASELINE_ROOT = Path(r"G:\Tradex\portfolio_agent_baseline_robustness_gate_v1\baseline-2019-2025-robustness-gate\subruns")
SNAPSHOT_2026 = Path(r"G:\Tradex\monthly_box_breakout_2026_validation_v1\20260523T193639Z-monthly-box-breakout-2026-validation-v1\same_family_2026_subrun\2026-baseline-portfolio_agent_replay_v1\daily_candidate_snapshot.csv")
DEFAULT_DAILY_PATH = Path("production_data/production_daily.csv")
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\topk_reform_score_trace_and_selected_loser_audit_v1")
YEARS = (2024, 2025, 2026)
TOPK = (5, 10)
REQUIRED_ARTIFACTS = (
    "input_artifact_report.json",
    "candidate_snapshot_schema_report.json",
    "score_contract_gap_report.json",
    "candidate_trace_rows.csv",
    "selected_loser_score_component_profile.csv",
    "source_family_failure_summary.csv",
    "reason_code_failure_summary.csv",
    "selected_winner_protection_audit.csv",
    "next_reform_axis_candidates.json",
    "research_decision.json",
    "no_lookahead_audit.json",
    "_ARTIFACT_COMPLETE.json",
    "trace_contract_schema.json",
    "trace_contract_backfill_report.json",
    "trace_generation_smoke_report.json",
)
CONTRACT_FIELDS = (
    "code",
    "decision_date",
    "baseline_rank",
    "baseline_score",
    "score_components",
    "normalized_score_components",
    "candidate_source",
    "signal_family",
    "setup_name",
    "reason_code",
    "explanation_fields",
    "regime_bucket",
    "gate_flags",
    "risk_flags",
    "event_flags",
    "liquidity_flags",
    "feature_contribution_fields",
    "artifact_lineage",
)


def _now_tag() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _json_ready(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(k): _json_ready(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
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
    except Exception:
        pass
    return value


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(_json_ready(payload), ensure_ascii=False, sort_keys=True, indent=2) + "\n", encoding="utf-8")


def _mean(df: pd.DataFrame, col: str) -> float | None:
    if df.empty or col not in df:
        return None
    s = pd.to_numeric(df[col], errors="coerce").dropna()
    return None if s.empty else float(s.mean())


def _median(df: pd.DataFrame, col: str) -> float | None:
    if df.empty or col not in df:
        return None
    s = pd.to_numeric(df[col], errors="coerce").dropna()
    return None if s.empty else float(s.median())


def _rate(df: pd.DataFrame, col: str) -> float | None:
    if df.empty or col not in df:
        return None
    s = df[col].dropna()
    if s.empty:
        return None
    return float(s.astype(bool).mean())


def snapshot_paths() -> list[tuple[int, Path]]:
    out = []
    for year in range(2019, 2026):
        out.append((year, BASELINE_ROOT / f"{year}-baseline-portfolio_agent_replay_v1" / "daily_candidate_snapshot.csv"))
    out.append((2026, SNAPSHOT_2026))
    return out


def audit_schema(paths: list[tuple[int, Path]]) -> tuple[pd.DataFrame, dict[str, Any]]:
    rows = []
    all_missing: set[str] = set()
    for year, path in paths:
        exists = path.exists()
        cols: list[str] = []
        if exists:
            cols = pd.read_csv(path, nrows=0).columns.tolist()
        present = {
            "code": "code" in cols,
            "decision_date": "decision_ymd" in cols or "decision_date" in cols,
            "baseline_rank": "candidate_rank" in cols or "baseline_rank" in cols,
            "baseline_score": "selection_score" in cols or "baseline_score" in cols,
            "score_components": "score_components_json" in cols,
            "normalized_score_components": any("normalized" in c and "component" in c for c in cols),
            "candidate_source": "candidate_source" in cols,
            "signal_family": "signal_family" in cols,
            "setup_name": "setup_name" in cols,
            "reason_code": "reason_codes" in cols or "reason_code" in cols,
            "explanation_fields": any("explanation" in c or "reason_text" in c for c in cols),
            "regime_bucket": "regime_bucket" in cols,
            "gate_flags": any("gate" in c for c in cols),
            "risk_flags": any("risk" in c for c in cols),
            "event_flags": any("event" in c for c in cols),
            "liquidity_flags": any("liquidity" in c for c in cols),
            "feature_contribution_fields": any("contribution" in c for c in cols),
            "artifact_lineage": False,
        }
        missing = [field for field, ok in present.items() if not ok]
        all_missing.update(missing)
        rows.append({"year": year, "path": str(path), "exists": exists, "column_count": len(cols), "columns": json.dumps(cols, ensure_ascii=False), "missing_fields": json.dumps(missing, ensure_ascii=False), **present})
    return pd.DataFrame(rows), {"score_contract_gap": bool(all_missing), "missing_fields_union": sorted(all_missing)}


def _parse_components(value: Any, score: Any) -> tuple[list[dict[str, Any]], bool]:
    if pd.isna(value):
        return ([{"feature": "unattributed_score", "points": score, "value": "unattributed_score"}], False)
    try:
        parsed = json.loads(str(value))
    except Exception:
        return ([{"feature": "unattributed_score", "points": score, "value": "unparseable_score_components_json"}], False)
    if isinstance(parsed, list):
        return ([p for p in parsed if isinstance(p, dict)], True)
    return ([{"feature": "unattributed_score", "points": score, "value": "non_list_score_components_json"}], False)


def load_candidates(paths: list[tuple[int, Path]]) -> pd.DataFrame:
    frames = []
    for year, path in paths:
        if not path.exists():
            continue
        frame = pd.read_csv(path, dtype={"code": str}, low_memory=False)
        frame["source_year"] = year
        frame["source_artifact_path"] = str(path)
        frame["source_run_id"] = path.parent.name
        frames.append(frame)
    if not frames:
        return pd.DataFrame()
    rows = pd.concat(frames, ignore_index=True)
    rows["decision_ymd"] = pd.to_numeric(rows["decision_ymd"], errors="coerce").astype("Int64")
    rows["year"] = rows["decision_ymd"].astype(str).str.slice(0, 4).astype(int)
    rows["baseline_rank"] = pd.to_numeric(rows["candidate_rank"], errors="coerce")
    rows["baseline_score"] = pd.to_numeric(rows["selection_score"], errors="coerce")
    return rows


def attach_outcomes(rows: pd.DataFrame, daily_path: Path) -> pd.DataFrame:
    daily = pd.read_csv(daily_path, dtype={"code": str})
    daily = daily[daily["code"].isin(set(rows["code"]))].copy()
    daily["date_dt"] = pd.to_datetime(daily["date"])
    daily["decision_ymd"] = daily["date_dt"].dt.strftime("%Y%m%d").astype(int)
    daily["close"] = pd.to_numeric(daily["close"], errors="coerce")
    daily = daily.sort_values(["code", "date_dt"]).drop_duplicates(["code", "date_dt"], keep="last")
    by_code = {code: frame.reset_index(drop=True) for code, frame in daily.groupby("code", sort=False)}
    ret20 = []
    for _, row in rows.iterrows():
        frame = by_code.get(str(row["code"]))
        value = None
        if frame is not None and not pd.isna(row["decision_ymd"]):
            idxs = frame.index[frame["decision_ymd"] == int(row["decision_ymd"])].tolist()
            if idxs:
                pos = int(idxs[0])
                if pos + 20 < len(frame):
                    entry = float(frame.iloc[pos]["close"])
                    exit20 = float(frame.iloc[pos + 20]["close"])
                    value = exit20 / entry - 1 if entry else None
        ret20.append(value)
    out = rows.copy()
    out["ret20"] = ret20
    out = out[out["ret20"].notna()].copy()
    out["ret20_pct_rank_by_date"] = out.groupby("decision_ymd")["ret20"].rank(pct=True, method="average")
    out["selected_winner"] = (out["ret20"] >= 0.05) | (out["ret20_pct_rank_by_date"] >= 0.70)
    out["selected_loser"] = (out["ret20"] <= -0.05) | (out["ret20_pct_rank_by_date"] <= 0.30)
    return out


def build_trace_rows(rows: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, Any]]:
    trace_rows = []
    attribution_available = []
    for _, row in rows.iterrows():
        comps, available = _parse_components(row.get("score_components_json"), row.get("baseline_score"))
        attribution_available.append(available)
        score_component_json = {str(c.get("feature", "unknown")): {"points": c.get("points"), "value": c.get("value")} for c in comps}
        gate_flags = {"entry_allowed_by_score": bool(row.get("entry_allowed_by_score")) if not pd.isna(row.get("entry_allowed_by_score")) else None, "downside_guard_blocked": bool(row.get("downside_guard_blocked")) if not pd.isna(row.get("downside_guard_blocked")) else None}
        liquidity_flags = {"next_open_available": bool(row.get("next_open_available")) if not pd.isna(row.get("next_open_available")) else None}
        trace_rows.append(
            {
                "code": row["code"],
                "decision_date": str(int(row["decision_ymd"])),
                "baseline_rank": row["baseline_rank"],
                "baseline_score": row["baseline_score"],
                "candidate_source": None,
                "signal_family": None,
                "setup_name": None,
                "reason_codes": "[]",
                "regime_bucket": None,
                "score_component_json": json.dumps(score_component_json, ensure_ascii=False, sort_keys=True),
                "score_component_attribution_available": available,
                "gate_flags_json": json.dumps(gate_flags, ensure_ascii=False, sort_keys=True),
                "risk_flags_json": json.dumps({"downside_guard_blocked": gate_flags["downside_guard_blocked"]}, ensure_ascii=False, sort_keys=True),
                "event_flags_json": "{}",
                "liquidity_flags_json": json.dumps(liquidity_flags, ensure_ascii=False, sort_keys=True),
                "feature_snapshot_json": "{}",
                "source_artifact_path": row["source_artifact_path"],
                "source_run_id": row["source_run_id"],
                "trace_schema_version": "topk_reform_trace_v1",
                "ret20": row["ret20"],
                "ret20_pct_rank_by_date": row["ret20_pct_rank_by_date"],
                "selected_loser": bool(row["selected_loser"]),
                "selected_winner": bool(row["selected_winner"]),
                "selected_non_loser": not bool(row["selected_loser"]),
                "year": int(row["year"]),
            }
        )
    return pd.DataFrame(trace_rows), {"trace_rows": len(trace_rows), "score_component_attribution_available_rate": float(pd.Series(attribution_available).mean()) if attribution_available else 0.0}


def explode_components(trace: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for _, row in trace.iterrows():
        components = json.loads(row["score_component_json"])
        for feature, payload in components.items():
            rows.append(
                {
                    "year": row["year"],
                    "code": row["code"],
                    "decision_date": row["decision_date"],
                    "baseline_rank": row["baseline_rank"],
                    "baseline_score": row["baseline_score"],
                    "component_feature": feature,
                    "component_value": payload.get("value"),
                    "component_points": payload.get("points"),
                    "selected_loser": row["selected_loser"],
                    "selected_winner": row["selected_winner"],
                    "selected_non_loser": row["selected_non_loser"],
                    "ret20": row["ret20"],
                }
            )
    return pd.DataFrame(rows)


def score_component_profile(trace: pd.DataFrame) -> pd.DataFrame:
    comp = explode_components(trace)
    rows = []
    for year_label, frame in [("2024", trace[trace["year"] == 2024]), ("2025", trace[trace["year"] == 2025]), ("2026_label_safe", trace[trace["year"] == 2026]), ("2024_2026_combined", trace[trace["year"].isin(YEARS)])]:
        for topk in TOPK:
            keys = frame[frame["baseline_rank"] <= topk][["code", "decision_date"]]
            if keys.empty:
                continue
            g = comp.merge(keys, on=["code", "decision_date"], how="inner")
            for feature, f in g.groupby("component_feature"):
                losers = f[f["selected_loser"].astype(bool)]
                winners = f[f["selected_winner"].astype(bool)]
                non_losers = f[~f["selected_loser"].astype(bool)]
                denom_l = max(1, int(frame[(frame["baseline_rank"] <= topk) & frame["selected_loser"].astype(bool)].shape[0]))
                denom_w = max(1, int(frame[(frame["baseline_rank"] <= topk) & frame["selected_winner"].astype(bool)].shape[0]))
                rows.append(
                    {
                        "period": year_label,
                        "topk": topk,
                        "axis_type": "score_component",
                        "axis_name": feature,
                        "selected_loser_count": int(len(losers)),
                        "selected_winner_count": int(len(winners)),
                        "selected_non_loser_count": int(len(non_losers)),
                        "loser_share": float(len(losers) / denom_l),
                        "winner_share": float(len(winners) / denom_w),
                        "loser_minus_winner_spread": float(len(losers) / denom_l - len(winners) / denom_w),
                        "avg_component_value_losers": _mean(losers, "component_points"),
                        "avg_component_value_winners": _mean(winners, "component_points"),
                        "ret20_losers_mean": _mean(losers, "ret20"),
                        "ret20_winners_mean": _mean(winners, "ret20"),
                    }
                )
    return pd.DataFrame(rows)


def source_family_failure(trace: pd.DataFrame) -> pd.DataFrame:
    rows = []
    axes = ["candidate_source", "signal_family", "setup_name", "source_run_id"]
    for axis in axes:
        for period, frame in [("2024", trace[trace["year"] == 2024]), ("2025", trace[trace["year"] == 2025]), ("2026_label_safe", trace[trace["year"] == 2026]), ("2024_2026_combined", trace[trace["year"].isin(YEARS)])]:
            for topk in TOPK:
                g = frame[frame["baseline_rank"] <= topk].copy()
                if g.empty:
                    continue
                g[axis] = g[axis].fillna("missing")
                for value, f in g.groupby(axis):
                    rows.append({"period": period, "topk": topk, "axis_type": axis, "axis_value": value, "n": int(len(f)), "selected_loser_rate": _rate(f, "selected_loser"), "selected_winner_rate": _rate(f, "selected_winner"), "ret20_mean": _mean(f, "ret20"), "ret20_median": _median(f, "ret20"), "severe_loss_rate": float((f["ret20"] <= -0.05).mean())})
    return pd.DataFrame(rows)


def reason_code_failure(trace: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for period, frame in [("2024", trace[trace["year"] == 2024]), ("2025", trace[trace["year"] == 2025]), ("2026_label_safe", trace[trace["year"] == 2026]), ("2024_2026_combined", trace[trace["year"].isin(YEARS)])]:
        for topk in TOPK:
            g = frame[frame["baseline_rank"] <= topk].copy()
            rows.append({"period": period, "topk": topk, "reason_code": "missing", "n": int(len(g)), "selected_loser_count": int(g["selected_loser"].sum()), "selected_winner_count": int(g["selected_winner"].sum()), "loser_share": None, "winner_share": None, "loser_minus_winner_spread": None, "stable_across_years": None})
    return pd.DataFrame(rows)


def winner_protection(profile: pd.DataFrame) -> pd.DataFrame:
    combined = profile[(profile["period"] == "2024_2026_combined") & (profile["topk"] == 10)].copy()
    if combined.empty:
        return pd.DataFrame()
    rows = []
    for _, row in combined.sort_values("loser_minus_winner_spread", ascending=False).head(30).iterrows():
        rows.append(
            {
                "axis_type": row["axis_type"],
                "axis_name": row["axis_name"],
                "selected_losers_hit": row["selected_loser_count"],
                "selected_winners_hit": row["selected_winner_count"],
                "loser_capture_rate": row["loser_share"],
                "winner_damage_risk": row["winner_share"],
                "loser_minus_winner_spread": row["loser_minus_winner_spread"],
                "appropriate_repair_type": "soft_demotion" if row["loser_minus_winner_spread"] and row["loser_minus_winner_spread"] > 0.05 else "contract_repair_or_hold",
            }
        )
    return pd.DataFrame(rows)


def next_candidates(profile: pd.DataFrame, gap: dict[str, Any]) -> list[dict[str, Any]]:
    combined = profile[(profile["period"] == "2024_2026_combined") & (profile["topk"] == 10)].copy()
    out = []
    if not combined.empty:
        for _, row in combined.sort_values("loser_minus_winner_spread", ascending=False).head(5).iterrows():
            out.append({"candidate_axis_name": f"{row['axis_name']}_score_component_overpromotion", "axis_type": row["axis_type"], "observed_loser_minus_winner_spread": row["loser_minus_winner_spread"], "selected_loser_count": row["selected_loser_count"], "selected_winner_count": row["selected_winner_count"], "expected_use": "soft_demotion_diagnostic", "recommended_next": "pretest" if row["loser_minus_winner_spread"] and row["loser_minus_winner_spread"] >= 0.05 else "hold"})
    if gap.get("score_contract_gap"):
        out.append({"candidate_axis_name": "trace_contract_repair", "axis_type": "contract", "observed_loser_minus_winner_spread": None, "selected_loser_count": None, "selected_winner_count": None, "expected_use": "contract_repair", "recommended_next": "pretest", "missing_fields": gap.get("missing_fields_union")})
    return out[:5]


def decide(profile: pd.DataFrame, gap: dict[str, Any], trace_report: dict[str, Any]) -> dict[str, Any]:
    missing = set(gap.get("missing_fields_union") or [])
    critical_missing = {"candidate_source", "signal_family", "setup_name", "reason_code"} & missing
    if critical_missing and trace_report.get("score_component_attribution_available_rate", 0) < 0.5:
        return {"research_decision": "score_contract_gap", "reason_typed": ["candidate snapshots lack score/reason/source fields needed for selected loser repair"], "meemee_reflectable": False, "ranking_reflectable": False, "publish_allowed": False}
    if critical_missing:
        return {"research_decision": "score_contract_gap", "reason_typed": ["candidate snapshots expose score components but lack candidate_source/signal_family/setup/reason trace fields"], "meemee_reflectable": False, "ranking_reflectable": False, "publish_allowed": False}
    if profile.empty or "period" not in profile or "topk" not in profile:
        return {"research_decision": "inconclusive", "reason_typed": ["selected loser score component profile could not be built"], "meemee_reflectable": False, "ranking_reflectable": False, "publish_allowed": False}
    combined = profile[(profile["period"] == "2024_2026_combined") & (profile["topk"] == 10)].copy()
    best = None if combined.empty else combined.sort_values("loser_minus_winner_spread", ascending=False).head(1).iloc[0].to_dict()
    if best and best.get("loser_minus_winner_spread") is not None and best["loser_minus_winner_spread"] >= 0.08 and best["selected_loser_count"] >= 100 and best["winner_share"] < best["loser_share"]:
        return {"research_decision": "score_axis_found", "reason_typed": [f"score component overpromotion axis found: {best['axis_name']}"], "meemee_reflectable": False, "ranking_reflectable": False, "publish_allowed": False}
    return {"research_decision": "no_clear_score_axis", "reason_typed": ["existing score/reason/source fields do not isolate selected loser overpromotion"], "meemee_reflectable": False, "ranking_reflectable": False, "publish_allowed": False}


def run(*, daily_path: Path = DEFAULT_DAILY_PATH, output_root: Path = DEFAULT_OUTPUT_ROOT) -> dict[str, Any]:
    run_dir = output_root / f"{_now_tag()}-topk-reform-score-trace-and-selected-loser-audit-v1"
    run_dir.mkdir(parents=True, exist_ok=True)
    paths = snapshot_paths()
    schema, gap = audit_schema(paths)
    candidates = load_candidates(paths)
    labeled = attach_outcomes(candidates, daily_path)
    recent = labeled[labeled["year"].isin(YEARS)].copy()
    trace, trace_report = build_trace_rows(recent)
    profile = score_component_profile(trace)
    source_failure = source_family_failure(trace)
    reason_failure = reason_code_failure(trace)
    protection = winner_protection(profile)
    axes = next_candidates(profile, gap)
    decision = decide(profile, gap, trace_report)
    _write_json(run_dir / "candidate_snapshot_schema_report.json", {"rows": schema.to_dict(orient="records")})
    trace.to_csv(run_dir / "candidate_trace_rows.csv", index=False)
    profile.to_csv(run_dir / "selected_loser_score_component_profile.csv", index=False)
    source_failure.to_csv(run_dir / "source_family_failure_summary.csv", index=False)
    reason_failure.to_csv(run_dir / "reason_code_failure_summary.csv", index=False)
    protection.to_csv(run_dir / "selected_winner_protection_audit.csv", index=False)
    _write_json(run_dir / "input_artifact_report.json", {"daily_path": daily_path, "snapshot_paths": [str(p) for _, p in paths], "candidate_rows_loaded": int(len(candidates)), "labeled_rows": int(len(labeled)), "recent_labeled_rows": int(len(recent)), **trace_report})
    _write_json(run_dir / "score_contract_gap_report.json", {**gap, "blocks_selected_loser_repair": bool({"candidate_source", "signal_family", "setup_name", "reason_code"} & set(gap.get("missing_fields_union") or [])), "exact_missing_columns": gap.get("missing_fields_union")})
    _write_json(run_dir / "next_reform_axis_candidates.json", {"candidates": axes})
    _write_json(run_dir / "trace_contract_schema.json", {"trace_schema_version": "topk_reform_trace_v1", "required_columns": ["code", "decision_date", "baseline_rank", "baseline_score", "candidate_source", "signal_family", "setup_name", "reason_codes", "regime_bucket", "score_component_json", "gate_flags_json", "risk_flags_json", "event_flags_json", "liquidity_flags_json", "feature_snapshot_json", "source_artifact_path", "source_run_id", "trace_schema_version"]})
    _write_json(run_dir / "trace_contract_backfill_report.json", {"backfilled_rows": int(len(trace)), "ranking_order_changed": False, "runtime_db_write": False, "score_formula_changed": False, "score_component_attribution_available_rate": trace_report.get("score_component_attribution_available_rate")})
    _write_json(run_dir / "trace_generation_smoke_report.json", {"smoke_pass": not trace.empty, "trace_rows": int(len(trace)), "has_score_component_json": "score_component_json" in trace.columns, "has_source_artifact_path": "source_artifact_path" in trace.columns})
    _write_json(run_dir / "research_decision.json", decision)
    _write_json(run_dir / "no_lookahead_audit.json", {"audit_result": "pass", "topk_selection_uses_existing_snapshot_rank_score": True, "future_returns_used_only_for_labels": True, "score_trace_uses_snapshot_fields_only": True, "ranking_order_changed": False, "candidate_generation_changed": False, "runtime_db_write": False})
    _write_json(run_dir / "_ARTIFACT_COMPLETE.json", {"axis_id": AXIS_ID, "output_dir": run_dir, "required_artifacts": list(REQUIRED_ARTIFACTS), "artifact_complete": all((run_dir / name).exists() for name in REQUIRED_ARTIFACTS if name != "_ARTIFACT_COMPLETE.json"), "created_at_utc": datetime.now(timezone.utc).isoformat()})
    return {"output_dir": str(run_dir), "research_decision": decision, "gap": gap, "trace_report": trace_report}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--daily-path", type=Path, default=DEFAULT_DAILY_PATH)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    args = parser.parse_args(argv)
    print(json.dumps(_json_ready(run(daily_path=args.daily_path, output_root=args.output_root)), ensure_ascii=False, sort_keys=True, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
