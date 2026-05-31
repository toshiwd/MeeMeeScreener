from __future__ import annotations

import json
import math
from pathlib import Path
from typing import Any

import pandas as pd


YEARS = (2024, 2025, 2026)
TOPK = (5, 10)


def json_ready(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(k): json_ready(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [json_ready(v) for v in value]
    if isinstance(value, Path):
        return str(value)
    try:
        import numpy as np

        if isinstance(value, np.generic):
            return json_ready(value.item())
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


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(json_ready(payload), ensure_ascii=False, sort_keys=True, indent=2) + "\n", encoding="utf-8")


def mean(df: pd.DataFrame, col: str) -> float | None:
    if df.empty or col not in df:
        return None
    s = pd.to_numeric(df[col], errors="coerce").dropna()
    return None if s.empty else float(s.mean())


def median(df: pd.DataFrame, col: str) -> float | None:
    if df.empty or col not in df:
        return None
    s = pd.to_numeric(df[col], errors="coerce").dropna()
    return None if s.empty else float(s.median())


def rate(df: pd.DataFrame, col: str) -> float | None:
    if df.empty or col not in df:
        return None
    s = df[col].dropna()
    if s.empty:
        return None
    return float(s.astype(bool).mean())


def attach_ret20(trace: pd.DataFrame, daily_path: Path) -> pd.DataFrame:
    daily = pd.read_csv(daily_path, dtype={"code": str})
    daily = daily[daily["code"].isin(set(trace["code"].astype(str)))].copy()
    daily["date_dt"] = pd.to_datetime(daily["date"])
    daily["decision_date"] = daily["date_dt"].dt.strftime("%Y%m%d").astype(int)
    daily["close"] = pd.to_numeric(daily["close"], errors="coerce")
    daily = daily.sort_values(["code", "date_dt"]).drop_duplicates(["code", "date_dt"], keep="last")
    by_code = {code: frame.reset_index(drop=True) for code, frame in daily.groupby("code", sort=False)}
    values = []
    for _, row in trace.iterrows():
        value = None
        frame = by_code.get(str(row["code"]))
        if frame is not None:
            idxs = frame.index[frame["decision_date"] == int(row["decision_date"])].tolist()
            if idxs:
                pos = int(idxs[0])
                if pos + 20 < len(frame):
                    entry = float(frame.iloc[pos]["close"])
                    exit20 = float(frame.iloc[pos + 20]["close"])
                    value = exit20 / entry - 1 if entry else None
        values.append(value)
    out = trace.copy()
    out["ret20"] = values
    out = out[out["ret20"].notna()].copy()
    out["ret20_pct_rank_by_date"] = out.groupby("decision_date")["ret20"].rank(pct=True, method="average")
    out["selected_loser"] = (out["ret20"] <= -0.05) | (out["ret20_pct_rank_by_date"] <= 0.30)
    out["selected_winner"] = (out["ret20"] >= 0.05) | (out["ret20_pct_rank_by_date"] >= 0.70)
    out["year"] = out["decision_date"].astype(str).str.slice(0, 4).astype(int)
    return out


def explode_components(trace: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for _, row in trace.iterrows():
        try:
            components = json.loads(str(row.get("score_components_json") or "{}"))
        except Exception:
            components = {}
        if isinstance(components, list):
            components = {str(c.get("feature", "unknown")): {"points": c.get("points"), "value": c.get("value")} for c in components if isinstance(c, dict)}
        for feature, payload in (components if isinstance(components, dict) else {}).items():
            rows.append(
                {
                    "year": row["year"],
                    "decision_date": row["decision_date"],
                    "code": row["code"],
                    "baseline_rank": row["baseline_rank"],
                    "component_feature": feature,
                    "component_points": payload.get("points") if isinstance(payload, dict) else None,
                    "component_value": payload.get("value") if isinstance(payload, dict) else None,
                    "selected_loser": row["selected_loser"],
                    "selected_winner": row["selected_winner"],
                    "ret20": row["ret20"],
                }
            )
    return pd.DataFrame(rows)


def score_component_failure_summary(trace: pd.DataFrame) -> pd.DataFrame:
    comp = explode_components(trace)
    rows = []
    for period, frame in [("2024", trace[trace["year"] == 2024]), ("2025", trace[trace["year"] == 2025]), ("2026_label_safe", trace[trace["year"] == 2026]), ("2024_2026_combined", trace[trace["year"].isin(YEARS)])]:
        for topk in TOPK:
            keys = frame[frame["baseline_rank"] <= topk][["decision_date", "code"]]
            if keys.empty:
                continue
            g = comp.merge(keys, on=["decision_date", "code"], how="inner")
            denom_l = max(1, int(frame[(frame["baseline_rank"] <= topk) & frame["selected_loser"]].shape[0]))
            denom_w = max(1, int(frame[(frame["baseline_rank"] <= topk) & frame["selected_winner"]].shape[0]))
            for feature, f in g.groupby("component_feature"):
                losers = f[f["selected_loser"]]
                winners = f[f["selected_winner"]]
                rows.append({"period": period, "topk": topk, "component_feature": feature, "selected_loser_count": int(len(losers)), "selected_winner_count": int(len(winners)), "loser_share": float(len(losers) / denom_l), "winner_share": float(len(winners) / denom_w), "loser_minus_winner_spread": float(len(losers) / denom_l - len(winners) / denom_w), "component_mean_losers": mean(losers, "component_points"), "component_mean_winners": mean(winners, "component_points")})
    return pd.DataFrame(rows)


def source_family_failure_summary(trace: pd.DataFrame) -> pd.DataFrame:
    rows = []
    axes = ["candidate_source", "signal_family", "setup_name", "source_run_id"]
    for period, frame in [("2024", trace[trace["year"] == 2024]), ("2025", trace[trace["year"] == 2025]), ("2026_label_safe", trace[trace["year"] == 2026]), ("2024_2026_combined", trace[trace["year"].isin(YEARS)])]:
        for topk in TOPK:
            g = frame[frame["baseline_rank"] <= topk].copy()
            if g.empty:
                continue
            for axis in axes:
                g[axis] = g[axis].fillna("missing")
                for value, f in g.groupby(axis):
                    rows.append({"period": period, "topk": topk, "axis_type": axis, "axis_value": value, "n": int(len(f)), "selected_loser_rate": rate(f, "selected_loser"), "selected_winner_rate": rate(f, "selected_winner"), "loser_minus_winner_rate": None if rate(f, "selected_loser") is None or rate(f, "selected_winner") is None else rate(f, "selected_loser") - rate(f, "selected_winner"), "ret20_mean": mean(f, "ret20"), "ret20_median": median(f, "ret20"), "severe_loss_rate": float((f["ret20"] <= -0.05).mean())})
    return pd.DataFrame(rows)


def reason_code_failure_summary(trace: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for period, frame in [("2024", trace[trace["year"] == 2024]), ("2025", trace[trace["year"] == 2025]), ("2026_label_safe", trace[trace["year"] == 2026]), ("2024_2026_combined", trace[trace["year"].isin(YEARS)])]:
        for topk in TOPK:
            g = frame[frame["baseline_rank"] <= topk]
            rows.append({"period": period, "topk": topk, "reason_code": "missing", "n": int(len(g)), "selected_loser_rate": rate(g, "selected_loser"), "selected_winner_rate": rate(g, "selected_winner"), "loser_minus_winner_rate": None if rate(g, "selected_loser") is None or rate(g, "selected_winner") is None else rate(g, "selected_loser") - rate(g, "selected_winner")})
    return pd.DataFrame(rows)


def trace_coverage_summary(trace: pd.DataFrame) -> dict[str, Any]:
    return {
        "rows": int(len(trace)),
        "candidate_source_available_rate": float(trace["candidate_source"].notna().mean()) if "candidate_source" in trace else 0.0,
        "signal_family_available_rate": float(trace["signal_family"].notna().mean()) if "signal_family" in trace else 0.0,
        "setup_name_available_rate": float(trace["setup_name"].notna().mean()) if "setup_name" in trace else 0.0,
        "reason_codes_available_rate": float((trace["reason_codes_json"].astype(str) != "[]").mean()) if "reason_codes_json" in trace else 0.0,
        "regime_bucket_available_rate": float(trace["regime_bucket"].notna().mean()) if "regime_bucket" in trace else 0.0,
        "score_component_attribution_available_rate": float(trace["score_component_attribution_available"].astype(bool).mean()) if "score_component_attribution_available" in trace else 0.0,
        "gate_flags_available_rate": float(trace["gate_flags_json"].notna().mean()) if "gate_flags_json" in trace else 0.0,
        "risk_flags_available_rate": float(trace["risk_flags_json"].notna().mean()) if "risk_flags_json" in trace else 0.0,
        "event_flags_available_rate": float((trace["event_flags_json"].astype(str) != "{}").mean()) if "event_flags_json" in trace else 0.0,
        "liquidity_flags_available_rate": float(trace["liquidity_flags_json"].notna().mean()) if "liquidity_flags_json" in trace else 0.0,
    }


def repair_axis_candidates(source: pd.DataFrame, score: pd.DataFrame, coverage: dict[str, Any]) -> list[dict[str, Any]]:
    out = []
    if coverage["candidate_source_available_rate"] == 0.0 and coverage["signal_family_available_rate"] == 0.0:
        out.append({"axis_name": "candidate_source_signal_family_contract", "based_on": "contract repair", "intended_use": "score contract repair", "selected_loser_hit_rate": None, "selected_winner_hit_rate": None, "winner_damage_risk": "unknown_until_semantics_exist", "sample_size": int(coverage["rows"]), "stability": "blocked_by_missing_semantics", "recommended_next": "contract_repair"})
    combined = (
        score[(score["period"] == "2024_2026_combined") & (score["topk"] == 10)].copy()
        if not score.empty and "period" in score and "topk" in score
        else pd.DataFrame()
    )
    if not combined.empty:
        for _, row in combined.sort_values("loser_minus_winner_spread", ascending=False).head(4).iterrows():
            out.append({"axis_name": f"{row['component_feature']}_score_component", "based_on": "score_component", "intended_use": "soft demotion", "selected_loser_hit_rate": row["loser_share"], "selected_winner_hit_rate": row["winner_share"], "winner_damage_risk": "high" if row["winner_share"] > 0.5 else "medium", "sample_size": int(row["selected_loser_count"] + row["selected_winner_count"]), "stability": "observed_component_only", "recommended_next": "drop" if abs(row["loser_minus_winner_spread"]) < 0.05 else "pretest"})
    return out[:5]
