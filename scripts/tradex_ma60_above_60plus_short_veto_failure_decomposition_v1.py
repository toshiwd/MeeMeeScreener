from __future__ import annotations

import argparse
import json
import math
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd


AXIS_ID = "ma60_above_60plus_short_veto_failure_decomposition_v1"
DEFAULT_INPUT_ROOT = Path(r"G:\Tradex\ma60_above_60plus_short_veto_replay_v1\20260523T145336Z-ma60-above-60plus-short-veto-replay-v1")
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\ma60_above_60plus_short_veto_failure_decomposition_v1")
REQUIRED_INPUTS = (
    "short_veto_rows.csv",
    "short_veto_summary.json",
    "period_stability_summary.csv",
    "regime_stability_summary.csv",
    "source_stability_summary.csv",
    "research_decision.json",
    "no_lookahead_audit.json",
)
REQUIRED_ARTIFACTS = (
    "input_artifact_report.json",
    "period_failure_decomposition.csv",
    "source_failure_decomposition.csv",
    "subtype_failure_decomposition.csv",
    "regime_failure_decomposition.csv",
    "recent_degradation_summary.json",
    "salvageability_summary.json",
    "research_decision.json",
    "_ARTIFACT_COMPLETE.json",
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
    values = pd.to_numeric(df[col], errors="coerce").dropna() if col in df else pd.Series(dtype=float)
    return None if values.empty else float(values.mean())


def _median(df: pd.DataFrame, col: str) -> float | None:
    values = pd.to_numeric(df[col], errors="coerce").dropna() if col in df else pd.Series(dtype=float)
    return None if values.empty else float(values.median())


def _coverage(df: pd.DataFrame, col: str) -> float | None:
    if df.empty or col not in df:
        return None
    return float(pd.to_numeric(df[col], errors="coerce").notna().mean())


def _bool_series(series: pd.Series) -> pd.Series:
    if series.dtype == bool:
        return series.fillna(False)
    normalized = series.astype("string").fillna("").str.strip().str.lower()
    return normalized.isin({"1", "true", "t", "yes", "y"})


def _rate(df: pd.DataFrame, col: str) -> float | None:
    if df.empty or col not in df:
        return None
    values = df[col].dropna()
    return None if values.empty else float(_bool_series(values).mean())


def _bucket_year(year: int) -> str:
    if 2019 <= year <= 2021:
        return "2019-2021"
    if 2022 <= year <= 2023:
        return "2022-2023"
    if 2024 <= year <= 2026:
        return "2024-2026"
    return str(year)


def _decomp(rows: pd.DataFrame, by: str) -> pd.DataFrame:
    out: list[dict[str, Any]] = []
    for key, group in rows.groupby(by, dropna=False):
        guard_hit = _bool_series(group["guard_hit"])
        hit = group[guard_hit]
        miss = group[~guard_hit]
        hit_ret20 = _mean(hit, "ret20_long")
        miss_ret20 = _mean(miss, "ret20_long")
        out.append(
            {
                by: key,
                "n": int(len(group)),
                "n_guard_hit": int(len(hit)),
                "guard_hit_share": None if len(group) == 0 else float(len(hit) / len(group)),
                "ret20_long_mean": _mean(group, "ret20_long"),
                "ret20_long_median": _median(group, "ret20_long"),
                "ret20_long_coverage": _coverage(group, "ret20_long"),
                "ret40_long_mean": _mean(group, "ret40_long"),
                "ret40_long_median": _median(group, "ret40_long"),
                "ret40_long_coverage": _coverage(group, "ret40_long"),
                "short_return20_mean": _mean(group, "short_return20"),
                "short_return20_median": _median(group, "short_return20"),
                "helped_veto_rate": _rate(group, "helped_veto"),
                "harmed_veto_rate": _rate(group, "harmed_veto"),
                "neutral_rate": _rate(group, "neutral_veto"),
                "guard_hit_ret20_long_mean": hit_ret20,
                "guard_hit_ret20_long_median": _median(hit, "ret20_long"),
                "guard_hit_ret20_long_coverage": _coverage(hit, "ret20_long"),
                "guard_hit_ret40_long_mean": _mean(hit, "ret40_long"),
                "guard_hit_ret40_long_coverage": _coverage(hit, "ret40_long"),
                "guard_hit_short_return20_mean": _mean(hit, "short_return20"),
                "guard_hit_helped_veto_rate": _rate(hit, "helped_veto"),
                "guard_hit_harmed_veto_rate": _rate(hit, "harmed_veto"),
                "guard_hit_neutral_rate": _rate(hit, "neutral_veto"),
                "guard_hit_ma20_break_rate": _rate(hit, "ma20_break_within_20d"),
                "guard_hit_ma60_break_rate": _rate(hit, "ma60_break_within_20d"),
                "guard_hit_ma20_and_ma60_break_rate": _rate(hit, "ma20_and_ma60_break_within_20d"),
                "hit_minus_miss_ret20_long": None if hit_ret20 is None or miss_ret20 is None else float(hit_ret20 - miss_ret20),
            }
        )
    return pd.DataFrame(out)


def build_decompositions(rows: pd.DataFrame) -> dict[str, pd.DataFrame]:
    work = rows.copy()
    work["period_bucket_calc"] = work["year"].astype(int).map(_bucket_year)
    work["guard_subtype"] = work["guard_anchor_type"].fillna("guard_miss_or_no_guard").astype(str)
    return {
        "period": _decomp(work, "period_bucket_calc"),
        "source": _decomp(work, "source_type"),
        "subtype": _decomp(work, "guard_subtype"),
        "regime": _decomp(work, "regime_proxy"),
    }


def concentration(hit: pd.DataFrame, col: str) -> dict[str, Any]:
    if hit.empty or col not in hit:
        return {"field": col, "largest_value": None, "largest_share": None}
    counts = hit[col].fillna("unknown").astype(str).value_counts()
    return {"field": col, "largest_value": str(counts.index[0]), "largest_share": float(counts.iloc[0] / len(hit)), "n": int(len(hit))}


def recent_degradation(rows: pd.DataFrame) -> dict[str, Any]:
    hit = rows[_bool_series(rows["guard_hit"])].copy()
    hit["period_bucket_calc"] = hit["year"].astype(int).map(_bucket_year)
    old = hit[hit["period_bucket_calc"].isin(["2019-2021", "2022-2023"])]
    recent = hit[hit["period_bucket_calc"] == "2024-2026"]
    old_ret = _mean(old, "ret20_long")
    recent_ret = _mean(recent, "ret20_long")
    old_help = _rate(old, "helped_veto")
    recent_help = _rate(recent, "helped_veto")
    return {
        "guard_hit_2019_2023_n": int(len(old)),
        "guard_hit_2024_2026_n": int(len(recent)),
        "ret20_long_mean_2019_2023": old_ret,
        "ret20_long_mean_2024_2026": recent_ret,
        "ret20_long_coverage_2019_2023": _coverage(old, "ret20_long"),
        "ret20_long_coverage_2024_2026": _coverage(recent, "ret20_long"),
        "helped_veto_rate_2019_2023": old_help,
        "helped_veto_rate_2024_2026": recent_help,
        "recent_degradation_score_ret20": None if old_ret is None or recent_ret is None else float(recent_ret - old_ret),
        "recent_degradation_score_helped_rate": None if old_help is None or recent_help is None else float(recent_help - old_help),
        "largest_year_share_2024_2026": concentration(recent, "year"),
        "largest_source_share_2024_2026": concentration(recent, "source_type"),
        "largest_subtype_share_2024_2026": concentration(recent, "guard_anchor_type"),
    }


def salvageability(rows: pd.DataFrame) -> dict[str, Any]:
    hit = rows[_bool_series(rows["guard_hit"])].copy()
    recent = hit[hit["year"].astype(int).between(2024, 2026)].copy()
    candidates: list[dict[str, Any]] = []
    for subtype, group in recent.groupby("guard_anchor_type", dropna=False):
        source_conc = concentration(group, "source_type")
        item = {
            "guard_subtype": str(subtype),
            "n": int(len(group)),
            "ret20_long_mean": _mean(group, "ret20_long"),
            "ret20_long_coverage": _coverage(group, "ret20_long"),
            "helped_veto_rate": _rate(group, "helped_veto"),
            "harmed_veto_rate": _rate(group, "harmed_veto"),
            "largest_source_share": source_conc["largest_share"],
        }
        item["salvageable_observation"] = bool(
            item["n"] >= 30
            and item["ret20_long_mean"] is not None
            and item["ret20_long_mean"] > 0
            and item["helped_veto_rate"] is not None
            and item["helped_veto_rate"] >= 0.55
            and item["harmed_veto_rate"] is not None
            and item["harmed_veto_rate"] <= 0.25
            and item["largest_source_share"] is not None
            and item["largest_source_share"] < 0.8
        )
        candidates.append(item)
    useful = [c for c in candidates if c["salvageable_observation"]]
    if useful:
        decision = "salvageable_hold"
        reason = "at least one 2024-2026 guard subtype clears observational salvage gates, but not promotion gates"
    elif not candidates:
        decision = "inconclusive"
        reason = "no 2024-2026 guard-hit subtype rows available"
    else:
        decision = "drop_short_veto"
        reason = "no 2024-2026 guard subtype clears usefulness/source concentration gates"
    return {"subtype_candidates": candidates, "salvageability_decision": decision, "reason_typed": reason}


def research_decision(salvage: dict[str, Any]) -> dict[str, Any]:
    decision = str(salvage["salvageability_decision"])
    return {
        "research_decision": decision,
        "reason_typed": [salvage["reason_typed"]],
        "promotion_allowed": False,
        "meemee_reflectable": False,
        "threshold_sweep": False,
    }


def run(*, input_root: Path = DEFAULT_INPUT_ROOT, output_root: Path = DEFAULT_OUTPUT_ROOT) -> dict[str, Any]:
    run_dir = output_root / f"{_now_tag()}-ma60-above-60plus-short-veto-failure-decomposition-v1"
    run_dir.mkdir(parents=True, exist_ok=True)
    missing = [name for name in REQUIRED_INPUTS if not (input_root / name).exists()]
    if missing:
        raise FileNotFoundError(f"missing required input artifacts: {missing}")
    rows = pd.read_csv(input_root / "short_veto_rows.csv", low_memory=False)
    source_decision = json.loads((input_root / "research_decision.json").read_text(encoding="utf-8"))
    source_audit = json.loads((input_root / "no_lookahead_audit.json").read_text(encoding="utf-8"))
    decomps = build_decompositions(rows)
    recent = recent_degradation(rows)
    salvage = salvageability(rows)
    decision = research_decision(salvage)
    decomps["period"].to_csv(run_dir / "period_failure_decomposition.csv", index=False)
    decomps["source"].to_csv(run_dir / "source_failure_decomposition.csv", index=False)
    decomps["subtype"].to_csv(run_dir / "subtype_failure_decomposition.csv", index=False)
    decomps["regime"].to_csv(run_dir / "regime_failure_decomposition.csv", index=False)
    _write_json(run_dir / "input_artifact_report.json", {"input_root": input_root, "required_inputs": list(REQUIRED_INPUTS), "source_research_decision": source_decision, "source_no_lookahead_audit": source_audit.get("audit_result"), "rows_loaded": int(len(rows)), "guard_hit_rows": int(_bool_series(rows["guard_hit"]).sum())})
    _write_json(run_dir / "recent_degradation_summary.json", recent)
    _write_json(run_dir / "salvageability_summary.json", salvage)
    _write_json(run_dir / "research_decision.json", decision)
    complete = {"axis_id": AXIS_ID, "output_dir": run_dir, "required_artifacts": list(REQUIRED_ARTIFACTS), "artifact_complete": all((run_dir / name).exists() for name in REQUIRED_ARTIFACTS if name != "_ARTIFACT_COMPLETE.json"), "created_at_utc": datetime.now(timezone.utc).isoformat()}
    _write_json(run_dir / "_ARTIFACT_COMPLETE.json", complete)
    return {"output_dir": str(run_dir), "research_decision": decision, "recent_degradation": recent, "salvageability": salvage}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Decompose recent failure of MA60 60plus short veto")
    parser.add_argument("--input-root", type=Path, default=DEFAULT_INPUT_ROOT)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    args = parser.parse_args(argv)
    print(json.dumps(_json_ready(run(input_root=args.input_root, output_root=args.output_root)), ensure_ascii=False, sort_keys=True, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
