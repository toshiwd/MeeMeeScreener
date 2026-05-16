from __future__ import annotations

import argparse
import hashlib
import json
import math
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

import duckdb
import pandas as pd

if __package__ in {None, ""}:
    sys.path.append(str(Path(__file__).resolve().parents[1]))

from app.backend.services import tradex_research_contracts as contracts
from scripts import tradex_pre_strength_guard_validation_v1 as guard_mod


AXIS_ID = "upside_capture_missed_winner_diagnosis_v1"
SCHEMA_PREFIX = "tradex_upside_capture_missed_winner_diagnosis_v1"
DEFAULT_PATTERN_ROOT = Path(r"G:\Tradex\pre_strength_pattern_mining_v1")
DEFAULT_GUARD_ROOT = Path(r"G:\Tradex\pre_strength_guard_validation_v1")
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\upside_capture_missed_winner_diagnosis_v1")
DEFAULT_PATTERN_RUN_ID = "20260513T000000Z-pre-strength-pattern-mining-v1"
DEFAULT_GUARD_RUN_ID = "20260513T010000Z-pre-strength-guard-validation-v1"

TOPK_REQUIRED_COVERAGE = 0.80
OPPORTUNITY_TOP10_AVG_RET20_THRESHOLD = 0.05

CANDIDATE_SET_COLUMNS = {
    "all_strength_baseline": "candidate_all_strength_baseline",
    "safe_full": "guard_safe_full",
    "safe_without_weekly_mixed": "guard_safe_without_weekly_mixed",
    "ma20_reclaim_only": "guard_ma20_reclaim_only",
    "flat_only": "guard_flat_only",
    "monthly_uptrend_only": "guard_monthly_uptrend_only",
    "extended_veto_only": "guard_extended_veto_only",
    "safe_full_plus_extended_veto": "guard_safe_full_plus_extended_veto",
    "negative_guard_matched": "negative_guard_match",
}

LABEL_COLUMNS = {
    "ret20_fwd",
    "mfe20",
    "mae20",
    "ret20_rank_pct_by_date",
    "MFE20_rank_pct_by_date",
    "is_future_top15_by_ret20",
    "is_future_top10_by_ret20",
    "is_future_top5_by_ret20",
    "is_big_winner_ret20_ge_5pct",
    "is_big_winner_ret20_ge_10pct",
    "is_big_winner_MFE20_ge_10pct",
    "is_big_winner_MFE20_ge_15pct",
}
GUARD_KEY_COLUMNS = guard_mod.GUARD_KEY_COLUMNS

REQUIRED_ARTIFACTS = (
    "evaluation_contract.json",
    "run_manifest.json",
    "source_artifact_refs.json",
    "opportunity_label_contract.json",
    "opportunity_day_ledger.jsonl",
    "candidate_set_capture_report.json",
    "missed_winner_report.json",
    "negative_guard_veto_report.json",
    "safe_full_opportunity_loss_report.json",
    "oracle_gap_report.json",
    "topk_selection_diagnostics.json",
    "ranking_coverage_audit.json",
    "research_decision.json",
    "_ARTIFACT_COMPLETE.json",
)


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _default_run_id() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + f"-{AXIS_ID}"


def _json_ready(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _json_ready(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_json_ready(item) for item in value]
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
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


def _json_text(payload: Any) -> str:
    return json.dumps(_json_ready(payload), ensure_ascii=False, sort_keys=True, default=str)


def _write_json(path: Path, payload: dict[str, Any]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(_json_text(payload) + "\n", encoding="utf-8")
    return path


def _write_jsonl(path: Path, rows: Iterable[dict[str, Any]]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("".join(_json_text(row) + "\n" for row in rows), encoding="utf-8")
    return path


def _stable_hash(payload: Any) -> str:
    return hashlib.sha256(_json_text(payload).encode("utf-8")).hexdigest()


def _safe_path(value: str | Path | None, default: Path) -> Path:
    if value is None or not str(value).strip():
        return default.resolve()
    return Path(str(value)).expanduser().resolve()


def _safe_rate(count: int | float, total: int | float) -> float:
    if not total:
        return 0.0
    return float(count) / float(total)


def _load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _run_dir(root: str | Path, run_id: str, default_root: Path) -> Path:
    return _safe_path(root, default_root) / run_id


def load_inputs(pattern_dir: Path, guard_dir: Path) -> dict[str, Any]:
    pattern = guard_mod.load_source_artifacts(pattern_dir)
    guard_required = [
        "_ARTIFACT_COMPLETE.json",
        "evaluation_contract.json",
        "source_artifact_refs.json",
        "positive_guard_report.json",
        "negative_guard_report.json",
        "topk_rotation_proxy_metrics.json",
        "research_decision.json",
    ]
    missing = [name for name in guard_required if not (guard_dir / name).exists()]
    if missing:
        raise FileNotFoundError(f"guard run missing required artifacts: {missing} at {guard_dir}")
    guard_json = {name: _load_json(guard_dir / name) for name in guard_required}
    complete = guard_json["_ARTIFACT_COMPLETE.json"]
    decision = guard_json["research_decision.json"]
    if complete.get("complete") is not True:
        raise RuntimeError("guard source artifact is not complete")
    if complete.get("silent_fallback_used") is not False or decision.get("silent_fallback_used") is not False:
        raise RuntimeError("guard source artifact used silent fallback")
    events = guard_mod.add_guard_flags(pattern["events"])
    events["candidate_all_strength_baseline"] = True
    return {"pattern_dir": pattern_dir, "guard_dir": guard_dir, "pattern": pattern, "guard_json": guard_json, "events": add_opportunity_labels(events)}


def add_opportunity_labels(events: pd.DataFrame) -> pd.DataFrame:
    frame = events.copy()
    group = frame.groupby("event_date", sort=False)
    frame["same_day_event_count"] = group["code"].transform("count")
    frame["ret20_rank_by_date"] = group["ret20_fwd"].rank(method="first", ascending=False)
    frame["MFE20_rank_by_date"] = group["mfe20"].rank(method="first", ascending=False)
    frame["ret20_rank_pct_by_date"] = frame["ret20_rank_by_date"] / frame["same_day_event_count"]
    frame["MFE20_rank_pct_by_date"] = frame["MFE20_rank_by_date"] / frame["same_day_event_count"]
    top15_cutoff = frame["same_day_event_count"].map(lambda value: max(1, int(math.ceil(float(value) * 0.15))))
    top10_cutoff = frame["same_day_event_count"].map(lambda value: max(1, int(math.ceil(float(value) * 0.10))))
    top5_cutoff = frame["same_day_event_count"].map(lambda value: max(1, int(math.ceil(float(value) * 0.05))))
    frame["is_future_top15_by_ret20"] = frame["ret20_rank_by_date"].le(top15_cutoff)
    frame["is_future_top10_by_ret20"] = frame["ret20_rank_by_date"].le(top10_cutoff)
    frame["is_future_top5_by_ret20"] = frame["ret20_rank_by_date"].le(top5_cutoff)
    frame["is_big_winner_ret20_ge_5pct"] = frame["ret20_fwd"].ge(0.05)
    frame["is_big_winner_ret20_ge_10pct"] = frame["ret20_fwd"].ge(0.10)
    frame["is_big_winner_MFE20_ge_10pct"] = frame["mfe20"].ge(0.10)
    frame["is_big_winner_MFE20_ge_15pct"] = frame["mfe20"].ge(0.15)
    top10_avg = group["ret20_fwd"].transform(lambda s: float(s.sort_values(ascending=False).head(min(10, len(s))).mean()))
    median_ret = group["ret20_fwd"].transform("median")
    frame["day_top10_ret20_avg"] = top10_avg
    frame["day_median_ret20"] = median_ret
    frame["opportunity_day_top15"] = group["is_future_top15_by_ret20"].transform("any")
    frame["opportunity_day_big_ret20"] = group["is_big_winner_ret20_ge_10pct"].transform("any")
    frame["opportunity_day_big_MFE"] = group["is_big_winner_MFE20_ge_15pct"].transform("any")
    frame["broad_rising_environment"] = median_ret.gt(0.0) | top10_avg.ge(OPPORTUNITY_TOP10_AVG_RET20_THRESHOLD)
    return frame


def _topn_mean(values: pd.Series, n: int) -> float | None:
    numeric = pd.to_numeric(values, errors="coerce").dropna()
    if numeric.empty:
        return None
    return float(numeric.sort_values(ascending=False).head(min(n, len(numeric))).mean())


def _day_oracle_stats(events: pd.DataFrame, mask: pd.Series, *, opportunity_days: set[str]) -> dict[str, Any]:
    rows = []
    candidate = events.loc[mask].copy()
    for event_date, day in events.groupby("event_date", sort=True):
        if event_date not in opportunity_days:
            continue
        cand = candidate[candidate["event_date"].eq(event_date)]
        best_available_ret = float(day["ret20_fwd"].max())
        best_available_mfe = float(day["mfe20"].max())
        all_top3_ret = _topn_mean(day["ret20_fwd"], 3)
        cand_best_ret = float(cand["ret20_fwd"].max()) if not cand.empty else None
        cand_best_mfe = float(cand["mfe20"].max()) if not cand.empty else None
        cand_top3_ret = _topn_mean(cand["ret20_fwd"], 3) if not cand.empty else None
        rows.append(
            {
                "event_date": event_date,
                "candidate_count": int(len(cand)),
                "best_available_ret20": best_available_ret,
                "best_candidate_ret20": cand_best_ret,
                "best_available_MFE20": best_available_mfe,
                "best_candidate_MFE20": cand_best_mfe,
                "all_strength_oracle_top3_ret20": all_top3_ret,
                "candidate_set_oracle_top3_ret20": cand_top3_ret,
            }
        )
    if not rows:
        return {
            "avg_best_available_ret20_by_day": None,
            "avg_best_candidate_ret20_by_day": None,
            "avg_candidate_set_oracle_top3_ret20": None,
            "oracle_top3_gap_vs_all_strength": None,
            "MFE20_capture_ratio": None,
            "ret20_capture_ratio": None,
        }
    frame = pd.DataFrame(rows)
    available_ret = float(frame["best_available_ret20"].mean())
    candidate_ret = float(frame["best_candidate_ret20"].dropna().mean()) if frame["best_candidate_ret20"].notna().any() else None
    available_mfe = float(frame["best_available_MFE20"].mean())
    candidate_mfe = float(frame["best_candidate_MFE20"].dropna().mean()) if frame["best_candidate_MFE20"].notna().any() else None
    all_top3 = float(frame["all_strength_oracle_top3_ret20"].dropna().mean()) if frame["all_strength_oracle_top3_ret20"].notna().any() else None
    cand_top3 = float(frame["candidate_set_oracle_top3_ret20"].dropna().mean()) if frame["candidate_set_oracle_top3_ret20"].notna().any() else None
    return {
        "avg_best_available_ret20_by_day": available_ret,
        "avg_best_candidate_ret20_by_day": candidate_ret,
        "avg_candidate_set_oracle_top3_ret20": cand_top3,
        "oracle_top3_gap_vs_all_strength": (cand_top3 - all_top3) if cand_top3 is not None and all_top3 is not None else None,
        "oracle_top3_gap_vs_full_universe_if_available": None,
        "MFE20_capture_ratio": (candidate_mfe / available_mfe) if candidate_mfe is not None and available_mfe else None,
        "ret20_capture_ratio": (candidate_ret / available_ret) if candidate_ret is not None and available_ret else None,
        "day_rows": rows,
    }


def _recall(events: pd.DataFrame, mask: pd.Series, column: str) -> tuple[int, int, float]:
    total = int(events[column].sum())
    captured = int((mask & events[column]).sum())
    return captured, total, _safe_rate(captured, total)


def build_candidate_set_capture_report(events: pd.DataFrame) -> dict[str, Any]:
    opportunity_days = set(events.loc[events["opportunity_day_top15"], "event_date"].unique().tolist())
    rows = []
    day_rows_by_set = {}
    for set_id, column in CANDIDATE_SET_COLUMNS.items():
        mask = events[column].astype(bool)
        candidate = events.loc[mask].copy()
        candidate_days = set(candidate["event_date"].unique().tolist())
        top15_c, top15_t, top15_r = _recall(events, mask, "is_future_top15_by_ret20")
        top10_c, top10_t, top10_r = _recall(events, mask, "is_future_top10_by_ret20")
        top5_c, top5_t, top5_r = _recall(events, mask, "is_future_top5_by_ret20")
        big10_c, big10_t, big10_r = _recall(events, mask, "is_big_winner_ret20_ge_10pct")
        mfe15_c, mfe15_t, mfe15_r = _recall(events, mask, "is_big_winner_MFE20_ge_15pct")
        oracle = _day_oracle_stats(events, mask, opportunity_days=opportunity_days)
        day_rows_by_set[set_id] = oracle.pop("day_rows", [])
        row = {
            "candidate_set_id": set_id,
            "n": int(mask.sum()),
            "candidate_days_count": len(candidate_days),
            "opportunity_days_total": len(opportunity_days),
            "no_candidate_on_opportunity_day_rate": _safe_rate(len(opportunity_days - candidate_days), len(opportunity_days)),
            "future_top15_recall_by_candidate_set": top15_r,
            "future_top10_recall_by_candidate_set": top10_r,
            "future_top5_recall_by_candidate_set": top5_r,
            "big_winner_ret20_ge_10_capture_rate": big10_r,
            "big_winner_MFE20_ge_15_capture_rate": mfe15_r,
            "missed_future_top15_count": top15_t - top15_c,
            "missed_future_top10_count": top10_t - top10_c,
            "missed_future_top5_count": top5_t - top5_c,
            "missed_big_winner_ret20_ge_10_count": big10_t - big10_c,
            "missed_big_winner_MFE20_ge_15_count": mfe15_t - mfe15_c,
            **{key: value for key, value in oracle.items() if key != "day_rows"},
        }
        rows.append(row)
    return {
        "schema_version": f"{SCHEMA_PREFIX}_candidate_set_capture_report_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "opportunity_day_definition": "opportunity_day_top15 over same-date all_strength event universe",
        "rows": rows,
        "oracle_day_rows_by_candidate_set": {key: value[:300] for key, value in day_rows_by_set.items()},
    }


def build_opportunity_day_ledger(events: pd.DataFrame) -> list[dict[str, Any]]:
    rows = []
    for event_date, group in events.groupby("event_date", sort=True):
        row: dict[str, Any] = {
            "event_date": event_date,
            "event_count": int(len(group)),
            "median_ret20": float(group["ret20_fwd"].median()),
            "top10_ret20_avg": _topn_mean(group["ret20_fwd"], 10),
            "best_ret20": float(group["ret20_fwd"].max()),
            "best_MFE20": float(group["mfe20"].max()),
            "opportunity_day_top15": bool(group["opportunity_day_top15"].any()),
            "opportunity_day_big_ret20": bool(group["opportunity_day_big_ret20"].any()),
            "opportunity_day_big_MFE": bool(group["opportunity_day_big_MFE"].any()),
            "broad_rising_environment": bool(group["broad_rising_environment"].any()),
        }
        for set_id, column in CANDIDATE_SET_COLUMNS.items():
            candidate = group[group[column].astype(bool)]
            row[f"{set_id}_candidate_count"] = int(len(candidate))
            row[f"{set_id}_best_ret20"] = float(candidate["ret20_fwd"].max()) if not candidate.empty else None
        rows.append(row)
    return rows


def build_missed_winner_report(events: pd.DataFrame, capture: dict[str, Any]) -> dict[str, Any]:
    rows = []
    for set_id, column in CANDIDATE_SET_COLUMNS.items():
        mask = events[column].astype(bool)
        no_candidate_dates = set(events.loc[events["opportunity_day_top15"], "event_date"].unique()) - set(events.loc[mask, "event_date"].unique())
        rows.append(
            {
                "candidate_set_id": set_id,
                "no_candidate_miss_days": len(no_candidate_dates),
                "guard_filter_miss_future_top10_count": int((~mask & events["is_future_top10_by_ret20"]).sum()),
                "guard_filter_miss_big_ret20_ge_10_count": int((~mask & events["is_big_winner_ret20_ge_10pct"]).sum()),
                "guard_filter_miss_big_MFE20_ge_15_count": int((~mask & events["is_big_winner_MFE20_ge_15pct"]).sum()),
                "selection_miss_available": False,
                "selection_miss_reason": "complete same-period ranking unavailable",
                "timing_miss_count": int((mask & events["is_big_winner_MFE20_ge_15pct"] & ~events["is_big_winner_ret20_ge_5pct"]).sum()),
            }
        )
    return {
        "schema_version": f"{SCHEMA_PREFIX}_missed_winner_report_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "miss_categories": ["no_candidate_miss", "guard_filter_miss", "selection_miss_unavailable_without_complete_ranking", "timing_miss"],
        "rows": rows,
    }


def build_negative_guard_veto_report(events: pd.DataFrame) -> dict[str, Any]:
    veto = events["negative_guard_match"].astype(bool)
    return {
        "schema_version": f"{SCHEMA_PREFIX}_negative_guard_veto_report_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "negative_guard_vetoed_event_count": int(veto.sum()),
        "negative_guard_vetoed_future_top10_count": int((veto & events["is_future_top10_by_ret20"]).sum()),
        "negative_guard_vetoed_future_top5_count": int((veto & events["is_future_top5_by_ret20"]).sum()),
        "negative_guard_vetoed_big_winner_count": int((veto & events["is_big_winner_ret20_ge_10pct"]).sum()),
        "negative_guard_vetoed_big_MFE20_ge_15_count": int((veto & events["is_big_winner_MFE20_ge_15pct"]).sum()),
        "negative_guard_vetoed_future_top10_rate": _safe_rate(int((veto & events["is_future_top10_by_ret20"]).sum()), int(events["is_future_top10_by_ret20"].sum())),
        "negative_guard_vetoed_big_winner_rate": _safe_rate(int((veto & events["is_big_winner_ret20_ge_10pct"]).sum()), int(events["is_big_winner_ret20_ge_10pct"].sum())),
        "negative_guard_bad_pick_rate": float(events.loc[veto, "bad_pick20"].mean()) if veto.any() else 0.0,
        "negative_guard_avg_ret20": float(events.loc[veto, "ret20_fwd"].mean()) if veto.any() else 0.0,
    }


def build_safe_full_opportunity_loss_report(events: pd.DataFrame, capture_rows: list[dict[str, Any]]) -> dict[str, Any]:
    row_by_id = {row["candidate_set_id"]: row for row in capture_rows}
    safe = row_by_id["safe_full"]
    safe_veto = row_by_id["safe_full_plus_extended_veto"]
    baseline = row_by_id["all_strength_baseline"]
    return {
        "schema_version": f"{SCHEMA_PREFIX}_safe_full_opportunity_loss_report_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "safe_full_missed_big_winner_count": safe["missed_big_winner_ret20_ge_10_count"],
        "safe_full_missed_future_top10_count": safe["missed_future_top10_count"],
        "safe_full_no_candidate_on_opportunity_day_rate": safe["no_candidate_on_opportunity_day_rate"],
        "safe_full_future_top10_recall": safe["future_top10_recall_by_candidate_set"],
        "safe_full_big_winner_ret20_ge_10_capture_rate": safe["big_winner_ret20_ge_10_capture_rate"],
        "safe_full_oracle_top3_gap_vs_all_strength": safe["oracle_top3_gap_vs_all_strength"],
        "safe_full_plus_extended_veto_oracle_top3_gap_vs_all_strength": safe_veto["oracle_top3_gap_vs_all_strength"],
        "all_strength_avg_candidate_set_oracle_top3_ret20": baseline["avg_candidate_set_oracle_top3_ret20"],
        "interpretation": "safe_full is evaluated as a hard-filter risk; this artifact does not promote it as production scoring",
    }


def build_oracle_gap_report(capture_rows: list[dict[str, Any]]) -> dict[str, Any]:
    rows = []
    for row in capture_rows:
        rows.append(
            {
                "candidate_set_id": row["candidate_set_id"],
                "avg_best_available_ret20_by_day": row["avg_best_available_ret20_by_day"],
                "avg_best_candidate_ret20_by_day": row["avg_best_candidate_ret20_by_day"],
                "avg_candidate_set_oracle_top3_ret20": row["avg_candidate_set_oracle_top3_ret20"],
                "oracle_top3_gap_vs_all_strength": row["oracle_top3_gap_vs_all_strength"],
                "MFE20_capture_ratio": row["MFE20_capture_ratio"],
                "ret20_capture_ratio": row["ret20_capture_ratio"],
            }
        )
    return {
        "schema_version": f"{SCHEMA_PREFIX}_oracle_gap_report_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "rows": rows,
    }


def _source_db_from_pattern(pattern: dict[str, Any]) -> Path | None:
    source_db = pattern["json"].get("evaluation_contract.json", {}).get("source_db")
    if not source_db:
        return None
    path = Path(str(source_db))
    return path if path.exists() else None


def build_ranking_coverage_audit(events: pd.DataFrame, pattern: dict[str, Any]) -> dict[str, Any]:
    source_db = _source_db_from_pattern(pattern)
    payload: dict[str, Any] = {
        "schema_version": f"{SCHEMA_PREFIX}_ranking_coverage_audit_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "complete_topk_ranking_available": False,
        "topk_required_coverage": TOPK_REQUIRED_COVERAGE,
        "ranking_table_checked": False,
        "reason": "source_db_missing_or_not_recorded",
    }
    if source_db is None:
        return payload
    try:
        with duckdb.connect(str(source_db), read_only=True) as conn:
            tables = {row[0] for row in conn.execute("SHOW TABLES").fetchall()}
            if "ranking_appearance_daily" not in tables:
                payload["reason"] = "ranking_appearance_daily_table_missing"
                payload["ranking_table_checked"] = True
                return payload
            ranking = conn.execute(
                """
                SELECT CAST(dt AS INTEGER) AS event_ymd, CAST(code AS VARCHAR) AS code, rank, display_score
                FROM ranking_appearance_daily
                WHERE dir = 'up' AND dt IS NOT NULL AND code IS NOT NULL
                """
            ).fetchdf()
    except Exception as exc:  # pragma: no cover
        payload["reason"] = f"ranking_table_read_failed: {type(exc).__name__}"
        payload["ranking_table_checked"] = True
        return payload
    payload["ranking_table_checked"] = True
    payload["ranking_rows"] = int(len(ranking))
    if ranking.empty:
        payload["reason"] = "ranking_appearance_daily_empty"
        return payload
    joined = events.merge(ranking, on=["event_ymd", "code"], how="left")
    coverage = float(joined["display_score"].notna().mean()) if len(joined) else 0.0
    payload.update(
        {
            "event_count": int(len(events)),
            "ranking_score_covered_count": int(joined["display_score"].notna().sum()),
            "ranking_score_coverage_rate": coverage,
            "event_ymd_min": int(events["event_ymd"].min()),
            "event_ymd_max": int(events["event_ymd"].max()),
            "ranking_dt_min": int(ranking["event_ymd"].min()),
            "ranking_dt_max": int(ranking["event_ymd"].max()),
            "complete_topk_ranking_available": coverage >= TOPK_REQUIRED_COVERAGE,
            "reason": "complete_same_period_ranking_available" if coverage >= TOPK_REQUIRED_COVERAGE else "ranking_score_coverage_below_same_period_threshold",
        }
    )
    return payload


def build_topk_selection_diagnostics(ranking_audit: dict[str, Any]) -> dict[str, Any]:
    available = bool(ranking_audit.get("complete_topk_ranking_available") is True)
    payload: dict[str, Any] = {
        "schema_version": f"{SCHEMA_PREFIX}_topk_selection_diagnostics_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "complete_topk_ranking_available": available,
        "selection_miss_available": available,
        "reason": ranking_audit.get("reason"),
    }
    if not available:
        payload.update(
            {
                "selected_top1_ret20": None,
                "selected_top3_avg_ret20": None,
                "selected_top3_future_top15_precision": None,
                "selected_top3_future_top10_precision": None,
                "selected_nonwinner_when_winner_available_rate": None,
                "selected_top3_oracle_regret": None,
                "selected_top3_MFE20_capture_ratio": None,
                "top3_changed_members_count": None,
                "overlap_with_champion_top3": None,
            }
        )
    return payload


def build_opportunity_label_contract() -> dict[str, Any]:
    payload = {
        "schema_version": f"{SCHEMA_PREFIX}_opportunity_label_contract_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "future_labels_used_for_evaluation_only": True,
        "future_labels_used_in_guard_key": False,
        "guard_key_columns": sorted(GUARD_KEY_COLUMNS),
        "future_label_columns": sorted(LABEL_COLUMNS),
        "opportunity_day_labels": {
            "opportunity_day_top15": "at least one same-date event is future top15 by ret20",
            "opportunity_day_big_ret20": "at least one same-date event has ret20 >= +10%",
            "opportunity_day_big_MFE": "at least one same-date event has MFE20 >= +15%",
            "broad_rising_environment": f"same-date median_ret20 > 0 or top10_ret20_avg >= {OPPORTUNITY_TOP10_AVG_RET20_THRESHOLD}",
        },
    }
    payload["contract_hash"] = _stable_hash(payload)
    return payload


def build_source_artifact_refs(pattern_dir: Path, guard_dir: Path) -> dict[str, Any]:
    refs = []
    for root_name, root, names in [
        ("pattern", pattern_dir, ["_ARTIFACT_COMPLETE.json", "evaluation_contract.json", "run_manifest.json", "feature_availability_audit.json", "pre_strength_event_ledger.jsonl", "research_decision.json"]),
        ("guard", guard_dir, ["_ARTIFACT_COMPLETE.json", "evaluation_contract.json", "source_artifact_refs.json", "research_decision.json", "topk_rotation_proxy_metrics.json"]),
    ]:
        for name in names:
            path = root / name
            item: dict[str, Any] = {"source": root_name, "name": name, "path": str(path), "exists": path.exists()}
            if path.exists() and path.suffix == ".json":
                item["content_hash"] = _stable_hash(_load_json(path))
            refs.append(item)
    return {
        "schema_version": f"{SCHEMA_PREFIX}_source_artifact_refs_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "pattern_artifact_root": str(pattern_dir),
        "guard_artifact_root": str(guard_dir),
        "refs": refs,
    }


def build_evaluation_contract(pattern_dir: Path, guard_dir: Path, events: pd.DataFrame, label_contract: dict[str, Any]) -> dict[str, Any]:
    payload = {
        "schema_version": f"{SCHEMA_PREFIX}_evaluation_contract_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "research_phase": "upside_capture_missed_winner_diagnosis",
        "boundary": "TRADEX-only",
        "axis_moved": "upside_capture_missed_winner_diagnosis",
        "pattern_artifact_root": str(pattern_dir),
        "guard_artifact_root": str(guard_dir),
        "period": {"start_date": str(int(events["event_ymd"].min())), "end_date": str(int(events["event_ymd"].max()))},
        "event_count": int(len(events)),
        "candidate_sets": sorted(CANDIDATE_SET_COLUMNS),
        "cost_slippage_evaluated": False,
        "cost_slippage_ignored_by_user_intent": True,
        "same_condition_controls": {
            "same_universe": True,
            "same_period": True,
            "same_top_k": "diagnostic only; topK selection only if complete same-period ranking is available",
            "same_artifact_detail_level": contracts.TRADEX_ARTIFACT_DETAIL_LEVEL_AUTHORITATIVE,
        },
        "opportunity_label_contract_hash": label_contract["contract_hash"],
        "candidate_scoring_created": False,
        "publish_bundle_created": False,
        "meemee_reflectable": False,
        "silent_fallback_used": False,
    }
    payload["contract_hash"] = _stable_hash(payload)
    return payload


def _reason(code: str, status: str, value: Any, threshold: Any | None = None) -> dict[str, Any]:
    row = {"code": code, "status": status, "value": value}
    if threshold is not None:
        row["threshold"] = threshold
    return row


def build_research_decision(
    *,
    guard_decision: str,
    capture_rows: list[dict[str, Any]],
    negative_report: dict[str, Any],
    ranking_audit: dict[str, Any],
    artifact_complete: bool,
) -> dict[str, Any]:
    by_id = {row["candidate_set_id"]: row for row in capture_rows}
    safe = by_id["safe_full"]
    safe_veto = by_id["safe_full_plus_extended_veto"]
    negative_top10_veto_rate = negative_report["negative_guard_vetoed_future_top10_rate"]
    negative_big_veto_rate = negative_report["negative_guard_vetoed_big_winner_rate"]
    complete_topk = bool(ranking_audit.get("complete_topk_ranking_available") is True)
    safe_too_narrow = safe["future_top10_recall_by_candidate_set"] < 0.15 or safe["no_candidate_on_opportunity_day_rate"] > 0.70
    negative_veto_too_many = negative_top10_veto_rate > 0.45 or negative_big_veto_rate > 0.45
    oracle_gap_bad = safe["oracle_top3_gap_vs_all_strength"] is not None and safe["oracle_top3_gap_vs_all_strength"] < -0.02
    promising = (
        safe["future_top10_recall_by_candidate_set"] >= 0.20
        and safe["big_winner_ret20_ge_10_capture_rate"] >= 0.20
        and not negative_veto_too_many
        and artifact_complete
    )
    if safe_too_narrow or negative_veto_too_many or oracle_gap_bad or not artifact_complete:
        decision = "drop"
        authoritative = "upside_capture_failed"
    elif promising and complete_topk:
        decision = "keep_candidate"
        authoritative = "upside_capture_promising"
    else:
        decision = "hold"
        authoritative = "upside_capture_hold"
    typed_reasons = []
    typed_reasons.append("safe_full_too_narrow_for_hard_filter" if safe_too_narrow else "safe_full_winner_recall_not_too_narrow")
    typed_reasons.append("negative_guard_vetoes_too_many_future_winners" if negative_veto_too_many else "negative_guard_winner_veto_acceptable")
    if not complete_topk:
        typed_reasons.append("complete_topk_ranking_unavailable")
    if oracle_gap_bad:
        typed_reasons.append("safe_full_oracle_gap_materially_worse")
    if decision == "drop":
        typed_reasons.append("do_not_promote_safe_full_as_hard_filter")
    reasons = [
        _reason("safe_full_future_top10_recall", "pass" if safe["future_top10_recall_by_candidate_set"] >= 0.15 else "fail", safe["future_top10_recall_by_candidate_set"], ">=0.15"),
        _reason("safe_full_no_candidate_on_opportunity_day_rate", "pass" if safe["no_candidate_on_opportunity_day_rate"] <= 0.70 else "fail", safe["no_candidate_on_opportunity_day_rate"], "<=0.70"),
        _reason("safe_full_big_winner_ret20_ge_10_capture_rate", "pass" if safe["big_winner_ret20_ge_10_capture_rate"] >= 0.15 else "fail", safe["big_winner_ret20_ge_10_capture_rate"], ">=0.15"),
        _reason("negative_guard_vetoed_future_top10_rate", "pass" if negative_top10_veto_rate <= 0.45 else "fail", negative_top10_veto_rate, "<=0.45"),
        _reason("negative_guard_vetoed_big_winner_rate", "pass" if negative_big_veto_rate <= 0.45 else "fail", negative_big_veto_rate, "<=0.45"),
        _reason("safe_full_oracle_top3_gap_vs_all_strength", "pass" if not oracle_gap_bad else "fail", safe["oracle_top3_gap_vs_all_strength"], ">=-0.02"),
        _reason("complete_topk_ranking_available", "pass" if complete_topk else "hold_blocker", complete_topk),
        _reason("artifact_complete", "pass" if artifact_complete else "fail", artifact_complete),
        _reason("future_labels_used_in_guard_key", "pass", False),
        _reason("silent_fallback_used", "pass", False),
    ]
    return {
        "schema_version": f"{SCHEMA_PREFIX}_research_decision_v1",
        "generated_at": _utc_now(),
        "research_phase": "upside_capture_missed_winner_diagnosis",
        "boundary": "TRADEX-only",
        "axis_moved": "upside_capture_missed_winner_diagnosis",
        "source_guard_decision": guard_decision,
        "decision": decision,
        "authoritative_research_decision": authoritative,
        "cost_slippage_evaluated": False,
        "cost_slippage_ignored_by_user_intent": True,
        "candidate_scoring_created": False,
        "publish_bundle_created": False,
        "meemee_reflectable": False,
        "silent_fallback_used": False,
        "future_labels_used_for_evaluation_only": True,
        "future_labels_used_in_guard_key": False,
        "complete_topk_ranking_available": complete_topk,
        "typed_reasons": typed_reasons,
        "decision_reasons": reasons,
        "safe_full_summary": safe,
        "safe_full_plus_extended_veto_summary": safe_veto,
        "negative_guard_summary": negative_report,
    }


def _artifact_complete(output_dir: Path, paths: dict[str, str], decision: dict[str, Any] | None = None) -> dict[str, Any]:
    excluded = {"_ARTIFACT_COMPLETE.json"}
    if decision is None:
        excluded.add("research_decision.json")
    required_existing = {name: (output_dir / name).exists() for name in REQUIRED_ARTIFACTS if name not in excluded}
    return {
        "schema_version": f"{SCHEMA_PREFIX}_artifact_complete_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "artifact_root": str(output_dir),
        "complete": all(required_existing.values()),
        "required_artifacts": required_existing,
        "paths": paths,
        "decision": decision.get("decision") if decision else None,
        "authoritative_research_decision": decision.get("authoritative_research_decision") if decision else None,
        "silent_fallback_used": False,
        "candidate_scoring_created": False,
        "publish_bundle_created": False,
        "meemee_reflectable": False,
    }


def run_upside_capture_missed_winner_diagnosis_v1(
    *,
    source_pattern_run_id: str = DEFAULT_PATTERN_RUN_ID,
    source_guard_run_id: str = DEFAULT_GUARD_RUN_ID,
    pattern_root: str | Path = DEFAULT_PATTERN_ROOT,
    guard_root: str | Path = DEFAULT_GUARD_ROOT,
    output_root: str | Path = DEFAULT_OUTPUT_ROOT,
    run_id: str | None = None,
) -> dict[str, Any]:
    pattern_dir = _run_dir(pattern_root, source_pattern_run_id, DEFAULT_PATTERN_ROOT)
    guard_dir = _run_dir(guard_root, source_guard_run_id, DEFAULT_GUARD_ROOT)
    inputs = load_inputs(pattern_dir, guard_dir)
    events = inputs["events"]
    output_dir = _safe_path(output_root, DEFAULT_OUTPUT_ROOT) / (run_id.strip() if isinstance(run_id, str) and run_id.strip() else _default_run_id())
    label_contract = build_opportunity_label_contract()
    evaluation_contract = build_evaluation_contract(pattern_dir, guard_dir, events, label_contract)
    source_refs = build_source_artifact_refs(pattern_dir, guard_dir)
    opportunity_rows = build_opportunity_day_ledger(events)
    capture_report = build_candidate_set_capture_report(events)
    capture_rows = capture_report["rows"]
    missed_report = build_missed_winner_report(events, capture_report)
    negative_report = build_negative_guard_veto_report(events)
    safe_loss_report = build_safe_full_opportunity_loss_report(events, capture_rows)
    oracle_gap_report = build_oracle_gap_report(capture_rows)
    ranking_audit = build_ranking_coverage_audit(events, inputs["pattern"])
    topk_diag = build_topk_selection_diagnostics(ranking_audit)
    run_manifest = contracts.build_run_manifest(
        session_id=output_dir.name,
        seed=0,
        random_seed=0,
        input_artifacts=[
            {"name": "source_pattern_artifact_root", "path": str(pattern_dir)},
            {"name": "source_guard_artifact_root", "path": str(guard_dir)},
            {"name": "evaluation_contract", "contract_hash": evaluation_contract["contract_hash"]},
        ],
        asof=str(int(events["event_ymd"].max())),
        config={
            "axis_id": AXIS_ID,
            "source_pattern_run_id": source_pattern_run_id,
            "source_guard_run_id": source_guard_run_id,
            "candidate_scoring_created": False,
            "cost_slippage_evaluated": False,
            "cost_slippage_ignored_by_user_intent": True,
        },
        universe=sorted(events["code"].astype(str).unique().tolist()),
        period={"start_date": str(int(events["event_ymd"].min())), "end_date": str(int(events["event_ymd"].max())), "label": "upside_capture_missed_winner_diagnosis"},
        horizon="20d",
        artifact_detail_level=contracts.TRADEX_ARTIFACT_DETAIL_LEVEL_AUTHORITATIVE,
        fallback_status=contracts.TRADEX_FALLBACK_STATUS_AUTHORITATIVE,
        cost_model=contracts.TRADEX_DEFAULT_COST_MODEL,
    )
    contracts.validate_run_manifest(run_manifest)

    paths: dict[str, str] = {}
    for name, payload in {
        "evaluation_contract.json": evaluation_contract,
        "run_manifest.json": run_manifest,
        "source_artifact_refs.json": source_refs,
        "opportunity_label_contract.json": label_contract,
        "candidate_set_capture_report.json": capture_report,
        "missed_winner_report.json": missed_report,
        "negative_guard_veto_report.json": negative_report,
        "safe_full_opportunity_loss_report.json": safe_loss_report,
        "oracle_gap_report.json": oracle_gap_report,
        "topk_selection_diagnostics.json": topk_diag,
        "ranking_coverage_audit.json": ranking_audit,
    }.items():
        paths[name] = str(_write_json(output_dir / name, payload))
    paths["opportunity_day_ledger.jsonl"] = str(_write_jsonl(output_dir / "opportunity_day_ledger.jsonl", opportunity_rows))
    pre_complete = _artifact_complete(output_dir, paths)
    decision = build_research_decision(
        guard_decision=inputs["guard_json"]["research_decision.json"].get("authoritative_research_decision", ""),
        capture_rows=capture_rows,
        negative_report=negative_report,
        ranking_audit=ranking_audit,
        artifact_complete=bool(pre_complete["complete"]),
    )
    paths["research_decision.json"] = str(_write_json(output_dir / "research_decision.json", decision))
    complete = _artifact_complete(output_dir, paths, decision)
    paths["_ARTIFACT_COMPLETE.json"] = str(_write_json(output_dir / "_ARTIFACT_COMPLETE.json", complete))
    return {
        "output_dir": str(output_dir),
        "decision": decision["decision"],
        "authoritative_research_decision": decision["authoritative_research_decision"],
        "safe_full_summary": decision["safe_full_summary"],
        "negative_guard_summary": negative_report,
        "complete_topk_ranking_available": decision["complete_topk_ranking_available"],
        "silent_fallback_used": False,
        "candidate_scoring_created": False,
        "publish_bundle_created": False,
        "meemee_reflectable": False,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-pattern-run-id", default=DEFAULT_PATTERN_RUN_ID)
    parser.add_argument("--source-guard-run-id", default=DEFAULT_GUARD_RUN_ID)
    parser.add_argument("--pattern-root", default=str(DEFAULT_PATTERN_ROOT))
    parser.add_argument("--guard-root", default=str(DEFAULT_GUARD_ROOT))
    parser.add_argument("--output-root", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--run-id", default="")
    args = parser.parse_args(argv)
    result = run_upside_capture_missed_winner_diagnosis_v1(
        source_pattern_run_id=args.source_pattern_run_id,
        source_guard_run_id=args.source_guard_run_id,
        pattern_root=args.pattern_root,
        guard_root=args.guard_root,
        output_root=args.output_root,
        run_id=args.run_id.strip() or None,
    )
    print(_json_text(result))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
