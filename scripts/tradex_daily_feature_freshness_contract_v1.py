from __future__ import annotations

import argparse
import json
import math
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd

if __package__ in {None, ""}:
    sys.path.append(str(Path(__file__).resolve().parents[1]))

from scripts import tradex_starter_candidate_review_pack_v2 as review_v2
from scripts.tradex_starter_entry_family_source_split_design_v1 import FAMILY_TO_SOURCE
from scripts.tradex_starter_entry_family_split_v1 import assign_families


AXIS_ID = "daily_feature_freshness_contract_v1"
DEFAULT_FAMILY_SOURCE_ROOT = Path(r"G:\Tradex\starter_entry_family_source_split_design_v1\20260525T041110Z-starter-entry-family-source-split-design-v1")
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\daily_feature_freshness_contract_v1")
DEFAULT_REVIEW_PACK_OUTPUT_ROOT = Path(r"G:\Tradex\starter_candidate_review_pack_v2")

REQUIRED_ARTIFACTS = (
    "input_artifact_report.json",
    "daily_feature_freshness_root_cause_report.json",
    "daily_source_contract.json",
    "current_family_feature_rows.csv",
    "current_family_surface_rows.csv",
    "family_feature_freshness_report.json",
    "review_pack_freshness_check.json",
    "rerun_review_pack_summary.json",
    "research_decision.json",
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


def _epoch_from_ymd(ymd: int) -> int:
    return int(pd.Timestamp(str(int(ymd)), tz="UTC").timestamp())


def _ymd_from_epoch(epoch: int | None) -> int | None:
    if epoch is None:
        return None
    return int(pd.to_datetime(int(epoch), unit="s", utc=True).strftime("%Y%m%d"))


def _max_csv_date(path: Path, date_col: str = "date") -> int | None:
    if not path.exists():
        return None
    try:
        latest: int | None = None
        for chunk in pd.read_csv(path, usecols=[date_col], chunksize=500_000, low_memory=False):
            vals = pd.to_datetime(chunk[date_col], errors="coerce").dt.strftime("%Y%m%d")
            value = pd.to_numeric(vals, errors="coerce").max()
            if pd.notna(value):
                latest = int(value) if latest is None else max(latest, int(value))
        return latest
    except Exception:
        return None


def _max_decision_date(path: Path) -> int | None:
    latest: int | None = None
    for chunk in pd.read_csv(path, usecols=["decision_date"], chunksize=500_000, low_memory=False):
        value = pd.to_numeric(chunk["decision_date"], errors="coerce").max()
        if pd.notna(value):
            latest = int(value) if latest is None else max(latest, int(value))
    return latest


def _read_date_rows(path: Path, decision_date: int) -> pd.DataFrame:
    parts: list[pd.DataFrame] = []
    for chunk in pd.read_csv(path, chunksize=500_000, low_memory=False):
        dates = pd.to_numeric(chunk["decision_date"], errors="coerce")
        part = chunk[dates.eq(decision_date)].copy()
        if not part.empty:
            parts.append(part)
    return pd.concat(parts, ignore_index=True) if parts else pd.DataFrame()


def _runtime_db_candidates() -> list[Path]:
    local = Path(os.environ.get("LOCALAPPDATA", str(Path.home() / "AppData" / "Local")))
    return [
        local / "MeeMeeScreener-dev" / "data" / "stocks.duckdb",
        local / "MeeMeeScreener" / "data" / "stocks.duckdb",
        Path("data/stocks.duckdb"),
        Path("app/backend/stocks.duckdb"),
    ]


def inspect_runtime_db(path: Path, review_date: int | None = None) -> dict[str, Any]:
    if not path.exists():
        return {"path": str(path), "exists": False}
    out: dict[str, Any] = {"path": str(path), "exists": True}
    try:
        con = duckdb.connect(str(path), read_only=True)
        cols = [r[1] for r in con.execute("PRAGMA table_info('daily_bars')").fetchall()]
        out["daily_bars_columns"] = cols
        max_epoch = con.execute("SELECT max(date) FROM daily_bars").fetchone()[0]
        out["daily_max_epoch"] = max_epoch
        out["daily_max_date"] = _ymd_from_epoch(max_epoch)
        if "source" in cols:
            by_source = con.execute("SELECT source, max(date), count(*) FROM daily_bars GROUP BY source ORDER BY max(date) DESC").fetchall()
            out["daily_max_by_source"] = [
                {"source": row[0], "max_epoch": row[1], "max_date": _ymd_from_epoch(row[1]), "row_count": row[2]}
                for row in by_source
            ]
            if review_date is not None:
                epoch = _epoch_from_ymd(review_date)
                rows = con.execute("SELECT source, count(*) FROM daily_bars WHERE date = ? GROUP BY source ORDER BY source", [epoch]).fetchall()
                out["review_date_source_counts"] = [{"source": r[0], "row_count": r[1]} for r in rows]
        con.close()
    except Exception as exc:
        out["error"] = repr(exc)
    return out


def select_daily_source(review_date: int) -> dict[str, Any]:
    inspected = [inspect_runtime_db(path, review_date) for path in _runtime_db_candidates()]
    covering = [x for x in inspected if x.get("daily_max_date") is not None and int(x["daily_max_date"]) >= review_date]
    selected = covering[0] if covering else None
    if not selected:
        return {"selected": None, "inspected_runtime_dbs": inspected, "decision": "blocked"}
    source_counts = selected.get("review_date_source_counts") or []
    sources = {str(x["source"]) for x in source_counts}
    if sources and sources.issubset({"pan", "txt", "confirmed"}):
        source_type = "confirmed"
        provisional_used = False
    elif sources and sources.issubset({"yahoo"}):
        source_type = "provisional"
        provisional_used = True
    elif sources:
        source_type = "mixed"
        provisional_used = "yahoo" in sources
    else:
        source_type = "blocked"
        provisional_used = False
    return {
        "selected": selected,
        "inspected_runtime_dbs": inspected,
        "decision": "ready" if source_type != "blocked" else "blocked",
        "source_type": source_type,
        "provisional_used": provisional_used,
    }


def _calc_streak(mask: pd.Series) -> int:
    count = 0
    for value in reversed(mask.fillna(False).astype(bool).tolist()):
        if not value:
            break
        count += 1
    return count


def _days_since_reclaim(df: pd.DataFrame, ma_col: str) -> float | None:
    close = df["c"]
    ma = df[ma_col]
    above = close > ma
    crossed = above & (~above.shift(1).fillna(False))
    idxs = df.index[crossed].tolist()
    if not idxs:
        return None
    return float(len(df) - 1 - idxs[-1])


def compute_feature_rows(candidate_rows: pd.DataFrame, db_path: Path, review_date: int, source_type: str, provisional_used: bool) -> pd.DataFrame:
    codes = [str(c).removesuffix(".0") for c in candidate_rows["code"].astype(str).tolist()]
    epoch = _epoch_from_ymd(review_date)
    con = duckdb.connect(str(db_path), read_only=True)
    bars = con.execute(
        "SELECT code, date, o, h, l, c, v, source FROM daily_bars WHERE code IN ? AND date <= ? ORDER BY code, date",
        [codes, epoch],
    ).df()
    con.close()
    bars["code"] = bars["code"].astype(str)
    bars["ymd"] = bars["date"].map(_ymd_from_epoch)
    rows: list[dict[str, Any]] = []
    for code in codes:
        g = bars[bars["code"].eq(code)].sort_values("date").tail(90).copy()
        base = candidate_rows[candidate_rows["code"].astype(str).str.removesuffix(".0").eq(code)].iloc[0].to_dict()
        rec = {"code": code, "decision_date": review_date}
        if len(g) < 60 or int(g["ymd"].max() or 0) < review_date:
            rec.update({"feature_freshness_status": "missing_or_insufficient_history", "daily_bar_source": source_type, "provisional_used": provisional_used})
            rows.append({**base, **rec})
            continue
        g["ma7"] = g["c"].rolling(7).mean()
        g["ma20"] = g["c"].rolling(20).mean()
        g["ma60"] = g["c"].rolling(60).mean()
        g["vol_ma20"] = g["v"].rolling(20).mean()
        g["prev_c"] = g["c"].shift(1)
        tr = pd.concat([(g["h"] - g["l"]), (g["h"] - g["prev_c"]).abs(), (g["l"] - g["prev_c"]).abs()], axis=1).max(axis=1)
        g["atr14"] = tr.rolling(14).mean()
        last = g.iloc[-1]
        prev7 = g.iloc[-8] if len(g) >= 8 else last
        prev20 = g.iloc[-21] if len(g) >= 21 else last
        prev60 = g.iloc[-61] if len(g) >= 61 else last
        body = abs(float(last["c"] - last["o"]))
        rng = max(float(last["h"] - last["l"]), 1e-9)
        rec.update(
            {
                "close_y": float(last["c"]),
                "ma7": float(last["ma7"]),
                "ma20": float(last["ma20"]),
                "ma60": float(last["ma60"]),
                "ma7_slope": float(last["ma7"] / prev7["ma7"] - 1) if pd.notna(prev7["ma7"]) and prev7["ma7"] else None,
                "ma20_slope": float(last["ma20"] / prev20["ma20"] - 1) if pd.notna(prev20["ma20"]) and prev20["ma20"] else None,
                "ma60_slope": float(last["ma60"] / prev60["ma60"] - 1) if pd.notna(prev60["ma60"]) and prev60["ma60"] else None,
                "dist_ma7_pct": float(last["c"] / last["ma7"] - 1),
                "dist_ma20_pct": float(last["c"] / last["ma20"] - 1),
                "dist_ma60_pct": float(last["c"] / last["ma60"] - 1),
                "ma7_gt_ma20_gt_ma60": bool(last["ma7"] > last["ma20"] > last["ma60"]),
                "above7_streak": _calc_streak(g["c"] > g["ma7"]),
                "above20_streak": _calc_streak(g["c"] > g["ma20"]),
                "above60_streak": _calc_streak(g["c"] > g["ma60"]),
                "days_since_ma20_reclaim": _days_since_reclaim(g, "ma20"),
                "days_since_ma60_reclaim": _days_since_reclaim(g, "ma60"),
                "upper_wick_ratio": float((last["h"] - max(last["o"], last["c"])) / rng),
                "lower_wick_ratio": float((min(last["o"], last["c"]) - last["l"]) / rng),
                "large_bullish_candle": bool(last["c"] > last["o"] and body / rng >= 0.6),
                "large_bearish_candle": bool(last["c"] < last["o"] and body / rng >= 0.6),
                "failed_high_update": bool(last["h"] < g["h"].tail(20).max() and last["c"] < last["o"]),
                "volume_ma20_ratio": float(last["v"] / last["vol_ma20"]) if pd.notna(last["vol_ma20"]) and last["vol_ma20"] else None,
                "realized_vol20": float(g["c"].pct_change().tail(20).std()),
                "atr14_pct": float(last["atr14"] / last["c"]) if pd.notna(last["atr14"]) and last["c"] else None,
                "monthly_high_zone_proxy": bool(last["c"] >= g["c"].tail(60).max() * 0.95),
                "monthly_box_breakout_proxy": bool(last["c"] >= g["h"].tail(60).quantile(0.9)),
                "monthly_box_inside_proxy": bool(last["c"] < g["h"].tail(60).quantile(0.8) and last["c"] > g["l"].tail(60).quantile(0.2)),
                "weekly_monthly_uptrend_proxy": bool(last["ma20"] > prev20["ma20"] and last["ma60"] >= prev60["ma60"]),
                "daily_bar_source": source_type,
                "daily_bar_max_date": int(g["ymd"].max()),
                "feature_source_max_date": int(g["ymd"].max()),
                "feature_freshness_status": "fresh" if int(g["ymd"].max()) >= review_date else "stale",
                "provisional_used": provisional_used,
            }
        )
        rows.append({**base, **rec})
    out = pd.DataFrame(rows)
    for col in ["dist_ma20_pct", "dist_ma60_pct", "ma7_slope", "realized_vol20"]:
        vals = pd.to_numeric(out[col], errors="coerce")
        out[f"{col.replace('_pct','')}_top_quartile"] = vals >= vals.quantile(0.75)
    return out


def build_surface_rows(feature_rows: pd.DataFrame) -> pd.DataFrame:
    rows = assign_families(feature_rows)
    rows["research_candidate_source_family"] = rows["primary_family"].map(FAMILY_TO_SOURCE).fillna("uncategorized_source")
    rows["research_family_surface"] = rows["research_candidate_source_family"]
    rows["research_family_source_schema_version"] = "research_family_source_schema_v1_current_review_source_contract"
    rows["feature_availability_json"] = rows.get("family_feature_availability_json", "{}")
    rows["research_family_assignment_reason_json"] = rows.get("family_assignment_reason_json", "{}")
    rows["labels_required_for_current_review"] = False
    rows["current_review_no_lookahead_mode"] = True
    if "path20_available" in rows:
        rows["path20_available"] = rows["path20_available"].fillna(False)
    else:
        rows["path20_available"] = False
    rows["within_family_baseline_rank"] = (
        rows.sort_values(["decision_date", "research_candidate_source_family", "baseline_score", "code"], ascending=[True, True, False, True])
        .groupby(["decision_date", "research_candidate_source_family"])
        .cumcount()
        + 1
    )
    return rows


def run(family_source_root: Path, output_root: Path, review_pack_output_root: Path) -> Path:
    out = output_root / f"{_now_tag()}-daily-feature-freshness-contract-v1"
    out.mkdir(parents=True, exist_ok=True)
    candidate_rows_path = family_source_root / "candidate_family_source_rows.csv"
    latest_global = _max_decision_date(candidate_rows_path)
    if latest_global is None:
        raise RuntimeError(f"no decision_date found in {candidate_rows_path}")
    candidate_rows = _read_date_rows(candidate_rows_path, latest_global)
    source_paths = sorted(str(p) for p in candidate_rows["source_artifact_path"].dropna().unique()) if "source_artifact_path" in candidate_rows else []
    source_selection = select_daily_source(latest_global)
    prod_max = _max_csv_date(Path("production_data/production_daily.csv"))
    selected = source_selection.get("selected") or {}
    root_cause = "production_data/production_daily.csv is a stale confirmed export; latest candidate generation used a newer artifact snapshot while runtime DuckDB daily_bars has newer bars"
    _write_json(
        out / "daily_feature_freshness_root_cause_report.json",
        {
            "latest_global_candidate_date": latest_global,
            "production_daily_max_date": prod_max,
            "candidate_source_max_date": latest_global,
            "runtime_db_daily_max_date": selected.get("daily_max_date"),
            "confirmed_daily_max_date": max((x["max_date"] for x in selected.get("daily_max_by_source", []) if x["source"] in {"pan", "txt", "confirmed"}), default=None),
            "provisional_daily_max_date": max((x["max_date"] for x in selected.get("daily_max_by_source", []) if x["source"] == "yahoo"), default=None),
            "exact_files_tables_inspected": {
                "candidate_family_rows": str(candidate_rows_path),
                "candidate_snapshot_paths": source_paths,
                "production_daily": str(Path("production_data/production_daily.csv").resolve()),
                "runtime_dbs": [x.get("path") for x in source_selection["inspected_runtime_dbs"]],
                "runtime_table": "daily_bars",
            },
            "root_cause": root_cause,
        },
    )
    contract = {
        "allowed_source_types": ["confirmed", "provisional", "mixed"],
        "current_review_provisional_rules": {
            "bar_source": "provisional or mixed",
            "feature_confidence": "provisional",
            "validated_buy_allowed": False,
            "silent_fallback_allowed": False,
        },
        "historical_validation_rule": "confirmed and label-safe logic remains separate",
        "required_row_fields": ["daily_bar_source", "daily_bar_max_date", "feature_source_max_date", "feature_freshness_status", "provisional_used"],
    }
    _write_json(out / "daily_source_contract.json", contract)
    _write_json(
        out / "input_artifact_report.json",
        {
            "family_source_root": family_source_root,
            "candidate_rows_path": candidate_rows_path,
            "latest_global_candidate_date": latest_global,
            "candidate_rows_at_latest_global_date": int(len(candidate_rows)),
            "selected_daily_source": selected,
            "source_type": source_selection.get("source_type"),
            "runtime_db_write": False,
            "meemee_changed": False,
            "production_ranking_changed": False,
        },
    )
    if source_selection["decision"] != "ready":
        decision = "daily_feature_freshness_blocked"
        blocker = "daily_bars_missing_for_latest_candidate_date"
        pd.DataFrame().to_csv(out / "current_family_feature_rows.csv", index=False)
        pd.DataFrame().to_csv(out / "current_family_surface_rows.csv", index=False)
        rerun_summary: dict[str, Any] = {}
    else:
        feature_rows = compute_feature_rows(
            candidate_rows,
            Path(selected["path"]),
            latest_global,
            str(source_selection["source_type"]),
            bool(source_selection["provisional_used"]),
        )
        feature_rows.to_csv(out / "current_family_feature_rows.csv", index=False)
        surface_rows = build_surface_rows(feature_rows)
        surface_rows.to_csv(out / "current_family_surface_rows.csv", index=False)
        requested = {"pullback_reclaim_source", "breakout_retest_source", "mature_trend_continuation_source", "early_trend_source", "range_reversal_source", "overextension_risk_source"}
        coverage = surface_rows["research_candidate_source_family"].value_counts().to_dict()
        requested_coverage = sorted(requested.intersection(coverage))
        feature_ok = bool((surface_rows["feature_freshness_status"] == "fresh").all())
        if not feature_ok:
            decision = "daily_feature_freshness_blocked"
            blocker = "point_in_time_features_stale"
        elif not requested_coverage:
            decision = "daily_feature_freshness_blocked"
            blocker = "family_assignment_unavailable"
        elif source_selection.get("source_type") in {"provisional", "mixed"}:
            decision = "current_review_ready_provisional"
            blocker = None
        else:
            decision = "current_review_ready_confirmed"
            blocker = None
        _write_json(
            out / "family_feature_freshness_report.json",
            {
                "latest_global_candidate_date": latest_global,
                "daily_bar_source": source_selection.get("source_type"),
                "daily_bar_max_date": selected.get("daily_max_date"),
                "feature_source_max_date": int(surface_rows["feature_source_max_date"].max()) if not surface_rows.empty else None,
                "feature_freshness_status_counts": surface_rows["feature_freshness_status"].value_counts().to_dict(),
                "provisional_used": bool(source_selection.get("provisional_used")),
                "family_assignment_coverage": coverage,
                "requested_family_coverage": requested_coverage,
                "current_review_blocked": decision == "daily_feature_freshness_blocked",
                "blocker": blocker,
                "no_lookahead_audit": {
                    "uses_only_bars_through_review_date": True,
                    "future_labels_required": False,
                    "source_provisional_status_recorded": True,
                },
            },
        )
        rerun_root = review_v2.build_pack(out, review_pack_output_root)
        rerun_summary = {
            "rerun_review_pack_root": rerun_root,
            "summary": json.loads((rerun_root / "review_pack_summary.json").read_text(encoding="utf-8")),
            "decision": json.loads((rerun_root / "review_pack_decision.json").read_text(encoding="utf-8")),
        }
    if not (out / "family_feature_freshness_report.json").exists():
        _write_json(out / "family_feature_freshness_report.json", {"current_review_blocked": True, "blocker": blocker})
    if rerun_summary:
        summary = rerun_summary["summary"]
        rdecision = rerun_summary["decision"]
    else:
        summary = {}
        rdecision = {}
    _write_json(
        out / "review_pack_freshness_check.json",
        {
            "review_date": summary.get("review_date"),
            "latest_global_candidate_date": latest_global,
            "review_date_equals_latest_global_date": summary.get("review_date") == latest_global,
            "manual_review_available": summary.get("manual_review_available", False),
            "decision_gate": decision,
            "blocker": blocker,
        },
    )
    _write_json(out / "rerun_review_pack_summary.json", rerun_summary)
    _write_json(
        out / "research_decision.json",
        {
            "research_decision": decision,
            "blocker_reason": blocker,
            "meemee_reflectable_candidate": False,
            "validated_buy_claim": False,
            "runtime_db_write": False,
            "production_ranking_changed": False,
            "counts": {
                "validated_buy": rdecision.get("validated_buy_count", 0),
                "starter_review": rdecision.get("starter_review_count", 0),
                "watch": rdecision.get("watch_count", 0),
                "wait": rdecision.get("wait_count", 0),
                "avoid": rdecision.get("avoid_count", 0),
            },
        },
    )
    existing = [name for name in REQUIRED_ARTIFACTS if name != "_ARTIFACT_COMPLETE.json" and (out / name).exists()]
    _write_json(out / "_ARTIFACT_COMPLETE.json", {"axis_id": AXIS_ID, "required_artifacts": list(REQUIRED_ARTIFACTS), "complete": len(existing) == len(REQUIRED_ARTIFACTS) - 1})
    return out


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--family-source-root", type=Path, default=DEFAULT_FAMILY_SOURCE_ROOT)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--review-pack-output-root", type=Path, default=DEFAULT_REVIEW_PACK_OUTPUT_ROOT)
    args = parser.parse_args()
    print(run(args.family_source_root, args.output_root, args.review_pack_output_root))


if __name__ == "__main__":
    main()
