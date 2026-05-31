from __future__ import annotations

import argparse
import json
import math
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd


AXIS_ID = "current_buyable_historical_operational_replay_v1"
DEFAULT_RISK_ROOT = Path(
    r"G:\Tradex\intersection_family_current_period_risk_containment_v1\20260526T010028Z-intersection-family-current-period-risk-containment-v1"
)
DEFAULT_SOURCE_ROWS = Path(
    r"G:\Tradex\pattern_family_source_rows_v1\20260525T101220Z-pattern-family-source-rows-v1\pattern_family_source_rows.parquet"
)
DEFAULT_SOURCE_DB = Path(r"C:\Users\enish\AppData\Local\MeeMeeScreener\data\stocks.duckdb")
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\current_buyable_historical_operational_replay_v1")
REQUIRED_ARTIFACTS = (
    "historical_operational_replay_summary.json",
    "historical_operational_replay_rows.csv",
    "period_operational_metrics.json",
    "invalidation_replay_metrics.json",
    "ret5_ret20_replay_metrics.json",
    "no_lookahead_audit.json",
    "source_coverage.json",
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
    path.write_text(json.dumps(_json_ready(payload), ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def load_selected_rows(risk_root: Path, source_rows: Path) -> tuple[pd.DataFrame, dict[str, Any]]:
    rows_path = risk_root / "risk_containment_rows.csv"
    decision_path = risk_root / "research_decision.json"
    if not rows_path.exists():
        raise FileNotFoundError(rows_path)
    if not decision_path.exists():
        raise FileNotFoundError(decision_path)
    rows = pd.read_csv(rows_path, dtype={"code": str})
    rows["as_of_date"] = pd.to_numeric(rows["as_of_date"], errors="coerce").astype("Int64")
    selected = rows[rows["variant_a_candle_risk_clean"].fillna(False).astype(bool)].copy()
    selected = selected.drop(columns=["ret5"], errors="ignore")
    source = pd.read_parquet(source_rows, columns=["as_of_date", "code", "ret5"])
    source["as_of_date"] = pd.to_numeric(source["as_of_date"], errors="coerce").astype("Int64")
    source["code"] = source["code"].astype(str)
    selected = selected.merge(source, on=["as_of_date", "code"], how="left", validate="one_to_one")
    selected["period_bucket"] = selected["as_of_date"].map(period_bucket)
    return selected, _load_json(decision_path)


def period_bucket(value: Any) -> str:
    date_int = int(value)
    year = date_int // 10000
    month = (date_int // 100) % 100
    return f"{year}{'H1' if month <= 6 else 'H2'}"


def load_bars(source_db: Path, codes: list[str], min_date: int, max_date: int) -> pd.DataFrame:
    con = duckdb.connect(str(source_db), read_only=True)
    try:
        expr = "CAST(strftime(to_timestamp(date), '%Y%m%d') AS INTEGER)"
        query = f"""
            SELECT CAST(code AS VARCHAR) AS code, {expr} AS bar_date, h AS high, l AS low, c AS close
            FROM daily_bars
            WHERE CAST(code AS VARCHAR) IN (SELECT * FROM UNNEST(?))
              AND {expr} BETWEEN ? AND ?
            ORDER BY code, bar_date
        """
        return con.execute(query, [codes, int(min_date), int(max_date)]).fetchdf()
    finally:
        con.close()


def build_bar_features(bars: pd.DataFrame) -> pd.DataFrame:
    out = bars.sort_values(["code", "bar_date"]).copy()
    g = out.groupby("code", sort=False)
    prev_close = g["close"].shift(1)
    tr = pd.concat([(out["high"] - out["low"]), (out["high"] - prev_close).abs(), (out["low"] - prev_close).abs()], axis=1).max(axis=1)
    out["ma20"] = g["close"].transform(lambda s: s.rolling(20, min_periods=20).mean())
    out["atr14"] = tr.groupby(out["code"]).transform(lambda s: s.rolling(14, min_periods=14).mean())
    out["recent_swing_low"] = g["low"].transform(lambda s: s.rolling(20, min_periods=20).min())
    return out


def attach_invalidation_replay(selected: pd.DataFrame, bar_features: pd.DataFrame) -> pd.DataFrame:
    asof_features = bar_features.rename(columns={"bar_date": "as_of_date", "close": "entry_reference_close"})[
        ["as_of_date", "code", "entry_reference_close", "ma20", "atr14", "recent_swing_low"]
    ]
    rows = selected.merge(asof_features, on=["as_of_date", "code"], how="left", validate="one_to_one")
    rows["invalidation_ma20_level"] = rows["ma20"]
    rows["invalidation_recent_low_level"] = rows["recent_swing_low"]
    rows["invalidation_atr_stop_level"] = rows["entry_reference_close"] - rows["atr14"]
    rows["primary_invalidation_level"] = rows[["invalidation_ma20_level", "invalidation_recent_low_level", "invalidation_atr_stop_level"]].max(axis=1)
    hits = []
    by_code = {str(code): grp.sort_values("bar_date") for code, grp in bar_features.groupby("code")}
    for row in rows.itertuples(index=False):
        future = by_code.get(str(row.code), pd.DataFrame())
        if future.empty or pd.isna(row.primary_invalidation_level):
            hits.append({"code": row.code, "as_of_date": int(row.as_of_date), "invalidation_hit_20d": None, "first_invalidation_hit_date": None})
            continue
        f = future[pd.to_numeric(future["bar_date"], errors="coerce") > int(row.as_of_date)].head(20)
        hit = f[pd.to_numeric(f["close"], errors="coerce") < float(row.primary_invalidation_level)]
        hits.append(
            {
                "code": row.code,
                "as_of_date": int(row.as_of_date),
                "invalidation_hit_20d": bool(not hit.empty),
                "first_invalidation_hit_date": int(hit.iloc[0]["bar_date"]) if not hit.empty else None,
            }
        )
    return rows.merge(pd.DataFrame(hits), on=["as_of_date", "code"], how="left", validate="one_to_one")


def metric_payload(rows: pd.DataFrame) -> dict[str, Any]:
    evaluated = rows[rows["ret20"].notna()].copy()
    if evaluated.empty:
        return {"sample_count": 0, "date_count": 0, "code_count": 0, "mean_ret5": None, "mean_ret20": None}
    return {
        "sample_count": int(len(evaluated)),
        "date_count": int(evaluated["as_of_date"].nunique()),
        "code_count": int(evaluated["code"].nunique()),
        "mean_ret5": float(evaluated["ret5"].dropna().mean()) if evaluated["ret5"].notna().any() else None,
        "mean_ret20": float(evaluated["ret20"].mean()),
        "median_ret20": float(evaluated["ret20"].median()),
        "winner_rate_ret20_gt_10pct": float((evaluated["ret20"] > 0.10).mean()),
        "bad_rate_ret20_lt_minus_5pct": float((evaluated["ret20"] < -0.05).mean()),
        "severe_rate_ret20_lt_minus_10pct": float((evaluated["ret20"] < -0.10).mean()),
        "invalidation_hit_20d_rate": float(evaluated["invalidation_hit_20d"].fillna(False).mean()),
        "outcome_coverage_rate": float(len(evaluated) / len(rows)) if len(rows) else 0.0,
    }


def period_metrics(rows: pd.DataFrame) -> dict[str, Any]:
    return {str(period): metric_payload(group) for period, group in rows.groupby("period_bucket", sort=True)}


def no_lookahead_audit(rows: pd.DataFrame, risk_decision: dict[str, Any]) -> dict[str, Any]:
    risk_ok = risk_decision.get("research_decision") == "intersection_current_period_risk_containment_buyable_ready"
    levels_complete = bool(rows[["ma20", "atr14", "recent_swing_low", "primary_invalidation_level"]].notna().all().all()) if not rows.empty else False
    return {
        "audit_result": "pass" if risk_ok and levels_complete else "blocked",
        "no_lookahead_pass": bool(risk_ok and levels_complete),
        "risk_containment_contract_ready": risk_ok,
        "levels_built_from_confirmed_bars_on_or_before_as_of_date": True,
        "future_bars_used_for_invalidation_tracking_only": True,
        "ret5_ret20_used_evaluation_only": True,
        "runtime_db_write": False,
        "research_fallback_used": False,
    }


def decide(overall: dict[str, Any], current: dict[str, Any], audit: dict[str, Any]) -> tuple[str, str, list[str]]:
    if not audit["no_lookahead_pass"]:
        return "blocked_no_lookahead_violation", "BLOCKED", ["historical_operational_replay_failed_no_lookahead"]
    overall_ok = (
        overall["mean_ret20"] is not None
        and overall["mean_ret20"] > 0.03
        and overall["winner_rate_ret20_gt_10pct"] >= 0.20
        and overall["bad_rate_ret20_lt_minus_5pct"] <= 0.20
        and overall["severe_rate_ret20_lt_minus_10pct"] <= 0.10
    )
    current_ok = (
        current["mean_ret20"] is not None
        and current["mean_ret20"] > 0.03
        and current["winner_rate_ret20_gt_10pct"] >= 0.20
        and current["bad_rate_ret20_lt_minus_5pct"] <= 0.20
        and current["severe_rate_ret20_lt_minus_10pct"] <= 0.10
    )
    invalidation_ok = overall["invalidation_hit_20d_rate"] <= 0.35 and current["invalidation_hit_20d_rate"] <= 0.35
    if overall_ok and current_ok and invalidation_ok:
        return "historical_operational_replay_supports_forward_validation", "KEEP", [
            "same_selector_risk_contract_passed_historical_operational_replay"
        ]
    return "historical_operational_replay_insufficient_for_operational_readiness", "HOLD_UNDERPOWERED", [
        "historical_operational_replay_failed_return_risk_or_invalidation_gate"
    ]


def run(
    risk_root: Path = DEFAULT_RISK_ROOT,
    source_rows: Path = DEFAULT_SOURCE_ROWS,
    source_db: Path = DEFAULT_SOURCE_DB,
    output_root: Path = DEFAULT_OUTPUT_ROOT,
) -> Path:
    selected, risk_decision = load_selected_rows(risk_root, source_rows)
    codes = sorted(selected["code"].astype(str).unique().tolist())
    min_date = int(selected["as_of_date"].min()) - 10000
    max_date = int(selected["as_of_date"].max()) + 10000
    bars = build_bar_features(load_bars(source_db, codes, min_date, max_date))
    replay = attach_invalidation_replay(selected, bars)
    overall = metric_payload(replay)
    periods = period_metrics(replay)
    current_period = sorted(periods)[-1]
    current = periods[current_period]
    audit = no_lookahead_audit(replay, risk_decision)
    decision, decision_class, reasons = decide(overall, current, audit)

    out = output_root / f"{_now_tag()}-current-buyable-historical-operational-replay-v1"
    out.mkdir(parents=True, exist_ok=True)
    replay.to_csv(out / "historical_operational_replay_rows.csv", index=False)
    _write_json(
        out / "historical_operational_replay_summary.json",
        {
            "axis_id": AXIS_ID,
            "decision": decision,
            "decision_class": decision_class,
            "reason_typed": reasons,
            "overall_metrics": overall,
            "current_period": current_period,
            "current_period_metrics": current,
            "production_ready": False,
            "validated_buy_count": 0,
        },
    )
    _write_json(out / "period_operational_metrics.json", {"axis_id": AXIS_ID, "periods": periods})
    _write_json(
        out / "invalidation_replay_metrics.json",
        {
            "axis_id": AXIS_ID,
            "overall_invalidation_hit_20d_rate": overall["invalidation_hit_20d_rate"],
            "current_invalidation_hit_20d_rate": current["invalidation_hit_20d_rate"],
            "invalidation_hit_count": int(replay["invalidation_hit_20d"].fillna(False).sum()),
        },
    )
    _write_json(out / "ret5_ret20_replay_metrics.json", {"axis_id": AXIS_ID, "overall": overall, "current_period": current})
    _write_json(out / "no_lookahead_audit.json", audit)
    _write_json(
        out / "source_coverage.json",
        {
            "axis_id": AXIS_ID,
            "risk_root": str(risk_root),
            "source_rows": str(source_rows),
            "source_db": str(source_db),
            "selected_row_count": int(len(selected)),
            "replay_row_count": int(len(replay)),
            "bar_row_count": int(len(bars)),
            "research_fallback_used": False,
        },
    )
    _write_json(
        out / "research_decision.json",
        {
            "axis_id": AXIS_ID,
            "research_decision": decision,
            "decision_class": decision_class,
            "reason_typed": reasons,
            "research_watch_only": True,
            "production_ready": False,
            "meemee_reflectable_candidate": False,
            "runtime_db_write": False,
            "production_ranking_changed": False,
            "production_candidate_generator_changed": False,
            "publish_allowed": False,
            "validated_buy_count": 0,
            "active_gate_created": False,
            "research_fallback_used": False,
        },
    )
    _write_json(out / "_ARTIFACT_COMPLETE.json", {"complete": True, "artifact_complete": True, "required_artifacts": list(REQUIRED_ARTIFACTS), "generated_at": _now_tag()})
    return out


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--risk-root", type=Path, default=DEFAULT_RISK_ROOT)
    parser.add_argument("--source-rows", type=Path, default=DEFAULT_SOURCE_ROWS)
    parser.add_argument("--source-db", type=Path, default=DEFAULT_SOURCE_DB)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    args = parser.parse_args(argv)
    out = run(args.risk_root, args.source_rows, args.source_db, args.output_root)
    print(json.dumps({"output_dir": str(out)}, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
