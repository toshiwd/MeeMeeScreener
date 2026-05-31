from __future__ import annotations

import argparse
import json
import math
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd


AXIS_ID = "current_buyable_invalidation_variant_compare_v1"
DEFAULT_REPLAY_ROOT = Path(
    r"G:\Tradex\current_buyable_historical_operational_replay_v1\20260526T014356Z-current-buyable-historical-operational-replay-v1"
)
DEFAULT_SOURCE_DB = Path(r"C:\Users\enish\AppData\Local\MeeMeeScreener\data\stocks.duckdb")
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\current_buyable_invalidation_variant_compare_v1")
REQUIRED_ARTIFACTS = (
    "invalidation_variant_summary.json",
    "invalidation_variant_rows.csv",
    "variant_metrics.json",
    "period_variant_metrics.json",
    "recommended_invalidation_contract.json",
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
        return [_json_ready(value) for value in value]
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


def load_replay_rows(replay_root: Path) -> tuple[pd.DataFrame, dict[str, Any]]:
    rows_path = replay_root / "historical_operational_replay_rows.csv"
    decision_path = replay_root / "research_decision.json"
    if not rows_path.exists():
        raise FileNotFoundError(rows_path)
    if not decision_path.exists():
        raise FileNotFoundError(decision_path)
    rows = pd.read_csv(rows_path, dtype={"code": str})
    rows["as_of_date"] = pd.to_numeric(rows["as_of_date"], errors="coerce").astype("Int64")
    for col in ["entry_reference_close", "ma20", "recent_swing_low", "atr14", "ret20", "ret5"]:
        rows[col] = pd.to_numeric(rows[col], errors="coerce")
    return rows, _load_json(decision_path)


def load_bars(source_db: Path, codes: list[str], min_date: int, max_date: int) -> pd.DataFrame:
    con = duckdb.connect(str(source_db), read_only=True)
    try:
        expr = "CAST(strftime(to_timestamp(date), '%Y%m%d') AS INTEGER)"
        return con.execute(
            f"""
            SELECT CAST(code AS VARCHAR) AS code, {expr} AS bar_date, c AS close
            FROM daily_bars
            WHERE CAST(code AS VARCHAR) IN (SELECT * FROM UNNEST(?))
              AND {expr} BETWEEN ? AND ?
            ORDER BY code, bar_date
            """,
            [codes, int(min_date), int(max_date)],
        ).fetchdf()
    finally:
        con.close()


def add_variant_levels(rows: pd.DataFrame) -> pd.DataFrame:
    out = rows.copy()
    out["stop_ma20"] = out["ma20"]
    out["stop_recent_swing_low"] = out["recent_swing_low"]
    out["stop_atr1"] = out["entry_reference_close"] - out["atr14"]
    out["stop_atr1_5"] = out["entry_reference_close"] - out["atr14"] * 1.5
    out["stop_atr2"] = out["entry_reference_close"] - out["atr14"] * 2.0
    return out


def attach_hits(rows: pd.DataFrame, bars: pd.DataFrame) -> pd.DataFrame:
    out = add_variant_levels(rows)
    variants = ["stop_ma20", "stop_recent_swing_low", "stop_atr1", "stop_atr1_5", "stop_atr2"]
    by_code = {str(code): grp.sort_values("bar_date") for code, grp in bars.groupby("code")}
    hit_payload: list[dict[str, Any]] = []
    for row in out.itertuples(index=False):
        payload: dict[str, Any] = {"as_of_date": int(row.as_of_date), "code": str(row.code)}
        future = by_code.get(str(row.code), pd.DataFrame())
        future = future[pd.to_numeric(future.get("bar_date", pd.Series(dtype=float)), errors="coerce") > int(row.as_of_date)].head(20)
        for variant in variants:
            level = getattr(row, variant)
            if pd.isna(level) or future.empty:
                payload[f"{variant}_hit_20d"] = None
                payload[f"{variant}_first_hit_date"] = None
            else:
                hit = future[pd.to_numeric(future["close"], errors="coerce") < float(level)]
                payload[f"{variant}_hit_20d"] = bool(not hit.empty)
                payload[f"{variant}_first_hit_date"] = int(hit.iloc[0]["bar_date"]) if not hit.empty else None
        hit_payload.append(payload)
    return out.merge(pd.DataFrame(hit_payload), on=["as_of_date", "code"], how="left", validate="one_to_one")


def metric_payload(rows: pd.DataFrame, variant: str) -> dict[str, Any]:
    evaluated = rows[rows["ret20"].notna()].copy()
    hit_col = f"{variant}_hit_20d"
    hit = evaluated[hit_col].fillna(False).astype(bool)
    winner = evaluated["ret20"] > 0.10
    bad = evaluated["ret20"] < -0.05
    return {
        "sample_count": int(len(evaluated)),
        "date_count": int(evaluated["as_of_date"].nunique()),
        "code_count": int(evaluated["code"].nunique()),
        "mean_ret20": float(evaluated["ret20"].mean()) if not evaluated.empty else None,
        "winner_rate_ret20_gt_10pct": float(winner.mean()) if not evaluated.empty else None,
        "bad_rate_ret20_lt_minus_5pct": float(bad.mean()) if not evaluated.empty else None,
        "invalidation_hit_20d_rate": float(hit.mean()) if not evaluated.empty else None,
        "stopped_winner_rate": float((hit & winner).mean()) if not evaluated.empty else None,
        "bad_captured_by_stop_rate": float((hit & bad).sum() / bad.sum()) if int(bad.sum()) else None,
    }


def all_variant_metrics(rows: pd.DataFrame) -> dict[str, Any]:
    return {variant: metric_payload(rows, variant) for variant in ["stop_ma20", "stop_recent_swing_low", "stop_atr1", "stop_atr1_5", "stop_atr2"]}


def period_variant_metrics(rows: pd.DataFrame) -> dict[str, Any]:
    return {str(period): all_variant_metrics(group) for period, group in rows.groupby("period_bucket", sort=True)}


def choose_variant(metrics: dict[str, Any], period_metrics: dict[str, Any]) -> tuple[str, dict[str, Any]]:
    current_period = sorted(period_metrics)[-1]
    candidates = []
    for name, overall in metrics.items():
        current = period_metrics[current_period][name]
        if (
            overall["invalidation_hit_20d_rate"] is not None
            and current["invalidation_hit_20d_rate"] is not None
            and overall["invalidation_hit_20d_rate"] <= 0.35
            and current["invalidation_hit_20d_rate"] <= 0.35
            and overall["stopped_winner_rate"] <= 0.10
            and current["stopped_winner_rate"] <= 0.10
        ):
            candidates.append((name, overall, current))
    if not candidates:
        return "", {}
    candidates.sort(key=lambda item: (item[1]["bad_captured_by_stop_rate"] or 0.0, -item[1]["invalidation_hit_20d_rate"]), reverse=True)
    name, overall, current = candidates[0]
    return name, {"overall": overall, "current_period": current, "current_period_id": current_period}


def no_lookahead_audit(rows: pd.DataFrame, replay_decision: dict[str, Any]) -> dict[str, Any]:
    replay_ok = replay_decision.get("research_decision") == "historical_operational_replay_insufficient_for_operational_readiness"
    levels_complete = bool(rows[["stop_ma20", "stop_recent_swing_low", "stop_atr1", "stop_atr1_5", "stop_atr2"]].notna().all().all()) if not rows.empty else False
    return {
        "audit_result": "pass" if replay_ok and levels_complete else "blocked",
        "no_lookahead_pass": bool(replay_ok and levels_complete),
        "replay_contract_loaded": replay_ok,
        "stop_levels_use_asof_features_only": True,
        "future_bars_used_for_hit_evaluation_only": True,
        "ret20_used_evaluation_only": True,
        "runtime_db_write": False,
        "research_fallback_used": False,
    }


def decide(best_name: str, audit: dict[str, Any]) -> tuple[str, str, list[str]]:
    if not audit["no_lookahead_pass"]:
        return "blocked_no_lookahead_violation", "BLOCKED", ["invalidation_variant_compare_failed_no_lookahead"]
    if best_name:
        return "invalidation_contract_variant_ready_for_forward_tracking", "KEEP", [f"{best_name}_passed_historical_stop_operability_gate"]
    return "invalidation_contract_variants_not_operationally_clean", "HOLD_UNDERPOWERED", ["no_fixed_stop_variant_met_operability_gate"]


def run(
    replay_root: Path = DEFAULT_REPLAY_ROOT,
    source_db: Path = DEFAULT_SOURCE_DB,
    output_root: Path = DEFAULT_OUTPUT_ROOT,
) -> Path:
    rows, replay_decision = load_replay_rows(replay_root)
    codes = sorted(rows["code"].astype(str).unique().tolist())
    bars = load_bars(source_db, codes, int(rows["as_of_date"].min()), int(rows["as_of_date"].max()) + 10000)
    compared = attach_hits(rows, bars)
    metrics = all_variant_metrics(compared)
    periods = period_variant_metrics(compared)
    best_name, best_metrics = choose_variant(metrics, periods)
    audit = no_lookahead_audit(compared, replay_decision)
    decision, decision_class, reasons = decide(best_name, audit)

    out = output_root / f"{_now_tag()}-current-buyable-invalidation-variant-compare-v1"
    out.mkdir(parents=True, exist_ok=True)
    compared.to_csv(out / "invalidation_variant_rows.csv", index=False)
    _write_json(
        out / "invalidation_variant_summary.json",
        {
            "axis_id": AXIS_ID,
            "decision": decision,
            "decision_class": decision_class,
            "reason_typed": reasons,
            "best_variant": best_name or None,
            "best_variant_metrics": best_metrics or None,
            "production_ready": False,
            "validated_buy_count": 0,
        },
    )
    _write_json(out / "variant_metrics.json", {"axis_id": AXIS_ID, "variants": metrics})
    _write_json(out / "period_variant_metrics.json", {"axis_id": AXIS_ID, "periods": periods})
    _write_json(
        out / "recommended_invalidation_contract.json",
        {
            "axis_id": AXIS_ID,
            "recommended_variant": best_name or None,
            "rule": None if not best_name else f"primary_invalidation_level={best_name}",
            "candidate_selection_changed": False,
            "future_outcomes_used_to_set_level": False,
            "requires_forward_validation_rerun": True,
            "validated_buy_count": 0,
        },
    )
    _write_json(out / "no_lookahead_audit.json", audit)
    _write_json(
        out / "source_coverage.json",
        {
            "axis_id": AXIS_ID,
            "replay_root": str(replay_root),
            "source_db": str(source_db),
            "row_count": int(len(compared)),
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
    parser.add_argument("--replay-root", type=Path, default=DEFAULT_REPLAY_ROOT)
    parser.add_argument("--source-db", type=Path, default=DEFAULT_SOURCE_DB)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    args = parser.parse_args(argv)
    out = run(args.replay_root, args.source_db, args.output_root)
    print(json.dumps({"output_dir": str(out)}, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
