from __future__ import annotations

import argparse
import json
import math
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping

import duckdb
import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts import tradex_common_ledger_field_repair_v1 as field_repair
from scripts import tradex_monthly_drawdown_guarded_momentum_starter_entry_pretest_v1 as pretest
from scripts import tradex_monthly_drawdown_guarded_momentum_top5_gate_v1 as top5_gate


AXIS_ID = "monthly_drawdown_guarded_momentum_entry_timing_confirmation_v1"
SCHEMA_PREFIX = "tradex_monthly_drawdown_guarded_momentum_entry_timing_confirmation"
DEFAULT_SOURCE_PRETEST_ROOT = Path(
    "G:/Tradex/monthly_drawdown_guarded_momentum_starter_entry_pretest_v1/"
    "20260515T003000Z-monthly-drawdown-guarded-momentum-starter-entry-pretest-v1"
)
DEFAULT_OUTPUT_PARENT = Path("G:/Tradex/monthly_drawdown_guarded_momentum_entry_timing_confirmation_v1")
DEFAULT_RUN_ID = "20260515T013000Z-monthly-drawdown-guarded-momentum-entry-timing-confirmation-v1"

REQUIRED_ARTIFACTS = [
    "entry_timing_confirmation_contract.json",
    "run_manifest.json",
    "source_artifact_refs.json",
    "confirmation_rule_report.json",
    "confirmed_candidate_metrics.json",
    "confirmation_by_year_report.json",
    "shakeout_recovery_case_report.json",
    "symbol_7327_case_report.json",
    "candidate_timing_confirmation_rows.jsonl",
    "manual_review_candidate_examples.json",
    "no_mutation_audit.json",
    "next_axis_recommendation.json",
    "research_decision.json",
    "_ARTIFACT_COMPLETE.json",
]


def _utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _read_json(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def _write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=False, indent=2, sort_keys=True)
        handle.write("\n")


def _write_jsonl(path: Path, rows: Iterable[Mapping[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            json.dump(row, handle, ensure_ascii=False, sort_keys=True)
            handle.write("\n")


def _date_expr(column: str) -> str:
    return field_repair._date_norm_expr(column)


def _load_daily_ohlcv(source_duckdb: Path, symbols: list[str], start_ymd: int, end_ymd: int) -> pd.DataFrame:
    if not source_duckdb.exists():
        raise FileNotFoundError(f"source DB not found: {source_duckdb}")
    if not symbols:
        return pd.DataFrame(columns=["code", "ymd", "o", "h", "l", "c", "v"])
    placeholders = ", ".join(["?"] * len(symbols))
    expr = _date_expr("date")
    query = f"""
        SELECT code, {expr} AS ymd, o, h, l, c, v
        FROM daily_bars
        WHERE code IN ({placeholders})
          AND {expr} BETWEEN ? AND ?
          AND lower(coalesce(source, '')) = 'pan'
          AND o > 0 AND h > 0 AND l > 0 AND c > 0
        ORDER BY code, ymd
    """
    params: list[Any] = [*symbols, int(start_ymd), int(end_ymd)]
    with duckdb.connect(str(source_duckdb), read_only=True) as conn:
        frame = conn.execute(query, params).fetchdf()
    if frame.empty:
        return frame
    frame["code"] = frame["code"].astype(str)
    frame["ymd"] = pd.to_numeric(frame["ymd"], errors="coerce").astype("Int64")
    frame = frame.dropna(subset=["ymd", "o", "h", "l", "c"]).copy()
    frame["ymd"] = frame["ymd"].astype(int)
    for col in ["o", "h", "l", "c", "v"]:
        frame[col] = pd.to_numeric(frame[col], errors="coerce")
    return frame


def _event_ymd(value: Any) -> int:
    out = field_repair._event_date_to_ymd(value)
    if out is None:
        raise ValueError(f"invalid event_date: {value}")
    return out


def _add_pit_features(daily: pd.DataFrame) -> pd.DataFrame:
    if daily.empty:
        return daily
    work = daily.sort_values(["code", "ymd"], kind="stable").copy()
    grouped = work.groupby("code", sort=False)
    for window in [7, 20, 60, 100, 200]:
        work[f"ma{window}"] = grouped["c"].transform(lambda s: s.rolling(window, min_periods=window).mean())
    work["prev_close"] = grouped["c"].shift(1)
    work["prev_ma20"] = grouped["ma20"].shift(1)
    work["ma20_5d_ago"] = grouped["ma20"].shift(5)
    work["vol20"] = grouped["v"].transform(lambda s: s.rolling(20, min_periods=10).mean())
    work["high20"] = grouped["h"].transform(lambda s: s.rolling(20, min_periods=10).max())
    work["close_above_ma20"] = work["c"] >= work["ma20"]
    work["close_above_ma60"] = work["c"] >= work["ma60"]
    work["ma20_rising_5d"] = work["ma20"] >= work["ma20_5d_ago"]
    work["ma20_reclaim_today"] = (work["c"] >= work["ma20"]) & (work["prev_close"] < work["prev_ma20"])
    work["close_up_day"] = work["c"] > work["prev_close"]
    work["volume_expansion"] = work["v"] >= work["vol20"] * 1.2
    work["near_20d_high"] = work["c"] >= work["high20"] * 0.97
    return work


def _prepare_selection(source_pretest_root: Path) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, dict[str, Any]]:
    manifest = _read_json(source_pretest_root / "run_manifest.json")
    source_top5_gate_root = Path(manifest["source_top5_gate_root"])
    source_field_repair_root = Path(manifest["source_field_repair_root"])
    source_top5_leaderboard = _read_json(source_top5_gate_root / "strict_gate_leaderboard.json")
    source_rows = top5_gate._read_jsonl(source_field_repair_root / "repaired_common_top5_candidate_ledger.jsonl")
    frame = top5_gate._prepare_frame(source_rows)
    baseline = pretest._select_with_score(frame, top5_gate._variant_specs()[0])
    starter = pretest._select_with_score(frame, source_top5_leaderboard["best_variant"]["spec"])
    starter = starter.sort_values(["event_date", "_candidate_score", "symbol"], ascending=[True, False, True], kind="stable").copy()
    starter["starter_rank"] = starter.groupby("event_date", sort=False).cumcount() + 1
    baseline = baseline.sort_values(["event_date", "_candidate_score", "symbol"], ascending=[True, False, True], kind="stable").copy()
    baseline["baseline_rank"] = baseline.groupby("event_date", sort=False).cumcount() + 1
    refs = {
        "source_top5_gate_root": str(source_top5_gate_root),
        "source_field_repair_root": str(source_field_repair_root),
        "source_duckdb": _read_json(source_field_repair_root / "run_manifest.json")["source_duckdb"],
    }
    return frame, starter, baseline, refs


def _merge_features(starter: pd.DataFrame, source_duckdb: Path) -> pd.DataFrame:
    event_ymds = starter["event_date"].map(_event_ymd)
    symbols = sorted(starter["symbol"].astype(str).unique())
    start_ymd = int(event_ymds.min()) - 20000
    end_ymd = int(event_ymds.max())
    daily = _add_pit_features(_load_daily_ohlcv(source_duckdb, symbols, start_ymd, end_ymd))
    feature_cols = [
        "code",
        "ymd",
        "o",
        "h",
        "l",
        "c",
        "v",
        "ma7",
        "ma20",
        "ma60",
        "ma100",
        "ma200",
        "vol20",
        "high20",
        "close_above_ma20",
        "close_above_ma60",
        "ma20_rising_5d",
        "ma20_reclaim_today",
        "close_up_day",
        "volume_expansion",
        "near_20d_high",
    ]
    features = daily[feature_cols].rename(columns={"code": "symbol", "ymd": "event_ymd"}).copy()
    work = starter.copy()
    work["event_ymd"] = event_ymds
    merged = work.merge(features, on=["symbol", "event_ymd"], how="left")
    return merged


def _confirmation_flags(frame: pd.DataFrame) -> pd.DataFrame:
    out = frame.copy()
    for col in [
        "close_above_ma20",
        "close_above_ma60",
        "ma20_rising_5d",
        "ma20_reclaim_today",
        "close_up_day",
        "volume_expansion",
        "near_20d_high",
    ]:
        out[col] = out[col].fillna(False).astype(bool)
    pullback_context = out["pre_ret20_state"].astype(str).isin(["pre20_down", "pre20_strong_down"]) | out[
        "monthly_prior_state"
    ].astype(str).isin(["monthly_prior_down_or_drawdown", "monthly_prior_recovery"])
    out["shakeout_recovery_confirmed"] = pullback_context & out["close_above_ma20"] & out["close_up_day"]
    out["momentum_continuation_confirmed"] = (
        out["momentum_candidate_flag"].astype(bool)
        & out["close_above_ma20"]
        & out["close_above_ma60"]
        & (out["ma20_rising_5d"] | out["near_20d_high"])
    )
    out["volume_breakout_confirmed"] = out["volume_expansion"] & out["near_20d_high"] & out["close_above_ma20"]
    out["entry_timing_confirmed"] = (
        out["shakeout_recovery_confirmed"] | out["momentum_continuation_confirmed"] | out["volume_breakout_confirmed"]
    )
    return out


def _rate(numer: int, denom: int) -> float:
    return float(numer / denom) if denom else 0.0


def _metrics(group: pd.DataFrame) -> dict[str, Any]:
    if group.empty:
        return {
            "candidate_count": 0,
            "avg_ret20": None,
            "win_rate20": None,
            "big_winner_rate": None,
            "future_top10_rate": None,
            "severe_loss_rate20": None,
            "bad_pick_rate": None,
            "human_selectable_rate": None,
        }
    return {
        "candidate_count": int(len(group)),
        "avg_ret20": float(group["ret20_fwd"].mean()),
        "win_rate20": float(group["win20"].mean()),
        "big_winner_rate": float(group["is_big_winner_ret20_ge_10pct"].mean()),
        "future_top10_rate": float(group["is_future_top10_by_ret20"].mean()),
        "severe_loss_rate20": float(group["severe_loss20"].mean()),
        "bad_pick_rate": float(group["is_bad_pick"].mean()),
        "human_selectable_rate": float(group["human_selectable"].mean()),
    }


def _by_year(frame: pd.DataFrame) -> list[dict[str, Any]]:
    rows = []
    work = frame.assign(year=frame["event_date"].astype(str).str[:4])
    for year, group in work.groupby("year", sort=True):
        confirmed = group[group["entry_timing_confirmed"]]
        rows.append(
            {
                "year": str(year),
                "candidate_count": int(len(group)),
                "confirmed_count": int(len(confirmed)),
                "confirmed_rate": _rate(len(confirmed), len(group)),
                "all_candidates": _metrics(group),
                "confirmed_candidates": _metrics(confirmed),
            }
        )
    return rows


def _json_row(row: pd.Series) -> dict[str, Any]:
    fields = [
        "event_date",
        "symbol",
        "starter_rank",
        "_candidate_score",
        "ret20_fwd",
        "win20",
        "severe_loss20",
        "is_bad_pick",
        "human_selectable",
        "is_big_winner_ret20_ge_10pct",
        "is_future_top10_by_ret20",
        "monthly_prior_state",
        "weekly_prior_state",
        "pre_ret20_state",
        "pre_ret5_state",
        "pre_ma20_path_state",
        "momentum_candidate_flag",
        "c",
        "ma20",
        "ma60",
        "v",
        "vol20",
        "close_above_ma20",
        "close_above_ma60",
        "ma20_rising_5d",
        "ma20_reclaim_today",
        "close_up_day",
        "volume_expansion",
        "near_20d_high",
        "shakeout_recovery_confirmed",
        "momentum_continuation_confirmed",
        "volume_breakout_confirmed",
        "entry_timing_confirmed",
    ]
    out: dict[str, Any] = {}
    for field in fields:
        value = row.get(field)
        if pd.isna(value):
            out[field] = None
        elif hasattr(value, "item"):
            out[field] = value.item()
        else:
            out[field] = value
    out["starter_score"] = out.pop("_candidate_score")
    return out


def _examples(frame: pd.DataFrame) -> dict[str, Any]:
    confirmed = frame[frame["entry_timing_confirmed"]].copy()
    unconfirmed = frame[~frame["entry_timing_confirmed"]].copy()
    high_quality = confirmed.sort_values(["human_selectable", "ret20_fwd"], ascending=[False, False], kind="stable").head(20)
    shakeout = frame[frame["shakeout_recovery_confirmed"]].sort_values(["ret20_fwd"], ascending=False, kind="stable").head(20)
    false_confirmed = confirmed[confirmed["is_bad_pick"]].sort_values(["ret20_fwd"], kind="stable").head(20)
    missed_winners = unconfirmed[unconfirmed["is_big_winner_ret20_ge_10pct"]].sort_values(["ret20_fwd"], ascending=False, kind="stable").head(20)
    return {
        "high_quality_confirmed_examples": [_json_row(row) for _, row in high_quality.iterrows()],
        "shakeout_recovery_confirmed_examples": [_json_row(row) for _, row in shakeout.iterrows()],
        "false_confirmed_risk_examples": [_json_row(row) for _, row in false_confirmed.iterrows()],
        "missed_big_winner_examples": [_json_row(row) for _, row in missed_winners.iterrows()],
    }


def run(args: argparse.Namespace) -> Path:
    output_root = args.output_parent / args.run_id
    output_root.mkdir(parents=True, exist_ok=True)
    source_pretest_decision = _read_json(args.source_pretest_root / "research_decision.json")
    _frame, starter, _baseline, refs = _prepare_selection(args.source_pretest_root)
    confirmed = _confirmation_flags(_merge_features(starter, Path(refs["source_duckdb"])))
    confirmed_rows = confirmed[confirmed["entry_timing_confirmed"]]
    unconfirmed_rows = confirmed[~confirmed["entry_timing_confirmed"]]
    metrics = {
        "all_starter_candidates": _metrics(confirmed),
        "entry_timing_confirmed_candidates": _metrics(confirmed_rows),
        "entry_timing_unconfirmed_candidates": _metrics(unconfirmed_rows),
        "confirmed_candidate_count": int(len(confirmed_rows)),
        "unconfirmed_candidate_count": int(len(unconfirmed_rows)),
        "confirmed_rate": _rate(len(confirmed_rows), len(confirmed)),
        "days_with_1_plus_confirmed_candidate": int((confirmed.groupby("event_date")["entry_timing_confirmed"].sum() >= 1).sum()),
        "days_with_2_plus_confirmed_candidates": int((confirmed.groupby("event_date")["entry_timing_confirmed"].sum() >= 2).sum()),
        "days_with_3_plus_confirmed_candidates": int((confirmed.groupby("event_date")["entry_timing_confirmed"].sum() >= 3).sum()),
        "starter_day_count": int(confirmed["event_date"].nunique()),
    }
    gates = {
        "confirmed_candidates_have_better_avg_ret20": (
            metrics["entry_timing_confirmed_candidates"]["avg_ret20"] or -999
        )
        > (metrics["all_starter_candidates"]["avg_ret20"] or 999),
        "confirmed_candidates_have_lower_severe_loss": (
            metrics["entry_timing_confirmed_candidates"]["severe_loss_rate20"] or 999
        )
        <= (metrics["all_starter_candidates"]["severe_loss_rate20"] or -999),
        "confirmed_candidates_keep_or_improve_win_rate": (
            metrics["entry_timing_confirmed_candidates"]["win_rate20"] or -999
        )
        >= (metrics["all_starter_candidates"]["win_rate20"] or 999),
        "coverage_not_too_sparse": metrics["confirmed_rate"] >= 0.20,
        "has_7327_case_rows": bool((confirmed["symbol"].astype(str) == "7327").any()),
        "artifact_rows_created": len(confirmed) > 0,
    }
    if all(gates.values()):
        decision = "keep_candidate"
        authoritative = "entry_timing_confirmation_keep"
        next_axis = "manual_candidate_review_with_entry_timing_v1"
        typed_reasons = ["entry_timing_confirmation_improves_confirmed_candidate_quality"]
    else:
        decision = "hold"
        authoritative = "entry_timing_confirmation_hold"
        next_axis = "entry_timing_confirmation_rule_decomposition_v1"
        typed_reasons = ["entry_timing_confirmation_failed_gates:" + ",".join(k for k, v in gates.items() if not v)]
    symbol_7327 = confirmed[confirmed["symbol"].astype(str) == "7327"].sort_values(["event_date", "starter_rank"], kind="stable")
    generated_at = _utc_now()
    payloads: dict[str, dict[str, Any]] = {
        "entry_timing_confirmation_contract.json": {
            "schema_version": f"{SCHEMA_PREFIX}_contract_v1",
            "axis_id": AXIS_ID,
            "boundary": "TRADEX-only",
            "source_pretest_decision": source_pretest_decision.get("authoritative_research_decision"),
            "candidate_generation_changed": False,
            "top5_candidate_pool_changed": False,
            "confirmation_uses_future_labels": False,
            "future_labels_used_for_evaluation_only": True,
            "auto_select_exactly_3": False,
        },
        "run_manifest.json": {
            "schema_version": f"{SCHEMA_PREFIX}_run_manifest_v1",
            "axis_id": AXIS_ID,
            "run_id": args.run_id,
            "generated_at_utc": generated_at,
            "source_pretest_root": str(args.source_pretest_root),
            "output_root": str(output_root),
            "candidate_row_count": int(len(confirmed)),
            "source_duckdb": refs["source_duckdb"],
        },
        "source_artifact_refs.json": {
            "schema_version": f"{SCHEMA_PREFIX}_source_refs_v1",
            "source_pretest_root": str(args.source_pretest_root),
            **refs,
        },
        "confirmation_rule_report.json": {
            "schema_version": f"{SCHEMA_PREFIX}_rule_report_v1",
            "rules": {
                "shakeout_recovery_confirmed": "pullback_or_recovery_context AND close_above_ma20 AND close_up_day",
                "momentum_continuation_confirmed": "momentum_candidate AND close_above_ma20 AND close_above_ma60 AND (ma20_rising_5d OR near_20d_high)",
                "volume_breakout_confirmed": "volume_expansion AND near_20d_high AND close_above_ma20",
            },
            "uses_ohlcv_through_event_date_only": True,
            "uses_future_labels_in_confirmation": False,
            "gate_results": gates,
        },
        "confirmed_candidate_metrics.json": {
            "schema_version": f"{SCHEMA_PREFIX}_metrics_v1",
            **metrics,
        },
        "confirmation_by_year_report.json": {
            "schema_version": f"{SCHEMA_PREFIX}_by_year_report_v1",
            "rows": _by_year(confirmed),
        },
        "shakeout_recovery_case_report.json": {
            "schema_version": f"{SCHEMA_PREFIX}_shakeout_recovery_case_report_v1",
            "candidate_count": int(confirmed["shakeout_recovery_confirmed"].sum()),
            "metrics": _metrics(confirmed[confirmed["shakeout_recovery_confirmed"]]),
            "examples": _examples(confirmed)["shakeout_recovery_confirmed_examples"],
        },
        "symbol_7327_case_report.json": {
            "schema_version": f"{SCHEMA_PREFIX}_symbol_7327_case_report_v1",
            "case_count": int(len(symbol_7327)),
            "confirmed_case_count": int(symbol_7327["entry_timing_confirmed"].sum()) if not symbol_7327.empty else 0,
            "rows": [_json_row(row) for _, row in symbol_7327.iterrows()],
        },
        "manual_review_candidate_examples.json": {
            "schema_version": f"{SCHEMA_PREFIX}_manual_review_examples_v1",
            **_examples(confirmed),
        },
        "no_mutation_audit.json": {
            "schema_version": f"{SCHEMA_PREFIX}_no_mutation_audit_v1",
            "axis_id": AXIS_ID,
            "production_ranking_changed": False,
            "runtime_duckdb_written": False,
            "display_score_changed": False,
            "publish_bundle_created": False,
            "production_publish_registered": False,
            "meemee_runtime_changed": False,
            "frontend_backend_changed": False,
            "no_mutation_pass": True,
        },
        "next_axis_recommendation.json": {
            "schema_version": f"{SCHEMA_PREFIX}_next_axis_recommendation_v1",
            "axis_id": AXIS_ID,
            "decision": authoritative,
            "next": next_axis,
            "reason": "entry_timing_confirmation_review_before_any_shadow_integration",
        },
        "research_decision.json": {
            "schema_version": f"{SCHEMA_PREFIX}_research_decision_v1",
            "generated_at_utc": generated_at,
            "research_phase": "entry_timing_confirmation",
            "boundary": "TRADEX-only",
            "axis_moved": "monthly_drawdown_guarded_momentum_entry_timing_confirmation",
            "source_pretest_decision": source_pretest_decision.get("authoritative_research_decision"),
            "entry_timing_confirmation_created": True,
            "candidate_generation_changed": False,
            "top5_candidate_pool_changed": False,
            "production_ranking_changed": False,
            "publish_bundle_created": False,
            "meemee_reflectable": False,
            "future_labels_used_for_evaluation_only": True,
            "future_labels_used_in_candidate_construction": False,
            "silent_fallback_used": False,
            "research_fallback_used": False,
            "decision": decision,
            "authoritative_research_decision": authoritative,
            "typed_reasons": typed_reasons,
        },
    }
    for name, payload in payloads.items():
        _write_json(output_root / name, payload)
    _write_jsonl(output_root / "candidate_timing_confirmation_rows.jsonl", [_json_row(row) for _, row in confirmed.iterrows()])
    complete = {
        "schema_version": f"{SCHEMA_PREFIX}_artifact_complete_v1",
        "axis_id": AXIS_ID,
        "run_id": args.run_id,
        "artifact_root": str(output_root),
        "required_artifacts": REQUIRED_ARTIFACTS,
        "artifacts": {},
        "complete": False,
    }
    for name in REQUIRED_ARTIFACTS:
        path = output_root / name
        complete["artifacts"][name] = {"exists": path.exists(), "bytes": path.stat().st_size if path.exists() else 0}
    complete["complete"] = all(item["exists"] and item["bytes"] > 0 for name, item in complete["artifacts"].items() if name != "_ARTIFACT_COMPLETE.json")
    _write_json(output_root / "_ARTIFACT_COMPLETE.json", complete)
    complete["artifacts"]["_ARTIFACT_COMPLETE.json"] = {
        "exists": (output_root / "_ARTIFACT_COMPLETE.json").exists(),
        "bytes": (output_root / "_ARTIFACT_COMPLETE.json").stat().st_size,
    }
    complete["complete"] = all(item["exists"] and item["bytes"] > 0 for item in complete["artifacts"].values())
    _write_json(output_root / "_ARTIFACT_COMPLETE.json", complete)
    return output_root


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-pretest-root", type=Path, default=DEFAULT_SOURCE_PRETEST_ROOT)
    parser.add_argument("--output-parent", type=Path, default=DEFAULT_OUTPUT_PARENT)
    parser.add_argument("--run-id", default=DEFAULT_RUN_ID)
    return parser


def main() -> None:
    output_root = run(_parser().parse_args())
    print(json.dumps({"axis_id": AXIS_ID, "artifact_root": str(output_root)}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
