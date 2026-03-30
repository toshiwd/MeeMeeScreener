from __future__ import annotations

import json
import math
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from app.backend.services import rankings_cache
from app.backend.services.toredex import toredex_config, toredex_runner
from app.backend.services.tradex_experiment_store import tradex_reports_root, write_json
from app.db.session import get_conn


RANKING_BACKTEST_SCHEMA_VERSION = "ranking_backtest_v1"
RAW_RANKING_BACKTEST_SCHEMA_VERSION = "raw_ranking_backtest_v1"
TOREDEX_POLICY_BACKTEST_SCHEMA_VERSION = "toredex_policy_backtest_v1"
DEFAULT_TRADE_HORIZONS = (5, 20, 60)
DEFAULT_TOP_BUCKETS = (5, 10, 20)
DEFAULT_LIMIT = 200


@dataclass(frozen=True)
class RankingBacktestContract:
    tf: str = "D"
    which: str = "latest"
    direction: str = "up"
    mode: str = "hybrid"
    risk_mode: str = "balanced"
    limit: int = DEFAULT_LIMIT


def _utc_now_iso() -> str:
    return datetime.now(tz=timezone.utc).isoformat()


def _subtract_months(value: date, months: int) -> date:
    year = value.year
    month = value.month - int(months)
    while month <= 0:
        year -= 1
        month += 12
    day = min(value.day, _month_last_day(year, month))
    return date(year, month, day)


def _month_last_day(year: int, month: int) -> int:
    if month == 12:
        next_month = date(year + 1, 1, 1)
    else:
        next_month = date(year, month + 1, 1)
    return (next_month - timedelta(days=1)).day


def _date_expr(column: str) -> str:
    return (
        f"CASE "
        f"WHEN {column} BETWEEN 19000101 AND 20991231 THEN CAST({column} AS INTEGER) "
        f"WHEN {column} >= 1000000000000 THEN CAST(strftime(to_timestamp({column} / 1000.0), '%Y%m%d') AS INTEGER) "
        f"WHEN {column} >= 1000000000 THEN CAST(strftime(to_timestamp({column}), '%Y%m%d') AS INTEGER) "
        f"ELSE NULL END"
    )


def _ymd_to_date(value: int) -> date:
    text = str(int(value))
    return date(int(text[:4]), int(text[4:6]), int(text[6:8]))


def _date_to_ymd(value: date) -> int:
    return int(value.strftime("%Y%m%d"))


def _default_period(end_date: date | None = None) -> tuple[date, date]:
    resolved_end = end_date or _latest_trade_date()
    return _subtract_months(resolved_end, 24), resolved_end


def _latest_trade_date() -> date:
    expr = _date_expr("date")
    with get_conn() as conn:
        row = conn.execute(f"SELECT MAX({expr}) FROM daily_bars").fetchone()
    if not row or row[0] is None:
        raise RuntimeError("daily_bars has no valid trade dates")
    return _ymd_to_date(int(row[0]))


def _list_trade_dates(*, start_date: date, end_date: date) -> list[int]:
    expr = _date_expr("date")
    with get_conn() as conn:
        rows = conn.execute(
            f"""
            SELECT DISTINCT {expr} AS ymd
            FROM daily_bars
            WHERE {expr} BETWEEN ? AND ?
            ORDER BY ymd ASC
            """,
            [_date_to_ymd(start_date), _date_to_ymd(end_date)],
        ).fetchall()
    return [int(row[0]) for row in rows if row and row[0] is not None]


def _load_price_frame(
    *,
    codes: list[str],
    start_date: date,
    end_date: date,
) -> pd.DataFrame:
    if not codes:
        return pd.DataFrame(columns=["code", "ymd", "o", "c"])
    expr = _date_expr("date")
    end_plus_buffer = end_date + timedelta(days=120)
    placeholders = ",".join(["?"] * len(codes))
    sql = f"""
        SELECT
            code,
            {expr} AS ymd,
            CAST(o AS DOUBLE) AS o,
            CAST(c AS DOUBLE) AS c
        FROM daily_bars
        WHERE code IN ({placeholders})
          AND {expr} BETWEEN ? AND ?
        ORDER BY code ASC, ymd ASC
    """
    params: list[Any] = [*codes, _date_to_ymd(start_date), _date_to_ymd(end_plus_buffer)]
    with get_conn() as conn:
        frame = conn.execute(sql, params).df()
    if frame.empty:
        return frame
    frame["ymd"] = frame["ymd"].astype(int)
    return frame


def _price_lookup_from_frame(frame: pd.DataFrame) -> dict[str, dict[str, list[float] | list[int]]]:
    out: dict[str, dict[str, list[float] | list[int]]] = {}
    if frame.empty:
        return out
    for code, group in frame.groupby("code", sort=False):
        ordered = group.sort_values("ymd", kind="stable")
        out[str(code)] = {
            "dates": [int(v) for v in ordered["ymd"].tolist()],
            "opens": [float(v) if pd.notna(v) else math.nan for v in ordered["o"].tolist()],
            "closes": [float(v) if pd.notna(v) else math.nan for v in ordered["c"].tolist()],
        }
    return out


def _first_trade_index_after(dates: list[int], as_of_ymd: int) -> int | None:
    lo = 0
    hi = len(dates)
    while lo < hi:
        mid = (lo + hi) // 2
        if dates[mid] <= as_of_ymd:
            lo = mid + 1
        else:
            hi = mid
    return lo if lo < len(dates) else None


def _compute_forward_returns(
    *,
    price_lookup: dict[str, dict[str, list[float] | list[int]]],
    code: str,
    as_of_ymd: int,
    horizons: tuple[int, ...],
) -> dict[int, float | None]:
    payload = price_lookup.get(str(code))
    if not isinstance(payload, dict):
        return {h: None for h in horizons}
    dates = payload.get("dates") or []
    opens = payload.get("opens") or []
    closes = payload.get("closes") or []
    if not isinstance(dates, list) or not isinstance(opens, list) or not isinstance(closes, list):
        return {h: None for h in horizons}
    entry_idx = _first_trade_index_after([int(v) for v in dates], int(as_of_ymd))
    if entry_idx is None or entry_idx >= len(opens):
        return {h: None for h in horizons}
    entry_price = float(opens[entry_idx])
    if not math.isfinite(entry_price) or entry_price <= 0:
        return {h: None for h in horizons}
    out: dict[int, float | None] = {}
    for horizon in horizons:
        exit_idx = entry_idx + int(horizon) - 1
        if exit_idx >= len(closes):
            out[int(horizon)] = None
            continue
        exit_price = float(closes[exit_idx])
        if not math.isfinite(exit_price):
            out[int(horizon)] = None
            continue
        out[int(horizon)] = float(exit_price / entry_price - 1.0)
    return out


def _safe_float(value: Any) -> float | None:
    try:
        if value is None:
            return None
        out = float(value)
    except Exception:
        return None
    return out if math.isfinite(out) else None


def _build_daily_selection_rows(
    *,
    trade_dates: list[int],
    contract: RankingBacktestContract,
) -> tuple[list[dict[str, Any]], list[str]]:
    rows: list[dict[str, Any]] = []
    code_universe: set[str] = set()
    for as_of_ymd in trade_dates:
        payload = rankings_cache.get_rankings_asof(
            contract.tf,
            contract.which,
            contract.direction,
            contract.limit,
            as_of=as_of_ymd,
            mode=contract.mode,
            risk_mode=contract.risk_mode,
        )
        items = payload.get("items") if isinstance(payload, dict) else []
        if not isinstance(items, list) or not items:
            continue
        for rank, item in enumerate(items, start=1):
            if not isinstance(item, dict):
                continue
            code = str(item.get("code") or "").strip()
            if not code:
                continue
            code_universe.add(code)
            rows.append(
                {
                    "as_of": int(as_of_ymd),
                    "as_of_iso": _ymd_to_date(int(as_of_ymd)).isoformat(),
                    "rank": int(rank),
                    "code": code,
                    "entryQualified": bool(item.get("entryQualified") is True),
                    "entryScore": _safe_float(item.get("entryScore")),
                    "hybridScore": _safe_float(item.get("hybridScore")),
                    "setupType": str(item.get("setupType") or "").strip() or None,
                    "predDt": payload.get("pred_dt"),
                }
            )
    return rows, sorted(code_universe)


def _attach_forward_returns(
    panel: pd.DataFrame,
    *,
    price_lookup: dict[str, dict[str, list[float] | list[int]]],
    horizons: tuple[int, ...],
) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for record in panel.to_dict(orient="records"):
        returns = _compute_forward_returns(
            price_lookup=price_lookup,
            code=str(record.get("code") or ""),
            as_of_ymd=int(record.get("as_of")),
            horizons=horizons,
        )
        for horizon in horizons:
            record[f"forward_return_{horizon}"] = returns.get(int(horizon))
        rows.append(record)
    return pd.DataFrame(rows)


def _bucket_panel(
    panel: pd.DataFrame,
    *,
    bucket_size: int,
    entry_only: bool,
    bottom: bool = False,
) -> pd.DataFrame:
    if panel.empty:
        return panel.copy()
    frame = panel.copy()
    if entry_only:
        frame = frame[frame["entryQualified"] == True].copy()  # noqa: E712
    if frame.empty:
        return frame
    order = [True, False] if bottom else [True, True]
    frame = frame.sort_values(["as_of", "rank"], ascending=order, kind="stable")
    return frame.groupby("as_of", sort=False).head(int(bucket_size)).copy()


def _bucket_sets(panel: pd.DataFrame) -> dict[int, set[str]]:
    if panel.empty:
        return {}
    out: dict[int, set[str]] = {}
    for as_of, group in panel.groupby("as_of", sort=True):
        out[int(as_of)] = {str(code) for code in group["code"].tolist() if str(code).strip()}
    return out


def _mean(values: list[float]) -> float | None:
    if not values:
        return None
    return float(sum(values) / len(values))


def _compute_overlap_metrics(panel: pd.DataFrame) -> dict[str, Any]:
    sets_by_day = _bucket_sets(panel)
    if len(sets_by_day) <= 1:
        return {"daily_overlap_rate": None, "daily_turnover_rate": None}
    overlap_values: list[float] = []
    turnover_values: list[float] = []
    previous: set[str] | None = None
    for _, current in sorted(sets_by_day.items()):
        if previous is None:
            previous = current
            continue
        denom = max(len(previous), len(current), 1)
        overlap = len(previous & current) / float(denom)
        overlap_values.append(float(overlap))
        turnover_values.append(float(1.0 - overlap))
        previous = current
    return {
        "daily_overlap_rate": _mean(overlap_values),
        "daily_turnover_rate": _mean(turnover_values),
    }


def _forward_metric_bundle(panel: pd.DataFrame, *, horizons: tuple[int, ...]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for horizon in horizons:
        column = f"forward_return_{int(horizon)}"
        series = panel[column].dropna() if column in panel.columns else pd.Series(dtype=float)
        values = [float(v) for v in series.tolist() if pd.notna(v)]
        out[f"mean_forward_return_{int(horizon)}"] = _mean(values)
    twenty = panel["forward_return_20"].dropna() if "forward_return_20" in panel.columns else pd.Series(dtype=float)
    twenty_values = [float(v) for v in twenty.tolist() if pd.notna(v)]
    out["hit_rate_20"] = (
        float(sum(1 for value in twenty_values if value > 0.0) / len(twenty_values))
        if twenty_values
        else None
    )
    out["sample_count_20"] = int(len(twenty_values))
    return out


def _build_raw_cohort_metrics(
    panel: pd.DataFrame,
    *,
    horizons: tuple[int, ...],
    top_buckets: tuple[int, ...],
) -> tuple[dict[str, Any], dict[str, Any]]:
    cohorts: dict[str, Any] = {}
    benchmarks: dict[str, Any] = {}
    all_ranked = panel.groupby("as_of", sort=False).head(DEFAULT_LIMIT).copy()
    all_ranked_metrics = _forward_metric_bundle(all_ranked, horizons=horizons)
    benchmarks["all_ranked_equal_weight"] = {
        **all_ranked_metrics,
        **_compute_overlap_metrics(all_ranked),
        "sample_count": int(len(all_ranked)),
    }

    for bucket in top_buckets:
        for entry_only in (False, True):
            name = f"top{bucket}_entryQualified" if entry_only else f"top{bucket}"
            cohort = _bucket_panel(panel, bucket_size=bucket, entry_only=entry_only, bottom=False)
            bottom = _bucket_panel(panel, bucket_size=bucket, entry_only=False, bottom=True)
            cohort_metrics = _forward_metric_bundle(cohort, horizons=horizons)
            bottom_metrics = _forward_metric_bundle(bottom, horizons=horizons)
            cohort_metrics.update(_compute_overlap_metrics(cohort))
            cohort_metrics["sample_count"] = int(len(cohort))
            cohort_metrics["daily_count"] = int(cohort["as_of"].nunique()) if not cohort.empty else 0
            cohort_metrics["lift_vs_all_ranked"] = (
                None
                if cohort_metrics.get("mean_forward_return_20") is None or all_ranked_metrics.get("mean_forward_return_20") is None
                else float(cohort_metrics["mean_forward_return_20"] - all_ranked_metrics["mean_forward_return_20"])
            )
            cohort_metrics["lift_vs_bottom_bucket"] = (
                None
                if cohort_metrics.get("mean_forward_return_20") is None or bottom_metrics.get("mean_forward_return_20") is None
                else float(cohort_metrics["mean_forward_return_20"] - bottom_metrics["mean_forward_return_20"])
            )
            cohorts[name] = cohort_metrics
            benchmarks[f"bottom_symmetric_bucket_{bucket}"] = {
                **bottom_metrics,
                **_compute_overlap_metrics(bottom),
                "sample_count": int(len(bottom)),
            }
    return cohorts, benchmarks


def _build_coverage_metrics(panel: pd.DataFrame, *, horizons: tuple[int, ...]) -> dict[str, Any]:
    out: dict[str, Any] = {
        "daily_count": int(panel["as_of"].nunique()) if not panel.empty else 0,
        "ranked_sample_count": int(len(panel)),
        "ranked_code_count": int(panel["code"].nunique()) if not panel.empty else 0,
    }
    for horizon in horizons:
        column = f"forward_return_{int(horizon)}"
        if column not in panel.columns or panel.empty:
            out[f"horizon_{horizon}_coverage"] = 0.0
            continue
        out[f"horizon_{horizon}_coverage"] = float(panel[column].notna().mean())
    return out


def _render_raw_markdown(payload: dict[str, Any]) -> str:
    lines = [
        "# Raw Ranking Backtest",
        "",
        f"- generated_at: {payload.get('generated_at')}",
        f"- period: {payload.get('period', {}).get('start_date')} .. {payload.get('period', {}).get('end_date')}",
        f"- daily_count: {payload.get('daily_count')}",
        "",
        "## Cohorts",
    ]
    cohort_metrics = payload.get("cohort_metrics") if isinstance(payload.get("cohort_metrics"), dict) else {}
    for name, metrics in cohort_metrics.items():
        if not isinstance(metrics, dict):
            continue
        lines.append(
            f"- {name}: mean20={metrics.get('mean_forward_return_20')}, hit20={metrics.get('hit_rate_20')}, "
            f"lift20={metrics.get('lift_vs_all_ranked')}, turnover={metrics.get('daily_turnover_rate')}"
        )
    lines.append("")
    return "\n".join(lines)


def _write_panel_parquet(path: Path, panel: pd.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    panel.to_parquet(path, index=False)


def run_raw_ranking_backtest(
    *,
    start_date: date | None = None,
    end_date: date | None = None,
    output_dir: Path | None = None,
    contract: RankingBacktestContract | None = None,
) -> dict[str, Any]:
    ranking_contract = contract or RankingBacktestContract()
    resolved_start, resolved_end = (
        (start_date, end_date) if start_date is not None and end_date is not None else _default_period(end_date=end_date)
    )
    if resolved_start is None or resolved_end is None:
        raise RuntimeError("ranking backtest period could not be resolved")
    trade_dates = _list_trade_dates(start_date=resolved_start, end_date=resolved_end)
    daily_rows, codes = _build_daily_selection_rows(trade_dates=trade_dates, contract=ranking_contract)
    panel = pd.DataFrame(daily_rows)
    if panel.empty:
        raise RuntimeError("no ranking rows returned for requested period")
    price_frame = _load_price_frame(codes=codes, start_date=resolved_start, end_date=resolved_end)
    price_lookup = _price_lookup_from_frame(price_frame)
    panel = _attach_forward_returns(panel, price_lookup=price_lookup, horizons=DEFAULT_TRADE_HORIZONS)
    cohort_metrics, benchmark_metrics = _build_raw_cohort_metrics(
        panel,
        horizons=DEFAULT_TRADE_HORIZONS,
        top_buckets=DEFAULT_TOP_BUCKETS,
    )
    coverage_metrics = _build_coverage_metrics(panel, horizons=DEFAULT_TRADE_HORIZONS)
    turnover_metrics = {
        name: {
            "daily_overlap_rate": metrics.get("daily_overlap_rate"),
            "daily_turnover_rate": metrics.get("daily_turnover_rate"),
        }
        for name, metrics in cohort_metrics.items()
        if isinstance(metrics, dict)
    }
    payload = {
        "schema_version": RAW_RANKING_BACKTEST_SCHEMA_VERSION,
        "generated_at": _utc_now_iso(),
        "ranking_contract": {
            "tf": ranking_contract.tf,
            "which": ranking_contract.which,
            "direction": ranking_contract.direction,
            "mode": ranking_contract.mode,
            "risk_mode": ranking_contract.risk_mode,
            "limit": int(ranking_contract.limit),
        },
        "period": {
            "start_date": resolved_start.isoformat(),
            "end_date": resolved_end.isoformat(),
        },
        "daily_count": int(panel["as_of"].nunique()),
        "cohort_metrics": cohort_metrics,
        "benchmark_metrics": benchmark_metrics,
        "turnover_metrics": turnover_metrics,
        "coverage_metrics": coverage_metrics,
    }
    if output_dir is not None:
        output_dir.mkdir(parents=True, exist_ok=True)
        write_json(output_dir / "raw_ranking_backtest.json", payload)
        (output_dir / "raw_ranking_backtest.md").write_text(_render_raw_markdown(payload), encoding="utf-8")
        _write_panel_parquet(output_dir / "daily_selection_panel.parquet", panel)
    return payload


def _render_toredex_markdown(payload: dict[str, Any]) -> str:
    return "\n".join(
        [
            "# ToreDex Policy Backtest",
            "",
            f"- generated_at: {payload.get('generated_at')}",
            f"- season_id: {payload.get('season_id')}",
            f"- period: {payload.get('period', {}).get('start_date')} .. {payload.get('period', {}).get('end_date')}",
            f"- processed_days: {payload.get('processed_days')}",
            f"- risk_gate_pass: {((payload.get('risk_gate') or {}).get('pass'))}",
            "",
        ]
    )


def run_toredex_policy_backtest(
    *,
    start_date: date,
    end_date: date,
    output_dir: Path | None = None,
) -> dict[str, Any]:
    season_id = f"ranking_backtest_{datetime.now(tz=timezone.utc).strftime('%Y%m%d%H%M%S')}"
    config_override = {
        "mode": "BACKTEST",
        "rankingMode": "hybrid",
        "sides": {
            "longEnabled": True,
            "shortEnabled": False,
        },
    }
    config_hash = toredex_config.load_toredex_config(override=config_override).config_hash
    result = toredex_runner.run_backtest(
        season_id=season_id,
        start_date=start_date.isoformat(),
        end_date=end_date.isoformat(),
        dry_run=False,
        config_override=config_override,
    )
    payload = {
        "schema_version": TOREDEX_POLICY_BACKTEST_SCHEMA_VERSION,
        "generated_at": _utc_now_iso(),
        "season_id": season_id,
        "policy_config_hash": str(config_hash),
        "period": {
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
        },
        "processed_days": int(result.get("processed_days") or 0),
        "final_metrics": result.get("final_metrics"),
        "performance_breakdown": result.get("performance_breakdown"),
        "risk_gate": result.get("risk_gate"),
        "rollup": result.get("rollup"),
        "reason_counts": result.get("reason_counts"),
        "status_counts": result.get("status_counts"),
        "stop_reason": result.get("stop_reason"),
    }
    if output_dir is not None:
        output_dir.mkdir(parents=True, exist_ok=True)
        write_json(output_dir / "toredex_policy_backtest.json", payload)
        (output_dir / "toredex_policy_backtest.md").write_text(_render_toredex_markdown(payload), encoding="utf-8")
    return payload


def _raw_summary(raw_payload: dict[str, Any]) -> dict[str, Any]:
    cohort_metrics = raw_payload.get("cohort_metrics") if isinstance(raw_payload.get("cohort_metrics"), dict) else {}
    top10 = cohort_metrics.get("top10") if isinstance(cohort_metrics.get("top10"), dict) else {}
    top10_entry = (
        cohort_metrics.get("top10_entryQualified")
        if isinstance(cohort_metrics.get("top10_entryQualified"), dict)
        else {}
    )
    return {
        "top10": {
            "mean_forward_return_5": top10.get("mean_forward_return_5"),
            "mean_forward_return_20": top10.get("mean_forward_return_20"),
            "mean_forward_return_60": top10.get("mean_forward_return_60"),
            "hit_rate_20": top10.get("hit_rate_20"),
            "lift_vs_all_ranked": top10.get("lift_vs_all_ranked"),
            "lift_vs_bottom_bucket": top10.get("lift_vs_bottom_bucket"),
            "daily_overlap_rate": top10.get("daily_overlap_rate"),
            "daily_turnover_rate": top10.get("daily_turnover_rate"),
        },
        "top10_entryQualified": {
            "mean_forward_return_5": top10_entry.get("mean_forward_return_5"),
            "mean_forward_return_20": top10_entry.get("mean_forward_return_20"),
            "mean_forward_return_60": top10_entry.get("mean_forward_return_60"),
            "hit_rate_20": top10_entry.get("hit_rate_20"),
            "lift_vs_all_ranked": top10_entry.get("lift_vs_all_ranked"),
            "lift_vs_bottom_bucket": top10_entry.get("lift_vs_bottom_bucket"),
            "daily_overlap_rate": top10_entry.get("daily_overlap_rate"),
            "daily_turnover_rate": top10_entry.get("daily_turnover_rate"),
        },
    }


def _classify_summary(*, raw_payload: dict[str, Any], toredex_payload: dict[str, Any]) -> str:
    cohort_metrics = raw_payload.get("cohort_metrics") if isinstance(raw_payload.get("cohort_metrics"), dict) else {}
    top10 = cohort_metrics.get("top10") if isinstance(cohort_metrics.get("top10"), dict) else {}
    top10_entry = (
        cohort_metrics.get("top10_entryQualified")
        if isinstance(cohort_metrics.get("top10_entryQualified"), dict)
        else {}
    )
    top10_lift = _safe_float(top10.get("lift_vs_all_ranked"))
    top10_entry_lift = _safe_float(top10_entry.get("lift_vs_all_ranked"))
    risk_gate_pass = bool(((toredex_payload.get("risk_gate") or {}).get("pass")) is True)
    raw_positive = bool(
        (top10_lift is not None and top10_lift > 0.0)
        or (top10_entry_lift is not None and top10_entry_lift > 0.0)
    )
    if raw_positive and risk_gate_pass:
        return "usable"
    if raw_positive:
        return "watch"
    return "not_usable_yet"


def _render_summary_markdown(payload: dict[str, Any]) -> str:
    raw_top10 = ((payload.get("raw_ranking") or {}).get("top10")) or {}
    raw_top10_entry = ((payload.get("raw_ranking") or {}).get("top10_entryQualified")) or {}
    toredex = payload.get("toredex_policy") or {}
    return "\n".join(
        [
            "# Ranking Backtest Summary",
            "",
            f"- generated_at: {payload.get('generated_at')}",
            f"- disposition: {payload.get('disposition')}",
            f"- period: {payload.get('period', {}).get('start_date')} .. {payload.get('period', {}).get('end_date')}",
            "",
            "## Raw Ranking",
            f"- top10 mean20: {raw_top10.get('mean_forward_return_20')}",
            f"- top10 lift20: {raw_top10.get('lift_vs_all_ranked')}",
            f"- top10_entryQualified mean20: {raw_top10_entry.get('mean_forward_return_20')}",
            f"- top10_entryQualified lift20: {raw_top10_entry.get('lift_vs_all_ranked')}",
            "",
            "## ToreDex",
            f"- risk_gate_pass: {((toredex.get('risk_gate') or {}).get('pass'))}",
            f"- total_return_pct: {toredex.get('total_return_pct')}",
            f"- worst_month_pct: {((toredex.get('rollup') or {}).get('worst_month_pct'))}",
            f"- max_turnover_pct_per_month: {((toredex.get('rollup') or {}).get('max_turnover_pct_per_month'))}",
            "",
        ]
    )


def _report_root(run_id: str) -> Path:
    path = tradex_reports_root() / "ranking_backtests" / str(run_id)
    path.mkdir(parents=True, exist_ok=True)
    return path


def _extract_toredex_total_return_pct(toredex_payload: dict[str, Any]) -> float | None:
    performance_breakdown = toredex_payload.get("performance_breakdown")
    if isinstance(performance_breakdown, dict):
        direct = _safe_float(performance_breakdown.get("total_return_pct"))
        if direct is not None:
            return direct
        net = performance_breakdown.get("net")
        if isinstance(net, dict):
            nested = _safe_float(net.get("cum_return_pct"))
            if nested is not None:
                return nested
    final_metrics = toredex_payload.get("final_metrics")
    if isinstance(final_metrics, dict):
        fallback = _safe_float(final_metrics.get("cum_return_pct"))
        if fallback is not None:
            return fallback
    return None


def run_ranking_backtest(
    *,
    start_date: date | None = None,
    end_date: date | None = None,
    output_dir: Path | None = None,
) -> dict[str, Any]:
    resolved_start, resolved_end = (
        (start_date, end_date) if start_date is not None and end_date is not None else _default_period(end_date=end_date)
    )
    if resolved_start is None or resolved_end is None:
        raise RuntimeError("ranking backtest period could not be resolved")
    run_id = f"ranking_backtest_{datetime.now(tz=timezone.utc).strftime('%Y%m%d%H%M%S')}"
    root = output_dir or _report_root(run_id)
    raw_payload = run_raw_ranking_backtest(
        start_date=resolved_start,
        end_date=resolved_end,
        output_dir=root,
    )
    toredex_payload = run_toredex_policy_backtest(
        start_date=resolved_start,
        end_date=resolved_end,
        output_dir=root,
    )
    summary_payload = {
        "schema_version": RANKING_BACKTEST_SCHEMA_VERSION,
        "generated_at": _utc_now_iso(),
        "run_id": run_id,
        "period": {
            "start_date": resolved_start.isoformat(),
            "end_date": resolved_end.isoformat(),
        },
        "ranking_contract": raw_payload.get("ranking_contract"),
        "raw_ranking": _raw_summary(raw_payload),
        "toredex_policy": {
            "final_equity": ((toredex_payload.get("final_metrics") or {}).get("equity")),
            "total_return_pct": _extract_toredex_total_return_pct(toredex_payload),
            "worst_month_pct": ((toredex_payload.get("rollup") or {}).get("worst_month_pct")),
            "max_turnover_pct_per_month": ((toredex_payload.get("rollup") or {}).get("max_turnover_pct_per_month")),
            "risk_gate": toredex_payload.get("risk_gate"),
        },
    }
    summary_payload["disposition"] = _classify_summary(
        raw_payload=raw_payload,
        toredex_payload=toredex_payload,
    )
    write_json(root / "ranking_backtest_summary.json", summary_payload)
    (root / "ranking_backtest_summary.md").write_text(_render_summary_markdown(summary_payload), encoding="utf-8")
    return {
        "run_id": run_id,
        "output_dir": str(root),
        "raw": raw_payload,
        "toredex": toredex_payload,
        "summary": summary_payload,
    }
