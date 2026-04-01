from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from app.backend.services.toredex import toredex_config, toredex_runner
from app.backend.services.toredex.toredex_self_improve import _compute_cut_loss_exit_share, _compute_gate_ng_exit_share
from app.backend.services.tradex_experiment_store import tradex_reports_root, write_json
from app.db.session import get_conn


TOREDEX_POLICY_DIAGNOSIS_SCHEMA_VERSION = "toredex_policy_diagnosis_v1"


def _utc_now_iso() -> str:
    return datetime.now(tz=timezone.utc).isoformat()


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


def _subtract_months(value: date, months: int) -> date:
    year = value.year
    month = value.month - int(months)
    while month <= 0:
        year -= 1
        month += 12
    day = min(value.day, _month_last_day(year, month))
    return date(year, month, day)


def _latest_trade_date() -> date:
    expr = _date_expr("date")
    with get_conn() as conn:
        row = conn.execute(f"SELECT MAX({expr}) FROM daily_bars").fetchone()
    if not row or row[0] is None:
        raise RuntimeError("daily_bars has no valid trade dates")
    text = str(int(row[0]))
    return date(int(text[:4]), int(text[4:6]), int(text[6:8]))


def _default_period(end_date: date | None = None) -> tuple[date, date]:
    resolved_end = end_date or _latest_trade_date()
    return _subtract_months(resolved_end, 24), resolved_end


def _parse_optional_date(value: str | date | None) -> date | None:
    if value is None or isinstance(value, date):
        return value
    text = str(value).strip()
    if not text:
        return None
    for fmt in ("%Y-%m-%d", "%Y%m%d", "%Y/%m/%d"):
        try:
            return datetime.strptime(text, fmt).date()
        except ValueError:
            continue
    raise ValueError("date must be YYYY-MM-DD or YYYYMMDD")


def _variant_overrides() -> dict[str, dict[str, Any]]:
    return {
        "base_current": {
            "rankingMode": "hybrid",
            "sides": {"longEnabled": True, "shortEnabled": False},
        },
        "holdings_1": {
            "rankingMode": "hybrid",
            "sides": {"longEnabled": True, "shortEnabled": False},
            "maxHoldings": 1,
            "portfolioConstraints": {"maxNetUnits": 1},
            "riskGates": {"champion": {"maxNetExposureUnits": 1}},
        },
        "holdings_2": {
            "rankingMode": "hybrid",
            "sides": {"longEnabled": True, "shortEnabled": False},
            "maxHoldings": 2,
            "portfolioConstraints": {"maxNetUnits": 2},
            "riskGates": {"champion": {"maxNetExposureUnits": 2}},
        },
        "turnover_tight": {
            "rankingMode": "hybrid",
            "sides": {"longEnabled": True, "shortEnabled": False},
            "thresholds": {
                "maxNewEntriesPerDay": 1.0,
                "switchMinEvGap": 0.08,
                "exitGateNgMinHoldingDays": 13.0,
            },
        },
        "gate_disabled_for_diagnosis": {
            "rankingMode": "hybrid",
            "sides": {"longEnabled": True, "shortEnabled": False},
            "riskGates": {"champion": {"enabled": False}},
        },
    }


def _query_holdings_count_max(season_id: str) -> int:
    with get_conn() as conn:
        row = conn.execute(
            """
            SELECT MAX(holdings_count)
            FROM toredex_daily_metrics
            WHERE season_id = ?
            """,
            [str(season_id)],
        ).fetchone()
    if not row or row[0] is None:
        return 0
    return int(row[0])


def _metric_improvement(base_value: float | None, candidate_value: float | None) -> float:
    if base_value is None or candidate_value is None:
        return 0.0
    return float(candidate_value) - float(base_value)


def _diagnosis_score(base_row: dict[str, Any], candidate_row: dict[str, Any]) -> float:
    return (
        _metric_improvement(base_row.get("total_return_pct"), candidate_row.get("total_return_pct"))
        + 0.5 * _metric_improvement(base_row.get("max_drawdown_pct"), candidate_row.get("max_drawdown_pct"))
        + 0.25 * _metric_improvement(base_row.get("worst_month_pct"), candidate_row.get("worst_month_pct"))
    )


def _resolve_primary_failure_axis(rows: list[dict[str, Any]]) -> str:
    row_map = {str(row["variant_name"]): row for row in rows}
    base_row = row_map["base_current"]
    holdings_score = max(
        _diagnosis_score(base_row, row_map["holdings_1"]),
        _diagnosis_score(base_row, row_map["holdings_2"]),
    )
    turnover_score = _diagnosis_score(base_row, row_map["turnover_tight"])
    risk_gate_score = _diagnosis_score(base_row, row_map["gate_disabled_for_diagnosis"])
    axis_scores = {
        "holdings": float(holdings_score),
        "turnover": float(turnover_score),
        "risk_gate_only": float(risk_gate_score),
    }
    best_axis = max(axis_scores, key=axis_scores.get)
    if float(axis_scores[best_axis]) >= 1.0:
        return str(best_axis)
    if float(base_row.get("cut_loss_exit_share_pct") or 0.0) >= float(base_row.get("gate_ng_exit_share_pct") or 0.0):
        return "exit_policy"
    return "risk_gate_only"


def _render_markdown(payload: dict[str, Any]) -> str:
    lines = [
        "# ToreDex Policy Diagnosis",
        "",
        "## Summary",
        f"- period: `{payload['period']['start_date']}` to `{payload['period']['end_date']}`",
        f"- primary_failure_axis: `{payload['primary_failure_axis']}`",
        "",
        "## Variant Results",
    ]
    for row in payload["variant_results"]:
        lines.append(
            f"- `{row['variant_name']}`: total_return_pct=`{row['total_return_pct']}`, max_drawdown_pct=`{row['max_drawdown_pct']}`, worst_month_pct=`{row['worst_month_pct']}`, risk_gate_pass=`{row['risk_gate_pass']}`"
        )
    return "\n".join(lines).strip() + "\n"


def run_toredex_policy_diagnosis(
    *,
    start_date: str | date | None = None,
    end_date: str | date | None = None,
    output_dir: Path | None = None,
) -> dict[str, Any]:
    resolved_end = _parse_optional_date(end_date) or _latest_trade_date()
    resolved_start = _parse_optional_date(start_date)
    if resolved_start is None:
        resolved_start, resolved_end = _default_period(resolved_end)
    if resolved_end < resolved_start:
        raise ValueError("end_date must be >= start_date")

    run_id = f"toredex_policy_diagnosis_{datetime.now(tz=timezone.utc).strftime('%Y%m%d%H%M%S')}"
    root = output_dir or (tradex_reports_root() / "toredex_policy_diagnosis" / run_id)
    root.mkdir(parents=True, exist_ok=True)

    variant_results: list[dict[str, Any]] = []
    for variant_name, override in _variant_overrides().items():
        season_id = f"diagnosis_{variant_name}_{datetime.now(tz=timezone.utc).strftime('%Y%m%d%H%M%S%f')}"
        config_hash = toredex_config.load_toredex_config(override=override).config_hash
        result = toredex_runner.run_backtest(
            season_id=season_id,
            start_date=resolved_start.isoformat(),
            end_date=resolved_end.isoformat(),
            config_override=override,
        )
        final_metrics = result.get("final_metrics") if isinstance(result.get("final_metrics"), dict) else {}
        rollup = result.get("rollup") if isinstance(result.get("rollup"), dict) else {}
        risk_gate = result.get("risk_gate") if isinstance(result.get("risk_gate"), dict) else {}
        reason_counts = result.get("reason_counts") if isinstance(result.get("reason_counts"), dict) else {}
        variant_results.append(
            {
                "variant_name": str(variant_name),
                "season_id": str(season_id),
                "policy_config_hash": str(config_hash),
                "processed_days": int(result.get("processed_days") or 0),
                "final_equity": float(final_metrics.get("equity")) if final_metrics.get("equity") is not None else None,
                "total_return_pct": float(final_metrics.get("net_cum_return_pct")) if final_metrics.get("net_cum_return_pct") is not None else None,
                "max_drawdown_pct": float(final_metrics.get("max_drawdown_pct")) if final_metrics.get("max_drawdown_pct") is not None else None,
                "worst_month_pct": float(rollup.get("worst_month_pct")) if rollup.get("worst_month_pct") is not None else None,
                "max_turnover_pct_per_month": float(rollup.get("max_turnover_pct_per_month")) if rollup.get("max_turnover_pct_per_month") is not None else None,
                "risk_gate_pass": bool(risk_gate.get("pass")),
                "risk_gate_reason": str(risk_gate.get("reason") or ""),
                "reason_counts": reason_counts,
                "cut_loss_exit_share_pct": float(round(_compute_cut_loss_exit_share(reason_counts) * 100.0, 6)),
                "gate_ng_exit_share_pct": float(round(_compute_gate_ng_exit_share(reason_counts) * 100.0, 6)),
                "holdings_count_max": int(_query_holdings_count_max(season_id)),
            }
        )

    payload = {
        "schema_version": TOREDEX_POLICY_DIAGNOSIS_SCHEMA_VERSION,
        "generated_at": _utc_now_iso(),
        "period": {
            "start_date": resolved_start.isoformat(),
            "end_date": resolved_end.isoformat(),
        },
        "ranking_contract": {
            "tf": "D",
            "which": "latest",
            "direction": "up",
            "mode": "hybrid",
            "risk_mode": "balanced",
            "long_only": True,
        },
        "variant_results": variant_results,
        "primary_failure_axis": _resolve_primary_failure_axis(variant_results),
    }

    json_path = root / "toredex_policy_diagnosis.json"
    md_path = root / "toredex_policy_diagnosis.md"
    write_json(json_path, payload)
    md_path.write_text(_render_markdown(payload), encoding="utf-8")
    return {
        "run_id": run_id,
        "toredex_policy_diagnosis_path": str(json_path),
        "toredex_policy_diagnosis_report_path": str(md_path),
        "primary_failure_axis": str(payload["primary_failure_axis"]),
    }
