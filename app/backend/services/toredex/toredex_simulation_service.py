from __future__ import annotations

from datetime import date
from typing import Any

from app.db.session import get_conn


DEFAULT_PRINCIPAL_JPY = 10_000_000
DEFAULT_LIMIT = 30
MAX_LIMIT = 200
MIN_METRIC_DAYS = 200
SEASON_ID_PATTERN = "%validate%"


def _to_iso_date(value: object) -> str | None:
    if isinstance(value, date):
        return value.isoformat()
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _calc_final_and_gain(*, principal_jpy: int, net_cum_return_pct: float) -> tuple[int, int]:
    final_jpy = int(round(float(principal_jpy) * (1.0 + float(net_cum_return_pct) / 100.0)))
    gain_jpy = int(final_jpy - int(principal_jpy))
    return final_jpy, gain_jpy


def _median(values: list[float]) -> float | None:
    if not values:
        return None
    ordered = sorted(float(v) for v in values)
    n = len(ordered)
    mid = n // 2
    if n % 2 == 1:
        return float(ordered[mid])
    return float((ordered[mid - 1] + ordered[mid]) / 2.0)


def _summary_point(
    *,
    principal_jpy: int,
    net_cum_return_pct: float | None,
    season_id: str | None = None,
) -> dict[str, Any]:
    if net_cum_return_pct is None:
        return {
            "season_id": season_id,
            "net_cum_return_pct": None,
            "final_jpy": None,
            "gain_jpy": None,
        }
    final_jpy, gain_jpy = _calc_final_and_gain(
        principal_jpy=principal_jpy,
        net_cum_return_pct=float(net_cum_return_pct),
    )
    return {
        "season_id": season_id,
        "net_cum_return_pct": float(net_cum_return_pct),
        "final_jpy": int(final_jpy),
        "gain_jpy": int(gain_jpy),
    }


def _fetch_filtered_rows() -> list[dict[str, Any]]:
    query = """
        WITH latest AS (
            SELECT
                season_id,
                "asOf" AS as_of,
                net_cum_return_pct,
                max_drawdown_pct,
                risk_gate_pass,
                ROW_NUMBER() OVER (PARTITION BY season_id ORDER BY "asOf" DESC) AS rn
            FROM toredex_daily_metrics
        ),
        metric_days AS (
            SELECT
                season_id,
                COUNT(*) AS metric_days
            FROM toredex_daily_metrics
            GROUP BY season_id
        ),
        trade_counts AS (
            SELECT
                season_id,
                COUNT(*) AS trades
            FROM toredex_trades
            GROUP BY season_id
        )
        SELECT
            s.season_id,
            s.start_date,
            COALESCE(s.end_date, l.as_of) AS end_date,
            md.metric_days,
            COALESCE(tc.trades, 0) AS trades,
            l.net_cum_return_pct,
            l.max_drawdown_pct
        FROM toredex_seasons s
        JOIN latest l ON s.season_id = l.season_id AND l.rn = 1
        JOIN metric_days md ON s.season_id = md.season_id
        LEFT JOIN trade_counts tc ON s.season_id = tc.season_id
        WHERE lower(s.season_id) LIKE lower(?)
          AND l.risk_gate_pass = TRUE
          AND md.metric_days >= ?
        ORDER BY l.net_cum_return_pct DESC, s.season_id ASC
    """
    with get_conn() as conn:
        rows = conn.execute(query, [SEASON_ID_PATTERN, MIN_METRIC_DAYS]).fetchall()

    out: list[dict[str, Any]] = []
    for row in rows:
        out.append(
            {
                "season_id": str(row[0]),
                "start_date": _to_iso_date(row[1]),
                "end_date": _to_iso_date(row[2]),
                "metric_days": int(row[3]) if row[3] is not None else 0,
                "trades": int(row[4]) if row[4] is not None else 0,
                "net_cum_return_pct": float(row[5]) if row[5] is not None else 0.0,
                "max_drawdown_pct": float(row[6]) if row[6] is not None else None,
            }
        )
    return out


def get_validate_simulation(
    principal_jpy: int = DEFAULT_PRINCIPAL_JPY,
    limit: int = DEFAULT_LIMIT,
) -> dict[str, Any]:
    resolved_principal = max(0, int(principal_jpy))
    resolved_limit = max(1, min(MAX_LIMIT, int(limit)))

    filtered = _fetch_filtered_rows()
    enriched: list[dict[str, Any]] = []
    for row in filtered:
        pct = float(row["net_cum_return_pct"])
        final_jpy, gain_jpy = _calc_final_and_gain(
            principal_jpy=resolved_principal,
            net_cum_return_pct=pct,
        )
        enriched.append(
            {
                **row,
                "final_jpy": int(final_jpy),
                "gain_jpy": int(gain_jpy),
            }
        )

    pcts = [float(row["net_cum_return_pct"]) for row in enriched]
    avg_pct = (sum(pcts) / len(pcts)) if pcts else None
    med_pct = _median(pcts)
    best_item = enriched[0] if enriched else None
    worst_item = enriched[-1] if enriched else None

    return {
        "principal_jpy": int(resolved_principal),
        "filters": {
            "season_id_like": SEASON_ID_PATTERN,
            "risk_gate_pass": True,
            "min_metric_days": int(MIN_METRIC_DAYS),
            "limit": int(resolved_limit),
        },
        "summary": {
            "count": int(len(enriched)),
            "avg": _summary_point(
                principal_jpy=resolved_principal,
                net_cum_return_pct=avg_pct,
            ),
            "median": _summary_point(
                principal_jpy=resolved_principal,
                net_cum_return_pct=med_pct,
            ),
            "best": _summary_point(
                principal_jpy=resolved_principal,
                net_cum_return_pct=(
                    float(best_item["net_cum_return_pct"]) if best_item is not None else None
                ),
                season_id=(str(best_item["season_id"]) if best_item is not None else None),
            ),
            "worst": _summary_point(
                principal_jpy=resolved_principal,
                net_cum_return_pct=(
                    float(worst_item["net_cum_return_pct"]) if worst_item is not None else None
                ),
                season_id=(str(worst_item["season_id"]) if worst_item is not None else None),
            ),
        },
        "items": enriched[:resolved_limit],
    }
