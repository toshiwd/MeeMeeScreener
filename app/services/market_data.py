from __future__ import annotations

def _get_config_value(config: dict, keys: list[str], default):
    current = config
    for key in keys:
        if not isinstance(current, dict) or key not in current:
            return default
        current = current[key]
    return current


def _group_rows_by_code(rows: list[tuple]) -> dict[str, list[tuple]]:
    grouped: dict[str, list[tuple]] = {}
    for row in rows:
        if not row:
            continue
        code = row[0]
        grouped.setdefault(code, []).append(row[1:])
    return grouped


def _fetch_daily_rows(conn, codes: list[str], as_of: int | None, limit: int) -> dict[str, list[tuple]]:
    if not codes:
        return {}
    placeholders = ",".join(["?"] * len(codes))
    where_clauses = [f"code IN ({placeholders})"]
    params: list = list(codes)
    if as_of is not None:
        where_clauses.append("date <= ?")
        params.append(as_of)
    where_sql = " AND ".join(where_clauses)

    query = f"""
        SELECT code, date, o, h, l, c, v
        FROM (
            SELECT
                code,
                date,
                o,
                h,
                l,
                c,
                v,
                ROW_NUMBER() OVER (PARTITION BY code ORDER BY date DESC) AS rn
            FROM daily_bars
            WHERE {where_sql}
        )
        WHERE rn <= ?
        ORDER BY code, date
    """
    params.append(limit)
    rows = conn.execute(query, params).fetchall()
    return _group_rows_by_code(rows)

def _detect_body_box(monthly_rows: list[tuple], config: dict) -> dict | None:
    thresholds = _get_config_value(config, ["monthly", "thresholds"], {})
    min_months = int(thresholds.get("min_months", 3))
    max_months = int(thresholds.get("max_months", 14))
    max_range_pct = float(thresholds.get("max_range_pct", 0.2))
    wild_wick_pct = float(thresholds.get("wild_wick_pct", 0.1))

    bars: list[dict] = []
    for row in monthly_rows:
        if len(row) < 5:
            continue
        month_value, open_, high, low, close = row[:5]
        if month_value is None or open_ is None or high is None or low is None or close is None:
            continue
        body_high = max(float(open_), float(close))
        body_low = min(float(open_), float(close))
        bars.append(
            {
                "time": int(month_value),
                "open": float(open_),
                "high": float(high),
                "low": float(low),
                "close": float(close),
                "body_high": body_high,
                "body_low": body_low
            }
        )

    if len(bars) < min_months:
        return None

    bars.sort(key=lambda item: item["time"])
    max_months = min(max_months, len(bars))

    for length in range(max_months, min_months - 1, -1):
        window = bars[-length:]
        upper = max(item["body_high"] for item in window)
        lower = min(item["body_low"] for item in window)
        base = max(abs(lower), 1e-9)
        range_pct = (upper - lower) / base
        if range_pct > max_range_pct:
            continue
        wild = False
        for item in window:
            if item["high"] > upper * (1 + wild_wick_pct) or item["low"] < lower * (1 - wild_wick_pct):
                wild = True
                break
        return {
            "start": window[0]["time"],
            "end": window[-1]["time"],
            "upper": upper,
            "lower": lower,
            "months": length,
            "range_pct": range_pct,
            "wild": wild,
            "last_close": window[-1]["close"]
        }

    return None

