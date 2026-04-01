from __future__ import annotations

import argparse
import json
import math
import os
from bisect import bisect_right
from contextlib import ExitStack
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any
from unittest.mock import patch

import numpy as np
import pandas as pd

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in __import__("sys").path:
    __import__("sys").path.insert(0, str(REPO_ROOT))

from app.backend.services.analysis import ranking_backtest_service
from app.backend.services.ml import rankings_cache
from app.backend.services.tradex_experiment_store import write_json
from app.core.config import config
from app.db.session import get_conn_for_path

DEFAULT_SELECTION_LIMIT = 10
DEFAULT_ROUND_TRIP_COST = 0.002
DEFAULT_MONTHS = 12
DEFAULT_FOCUS_YMD = 20260227
ENTRY_BACKTEST_SCHEMA_VERSION = "ranking_month_end_entry_backtest_v2"
DEFAULT_DIRECTION = "up"
ALLOWED_STRICT_SETUP_TYPES_UP = {"breakout", "breakout20", "accumulation"}
ALLOWED_STRICT_SETUP_TYPES_DOWN = {"breakout", "breakout20", "breakdown", "pressure", "continuation"}
STATE_BOX_STATES = {"box_lower", "box_mid", "box_upper", "breakout_up"}


def _parse_date(value: str | None) -> date | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    for fmt in ("%Y-%m-%d", "%Y%m%d", "%Y/%m/%d"):
        try:
            return datetime.strptime(text, fmt).date()
        except ValueError:
            continue
    raise ValueError("date must be YYYY-MM-DD or YYYYMMDD")


def _safe_float(value: Any) -> float | None:
    try:
        if value is None:
            return None
        out = float(value)
    except Exception:
        return None
    return out if math.isfinite(out) else None


def _safe_bool(value: Any) -> bool:
    return bool(value is True or value == 1 or value == 1.0)


def _mean(values: list[float]) -> float | None:
    if not values:
        return None
    return float(sum(values) / len(values))


def _summary_from_returns(values: pd.Series) -> dict[str, Any]:
    arr = pd.to_numeric(values, errors="coerce").dropna().to_numpy(dtype=np.float64, copy=False)
    if arr.size == 0:
        return {
            "n": 0,
            "mean": None,
            "median": None,
            "win_rate": None,
            "profit_factor": None,
            "sum": None,
            "mdd": None,
        }
    gains = float(arr[arr > 0.0].sum())
    losses = float(-arr[arr < 0.0].sum())
    if losses <= 1e-12:
        profit_factor = float("inf") if gains > 0.0 else 0.0
    else:
        profit_factor = float(gains / losses)
    growth = np.clip(1.0 + arr, 1e-6, 1e6)
    log_equity = np.cumsum(np.log(growth))
    log_equity = np.clip(log_equity, -60.0, 60.0)
    equity = np.exp(log_equity)
    peak = np.maximum.accumulate(equity)
    drawdown = np.where(peak > 0.0, equity / peak - 1.0, 0.0)
    return {
        "n": int(arr.size),
        "mean": float(arr.mean()),
        "median": float(np.median(arr)),
        "win_rate": float(np.mean(arr > 0.0)),
        "profit_factor": profit_factor,
        "sum": float(arr.sum()),
        "mdd": float(max(0.0, -drawdown.min())),
    }


def _code_concentration(panel: pd.DataFrame) -> dict[str, Any]:
    if panel.empty or "code" not in panel.columns:
        return {"unique_codes": 0, "top_code_share": None, "top5_code_share": None}
    counts = panel["code"].astype(str).value_counts(dropna=True)
    total = int(counts.sum()) if not counts.empty else 0
    if total <= 0 or counts.empty:
        return {"unique_codes": 0, "top_code_share": None, "top5_code_share": None}
    shares = counts / float(total)
    return {
        "unique_codes": int(counts.size),
        "top_code_share": float(shares.iloc[0]),
        "top5_code_share": float(shares.head(5).sum()),
    }


def _monthly_summary(panel: pd.DataFrame, *, return_col: str) -> dict[str, Any]:
    if panel.empty or return_col not in panel.columns:
        return {"month_count": 0, "positive_month_rate": None, "worst_month": None, "best_month": None}
    frame = panel.copy()
    frame["month"] = frame["as_of"].astype(str).str.slice(0, 6)
    monthly = frame.groupby("month", sort=True)[return_col].mean().dropna()
    if monthly.empty:
        return {"month_count": 0, "positive_month_rate": None, "worst_month": None, "best_month": None}
    return {
        "month_count": int(monthly.size),
        "positive_month_rate": float((monthly > 0.0).mean()),
        "worst_month": float(monthly.min()),
        "best_month": float(monthly.max()),
    }


def _resolve_db_path(cli_value: str | None) -> Path:
    if cli_value:
        path = Path(cli_value).expanduser().resolve()
        if not path.exists():
            raise FileNotFoundError(f"DB not found: {path}")
        return path

    env = os.getenv("STOCKS_DB_PATH")
    if env:
        path = Path(env).expanduser().resolve()
        if path.exists():
            return path

    env_data_dir = os.getenv("MEEMEE_DATA_DIR")
    if env_data_dir:
        candidate = Path(env_data_dir).expanduser().resolve() / "stocks.duckdb"
        if candidate.exists():
            return candidate

    local_app_data = Path(os.getenv("LOCALAPPDATA") or str(Path.home()))
    user_candidate = (local_app_data / "MeeMeeScreener" / "data" / "stocks.duckdb").resolve()
    if user_candidate.exists():
        return user_candidate

    fallback = Path(config.DB_PATH).expanduser().resolve()
    if fallback.exists():
        return fallback
    raise FileNotFoundError("Could not resolve DB path. Pass --db-path or set STOCKS_DB_PATH.")


def _latest_trade_date(db_path: Path) -> int:
    expr = ranking_backtest_service._date_expr("date")  # type: ignore[attr-defined]
    with get_conn_for_path(str(db_path), timeout_sec=2.5, read_only=True) as conn:
        row = conn.execute(f"SELECT MAX({expr}) FROM daily_bars").fetchone()
    if not row or row[0] is None:
        raise RuntimeError("daily_bars has no valid trade dates")
    return int(row[0])


def _month_end_dates_from_trade_dates(trade_dates: list[int]) -> list[int]:
    month_end_dates: list[int] = []
    current_ym: int | None = None
    last_ymd: int | None = None
    for ymd in sorted(int(v) for v in trade_dates if v is not None):
        ym = int(ymd) // 100
        if current_ym is None:
            current_ym = ym
        elif ym != current_ym:
            if last_ymd is not None:
                month_end_dates.append(int(last_ymd))
            current_ym = ym
        last_ymd = int(ymd)
    if last_ymd is not None:
        month_end_dates.append(int(last_ymd))
    return month_end_dates


def _month_end_dates(
    db_path: Path,
    *,
    start_ymd: int,
    end_ymd: int,
) -> list[int]:
    expr = ranking_backtest_service._date_expr("date")  # type: ignore[attr-defined]
    with get_conn_for_path(str(db_path), timeout_sec=2.5, read_only=True) as conn:
        rows = conn.execute(
            f"""
            SELECT DISTINCT {expr} AS ymd
            FROM daily_bars
            WHERE {expr} BETWEEN ? AND ?
            ORDER BY ymd ASC
            """,
            [int(start_ymd), int(end_ymd)],
        ).fetchall()
    trade_dates = [int(row[0]) for row in rows if row and row[0] is not None]
    return _month_end_dates_from_trade_dates(trade_dates)


def _month_window(
    db_path: Path,
    *,
    months: int = DEFAULT_MONTHS,
    end_ymd: int | None = None,
    start_ymd: int | None = None,
) -> list[int]:
    resolved_end = int(end_ymd) if end_ymd is not None else _latest_trade_date(db_path)
    if start_ymd is not None:
        return _month_end_dates(db_path, start_ymd=int(start_ymd), end_ymd=resolved_end)
    end_date = ranking_backtest_service._ymd_to_date(resolved_end)  # type: ignore[attr-defined]
    lookback = ranking_backtest_service._subtract_months(end_date, int(months) + 3)  # type: ignore[attr-defined]
    month_end_dates = _month_end_dates(
        db_path,
        start_ymd=ranking_backtest_service._date_to_ymd(lookback),  # type: ignore[attr-defined]
        end_ymd=resolved_end,
    )
    return month_end_dates[-int(months):] if len(month_end_dates) > int(months) else month_end_dates


def _row_from_item(*, as_of_ymd: int, rank: int, item: dict[str, Any]) -> dict[str, Any]:
    entry_score = _safe_float(item.get("entryScore"))
    hybrid_score = _safe_float(item.get("hybridScore"))
    display_score = entry_score if entry_score is not None else hybrid_score
    if display_score is None:
        display_score = _safe_float(item.get("displayScore"))
    return {
        "as_of": int(as_of_ymd),
        "as_of_iso": ranking_backtest_service._ymd_to_date(int(as_of_ymd)).isoformat(),  # type: ignore[attr-defined]
        "rank": int(rank),
        "code": str(item.get("code") or "").strip(),
        "entryQualified": bool(item.get("entryQualified") is True),
        "entryQualifiedByFallback": bool(item.get("entryQualifiedByFallback") is True),
        "entryQualifiedFallbackStage": str(item.get("entryQualifiedFallbackStage") or "").strip() or None,
        "entryScore": entry_score,
        "hybridScore": hybrid_score,
        "displayScore": display_score,
        "setupType": str(item.get("setupType") or "").strip() or None,
        "monthlyBoxState": str(item.get("monthlyBoxState") or "").strip() or None,
        "monthlyBoxMonths": _safe_float(item.get("monthlyBoxMonths")),
        "monthlyBoxPos": _safe_float(item.get("monthlyBoxPos")),
        "monthlyBoxWild": _safe_bool(item.get("monthlyBoxWild")) if item.get("monthlyBoxWild") is not None else None,
        "boxBottomAligned": _safe_bool(item.get("boxBottomAligned")) if item.get("boxBottomAligned") is not None else None,
        "reclaim60": _safe_float(item.get("reclaim60")),
        "v60Core": _safe_float(item.get("v60Core")),
        "v60Strong": _safe_float(item.get("v60Strong")),
        "trendUpStrict": _safe_bool(item.get("trendUpStrict")) if item.get("trendUpStrict") is not None else None,
        "trendDownStrict": _safe_bool(item.get("trendDownStrict")) if item.get("trendDownStrict") is not None else None,
        "weeklyBreakoutUpProb": _safe_float(item.get("weeklyBreakoutUpProb")),
        "weeklyBreakoutDownProb": _safe_float(item.get("weeklyBreakoutDownProb")),
        "monthlyBreakoutUpProb": _safe_float(item.get("monthlyBreakoutUpProb")),
        "monthlyBreakoutDownProb": _safe_float(item.get("monthlyBreakoutDownProb")),
        "candleBodyRatio": _safe_float(item.get("candleBodyRatio")),
        "candleUpperWickRatio": _safe_float(item.get("candleUpperWickRatio")),
        "candleLowerWickRatio": _safe_float(item.get("candleLowerWickRatio")),
        "candleTripletUp": _safe_float(item.get("candleTripletUp")),
        "candleTripletDown": _safe_float(item.get("candleTripletDown")),
        "bullMarubozu": _safe_bool(item.get("bullMarubozu")) if item.get("bullMarubozu") is not None else None,
        "bearMarubozu": _safe_bool(item.get("bearMarubozu")) if item.get("bearMarubozu") is not None else None,
        "threeWhiteSoldiers": _safe_bool(item.get("threeWhiteSoldiers")) if item.get("threeWhiteSoldiers") is not None else None,
        "threeBlackCrows": _safe_bool(item.get("threeBlackCrows")) if item.get("threeBlackCrows") is not None else None,
        "morningStar": _safe_bool(item.get("morningStar")) if item.get("morningStar") is not None else None,
        "bullEngulfing": _safe_bool(item.get("bullEngulfing")) if item.get("bullEngulfing") is not None else None,
        "shootingStarLike": _safe_bool(item.get("shootingStarLike")) if item.get("shootingStarLike") is not None else None,
        "predDt": item.get("predDt"),
        "selectionVariant": item.get("selectionVariant"),
        "marketRegime": str(item.get("marketRegime") or "").strip() or None,
    }


def _build_month_rows(
    db_path: Path,
    month_end_dates: list[int],
    *,
    limit: int,
    rank_mode: str = "trade",
    risk_mode: str = "balanced",
    direction: str = DEFAULT_DIRECTION,
) -> tuple[pd.DataFrame, list[str]]:
    rows: list[dict[str, Any]] = []
    universe: set[str] = set()
    for as_of_ymd in month_end_dates:
        payload = rankings_cache.get_rankings_asof(
            "D",
            "latest",
            str(direction),
            int(limit),
            as_of=int(as_of_ymd),
            mode=str(rank_mode),
            risk_mode=risk_mode,
        )
        items = payload.get("items") if isinstance(payload, dict) else []
        if not isinstance(items, list):
            continue
        ordered_items = [item for item in items if isinstance(item, dict)]
        for rank, item in enumerate(ordered_items, start=1):
            code = str(item.get("code") or "").strip()
            if not code:
                continue
            universe.add(code)
            rows.append(_row_from_item(as_of_ymd=int(as_of_ymd), rank=int(rank), item=item))
    return pd.DataFrame(rows), sorted(universe)


def _first_trade_index_after(dates: list[int], as_of_ymd: int) -> int | None:
    lo = 0
    hi = len(dates)
    while lo < hi:
        mid = (lo + hi) // 2
        if int(dates[mid]) <= int(as_of_ymd):
            lo = mid + 1
        else:
            hi = mid
    return lo if lo < len(dates) else None


def _last_trade_index_on_or_before(dates: list[int], ymd: int) -> int | None:
    idx = bisect_right([int(v) for v in dates], int(ymd)) - 1
    return idx if idx >= 0 else None


def _compute_forward_returns(
    *,
    price_lookup: dict[str, dict[str, list[float] | list[int]]],
    code: str,
    as_of_ymd: int,
    horizons: tuple[int, ...],
    direction: str = DEFAULT_DIRECTION,
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
    direction_key = str(direction).strip().lower()
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
        if direction_key == "down":
            if exit_price <= 0.0:
                out[int(horizon)] = None
            else:
                out[int(horizon)] = float(entry_price / exit_price - 1.0)
        else:
            out[int(horizon)] = float(exit_price / entry_price - 1.0)
    return out


def _compute_forward_return_to_ymd(
    *,
    price_lookup: dict[str, dict[str, list[float] | list[int]]],
    code: str,
    as_of_ymd: int,
    exit_ymd: int | None,
    direction: str = DEFAULT_DIRECTION,
) -> float | None:
    if exit_ymd is None:
        return None
    payload = price_lookup.get(str(code))
    if not isinstance(payload, dict):
        return None
    dates = payload.get("dates") or []
    opens = payload.get("opens") or []
    closes = payload.get("closes") or []
    if not isinstance(dates, list) or not isinstance(opens, list) or not isinstance(closes, list):
        return None
    entry_idx = _first_trade_index_after([int(v) for v in dates], int(as_of_ymd))
    exit_idx = _last_trade_index_on_or_before([int(v) for v in dates], int(exit_ymd))
    if entry_idx is None or exit_idx is None or exit_idx < entry_idx:
        return None
    entry_price = float(opens[entry_idx])
    exit_price = float(closes[exit_idx])
    if not math.isfinite(entry_price) or not math.isfinite(exit_price) or entry_price <= 0.0:
        return None
    direction_key = str(direction).strip().lower()
    if direction_key == "down":
        if exit_price <= 0.0:
            return None
        return float(entry_price / exit_price - 1.0)
    return float(exit_price / entry_price - 1.0)


def _apply_round_trip_cost(series: pd.Series, *, round_trip_cost: float) -> pd.Series:
    values = pd.to_numeric(series, errors="coerce")
    return values - float(round_trip_cost)


def _ensure_column(frame: pd.DataFrame, column: str, default: Any) -> None:
    if column not in frame.columns:
        frame[column] = default


def _strict_filter_frame(frame: pd.DataFrame, *, variant: str, direction: str) -> pd.DataFrame:
    if frame.empty:
        return frame.copy()
    working = frame.copy()
    working["entryQualified"] = working["entryQualified"] == True  # noqa: E712
    working["entryQualifiedByFallback"] = working["entryQualifiedByFallback"] == True  # noqa: E712
    working["setupType"] = working["setupType"].fillna("watch").astype(str)
    working["monthlyBoxState"] = working["monthlyBoxState"].fillna("no_box").astype(str)
    working["monthlyBoxMonths"] = pd.to_numeric(working["monthlyBoxMonths"], errors="coerce")
    if "monthlyBoxPos" not in working.columns:
        working["monthlyBoxPos"] = np.nan
    working["monthlyBoxPos"] = pd.to_numeric(working["monthlyBoxPos"], errors="coerce")
    working["monthlyBoxWild"] = working["monthlyBoxWild"] == True  # noqa: E712
    for col in ("bullMarubozu", "bearMarubozu", "threeWhiteSoldiers", "threeBlackCrows", "morningStar", "bullEngulfing", "shootingStarLike"):
        if col not in working.columns:
            working[col] = False
    _ensure_column(working, "candleTripletUp", np.nan)
    _ensure_column(working, "candleTripletDown", np.nan)
    working["candleBodyRatio"] = pd.to_numeric(working["candleBodyRatio"], errors="coerce")
    working["candleUpperWickRatio"] = pd.to_numeric(working["candleUpperWickRatio"], errors="coerce")
    working["candleLowerWickRatio"] = pd.to_numeric(working["candleLowerWickRatio"], errors="coerce")
    working["candleTripletUp"] = pd.to_numeric(working["candleTripletUp"], errors="coerce")
    working["candleTripletDown"] = pd.to_numeric(working["candleTripletDown"], errors="coerce")
    working["reclaim60"] = pd.to_numeric(working["reclaim60"], errors="coerce")
    working["v60Core"] = pd.to_numeric(working["v60Core"], errors="coerce")
    working["v60Strong"] = pd.to_numeric(working["v60Strong"], errors="coerce")
    working["displayScore"] = pd.to_numeric(working["displayScore"], errors="coerce")
    working["entryScore"] = pd.to_numeric(working["entryScore"], errors="coerce")
    working["hybridScore"] = pd.to_numeric(working["hybridScore"], errors="coerce")
    working["boxBottomAligned"] = working["boxBottomAligned"] == True  # noqa: E712

    direction_key = str(direction).strip().lower()
    allowed_setup_types = ALLOWED_STRICT_SETUP_TYPES_UP if direction_key == "up" else ALLOWED_STRICT_SETUP_TYPES_DOWN

    working = working[
        working["entryQualified"]
        & (~working["entryQualifiedByFallback"])
        & working["setupType"].isin(allowed_setup_types)
    ].copy()
    if working.empty:
        return working

    if direction_key == "up":
        if variant == "strict_buy":
            return working
        if variant != "strict_buy_state":
            raise ValueError(f"unknown variant: {variant}")
        state_mask = (
            working["monthlyBoxState"].isin(STATE_BOX_STATES)
            & working["monthlyBoxMonths"].fillna(-np.inf).ge(4.0)
            & (~working["monthlyBoxWild"])
            & (
                (working["reclaim60"].fillna(0.0) >= 0.5)
                | (working["v60Core"].fillna(0.0) >= 0.5)
                | (working["v60Strong"].fillna(0.0) >= 0.5)
            )
        )
        return working[state_mask].copy()

    if variant == "strict_sell":
        return working
    if variant != "strict_sell_state":
        raise ValueError(f"unknown variant: {variant}")
    sell_state_mask = (
        (
            working["monthlyBoxState"].isin({"box_upper", "breakout_up"})
            | working["monthlyBoxPos"].fillna(-np.inf).ge(0.6)
        )
        & working["monthlyBoxMonths"].fillna(-np.inf).ge(4.0)
        & (~working["monthlyBoxWild"])
        & (
            working["bearMarubozu"]
            | working["threeBlackCrows"]
            | working["shootingStarLike"]
            | (
                (working["candleUpperWickRatio"].fillna(1.0) >= 0.25)
                & (working["candleBodyRatio"].fillna(0.0) >= 0.45)
            )
        )
    )
    return working[sell_state_mask].copy()


def _strict_score_frame(frame: pd.DataFrame, *, variant: str, direction: str) -> pd.DataFrame:
    if frame.empty:
        return frame.copy()
    working = frame.copy()
    working["rank"] = pd.to_numeric(working["rank"], errors="coerce").fillna(9999).astype(int)
    working["entryScore"] = pd.to_numeric(working["entryScore"], errors="coerce")
    working["hybridScore"] = pd.to_numeric(working["hybridScore"], errors="coerce")
    working["displayScore"] = pd.to_numeric(working["displayScore"], errors="coerce")
    working["monthlyBoxMonths"] = pd.to_numeric(working["monthlyBoxMonths"], errors="coerce")
    if "monthlyBoxPos" not in working.columns:
        working["monthlyBoxPos"] = np.nan
    working["monthlyBoxPos"] = pd.to_numeric(working["monthlyBoxPos"], errors="coerce")
    working["candleBodyRatio"] = pd.to_numeric(working["candleBodyRatio"], errors="coerce")
    working["candleUpperWickRatio"] = pd.to_numeric(working["candleUpperWickRatio"], errors="coerce")
    working["candleLowerWickRatio"] = pd.to_numeric(working["candleLowerWickRatio"], errors="coerce")
    working["candleTripletUp"] = pd.to_numeric(working["candleTripletUp"], errors="coerce")
    working["candleTripletDown"] = pd.to_numeric(working["candleTripletDown"], errors="coerce")
    working["reclaim60"] = pd.to_numeric(working["reclaim60"], errors="coerce")
    working["v60Core"] = pd.to_numeric(working["v60Core"], errors="coerce")
    working["v60Strong"] = pd.to_numeric(working["v60Strong"], errors="coerce")
    working["boxBottomAligned"] = working["boxBottomAligned"] == True  # noqa: E712
    working["monthlyBoxWild"] = working["monthlyBoxWild"] == True  # noqa: E712
    working["monthlyBoxState"] = working["monthlyBoxState"].fillna("no_box").astype(str)
    working["setupType"] = working["setupType"].fillna("watch").astype(str)
    for col in ("bullMarubozu", "bearMarubozu", "threeWhiteSoldiers", "threeBlackCrows", "morningStar", "bullEngulfing", "shootingStarLike"):
        if col not in working.columns:
            working[col] = False
    _ensure_column(working, "candleTripletUp", np.nan)
    _ensure_column(working, "candleTripletDown", np.nan)
    working["bullMarubozu"] = working["bullMarubozu"] == True  # noqa: E712
    working["bullEngulfing"] = working["bullEngulfing"] == True  # noqa: E712
    working["morningStar"] = working["morningStar"] == True  # noqa: E712
    working["bearMarubozu"] = working["bearMarubozu"] == True  # noqa: E712
    working["threeBlackCrows"] = working["threeBlackCrows"] == True  # noqa: E712
    working["shootingStarLike"] = working["shootingStarLike"] == True  # noqa: E712
    max_rank = working.groupby("as_of")["rank"].transform("max").astype(float).clip(lower=1.0)
    working["rank_score"] = 1.0 - (working["rank"].astype(float) - 1.0) / max_rank

    base_score = working["entryScore"].fillna(working["hybridScore"]).fillna(working["displayScore"]).fillna(working["rank_score"])
    score = 0.70 * base_score + 0.30 * working["rank_score"]

    direction_key = str(direction).strip().lower()
    if direction_key == "up":
        score = score + np.where(working["setupType"].eq("breakout20"), 0.10, 0.0)
        score = score + np.where(working["setupType"].eq("breakout"), 0.07, 0.0)
        score = score + np.where(working["setupType"].eq("accumulation"), 0.05, 0.0)
        score = score + np.where(working["monthlyBoxState"].isin(STATE_BOX_STATES), 0.05, 0.0)
        score = score + np.where(working["monthlyBoxState"].eq("no_box"), -0.10, 0.0)
        score = score + np.where(working["monthlyBoxWild"], -0.05, 0.0)
        score = score + np.where(working["monthlyBoxMonths"].fillna(-np.inf) >= 4.0, 0.03, -0.02)
        score = score + np.where(working["boxBottomAligned"], 0.03, 0.0)
        score = score + np.where(
            (working["reclaim60"].fillna(0.0) >= 0.5)
            | (working["v60Core"].fillna(0.0) >= 0.5)
            | (working["v60Strong"].fillna(0.0) >= 0.5),
            0.03,
            0.0,
        )
        score = score + np.where(working["candleBodyRatio"].fillna(0.0) >= 0.50, 0.02, 0.0)
        score = score + np.where(working["candleUpperWickRatio"].fillna(1.0) <= 0.25, 0.02, 0.0)
        score = score + np.where(working["candleLowerWickRatio"].fillna(0.0) >= 0.25, 0.01, 0.0)
        score = score + np.where(working["candleTripletUp"].fillna(0.0) >= 0.58, 0.03, 0.0)
        working["selectionScore"] = score.astype(float)

        if variant == "strict_buy_state":
            working["selectionScore"] = working["selectionScore"] + np.where(working["monthlyBoxState"].isin(STATE_BOX_STATES), 0.04, 0.0)
            working["selectionScore"] = working["selectionScore"] + np.where(
                (working["candleTripletUp"].fillna(0.0) >= 0.58)
                | (working["bullMarubozu"] == True)  # noqa: E712
                | (working["bullEngulfing"] == True)  # noqa: E712
                | (working["morningStar"] == True),  # noqa: E712
                0.04,
                0.0,
            )
        elif variant != "strict_buy":
            raise ValueError(f"unknown variant: {variant}")
        return working

    score = score + np.where(working["setupType"].eq("breakdown"), 0.10, 0.0)
    score = score + np.where(working["setupType"].eq("pressure"), 0.07, 0.0)
    score = score + np.where(working["setupType"].eq("breakout"), 0.06, 0.0)
    score = score + np.where(working["setupType"].eq("breakout20"), 0.05, 0.0)
    score = score + np.where(working["setupType"].eq("continuation"), 0.03, 0.0)
    score = score + np.where(working["monthlyBoxState"].isin({"box_upper", "breakout_up"}) | working["monthlyBoxPos"].fillna(-np.inf).ge(0.6), 0.04, 0.0)
    score = score + np.where(working["monthlyBoxMonths"].fillna(-np.inf) >= 4.0, 0.02, 0.0)
    score = score + np.where(~working["monthlyBoxWild"], 0.01, -0.02)
    score = score + np.where(working["bearMarubozu"], 0.05, 0.0)
    score = score + np.where(working["threeBlackCrows"], 0.06, 0.0)
    score = score + np.where(working["shootingStarLike"], 0.04, 0.0)
    score = score + np.where(working["candleUpperWickRatio"].fillna(0.0) >= 0.25, 0.03, 0.0)
    score = score + np.where(working["candleBodyRatio"].fillna(0.0) >= 0.50, 0.01, 0.0)
    score = score + np.where(working["candleLowerWickRatio"].fillna(1.0) <= 0.20, 0.01, 0.0)
    score = score + np.where(working["candleTripletDown"].fillna(0.0) >= 0.58, 0.03, 0.0)
    if variant == "strict_sell_state":
        score = score + np.where(
            (working["monthlyBoxState"].isin({"box_upper", "breakout_up"}) | working["monthlyBoxPos"].fillna(-np.inf).ge(0.65))
            & (
                working["bearMarubozu"]
                | working["threeBlackCrows"]
                | working["shootingStarLike"]
            ),
            0.04,
            0.0,
        )
    elif variant != "strict_sell":
        raise ValueError(f"unknown variant: {variant}")
    working["selectionScore"] = score.astype(float)
    return working


def _select_variant(panel: pd.DataFrame, *, bucket_size: int, variant: str, direction: str) -> pd.DataFrame:
    if panel.empty:
        return panel.copy()

    frame = panel.copy()
    frame["setupType"] = frame["setupType"].fillna("watch").astype(str)
    frame["entryQualified"] = frame["entryQualified"] == True  # noqa: E712
    frame["entryQualifiedByFallback"] = frame["entryQualifiedByFallback"] == True  # noqa: E712

    selected_frames: list[pd.DataFrame] = []
    for _, group in frame.groupby("as_of", sort=False):
        working = group.copy()
        if variant == "baseline":
            working = working.sort_values(["rank", "code"], ascending=[True, True], kind="stable").head(int(bucket_size)).copy()
        else:
            working = _strict_filter_frame(working, variant=variant, direction=direction)
            if working.empty:
                continue
            working = _strict_score_frame(working, variant=variant, direction=direction)
            working = working.sort_values(
                ["selectionScore", "entryScore", "displayScore", "rank", "code"],
                ascending=[False, False, False, True, True],
                kind="stable",
            ).head(int(bucket_size)).copy()
        if not working.empty:
            selected_frames.append(working)
    if not selected_frames:
        return frame.iloc[0:0].copy()
    return pd.concat(selected_frames, ignore_index=True)


def _attach_forward_metrics(
    panel: pd.DataFrame,
    *,
    price_lookup: dict[str, dict[str, list[float] | list[int]]],
    month_end_dates: list[int],
    round_trip_cost: float,
    direction: str = DEFAULT_DIRECTION,
) -> pd.DataFrame:
    if panel.empty:
        return panel.copy()
    next_month_end_map: dict[int, int | None] = {}
    for idx, as_of in enumerate(month_end_dates):
        next_month_end_map[int(as_of)] = int(month_end_dates[idx + 1]) if idx + 1 < len(month_end_dates) else None
    rows: list[dict[str, Any]] = []
    for record in panel.to_dict(orient="records"):
        code = str(record.get("code") or "")
        as_of = int(record.get("as_of"))
        horizon_returns = _compute_forward_returns(
            price_lookup=price_lookup,
            code=code,
            as_of_ymd=as_of,
            horizons=(5, 20, 60),
            direction=direction,
        )
        next_month_end = next_month_end_map.get(as_of)
        month_end_return = _compute_forward_return_to_ymd(
            price_lookup=price_lookup,
            code=code,
            as_of_ymd=as_of,
            exit_ymd=next_month_end,
            direction=direction,
        )
        for horizon, value in horizon_returns.items():
            record[f"forward_return_{horizon}"] = value
            record[f"forward_return_{horizon}_net"] = None if value is None else float(value - round_trip_cost)
        record["forward_return_month_end"] = month_end_return
        record["forward_return_month_end_net"] = None if month_end_return is None else float(month_end_return - round_trip_cost)
        record["next_month_end"] = next_month_end
        rows.append(record)
    return pd.DataFrame(rows)


def _cohort_summary(panel: pd.DataFrame) -> dict[str, Any]:
    if panel.empty:
        return {
            "sample_count": 0,
            "daily_count": 0,
            "avg_per_day": None,
            "return_20": {},
            "return_20_net": {},
            "return_month_end": {},
            "return_month_end_net": {},
            "concentration": {"unique_codes": 0, "top_code_share": None, "top5_code_share": None},
            "monthly": {"month_count": 0, "positive_month_rate": None, "worst_month": None, "best_month": None},
            "watch_count": 0,
            "reject_count": 0,
            "fallback_count": 0,
            "selected_count_by_month": {},
        }
    return {
        "sample_count": int(len(panel)),
        "daily_count": int(panel["as_of"].nunique()) if "as_of" in panel.columns else 0,
        "avg_per_day": float(len(panel) / max(1, int(panel["as_of"].nunique()))) if "as_of" in panel.columns else None,
        "return_20": _summary_from_returns(panel["forward_return_20"] if "forward_return_20" in panel.columns else pd.Series(dtype=float)),
        "return_20_net": _summary_from_returns(panel["forward_return_20_net"] if "forward_return_20_net" in panel.columns else pd.Series(dtype=float)),
        "return_month_end": _summary_from_returns(panel["forward_return_month_end"] if "forward_return_month_end" in panel.columns else pd.Series(dtype=float)),
        "return_month_end_net": _summary_from_returns(panel["forward_return_month_end_net"] if "forward_return_month_end_net" in panel.columns else pd.Series(dtype=float)),
        "concentration": _code_concentration(panel),
        "monthly": _monthly_summary(panel, return_col="forward_return_20_net"),
        "watch_count": int((panel["setupType"].astype(str) == "watch").sum()) if "setupType" in panel.columns else 0,
        "reject_count": int((panel["setupType"].astype(str) == "reject").sum()) if "setupType" in panel.columns else 0,
        "fallback_count": int((panel["entryQualifiedByFallback"] == True).sum()) if "entryQualifiedByFallback" in panel.columns else 0,  # noqa: E712
        "selected_count_by_month": {
            str(month): int(count) for month, count in panel.groupby(panel["as_of"].astype(str).str.slice(0, 6)).size().items()
        },
    }


def _month_detail_payload(
    panel: pd.DataFrame,
    *,
    variant: str,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    if panel.empty:
        return rows
    for as_of, group in panel.groupby("as_of", sort=True):
        ordered = group.sort_values(
            ["selectionScore", "entryScore", "displayScore", "rank", "code"],
            ascending=[False, False, False, True, True],
            kind="stable",
        ) if "selectionScore" in group.columns else group.sort_values(["rank", "code"], ascending=[True, True], kind="stable")
        rows.append(
            {
                "variant": variant,
                "as_of": int(as_of),
                "as_of_iso": str(group["as_of_iso"].iloc[0]) if "as_of_iso" in group.columns and not group.empty else None,
                "selected_count": int(len(group)),
                "watch_count": int((group["setupType"].astype(str) == "watch").sum()) if "setupType" in group.columns else 0,
                "reject_count": int((group["setupType"].astype(str) == "reject").sum()) if "setupType" in group.columns else 0,
                "fallback_count": int((group["entryQualifiedByFallback"] == True).sum()) if "entryQualifiedByFallback" in group.columns else 0,  # noqa: E712
                "mean_20_net": _summary_from_returns(group["forward_return_20_net"])["mean"] if "forward_return_20_net" in group.columns else None,
                "mean_month_end_net": _summary_from_returns(group["forward_return_month_end_net"])["mean"] if "forward_return_month_end_net" in group.columns else None,
                "codes": [str(code) for code in ordered["code"].tolist()],
                "setup_types": [str(v) for v in ordered["setupType"].tolist()],
                "monthly_box_states": [str(v) for v in ordered["monthlyBoxState"].tolist()],
                "entry_qualified": [bool(v) for v in ordered["entryQualified"].tolist()],
                "selection_scores": [None if pd.isna(v) else float(v) for v in ordered.get("selectionScore", pd.Series(dtype=float)).tolist()],
                "top_items": [
                    {
                        "rank": int(row.get("rank") or 0),
                        "code": str(row.get("code") or ""),
                        "setupType": row.get("setupType"),
                        "monthlyBoxState": row.get("monthlyBoxState"),
                        "entryQualified": bool(row.get("entryQualified") is True),
                        "entryQualifiedByFallback": bool(row.get("entryQualifiedByFallback") is True),
                        "entryScore": _safe_float(row.get("entryScore")),
                        "displayScore": _safe_float(row.get("displayScore")),
                        "selectionScore": _safe_float(row.get("selectionScore")),
                        "forward_return_20_net": _safe_float(row.get("forward_return_20_net")),
                        "forward_return_month_end_net": _safe_float(row.get("forward_return_month_end_net")),
                    }
                    for _, row in ordered.head(10).iterrows()
                ],
            }
        )
    return rows


def _build_focus_case(
    payload: dict[str, Any],
    *,
    focus_ymd: int,
) -> dict[str, Any] | None:
    variants = payload.get("variants") if isinstance(payload.get("variants"), dict) else {}
    case: dict[str, Any] = {"focus_ymd": int(focus_ymd), "variants": {}}
    found = False
    for variant_name, variant_payload in variants.items():
        if not isinstance(variant_payload, dict):
            continue
        month_rows = variant_payload.get("month_rows") if isinstance(variant_payload.get("month_rows"), list) else []
        focus_rows = [row for row in month_rows if isinstance(row, dict) and int(row.get("as_of") or 0) == int(focus_ymd)]
        if not focus_rows:
            continue
        found = True
        row = focus_rows[0]
        case["variants"][variant_name] = {
            "selected_count": row.get("selected_count"),
            "watch_count": row.get("watch_count"),
            "reject_count": row.get("reject_count"),
            "fallback_count": row.get("fallback_count"),
            "mean_20_net": row.get("mean_20_net"),
            "mean_month_end_net": row.get("mean_month_end_net"),
            "codes": row.get("codes"),
            "setup_types": row.get("setup_types"),
            "monthly_box_states": row.get("monthly_box_states"),
            "top_items": row.get("top_items"),
        }
    return case if found else None


def _fmt(value: Any, digits: int = 3) -> str:
    if value is None:
        return "-"
    if isinstance(value, float) and not math.isfinite(value):
        return "-"
    if isinstance(value, (int, np.integer)):
        return str(int(value))
    if isinstance(value, float):
        return f"{value:.{digits}f}"
    return str(value)


def _render_markdown(payload: dict[str, Any]) -> str:
    comparison = payload.get("comparison") if isinstance(payload.get("comparison"), dict) else {}
    period = payload.get("period") if isinstance(payload.get("period"), dict) else {}
    direction = str(period.get("direction") or payload.get("direction") or "up").strip().lower()
    primary_variant = "strict_buy" if direction == "up" else "strict_sell"
    state_variant = "strict_buy_state" if direction == "up" else "strict_sell_state"
    lines = [
        "# 月末 仕込み専用選定 バックテスト",
        "",
        f"- generated_at: {payload.get('generated_at')}",
        f"- db_path: {payload.get('db_path')}",
        f"- period: {period.get('start_ymd')} .. {period.get('end_ymd')}",
        f"- month_end_count: {period.get('month_end_count')}",
        f"- month_end_mode: {period.get('month_end_mode')}",
        f"- direction: {direction}",
        f"- round_trip_cost: {payload.get('round_trip_cost')}",
        "",
        "## Verdict",
        f"- verdict: {payload.get('verdict')}",
        "",
        "## Comparison",
    ]
    for key in (
        "baseline_top10_mean20_net",
        f"{primary_variant}_top10_mean20_net",
        f"{state_variant}_top10_mean20_net",
        "baseline_top10_pf20_net",
        f"{primary_variant}_top10_pf20_net",
        f"{state_variant}_top10_pf20_net",
        "baseline_top10_mean_month_end_net",
        f"{primary_variant}_top10_mean_month_end_net",
        f"{state_variant}_top10_mean_month_end_net",
    ):
        lines.append(f"- {key}: {comparison.get(key)}")
    lines.append("")
    lines.append("## Variant Summary")
    variants = payload.get("variants") if isinstance(payload.get("variants"), dict) else {}
    for variant_name, variant_payload in variants.items():
        if not isinstance(variant_payload, dict):
            continue
        top = variant_payload.get("top10") if isinstance(variant_payload.get("top10"), dict) else {}
        lines.append(
            f"- {variant_name}: sample={top.get('sample_count')}, days={top.get('daily_count')}, "
            f"net20_mean={_fmt((top.get('return_20_net') or {}).get('mean'))}, "
            f"net20_pf={_fmt((top.get('return_20_net') or {}).get('profit_factor'))}, "
            f"month_end_mean={_fmt((top.get('return_month_end_net') or {}).get('mean'))}, "
            f"watch={top.get('watch_count')}, reject={top.get('reject_count')}, fallback={top.get('fallback_count')}"
        )
    lines.append("")
    lines.append("## Regression Case")
    regression = payload.get("regression_case")
    if isinstance(regression, dict):
        lines.append(f"- focus_ymd: {regression.get('focus_ymd')}")
        for variant_name, item in (regression.get("variants") or {}).items():
            if not isinstance(item, dict):
                continue
            lines.append(
                f"- {variant_name}: sample={item.get('selected_count')}, net20={_fmt(item.get('mean_20_net'))}, "
                f"month_end={_fmt(item.get('mean_month_end_net'))}, watch={item.get('watch_count')}, "
                f"reject={item.get('reject_count')}, fallback={item.get('fallback_count')}"
            )
    else:
        lines.append("- regression case not available")
    lines.append("")
    lines.append("## Monthly Detail")
    for variant_name, variant_payload in variants.items():
        if not isinstance(variant_payload, dict):
            continue
        lines.append(f"### {variant_name}")
        month_rows = variant_payload.get("month_rows") if isinstance(variant_payload.get("month_rows"), list) else []
        for row in month_rows:
            if not isinstance(row, dict):
                continue
            lines.append(
                f"- {row.get('as_of_iso')}: n={row.get('selected_count')}, net20={_fmt(row.get('mean_20_net'))}, "
                f"month_end={_fmt(row.get('mean_month_end_net'))}, watch={row.get('watch_count')}, "
                f"reject={row.get('reject_count')}, fallback={row.get('fallback_count')}, codes={', '.join(row.get('codes') or [])}"
            )
        lines.append("")
    return "\n".join(lines)


def run_ranking_month_end_entry_backtest(
    *,
    start_ymd: int | None = None,
    end_ymd: int | None = None,
    months: int = DEFAULT_MONTHS,
    rank_mode: str = "trade",
    direction: str = DEFAULT_DIRECTION,
    output_dir: Path | None = None,
    selection_limit: int = DEFAULT_SELECTION_LIMIT,
    round_trip_cost: float = DEFAULT_ROUND_TRIP_COST,
    focus_ymd: int = DEFAULT_FOCUS_YMD,
    db_path: Path | None = None,
) -> dict[str, Any]:
    resolved_db = _resolve_db_path(str(db_path) if db_path is not None else None)
    os.environ["STOCKS_DB_PATH"] = str(resolved_db)
    direction_key = str(direction).strip().lower()
    if direction_key not in {"up", "down"}:
        raise ValueError("direction must be 'up' or 'down'")
    root = output_dir if output_dir is not None else Path("tmp") / (
        "ranking_month_end_entry_backtest" if direction_key == "up" else "ranking_month_end_sell_backtest"
    )
    root = root.expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)

    conn_factory = lambda: get_conn_for_path(str(resolved_db), timeout_sec=2.5, read_only=True)
    with ExitStack() as stack:
        stack.enter_context(patch.dict(os.environ, {"MEEMEE_ENABLE_DUCKDB_READ_ONLY": "1", "STOCKS_DB_PATH": str(resolved_db)}))
        stack.enter_context(patch.object(rankings_cache, "get_conn", conn_factory))
        stack.enter_context(patch.object(ranking_backtest_service, "get_conn", conn_factory))

        resolved_end = int(end_ymd) if end_ymd is not None else _latest_trade_date(resolved_db)
        month_end_dates = (
            _month_end_dates(resolved_db, start_ymd=int(start_ymd), end_ymd=resolved_end)
            if start_ymd is not None
            else _month_window(resolved_db, months=int(months), end_ymd=resolved_end, start_ymd=None)
        )
        if not month_end_dates:
            raise RuntimeError("month-end dates could not be resolved")
        panel, universe = _build_month_rows(
            resolved_db,
            month_end_dates,
            limit=int(max(selection_limit, 200)),
            rank_mode=str(rank_mode),
            direction=direction_key,
        )
        if panel.empty:
            raise RuntimeError("month-end ranking panel is empty")

        start_date = ranking_backtest_service._ymd_to_date(int(month_end_dates[0]))  # type: ignore[attr-defined]
        end_date = ranking_backtest_service._ymd_to_date(int(month_end_dates[-1]))  # type: ignore[attr-defined]
        price_frame = ranking_backtest_service._load_price_frame(  # type: ignore[attr-defined]
            codes=universe,
            start_date=start_date,
            end_date=end_date,
        )
        price_lookup = ranking_backtest_service._price_lookup_from_frame(price_frame)  # type: ignore[attr-defined]

        panel = _attach_forward_metrics(
            panel,
            price_lookup=price_lookup,
            month_end_dates=month_end_dates,
            round_trip_cost=float(round_trip_cost),
            direction=direction_key,
        )

        variants: dict[str, dict[str, Any]] = {}
        if direction_key == "up":
            variant_names = ("baseline", "strict_buy", "strict_buy_state")
            variant_labels = {
                "baseline": "current broad ranking",
                "strict_buy": "strict buy mode",
                "strict_buy_state": "strict buy + state",
            }
        else:
            variant_names = ("baseline", "strict_sell", "strict_sell_state")
            variant_labels = {
                "baseline": "current broad ranking",
                "strict_sell": "strict sell mode",
                "strict_sell_state": "strict sell + state",
            }
        for variant_name in variant_names:
            selected = _select_variant(panel, bucket_size=int(selection_limit), variant=variant_name, direction=direction_key)
            variants[variant_name] = {
                "label": variant_labels[variant_name],
                "top10": _cohort_summary(selected),
                "month_rows": _month_detail_payload(selected, variant=variant_name),
            }

        primary_variant = "strict_buy" if direction_key == "up" else "strict_sell"
        state_variant = "strict_buy_state" if direction_key == "up" else "strict_sell_state"
        comparison = {
            "baseline_top10_mean20_net": _safe_float((variants["baseline"]["top10"].get("return_20_net") or {}).get("mean")),
            f"{primary_variant}_top10_mean20_net": _safe_float((variants[primary_variant]["top10"].get("return_20_net") or {}).get("mean")),
            f"{state_variant}_top10_mean20_net": _safe_float((variants[state_variant]["top10"].get("return_20_net") or {}).get("mean")),
            "baseline_top10_pf20_net": _safe_float((variants["baseline"]["top10"].get("return_20_net") or {}).get("profit_factor")),
            f"{primary_variant}_top10_pf20_net": _safe_float((variants[primary_variant]["top10"].get("return_20_net") or {}).get("profit_factor")),
            f"{state_variant}_top10_pf20_net": _safe_float((variants[state_variant]["top10"].get("return_20_net") or {}).get("profit_factor")),
            "baseline_top10_mean_month_end_net": _safe_float((variants["baseline"]["top10"].get("return_month_end_net") or {}).get("mean")),
            f"{primary_variant}_top10_mean_month_end_net": _safe_float((variants[primary_variant]["top10"].get("return_month_end_net") or {}).get("mean")),
            f"{state_variant}_top10_mean_month_end_net": _safe_float((variants[state_variant]["top10"].get("return_month_end_net") or {}).get("mean")),
        }
        comparison[f"lift_{primary_variant}_vs_baseline_20"] = (
            None
            if comparison["baseline_top10_mean20_net"] is None or comparison[f"{primary_variant}_top10_mean20_net"] is None
            else float(comparison[f"{primary_variant}_top10_mean20_net"] - comparison["baseline_top10_mean20_net"])
        )
        comparison[f"lift_{state_variant}_vs_baseline_20"] = (
            None
            if comparison["baseline_top10_mean20_net"] is None or comparison[f"{state_variant}_top10_mean20_net"] is None
            else float(comparison[f"{state_variant}_top10_mean20_net"] - comparison["baseline_top10_mean20_net"])
        )

        regression_case = _build_focus_case({"variants": variants}, focus_ymd=int(focus_ymd))
        payload = {
            "schema_version": ENTRY_BACKTEST_SCHEMA_VERSION,
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "db_path": str(resolved_db),
            "period": {
                "start_ymd": int(month_end_dates[0]),
                "end_ymd": int(month_end_dates[-1]),
                "month_end_count": int(len(month_end_dates)),
                "month_end_mode": "rolling_last_n" if start_ymd is None else "explicit_range",
                "month_end_dates": [int(v) for v in month_end_dates],
                "selection_limit": int(selection_limit),
                "rank_mode": str(rank_mode),
                "direction": str(direction_key),
                "focus_ymd": int(focus_ymd),
            },
            "round_trip_cost": float(round_trip_cost),
            "comparison": comparison,
            "variants": variants,
            "regression_case": regression_case,
        }

        baseline_pf = _safe_float(comparison["baseline_top10_pf20_net"])
        primary_pf = _safe_float(comparison[f"{primary_variant}_top10_pf20_net"])
        primary_mean = _safe_float(comparison[f"{primary_variant}_top10_mean20_net"])
        primary_monthly = ((variants[primary_variant]["top10"].get("monthly") or {}).get("positive_month_rate"))
        verdict = "watch"
        if (
            primary_mean is not None
            and primary_mean > 0.0
            and primary_pf is not None
            and primary_pf >= 1.2
            and (primary_monthly is None or primary_monthly >= 0.6)
        ):
            verdict = "usable" if comparison[f"lift_{state_variant}_vs_baseline_20"] is not None and comparison[f"lift_{state_variant}_vs_baseline_20"] > 0.0 else "watch"
        elif primary_mean is not None and primary_mean > 0.0:
            verdict = "watch"
        else:
            verdict = "not_usable_yet"
        payload["verdict"] = verdict
        payload["summary"] = {
            "baseline_pf20": baseline_pf,
            f"{primary_variant}_pf20": primary_pf,
            "baseline_rows": int(variants["baseline"]["top10"].get("sample_count") or 0),
            f"{primary_variant}_rows": int(variants[primary_variant]["top10"].get("sample_count") or 0),
            f"{state_variant}_rows": int(variants[state_variant]["top10"].get("sample_count") or 0),
        }
        write_json(root / "ranking_month_end_entry_backtest.json", payload)
        (root / "ranking_month_end_entry_backtest.md").write_text(_render_markdown(payload), encoding="utf-8")
        return {"output_dir": str(root), "payload": payload}


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run month-end strict-buy backtest")
    parser.add_argument("--db-path", default=None, help="stocks.duckdb path")
    parser.add_argument("--start-ymd", type=int, default=None, help="optional exact start date (YYYYMMDD)")
    parser.add_argument("--end-ymd", type=int, default=None, help="optional end date (YYYYMMDD)")
    parser.add_argument("--months", type=int, default=DEFAULT_MONTHS, help="rolling months when start-ymd is omitted")
    parser.add_argument("--direction", default=DEFAULT_DIRECTION, choices=("up", "down"), help="ranking direction to evaluate")
    parser.add_argument(
        "--rank-mode",
        default="trade",
        choices=("trade", "hybrid", "turn", "rule", "ml"),
        help="ranking mode to evaluate",
    )
    parser.add_argument("--selection-limit", type=int, default=DEFAULT_SELECTION_LIMIT)
    parser.add_argument("--round-trip-cost", type=float, default=DEFAULT_ROUND_TRIP_COST)
    parser.add_argument("--focus-ymd", type=int, default=DEFAULT_FOCUS_YMD)
    parser.add_argument("--output-dir", default=None)
    return parser


def main() -> int:
    args = _build_parser().parse_args()
    result = run_ranking_month_end_entry_backtest(
        start_ymd=args.start_ymd,
        end_ymd=args.end_ymd,
        months=int(args.months),
        rank_mode=str(args.rank_mode),
        direction=str(args.direction),
        output_dir=Path(args.output_dir).expanduser().resolve() if args.output_dir else None,
        selection_limit=int(args.selection_limit),
        round_trip_cost=float(args.round_trip_cost),
        focus_ymd=int(args.focus_ymd),
        db_path=Path(args.db_path).expanduser().resolve() if args.db_path else None,
    )
    print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


