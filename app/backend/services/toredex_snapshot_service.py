from __future__ import annotations

from datetime import date
import math
from typing import Any

from app.backend.services import rankings_cache
from app.backend.services.toredex_config import ToredexConfig


def _first_finite(*values: Any) -> float | None:
    for value in values:
        try:
            if value is None:
                continue
            f = float(value)
            if math.isfinite(f):
                return float(f)
        except Exception:
            continue
    return None


def _map_regime(item: dict[str, Any]) -> str:
    trend_up = bool(item.get("trendUp"))
    trend_down = bool(item.get("trendDown"))
    trend_up_strict = bool(item.get("trendUpStrict"))
    trend_down_strict = bool(item.get("trendDownStrict"))

    if trend_up_strict:
        return "UP"
    if trend_down_strict:
        return "DOWN"
    if trend_up and not trend_down:
        return "UP_WEAK"
    if trend_down and not trend_up:
        return "DOWN_WEAK"
    return "RANGE"


def _map_gate(item: dict[str, Any]) -> dict[str, Any]:
    setup = str(item.get("setupType") or "watch")
    ok = bool(item.get("entryQualified"))
    return {
        "ok": ok,
        "reason": "ENTRY_OK" if ok else f"SETUP_{setup}",
    }


def _map_rank_item(item: dict[str, Any]) -> dict[str, Any]:
    code = str(item.get("code") or "")
    if not code:
        return {}
    return {
        "ticker": code,
        "ev": _first_finite(item.get("mlEv20Net"), item.get("mlEvShortNet"), item.get("changePct")),
        "upProb": _first_finite(item.get("mlPUpShort"), item.get("mlPUp"), item.get("probSide")),
        "revRisk": _first_finite(item.get("mlPTurnDownShort"), item.get("mlPDownShort"), item.get("mlPDown")),
        "regime": _map_regime(item),
        "gate": _map_gate(item),
        "close": _first_finite(item.get("close")),
        "entryScore": _first_finite(item.get("entryScore"), item.get("hybridScore")),
        "sourceAsOf": item.get("asOf"),
    }


def _index_close_map(rankings: list[dict[str, Any]]) -> dict[str, float]:
    out: dict[str, float] = {}
    for item in rankings:
        ticker = str(item.get("ticker") or "")
        close = _first_finite(item.get("close"))
        if ticker and close is not None:
            out[ticker] = close
    return out


def _position_with_mark(
    pos: dict[str, Any],
    close_map: dict[str, float],
    as_of: date,
) -> dict[str, Any]:
    ticker = str(pos.get("ticker") or "")
    side = str(pos.get("side") or "LONG").upper()
    avg_price = float(pos.get("avgPrice") or 0.0)
    close = close_map.get(ticker)
    pnl_pct = float(pos.get("pnlPct") or 0.0)
    if close is not None and avg_price > 0:
        if side == "SHORT":
            pnl_pct = (avg_price - close) / avg_price * 100.0
        else:
            pnl_pct = (close - avg_price) / avg_price * 100.0
    return {
        "ticker": ticker,
        "side": side,
        "units": int(pos.get("units") or 0),
        "avgPrice": avg_price,
        "pnlPct": pnl_pct,
        "stage": str(pos.get("stage") or "PROBE"),
        "openedAt": str(pos.get("openedAt") or as_of.isoformat()),
        "holdingDays": int(pos.get("holdingDays") or 0),
    }


def build_snapshot(
    *,
    season_id: str,
    as_of: date,
    config: ToredexConfig,
    positions: list[dict[str, Any]],
) -> dict[str, Any]:
    as_of_iso = as_of.isoformat()

    buy_resp = rankings_cache.get_rankings_asof(
        "D",
        "latest",
        "up",
        config.top_n,
        as_of=as_of_iso,
        mode=str(config.ranking_mode),
    )
    sell_resp = rankings_cache.get_rankings_asof(
        "D",
        "latest",
        "down",
        config.top_n,
        as_of=as_of_iso,
        mode=str(config.ranking_mode),
    )

    buy_items_raw = buy_resp.get("items") if isinstance(buy_resp.get("items"), list) else []
    sell_items_raw = sell_resp.get("items") if isinstance(sell_resp.get("items"), list) else []

    buy_items: list[dict[str, Any]] = []
    for item in buy_items_raw:
        if isinstance(item, dict):
            mapped = _map_rank_item(item)
            if mapped:
                buy_items.append(mapped)

    sell_items: list[dict[str, Any]] = []
    for item in sell_items_raw:
        if isinstance(item, dict):
            mapped = _map_rank_item(item)
            if mapped:
                sell_items.append(mapped)

    close_map = _index_close_map(buy_items)
    for ticker, close in _index_close_map(sell_items).items():
        close_map.setdefault(ticker, close)

    snap_positions = [_position_with_mark(pos, close_map, as_of) for pos in positions]

    universe = sorted({str(item.get("ticker")) for item in [*buy_items, *sell_items] if item.get("ticker")})
    no_future = True
    for item in [*buy_items, *sell_items]:
        source_as_of = str(item.get("sourceAsOf") or "")
        if source_as_of and source_as_of > as_of_iso:
            no_future = False
            break

    snapshot: dict[str, Any] = {
        "asOf": as_of_iso,
        "seasonId": season_id,
        "mode": "LIVE",
        "policyVersion": config.policy_version,
        "configHash": config.config_hash,
        "universe": universe,
        "rankings": {
            "buy": buy_items,
            "sell": sell_items,
        },
        "positions": snap_positions,
        "meta": {
            "requestedAsOf": as_of_iso,
            "predDtBuy": buy_resp.get("pred_dt"),
            "predDtSell": sell_resp.get("pred_dt"),
            "noFutureLeakOk": bool(no_future),
        },
    }
    return snapshot


def snapshot_has_minimum_fields(snapshot: dict[str, Any]) -> bool:
    if not snapshot:
        return False
    if not snapshot.get("asOf"):
        return False
    if not snapshot.get("policyVersion"):
        return False
    rankings = snapshot.get("rankings")
    if not isinstance(rankings, dict):
        return False
    buy = rankings.get("buy")
    if not isinstance(buy, list):
        return False
    return len(buy) > 0
