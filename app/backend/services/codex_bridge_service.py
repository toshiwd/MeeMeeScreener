from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from functools import lru_cache
from pathlib import Path
from typing import Any

from app.backend.api.routers.ticker import _build_edinet_financials_payload, _build_edinet_summary, _build_sell_context_from_row
from app.backend.edinetdb.repository import EdinetdbRepository
from app.backend.infra.duckdb.stock_repo import StockRepository
from app.backend.services import rankings_cache
from app.backend.services.analysis.analysis_decision import build_analysis_decision
from app.backend.services.analysis_bridge.reader import get_analysis_bridge_snapshot, get_similar_cases_rows, get_state_eval_rows
from app.backend.services.data.taisyaku_import import load_taisyaku_snapshot
from app.backend.services.tradex_analysis_service import build_tradex_detail_analysis_snapshot
from app.backend.services.tradex_research_bridge_service import get_internal_state_eval_promotion_review
from app.backend.tdnetdb.repository import TdnetdbRepository
from app.core.config import config as app_config
from app.db.session import get_conn_for_path

_DEFAULT_RANKINGS_THRESHOLD_DAYS = 5
_MAX_SCREENING_TOP_N = 20
_MAX_SCREENING_CODES = 20
_MAX_SIMILAR_CASES = 5
_MAX_TDNET_DISCLOSURES = 10
_MAX_TAISYAKU_HISTORY = 10


def _normalize_text(value: Any) -> str:
    return str(value or "").strip()


def _normalize_risk_mode(value: Any) -> str:
    mode = _normalize_text(value).lower() or "balanced"
    if mode not in {"defensive", "balanced", "aggressive"}:
        raise ValueError("risk_mode must be one of defensive, balanced, aggressive")
    return mode


def _normalize_side(value: Any) -> str:
    side = _normalize_text(value).lower() or "both"
    if side not in {"long", "short", "both"}:
        raise ValueError("side must be one of long, short, both")
    return side


def _require_int(value: Any, *, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"{name} must be an integer")
    return int(value)


def _normalize_code(value: Any) -> str:
    code = _normalize_text(value)
    if not code:
        raise ValueError("code is required")
    return code


def _parse_asof(value: Any) -> tuple[date, int, str, int]:
    if value is None:
        raise ValueError("asof is required")
    raw = str(value).strip()
    if not raw:
        raise ValueError("asof is required")
    for fmt in ("%Y-%m-%d", "%Y%m%d", "%Y/%m/%d"):
        try:
            parsed = datetime.strptime(raw, fmt).date()
            asof_ymd = int(parsed.strftime("%Y%m%d"))
            asof_dt = int(datetime(parsed.year, parsed.month, parsed.day, tzinfo=timezone.utc).timestamp())
            return parsed, asof_ymd, parsed.isoformat(), asof_dt
        except ValueError:
            continue
    raise ValueError("asof must be YYYY-MM-DD or YYYYMMDD")


def _current_jst_date() -> date:
    return (datetime.now(timezone.utc) + timedelta(hours=9)).date()


def _freshness_threshold_days() -> int:
    raw = os.getenv("MEEMEE_RANK_CURRENT_CANDIDATE_MAX_AGE_DAYS", str(_DEFAULT_RANKINGS_THRESHOLD_DAYS))
    try:
        return max(1, int(str(raw).strip()))
    except Exception:
        return _DEFAULT_RANKINGS_THRESHOLD_DAYS


def _freshness_days_from_ymd(value: int | None) -> int | None:
    if value is None:
        return None
    try:
        as_of = datetime.strptime(str(int(value)), "%Y%m%d").date()
    except Exception:
        return None
    return (_current_jst_date() - as_of).days


def _ymd_to_iso(value: int | None) -> str | None:
    if value is None:
        return None
    try:
        return datetime.strptime(str(int(value)), "%Y%m%d").date().isoformat()
    except Exception:
        return None


def _coerce_db_date_to_ymd(value: Any) -> int | None:
    if value is None:
        return None
    try:
        raw = int(value)
    except Exception:
        return None
    if 19_000_101 <= raw <= 20_991_231:
        return raw
    if raw >= 1_000_000_000_000:
        try:
            return int(datetime.fromtimestamp(raw / 1000, tz=timezone.utc).strftime("%Y%m%d"))
        except Exception:
            return None
    if raw >= 1_000_000_000:
        try:
            return int(datetime.fromtimestamp(raw, tz=timezone.utc).strftime("%Y%m%d"))
        except Exception:
            return None
    return None


@lru_cache(maxsize=1)
def get_stock_repo() -> StockRepository:
    return StockRepository(str(app_config.DB_PATH))


@lru_cache(maxsize=1)
def get_edinet_repo() -> EdinetdbRepository:
    return EdinetdbRepository(app_config.DB_PATH)


@lru_cache(maxsize=1)
def get_tdnet_repo() -> TdnetdbRepository:
    return TdnetdbRepository(app_config.DB_PATH)


def _runtime_stock_db_freshness_state(latest_global_date: int | None) -> dict[str, Any]:
    threshold_days = _freshness_threshold_days()
    freshness_days = _freshness_days_from_ymd(latest_global_date)
    fresh = bool(freshness_days is not None and freshness_days < threshold_days)
    return {
        "freshness_state": "fresh" if fresh else "stale",
        "freshness_days": freshness_days,
        "freshness_threshold_days": threshold_days,
        "stale": not fresh,
    }


def _inspect_latest_table_dates(db_path: Path) -> dict[str, Any]:
    if not db_path.exists():
        return {
            "daily_bars": None,
            "daily_bars_confirmed": None,
            "daily_bars_by_source": {},
            "feature_snapshot_daily": None,
            "ml_pred_20d": None,
        }
    with get_conn_for_path(str(db_path), timeout_sec=2.5, read_only=True) as conn:
        def _table_columns(table_name: str) -> set[str]:
            rows = conn.execute(
                """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = 'main' AND lower(table_name) = lower(?)
                """,
                [table_name],
            ).fetchall()
            return {str(row[0]).strip().lower() for row in rows if str(row[0]).strip()}

        def _date_expr(column_name: str) -> str:
            return f"""
                CASE
                    WHEN "{column_name}" BETWEEN 19000101 AND 20991231 THEN CAST("{column_name}" AS INTEGER)
                    WHEN "{column_name}" >= 1000000000000 THEN CAST(strftime(to_timestamp("{column_name}" / 1000), '%Y%m%d') AS INTEGER)
                    WHEN "{column_name}" >= 1000000000 THEN CAST(strftime(to_timestamp("{column_name}"), '%Y%m%d') AS INTEGER)
                    ELSE NULL
                END
            """

        def _date_column_for_table(table_name: str) -> str | None:
            columns = _table_columns(table_name)
            if not columns:
                return None
            for candidate in ("date", "dt", "as_of", "asof", "snapshot_date", "trade_date", "ymd"):
                if candidate in columns:
                    return candidate
            return None

        def _latest_ymd_from_table(table_name: str) -> int | None:
            date_column = _date_column_for_table(table_name)
            if not date_column:
                return None
            row = conn.execute(
                f"""
                SELECT MAX({_date_expr(date_column)})
                FROM "{table_name}"
                """
            ).fetchone()
            if not row:
                return None
            return _coerce_db_date_to_ymd(row[0])

        def _daily_bars_by_source() -> dict[str, Any]:
            columns = _table_columns("daily_bars")
            if not columns or "date" not in columns:
                return {}
            source_expr = "COALESCE(source, 'unknown')" if "source" in columns else "'unknown'"
            rows = conn.execute(
                f"""
                SELECT
                    {source_expr} AS source_name,
                    MAX({_date_expr("date")}) AS latest_date,
                    COUNT(*) AS row_count,
                    COUNT(DISTINCT code) AS symbol_count
                FROM daily_bars
                GROUP BY 1
                ORDER BY latest_date DESC
                """
            ).fetchall()
            out: dict[str, Any] = {}
            for row in rows:
                source_name = str(row[0] or "unknown")
                latest_ymd = _coerce_db_date_to_ymd(row[1])
                out[source_name] = {
                    "latest_date": latest_ymd,
                    "latest_date_iso": _ymd_to_iso(latest_ymd),
                    "row_count": int(row[2] or 0),
                    "symbol_count": int(row[3] or 0),
                }
            return out

        daily_by_source = _daily_bars_by_source()
        confirmed_sources = [
            payload.get("latest_date")
            for source_name, payload in daily_by_source.items()
            if source_name.lower() != "yahoo" and isinstance(payload, dict)
        ]
        latest_confirmed_daily = max([int(value) for value in confirmed_sources if value is not None], default=None)

        return {
            "daily_bars": _latest_ymd_from_table("daily_bars"),
            "daily_bars_confirmed": latest_confirmed_daily,
            "daily_bars_by_source": daily_by_source,
            "feature_snapshot_daily": _latest_ymd_from_table("feature_snapshot_daily"),
            "ml_pred_20d": _latest_ymd_from_table("ml_pred_20d"),
        }


def get_runtime_stock_db_status() -> dict[str, Any]:
    from shared.runtime_stock_db_contract import inspect_runtime_stock_db, resolve_runtime_stock_db_selection

    selection = resolve_runtime_stock_db_selection()
    runtime_db_path = Path(selection["runtime_db_path"]).expanduser().resolve(strict=False)
    inspection = inspect_runtime_stock_db(runtime_db_path=runtime_db_path)
    table_dates = _inspect_latest_table_dates(runtime_db_path)
    freshness = _runtime_stock_db_freshness_state(inspection.get("latest_available_global_date"))
    latest_global_date = inspection.get("latest_available_global_date")
    return {
        "confirmed": True,
        "selected_runtime_db_path": str(runtime_db_path),
        "resolution_source": selection["resolution_source"],
        "resolution_reason": selection["resolution_reason"],
        "validated": bool(selection["validated"]),
        "db_exists": bool(selection["db_exists"]),
        "daily_bars_rows": selection.get("daily_bars_rows"),
        "market_regime_daily_rows": selection.get("market_regime_daily_rows"),
        "latest_available_global_date": latest_global_date,
        "latest_available_global_date_iso": inspection.get("latest_available_global_date_iso"),
        "latest_daily_bars_date": table_dates["daily_bars"],
        "latest_daily_bars_date_iso": _ymd_to_iso(table_dates["daily_bars"]),
        "latest_confirmed_daily_bars_date": table_dates["daily_bars_confirmed"],
        "latest_confirmed_daily_bars_date_iso": _ymd_to_iso(table_dates["daily_bars_confirmed"]),
        "daily_bars_by_source": table_dates["daily_bars_by_source"],
        "latest_feature_snapshot_daily_date": table_dates["feature_snapshot_daily"],
        "latest_feature_snapshot_daily_date_iso": _ymd_to_iso(table_dates["feature_snapshot_daily"]),
        "latest_ml_pred_20d_date": table_dates["ml_pred_20d"],
        "latest_ml_pred_20d_date_iso": _ymd_to_iso(table_dates["ml_pred_20d"]),
        "source_freshness_status": inspection.get("source_freshness_status"),
        "freshness_blocked": bool(inspection.get("freshness_blocked")),
        **freshness,
    }


def get_rankings_freshness(
    *,
    tf: str = "D",
    which: str = "latest",
    direction: str = "up",
    mode: str = "trade",
    risk_mode: str = "balanced",
    limit: int = 50,
) -> dict[str, Any]:
    tf = str(tf or "D").upper()
    which = str(which or "latest").lower()
    direction = str(direction or "up").lower()
    mode = str(mode or "trade").lower()
    risk_mode = _normalize_risk_mode(risk_mode)
    runtime_db = get_runtime_stock_db_status()
    try:
        payload = rankings_cache.get_rankings(tf, which, direction, int(limit), mode=mode, risk_mode=risk_mode)
    except Exception as exc:
        return {
            "confirmed": True,
            "ranking_endpoint_source_path": "app/backend/api/routers/rankings.py",
            "rankings_cache_contract_path": "app/backend/services/ml/rankings_cache.py",
            "tf": tf,
            "which": which,
            "direction": direction,
            "mode": mode,
            "risk_mode": risk_mode,
            "limit": int(limit),
            "snapshot_as_of": None,
            "freshness_state": "stale",
            "freshness_days": None,
            "stale": True,
            "current_candidate_available": False,
            "freshness_threshold_days": _freshness_threshold_days(),
            "runtime_db_path": runtime_db.get("selected_runtime_db_path"),
            "runtime_db_freshness_state": runtime_db.get("freshness_state"),
            "runtime_db_freshness_days": runtime_db.get("freshness_days"),
            "note": f"rankings cache unavailable: {exc}",
        }
    note = None
    if runtime_db.get("stale"):
        note = "runtime DB freshness is stale; rankings reflect stale local data"
    elif payload.get("stale"):
        confirmed_iso = runtime_db.get("latest_confirmed_daily_bars_date_iso")
        latest_iso = runtime_db.get("latest_available_global_date_iso")
        if confirmed_iso and latest_iso and confirmed_iso != latest_iso:
            note = (
                "confirmed rankings are stale because confirmed non-Yahoo daily bars lag latest runtime availability "
                f"({confirmed_iso} vs {latest_iso})"
            )
        else:
            note = "rankings cache is stale even though runtime DB is fresh"
    return {
        "confirmed": True,
        "ranking_endpoint_source_path": "app/backend/api/routers/rankings.py",
        "rankings_cache_contract_path": "app/backend/services/ml/rankings_cache.py",
        "tf": tf,
        "which": which,
        "direction": direction,
        "mode": mode,
        "risk_mode": risk_mode,
        "limit": int(limit),
        "snapshot_as_of": payload.get("snapshot_as_of"),
        "freshness_state": payload.get("freshness_state"),
        "freshness_days": payload.get("freshness_days"),
        "stale": bool(payload.get("stale")),
        "current_candidate_available": bool(payload.get("current_candidate_available")),
        "freshness_threshold_days": _freshness_threshold_days(),
        "runtime_db_path": runtime_db.get("selected_runtime_db_path"),
        "runtime_db_freshness_state": runtime_db.get("freshness_state"),
        "runtime_db_freshness_days": runtime_db.get("freshness_days"),
        "runtime_latest_available_global_date": runtime_db.get("latest_available_global_date_iso"),
        "runtime_latest_confirmed_daily_bars_date": runtime_db.get("latest_confirmed_daily_bars_date_iso"),
        "runtime_daily_bars_by_source": runtime_db.get("daily_bars_by_source"),
        "note": note,
    }


def _build_runtime_guard(*, risk_mode: str = "balanced") -> dict[str, Any]:
    runtime_status = get_runtime_stock_db_status()
    long_rankings = get_rankings_freshness(direction="up", risk_mode=risk_mode)
    short_rankings = get_rankings_freshness(direction="down", risk_mode=risk_mode)
    return {
        "runtime_stock_db_status": runtime_status,
        "rankings_freshness": {
            "long": long_rankings,
            "short": short_rankings,
        },
        "stale": bool(runtime_status.get("stale") or long_rankings.get("stale") or short_rankings.get("stale")),
    }


def _build_runtime_warnings(runtime_guard: dict[str, Any]) -> list[str]:
    warnings: list[str] = []
    runtime_status = runtime_guard.get("runtime_stock_db_status") or {}
    rankings_freshness = runtime_guard.get("rankings_freshness") or {}
    if bool(runtime_status.get("stale")):
        warnings.append("runtime DB freshness is stale")
    for key in ("long", "short"):
        status = rankings_freshness.get(key) or {}
        if bool(status.get("stale")):
            note = status.get("note")
            warnings.append(str(note) if note else f"rankings freshness is stale for {key}")
    return list(dict.fromkeys(warnings))


def _row_to_datetime(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    text = str(value).strip()
    if not text:
        return None
    text = text.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def _safe_float(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return None
    try:
        parsed = float(value)
    except Exception:
        return None
    return parsed if parsed == parsed else None


def _safe_int(value: Any) -> int | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        return int(value)
    except Exception:
        return None


def _normalize_ml_pred_item(row: tuple[Any, ...] | None) -> dict[str, Any] | None:
    if not row:
        return None
    return {
        "dt": row[0],
        "pUp": _safe_float(row[1]) if len(row) > 1 else None,
        "pDown": _safe_float(row[2]) if len(row) > 2 else None,
        "pUp5": _safe_float(row[3]) if len(row) > 3 else None,
        "pUp10": _safe_float(row[4]) if len(row) > 4 else None,
        "pTurnUp": _safe_float(row[5]) if len(row) > 5 else None,
        "pTurnDown": _safe_float(row[6]) if len(row) > 6 else None,
        "pTurnDown5": _safe_float(row[7]) if len(row) > 7 else None,
        "pTurnDown10": _safe_float(row[8]) if len(row) > 8 else None,
        "pTurnDown20": _safe_float(row[9]) if len(row) > 9 else None,
        "retPred5": _safe_float(row[10]) if len(row) > 10 else None,
        "retPred10": _safe_float(row[11]) if len(row) > 11 else None,
        "retPred20": _safe_float(row[12]) if len(row) > 12 else None,
        "ev20": _safe_float(row[13]) if len(row) > 13 else None,
        "ev20Net": _safe_float(row[14]) if len(row) > 14 else None,
        "ev5Net": _safe_float(row[15]) if len(row) > 15 else None,
        "ev10Net": _safe_float(row[16]) if len(row) > 16 else None,
        "modelVersion": str(row[17]).strip() if len(row) > 17 and row[17] is not None else None,
    }


def _normalize_sell_item(row: tuple[Any, ...] | None) -> dict[str, Any] | None:
    if not row:
        return None
    return {
        "dt": row[0],
        "close": _safe_float(row[1]) if len(row) > 1 else None,
        "dayChangePct": _safe_float(row[2]) if len(row) > 2 else None,
        "pDown": _safe_float(row[3]) if len(row) > 3 else None,
        "pTurnDown": _safe_float(row[4]) if len(row) > 4 else None,
        "ev20Net": _safe_float(row[5]) if len(row) > 5 else None,
        "rankDown20": _safe_float(row[6]) if len(row) > 6 else None,
        "predDt": row[7] if len(row) > 7 else None,
        "pUp5": _safe_float(row[8]) if len(row) > 8 else None,
        "pUp10": _safe_float(row[9]) if len(row) > 9 else None,
        "pUp20": _safe_float(row[10]) if len(row) > 10 else None,
        "shortScore": _safe_float(row[11]) if len(row) > 11 else None,
        "aScore": _safe_float(row[12]) if len(row) > 12 else None,
        "bScore": _safe_float(row[13]) if len(row) > 13 else None,
        "ma20": _safe_float(row[14]) if len(row) > 14 else None,
        "ma60": _safe_float(row[15]) if len(row) > 15 else None,
        "ma20Slope": _safe_float(row[16]) if len(row) > 16 else None,
        "ma60Slope": _safe_float(row[17]) if len(row) > 17 else None,
        "distMa20Signed": _safe_float(row[18]) if len(row) > 18 else None,
        "distMa60Signed": _safe_float(row[19]) if len(row) > 19 else None,
        "trendDown": bool(row[20]) if len(row) > 20 and row[20] is not None else None,
        "trendDownStrict": bool(row[21]) if len(row) > 21 and row[21] is not None else None,
        "fwdClose5": _safe_float(row[22]) if len(row) > 22 else None,
        "fwdClose10": _safe_float(row[23]) if len(row) > 23 else None,
        "fwdClose20": _safe_float(row[24]) if len(row) > 24 else None,
        "shortRet5": _safe_float(row[25]) if len(row) > 25 else None,
        "shortRet10": _safe_float(row[26]) if len(row) > 26 else None,
        "shortRet20": _safe_float(row[27]) if len(row) > 27 else None,
        "shortWin5": bool(row[28]) if len(row) > 28 and row[28] is not None else None,
        "shortWin10": bool(row[29]) if len(row) > 29 and row[29] is not None else None,
        "shortWin20": bool(row[30]) if len(row) > 30 and row[30] is not None else None,
    }


def _normalize_tdnet_item(item: dict[str, Any]) -> dict[str, Any]:
    return {
        "disclosureId": item.get("disclosureId"),
        "secCode": item.get("secCode"),
        "companyName": item.get("companyName"),
        "title": item.get("title"),
        "category": item.get("category"),
        "publishedAt": item.get("publishedAt"),
        "eventType": item.get("eventType"),
        "sentiment": item.get("sentiment"),
        "importanceScore": _safe_float(item.get("importanceScore")),
        "tdnetUrl": item.get("tdnetUrl"),
        "pdfUrl": item.get("pdfUrl"),
        "xbrlUrl": item.get("xbrlUrl"),
        "summaryText": item.get("summaryText"),
    }


def _normalize_edinet_official_item(item: dict[str, Any]) -> dict[str, Any]:
    return {
        "docId": item.get("doc_id"),
        "secCode": item.get("sec_code"),
        "edinetCode": item.get("edinet_code"),
        "filerName": item.get("filer_name"),
        "formCode": item.get("form_code"),
        "docTypeCode": item.get("doc_type_code"),
        "periodStart": item.get("period_start"),
        "periodEnd": item.get("period_end"),
        "submitDatetime": item.get("submit_datetime"),
        "docDescription": item.get("doc_description"),
        "legalStatus": item.get("legal_status"),
        "csvFlag": item.get("csv_flag"),
        "pdfFlag": item.get("pdf_flag"),
        "xbrlFlag": item.get("xbrl_flag"),
    }


def _build_section_payload(*, available: bool, reason: str | None, **kwargs: Any) -> dict[str, Any]:
    return {"available": available, "reason": reason, **kwargs}


def _build_stock_analysis_section(code: str, asof_dt: int | None, risk_mode: str) -> dict[str, Any]:
    repo = get_stock_repo()
    row = None
    try:
        row = repo.get_ml_analysis_pred(code, asof_dt)
    except Exception as exc:
        return _build_section_payload(available=False, reason=f"analysis_unavailable:{exc}", item=None)
    if not row:
        return _build_section_payload(available=False, reason="analysis_missing", item=None)
    ml_pred = _normalize_ml_pred_item(row)
    sell_row = None
    try:
        sell_row = repo.get_sell_analysis_snapshot(code, asof_dt)
    except Exception:
        sell_row = None
    sell_context = _build_sell_context_from_row(sell_row)
    buy_stage_precision = None
    try:
        buy_stage_precision = repo.get_buy_stage_precision(code, asof_dt, lookback_bars=360, horizon=20)
    except Exception:
        buy_stage_precision = None
    decision = build_analysis_decision(
        analysis_p_up=ml_pred.get("pUp") if ml_pred else None,
        analysis_p_down=ml_pred.get("pDown") if ml_pred else None,
        analysis_p_turn_up=ml_pred.get("pTurnUp") if ml_pred else None,
        analysis_p_turn_down=ml_pred.get("pTurnDown") if ml_pred else None,
        analysis_ev_net=ml_pred.get("ev20Net") if ml_pred else None,
        playbook_up_score_bonus=None,
        playbook_down_score_bonus=None,
        additive_signals=None,
        sell_analysis=sell_context if isinstance(sell_context, dict) else None,
    )
    item = {
        "code": code,
        "asof": _ymd_to_iso(_safe_int(datetime.fromtimestamp(asof_dt, tz=timezone.utc).strftime("%Y%m%d")) if asof_dt is not None else None),
        "riskMode": risk_mode,
        "mlPred": ml_pred,
        "sellAnalysis": sell_context,
        "buyStagePrecision": buy_stage_precision,
        "decision": decision,
    }
    return _build_section_payload(available=True, reason=None, item=item)


def _build_sell_analysis_section(code: str, asof_dt: int | None) -> dict[str, Any]:
    repo = get_stock_repo()
    try:
        row = repo.get_sell_analysis_snapshot(code, asof_dt)
    except Exception as exc:
        return _build_section_payload(available=False, reason=f"sell_analysis_unavailable:{exc}", item=None)
    item = _normalize_sell_item(row)
    if not item:
        return _build_section_payload(available=False, reason="sell_analysis_missing", item=None)
    return _build_section_payload(available=True, reason=None, item=item)


def _build_edinet_summary_section(code: str, asof_dt: int | None) -> dict[str, Any]:
    try:
        payload = _build_edinet_summary(code, asof_dt)
    except Exception as exc:
        return _build_section_payload(available=False, reason=f"edinet_summary_unavailable:{exc}", item=None)
    if not payload:
        return _build_section_payload(available=False, reason="edinet_summary_missing", item=None)
    return _build_section_payload(available=True, reason=None, item=payload)


def _build_edinet_financials_section(code: str) -> dict[str, Any]:
    try:
        payload = _build_edinet_financials_payload(code)
    except Exception as exc:
        return _build_section_payload(available=False, reason=f"edinet_financials_unavailable:{exc}", item=None)
    if not payload:
        return _build_section_payload(available=False, reason="edinet_financials_missing", item=None)
    return _build_section_payload(available=True, reason=None, item=payload)


def _build_tdnet_disclosures_section(code: str) -> dict[str, Any]:
    repo = get_tdnet_repo()
    try:
        items = repo.list_disclosures_by_code(code, limit=_MAX_TDNET_DISCLOSURES)
    except Exception as exc:
        return _build_section_payload(available=False, reason=f"tdnet_disclosures_unavailable:{exc}", items=[])
    normalized = [_normalize_tdnet_item(item) for item in items[:_MAX_TDNET_DISCLOSURES]]
    reason = None if normalized else "tdnet_disclosures_missing"
    return _build_section_payload(available=bool(normalized), reason=reason, items=normalized)


def _build_taisyaku_snapshot_section(code: str) -> dict[str, Any]:
    try:
        snapshot = load_taisyaku_snapshot(code, history_limit=_MAX_TAISYAKU_HISTORY)
    except Exception as exc:
        return _build_section_payload(available=False, reason=f"taisyaku_snapshot_unavailable:{exc}", item=None)
    if not snapshot:
        return _build_section_payload(available=False, reason="taisyaku_snapshot_missing", item=None)
    return _build_section_payload(available=True, reason=None, item=snapshot)


def _build_tradex_analysis_bridge_status() -> dict[str, Any]:
    try:
        snapshot = get_analysis_bridge_snapshot()
    except Exception as exc:
        return _build_section_payload(available=False, reason=f"analysis_bridge_unavailable:{exc}", item=None)
    if snapshot.get("degraded"):
        return _build_section_payload(
            available=False,
            reason=str(snapshot.get("degrade_reason") or "analysis_bridge_degraded"),
            item=snapshot,
        )
    return _build_section_payload(available=True, reason=None, item=snapshot)


def _build_tradex_detail_section(code: str, asof_dt: int | None) -> dict[str, Any]:
    repo = get_stock_repo()
    try:
        snapshot = build_tradex_detail_analysis_snapshot(code=code, asof_dt=asof_dt, repo=repo, enabled=None)
    except Exception as exc:
        return _build_section_payload(available=False, reason=f"tradex_detail_unavailable:{exc}", item=None, fallback_used=True)
    available = bool(snapshot.get("available"))
    fallback_used = bool(snapshot.get("forecast_surface", {}).get("fallback")) if isinstance(snapshot.get("forecast_surface"), dict) else False
    reason = None if available else str(snapshot.get("reason") or "tradex_detail_unavailable")
    return _build_section_payload(available=available, reason=reason, item=snapshot, fallback_used=fallback_used)


def _build_tradex_forecast_surface_section(detail_snapshot: dict[str, Any] | None) -> dict[str, Any]:
    if not detail_snapshot:
        return _build_section_payload(available=False, reason="forecast_surface_missing", item=None, fallback_used=True)
    forecast_surface = detail_snapshot.get("forecast_surface")
    if not isinstance(forecast_surface, dict):
        return _build_section_payload(available=False, reason="forecast_surface_missing", item=None, fallback_used=True)
    available = bool(forecast_surface.get("available"))
    fallback_used = bool(forecast_surface.get("fallback"))
    reason = None if available else str(forecast_surface.get("reason") or "forecast_surface_unavailable")
    return _build_section_payload(available=available, reason=reason, item=forecast_surface, fallback_used=fallback_used)


def _build_tradex_state_eval_rows(code: str) -> dict[str, Any]:
    try:
        snapshot = get_state_eval_rows(code=code, limit=10)
    except Exception as exc:
        return _build_section_payload(available=False, reason=f"state_eval_unavailable:{exc}", rows=[])
    rows = list(snapshot.get("rows") or [])
    return _build_section_payload(
        available=bool(rows),
        reason=None if rows else str(snapshot.get("degrade_reason") or snapshot.get("reason") or "state_eval_missing"),
        rows=rows,
        status=snapshot,
    )


def _build_tradex_similar_cases(code: str) -> dict[str, Any]:
    try:
        snapshot = get_similar_cases_rows(code=code, limit=_MAX_SIMILAR_CASES)
    except Exception as exc:
        return _build_section_payload(available=False, reason=f"similar_cases_unavailable:{exc}", rows=[], count=0)
    rows = list(snapshot.get("rows") or [])[:_MAX_SIMILAR_CASES]
    return _build_section_payload(
        available=bool(rows),
        reason=None if rows else str(snapshot.get("degrade_reason") or snapshot.get("reason") or "similar_cases_missing"),
        rows=rows,
        count=len(rows),
        status=snapshot,
    )


def _build_tradex_promotion_review() -> dict[str, Any]:
    try:
        snapshot = get_internal_state_eval_promotion_review()
    except Exception as exc:
        return _build_section_payload(available=False, reason=f"promotion_review_unavailable:{exc}", item=None)
    review = snapshot.get("review")
    if not review:
        return _build_section_payload(
            available=False,
            reason=str(snapshot.get("degrade_reason") or "promotion_review_missing"),
            item=snapshot,
        )
    return _build_section_payload(available=True, reason=None, item=snapshot)


def _build_event_risk_section(code: str, asof_date: date, asof_dt: int | None) -> dict[str, Any]:
    tdnet_section = _build_tdnet_disclosures_section(code)
    edinet_items: list[dict[str, Any]] = []
    try:
        repo = get_edinet_repo()
        edinet_code = ""
        try:
            edinet_code = str(repo.lookup_edinet_codes([code]).get(code) or "").strip()
        except Exception:
            edinet_code = ""
        filings = repo.list_official_documents(sec_code=code, edinet_code=edinet_code or None, limit=3)
        edinet_items = [_normalize_edinet_official_item(item) for item in filings[:3]]
    except Exception:
        edinet_items = []
    tdnet_recent = list(tdnet_section.get("items") or [])[:3]
    rights_warning = None
    taisyaku_snapshot = _build_taisyaku_snapshot_section(code)
    taisyaku_item = taisyaku_snapshot.get("item") if isinstance(taisyaku_snapshot, dict) else None
    if isinstance(taisyaku_item, dict):
        restrictions = list(taisyaku_item.get("restrictions") or [])
        rights_warning = restrictions[0] if restrictions else None
    item = {
        "tdnet_recent": tdnet_recent,
        "edinet_recent": edinet_items,
        "rights_warning": rights_warning,
    }
    available = bool(tdnet_recent or edinet_items or rights_warning)
    reason = None if available else "event_risk_missing"
    return _build_section_payload(available=available, reason=reason, item=item)


def _build_supply_demand_risk_section(code: str) -> dict[str, Any]:
    taisyaku_snapshot = _build_taisyaku_snapshot_section(code)
    snapshot_item = taisyaku_snapshot.get("item") if isinstance(taisyaku_snapshot, dict) else None
    borrow_cost_warning = None
    blocking_reasons: list[str] = []
    if isinstance(snapshot_item, dict):
        latest_fee = snapshot_item.get("latestFee") or {}
        latest_balance = snapshot_item.get("latestBalance") or {}
        restrictions = list(snapshot_item.get("restrictions") or [])
        current_fee = _safe_float(latest_fee.get("currentFeeYen"))
        loan_ratio = _safe_float(latest_balance.get("loanRatio"))
        if restrictions:
            blocking_reasons.append("restriction_notice")
        if current_fee is not None and current_fee > 0:
            blocking_reasons.append("current_fee_positive")
        if loan_ratio is not None and loan_ratio >= 1.0:
            blocking_reasons.append("loan_ratio_high")
        if blocking_reasons:
            borrow_cost_warning = ";".join(blocking_reasons)
    available = bool(snapshot_item)
    reason = None if available else "taisyaku_snapshot_missing"
    return _build_section_payload(available=available, reason=reason, item=snapshot_item, borrow_cost_warning=borrow_cost_warning)


def _build_meemee_bundle(code: str, asof_dt: int | None, risk_mode: str) -> dict[str, Any]:
    analysis = _build_stock_analysis_section(code, asof_dt, risk_mode)
    sell_analysis = _build_sell_analysis_section(code, asof_dt)
    edinet_summary = _build_edinet_summary_section(code, asof_dt)
    edinet_financials = _build_edinet_financials_section(code)
    tdnet_disclosures = _build_tdnet_disclosures_section(code)
    taisyaku_snapshot = _build_taisyaku_snapshot_section(code)
    sub_sections = [analysis, sell_analysis, edinet_summary, edinet_financials, tdnet_disclosures, taisyaku_snapshot]
    available = any(bool(section.get("available")) for section in sub_sections)
    reason = None if available else "no_meemee_read_data"
    return {
        "owner": "MeeMee",
        "available": available,
        "reason": reason,
        "analysis": analysis,
        "sell_analysis": sell_analysis,
        "edinet_summary": edinet_summary,
        "edinet_financials": edinet_financials,
        "tdnet_disclosures": tdnet_disclosures,
        "taisyaku_snapshot": taisyaku_snapshot,
    }


def _build_tradex_bundle(code: str, asof_dt: int | None) -> dict[str, Any]:
    analysis_bridge_status = _build_tradex_analysis_bridge_status()
    detail_analysis = _build_tradex_detail_section(code, asof_dt)
    forecast_surface = _build_tradex_forecast_surface_section(detail_analysis.get("item") if isinstance(detail_analysis, dict) else None)
    state_eval_rows = _build_tradex_state_eval_rows(code)
    similar_cases = _build_tradex_similar_cases(code)
    promotion_review = _build_tradex_promotion_review()
    sub_sections = [analysis_bridge_status, detail_analysis, forecast_surface, state_eval_rows, similar_cases, promotion_review]
    available = any(bool(section.get("available")) for section in sub_sections)
    reason = None if available else "no_tradex_read_data"
    return {
        "owner": "TRADEX",
        "available": available,
        "reason": reason,
        "analysis_bridge_status": analysis_bridge_status,
        "detail_analysis": detail_analysis,
        "forecast_surface": forecast_surface,
        "state_eval_rows": state_eval_rows,
        "similar_cases": similar_cases,
        "promotion_review": promotion_review,
    }


def build_stock_analysis_bundle(*, code: str, asof: Any = None, risk_mode: Any = "balanced") -> dict[str, Any]:
    code_key = _normalize_code(code)
    risk_mode_key = _normalize_risk_mode(risk_mode)
    parsed_asof: date | None = None
    asof_ymd: int | None = None
    asof_iso: str | None = None
    asof_dt: int | None = None
    if asof is not None:
        parsed_asof, asof_ymd, asof_iso, asof_dt = _parse_asof(asof)
    runtime_guard = _build_runtime_guard(risk_mode=risk_mode_key)
    warnings = _build_runtime_warnings(runtime_guard)
    if bool((runtime_guard.get("runtime_stock_db_status") or {}).get("stale")):
        warnings.insert(0, "runtime DB freshness is stale")
    meemee = _build_meemee_bundle(code_key, asof_dt, risk_mode_key)
    tradex = _build_tradex_bundle(code_key, asof_dt)
    if bool((tradex.get("analysis_bridge_status") or {}).get("available")) is False and bool((tradex.get("analysis_bridge_status") or {}).get("reason")):
        warnings.append(str((tradex.get("analysis_bridge_status") or {}).get("reason")))
    return {
        "confirmed": True,
        "code": code_key,
        "asof": {"requested": asof_iso if asof_iso else None, "resolved_dt": asof_dt, "resolved_ymd": asof_ymd},
        "risk_mode": risk_mode_key,
        "runtime_guard": runtime_guard,
        "warnings": list(dict.fromkeys(warnings)),
        "meemee": meemee,
        "tradex": tradex,
    }


def _ranking_snapshot_meta(payload: dict[str, Any]) -> dict[str, Any]:
    return {
        "snapshot_as_of": payload.get("snapshot_as_of"),
        "freshness_state": payload.get("freshness_state"),
        "candidate_source": payload.get("candidate_source"),
        "legacy_analysis_disabled": bool(payload.get("legacy_analysis_disabled")),
        "mode": payload.get("mode"),
        "risk_mode": payload.get("risk_mode"),
        "stale": bool(payload.get("stale")),
    }


def _rank_item_score(item: dict[str, Any]) -> float:
    value = _safe_float(item.get("tradePriorityScore"))
    return value if value is not None else float("-inf")


def _selected_direction_label(direction: str | None) -> str | None:
    if direction == "up":
        return "long"
    if direction == "down":
        return "short"
    return None


def _trade_side_label(direction: str | None) -> str | None:
    if direction in {"long", "up"}:
        return "up"
    if direction in {"short", "down"}:
        return "down"
    return None


def _public_action_state(value: Any) -> str:
    return _normalize_text(value).lower()


def _load_rankings_for_direction(*, asof_ymd: int, direction: str, risk_mode: str, limit: int) -> dict[str, Any]:
    try:
        return rankings_cache.get_rankings_asof("D", "latest", direction, limit, as_of=asof_ymd, mode="trade", risk_mode=risk_mode)
    except Exception as exc:
        return {
            "tf": "D",
            "which": "latest",
            "dir": direction,
            "mode": "trade",
            "risk_mode": risk_mode,
            "legacy_analysis_disabled": None,
            "candidate_source": None,
            "requested_as_of": f"{asof_ymd:08d}",
            "items": [],
            "stale": True,
            "freshness_state": "stale",
            "freshness_days": None,
            "snapshot_as_of": _ymd_to_iso(asof_ymd),
            "reason": str(exc),
        }


def _build_meemee_summary_candidate(
    *,
    code: str,
    ranking_item: dict[str, Any] | None,
    selected_direction: str | None,
    state_eval_row: dict[str, Any] | None,
) -> dict[str, Any]:
    available = ranking_item is not None
    reason = None if available else "not_in_ranking_snapshot"
    row = state_eval_row or {}
    return {
        "available": available,
        "reason": reason,
        "machine_action_state": row.get("machine_action_state"),
        "human_readable_judgement": row.get("human_readable_judgement"),
        "buy_score": _safe_float(row.get("buy_score")),
        "environment_score": _safe_float(row.get("environment_score")),
        "trend_score": _safe_float(row.get("trend_score")),
        "trigger_score": _safe_float(row.get("trigger_score")),
        "risk_score": _safe_float(row.get("risk_score")),
        "invalidation_price": _safe_float(row.get("invalidation_price")),
        "ranking_setup_type": ranking_item.get("setupType") if ranking_item else None,
        "ranking_trade_priority_score": _safe_float(ranking_item.get("tradePriorityScore")) if ranking_item else None,
        "selected_direction": selected_direction,
    }


def _event_recent_flags(
    *,
    asof_date: date,
    tdnet_items: list[dict[str, Any]],
    edinet_items: list[dict[str, Any]],
) -> tuple[list[str], list[str]]:
    tdnet_flags: list[str] = []
    edinet_flags: list[str] = []
    for item in tdnet_items:
        published = _row_to_datetime(item.get("publishedAt"))
        if published is None:
            continue
        days = (asof_date - published.date()).days
        if 0 <= days <= 5 and (_safe_float(item.get("importanceScore")) or 0.0) >= 0.7:
            tdnet_flags.append("recent_tdnet_high_importance")
            break
    for item in edinet_items:
        submit = _row_to_datetime(item.get("submitDatetime"))
        if submit is None:
            continue
        days = (asof_date - submit.date()).days
        if 0 <= days <= 14:
            edinet_flags.append("recent_edinet_filing")
            break
    return tdnet_flags, edinet_flags


def _build_candidate_event_risk(
    *,
    code: str,
    asof_date: date,
) -> dict[str, Any]:
    tdnet_items = _build_tdnet_disclosures_section(code).get("items") or []
    edinet_items = []
    try:
        repo = get_edinet_repo()
        edinet_code = ""
        try:
            edinet_code = str(repo.lookup_edinet_codes([code]).get(code) or "").strip()
        except Exception:
            edinet_code = ""
        filings = repo.list_official_documents(sec_code=code, edinet_code=edinet_code or None, limit=3)
        edinet_items = [_normalize_edinet_official_item(item) for item in filings[:3]]
    except Exception:
        edinet_items = []
    taishaku_section = _build_taisyaku_snapshot_section(code)
    taishaku = taishaku_section.get("item") if isinstance(taishaku_section, dict) else None
    rights_warning = None
    if isinstance(taishaku, dict):
        restrictions = list(taishaku.get("restrictions") or [])
        rights_warning = restrictions[0] if restrictions else None
    tdnet_flags, edinet_flags = _event_recent_flags(asof_date=asof_date, tdnet_items=tdnet_items, edinet_items=edinet_items)
    available = bool(tdnet_items or edinet_items or rights_warning)
    return {
        "available": available,
        "reason": None if available else "event_risk_missing",
        "tdnet_recent": tdnet_items[:3],
        "edinet_recent": edinet_items[:3],
        "rights_warning": rights_warning,
        "recent_tdnet_high_importance": bool(tdnet_flags),
        "recent_edinet_filing": bool(edinet_flags),
    }


def _build_candidate_supply_demand_risk(*, code: str) -> dict[str, Any]:
    taishaku_section = _build_taisyaku_snapshot_section(code)
    snapshot = taishaku_section.get("item") if isinstance(taishaku_section, dict) else None
    borrow_cost_warning = None
    if isinstance(snapshot, dict):
        latest_fee = snapshot.get("latestFee") or {}
        latest_balance = snapshot.get("latestBalance") or {}
        restrictions = list(snapshot.get("restrictions") or [])
        current_fee = _safe_float(latest_fee.get("currentFeeYen"))
        loan_ratio = _safe_float(latest_balance.get("loanRatio"))
        reasons: list[str] = []
        if restrictions:
            reasons.append("taisyaku_restriction_notice")
        if current_fee is not None and current_fee > 0:
            reasons.append("borrow_cost_positive")
        if loan_ratio is not None and loan_ratio >= 1.0:
            reasons.append("loan_ratio_high")
        if reasons:
            borrow_cost_warning = ";".join(reasons)
    return {
        "available": bool(snapshot),
        "reason": None if snapshot else "taisyaku_snapshot_missing",
        "taisyaku_snapshot": snapshot,
        "borrow_cost_warning": borrow_cost_warning,
    }


def _build_candidate_tradex_summary(*, code: str, detail_section: dict[str, Any], state_eval_rows: dict[str, Any], similar_cases: dict[str, Any]) -> dict[str, Any]:
    detail_available = bool(detail_section.get("available"))
    state_eval_available = bool(list(state_eval_rows.get("rows") or []))
    similar_cases_count = int(similar_cases.get("count") or len(similar_cases.get("rows") or []))
    fallback_used = bool(detail_section.get("fallback_used")) or not detail_available
    available = bool(detail_available or state_eval_available or similar_cases_count)
    return {
        "available": available,
        "reason": None if available else "tradex_summary_unavailable",
        "detail_analysis_available": detail_available,
        "state_eval_available": state_eval_available,
        "similar_cases_count": similar_cases_count,
        "fallback_used": fallback_used,
    }


def _candidate_blocking_flags(
    *,
    ranking_item: dict[str, Any] | None,
    selected_direction: str | None,
    event_risk: dict[str, Any],
    supply_demand_risk: dict[str, Any],
    tradex_summary: dict[str, Any],
) -> list[str]:
    flags: list[str] = []
    if ranking_item is None:
        flags.append("entry_fallback_only")
    else:
        if bool(ranking_item.get("entryQualifiedByFallback")) or str(ranking_item.get("entryQualifiedFallbackStage") or "").strip():
            flags.append("entry_fallback_only")
    if selected_direction is None and ranking_item is not None:
        flags.append("entry_fallback_only")
    if bool(event_risk.get("recent_tdnet_high_importance")):
        flags.append("recent_tdnet_high_importance")
    if bool(event_risk.get("recent_edinet_filing")):
        flags.append("recent_edinet_filing")
    rights_warning = event_risk.get("rights_warning")
    if rights_warning:
        flags.append("taisyaku_restriction_notice")
    warning = supply_demand_risk.get("borrow_cost_warning")
    if warning:
        if "borrow_cost_positive" in warning:
            flags.append("borrow_cost_positive")
        if "loan_ratio_high" in warning:
            flags.append("loan_ratio_high")
        if "taisyaku_restriction_notice" in warning:
            flags.append("taisyaku_restriction_notice")
    if not bool(tradex_summary.get("detail_analysis_available")):
        flags.append("tradex_detail_unavailable")
    return list(dict.fromkeys(flags))


def _candidate_confidence_band(
    *,
    ranking_item: dict[str, Any] | None,
    blocking_flags: list[str],
    tradex_summary: dict[str, Any],
) -> str:
    if blocking_flags:
        return "low"
    score = _safe_float(ranking_item.get("tradePriorityScore")) if ranking_item else None
    if (
        score is not None
        and score >= 0.80
        and not bool(tradex_summary.get("fallback_used"))
        and bool(tradex_summary.get("detail_analysis_available"))
        and bool(tradex_summary.get("state_eval_available"))
    ):
        return "high"
    if score is not None and score >= 0.70:
        return "medium"
    return "low"


def _candidate_action_label(
    *,
    ranking_item: dict[str, Any] | None,
    selected_direction: str | None,
    blocking_flags: list[str],
    meemee_summary: dict[str, Any],
) -> str:
    if blocking_flags:
        return "avoid"
    judgement = _public_action_state(meemee_summary.get("human_readable_judgement"))
    machine_state = _public_action_state(meemee_summary.get("machine_action_state"))
    setup_type = _public_action_state(ranking_item.get("setupType") if ranking_item else None)
    if judgement in {"skip", "reject"} or machine_state in {"skip", "reject"} or setup_type in {"reject"}:
        return "avoid"
    if judgement in {"wait", "hold", "watch"} or machine_state in {"wait", "hold", "watch"} or setup_type in {"watch"}:
        return "hold"
    if ranking_item is not None and ranking_item.get("entryQualified") is not True:
        return "hold"
    if selected_direction == "long":
        return "buy"
    if selected_direction == "short":
        return "sell"
    return "hold"


def _candidate_reasons(
    *,
    ranking_item: dict[str, Any] | None,
    selected_direction: str | None,
    meemee_summary: dict[str, Any],
    event_risk: dict[str, Any],
    supply_demand_risk: dict[str, Any],
    tradex_summary: dict[str, Any],
    action_label: str,
    confidence_band: str,
) -> list[str]:
    reasons: list[str] = []
    if selected_direction:
        reasons.append(f"direction={selected_direction}")
    if ranking_item is not None:
        score = _safe_float(ranking_item.get("tradePriorityScore"))
        if score is not None:
            reasons.append(f"tradePriorityScore={score:.2f}")
        setup_type = _normalize_text(ranking_item.get("setupType"))
        if setup_type:
            reasons.append(f"setup={setup_type}")
    judgement = _public_action_state(meemee_summary.get("human_readable_judgement"))
    if judgement:
        reasons.append(f"judgement={judgement}")
    machine_state = _public_action_state(meemee_summary.get("machine_action_state"))
    if machine_state:
        reasons.append(f"state={machine_state}")
    for flag in event_risk.get("recent_tdnet_high_importance"), event_risk.get("recent_edinet_filing"):
        if flag:
            reasons.append(str(flag))
    warning = supply_demand_risk.get("borrow_cost_warning")
    if warning:
        reasons.append(str(warning))
    if bool(tradex_summary.get("fallback_used")):
        reasons.append("tradex_fallback")
    reasons.append(f"action={action_label}")
    reasons.append(f"confidence={confidence_band}")
    return reasons


def _build_candidate_payload(
    *,
    code: str,
    selected_direction: str | None,
    rank: int | None,
    ranking_item: dict[str, Any] | None,
    asof_date: date,
) -> dict[str, Any]:
    state_eval_snapshot = {}
    try:
        state_eval_snapshot = get_state_eval_rows(code=code, limit=10)
    except Exception:
        state_eval_snapshot = {"rows": []}
    state_eval_row = None
    for row in list(state_eval_snapshot.get("rows") or []):
        row_side = _normalize_text(row.get("side")).lower()
        if selected_direction and row_side in {selected_direction, _trade_side_label(selected_direction)}:
            state_eval_row = row
            break
    if state_eval_row is None:
        state_eval_row = next((row for row in list(state_eval_snapshot.get("rows") or []) if _normalize_text(row.get("code")) == code), None)
    meemee_summary = _build_meemee_summary_candidate(
        code=code,
        ranking_item=ranking_item,
        selected_direction=selected_direction,
        state_eval_row=state_eval_row,
    )
    event_risk = _build_candidate_event_risk(code=code, asof_date=asof_date)
    supply_demand_risk = _build_candidate_supply_demand_risk(code=code)
    detail_section = _build_tradex_detail_section(code, int(datetime(asof_date.year, asof_date.month, asof_date.day, tzinfo=timezone.utc).timestamp()))
    similar_cases = _build_tradex_similar_cases(code)
    tradex_summary = _build_candidate_tradex_summary(
        code=code,
        detail_section=detail_section,
        state_eval_rows=state_eval_snapshot,
        similar_cases=similar_cases,
    )
    blocking_flags = _candidate_blocking_flags(
        ranking_item=ranking_item,
        selected_direction=selected_direction,
        event_risk=event_risk,
        supply_demand_risk=supply_demand_risk,
        tradex_summary=tradex_summary,
    )
    confidence_band = _candidate_confidence_band(
        ranking_item=ranking_item,
        blocking_flags=blocking_flags,
        tradex_summary=tradex_summary,
    )
    action_label = _candidate_action_label(
        ranking_item=ranking_item,
        selected_direction=selected_direction,
        blocking_flags=blocking_flags,
        meemee_summary=meemee_summary,
    )
    reasons = _candidate_reasons(
        ranking_item=ranking_item,
        selected_direction=selected_direction,
        meemee_summary=meemee_summary,
        event_risk=event_risk,
        supply_demand_risk=supply_demand_risk,
        tradex_summary=tradex_summary,
        action_label=action_label,
        confidence_band=confidence_band,
    )
    owner = "MeeMee"
    return {
        "code": code,
        "rank": rank,
        "selected_direction": selected_direction,
        "owner": owner,
        "meemee_summary": meemee_summary,
        "event_risk": event_risk,
        "supply_demand_risk": supply_demand_risk,
        "tradex_summary": tradex_summary,
        "actionability": {
            "action_label": action_label,
            "confidence_band": confidence_band,
            "reasons": reasons,
            "blocking_flags": blocking_flags,
        },
    }


def _top_n_direction_candidates(
    *,
    payload: dict[str, Any],
    selected_side: str,
    top_n: int,
) -> list[dict[str, Any]]:
    items = list(payload.get("items") or [])
    if selected_side == "long":
        target_items = items[:top_n]
    else:
        target_items = items[:top_n]
    out: list[dict[str, Any]] = []
    for index, item in enumerate(target_items):
        code = _normalize_text(item.get("code"))
        if not code:
            continue
        out.append(
            {
                "code": code,
                "rank": index + 1,
                "selected_direction": selected_side,
                "ranking_item": item,
            }
        )
    return out


def _combine_both_side_candidates(
    *,
    long_payload: dict[str, Any],
    short_payload: dict[str, Any],
    top_n: int,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], set[str]]:
    long_items = list(long_payload.get("items") or [])[:top_n]
    short_items = list(short_payload.get("items") or [])[:top_n]
    combined: dict[str, dict[str, Any]] = {}
    boundary_items: list[dict[str, Any]] = []
    conflict_codes: set[str] = set()
    for side, items in (("long", long_items), ("short", short_items)):
        for index, item in enumerate(items):
            code = _normalize_text(item.get("code"))
            if not code:
                continue
            entry = {
                "code": code,
                "rank": index + 1,
                "selected_direction": side,
                "ranking_item": item,
            }
            boundary_items.append(entry)
            existing = combined.get(code)
            if existing is None:
                combined[code] = entry
                continue
            conflict_codes.add(code)
            existing_score = _rank_item_score(existing["ranking_item"])
            new_score = _rank_item_score(item)
            if new_score > existing_score:
                combined[code] = entry
            elif new_score == existing_score and _safe_int(entry["rank"]) is not None and _safe_int(existing["rank"]) is not None:
                if int(entry["rank"]) < int(existing["rank"]):
                    combined[code] = entry
    ordered = sorted(combined.values(), key=lambda row: (row["selected_direction"], row["rank"], row["code"]))
    return ordered, boundary_items, conflict_codes


def _top_boundary_observability(payload: dict[str, Any], *, top_n: int) -> dict[str, Any]:
    items = list(payload.get("items") or [])
    if not items:
        return {}
    cutoff_item = items[min(top_n, len(items)) - 1] if len(items) >= top_n else items[-1]
    next_item = items[top_n] if len(items) > top_n else None
    cutoff_score = _safe_float(cutoff_item.get("tradePriorityScore"))
    next_score = _safe_float(next_item.get("tradePriorityScore")) if next_item else None
    return {
        "cutoff_rank": top_n if len(items) >= top_n else len(items),
        "cutoff_code": cutoff_item.get("code"),
        "next_rank": top_n + 1 if next_item else None,
        "next_code": next_item.get("code") if next_item else None,
        "cutoff_trade_priority_score": cutoff_score,
        "next_trade_priority_score": next_score,
        "score_gap": (cutoff_score - next_score) if cutoff_score is not None and next_score is not None else None,
    }


def _near_boundary_codes(payload: dict[str, Any], *, top_n: int, selected_direction: str) -> list[dict[str, Any]]:
    items = list(payload.get("items") or [])
    if len(items) <= top_n:
        return []
    out: list[dict[str, Any]] = []
    for index, item in enumerate(items[top_n : top_n + 3], start=top_n + 1):
        code = _normalize_text(item.get("code"))
        if not code:
            continue
        out.append(
            {
                "code": code,
                "rank": index,
                "selected_direction": selected_direction,
                "tradePriorityScore": _safe_float(item.get("tradePriorityScore")),
            }
        )
    return out


def _screening_candidate_from_code(
    *,
    code: str,
    side: str,
    asof_date: date,
    up_payload: dict[str, Any],
    down_payload: dict[str, Any],
) -> dict[str, Any]:
    long_item = next((item for item in list(up_payload.get("items") or []) if _normalize_text(item.get("code")) == code), None)
    short_item = next((item for item in list(down_payload.get("items") or []) if _normalize_text(item.get("code")) == code), None)
    selected_direction: str | None = None
    rank: int | None = None
    ranking_item: dict[str, Any] | None = None
    if side == "long":
        if long_item:
            selected_direction = "long"
            rank = next((index + 1 for index, item in enumerate(list(up_payload.get("items") or [])) if _normalize_text(item.get("code")) == code), None)
            ranking_item = long_item
    elif side == "short":
        if short_item:
            selected_direction = "short"
            rank = next((index + 1 for index, item in enumerate(list(down_payload.get("items") or [])) if _normalize_text(item.get("code")) == code), None)
            ranking_item = short_item
    else:
        if long_item and short_item:
            long_score = _rank_item_score(long_item)
            short_score = _rank_item_score(short_item)
            if short_score > long_score:
                selected_direction = "short"
                ranking_item = short_item
                rank = next((index + 1 for index, item in enumerate(list(down_payload.get("items") or [])) if _normalize_text(item.get("code")) == code), None)
            else:
                selected_direction = "long"
                ranking_item = long_item
                rank = next((index + 1 for index, item in enumerate(list(up_payload.get("items") or [])) if _normalize_text(item.get("code")) == code), None)
        elif long_item:
            selected_direction = "long"
            ranking_item = long_item
            rank = next((index + 1 for index, item in enumerate(list(up_payload.get("items") or [])) if _normalize_text(item.get("code")) == code), None)
        elif short_item:
            selected_direction = "short"
            ranking_item = short_item
            rank = next((index + 1 for index, item in enumerate(list(down_payload.get("items") or [])) if _normalize_text(item.get("code")) == code), None)
    payload = _build_candidate_payload(
        code=code,
        selected_direction=selected_direction,
        rank=rank,
        ranking_item=ranking_item,
        asof_date=asof_date,
    )
    if long_item and short_item and side == "both":
        payload["actionability"]["blocking_flags"] = list(dict.fromkeys(list(payload["actionability"]["blocking_flags"]) + ["dual_direction_conflict"]))
        payload["actionability"]["confidence_band"] = "low"
        payload["actionability"]["action_label"] = "avoid"
        payload["actionability"]["reasons"].append("dual_direction_conflict")
    return payload


def _screening_candidate_from_ranking_item(
    *,
    code: str,
    selected_direction: str,
    rank: int,
    ranking_item: dict[str, Any],
    asof_date: date,
) -> dict[str, Any]:
    return _build_candidate_payload(
        code=code,
        selected_direction=selected_direction,
        rank=rank,
        ranking_item=ranking_item,
        asof_date=asof_date,
    )


def build_screening_review_bundle(
    *,
    asof: Any,
    top_n: Any | None = None,
    codes: list[Any] | None = None,
    side: Any = "both",
    risk_mode: Any = "balanced",
    include_near_boundary: Any = False,
) -> dict[str, Any]:
    asof_date, asof_ymd, asof_iso, asof_dt = _parse_asof(asof)
    side_key = _normalize_side(side)
    risk_mode_key = _normalize_risk_mode(risk_mode)
    if isinstance(include_near_boundary, bool) is False:
        raise ValueError("include_near_boundary must be a boolean")
    has_top_n = top_n is not None
    has_codes = codes is not None
    if has_top_n == has_codes:
        raise ValueError("exactly one of top_n or codes is required")
    if has_top_n:
        top_n_value = _require_int(top_n, name="top_n")
        if top_n_value < 1 or top_n_value > _MAX_SCREENING_TOP_N:
            raise ValueError("top_n must be between 1 and 20")
    else:
        if not isinstance(codes, list):
            raise ValueError("codes must be a list of strings")
        if not codes:
            raise ValueError("codes must contain at least one code")
        if len(codes) > _MAX_SCREENING_CODES:
            raise ValueError("codes must contain at most 20 items")
        for item in codes:
            _normalize_code(item)
        top_n_value = None
    runtime_guard = _build_runtime_guard(risk_mode=risk_mode_key)
    warnings = _build_runtime_warnings(runtime_guard)
    up_limit = max(20, int(top_n_value or 20) + 3, len(codes or []), 200)
    down_limit = up_limit
    up_payload = _load_rankings_for_direction(asof_ymd=asof_ymd, direction="up", risk_mode=risk_mode_key, limit=up_limit)
    down_payload = _load_rankings_for_direction(asof_ymd=asof_ymd, direction="down", risk_mode=risk_mode_key, limit=down_limit)
    screening_source = {
        "selection_mode": "top_n" if has_top_n else "codes",
        "ranking_snapshot_status": {
            "long": _ranking_snapshot_meta(up_payload),
            "short": _ranking_snapshot_meta(down_payload),
        },
    }
    candidates: list[dict[str, Any]] = []
    boundary_review = {"top_boundary_observability": {}, "near_boundary_codes": []}
    if has_top_n:
        if side_key == "long":
            records = _top_n_direction_candidates(payload=up_payload, selected_side="long", top_n=top_n_value or 0)
            candidates = [
                _screening_candidate_from_ranking_item(
                    code=row["code"],
                    selected_direction="long",
                    rank=row["rank"],
                    ranking_item=row["ranking_item"],
                    asof_date=asof_date,
                )
                for row in records
            ]
            boundary_review["top_boundary_observability"] = {"long": _top_boundary_observability(up_payload, top_n=top_n_value or 0)}
            if include_near_boundary:
                boundary_review["near_boundary_codes"] = _near_boundary_codes(up_payload, top_n=top_n_value or 0, selected_direction="long")
        elif side_key == "short":
            records = _top_n_direction_candidates(payload=down_payload, selected_side="short", top_n=top_n_value or 0)
            candidates = [
                _screening_candidate_from_ranking_item(
                    code=row["code"],
                    selected_direction="short",
                    rank=row["rank"],
                    ranking_item=row["ranking_item"],
                    asof_date=asof_date,
                )
                for row in records
            ]
            boundary_review["top_boundary_observability"] = {"short": _top_boundary_observability(down_payload, top_n=top_n_value or 0)}
            if include_near_boundary:
                boundary_review["near_boundary_codes"] = _near_boundary_codes(down_payload, top_n=top_n_value or 0, selected_direction="short")
        else:
            records, _boundary_items, conflict_codes = _combine_both_side_candidates(long_payload=up_payload, short_payload=down_payload, top_n=top_n_value or 0)
            for row in records:
                payload = _screening_candidate_from_ranking_item(
                    code=row["code"],
                    selected_direction=row["selected_direction"],
                    rank=row["rank"],
                    ranking_item=row["ranking_item"],
                    asof_date=asof_date,
                )
                if row["code"] in conflict_codes:
                    payload["actionability"]["blocking_flags"] = list(
                        dict.fromkeys(list(payload["actionability"]["blocking_flags"]) + ["dual_direction_conflict"])
                    )
                    payload["actionability"]["confidence_band"] = "low"
                    payload["actionability"]["action_label"] = "avoid"
                    payload["actionability"]["reasons"].append("dual_direction_conflict")
                candidates.append(payload)
            boundary_review["top_boundary_observability"] = {
                "long": _top_boundary_observability(up_payload, top_n=top_n_value or 0),
                "short": _top_boundary_observability(down_payload, top_n=top_n_value or 0),
            }
            if include_near_boundary:
                boundary_review["near_boundary_codes"] = (
                    _near_boundary_codes(up_payload, top_n=top_n_value or 0, selected_direction="long")
                    + _near_boundary_codes(down_payload, top_n=top_n_value or 0, selected_direction="short")
                )
    else:
        if include_near_boundary:
            warnings.append("near_boundary_not_applicable_for_codes_mode")
        for code in codes or []:
            code_key = _normalize_code(code)
            candidates.append(
                _screening_candidate_from_code(
                    code=code_key,
                    side=side_key,
                    asof_date=asof_date,
                    up_payload=up_payload,
                    down_payload=down_payload,
                )
            )
        boundary_review = {"top_boundary_observability": {}, "near_boundary_codes": []}
    if not has_top_n:
        boundary_review = {"top_boundary_observability": {}, "near_boundary_codes": []}
    return {
        "confirmed": True,
        "asof": {"requested": asof_iso, "resolved_dt": asof_dt, "resolved_ymd": asof_ymd},
        "runtime_guard": runtime_guard,
        "screening_source": screening_source,
        "warnings": list(dict.fromkeys(warnings)),
        "candidates": candidates,
        "boundary_review": boundary_review,
    }
