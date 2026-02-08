from fastapi import APIRouter, Depends
from typing import List, Any, Dict
from datetime import datetime, timedelta

from app.backend.infra.duckdb.screener_repo import ScreenerRepository
from app.backend.infra.duckdb.stock_repo import StockRepository
from app.backend.api.dependencies import get_screener_repo, get_stock_repo
from app.backend.domain.screening import metrics, ranking
from app.utils.date_utils import jst_now

router = APIRouter(prefix="/api/grid", tags=["grid"])

def _group_rows_by_code(rows: list[tuple]) -> dict[str, list[tuple]]:
    grouped: dict[str, list[tuple]] = {}
    for row in rows:
        if not row:
            continue
        code = row[0]
        grouped.setdefault(code, []).append(row)
    return grouped


def _apply_short_scores(items: list[dict[str, Any]], score_map: dict[str, dict[str, Any]]) -> None:
    for item in items:
        code = item.get("code")
        if not isinstance(code, str):
            continue
        short_info = score_map.get(code) or {}
        short_a = short_info.get("score_a")
        short_b = short_info.get("score_b")
        short_reasons = short_info.get("reasons") if isinstance(short_info.get("reasons"), list) else []
        short_badges = short_info.get("badges") if isinstance(short_info.get("badges"), list) else []
        short_total = None
        if isinstance(short_a, (int, float)) or isinstance(short_b, (int, float)):
            short_total = float(short_a or 0.0) + float(short_b or 0.0)

        item["shortScore"] = short_total
        item["aScore"] = float(short_a) if isinstance(short_a, (int, float)) else None
        item["bScore"] = float(short_b) if isinstance(short_b, (int, float)) else None
        item["shortBadges"] = short_badges
        item["shortReasons"] = short_reasons

# Simple in-memory cache for screener results (to match legacy behavior of caching)
# In production, use Redis or similar.
_screener_cache = {
    "data": [],
    "last_updated": None
}

@router.get("/screener", response_model=List[Dict[str, Any]])
def get_screener_rows(
    limit: int = 260,
    force_update: bool = False,
    screener_repo: ScreenerRepository = Depends(get_screener_repo),
    stock_repo: StockRepository = Depends(get_stock_repo),
):
    global _screener_cache
    
    # Check cache (1 hour expiry for example, or based on force_update)
    now = datetime.now()
    if not force_update and _screener_cache["data"] and _screener_cache["last_updated"]:
        if (now - _screener_cache["last_updated"]).total_seconds() < 3600:
             score_map = stock_repo.get_scores()
             _apply_short_scores(_screener_cache["data"], score_map)
             return _screener_cache["data"]

    # 1. Fetch Data
    today = jst_now().date()
    window_end = today + timedelta(days=30)
    
    (
        codes,
        meta_rows,
        daily_rows,
        monthly_rows,
        earnings_rows,
        rights_rows
    ) = screener_repo.fetch_screener_batch(
        daily_limit=limit,
        earnings_start=today,
        earnings_end=window_end,
        rights_min_date=today
    )
    
    # 2. Process Data
    meta_map = {row[0]: row for row in meta_rows}
    sector_map = screener_repo.fetch_sector_map(codes)
    daily_map = _group_rows_by_code(daily_rows)
    monthly_map = _group_rows_by_code(monthly_rows)
    earnings_map = {row[0]: row[1] for row in earnings_rows}
    rights_map = {row[0]: row[1] for row in rights_rows}
    short_score_map = stock_repo.get_scores()
    
    asof_map: dict[str, int | None] = {}
    results = []
    for code in codes:
        # Extract specific rows for this code
        d_rows = daily_map.get(code, [])
        m_rows = monthly_map.get(code, [])
        asof_map[code] = d_rows[-1][1] if d_rows else None
        
        # We need to strip the code from the rows for metrics computation if it expects (date, o, h, l, c, v)
        # generic _group_rows_by_code preserves the full tuple including code at index 0.
        # metrics.py expects: date at index 0?
        # Let's check logic in metrics.py.
        # It uses row[0] as date.
        # ScreenerRepository returns (code, date, o, h, l, c, v).
        # So we need to pass `row[1:]` to metrics if metrics expects (date, ...).
        # Let's double check metrics.py logic.
        # metrics.py: `date_value = row[0]` inside `_build_weekly_bars`.
        # So it expects `(date, o, h, l, c, v)`.
        # So we must slice `row[1:]`.
        
        d_rows_sliced = [r[1:] for r in d_rows]
        m_rows_sliced = [r[1:] for r in m_rows]
        
        meta = meta_map.get(code)
        
        computed = metrics.compute_screener_metrics(d_rows_sliced, m_rows_sliced)
        
        # Merge Meta
        # Meta row: code, name, stage, score, reason, score_status, missing_reasons, score_breakdown
        sector_info = sector_map.get(code)
        industry_name = sector_info[0] if sector_info else None
        name = meta[1] if meta else (industry_name or code)
        stage = meta[2] if meta else None
        score = meta[3] if meta else None
        reason = meta[4] if meta else None
        score_status = meta[5] if meta else None
        
        # Fallback/Default logic (simplified from screener_engine)
        if not stage or stage == "UNKNOWN":
             stage = computed.get("statusLabel", "UNKNOWN")

        # Construct Result Item
        item = {
            "code": code,
            "name": name,
            "stage": stage,
            "score": score,
            "reason": reason,
            "scoreStatus": score_status,
            "eventEarningsDate": earnings_map.get(code),
            "eventRightsDate": rights_map.get(code),
            "sector33_code": sector_info[1] if sector_info else None,
            "sector33_name": sector_info[2] if sector_info else None,
            **computed
        }
        results.append(item)

    phase_map = screener_repo.fetch_phase_pred_map(asof_map)
    for item in results:
        phase_info = phase_map.get(item["code"])
        if not phase_info:
            continue
        item["earlyScore"] = phase_info["early_score"]
        item["lateScore"] = phase_info["late_score"]
        item["bodyScore"] = phase_info["body_score"]
        item["phaseN"] = phase_info["n"]
        item["phaseDt"] = phase_info["dt"]

    _apply_short_scores(results, short_score_map)

    _screener_cache["data"] = results
    _screener_cache["last_updated"] = now
    
    return results

@router.get("/ranking", response_model=Dict[str, Any])
def get_ranking(
    limit: int = 50,
    screener_repo: ScreenerRepository = Depends(get_screener_repo)
):
    # This roughly maps to `build_weekly_ranking`
    # We need a way to load rank config. 
    # For now, use an empty config or default.
    # Ideally inject ConfigRepository and load it.
    
    # 1. Fetch Data (reuses fetch_screener_batch for efficiency?)
    # ranking needs daily bars.
    # fetch_screener_batch gets 260 days.
    
    today = jst_now().date()
    (
        codes,
        meta_rows,
        daily_rows,
        monthly_rows,
        _, _
    ) = screener_repo.fetch_screener_batch(
        daily_limit=260, # Ranking needs ~260 for MA200
        earnings_start=today, # Not used for ranking but required by signature
        earnings_end=today,
        rights_min_date=today
    )
    
    daily_map = _group_rows_by_code(daily_rows)
    meta_map = {row[0]: row[1] for row in meta_rows} # code -> name
    
    up_items = []
    down_items = []
    
    # Config is required.
    # We should define a minimal default config if file not found, or use ConfigRepository.
    # Assuming default for now.
    config = {
        "common": {"min_daily_bars": 80},
        "weekly": {
             "weights": {"ma_alignment": 10}, 
             "thresholds": {"volume_ratio": 1.5}
        }
    }
    
    # Process
    for code in codes:
        d_rows = daily_map.get(code, [])
        d_rows_sliced = [r[1:] for r in d_rows]
        
        name = meta_map.get(code, code)
        
        up, down, err = ranking.score_weekly_candidate(
            code, name, d_rows_sliced, config, None
        )
        
        if up: up_items.append(up)
        if down: down_items.append(down)
        
    up_items.sort(key=lambda x: x["total_score"], reverse=True)
    down_items.sort(key=lambda x: x["total_score"], reverse=True)
    
    return {
        "up": up_items[:limit],
        "down": down_items[:limit],
        "meta": {"count": len(codes)}
    }
