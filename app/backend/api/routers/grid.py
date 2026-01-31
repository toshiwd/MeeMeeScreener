from fastapi import APIRouter, Depends
from typing import List, Any, Dict
from datetime import datetime, timedelta

from app.backend.infra.duckdb.screener_repo import ScreenerRepository
from app.backend.api.dependencies import get_screener_repo
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
    screener_repo: ScreenerRepository = Depends(get_screener_repo)
):
    global _screener_cache
    
    # Check cache (1 hour expiry for example, or based on force_update)
    now = datetime.now()
    if not force_update and _screener_cache["data"] and _screener_cache["last_updated"]:
        if (now - _screener_cache["last_updated"]).total_seconds() < 3600:
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
    
    results = []
    for code in codes:
        # Extract specific rows for this code
        d_rows = daily_map.get(code, [])
        m_rows = monthly_map.get(code, [])
        
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
        name = meta[1] if meta else code
        stage = meta[2] if meta else None
        score = meta[3] if meta else None
        reason = meta[4] if meta else None
        score_status = meta[5] if meta else None
        
        # Fallback/Default logic (simplified from screener_engine)
        if not stage or stage == "UNKNOWN":
             stage = computed.get("statusLabel", "UNKNOWN")

        sector_info = sector_map.get(code)

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
            "sector33_code": sector_info[0] if sector_info else None,
            "sector33_name": sector_info[1] if sector_info else None,
            **computed
        }
        results.append(item)

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
