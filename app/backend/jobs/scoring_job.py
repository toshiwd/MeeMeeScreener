import logging
from app.backend.infra.duckdb.stock_repo import StockRepository
from app.backend.domain.scoring.short_selling import calc_short_a_score, calc_short_b_score
from app.backend.domain.indicators.basic import (
    build_ma_series, calc_slope, compute_atr
)

logger = logging.getLogger(__name__)
_SCORING_BATCH_SIZE = 200

class ScoringJob:
    def __init__(self, stock_repo: StockRepository):
        self.stock_repo = stock_repo

    def run(self):
        logger.info("Starting Scoring Job")
        codes = self.stock_repo.get_all_codes()
        logger.info(f"Found {len(codes)} codes to score")
        
        results = []
        for start in range(0, len(codes), _SCORING_BATCH_SIZE):
            chunk_codes = codes[start : start + _SCORING_BATCH_SIZE]
            bars_by_code = self.stock_repo.get_daily_bars_batch(chunk_codes, limit=200)
            for code in chunk_codes:
                try:
                    # Bulk fetch keeps DB reopen frequency low during TXT update.
                    bars = bars_by_code.get(code) or []
                    # Logic requires ~60-100 bars for MA60/MA100
                    if len(bars) < 60:
                        continue

                    # Prepare data for scoring
                    # Bar format from repo: (date, o, h, l, c, v)
                    closes = [b[4] for b in bars]
                    opens = [b[1] for b in bars]
                    highs = [b[2] for b in bars]
                    lows = [b[3] for b in bars]
                    volumes = [b[5] for b in bars]

                    ma5 = build_ma_series(closes, 5)
                    ma7 = build_ma_series(closes, 7)
                    ma20 = build_ma_series(closes, 20)
                    ma60 = build_ma_series(closes, 60)
                    atr14 = compute_atr(highs, lows, closes, 14)
                    
                    # Slope calculation
                    slope20 = calc_slope(ma20, 3)
                    slope60 = calc_slope(ma60, 3)
                    
                    # Down streak (simplified calculation for now or need helper)
                    # Logic requires 'down7' (streak of closing below MA7)
                    # We need to implement streak counter logic here or in domain helper
                    down7 = 0
                    for i in range(1, 10):
                        idx = -i
                        if idx < -len(closes):
                            break
                        if ma7[idx] is not None and closes[idx] < ma7[idx]:
                            down7 += 1
                        else:
                            break
                    
                    down20 = 0
                    for i in range(1, 20):
                        idx = -i
                        if idx < -len(closes):
                            break
                        if ma20[idx] is not None and closes[idx] < ma20[idx]:
                            down20 += 1
                        else:
                            break

                    avg_vol = sum(volumes[-20:]) / 20 if len(volumes) >= 20 else None

                    # Calculate Scores
                    score_a, reasons_a, badges_a = calc_short_a_score(
                        closes, opens, lows, ma5, ma20, atr14, volumes, avg_vol, down7
                    )

                    score_b, reasons_b, badges_b = calc_short_b_score(
                        closes, opens, lows, ma5, ma20, ma60, ma7, slope20, slope60, atr14, 
                        volumes, avg_vol, down20
                    )

                    if score_a > 0 or score_b > 0:
                        results.append({
                            "code": code,
                            "score_a": score_a,
                            "score_b": score_b,
                            "reasons": reasons_a + reasons_b,
                            "badges": list(set(badges_a + badges_b))
                        })

                except Exception as e:
                    logger.error(f"Error scoring {code}: {e}")
                    continue
        
        logger.info(f"Scoring complete. Found {len(results)} candidates.")
        if results:
            # Replace mode keeps stock_scores aligned with the latest scoring run.
            self.stock_repo.save_scores(results, replace=True)
            logger.info("Saved %d scoring results to stock_scores.", len(results))
        else:
            self.stock_repo.save_scores([], replace=True)
            logger.info("No scoring candidates found. Cleared stock_scores.")
        return results
