from __future__ import annotations

import json
import os
import re
import time
from datetime import datetime, timedelta

from app.core.config import DATA_DIR, DEFAULT_DB_PATH, find_code_txt_path, resolve_pan_out_txt_dir
from app.db.session import get_conn
from app.services.box_detector import detect_boxes
from app.utils.date_utils import _format_event_date, _parse_daily_date, jst_now
from app.utils.math_utils import _build_ma_series, _calc_slope, _compute_atr, _pct_change

_rank_cache = {"mtime": None, "config_mtime": None, "weekly": {}, "monthly": {}}
_rank_config_cache = {"mtime": None, "config": None}
_screener_cache = {"mtime": None, "rows": []}

RANK_CONFIG_PATH = os.getenv("RANK_CONFIG_PATH", os.path.join(os.path.dirname(__file__), "..", "backend", "rank_config.json"))

def _load_rank_config() -> dict:
    path = RANK_CONFIG_PATH
    mtime = os.path.getmtime(path) if os.path.isfile(path) else None
    cached = _rank_config_cache.get("config")
    if _rank_config_cache.get("mtime") == mtime and cached is not None:
        return cached
    config: dict = {}
    if mtime is not None:
        try:
            with open(path, "r", encoding="utf-8") as handle:
                config = json.load(handle) or {}
        except (OSError, json.JSONDecodeError):
            config = {}
    _rank_config_cache["mtime"] = mtime
    _rank_config_cache["config"] = config
    return config

def _load_universe_codes(universe: str | None) -> tuple[list[str], str | None, float | None]:
    if not universe:
        return [], None, None
    key = universe.strip().lower()
    if not key or key in ("all", "*"):
        return [], None, None

    path = None
    if key in ("watchlist", "code", "code.txt"):
        path = find_code_txt_path(DATA_DIR)
    else:
        candidates = [
            os.path.join(DATA_DIR, f"{universe}.txt"),
            os.path.join(os.path.dirname(DATA_DIR), f"{universe}.txt"),
            os.path.join(os.path.dirname(os.path.dirname(DATA_DIR)), f"{universe}.txt")
        ]
        for candidate in candidates:
            if os.path.isfile(candidate):
                path = candidate
                break

    if not path or not os.path.isfile(path):
        return [], None, None

    try:
        with open(path, "r", encoding="utf-8") as handle:
            text = handle.read()
    except OSError:
        return [], path, None

    codes = _parse_codes_from_text(text)
    mtime = os.path.getmtime(path) if os.path.isfile(path) else None
    return codes, path, mtime

def _resolve_universe_codes(conn, universe: str | None) -> tuple[list[str], dict]:
    all_codes = [row[0] for row in conn.execute(
        "SELECT DISTINCT code FROM daily_bars ORDER BY code"
    ).fetchall()]
    if not universe or universe.strip().lower() in ("", "all", "*"):
        return all_codes, {"source": "all", "requested": universe}

    universe_codes, path, mtime = _load_universe_codes(universe)
    if not universe_codes:
        return all_codes, {"source": "all", "requested": universe, "warning": "universe_not_found"}

    allowed = set(all_codes)
    filtered = [code for code in universe_codes if code in allowed]
    return filtered, {
        "source": "file",
        "requested": universe,
        "path": path,
        "mtime": mtime,
        "missing": len(universe_codes) - len(filtered)
    }

def _rank_cache_key(as_of: str | None, limit: int, universe_meta: dict) -> str:
    uni_key = universe_meta.get("path") or universe_meta.get("requested") or "all"
    mtime = universe_meta.get("mtime")
    return f"{as_of or 'latest'}|{limit}|{uni_key}|{mtime or 'none'}"

def _ensure_rank_cache_state() -> tuple[float | None, float | None]:
    db_mtime = os.path.getmtime(DEFAULT_DB_PATH) if os.path.isfile(DEFAULT_DB_PATH) else None
    config_mtime = _rank_config_cache.get("mtime")
    if _rank_cache.get("mtime") != db_mtime or _rank_cache.get("config_mtime") != config_mtime:
        _rank_cache["weekly"] = {}
        _rank_cache["monthly"] = {}
        _rank_cache["mtime"] = db_mtime
        _rank_cache["config_mtime"] = config_mtime
    return db_mtime, config_mtime

def _calc_short_a_score(
    closes: list[float],
    opens: list[float],
    lows: list[float],
    ma5_series: list[float | None],
    ma20_series: list[float | None],
    atr14: float | None,
    volumes: list[float],
    avg_volume: float | None,
    down7: int | None,
    highs: list[float]
) -> tuple[int, list[str], list[str]]:
    """
    A型: 反転確定ショート（20割れ2本 + 決定打B/G/M 2/3成立）
    Returns (score, reasons, badges)
    """
    if len(closes) < 3 or len(ma20_series) < 3 or ma20_series[-1] is None:
        return 0, [], []

    close = closes[-1]
    ma20 = ma20_series[-1]
    prev_close = closes[-2]
    prev_ma20 = ma20_series[-2] if len(ma20_series) >= 2 and ma20_series[-2] is not None else None

    # A型の必須条件
    # 1. 終値 < MA20（実体割れ扱い）
    if close >= ma20:
        return 0, [], []

    # 2. 直近2本のうち 2本連続で終値 < MA20（=「20割れ2本」）
    if prev_ma20 is None or prev_close >= prev_ma20:
        return 0, [], []

    # 3. 下げの決定打（B/G/M の 2/3成立）
    decisive_count = 0
    reasons: list[str] = []
    badges: list[str] = ["20割れ2本"]

    # B（大陰線）：|C−O| ≥ 0.8×ATR(14) かつ 下ヒゲ ≤ 0.25×実体
    b_condition = False
    if atr14 is not None and len(opens) >= 1:
        body = _calc_body(opens[-1], close)
        lower_shadow = _calc_lower_shadow(opens[-1], lows[-1], close)
        if body >= 0.8 * atr14 and close < opens[-1]:  # Bearish candle
            if body > 0 and lower_shadow <= 0.25 * body:
                b_condition = True
                decisive_count += 1
                reasons.append("大陰線")

    # G（ギャップダウン）：GD幅 ≥ 0.5×ATR(14)
    g_condition = False
    if atr14 is not None and len(closes) >= 2:
        gap_down = closes[-2] - opens[-1]  # Previous close - current open
        if gap_down >= 0.5 * atr14:
            g_condition = True
            decisive_count += 1
            reasons.append("ギャップダウン")

    # M：終値 < MA5
    m_condition = False
    if ma5_series and len(ma5_series) >= 1 and ma5_series[-1] is not None:
        if close < ma5_series[-1]:
            m_condition = True
            decisive_count += 1
            reasons.append("MA5下")

    # B/G/Mの2/3成立が必須
    if decisive_count < 2:
        return 0, [], []

    badges.append("B/G/M")

    # ベーススコア: 70点
    score = 70

    # 加点
    if b_condition:
        score += 25  # B成立 +25

    if g_condition:
        score += 20  # G成立 +20
        if b_condition:
            score += 10  # B+Gならさらに +10

    if m_condition:
        score += 10  # M成立 +10

    # 出来高≥20日平均 +10
    if avg_volume is not None and len(volumes) >= 1 and avg_volume > 0:
        if volumes[-1] >= avg_volume:
            score += 10
            reasons.append("出来高増")

    # 直近10日安値を終値で更新 +10
    if len(lows) >= 10:
        recent_low = min(lows[-10:-1]) if len(lows) > 1 else lows[-1]
        if close < recent_low:
            score += 10
            reasons.append("安値更新")

    # 7下本数が1〜3本目 +5（下げ初動を優先）
    if down7 is not None and 1 <= down7 <= 3:
        score += 5
        reasons.append(f"下げ初動（{down7}本目）")

    # 減点
    # 終値がMA20から乖離（終値 < MA20 − 1.0×ATR） -15
    if atr14 is not None and close < ma20 - 1.0 * atr14:
        score -= 15
        reasons.append("MA20乖離大")

    badges.insert(0, "反転確定")
    return max(0, score), reasons, badges

def _calc_short_b_score(
    closes: list[float],
    opens: list[float],
    lows: list[float],
    ma5_series: list[float | None],
    ma20_series: list[float | None],
    ma60_series: list[float | None],
    slope20: float | None,
    slope60: float | None,
    atr14: float | None,
    volumes: list[float],
    avg_volume: float | None,
    down20: int | None,
    ma7_series: list[float | None]
) -> tuple[int, list[str], list[str]]:
    """
    B型: 下落トレンドの戻り売り（MA60下向き + 戻り失速）
    Returns (score, reasons, badges)
    """
    if len(closes) < 5 or len(ma60_series) < 5 or ma60_series[-1] is None:
        return 0, [], []

    close = closes[-1]
    ma20 = ma20_series[-1] if ma20_series and ma20_series[-1] is not None else None
    ma60 = ma60_series[-1]

    # B型の必須条件
    # 1. MA60傾き < 0（下向き）
    if slope60 is None or slope60 >= 0:
        return 0, [], []

    # 2. 終値 < MA60
    if close >= ma60:
        return 0, [], []

    # 3. 終値 < MA20
    if ma20 is not None and close >= ma20:
        return 0, [], []

    # 4.「戻り失速」判定
    pullback_stall = False
    reasons: list[str] = []

    # 直近5本以内に終値がMA7〜MA20帯に接近→その後2本以内で終値<MA5
    ma7 = ma7_series[-1] if ma7_series and len(ma7_series) >= 1 and ma7_series[-1] is not None else None
    ma5 = ma5_series[-1] if ma5_series and len(ma5_series) >= 1 and ma5_series[-1] is not None else None

    if ma7 is not None and ma20 is not None and ma5 is not None:
        # Check if price approached MA7-MA20 band in last 5 bars
        for i in range(-5, 0):
            if abs(i) > len(closes) or abs(i) > len(ma7_series) or abs(i) > len(ma20_series):
                continue
            past_close = closes[i]
            past_ma7 = ma7_series[i] if ma7_series[i] is not None else None
            past_ma20 = ma20_series[i] if ma20_series[i] is not None else None
            if past_ma7 is not None and past_ma20 is not None:
                band_low = min(past_ma7, past_ma20)
                band_high = max(past_ma7, past_ma20)
                if band_low <= past_close <= band_high:
                    # Check if current close < MA5
                    if close < ma5:
                        pullback_stall = True
                        reasons.append("戻り失速")
                        break

    # Alternative: 陰線実体 + 翌日安値更新
    if not pullback_stall and len(closes) >= 2 and len(opens) >= 2:
        prev_bearish = closes[-2] < opens[-2]  # Previous bar was bearish
        low_break = lows[-1] < lows[-2] if len(lows) >= 2 else False
        if prev_bearish and low_break:
            pullback_stall = True
            reasons.append("陰線後安値更新")

    if not pullback_stall:
        return 0, [], []

    badges: list[str] = ["戻り売り"]

    # ベーススコア: 60点
    score = 60

    # 加点
    # MA20傾き < 0 +15
    if slope20 is not None and slope20 < 0:
        score += 15
        reasons.append("MA20下向き")

    # 20下本数が10本以上 +10
    if down20 is not None and down20 >= 10:
        score += 10
        reasons.append(f"下落明確（{down20}本）")

    # 前安値ラインを実体で割る（終値で前安値割れ） +20
    if len(lows) >= 11:
        prev_low = min(lows[-11:-1]) if len(lows) > 1 else lows[-1]
        if close < prev_low:
            score += 20
            reasons.append("前安値割れ")

    # 出来高≥20日平均 +10
    if avg_volume is not None and len(volumes) >= 1 and avg_volume > 0:
        if volumes[-1] >= avg_volume:
            score += 10
            reasons.append("出来高増")

    # 7MA上に戻しても1〜2本で失速（戻り弱） +10
    if ma7 is not None and len(closes) >= 3:
        was_above_ma7 = False
        for i in range(-3, -1):
            if abs(i) <= len(closes) and abs(i) <= len(ma7_series):
                past_close = closes[i]
                past_ma7 = ma7_series[i] if ma7_series[i] is not None else None
                if past_ma7 is not None and past_close > past_ma7:
                    was_above_ma7 = True
                    break
        if was_above_ma7 and close < ma7:
            score += 10
            reasons.append("戻り弱")

    # 減点
    # 末期（終値 < MA20 − 1.2×ATR） -30 (Z2は既にチェック済みだが、ここでもペナルティ)
    if ma20 is not None and atr14 is not None and close < ma20 - 1.2 * atr14:
        score -= 30
        reasons.append("末期警戒")

    return max(0, score), reasons, badges

def _score_weekly_candidate(code: str, name: str, rows: list[tuple], config: dict, as_of: int | None) -> tuple[dict | None, dict | None, str | None]:
    rows = _normalize_daily_rows(rows, as_of)
    common = _get_config_value(config, ["common"], {})
    min_bars = int(common.get("min_daily_bars", 80))
    if len(rows) < min_bars:
        return None, None, "insufficient_daily_bars"

    dates = [int(row[0]) for row in rows]
    opens = [float(row[1]) for row in rows]
    highs = [float(row[2]) for row in rows]
    lows = [float(row[3]) for row in rows]
    closes = [float(row[4]) for row in rows]
    volumes = [float(row[5]) if row[5] is not None else 0.0 for row in rows]

    close = closes[-1] if closes else None
    if close is None:
        return None, None, "missing_close"

    ma7_series = _build_ma_series(closes, 7)
    ma20_series = _build_ma_series(closes, 20)
    ma60_series = _build_ma_series(closes, 60)
    ma100_series = _build_ma_series(closes, 100)
    ma200_series = _build_ma_series(closes, 200)

    ma7 = ma7_series[-1] if ma7_series else None
    ma20 = ma20_series[-1] if ma20_series else None
    ma60 = ma60_series[-1] if ma60_series else None
    ma100 = ma100_series[-1] if ma100_series else None
    ma200 = ma200_series[-1] if ma200_series else None
    if ma20 is None or ma60 is None:
        return None, None, "missing_ma"
    if ma100 is None or ma200 is None:
        return None, None, "missing_ma_long_term"

    slope_lookback = int(common.get("slope_lookback", 3))
    slope20 = _calc_slope(ma20_series, slope_lookback)
    slope100 = _calc_slope(ma100_series, slope_lookback)
    slope200 = _calc_slope(ma200_series, slope_lookback)

    atr_period = int(common.get("atr_period", 14))
    atr14 = _compute_atr(highs, lows, closes, atr_period)

    volume_period = int(common.get("volume_period", 20))
    include_latest = common.get("volume_ratio_mode", "exclude_latest") == "include_latest"
    volume_ratio = _compute_volume_ratio(volumes, volume_period, include_latest)

    up7 = _count_streak(closes, ma7_series, "up")
    down7 = _count_streak(closes, ma7_series, "down")

    trigger_lookback = int(common.get("trigger_lookback", 20))
    recent_high, recent_low = _calc_recent_bounds(highs, lows, trigger_lookback)
    break_up_pct = None
    break_down_pct = None
    if recent_high is not None and close:
        break_up_pct = max(0.0, (recent_high - close) / close * 100)
    if recent_low is not None and close:
        break_down_pct = max(0.0, (close - recent_low) / close * 100)

    weekly = _get_config_value(config, ["weekly"], {})
    weights = weekly.get("weights", {})
    thresholds = weekly.get("thresholds", {})
    down_weights = weekly.get("down_weights", {})
    down_thresholds = weekly.get("down_thresholds", {})
    max_reasons = int(common.get("max_reasons", 6))

    up_reasons: list[tuple[float, str]] = []
    down_reasons: list[tuple[float, str]] = []
    up_badges: list[str] = []
    down_badges: list[str] = []
    up_score = 0.0
    down_score = 0.0

    def push_reason(target: list[tuple[float, str]], weight: float, label: str):
        if weight:
            target.append((weight, label))

    def push_badge(target: list[str], label: str):
        if label and label not in target:
            target.append(label)

    if close > ma20 and ma20 > ma60:
        weight = float(weights.get("ma_alignment", 0))
        up_score += weight
        push_reason(up_reasons, weight, "MA20 > MA60")
        push_badge(up_badges, "MA整列")

    if ma60 > ma100:
        weight = float(weights.get("ma_alignment_100", 0))
        up_score += weight
        push_reason(up_reasons, weight, "MA60 > MA100")

    if ma100 > ma200:
        weight = float(weights.get("ma_alignment_200", 0))
        up_score += weight
        push_reason(up_reasons, weight, "MA100 > MA200")

    if close > ma100:
        weight = float(weights.get("obs_above_ma100", 0))
        up_score += weight
        push_reason(up_reasons, weight, "MA100より上")
    
    if close > ma200:
        weight = float(weights.get("obs_above_ma200", 0))
        up_score += weight
        push_reason(up_reasons, weight, "MA200より上")

    pull_min = int(thresholds.get("pullback_down7_min", 1))
    pull_max = int(thresholds.get("pullback_down7_max", 2))
    slope_min = float(thresholds.get("slope_min", 0))
    if close > ma20 and down7 is not None and pull_min <= down7 <= pull_max:
        if slope20 is None or slope20 >= slope_min:
            weight = float(weights.get("pullback_above_ma20", 0))
            up_score += weight
            push_reason(up_reasons, weight, f"MA20上で押し目（下{down7}本）")
            push_badge(up_badges, "押し目")

    vol_thresh = float(thresholds.get("volume_ratio", 1.5))
    if volume_ratio is not None and volume_ratio >= vol_thresh:
        weight = float(weights.get("volume_spike", 0))
        up_score += weight
        push_reason(up_reasons, weight, f"出来高増（20日比{volume_ratio:.2f}倍）")
        push_badge(up_badges, "出来高増")

    near_pct = float(thresholds.get("near_break_pct", 2.0))
    if break_up_pct is not None and break_up_pct <= near_pct:
        weight = float(weights.get("near_high_break", 0))
        up_score += weight
        push_reason(up_reasons, weight, f"高値ブレイク接近（{break_up_pct:.1f}%）")
        push_badge(up_badges, "高値接近")

    if slope20 is not None and slope20 >= slope_min:
        weight = float(weights.get("slope_up", 0))
        up_score += weight
        push_reason(up_reasons, weight, "MA20上向き")
        push_badge(up_badges, "MA上向き")

    if slope100 is not None and slope100 >= slope_min:
        weight = float(weights.get("slope_up_100", 0))
        up_score += weight
        push_reason(up_reasons, weight, "MA100上向き")

    if slope200 is not None and slope200 >= slope_min:
        weight = float(weights.get("slope_up_200", 0))
        up_score += weight
        push_reason(up_reasons, weight, "MA200上向き")

    big_candle = float(thresholds.get("big_candle_atr", 1.2))
    if atr14 is not None and abs(close - opens[-1]) >= atr14 * big_candle and close > opens[-1]:
        weight = float(weights.get("big_bull_candle", 0))
        up_score += weight
        push_reason(up_reasons, weight, "強い陽線")
        push_badge(up_badges, "陽線強")

    ma20_dist = float(thresholds.get("ma20_distance_pct", 2.0))
    if ma20:
        dist_pct = abs(close - ma20) / ma20 * 100
        if close >= ma20 and dist_pct <= ma20_dist:
            weight = float(weights.get("ma20_support", 0))
            up_score += weight
            push_reason(up_reasons, weight, f"MA20近接（{dist_pct:.1f}%）")
            push_badge(up_badges, "MA20近接")

    ma100_thresh = float(thresholds.get("ma100_distance_pct", 3.0))
    if close >= ma100:
        dist100 = abs(close - ma100) / ma100 * 100
        if dist100 <= ma100_thresh:
            weight = float(weights.get("ma100_support", 0))
            up_score += weight
            push_reason(up_reasons, weight, f"MA100近接（{dist100:.1f}%）")

    ma200_thresh = float(thresholds.get("ma200_distance_pct", 3.0))
    if close >= ma200:
        dist200 = abs(close - ma200) / ma200 * 100
        if dist200 <= ma200_thresh:
            weight = float(weights.get("ma200_support", 0))
            up_score += weight
            push_reason(up_reasons, weight, f"MA200近接（{dist200:.1f}%）")

    if close < ma20 and ma20 < ma60:
        weight = float(down_weights.get("ma_alignment", 0))
        down_score += weight
        push_reason(down_reasons, weight, "MA20 < MA60")
        push_badge(down_badges, "MA逆転")

    if ma60 < ma100:
        weight = float(down_weights.get("ma_alignment_100", 0))
        down_score += weight
        push_reason(down_reasons, weight, "MA60 < MA100")

    if ma100 < ma200:
        weight = float(down_weights.get("ma_alignment_200", 0))
        down_score += weight
        push_reason(down_reasons, weight, "MA100 < MA200")

    if close < ma100:
        weight = float(down_weights.get("obs_below_ma100", 0))
        down_score += weight
        push_reason(down_reasons, weight, "MA100より下")
    
    if close < ma200:
        weight = float(down_weights.get("obs_below_ma200", 0))
        down_score += weight
        push_reason(down_reasons, weight, "MA200より下")

    pull_min = int(down_thresholds.get("pullback_up7_min", 1))
    pull_max = int(down_thresholds.get("pullback_up7_max", 2))
    slope_max = float(down_thresholds.get("slope_max", 0))
    if close < ma20 and up7 is not None and pull_min <= up7 <= pull_max:
        if slope20 is None or slope20 <= slope_max:
            weight = float(down_weights.get("pullback_below_ma20", 0))
            down_score += weight
            push_reason(down_reasons, weight, f"MA20下で戻り（上{up7}本）")
            push_badge(down_badges, "戻り")

    vol_thresh = float(down_thresholds.get("volume_ratio", vol_thresh))
    if volume_ratio is not None and volume_ratio >= vol_thresh:
        weight = float(down_weights.get("volume_spike", 0))
        down_score += weight
        push_reason(down_reasons, weight, f"出来高増（20日比{volume_ratio:.2f}倍）")
        push_badge(down_badges, "出来高増")

    near_pct = float(down_thresholds.get("near_break_pct", near_pct))
    if break_down_pct is not None and break_down_pct <= near_pct:
        weight = float(down_weights.get("near_low_break", 0))
        down_score += weight
        push_reason(down_reasons, weight, f"安値ブレイク接近（{break_down_pct:.1f}%）")
        push_badge(down_badges, "安値接近")

    if slope20 is not None and slope20 <= slope_max:
        weight = float(down_weights.get("slope_down", 0))
        down_score += weight
        push_reason(down_reasons, weight, "MA20下向き")
        push_badge(down_badges, "MA下向き")

    if slope100 is not None and slope100 <= slope_max:
        weight = float(down_weights.get("slope_down_100", 0))
        down_score += weight
        push_reason(down_reasons, weight, "MA100下向き")

    if slope200 is not None and slope200 <= slope_max:
        weight = float(down_weights.get("slope_down_200", 0))
        down_score += weight
        push_reason(down_reasons, weight, "MA200下向き")

    big_candle = float(down_thresholds.get("big_candle_atr", big_candle))
    if atr14 is not None and abs(close - opens[-1]) >= atr14 * big_candle and close < opens[-1]:
        weight = float(down_weights.get("big_bear_candle", 0))
        down_score += weight
        push_reason(down_reasons, weight, "強い陰線")
        push_badge(down_badges, "陰線強")

    ma20_dist = float(down_thresholds.get("ma20_distance_pct", ma20_dist))
    if ma20:
        dist_pct = abs(close - ma20) / ma20 * 100
        if close <= ma20 and dist_pct <= ma20_dist:
            weight = float(down_weights.get("ma20_resistance", 0))
            down_score += weight
            push_reason(down_reasons, weight, f"MA20近接（{dist_pct:.1f}%）")
            push_badge(down_badges, "MA20近接")

    ma100_thresh = float(down_thresholds.get("ma100_distance_pct", 3.0))
    if close <= ma100:
        dist100 = abs(close - ma100) / ma100 * 100
        if dist100 <= ma100_thresh:
            weight = float(down_weights.get("ma100_resistance", 0))
            down_score += weight
            push_reason(down_reasons, weight, f"MA100近接（{dist100:.1f}%）")

    ma200_thresh = float(down_thresholds.get("ma200_distance_pct", 3.0))
    if close <= ma200:
        dist200 = abs(close - ma200) / ma200 * 100
        if dist200 <= ma200_thresh:
            weight = float(down_weights.get("ma200_resistance", 0))
            down_score += weight
            push_reason(down_reasons, weight, f"MA200近接（{dist200:.1f}%）")

    up_reasons.sort(key=lambda item: item[0], reverse=True)
    down_reasons.sort(key=lambda item: item[0], reverse=True)

    levels = {
        "close": close,
        "ma7": ma7,
        "ma20": ma20,
        "ma60": ma60,
        "atr14": atr14,
        "volume_ratio": volume_ratio
    }

    chart_hint = {
        "lines": {
            "ma20": ma20,
            "ma60": ma60,
            "ma100": ma100,
            "ma200": ma200,
            "recent_high": recent_high,
            "recent_low": recent_low
        }
    }

    as_of_label = _format_daily_label(dates[-1])
    series_bars = int(common.get("rank_series_bars", 60))
    series_rows = rows[-series_bars:] if series_bars > 0 else rows
    series = [
        [int(item[0]), float(item[1]), float(item[2]), float(item[3]), float(item[4])]
        for item in series_rows
    ]

    base = {
        "code": code,
        "name": name or code,
        "as_of": as_of_label,
        "levels": levels,
        "series": series,
        "distance_to_trigger": {
            "break_up_pct": break_up_pct,
            "break_down_pct": break_down_pct
        },
        "chart_hint": chart_hint
    }

    up_item = {
        **base,
        "total_score": round(up_score, 3),
        "reasons": [label for _, label in up_reasons[:max_reasons]],
        "badges": up_badges[:max_reasons]
    }
    down_item = {
        **base,
        "total_score": round(down_score, 3),
        "reasons": [label for _, label in down_reasons[:max_reasons]],
        "badges": down_badges[:max_reasons]
    }

    return up_item, down_item, None

def _score_monthly_candidate(code: str, name: str, rows: list[tuple], config: dict, as_of_month: int | None) -> tuple[dict | None, str | None]:
    rows = _normalize_monthly_rows(rows, as_of_month)
    thresholds = _get_config_value(config, ["monthly", "thresholds"], {})
    min_months = int(thresholds.get("min_months", 3))
    if len(rows) < min_months:
        return None, "insufficient_monthly_bars"

    box = _detect_body_box(rows, config)
    if not box:
        return None, "no_box"

    weights = _get_config_value(config, ["monthly", "weights"], {})
    max_reasons = int(_get_config_value(config, ["common", "max_reasons"], 6))
    near_edge_pct = float(thresholds.get("near_edge_pct", 4.0))
    wild_penalty = float(weights.get("wild_box_penalty", 0))

    close = float(box["last_close"])
    upper = float(box["upper"])
    lower = float(box["lower"])
    break_up_pct = max(0.0, (upper - close) / close * 100) if close else None
    break_down_pct = max(0.0, (close - lower) / close * 100) if close else None
    edge_pct = None
    if break_up_pct is not None and break_down_pct is not None:
        edge_pct = min(break_up_pct, break_down_pct)

    reasons: list[tuple[float, str]] = []
    score = 0.0

    months = int(box["months"])
    weight_month = float(weights.get("box_months", 0))
    if weight_month:
        score += weight_month * months
        reasons.append((weight_month, f"箱の期間{months}か月"))

    if edge_pct is not None and edge_pct <= near_edge_pct:
        weight = float(weights.get("near_edge", 0))
        ratio = 1 - edge_pct / near_edge_pct if near_edge_pct else 1
        score += weight * ratio
        if break_up_pct is not None and break_down_pct is not None:
            if break_up_pct <= break_down_pct:
                reasons.append((weight, f"上抜けまで{break_up_pct:.1f}%"))
            else:
                reasons.append((weight, f"下抜けまで{break_down_pct:.1f}%"))

    if box["wild"] and wild_penalty:
        score += wild_penalty
        reasons.append((wild_penalty, "荒れ箱"))

    closes = [float(row[4]) for row in rows if len(row) >= 5 and row[4] is not None]
    ma7_series = _build_ma_series(closes, 7)
    ma20_series = _build_ma_series(closes, 20)
    ma60_series = _build_ma_series(closes, 60)
    ma7 = ma7_series[-1] if ma7_series else None
    ma20 = ma20_series[-1] if ma20_series else None
    ma60 = ma60_series[-1] if ma60_series else None

    # New Logic: MA Alignment for Monthly
    if ma7 and ma20 and ma60:
        if ma7 > ma20 and ma20 > ma60:
            w_order = float(weights.get("ma_order_7_20_60", 0))
            score += w_order
            reasons.append((w_order, "月足MA配列(7>20>60)"))

        # Simple slope using last 2 points
        s7 = ma7_series[-1] - ma7_series[-2] if len(ma7_series) > 1 else 0
        s20 = ma20_series[-1] - ma20_series[-2] if len(ma20_series) > 1 else 0
        if s7 > 0 and s20 > 0:
            w_slopes = float(weights.get("ma_slopes_up", 0))
            score += w_slopes
            reasons.append((w_slopes, "月足MA上昇"))

    reasons.sort(key=lambda item: item[0], reverse=True)

    levels = {
        "close": close,
        "ma7": ma7,
        "ma20": ma20,
        "ma60": ma60,
        "atr14": None
    }

    chart_hint = {
        "lines": {
            "box_upper": upper,
            "box_lower": lower,
            "ma20": ma20
        }
    }

    return {
        "code": code,
        "name": name or code,
        "as_of": _format_month_label(box["end"]),
        "total_score": round(score, 3),
        "reasons": [label for _, label in reasons[:max_reasons]],
        "levels": levels,
        "distance_to_trigger": {
            "break_up_pct": break_up_pct,
            "break_down_pct": break_down_pct
        },
        "box_info": {
            "box_start": _format_month_label(box["start"]),
            "box_end": _format_month_label(box["end"]),
            "box_upper_body": upper,
            "box_lower_body": lower,
            "box_months": months,
            "wild_box_flag": box["wild"],
            "range_pct": box["range_pct"]
        },
        "box_start": _format_month_label(box["start"]),
        "box_end": _format_month_label(box["end"]),
        "box_upper_body": upper,
        "box_lower_body": lower,
        "box_months": months,
        "wild_box_flag": box["wild"],
        "chart_hint": chart_hint
    }, None

def _build_weekly_ranking(as_of: str | None, limit: int, universe: str | None) -> dict:
    start = time.perf_counter()
    config = _load_rank_config()
    _ensure_rank_cache_state()
    as_of_int = _as_of_int(as_of)
    common = _get_config_value(config, ["common"], {})
    max_bars = int(common.get("max_daily_bars", 260))

    with get_conn() as conn:
        codes, universe_meta = _resolve_universe_codes(conn, universe)
        if not codes:
            return {"up": [], "down": [], "meta": {"as_of": as_of, "count": 0, "errors": []}}
        cache_key = _rank_cache_key(as_of, limit, universe_meta)
        cached = _rank_cache["weekly"].get(cache_key)
        if cached:
            return cached
        meta_rows = conn.execute(
            f"SELECT code, name FROM stock_meta WHERE code IN ({','.join(['?'] * len(codes))})",
            codes
        ).fetchall()
        name_map = {row[0]: row[1] for row in meta_rows}
        daily_map = _fetch_daily_rows(conn, codes, as_of_int, max_bars)

    up_items: list[dict] = []
    down_items: list[dict] = []
    skipped: list[dict] = []

    for code in codes:
        rows = daily_map.get(code, [])
        up_item, down_item, skip_reason = _score_weekly_candidate(code, name_map.get(code, code), rows, config, as_of_int)
        if skip_reason:
            skipped.append({"code": code, "reason": skip_reason})
            continue
        if up_item:
            up_items.append(up_item)
        if down_item:
            down_items.append(down_item)

    up_items.sort(key=lambda item: item.get("total_score", 0), reverse=True)
    down_items.sort(key=lambda item: item.get("total_score", 0), reverse=True)

    elapsed = (time.perf_counter() - start) * 1000
    print(f"[rank_weekly] codes={len(codes)} skipped={len(skipped)} ms={elapsed:.1f}")

    result = {
        "up": up_items[:limit],
        "down": down_items[:limit],
        "meta": {
            "as_of": as_of,
            "count": len(codes),
            "skipped": skipped,
            "elapsed_ms": round(elapsed, 2),
            "universe": universe_meta,
            "errors": []
        }
    }
    _rank_cache["weekly"][cache_key] = result
    return result

def _build_monthly_ranking(as_of: str | None, limit: int, universe: str | None) -> dict:
    start = time.perf_counter()
    config = _load_rank_config()
    _ensure_rank_cache_state()
    as_of_month = _as_of_month_int(as_of)
    common = _get_config_value(config, ["common"], {})
    max_bars = int(common.get("max_monthly_bars", 120))

    with get_conn() as conn:
        codes, universe_meta = _resolve_universe_codes(conn, universe)
        if not codes:
            return {"box": [], "meta": {"as_of": as_of, "count": 0, "errors": []}}
        cache_key = _rank_cache_key(as_of, limit, universe_meta)
        cached = _rank_cache["monthly"].get(cache_key)
        if cached:
            return cached
        meta_rows = conn.execute(
            f"SELECT code, name FROM stock_meta WHERE code IN ({','.join(['?'] * len(codes))})",
            codes
        ).fetchall()
        name_map = {row[0]: row[1] for row in meta_rows}
        monthly_map = _fetch_monthly_rows(conn, codes, as_of_month, max_bars)

    items: list[dict] = []
    skipped: list[dict] = []

    for code in codes:
        rows = monthly_map.get(code, [])
        item, skip_reason = _score_monthly_candidate(code, name_map.get(code, code), rows, config, as_of_month)
        if skip_reason:
            skipped.append({"code": code, "reason": skip_reason})
            continue
        if item:
            items.append(item)

    items.sort(key=lambda item: item.get("total_score", 0), reverse=True)
    elapsed = (time.perf_counter() - start) * 1000
    print(f"[rank_monthly] codes={len(codes)} skipped={len(skipped)} ms={elapsed:.1f}")

    result = {
        "box": items[:limit],
        "meta": {
            "as_of": as_of,
            "count": len(codes),
            "skipped": skipped,
            "elapsed_ms": round(elapsed, 2),
            "universe": universe_meta,
            "errors": []
        }
    }
    _rank_cache["monthly"][cache_key] = result
    return result

def _get_screener_rows() -> list[dict]:
    mtime = None
    if os.path.isfile(DEFAULT_DB_PATH):
        mtime = os.path.getmtime(DEFAULT_DB_PATH)
    if _screener_cache["mtime"] == mtime and _screener_cache["rows"]:
        return _screener_cache["rows"]

    rows = _build_screener_rows()
    _screener_cache["mtime"] = mtime
    _screener_cache["rows"] = rows
    return rows

def _invalidate_screener_cache() -> None:
    _screener_cache["mtime"] = None
    _screener_cache["rows"] = []
    _rank_cache["weekly"] = {}
    _rank_cache["monthly"] = {}
    _rank_cache["mtime"] = None
    _rank_cache["config_mtime"] = _rank_config_cache.get("mtime")


def _get_config_value(config: dict, keys: list[str], default):
    current = config
    for key in keys:
        if not isinstance(current, dict) or key not in current:
            return default
        current = current[key]
    return current


def _parse_as_of_date(value: str | None) -> datetime | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    if re.match(r"^\d{8}$", text):
        try:
            year = int(text[:4])
            month = int(text[4:6])
            day = int(text[6:8])
            return datetime(year, month, day)
        except ValueError:
            return None
    if re.match(r"^\d{6}$", text):
        try:
            year = int(text[:4])
            month = int(text[4:6])
            return datetime(year, month, 1)
        except ValueError:
            return None
    for fmt in ("%Y-%m-%d", "%Y/%m/%d"):
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            continue
    return None


def _as_of_int(value: str | None) -> int | None:
    dt = _parse_as_of_date(value)
    if not dt:
        return None
    return dt.year * 10000 + dt.month * 100 + dt.day


def _as_of_month_int(value: str | None) -> int | None:
    dt = _parse_as_of_date(value)
    if not dt:
        return None
    return dt.year * 100 + dt.month


def _parse_month_value(value: int | str | None) -> datetime | None:
    if value is None:
        return None
    try:
        raw = str(int(value)).zfill(6)
        year = int(raw[:4])
        month = int(raw[4:6])
        return datetime(year, month, 1)
    except (ValueError, TypeError):
        return None


def _month_label_to_int(label: str | None) -> int | None:
    if not label:
        return None
    try:
        parts = label.split("-")
        if len(parts) != 2:
            return None
        year = int(parts[0])
        month = int(parts[1])
        if month < 1 or month > 12:
            return None
        return year * 100 + month
    except (TypeError, ValueError):
        return None


def _format_month_label(value: int | str | None) -> str | None:
    month = _parse_month_value(value)
    if not month:
        return None
    return f"{month.year:04d}-{month.month:02d}"


def _format_daily_label(value: int | None) -> str | None:
    if value is None:
        return None
    raw = str(int(value)).zfill(8)
    return f"{raw[:4]}-{raw[4:6]}-{raw[6:8]}"


def _parse_codes_from_text(text: str) -> list[str]:
    codes = re.findall(r"\d{4}", text)
    return sorted(set(codes))


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


def _fetch_monthly_rows(conn, codes: list[str], as_of_month: int | None, limit: int) -> dict[str, list[tuple]]:
    if not codes:
        return {}
    placeholders = ",".join(["?"] * len(codes))
    where_clauses = [f"code IN ({placeholders})"]
    params: list = list(codes)
    if as_of_month is not None:
        where_clauses.append("month <= ?")
        params.append(as_of_month)
    where_sql = " AND ".join(where_clauses)
    query = f"""
        SELECT code, month, o, h, l, c
        FROM (
            SELECT
                code,
                month,
                o,
                h,
                l,
                c,
                ROW_NUMBER() OVER (PARTITION BY code ORDER BY month DESC) AS rn
            FROM monthly_bars
            WHERE {where_sql}
        )
        WHERE rn <= ?
        ORDER BY code, month
    """
    params.append(limit)
    rows = conn.execute(query, params).fetchall()
    return _group_rows_by_code(rows)


def _normalize_daily_rows(rows: list[tuple], as_of: int | None) -> list[tuple]:
    by_date: dict[int, tuple] = {}
    for row in rows:
        if len(row) < 6:
            continue
        date_value = row[0]
        if date_value is None:
            continue
        date_int = int(date_value)
        if as_of is not None and date_int > as_of:
            continue
        by_date[date_int] = row
    return [by_date[key] for key in sorted(by_date.keys())]


def _normalize_monthly_rows(rows: list[tuple], as_of_month: int | None) -> list[tuple]:
    by_month: dict[int, tuple] = {}
    for row in rows:
        if len(row) < 5:
            continue
        month_value = row[0]
        if month_value is None:
            continue
        month_int = int(month_value)
        if as_of_month is not None and month_int > as_of_month:
            continue
        by_month[month_int] = row
    return [by_month[key] for key in sorted(by_month.keys())]


def _compute_volume_ratio(volumes: list[float], period: int, include_latest: bool) -> float | None:
    if period <= 0:
        return None
    if include_latest:
        if len(volumes) < period:
            return None
        window = volumes[-period:]
    else:
        if len(volumes) < period + 1:
            return None
        window = volumes[-period - 1:-1]
    avg = sum(window) / period if period else 0
    if avg <= 0:
        return None
    latest = volumes[-1]
    return latest / avg


def _count_streak(
    values: list[float],
    averages: list[float | None],
    direction: str
) -> int | None:
    count = 0
    opposite = 0
    has_values = False
    for value, avg in zip(values, averages):
        if avg is None:
            continue
        has_values = True
        if direction == "up":
            if value > avg:
                count += 1
                opposite = 0
            elif value < avg:
                opposite += 1
                if opposite >= 2:
                    count = 0
            else:
                opposite = 0
        else:
            if value < avg:
                count += 1
                opposite = 0
            elif value > avg:
                opposite += 1
                if opposite >= 2:
                    count = 0
            else:
                opposite = 0
    return None if not has_values else count


def _calc_recent_bounds(highs: list[float], lows: list[float], lookback: int) -> tuple[float | None, float | None]:
    if not highs or not lows:
        return None, None
    if lookback <= 0:
        return max(highs), min(lows)
    window_highs = highs[-lookback:] if len(highs) >= lookback else highs
    window_lows = lows[-lookback:] if len(lows) >= lookback else lows
    return max(window_highs), min(window_lows)


def _calc_body(open_: float, close: float) -> float:
    """Calculate body = |C - O|."""
    return abs(close - open_)


def _calc_lower_shadow(open_: float, low: float, close: float) -> float:
    """Calculate lower shadow = min(O, C) - L."""
    return min(open_, close) - low


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


def _drop_incomplete_weekly(weekly: list[dict], last_daily: datetime | None) -> list[dict]:
    if not weekly or not last_daily:
        return weekly
    last_week_start = (last_daily - timedelta(days=last_daily.weekday())).date()
    if weekly[-1]["week_start"] == last_week_start and last_daily.weekday() < 4:
        return weekly[:-1]
    return weekly


def _drop_incomplete_monthly(monthly_rows: list[tuple], last_daily: datetime | None) -> list[tuple]:
    if not monthly_rows or not last_daily:
        return monthly_rows
    last_month = _parse_month_value(monthly_rows[-1][0] if monthly_rows else None)
    if last_month and last_month.year == last_daily.year and last_month.month == last_daily.month:
        return monthly_rows[:-1]
    return monthly_rows


def _build_weekly_bars(daily_rows: list[tuple]) -> list[dict]:
    items: list[dict] = []
    current_key = None
    for row in daily_rows:
        if len(row) < 5:
            continue
        date_value, open_, high, low, close = row[:5]
        if open_ is None or high is None or low is None or close is None:
            continue
        dt = _parse_daily_date(date_value)
        if not dt:
            continue
        week_start = (dt - timedelta(days=dt.weekday())).date()
        if current_key != week_start:
            items.append(
                {
                    "week_start": week_start,
                    "o": float(open_),
                    "h": float(high),
                    "l": float(low),
                    "c": float(close),
                    "last_date": dt.date()
                }
            )
            current_key = week_start
        else:
            current = items[-1]
            current["h"] = max(current["h"], float(high))
            current["l"] = min(current["l"], float(low))
            current["c"] = float(close)
            current["last_date"] = dt.date()
    return items


def _build_quarterly_bars(monthly_rows: list[tuple]) -> list[dict]:
    items: list[dict] = []
    current_key: tuple[int, int] | None = None
    for row in monthly_rows:
        if len(row) < 5:
            continue
        month_value, open_, high, low, close = row[:5]
        dt = _parse_month_value(month_value)
        if not dt:
            continue
        quarter = (dt.month - 1) // 3 + 1
        key = (dt.year, quarter)
        if current_key != key:
            items.append(
                {
                    "year": dt.year,
                    "quarter": quarter,
                    "o": float(open_),
                    "h": float(high),
                    "l": float(low),
                    "c": float(close)
                }
            )
            current_key = key
        else:
            current = items[-1]
            current["h"] = max(current["h"], float(high))
            current["l"] = min(current["l"], float(low))
            current["c"] = float(close)
    return items


def _build_yearly_bars(monthly_rows: list[tuple]) -> list[dict]:
    items: list[dict] = []
    current_year = None
    for row in monthly_rows:
        if len(row) < 5:
            continue
        month_value, open_, high, low, close = row[:5]
        dt = _parse_month_value(month_value)
        if not dt:
            continue
        if current_year != dt.year:
            items.append(
                {
                    "year": dt.year,
                    "o": float(open_),
                    "h": float(high),
                    "l": float(low),
                    "c": float(close)
                }
            )
            current_year = dt.year
        else:
            current = items[-1]
            current["h"] = max(current["h"], float(high))
            current["l"] = min(current["l"], float(low))
            current["c"] = float(close)
    return items


def _calc_regression_slope(values: list[float | None], window: int = 5) -> float | None:
    """Calculate regression slope over the last `window` values (simple difference average)."""
    if len(values) < window:
        return None
    recent = values[-window:]
    valid = [v for v in recent if v is not None]
    if len(valid) < 2:
        return None
    # Simple: average of consecutive differences
    diffs = [valid[i + 1] - valid[i] for i in range(len(valid) - 1)]
    return sum(diffs) / len(diffs) if diffs else None


def _calc_range_bounds_with_mid(
    highs: list[float], lows: list[float], lookback: int
) -> tuple[float | None, float | None, float | None]:
    """Calculate (high, low, midpoint) for the range over `lookback` periods."""
    if not highs or not lows:
        return None, None, None
    window_highs = highs[-lookback:] if len(highs) >= lookback else highs
    window_lows = lows[-lookback:] if len(lows) >= lookback else lows
    range_high = max(window_highs)
    range_low = min(window_lows)
    mid = (range_high + range_low) / 2
    return range_high, range_low, mid


def _check_short_prohibition_zones(
    close: float,
    ma20: float | None,
    ma60: float | None,
    slope20: float | None,
    slope60: float | None,
    atr14: float | None,
    range_mid: float | None,
    range_high: float | None,
    range_low: float | None
) -> tuple[str | None, int]:
    """
    Check prohibition zones for short selling.
    Returns (zone_name, penalty_score):
    - Z1: 上昇優位 -> ShortScore = 0 (force)
    - Z2: 末期下げ -> ShortScore = 0 (force)
    - Z3: レンジ中央 -> -30 penalty
    - None: No prohibition
    """
    if ma20 is None or ma60 is None:
        return None, 0

    # Z1: 上昇優位（ネットショート事故ゾーン）
    # 終値 > MA20 かつ MA20傾き > 0（上向き）
    # かつ（終値 > MA60 または MA60傾き > 0）
    if close > ma20 and (slope20 is not None and slope20 > 0):
        if close > ma60 or (slope60 is not None and slope60 > 0):
            return "Z1", -9999  # Force to 0

    # Z2: 末期下げ（利確・触らないゾーン）
    # 終値 < MA20 － 1.2×ATR(14)
    if atr14 is not None and close < ma20 - 1.2 * atr14:
        return "Z2", -9999  # Force to 0

    # Z3: レンジ中央（期待値薄）
    # 直近60日の高安の中点±15%に終値が位置
    if range_mid is not None and range_high is not None and range_low is not None:
        range_band = (range_high - range_low) * 0.15
        if range_mid - range_band <= close <= range_mid + range_band:
            return "Z3", -30  # Penalty

    return None, 0


def _build_box_metrics(
    monthly_rows: list[tuple],
    last_close: float | None
) -> tuple[dict | None, str, str | None, str | None, str]:
    if not monthly_rows:
        return None, "NONE", None, None, "NONE"
    boxes = detect_boxes(monthly_rows, range_basis="body", max_range_pct=0.2)
    if not boxes:
        return None, "NONE", None, None, "NONE"

    bars = []
    for row in monthly_rows:
        if len(row) < 5:
            continue
        month_value, open_, high, low, close = row[:5]
        if open_ is None or close is None:
            continue
        bars.append(
            {
                "month": month_value,
                "open": float(open_),
                "close": float(close)
            }
        )

    if not bars:
        return None, "NONE", None, None, "NONE"

    latest_box = max(boxes, key=lambda item: item["endIndex"])
    months = latest_box["endIndex"] - latest_box["startIndex"] + 1
    if months < 3:
        return None, "NONE", None, None, "NONE"

    active_box = {**latest_box, "months": months}
    latest_index = len(bars) - 1
    start_index = active_box["startIndex"]
    end_index = active_box["endIndex"]
    body_low = None
    body_high = None
    for bar in bars[start_index: end_index + 1]:
        low = min(bar["open"], bar["close"])
        high = max(bar["open"], bar["close"])
        body_low = low if body_low is None else min(body_low, low)
        body_high = high if body_high is None else max(body_high, high)

    if body_low is None or body_high is None:
        return None, "NONE", None, None, "NONE"

    base = max(abs(body_low), 1e-9)
    range_pct = (body_high - body_low) / base
    start_label = _format_month_label(active_box["startTime"])
    end_label = _format_month_label(active_box["endTime"])

    box_state = "NONE"
    if end_index == latest_index:
        box_state = "IN_BOX"
    elif end_index == latest_index - 1:
        box_state = "JUST_BREAKOUT"

    breakout_month = None
    if box_state == "JUST_BREAKOUT" and latest_index >= 0:
        breakout_month = _format_month_label(bars[latest_index]["month"])

    direction_state = "NONE"
    if box_state != "NONE" and last_close is not None:
        if last_close > body_high:
            direction_state = "BREAKOUT_UP"
        elif last_close < body_low:
            direction_state = "BREAKOUT_DOWN"
        else:
            direction_state = "IN_BOX"

    payload = {
        "startDate": start_label,
        "endDate": end_label,
        "bodyLow": body_low,
        "bodyHigh": body_high,
        "months": active_box["months"],
        "rangePct": range_pct,
        "isActive": box_state == "IN_BOX",
        "boxState": box_state,
        "boxEndMonth": end_label,
        "breakoutMonth": breakout_month
    }
    return payload, box_state, end_label, breakout_month, direction_state


def _compute_screener_metrics(
    daily_rows: list[tuple],
    monthly_rows: list[tuple]
) -> dict:
    reasons: list[str] = []
    daily_rows = sorted(daily_rows, key=lambda item: item[0])
    monthly_rows = sorted(monthly_rows, key=lambda item: item[0])

    last_daily = _parse_daily_date(daily_rows[-1][0]) if daily_rows else None
    closes = [float(row[4]) for row in daily_rows if len(row) >= 5 and row[4] is not None]
    opens = [float(row[1]) for row in daily_rows if len(row) >= 5 and row[1] is not None]
    highs = [float(row[2]) for row in daily_rows if len(row) >= 5 and row[2] is not None]
    lows = [float(row[3]) for row in daily_rows if len(row) >= 5 and row[3] is not None]
    volumes = [float(row[5]) if len(row) >= 6 and row[5] is not None else 0.0 for row in daily_rows]
    last_close = closes[-1] if closes else None
    if last_close is None:
        reasons.append("missing_last_close")

    chg1d = _pct_change(closes[-1], closes[-2]) if len(closes) >= 2 else None

    weekly = _build_weekly_bars(daily_rows)
    weekly = _drop_incomplete_weekly(weekly, last_daily)
    weekly_closes = [item["c"] for item in weekly]
    chg1w = _pct_change(weekly_closes[-1], weekly_closes[-2]) if len(weekly_closes) >= 2 else None
    prev_week_chg = _pct_change(weekly_closes[-2], weekly_closes[-3]) if len(weekly_closes) >= 3 else None

    confirmed_monthly = _drop_incomplete_monthly(monthly_rows, last_daily)
    monthly_closes = [float(row[4]) for row in confirmed_monthly if len(row) >= 5 and row[4] is not None]
    chg1m = _pct_change(monthly_closes[-1], monthly_closes[-2]) if len(monthly_closes) >= 2 else None
    prev_month_chg = _pct_change(monthly_closes[-2], monthly_closes[-3]) if len(monthly_closes) >= 3 else None

    quarterly = _build_quarterly_bars(confirmed_monthly)
    quarterly_closes = [item["c"] for item in quarterly]
    chg1q = _pct_change(quarterly_closes[-1], quarterly_closes[-2]) if len(quarterly_closes) >= 2 else None
    prev_quarter_chg = _pct_change(quarterly_closes[-2], quarterly_closes[-3]) if len(quarterly_closes) >= 3 else None

    yearly = _build_yearly_bars(confirmed_monthly)
    yearly_closes = [item["c"] for item in yearly]
    chg1y = _pct_change(yearly_closes[-1], yearly_closes[-2]) if len(yearly_closes) >= 2 else None
    prev_year_chg = _pct_change(yearly_closes[-2], yearly_closes[-3]) if len(yearly_closes) >= 3 else None

    ma5_series = _build_ma_series(closes, 5)
    ma7_series = _build_ma_series(closes, 7)
    ma20_series = _build_ma_series(closes, 20)
    ma60_series = _build_ma_series(closes, 60)
    ma100_series = _build_ma_series(closes, 100)

    ma7 = ma7_series[-1] if ma7_series else None
    ma20 = ma20_series[-1] if ma20_series else None
    ma60 = ma60_series[-1] if ma60_series else None
    ma100 = ma100_series[-1] if ma100_series else None

    prev_ma20 = ma20_series[-2] if len(ma20_series) >= 2 else None
    slope20 = ma20 - prev_ma20 if ma20 is not None and prev_ma20 is not None else None

    # Calculate regression slopes for short-selling (5-bar average of differences)
    slope20_reg = _calc_regression_slope(ma20_series, 5)
    slope60_reg = _calc_regression_slope(ma60_series, 5)

    # Calculate ATR(14) for short-selling
    atr14 = _compute_atr(highs, lows, closes, 14)

    # Calculate 20-day volume average
    volume_avg_20 = sum(volumes[-20:]) / 20 if len(volumes) >= 20 else None

    up7 = _count_streak(closes, ma7_series, "up")
    down7 = _count_streak(closes, ma7_series, "down")
    up20 = _count_streak(closes, ma20_series, "up")
    down20 = _count_streak(closes, ma20_series, "down")
    up60 = _count_streak(closes, ma60_series, "up")
    down60 = _count_streak(closes, ma60_series, "down")
    up100 = _count_streak(closes, ma100_series, "up")
    down100 = _count_streak(closes, ma100_series, "down")

    if ma20 is None:
        reasons.append("missing_ma20")
    if ma60 is None:
        reasons.append("missing_ma60")
    if ma100 is None:
        reasons.append("missing_ma100")
    if chg1m is None:
        reasons.append("missing_chg1m")
    if chg1q is None:
        reasons.append("missing_chg1q")
    if chg1y is None:
        reasons.append("missing_chg1y")

    box_monthly, box_state, box_end_month, breakout_month, box_direction = _build_box_metrics(
        monthly_rows, last_close
    )

    latest_month_label = _format_month_label(confirmed_monthly[-1][0]) if confirmed_monthly else None
    prev_month_label = _format_month_label(confirmed_monthly[-2][0]) if len(confirmed_monthly) >= 2 else None
    latest_month_value = _month_label_to_int(latest_month_label)
    prev_month_value = _month_label_to_int(prev_month_label)
    box_active = False
    if box_monthly:
        box_start_value = _month_label_to_int(box_monthly.get("startDate"))
        box_end_value = _month_label_to_int(box_monthly.get("endDate"))
        if box_start_value is not None and box_end_value is not None:
            if latest_month_value is not None and box_start_value <= latest_month_value <= box_end_value:
                box_active = True
            elif prev_month_value is not None and box_start_value <= prev_month_value <= box_end_value:
                box_active = True

    monthly_ma7_series = _build_ma_series(monthly_closes, 7)
    monthly_ma20_series = _build_ma_series(monthly_closes, 20)
    monthly_down20 = _count_streak(monthly_closes, monthly_ma20_series, "down")
    bottom_zone = bool(monthly_down20 is not None and monthly_down20 >= 6)

    weekly_closes = [item["c"] for item in weekly]
    weekly_highs = [item["h"] for item in weekly]
    weekly_lows = [item["l"] for item in weekly]
    weekly_ma7_series = _build_ma_series(weekly_closes, 7)
    weekly_ma20_series = _build_ma_series(weekly_closes, 20)
    weekly_ma7 = weekly_ma7_series[-1] if weekly_ma7_series else None
    weekly_ma20 = weekly_ma20_series[-1] if weekly_ma20_series else None
    weekly_above_ma7 = (
        weekly_closes[-1] > weekly_ma7 if weekly_ma7 is not None and weekly_closes else False
    )
    weekly_above_ma20 = (
        weekly_closes[-1] > weekly_ma20 if weekly_ma20 is not None and weekly_closes else False
    )

    weekly_low_stop = False
    if len(weekly_lows) >= 6:
        recent_lows = weekly_lows[-6:]
        previous_lows = weekly_lows[:-6]
        if previous_lows:
            weekly_low_stop = min(recent_lows) >= min(previous_lows)

    weekly_range_contraction = False
    if len(weekly_highs) >= 12:
        recent_range = max(weekly_highs[-6:]) - min(weekly_lows[-6:])
        prev_range = max(weekly_highs[-12:-6]) - min(weekly_lows[-12:-6])
        if prev_range > 0 and recent_range <= prev_range * 0.8:
            weekly_range_contraction = True

    daily_cross_ma7 = False
    daily_cross_ma20 = False
    if len(closes) >= 2 and len(ma7_series) >= 2:
        daily_cross_ma7 = closes[-1] > ma7_series[-1] and closes[-2] <= ma7_series[-2]
    if len(closes) >= 2 and len(ma20_series) >= 2:
        daily_cross_ma20 = closes[-1] > ma20_series[-1] and closes[-2] <= ma20_series[-2]

    daily_pre_signal = False
    if daily_rows:
        last_row = daily_rows[-1]
        if len(last_row) >= 5:
            open_ = float(last_row[1]) if last_row[1] is not None else None
            high = float(last_row[2]) if last_row[2] is not None else None
            low = float(last_row[3]) if last_row[3] is not None else None
            close = float(last_row[4]) if last_row[4] is not None else None
            if open_ is not None and high is not None and low is not None and close is not None:
                rng = max(high - low, 1e-9)
                body = abs(close - open_)
                lower_shadow = min(open_, close) - low
                if body / rng <= 0.35 or lower_shadow / rng >= 0.45:
                    daily_pre_signal = True

    daily_low_break = False
    if len(daily_rows) >= 11:
        lows = [
            float(row[3])
            for row in daily_rows[-11:-1]
            if len(row) >= 4 and row[3] is not None
        ]
        if lows and daily_rows[-1][3] is not None:
            daily_low_break = float(daily_rows[-1][3]) < min(lows)

    weekly_low_break = False
    if len(weekly_lows) >= 7:
        weekly_low_break = weekly_lows[-1] < min(weekly_lows[-7:-1])

    falling_knife = daily_low_break or weekly_low_break
    monthly_ok = box_active or bottom_zone

    score_monthly = 0
    if box_active:
        score_monthly += 18
    if bottom_zone:
        score_monthly += 12

    score_weekly = 0
    if weekly_low_stop:
        score_weekly += 15
    if weekly_range_contraction:
        score_weekly += 10
    if weekly_above_ma7:
        score_weekly += 7
    if weekly_above_ma20:
        score_weekly += 8

    score_daily = 0
    if daily_cross_ma7:
        score_daily += 10
    if daily_cross_ma20:
        score_daily += 12
    if daily_pre_signal:
        score_daily += 8

    daily_ma20_down = False
    if len(ma20_series) >= 2:
        daily_ma20_down = ma20_series[-1] < ma20_series[-2]

    buy_state = "その他"
    buy_state_rank = 0
    buy_state_score = 0
    buy_state_reason_parts: list[str] = []

    if monthly_ok and weekly_low_stop and not falling_knife:
        if daily_cross_ma7 or daily_cross_ma20 or daily_pre_signal:
            buy_state = "初動"
            buy_state_rank = 2
            buy_state_score = score_monthly + score_weekly + score_daily
            if daily_ma20_down and ma20 is not None and last_close is not None and last_close < ma20:
                buy_state_score -= 15
        elif weekly_range_contraction:
            buy_state = "底がため"
            buy_state_rank = 1
            buy_state_score = score_monthly + score_weekly + min(score_daily, 10)

    if buy_state_score < 0:
        buy_state_score = 0
    if buy_state == "初動":
        buy_state_score = min(100, buy_state_score)
    elif buy_state == "底がため":
        buy_state_score = min(80, buy_state_score)

    if monthly_ok:
        month_parts = []
        if box_active:
            month_parts.append("箱有")
        if bottom_zone:
            month_parts.append("大底警戒")
        buy_state_reason_parts.append(f"月:{'/'.join(month_parts)}")
    if weekly_low_stop or weekly_range_contraction:
        week_parts = []
        if weekly_low_stop:
            week_parts.append("安値更新停止")
        if weekly_range_contraction:
            week_parts.append("収縮")
        if weekly_above_ma7:
            week_parts.append("7MA上")
        if weekly_above_ma20:
            week_parts.append("20MA上")
        buy_state_reason_parts.append(f"週:{'/'.join(week_parts)}")
    if daily_cross_ma7 or daily_cross_ma20 or daily_pre_signal:
        day_parts = []
        if daily_cross_ma7:
            day_parts.append("7MA上抜け")
        if daily_cross_ma20:
            day_parts.append("20MA上抜け")
        if daily_pre_signal:
            day_parts.append("事前決定打")
        buy_state_reason_parts.append(f"日:{'/'.join(day_parts)}")
    if falling_knife:
        buy_state_reason_parts.append("落ちるナイフ")

    buy_state_reason = " / ".join(buy_state_reason_parts) if buy_state_reason_parts else "N/A"

    buy_risk_distance = None
    if last_close is not None and box_monthly and box_monthly.get("bodyLow") is not None:
        body_low = float(box_monthly["bodyLow"])
        if last_close > 0:
            buy_risk_distance = max(0.0, (last_close - body_low) / last_close * 100)

    status_label = "UNKNOWN"
    essential_missing = last_close is None or ma20 is None or ma60 is None
    if not essential_missing:
        if last_close > ma20 and ma20 > ma60:
            status_label = "UP"
        elif last_close < ma20 and ma20 < ma60:
            status_label = "DOWN"
        else:
            status_label = "RANGE"

    up_score = None
    down_score = None
    overheat_up = None
    overheat_down = None

    if status_label != "UNKNOWN" and last_close is not None and ma20 is not None and ma60 is not None:
        up_score = 0
        down_score = 0

        if last_close > ma20:
            up_score += 10
        if ma20 > ma60:
            up_score += 10
        if slope20 is not None and slope20 > 0:
            up_score += 10

        if up7 is not None:
            if up7 >= 14:
                up_score += 20
            elif up7 >= 7:
                up_score += 10

        if box_state != "NONE":
            if box_direction == "BREAKOUT_UP":
                up_score += 30
            elif box_state == "IN_BOX" and box_monthly and box_monthly.get("months", 0) >= 3:
                up_score += 10

        if chg1m is not None and chg1m > 0:
            up_score += 10
        if chg1q is not None and chg1q > 0:
            up_score += 10

        if last_close < ma20:
            down_score += 10
        if ma20 < ma60:
            down_score += 10
        if slope20 is not None and slope20 < 0:
            down_score += 10

        if down7 is not None:
            if down7 >= 14:
                down_score += 20
            elif down7 >= 7:
                down_score += 10

        if box_state != "NONE" and box_direction == "BREAKOUT_DOWN":
            down_score += 30

        if chg1m is not None and chg1m < 0:
            down_score += 10
        if chg1q is not None and chg1q < 0:
            down_score += 10

        up_score = min(100, max(0, up_score))
        down_score = min(100, max(0, down_score))

        if up20 is not None:
            overheat_up = min(1.0, max(0.0, (up20 - 16) / 4))
        if down20 is not None:
            overheat_down = min(1.0, max(0.0, (down20 - 16) / 4))

    # ========================================================================
    # Short-selling score calculation
    # ========================================================================
    short_score = None
    a_score = None
    b_score = None
    short_type = None
    short_badges: list[str] = []
    short_reasons: list[str] = []
    short_prohibition = None

    if last_close is not None and ma20 is not None and ma60 is not None:
        # Calculate 60-day range bounds for Z3 check
        range_high_60, range_low_60, range_mid_60 = _calc_range_bounds_with_mid(highs, lows, 60)

        # Check prohibition zones
        short_prohibition, zone_penalty = _check_short_prohibition_zones(
            last_close, ma20, ma60, slope20_reg, slope60_reg, atr14,
            range_mid_60, range_high_60, range_low_60
        )

        # Calculate A-type score (反転確定ショート)
        a_score_raw, a_reasons, a_badges = _calc_short_a_score(
            closes, opens, lows, ma5_series, ma20_series, atr14,
            volumes, volume_avg_20, down7, highs
        )

        # Calculate B-type score (戻り売り)
        b_score_raw, b_reasons, b_badges = _calc_short_b_score(
            closes, opens, lows, ma5_series, ma20_series, ma60_series,
            slope20_reg, slope60_reg, atr14, volumes, volume_avg_20, down20, ma7_series
        )

        # Apply Z3 penalty (not forced to 0, just penalty)
        if short_prohibition == "Z3":
            a_score_raw = max(0, a_score_raw + zone_penalty)
            b_score_raw = max(0, b_score_raw + zone_penalty)

        # Determine final score and type
        if short_prohibition in ("Z1", "Z2"):
            # Forced to 0 for prohibition zones
            short_score = 0
            a_score = 0
            b_score = 0
            short_type = None
            short_badges = []
            short_reasons = [f"禁止ゾーン: {short_prohibition}"]
        else:
            a_score = a_score_raw
            b_score = b_score_raw
            short_score = max(a_score, b_score)

            if a_score >= b_score and a_score > 0:
                short_type = "A"
                short_badges = a_badges
                short_reasons = a_reasons
            elif b_score > 0:
                short_type = "B"
                short_badges = b_badges
                short_reasons = b_reasons
            else:
                short_type = None
                short_badges = []
                short_reasons = []

    return {
        "lastClose": last_close,
        "chg1D": chg1d,
        "chg1W": chg1w,
        "chg1M": chg1m,
        "chg1Q": chg1q,
        "chg1Y": chg1y,
        "prevWeekChg": prev_week_chg,
        "prevMonthChg": prev_month_chg,
        "prevQuarterChg": prev_quarter_chg,
        "prevYearChg": prev_year_chg,
        "ma7": ma7,
        "ma20": ma20,
        "ma60": ma60,
        "ma100": ma100,
        "slope20": slope20,
        "counts": {
            "up7": up7,
            "down7": down7,
            "up20": up20,
            "down20": down20,
            "up60": up60,
            "down60": down60,
            "up100": up100,
            "down100": down100
        },
        "boxMonthly": box_monthly,
        "boxState": box_state,
        "boxEndMonth": box_end_month,
        "breakoutMonth": breakout_month,
        "boxActive": box_active,
        "hasBox": box_active,
        "box_state": box_state,
        "box_end_month": box_end_month,
        "breakout_month": breakout_month,
        "box_active": box_active,
        "buyState": buy_state,
        "buyStateRank": buy_state_rank,
        "buyStateScore": buy_state_score,
        "buyStateReason": buy_state_reason,
        "buyRiskDistance": buy_risk_distance,
        "buy_state": buy_state,
        "buy_state_rank": buy_state_rank,
        "buy_state_score": buy_state_score,
        "buy_state_reason": buy_state_reason,
        "buy_risk_distance": buy_risk_distance,
        "buyStateDetails": {
            "monthly": score_monthly,
            "weekly": score_weekly,
            "daily": score_daily
        },
        "scores": {
            "upScore": up_score,
            "downScore": down_score,
            "overheatUp": overheat_up,
            "overheatDown": overheat_down
        },
        "statusLabel": status_label,
        "reasons": reasons,
        # Short-selling score fields
        "shortScore": short_score,
        "aScore": a_score,
        "bScore": b_score,
        "shortType": short_type,
        "shortBadges": short_badges,
        "shortReasons": short_reasons,
        "shortProhibition": short_prohibition
    }


def _build_name_map_from_txt() -> dict[str, str]:
    pan_out_txt_dir = resolve_pan_out_txt_dir()
    if not os.path.isdir(pan_out_txt_dir):
        return {}
    name_map: dict[str, str] = {}
    for filename in os.listdir(pan_out_txt_dir):
        if not filename.endswith(".txt") or filename.lower() == "code.txt":
            continue
        base = os.path.splitext(filename)[0]
        if "_" not in base:
            continue
        code, name = base.split("_", 1)
        code = code.strip()
        name = name.strip()
        if code and name and code not in name_map:
            name_map[code] = name
    return name_map


def _build_screener_rows() -> list[dict]:
    today = jst_now().date()
    window_end = today + timedelta(days=30)
    with get_conn() as conn:
        codes = [row[0] for row in conn.execute("SELECT DISTINCT code FROM daily_bars ORDER BY code").fetchall()]
        meta_rows = conn.execute(
            "SELECT code, name, stage, score, reason, score_status, missing_reasons_json, score_breakdown_json FROM stock_meta"
        ).fetchall()
        daily_rows = conn.execute(
            """
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
            )
            WHERE rn <= 260
            ORDER BY code, date
            """
        ).fetchall()
        monthly_rows = conn.execute(
            """
            SELECT code, month, o, h, l, c
            FROM monthly_bars
            ORDER BY code, month
            """
        ).fetchall()
        earnings_rows = conn.execute(
            """
            SELECT code, MIN(planned_date) AS planned_date
            FROM earnings_planned
            WHERE planned_date BETWEEN ? AND ?
            GROUP BY code
            """,
            [today, window_end]
        ).fetchall()
        rights_rows = conn.execute(
            """
            SELECT code, MIN(COALESCE(last_rights_date, ex_date)) AS rights_date
            FROM ex_rights
            WHERE COALESCE(last_rights_date, ex_date) >= ?
            GROUP BY code
            """,
            [today]
        ).fetchall()

    meta_map = {row[0]: row for row in meta_rows}
    fallback_names = _build_name_map_from_txt()
    daily_map: dict[str, list[tuple]] = {}
    monthly_map: dict[str, list[tuple]] = {}
    earnings_map = {row[0]: row[1] for row in earnings_rows}
    rights_map = {row[0]: row[1] for row in rights_rows}

    for row in daily_rows:
        code = row[0]
        daily_map.setdefault(code, []).append(row[1:])

    for row in monthly_rows:
        code = row[0]
        monthly_map.setdefault(code, []).append(row[1:])

    items: list[dict] = []
    for code in codes:
        meta = meta_map.get(code)
        name = meta[1] if meta else None
        stage = meta[2] if meta else None
        score = meta[3] if meta and meta[3] is not None else None
        reason = meta[4] if meta and meta[4] is not None else ""
        score_status = meta[5] if meta else None
        missing_reasons = []
        if meta and meta[6]:
            try:
                missing_reasons = json.loads(meta[6]) or []
            except (TypeError, json.JSONDecodeError):
                missing_reasons = []
        score_breakdown = None
        if meta and meta[7]:
            try:
                score_breakdown = json.loads(meta[7]) or None
            except (TypeError, json.JSONDecodeError):
                score_breakdown = None
        metrics = _compute_screener_metrics(daily_map.get(code, []), monthly_map.get(code, []))
        fallback_name = fallback_names.get(code)
        if not name or name == code:
            name = fallback_name
        if not name:
            name = code
        if not stage or stage.upper() == "UNKNOWN":
            stage = metrics.get("statusLabel") or stage or "UNKNOWN"
        if isinstance(score, (int, float)) and float(score) == 0.0:
            if (
                not score_status
                or score_status == "INSUFFICIENT_DATA"
                or not reason
                or reason == "TODO"
                or not stage
                or (isinstance(stage, str) and stage.upper() == "UNKNOWN")
            ):
                score = None
                score_status = "INSUFFICIENT_DATA"
        if score is None:
            fallback_score = None
            buy_score = metrics.get("buyStateScore")
            if isinstance(buy_score, (int, float)) and buy_score > 0:
                fallback_score = float(buy_score)
            else:
                scores = metrics.get("scores") or {}
                if isinstance(scores, dict):
                    values = [
                        scores.get("upScore"),
                        scores.get("downScore")
                    ]
                    values = [float(v) for v in values if isinstance(v, (int, float)) and v > 0]
                    if values:
                        fallback_score = max(values)
            if fallback_score is not None:
                score = fallback_score
                if not reason:
                    reason = "DERIVED"
                if not score_status:
                    score_status = "OK"
        if not score_status:
            score_status = "OK" if score is not None else "INSUFFICIENT_DATA"
        if not missing_reasons:
            missing_reasons = metrics.get("reasons") or []
        event_earnings_date = _format_event_date(earnings_map.get(code))
        event_rights_date = _format_event_date(rights_map.get(code))
        items.append(
            {
                "code": code,
                "name": name,
                "stage": stage,
                "score": score,
                "reason": reason,
                "scoreStatus": score_status,
                "score_status": score_status,
                "missingReasons": missing_reasons,
                "missing_reasons": missing_reasons,
                "scoreBreakdown": score_breakdown,
                "score_breakdown": score_breakdown,
                "eventEarningsDate": event_earnings_date,
                "eventRightsDate": event_rights_date,
                "event_earnings_date": event_earnings_date,
                "event_rights_date": event_rights_date,
                **metrics
            }
        )
    return items

