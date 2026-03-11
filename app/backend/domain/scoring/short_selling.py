from typing import List, Tuple, Optional

def calc_short_a_score(
    closes: List[float],
    opens: List[float],
    lows: List[float],
    ma5_series: List[Optional[float]],
    ma20_series: List[Optional[float]],
    atr14: Optional[float],
    volumes: List[float],
    avg_volume: Optional[float],
    down7: Optional[int]
) -> Tuple[int, List[str], List[str]]:
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
    reasons: List[str] = []
    badges: List[str] = ["20割れ2本"]

    # B（大陰線）：|C−O| ≥ 0.8×ATR(14) かつ 下ヒゲ ≤ 0.25×実体
    b_condition = False
    if atr14 is not None and len(opens) >= 1:
        body = abs(opens[-1] - close)
        lower_shadow = min(opens[-1], close) - lows[-1]
        
        # Check calling code ensures len(lows) match
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


def calc_short_b_score(
    closes: List[float],
    opens: List[float],
    lows: List[float],
    ma5_series: List[Optional[float]],
    ma20_series: List[Optional[float]],
    ma60_series: List[Optional[float]],
    ma7_series: List[Optional[float]],
    slope20: Optional[float],
    slope60: Optional[float],
    atr14: Optional[float],
    volumes: List[float],
    avg_volume: Optional[float],
    down20: Optional[int]
) -> Tuple[int, List[str], List[str]]:
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
    reasons: List[str] = []

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

    badges: List[str] = ["戻り売り"]

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
