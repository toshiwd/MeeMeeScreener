import os
import json
import os
import json
import re
import time
from datetime import datetime, timezone

import pandas as pd


# Add parent directory to path to allow importing core
import sys
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

try:
    from app.db.session import get_conn
    from app.db.schema import init_schema
    from app.core.config import config
except ModuleNotFoundError:  # pragma: no cover - legacy tooling may import from app/backend on sys.path
    from db import get_conn, init_schema  # type: ignore
    from core.config import config  # type: ignore

try:
    from app.backend.infra.duckdb.industry_master import ensure_industry_master
except ImportError:
    from infra.duckdb.industry_master import ensure_industry_master



REPO_ROOT = str(config.REPO_ROOT)
DEFAULT_PAN_CODE_PATH = os.path.join(REPO_ROOT, "tools", "code.txt")
DEFAULT_PAN_OUT_DIR = str(config.PAN_OUT_TXT_DIR)
INGEST_STATE_PATH = str(config.DATA_DIR / "ingest_state.json")


def resolve_data_dir() -> str:
    return str(config.PAN_OUT_TXT_DIR)


DATA_DIR = resolve_data_dir()
CODE_PATTERN_DEFAULT = r"^[0-9A-Za-z]{4,16}$"
CODE_PATTERN = re.compile(os.getenv("CODE_PATTERN", CODE_PATTERN_DEFAULT))
STRICT_CODE_VALIDATION = os.getenv("CODE_STRICT", "0") == "1"
USE_CODE_TXT = os.getenv("USE_CODE_TXT", "0") == "1"

HEADER_ALIASES = {
    "code": {"code", "ticker", "symbol", "銘柄", "銘柄コード", "コード"},
    "date": {"date", "日付", "年月日", "日時", "dateymd", "dateyyyymmdd"},
    "o": {"o", "open", "始値", "始"},
    "h": {"h", "high", "高値", "高"},
    "l": {"l", "low", "安値", "安"},
    "c": {"c", "close", "終値", "終"},
    "v": {"v", "volume", "vol", "出来高", "出来高株", "売買高", "売買高株"},
}

TRADE_FLAG_CONFIG = {
    "BOX_MONTHS_MIN": 4,
    "BOX_RANGE_PCT": 0.20,
    "HITEI_BODY_RATIO": 0.5,
    "GAP_PCT": 0.01,
    "SIGNAL_ZONE_PCT": 0.03,
    "HIGARA_SWING_WINDOW": 20,
    "HIGARA_TARGET_DAYS": [7, 9, 17, 26],
    "HIGARA_DAY_RANGE": 1
}


def find_code_txt_path(data_dir: str) -> str | None:
    code_path = os.path.abspath(os.getenv("PAN_CODE_TXT_PATH") or DEFAULT_PAN_CODE_PATH)
    if os.path.exists(code_path):
        return code_path
    return None


def name_from_filename(path: str, code: str) -> str | None:
    base = os.path.splitext(os.path.basename(path))[0]
    if "_" not in base:
        return None
    code_part, name_part = base.split("_", 1)
    if code_part != code:
        return None
    name = name_part.strip()
    return name if name else None



def _build_ma_series(values: list[float], period: int) -> list[float | None]:
    if period <= 0:
        return [None for _ in values]
    result: list[float | None] = []
    total = 0.0
    for index, value in enumerate(values):
        total += value
        if index >= period:
            total -= values[index - period]
        if index >= period - 1:
            result.append(total / period)
        else:
            result.append(None)
    return result


def _count_streak(values: list[float], averages: list[float | None], direction: str) -> int | None:
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


def _build_streak_series(
    values: list[float],
    averages: list[float | None],
    direction: str
) -> list[int | None]:
    count = 0
    opposite = 0
    has_values = False
    result: list[int | None] = []
    for value, avg in zip(values, averages):
        if avg is None:
            result.append(None)
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
        result.append(count)
    return result if has_values else [None for _ in values]


def _pct_change(current: float | None, previous: float | None) -> float | None:
    if current is None or previous is None or previous == 0:
        return None
    return current / previous - 1


def _safe_float(value) -> float | None:
    if value is None:
        return None
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    if pd.isna(result):
        return None
    return result


def _compute_volume_ratio(volumes: list[float | None], period: int = 20) -> float | None:
    cleaned = [value for value in volumes if value is not None]
    if len(cleaned) < period:
        return None
    window = cleaned[-period:]
    avg = sum(window) / period
    if avg <= 0:
        return None
    return cleaned[-1] / avg


def _normalize_header_label(value: str) -> str:
    text = str(value).strip().lower()
    text = text.replace("（", "(").replace("）", ")")
    text = re.sub(r"[\\s_\\-]", "", text)
    text = re.sub(r"[()]", "", text)
    text = text.replace("株数", "株")
    return text


def _match_header_key(value: str) -> str | None:
    normalized = _normalize_header_label(value)
    for key, aliases in HEADER_ALIASES.items():
        if normalized in aliases:
            return key
    return None


def _map_headered_frame(df: pd.DataFrame) -> pd.DataFrame | None:
    mapping: dict[str, str] = {}
    for col in df.columns:
        key = _match_header_key(col)
        if key and key not in mapping:
            mapping[key] = col
    required = {"code", "date", "o", "h", "l", "c"}
    if not required.issubset(mapping.keys()):
        return None
    result = pd.DataFrame(
        {
            "code": df[mapping["code"]],
            "date": df[mapping["date"]],
            "o": df[mapping["o"]],
            "h": df[mapping["h"]],
            "l": df[mapping["l"]],
            "c": df[mapping["c"]],
            "v": df[mapping["v"]] if "v" in mapping else None,
        }
    )
    return result


def _compute_monthly_box_info(monthly_df: pd.DataFrame, config: dict) -> dict:
    bars: list[dict] = []
    if monthly_df.empty:
        return {
            "status": None,
            "duration": None,
            "upper": None,
            "lower": None,
            "ma20_trend": None
        }
    monthly_sorted = monthly_df.sort_values("month")
    for row in monthly_sorted[["month", "h", "l", "c"]].itertuples(index=False):
        month_value, high, low, close = row
        high_v = _safe_float(high)
        low_v = _safe_float(low)
        close_v = _safe_float(close)
        if month_value is None or high_v is None or low_v is None or close_v is None:
            continue
        bars.append(
            {
                "time": int(month_value),
                "high": high_v,
                "low": low_v,
                "close": close_v
            }
        )
    if len(bars) < config["BOX_MONTHS_MIN"]:
        last_close = bars[-1]["close"] if bars else None
        ma20 = None
        ma20_trend = None
        if bars:
            ma20_series = _build_ma_series([item["close"] for item in bars], 20)
            ma20 = ma20_series[-1] if ma20_series else None
        if last_close is not None and ma20 is not None:
            ma20_trend = 1 if last_close >= ma20 else -1
        return {
            "status": None,
            "duration": None,
            "upper": None,
            "lower": None,
            "ma20_trend": ma20_trend
        }

    bars.sort(key=lambda item: item["time"])
    closes = [item["close"] for item in bars]
    ma20_series = _build_ma_series(closes, 20)
    ma20 = ma20_series[-1] if ma20_series else None
    last_close = closes[-1] if closes else None
    ma20_trend = None
    if last_close is not None and ma20 is not None:
        ma20_trend = 1 if last_close >= ma20 else -1

    box_status = None
    box_duration = None
    box_upper = None
    box_lower = None
    min_months = config["BOX_MONTHS_MIN"]
    max_range_pct = config["BOX_RANGE_PCT"]
    for length in range(len(bars), min_months - 1, -1):
        window = bars[-length:]
        upper = max(item["high"] for item in window)
        lower = min(item["low"] for item in window)
        base = max(lower, 1e-9)
        range_pct = (upper - lower) / base
        if range_pct > max_range_pct:
            continue
        box_duration = length
        box_upper = upper
        box_lower = lower
        pre = bars[:-length]
        trend = None
        if len(pre) >= 2:
            if pre[-1]["close"] > pre[0]["close"]:
                trend = "up"
            elif pre[-1]["close"] < pre[0]["close"]:
                trend = "down"
        if trend == "up" and ma20_trend == 1:
            box_status = "Ceiling_Box"
        elif trend == "down" and last_close is not None and box_lower is not None:
            if last_close <= box_lower * (1 + config["SIGNAL_ZONE_PCT"]):
                box_status = "Bottom_Box"
        else:
            box_status = "None"
        break

    if box_duration is None:
        if ma20_trend == 1:
            box_status = "Trend_Up"
        else:
            box_status = None

    return {
        "status": box_status,
        "duration": box_duration,
        "upper": box_upper,
        "lower": box_lower,
        "ma20_trend": ma20_trend
    }


def _compute_daily_signal_flags(
    daily_df: pd.DataFrame,
    box_upper: float | None,
    box_lower: float | None,
    config: dict
) -> tuple[dict, int | None, int | None]:
    if daily_df.empty:
        return {}, None, None
    daily_sorted = daily_df.sort_values("date")
    closes = daily_sorted["c"].astype(float).tolist()
    last_close = closes[-1] if closes else None

    window = config["HIGARA_SWING_WINDOW"]
    series = pd.Series(closes)
    rolling_high = series.rolling(window=window, min_periods=1).max()
    rolling_low = series.rolling(window=window, min_periods=1).min()
    peak_mask = series == rolling_high
    bottom_mask = series == rolling_low
    last_peak_index = int(peak_mask[peak_mask].index.max()) if peak_mask.any() else None
    last_bottom_index = int(bottom_mask[bottom_mask].index.max()) if bottom_mask.any() else None
    days_since_peak = None
    if last_peak_index is not None:
        days_since_peak = len(series) - 1 - last_peak_index
    days_since_bottom = None
    if last_bottom_index is not None:
        days_since_bottom = len(series) - 1 - last_bottom_index

    targets = config["HIGARA_TARGET_DAYS"]
    tol = config["HIGARA_DAY_RANGE"]
    def _hit(days: int | None) -> bool:
        if days is None:
            return False
        return any(abs(days - target) <= tol for target in targets)

    near_upper = False
    near_lower = False
    if last_close is not None:
        if box_upper is not None:
            near_upper = abs(last_close - box_upper) / box_upper <= config["SIGNAL_ZONE_PCT"]
        if box_lower is not None:
            near_lower = abs(last_close - box_lower) / box_lower <= config["SIGNAL_ZONE_PCT"]
    in_zone = near_upper or near_lower

    hitei = False
    tsutsumi = False
    gap = False
    if len(daily_sorted) >= 2:
        prev = daily_sorted.iloc[-2]
        curr = daily_sorted.iloc[-1]
        prev_open = float(prev["o"])
        prev_close = float(prev["c"])
        curr_open = float(curr["o"])
        curr_close = float(curr["c"])
        prev_body = abs(prev_close - prev_open)
        curr_body = abs(curr_close - curr_open)
        prev_bull = prev_close > prev_open
        prev_bear = prev_close < prev_open
        curr_bull = curr_close > curr_open
        curr_bear = curr_close < curr_open

        if prev_body > 0:
            hitei_ratio = curr_body / prev_body
            hitei = prev_bull and curr_bear and hitei_ratio >= config["HITEI_BODY_RATIO"]

        body_min_prev = min(prev_open, prev_close)
        body_max_prev = max(prev_open, prev_close)
        body_min_curr = min(curr_open, curr_close)
        body_max_curr = max(curr_open, curr_close)
        tsutsumi = (
            prev_bear
            and curr_bull
            and body_min_curr <= body_min_prev
            and body_max_curr >= body_max_prev
        )

        if prev_close > 0:
            gap = abs(curr_open - prev_close) / prev_close >= config["GAP_PCT"]

    if not in_zone:
        hitei = False
        tsutsumi = False

    flags = {
        "hitei": bool(hitei),
        "tsutsumi": bool(tsutsumi),
        "gap": bool(gap),
        "box_near_upper": bool(near_upper),
        "box_near_lower": bool(near_lower),
        "higara_peak_hit": _hit(days_since_peak),
        "higara_bottom_hit": _hit(days_since_bottom),
        "higara_peak_days": days_since_peak,
        "higara_bottom_days": days_since_bottom
    }
    return flags, days_since_peak, days_since_bottom


def _detect_body_box(monthly_rows: list[tuple]) -> dict | None:
    min_months = 3
    max_months = 14
    max_range_pct = 0.2
    wild_wick_pct = 0.1

    bars: list[dict] = []
    for row in monthly_rows:
        if len(row) < 5:
            continue
        month_value, open_, high, low, close = row[:5]
        open_v = _safe_float(open_)
        high_v = _safe_float(high)
        low_v = _safe_float(low)
        close_v = _safe_float(close)
        if month_value is None or open_v is None or high_v is None or low_v is None or close_v is None:
            continue
        body_high = max(open_v, close_v)
        body_low = min(open_v, close_v)
        bars.append(
            {
                "time": int(month_value),
                "open": open_v,
                "high": high_v,
                "low": low_v,
                "close": close_v,
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
            "wild": wild,
            "range_pct": range_pct
        }

    return None


def compute_stage_score(
    daily_df: pd.DataFrame, monthly_df: pd.DataFrame
) -> tuple[str, float | None, str, list[str], dict]:
    missing_reasons: list[str] = []
    score_breakdown: dict[str, float] = {}

    daily = daily_df.sort_values("date")
    closes = [float(v) for v in daily["c"].tolist() if _safe_float(v) is not None]
    volumes = [_safe_float(v) for v in daily["v"].tolist()]

    last_close = closes[-1] if closes else None
    if last_close is None:
        missing_reasons.append("missing_last_close")

    if len(closes) < 60:
        missing_reasons.append("insufficient_daily_bars")

    ma7_series = _build_ma_series(closes, 7)
    ma20_series = _build_ma_series(closes, 20)
    ma60_series = _build_ma_series(closes, 60)
    ma100_series = _build_ma_series(closes, 100)

    ma7 = ma7_series[-1] if ma7_series else None
    ma20 = ma20_series[-1] if ma20_series else None
    ma60 = ma60_series[-1] if ma60_series else None
    ma100 = ma100_series[-1] if ma100_series else None

    if ma20 is None:
        missing_reasons.append("missing_ma20")
    if ma60 is None:
        missing_reasons.append("missing_ma60")
    if ma100 is None:
        missing_reasons.append("missing_ma100")

    slope20 = (
        ma20_series[-1] - ma20_series[-2]
        if len(ma20_series) >= 2 and ma20_series[-1] is not None and ma20_series[-2] is not None
        else None
    )
    slope60 = (
        ma60_series[-1] - ma60_series[-2]
        if len(ma60_series) >= 2 and ma60_series[-1] is not None and ma60_series[-2] is not None
        else None
    )

    monthly = monthly_df.sort_values("month")
    monthly_rows = monthly[["month", "o", "h", "l", "c"]].values.tolist()
    monthly_closes = [
        _safe_float(row[4]) for row in monthly_rows if len(row) >= 5 and _safe_float(row[4]) is not None
    ]

    if len(monthly_closes) < 3:
        missing_reasons.append("insufficient_monthly_bars")

    chg1m = _pct_change(monthly_closes[-1], monthly_closes[-2]) if len(monthly_closes) >= 2 else None
    chg1q = _pct_change(monthly_closes[-1], monthly_closes[-4]) if len(monthly_closes) >= 4 else None
    chg1y = _pct_change(monthly_closes[-1], monthly_closes[-13]) if len(monthly_closes) >= 13 else None

    if chg1m is None:
        missing_reasons.append("missing_chg1m")
    if chg1q is None:
        missing_reasons.append("missing_chg1q")
    if chg1y is None:
        missing_reasons.append("missing_chg1y")

    box = _detect_body_box(monthly_rows)
    if box is None and len(monthly_rows) >= 3:
        missing_reasons.append("no_box")

    box_active = False
    breakout_up = False
    if box and monthly_rows:
        latest_month = int(monthly_rows[-1][0])
        prev_month = int(monthly_rows[-2][0]) if len(monthly_rows) >= 2 else None
        if box["start"] <= latest_month <= box["end"]:
            box_active = True
        elif prev_month is not None and box["start"] <= prev_month <= box["end"]:
            box_active = True
        if box_active and last_close is not None and last_close > box["upper"]:
            breakout_up = True

    essential_missing = (
        last_close is None
        or ma20 is None
        or ma60 is None
        or len(closes) < 60
    )

    if essential_missing:
        return "UNKNOWN", None, "INSUFFICIENT_DATA", missing_reasons, score_breakdown

    up60 = _count_streak(closes, ma60_series, "up")
    down60 = _count_streak(closes, ma60_series, "down")
    down20 = _count_streak(closes, ma20_series, "down")
    up20 = _count_streak(closes, ma20_series, "up")

    stage = "B"
    if up60 is not None and (up60 >= 22 or (last_close > ma60 and (slope60 or 0) >= 0)):
        stage = "C"
    elif down60 is not None and down20 is not None and down60 >= 20 and down20 >= 10:
        stage = "A"

    trend = 0.0
    if last_close > ma20:
        trend += 8
    if ma20 > ma60:
        trend += 10
    if last_close > ma60:
        trend += 12
    if ma60 is not None and ma100 is not None and ma60 > ma100:
        trend += 10
    if slope20 is not None and slope20 > 0:
        trend += 3
    trend = min(40, trend)

    init_move = 0.0
    if len(closes) >= 2 and len(ma20_series) >= 2:
        prev_ma20 = ma20_series[-2]
        if prev_ma20 is not None and closes[-2] <= prev_ma20 and last_close > ma20:
            init_move += 15
    if breakout_up:
        init_move += 10
    init_move = min(25, init_move)

    base_build = 0.0
    if stage == "A" and ma20 is not None and last_close >= ma20 * 0.98:
        base_build += 8
    if ma7 is not None and len(ma7_series) >= 2 and ma7_series[-2] is not None:
        if ma7 > ma7_series[-2]:
            base_build += 4
    base_build = min(15, base_build)

    box_score = 0.0
    if breakout_up:
        box_score += 12
    elif box_active:
        box_score += 8
    box_score = min(15, box_score)

    volume_score = 0.0
    volume_ratio = _compute_volume_ratio(volumes, 20)
    if volume_ratio is not None and volume_ratio >= 1.5:
        volume_score = 5
    elif volume_ratio is not None and volume_ratio >= 1.1:
        volume_score = 2

    penalty = 0.0
    if up20 is not None and up20 >= 20:
        penalty -= 5
    if up60 is not None and up60 >= 22:
        penalty -= 10
    penalty = max(-20, penalty)

    score_breakdown = {
        "trend": trend,
        "init_move": init_move,
        "base_build": base_build,
        "box": box_score,
        "volume": volume_score,
        "penalty": penalty
    }

    score = trend + init_move + base_build + box_score + volume_score + penalty
    score = max(0.0, min(100.0, score))

    return stage, round(score, 3), "OK", missing_reasons, score_breakdown



def load_watchlist(data_dir: str) -> list[str]:
    path = find_code_txt_path(data_dir) if USE_CODE_TXT else None
    exists = bool(path and os.path.exists(path))
    if not USE_CODE_TXT:
        print("WATCHLIST_PATH=disabled exists=false count=0")
        return []
    if not path:
        print("WARNING: watchlist missing. WATCHLIST_PATH=none exists=false count=0")
        return []
    if not exists:
        print(f"WARNING: watchlist missing. WATCHLIST_PATH={path} exists=false count=0")
        return []
    try:
        with open(path, "r", encoding="utf-8") as f:
            codes = [line.strip() for line in f.readlines() if line.strip()]
        print(f"WATCHLIST_PATH={path} exists=true count={len(codes)}")
        return codes
    except OSError as exc:
        print(f"WARNING: watchlist read failed. WATCHLIST_PATH={path} exists=true count=0 reason={exc}")
        return []


def list_txt_files(data_dir: str) -> list[str]:
    if not os.path.isdir(data_dir):
        return []
    return [
        os.path.join(data_dir, name)
        for name in os.listdir(data_dir)
        if name.endswith(".txt") and name.lower() != "code.txt"
    ]


def read_csv_with_fallback(path: str) -> pd.DataFrame:
    encodings = ["utf-8", "shift_jis", "cp932"]
    last_err: Exception | None = None
    for encoding in encodings:
        try:
            header_df = pd.read_csv(path, header=0, dtype="string", encoding=encoding)
            mapped = _map_headered_frame(header_df)
            if mapped is not None:
                return mapped
            return pd.read_csv(
                path,
                header=None,
                names=["code", "date", "o", "h", "l", "c", "v"],
                dtype="string",
                encoding=encoding,
                usecols=[0, 1, 2, 3, 4, 5, 6]
            )
        except Exception as exc:
            last_err = exc
    if last_err:
        raise last_err
    raise RuntimeError("Failed to read CSV")


def strip_header_row(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    first = df.iloc[0].astype(str).tolist()
    header_hits = {_match_header_key(value) for value in first}
    header_hits.discard(None)
    if "code" in header_hits and "date" in header_hits:
        return df.iloc[1:].reset_index(drop=True)
    return df


def normalize_code(df: pd.DataFrame) -> tuple[pd.DataFrame, int, int, int]:
    df["code"] = df["code"].where(df["code"].notna(), "")
    df["code"] = df["code"].astype(str).str.strip()
    missing_mask = (df["code"] == "") | (df["code"].str.lower() == "nan")
    missing_count = int(missing_mask.sum())
    df = df[~missing_mask]

    nonstandard_mask = ~df["code"].str.match(CODE_PATTERN, na=False)
    nonstandard_count = int(nonstandard_mask.sum())
    invalid_count = 0
    if STRICT_CODE_VALIDATION and nonstandard_count:
        invalid_count = nonstandard_count
        df = df[~nonstandard_mask]

    return df, missing_count, nonstandard_count, invalid_count


def parse_file(path: str, watchlist: set[str] | None, counts: dict) -> pd.DataFrame:
    try:
        df = read_csv_with_fallback(path)
    except Exception as exc:
        counts["file_error"] += 1
        print(f"Warning: failed to read {path}: {exc}")
        return pd.DataFrame(columns=["code", "date", "o", "h", "l", "c", "v"])

    df = strip_header_row(df)
    if df.empty:
        return df

    df, missing_code, nonstandard_code, invalid_code = normalize_code(df)
    counts["missing_code"] += missing_code
    counts["nonstandard_code"] += nonstandard_code
    counts["invalid_code"] += invalid_code

    if df.empty:
        return df

    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    invalid_date = int(df["date"].isna().sum())
    counts["invalid_date"] += invalid_date
    df = df[df["date"].notna()]
    if df.empty:
        return df

    if watchlist:
        before = len(df)
        df = df[df["code"].isin(watchlist)]
        counts["filtered_watchlist"] += int(before - len(df))

    if df.empty:
        return df

    for col in ["o", "h", "l", "c", "v"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    non_numeric_mask = df[["o", "h", "l", "c"]].isna().any(axis=1)
    counts["non_numeric"] += int(non_numeric_mask.sum())
    df = df[~non_numeric_mask]
    if df.empty:
        return df

    df["v"] = df["v"].round().astype("Int64")
    return df


def read_daily_files(
    files: list[str], watchlist: set[str] | None, counts: dict
) -> tuple[pd.DataFrame, dict[str, str]]:
    latest_by_code: dict[str, tuple[float, pd.DataFrame]] = {}
    name_map: dict[str, str] = {}
    for path in files:
        df = parse_file(path, watchlist, counts)
        if df.empty:
            continue

        mtime = os.path.getmtime(path)
        for code, group in df.groupby("code"):
            existing = latest_by_code.get(code)
            if existing is None:
                latest_by_code[code] = (mtime, group)
                display_name = name_from_filename(path, code)
                if display_name:
                    name_map[code] = display_name
                continue
            if existing[0] >= mtime:
                counts["older_file"] += len(group)
                continue
            counts["older_file"] += len(existing[1])
            latest_by_code[code] = (mtime, group)
            display_name = name_from_filename(path, code)
            if display_name:
                name_map[code] = display_name

    frames = [entry[1] for entry in latest_by_code.values()]
    if not frames:
        return pd.DataFrame(columns=["code", "date", "o", "h", "l", "c", "v"]), name_map

    daily = pd.concat(frames, ignore_index=True)
    daily["date"] = daily["date"].dt.tz_localize("UTC")
    daily["date"] = (daily["date"].astype("int64") // 1_000_000_000).astype("int64")
    before_dedup = len(daily)
    daily = daily.sort_values(["code", "date"]).drop_duplicates(["code", "date"], keep="last")
    counts["duplicate_rows"] += int(before_dedup - len(daily))
    return daily, name_map


def build_monthly(daily: pd.DataFrame) -> pd.DataFrame:
    daily_dt = pd.to_datetime(daily["date"], unit="s", utc=True)
    daily = daily.assign(dt=daily_dt)
    daily["month"] = daily["dt"].dt.to_period("M").dt.to_timestamp()
    grouped = daily.sort_values("dt").groupby(["code", "month"], as_index=False)
    monthly = grouped.agg(
        o=("o", "first"),
        h=("h", "max"),
        l=("l", "min"),
        c=("c", "last"),
        v=("v", lambda s: s.sum(min_count=1))
    )
    monthly["month"] = (monthly["month"].astype("int64") // 1_000_000_000).astype("int64")
    return monthly



def build_monthly_ma(monthly: pd.DataFrame) -> pd.DataFrame:
    monthly = monthly.sort_values(["code", "month"]).copy()
    monthly["ma7"] = monthly.groupby("code")["c"].rolling(7).mean().reset_index(level=0, drop=True)
    monthly["ma20"] = monthly.groupby("code")["c"].rolling(20).mean().reset_index(level=0, drop=True)
    monthly["ma60"] = monthly.groupby("code")["c"].rolling(60).mean().reset_index(level=0, drop=True)
    return monthly[["code", "month", "ma7", "ma20", "ma60"]]


def build_daily_ma(daily: pd.DataFrame) -> pd.DataFrame:
    daily = daily.sort_values(["code", "date"]).copy()
    daily["ma7"] = daily.groupby("code")["c"].rolling(7).mean().reset_index(level=0, drop=True)
    daily["ma20"] = daily.groupby("code")["c"].rolling(20).mean().reset_index(level=0, drop=True)
    daily["ma60"] = daily.groupby("code")["c"].rolling(60).mean().reset_index(level=0, drop=True)
    return daily[["code", "date", "ma7", "ma20", "ma60"]]


def build_feature_snapshot_daily(daily: pd.DataFrame, daily_ma: pd.DataFrame) -> pd.DataFrame:
    if daily.empty:
        return pd.DataFrame(
            columns=[
                "dt",
                "code",
                "close",
                "ma7",
                "ma20",
                "ma60",
                "atr14",
                "diff20_pct",
                "diff20_atr",
                "cnt_20_above",
                "cnt_7_above",
                "day_count",
                "candle_flags"
            ]
        )

    merged = daily.sort_values(["code", "date"]).merge(
        daily_ma.sort_values(["code", "date"]),
        on=["code", "date"],
        how="left"
    )

    snapshots: list[pd.DataFrame] = []
    for _, group in merged.groupby("code"):
        group = group.sort_values("date").copy()
        closes = [float(v) for v in group["c"].tolist()]
        ma7_series = group["ma7"].tolist()
        ma20_series = group["ma20"].tolist()
        cnt_7_above = _build_streak_series(closes, ma7_series, "up")
        cnt_20_above = _build_streak_series(closes, ma20_series, "up")
        group["cnt_7_above"] = cnt_7_above
        group["cnt_20_above"] = cnt_20_above
        group["atr14"] = None
        group["diff20_pct"] = None
        group["diff20_atr"] = None
        group["day_count"] = None
        group["candle_flags"] = None

        valid = group["ma20"].notna() & group["c"].notna() & (group["ma20"] != 0)
        group.loc[valid, "diff20_pct"] = (group.loc[valid, "c"] - group.loc[valid, "ma20"]) / group.loc[valid, "ma20"]

        snapshots.append(
            group[
                [
                    "date",
                    "code",
                    "c",
                    "ma7",
                    "ma20",
                    "ma60",
                    "atr14",
                    "diff20_pct",
                    "diff20_atr",
                    "cnt_20_above",
                    "cnt_7_above",
                    "day_count",
                    "candle_flags"
                ]
            ].rename(columns={"date": "dt", "c": "close"})
        )

    return pd.concat(snapshots, ignore_index=True)


def build_stock_meta(
    daily: pd.DataFrame,
    monthly: pd.DataFrame,
    name_map: dict[str, str]
) -> tuple[pd.DataFrame, dict]:
    now = datetime.now(tz=timezone.utc)
    records = []
    score_ok_count = 0
    score_insufficient_count = 0
    stage_counts: dict[str, int] = {}
    missing_reason_counts: dict[str, int] = {}

    daily_groups = {code: group for code, group in daily.groupby("code")}
    monthly_groups = {code: group for code, group in monthly.groupby("code")}

    for code, group in daily_groups.items():
        monthly_group = monthly_groups.get(code, pd.DataFrame(columns=monthly.columns))
        stage, score, score_status, missing_reasons, score_breakdown = compute_stage_score(
            group, monthly_group
        )
        box_info = _compute_monthly_box_info(monthly_group, TRADE_FLAG_CONFIG)
        signal_flags, days_since_peak, days_since_bottom = _compute_daily_signal_flags(
            group,
            box_info.get("upper"),
            box_info.get("lower"),
            TRADE_FLAG_CONFIG
        )
        latest_close = None
        if not group.empty:
            latest_close = _safe_float(group.sort_values("date")["c"].iloc[-1])
        if score_breakdown is not None:
            score_breakdown = {
                **score_breakdown,
                "trade_flags": {
                    "monthly_box_status": box_info.get("status"),
                    "box_duration": box_info.get("duration"),
                    "hitei": signal_flags.get("hitei"),
                    "tsutsumi": signal_flags.get("tsutsumi"),
                    "gap": signal_flags.get("gap"),
                    "higara_peak_hit": signal_flags.get("higara_peak_hit"),
                    "higara_bottom_hit": signal_flags.get("higara_bottom_hit")
                }
            }
        stage_counts[stage] = stage_counts.get(stage, 0) + 1
        if score_status == "OK":
            score_ok_count += 1
        else:
            score_insufficient_count += 1
        for reason in missing_reasons:
            missing_reason_counts[reason] = missing_reason_counts.get(reason, 0) + 1
        records.append(
            {
                "code": code,
                "name": name_map.get(code, code),
                "stage": stage,
                "score": score,
                "reason": score_status,
                "score_status": score_status,
                "missing_reasons_json": json.dumps(missing_reasons, ensure_ascii=False),
                "score_breakdown_json": json.dumps(score_breakdown, ensure_ascii=False),
                "latest_close": latest_close,
                "monthly_box_status": box_info.get("status"),
                "box_duration": box_info.get("duration"),
                "box_upper": box_info.get("upper"),
                "box_lower": box_info.get("lower"),
                "ma20_monthly_trend": box_info.get("ma20_trend"),
                "days_since_peak": days_since_peak,
                "days_since_bottom": days_since_bottom,
                "signal_flags": json.dumps(signal_flags, ensure_ascii=False),
                "updated_at": now
            }
        )
    summary = {
        "score_ok": score_ok_count,
        "score_insufficient": score_insufficient_count,
        "stage_counts": stage_counts,
        "missing_reason_counts": missing_reason_counts
    }
    return pd.DataFrame(records), summary


def clear_tables() -> None:
    with get_conn() as conn:
        conn.execute("DELETE FROM daily_bars")
        conn.execute("DELETE FROM daily_ma")
        conn.execute("DELETE FROM feature_snapshot_daily")
        conn.execute("DELETE FROM monthly_bars")
        conn.execute("DELETE FROM monthly_ma")
        conn.execute("DELETE FROM stock_meta")
        conn.execute("DELETE FROM tickers")


def log_counts(counts: dict, parsed_rows: int) -> None:
    skipped_total = sum(
        counts[key]
        for key in [
            "missing_code",
            "invalid_date",
            "non_numeric",
            "invalid_code",
            "older_file",
            "filtered_watchlist",
            "duplicate_rows"
        ]
    )
    reason_text = (
        f"missing_code={counts['missing_code']}, "
        f"invalid_date={counts['invalid_date']}, "
        f"non_numeric={counts['non_numeric']}, "
        f"invalid_code={counts['invalid_code']}, "
        f"older_file={counts['older_file']}, "
        f"filtered_watchlist={counts['filtered_watchlist']}, "
        f"duplicate_rows={counts['duplicate_rows']}"
    )
    print(f"PARSED_ROWS={parsed_rows}")
    print(f"SKIPPED_ROWS={skipped_total} ({reason_text})")
    print(f"NONSTANDARD_CODE_ROWS={counts['nonstandard_code']}")
    print(f"FILE_ERRORS={counts['file_error']}")


def log_volume_stats(stage: str, df: pd.DataFrame) -> None:
    if "v" not in df.columns:
        print(f"VOLUME_STATS stage={stage} missing_column=true")
        return
    series = df["v"]
    total = len(series)
    nulls = int(series.isna().sum())
    zeros = int(series.eq(0).fillna(False).sum())
    non_null = series.dropna()
    min_v = int(non_null.min()) if not non_null.empty else None
    max_v = int(non_null.max()) if not non_null.empty else None
    print(
        f"VOLUME_STATS stage={stage} total={total} null={nulls} zero={zeros} "
        f"min={min_v} max={max_v}"
    )


def _load_ingest_state() -> dict[str, float]:
    if not os.path.exists(INGEST_STATE_PATH):
        return {}
    try:
        with open(INGEST_STATE_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def _save_ingest_state(state: dict[str, float]) -> None:
    try:
        with open(INGEST_STATE_PATH, "w", encoding="utf-8") as f:
            json.dump(state, f, indent=2)
    except Exception as e:
        print(f"Warning: Failed to save ingest state: {e}")


def ingest(incremental: bool = False) -> None:
    def step_start(label: str) -> float:
        print(f"[STEP_START] {label}")
        return time.perf_counter()

    def step_end(label: str, start: float, **stats) -> None:
        elapsed_ms = int((time.perf_counter() - start) * 1000)
        stats_text = " ".join(
            f"{key}={value}" for key, value in stats.items() if value is not None
        )
        if stats_text:
            print(f"[STEP_END] {label} ms={elapsed_ms} {stats_text}")
        else:
            print(f"[STEP_END] {label} ms={elapsed_ms}")

    total_start = time.perf_counter()

    start = step_start("init_schema")
    init_schema()
    step_end("init_schema", start)

    start = step_start("list_txt_files")
    print(f"TXT_DIR={DATA_DIR}")
    files = list_txt_files(DATA_DIR)
    
    # Differential Logic
    state = _load_ingest_state()
    new_state = {}
    changed_files = []
    
    total_bytes = 0
    skipped_count = 0
    now_ts = time.time()
    
    force_full = False
    
    for path in files:
        try:
            mtime = os.path.getmtime(path)
            size = os.path.getsize(path)
            filename = os.path.basename(path)
            new_state[filename] = mtime
            total_bytes += size
            
            # Sanity Check: Future mtime (allow 1 day slack)
            if mtime > now_ts + 86400:
                print(f"Warning: File {filename} has future mtime. Forcing full update.")
                force_full = True
            
            if incremental and not force_full:
                last_mtime = state.get(filename)
                if last_mtime is not None:
                    # Sanity Check: Size drop? (Optional, but user suggested)
                    # We don't store last size in state, so we can't check size drop easily 
                    # unless we update state schema. Skipping size check for now.
                    if mtime <= last_mtime:
                        skipped_count += 1
                        continue
            
            changed_files.append(path)
        except OSError:
            pass

    if incremental and not force_full:
        print(f"Incremental Mode: Found {len(changed_files)} changed files, skipped {skipped_count}.")
        files = changed_files
    else:
        reason = "Forced Full" if force_full else "Full Mode"
        print(f"{reason}: Processing {len(files)} files.")
        incremental = False # Disable incremental flag for DB operations downstream

    step_end("list_txt_files", start, file_count=len(files), total_bytes=total_bytes, skipped=skipped_count)

    counts = {
        "missing_code": 0,
        "invalid_date": 0,
        "non_numeric": 0,
        "invalid_code": 0,
        "older_file": 0,
        "filtered_watchlist": 0,
        "duplicate_rows": 0,
        "nonstandard_code": 0,
        "file_error": 0
    }

    if not files:
        if not incremental:
             clear_tables()
             log_counts(counts, 0)
             print("No TXT data found. Tables cleared.")
        else:
             print("No changed files to process.")
             _save_ingest_state(new_state) # Update state anyway to sync mtimes
        
        total_ms = int((time.perf_counter() - total_start) * 1000)
        print(f"[STEP_END] ingest_total ms={total_ms} rows=0")
        return

    start = step_start("load_watchlist")
    watchlist = load_watchlist(DATA_DIR)
    step_end("load_watchlist", start, watchlist_count=len(watchlist))

    start = step_start("read_daily_files")
    daily, name_map = read_daily_files(files, watchlist, counts)
    daily_rows = len(daily)
    daily_codes = int(daily["code"].nunique()) if not daily.empty else 0
    step_end("read_daily_files", start, daily_rows=daily_rows, daily_codes=daily_codes)
    if not daily.empty:
        log_volume_stats("after_read", daily)

    if daily.empty:
        clear_tables()
        log_counts(counts, 0)
        print("No valid TXT rows found. Tables cleared.")
        total_ms = int((time.perf_counter() - total_start) * 1000)
        print(f"[STEP_END] ingest_total ms={total_ms} rows=0")
        return

    start = step_start("build_monthly")
    monthly = build_monthly(daily)
    step_end("build_monthly", start, monthly_rows=len(monthly))

    start = step_start("build_monthly_ma")
    monthly_ma = build_monthly_ma(monthly)
    step_end("build_monthly_ma", start, monthly_ma_rows=len(monthly_ma))

    start = step_start("build_daily_ma")
    daily_ma = build_daily_ma(daily)
    step_end("build_daily_ma", start, daily_ma_rows=len(daily_ma))

    start = step_start("build_feature_snapshot_daily")
    feature_snapshot = build_feature_snapshot_daily(daily, daily_ma)
    step_end("build_feature_snapshot_daily", start, snapshot_rows=len(feature_snapshot))

    start = step_start("build_stock_meta")
    meta, meta_summary = build_stock_meta(daily, monthly, name_map)
    step_end("build_stock_meta", start, meta_rows=len(meta), score_ok=meta_summary.get("score_ok"), score_insufficient=meta_summary.get("score_insufficient"))
    if meta_summary.get("stage_counts"):
        stage_counts = ",".join(
            f"{key}:{value}" for key, value in sorted(meta_summary["stage_counts"].items())
        )
        print(f"STAGE_COUNTS={stage_counts}")
    if meta_summary.get("missing_reason_counts"):
        missing_sorted = sorted(
            meta_summary["missing_reason_counts"].items(),
            key=lambda item: item[1],
            reverse=True
        )[:10]
        missing_text = ",".join(f"{key}:{value}" for key, value in missing_sorted)
        print(f"MISSING_REASONS_TOP={missing_text}")


    start = step_start("db_replace")
    log_volume_stats("pre_db", daily)
    
    with get_conn() as conn:
        if not incremental:
            conn.execute("DELETE FROM daily_bars")
            conn.execute("DELETE FROM daily_ma")
            conn.execute("DELETE FROM feature_snapshot_daily")
            conn.execute("DELETE FROM monthly_bars")
            conn.execute("DELETE FROM monthly_ma")
            conn.execute("DELETE FROM stock_meta")
            conn.execute("DELETE FROM tickers")
        else:
            # Incremental: Delete only processed codes
            codes = daily["code"].unique().tolist()
            if codes:
                placeholders = ",".join(["?"] * len(codes))
                # Note: DuckDB supports DELETE FROM ... WHERE code IN (...)
                # We need to run delete for all tables
                conn.execute(f"DELETE FROM daily_bars WHERE code IN ({placeholders})", codes)
                conn.execute(f"DELETE FROM daily_ma WHERE code IN ({placeholders})", codes)
                conn.execute(f"DELETE FROM feature_snapshot_daily WHERE code IN ({placeholders})", codes)
                conn.execute(f"DELETE FROM monthly_bars WHERE code IN ({placeholders})", codes)
                conn.execute(f"DELETE FROM monthly_ma WHERE code IN ({placeholders})", codes)
                conn.execute(f"DELETE FROM stock_meta WHERE code IN ({placeholders})", codes)
                conn.execute(f"DELETE FROM tickers WHERE code IN ({placeholders})", codes)

        conn.register("daily_df", daily)
        conn.execute("INSERT INTO daily_bars SELECT code, date, o, h, l, c, v FROM daily_df")

        conn.register("daily_ma_df", daily_ma)
        conn.execute("INSERT INTO daily_ma SELECT code, date, ma7, ma20, ma60 FROM daily_ma_df")

        conn.register("feature_snapshot_df", feature_snapshot)
        conn.execute(
            """
            INSERT INTO feature_snapshot_daily (
                dt,
                code,
                close,
                ma7,
                ma20,
                ma60,
                atr14,
                diff20_pct,
                diff20_atr,
                cnt_20_above,
                cnt_7_above,
                day_count,
                candle_flags
            )
            SELECT
                dt,
                code,
                close,
                ma7,
                ma20,
                ma60,
                atr14,
                diff20_pct,
                diff20_atr,
                cnt_20_above,
                cnt_7_above,
                day_count,
                candle_flags
            FROM feature_snapshot_df
            """
        )

        conn.register("monthly_df", monthly)
        conn.execute("INSERT INTO monthly_bars SELECT code, month, o, h, l, c, v FROM monthly_df")

        conn.register("monthly_ma_df", monthly_ma)
        conn.execute("INSERT INTO monthly_ma SELECT code, month, ma7, ma20, ma60 FROM monthly_ma_df")

        conn.register("meta_df", meta)
        conn.execute(
            """
            INSERT INTO stock_meta (
                code,
                name,
                stage,
                score,
                reason,
                score_status,
                missing_reasons_json,
                score_breakdown_json,
                latest_close,
                monthly_box_status,
                box_duration,
                box_upper,
                box_lower,
                ma20_monthly_trend,
                days_since_peak,
                days_since_bottom,
                signal_flags,
                updated_at
            )
            SELECT
                code,
                name,
                stage,
                score,
                reason,
                score_status,
                missing_reasons_json,
                score_breakdown_json,
                latest_close,
                monthly_box_status,
                box_duration,
                box_upper,
                box_lower,
                ma20_monthly_trend,
                days_since_peak,
                days_since_bottom,
                signal_flags,
                updated_at
            FROM meta_df
            """
        )

        conn.execute("INSERT INTO tickers SELECT code, name FROM meta_df")
        
        # Ensure industry_master exists and is populated (fixes heatmap on fresh install)
        ensure_industry_master(conn)

    step_end("db_replace", start, daily_rows=len(daily), monthly_rows=len(monthly), meta_rows=len(meta))

    _save_ingest_state(new_state)

    log_counts(counts, len(daily))
    print(f"Inserted {len(meta)} tickers")
    print(f"Inserted {len(monthly)} monthly rows")
    print(f"Inserted {len(daily)} daily rows")
    total_ms = int((time.perf_counter() - total_start) * 1000)
    print(f"[STEP_END] ingest_total ms={total_ms} rows={len(daily)}")


def main() -> None:

    ingest()


if __name__ == "__main__":
    main()
