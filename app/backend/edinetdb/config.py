from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from app.core.config import config as core_config

JST = ZoneInfo("Asia/Tokyo")
_FREE_CUTOVER_DATE = date(2026, 3, 8)


def _now_jst(now: datetime | None = None) -> datetime:
    base = now or datetime.now(tz=JST)
    if base.tzinfo is None:
        return base.replace(tzinfo=JST)
    return base.astimezone(JST)


def _to_int(name: str, default: int, minimum: int = 1) -> int:
    raw = os.getenv(name)
    if raw is None or not str(raw).strip():
        return default
    try:
        value = int(str(raw).strip())
    except ValueError:
        return default
    return max(minimum, value)


def _to_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None or not str(raw).strip():
        return default
    return str(raw).strip().lower() in {"1", "true", "yes", "on"}


def _default_daily_budget(now_jst: datetime) -> int:
    return 100 if now_jst.date() >= _FREE_CUTOVER_DATE else 1000


def _resolve_db_path() -> Path:
    explicit = os.getenv("EDINETDB_DB_PATH") or os.getenv("STOCKS_DB_PATH")
    if explicit:
        return Path(explicit).expanduser().resolve()
    return Path(core_config.DB_PATH).expanduser().resolve()


def _resolve_raw_dir() -> Path:
    explicit = os.getenv("EDINETDB_RAW_DIR")
    if explicit:
        return Path(explicit).expanduser().resolve()
    return (Path("data") / "edinetdb" / "raw").resolve()


def mask_api_key(api_key: str | None) -> str:
    if not api_key:
        return "(unset)"
    if len(api_key) <= 4:
        return "*" * len(api_key)
    return f"{api_key[:4]}***"


@dataclass(frozen=True)
class EdinetdbConfig:
    api_keys: tuple[str, ...]
    base_url: str
    daily_budget: int
    daily_watch_analysis_enabled: bool
    daily_watch_analysis_reserve: int
    daily_watch_analysis_max_calls: int
    text_years_max: int
    raw_dir: Path
    db_path: Path
    rotation_buckets: int
    ranking_limit: int
    timeout_sec: int
    now_jst: datetime


def _load_api_keys() -> tuple[str, ...]:
    raw_multi = os.getenv("EDINETDB_API_KEYS")
    keys: list[str] = []
    if raw_multi:
        for item in str(raw_multi).split(","):
            key = item.strip()
            if key:
                keys.append(key)
    single = (os.getenv("EDINETDB_API_KEY") or "").strip()
    if single:
        keys.append(single)
    uniq: list[str] = []
    seen: set[str] = set()
    for key in keys:
        if key in seen:
            continue
        seen.add(key)
        uniq.append(key)
    return tuple(uniq)


def load_config(now: datetime | None = None) -> EdinetdbConfig:
    now_jst = _now_jst(now)
    daily_budget = _to_int(
        "EDINETDB_DAILY_BUDGET",
        _default_daily_budget(now_jst),
        minimum=1,
    )
    return EdinetdbConfig(
        api_keys=_load_api_keys(),
        base_url="https://edinetdb.jp/v1",
        daily_budget=daily_budget,
        daily_watch_analysis_enabled=_to_bool("EDINETDB_DAILY_WATCH_ANALYSIS_ENABLED", True),
        daily_watch_analysis_reserve=_to_int("EDINETDB_DAILY_WATCH_ANALYSIS_RESERVE", 12, minimum=0),
        daily_watch_analysis_max_calls=_to_int("EDINETDB_DAILY_WATCH_ANALYSIS_MAX_CALLS", 8, minimum=0),
        text_years_max=_to_int("EDINETDB_TEXT_YEARS_MAX", 6, minimum=1),
        raw_dir=_resolve_raw_dir(),
        db_path=_resolve_db_path(),
        rotation_buckets=_to_int("EDINETDB_ROTATION_BUCKETS", 7, minimum=1),
        ranking_limit=_to_int("EDINETDB_RANKING_LIMIT", 100, minimum=1),
        timeout_sec=_to_int("EDINETDB_TIMEOUT_SEC", 20, minimum=1),
        now_jst=now_jst,
    )
