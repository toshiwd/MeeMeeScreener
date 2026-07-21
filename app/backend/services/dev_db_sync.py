from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime
import logging
import os
from pathlib import Path
import shutil
import uuid

import duckdb

from shared.runtime_stock_db_contract import APP_NAME, DEV_APP_SUFFIX, RUNTIME_STOCK_DB_FILENAME


logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class DevDbSyncResult:
    attempted: bool
    synced: bool
    skipped_reason: str | None
    source_db_path: str
    dev_db_path: str
    confirmed_latest_date: int | None
    removed_yahoo_rows: int | None
    error: str | None = None

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True)
class RuntimePeerDbSyncResult:
    attempted: bool
    synced: bool
    skipped_reason: str | None
    source_db_path: str
    target_db_path: str
    confirmed_latest_date: int | None
    target_confirmed_latest_date_before: int | None
    removed_yahoo_rows: int | None
    error: str | None = None

    def to_dict(self) -> dict[str, object]:
        payload = asdict(self)
        # Backward-compatible aliases used by txt_update status text/tests.
        payload["dev_db_path"] = payload["target_db_path"]
        return payload


def _env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return str(raw).strip().lower() in {"1", "true", "yes", "on"}


def _local_appdata_data_dir(app_name: str) -> Path:
    base = os.getenv("LOCALAPPDATA") or str(Path.home())
    return Path(base) / app_name / "data"


def production_stock_db_path() -> Path:
    return _local_appdata_data_dir(APP_NAME) / RUNTIME_STOCK_DB_FILENAME


def development_stock_db_path() -> Path:
    return _local_appdata_data_dir(DEV_APP_SUFFIX) / RUNTIME_STOCK_DB_FILENAME


def _normalize(path: str | Path) -> Path:
    return Path(path).expanduser().resolve(strict=False)


def _same_path(left: Path, right: Path) -> bool:
    try:
        return os.path.normcase(str(left)) == os.path.normcase(str(right))
    except Exception:
        return str(left) == str(right)


def _latest_confirmed_date(conn: duckdb.DuckDBPyConnection) -> int | None:
    row = conn.execute(
        """
        SELECT MAX(
            CASE
                WHEN date BETWEEN 19000101 AND 20991231 THEN CAST(date AS INTEGER)
                WHEN date >= 1000000000000 THEN CAST(strftime(to_timestamp(date / 1000), '%Y%m%d') AS INTEGER)
                WHEN date >= 1000000000 THEN CAST(strftime(to_timestamp(date), '%Y%m%d') AS INTEGER)
                ELSE NULL
            END
        )
        FROM daily_bars
        WHERE COALESCE(source, 'pan') <> 'yahoo'
        """
    ).fetchone()
    return int(row[0]) if row and row[0] is not None else None


def _sanitize_confirmed_only(db_path: Path) -> tuple[int | None, int]:
    conn = duckdb.connect(str(db_path), read_only=False)
    try:
        tables = {
            str(row[0])
            for row in conn.execute(
                "SELECT table_name FROM information_schema.tables WHERE table_schema='main'"
            ).fetchall()
        }
        if "daily_bars" not in tables:
            return None, 0
        confirmed_latest = _latest_confirmed_date(conn)
        row = conn.execute(
            "SELECT COUNT(*) FROM daily_bars WHERE COALESCE(source, 'pan') = 'yahoo'"
        ).fetchone()
        removed = int(row[0] or 0) if row else 0
        conn.execute("DELETE FROM daily_bars WHERE COALESCE(source, 'pan') = 'yahoo'")
        conn.execute("CHECKPOINT")
        return confirmed_latest, removed
    finally:
        conn.close()


def _read_latest_confirmed_date(db_path: Path) -> int | None:
    if not db_path.exists():
        return None
    conn = duckdb.connect(str(db_path), read_only=True)
    try:
        tables = {
            str(row[0])
            for row in conn.execute(
                "SELECT table_name FROM information_schema.tables WHERE table_schema='main'"
            ).fetchall()
        }
        if "daily_bars" not in tables:
            return None
        return _latest_confirmed_date(conn)
    finally:
        conn.close()


def _local_runtime_peer_for_source(source: Path) -> Path | None:
    production = _normalize(production_stock_db_path())
    dev = _normalize(development_stock_db_path())
    if _same_path(source, production):
        return dev
    return None


def sync_updated_stock_db_to_local_peer(
    *,
    source_db_path: str | Path,
    target_db_path: str | Path | None = None,
) -> dict[str, object]:
    source = _normalize(source_db_path)
    target = _normalize(target_db_path) if target_db_path is not None else _local_runtime_peer_for_source(source)

    if not _env_bool("MEEMEE_SYNC_UPDATED_DB_TO_LOCAL_PEER", True):
        return RuntimePeerDbSyncResult(False, False, "disabled_by_env", str(source), str(target or ""), None, None, None).to_dict()
    if not source.exists():
        return RuntimePeerDbSyncResult(False, False, "source_missing", str(source), str(target or ""), None, None, None).to_dict()
    if target is None:
        return RuntimePeerDbSyncResult(False, False, "source_is_not_local_runtime_db", str(source), "", None, None, None).to_dict()
    if _same_path(source, target):
        return RuntimePeerDbSyncResult(False, False, "source_equals_target_db", str(source), str(target), None, None, None).to_dict()

    source_latest = _read_latest_confirmed_date(source)
    target_latest = _read_latest_confirmed_date(target)
    if source_latest is None:
        return RuntimePeerDbSyncResult(False, False, "source_confirmed_date_missing", str(source), str(target), None, target_latest, None).to_dict()
    if target_latest is not None and int(target_latest) >= int(source_latest):
        _, removed_yahoo = _sanitize_confirmed_only(target)
        return RuntimePeerDbSyncResult(
            True,
            removed_yahoo > 0,
            "target_not_older",
            str(source),
            str(target),
            source_latest,
            target_latest,
            removed_yahoo,
        ).to_dict()

    target.parent.mkdir(parents=True, exist_ok=True)
    temp_path = target.with_name(f"{target.name}.tmp-{os.getpid()}-{uuid.uuid4().hex}")
    try:
        shutil.copy2(source, temp_path)
        confirmed_latest, removed_yahoo = _sanitize_confirmed_only(temp_path)
        os.replace(temp_path, target)
        result = RuntimePeerDbSyncResult(
            attempted=True,
            synced=True,
            skipped_reason=None,
            source_db_path=str(source),
            target_db_path=str(target),
            confirmed_latest_date=confirmed_latest,
            target_confirmed_latest_date_before=target_latest,
            removed_yahoo_rows=removed_yahoo,
        )
        logger.info(
            "Synced updated stocks DB to local peer: source=%s target=%s confirmed_latest=%s target_before=%s removed_yahoo=%s",
            source,
            target,
            confirmed_latest,
            target_latest,
            removed_yahoo,
        )
        return result.to_dict()
    except Exception as exc:
        logger.warning("Updated DB peer sync failed: source=%s target=%s err=%s", source, target, exc)
        try:
            if temp_path.exists():
                temp_path.unlink()
        except Exception:
            logger.debug("Failed to remove temp peer DB sync file: %s", temp_path, exc_info=True)
        return RuntimePeerDbSyncResult(
            attempted=True,
            synced=False,
            skipped_reason=None,
            source_db_path=str(source),
            target_db_path=str(target),
            confirmed_latest_date=None,
            target_confirmed_latest_date_before=target_latest,
            removed_yahoo_rows=None,
            error=str(exc),
        ).to_dict()


def sync_confirmed_production_db_to_dev(
    *,
    source_db_path: str | Path,
    dev_db_path: str | Path | None = None,
) -> dict[str, object]:
    source = _normalize(source_db_path)
    production = _normalize(production_stock_db_path())
    dev = _normalize(dev_db_path or development_stock_db_path())

    if not _env_bool("MEEMEE_SYNC_PROD_DB_TO_DEV", True):
        return DevDbSyncResult(False, False, "disabled_by_env", str(source), str(dev), None, None).to_dict()
    if not source.exists():
        return DevDbSyncResult(False, False, "source_missing", str(source), str(dev), None, None).to_dict()
    if not _same_path(source, production):
        return DevDbSyncResult(False, False, "source_is_not_production_db", str(source), str(dev), None, None).to_dict()
    if _same_path(source, dev):
        return DevDbSyncResult(False, False, "source_equals_dev_db", str(source), str(dev), None, None).to_dict()

    source_latest = _read_latest_confirmed_date(source)
    dev_latest = _read_latest_confirmed_date(dev)
    if source_latest is None:
        return DevDbSyncResult(False, False, "source_confirmed_date_missing", str(source), str(dev), None, None).to_dict()
    if dev_latest is not None and int(dev_latest) >= int(source_latest):
        return DevDbSyncResult(False, False, "dev_not_older", str(source), str(dev), source_latest, None).to_dict()

    dev.parent.mkdir(parents=True, exist_ok=True)
    temp_path = dev.with_name(f"{dev.name}.tmp-{os.getpid()}-{uuid.uuid4().hex}")
    try:
        shutil.copy2(source, temp_path)
        confirmed_latest, removed_yahoo = _sanitize_confirmed_only(temp_path)
        os.replace(temp_path, dev)
        result = DevDbSyncResult(
            attempted=True,
            synced=True,
            skipped_reason=None,
            source_db_path=str(source),
            dev_db_path=str(dev),
            confirmed_latest_date=confirmed_latest,
            removed_yahoo_rows=removed_yahoo,
        )
        logger.info(
            "Synced production stocks DB to dev: source=%s dev=%s confirmed_latest=%s removed_yahoo=%s",
            source,
            dev,
            confirmed_latest,
            removed_yahoo,
        )
        return result.to_dict()
    except Exception as exc:
        logger.warning("Production-to-dev DB sync failed: source=%s dev=%s err=%s", source, dev, exc)
        try:
            if temp_path.exists():
                temp_path.unlink()
        except Exception:
            logger.debug("Failed to remove temp dev DB sync file: %s", temp_path, exc_info=True)
        return DevDbSyncResult(
            attempted=True,
            synced=False,
            skipped_reason=None,
            source_db_path=str(source),
            dev_db_path=str(dev),
            confirmed_latest_date=None,
            removed_yahoo_rows=None,
            error=str(exc),
        ).to_dict()


def record_dev_db_sync_state(state: dict, result: dict[str, object]) -> None:
    state["last_dev_db_sync_at"] = datetime.now().isoformat()
    state["last_dev_db_sync_result"] = result
