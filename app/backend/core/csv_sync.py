
import os
import logging
from pathlib import Path
try:
    from app.backend.import_positions import process_import_rakuten, process_import_sbi
    from app.backend.infra.files.trade_repo import TradeRepository
    from app.db.session import get_conn
    from app.core.config import config
except ModuleNotFoundError:  # pragma: no cover - legacy tooling may import from app/backend on sys.path
    from import_positions import process_import_rakuten, process_import_sbi  # type: ignore
    from backend.infra.files.trade_repo import TradeRepository  # type: ignore
    from db import get_conn  # type: ignore
    from core.config import config  # type: ignore

logger = logging.getLogger(__name__)
_TRADE_EVENTS_BACKUP_TABLE = "trade_events_backup_sync"

def resolve_trade_csv_paths() -> list[str]:
    paths = []
    preferred_names = (
        "rakuten_trade_history.csv",
        "sbi_trade_history.csv",
        "楽天証券取引履歴.csv",
        "SBI証券取引履歴.csv",
    )
    legacy_names = (
        "????????.csv",
        "SBI??????.csv",
        "????????????????.csv",
        "SBI????????????.csv",
    )

    def _scan_dir(base: Path) -> None:
        if not base or not base.is_dir():
            return
        for filename in (*preferred_names, *legacy_names):
            candidate = base / filename
            if candidate.exists():
                paths.append(str(candidate))
        try:
            for entry in base.iterdir():
                if not entry.is_file():
                    continue
                if entry.suffix.lower() != ".csv":
                    continue
                name = entry.name.lower()
                if "????????" not in entry.name and "trade" not in name:
                    continue
                if any(key in entry.name for key in ("????", "??????", "SBI")) or any(key in name for key in ("rakuten", "sbi")):
                    paths.append(str(entry))
        except OSError:
            return

    _scan_dir(config.DATA_DIR)
    _scan_dir(config.DATA_DIR / "csv")

    trade_csv_dir = os.getenv("TRADE_CSV_DIR")
    if trade_csv_dir:
        try:
            _scan_dir(Path(trade_csv_dir))
        except Exception as exc:
            logger.warning("Failed to scan TRADE_CSV_DIR (%s): %s", trade_csv_dir, exc)

    env = os.getenv("TRADE_CSV_PATH")
    if env:
        parts = [p.strip() for p in env.split(";") if p.strip()]
        paths.extend([os.path.abspath(part) for part in parts])

    return list(set(paths))

def sync_trade_csvs() -> dict:
    paths = resolve_trade_csv_paths()
    results = {
        "found": len(paths),
        "imported": 0,
        "details": [],
        "warnings": []
    }
    
    if not paths:
        results["warnings"].append("No valid CSV files found in data directory.")
        return results

    with get_conn() as conn:
        try:
            # 1. Backup existing data
            conn.execute(f"DROP TABLE IF EXISTS temp.{_TRADE_EVENTS_BACKUP_TABLE}")
            conn.execute(f"CREATE TEMP TABLE {_TRADE_EVENTS_BACKUP_TABLE} AS SELECT * FROM trade_events")
            count_before = conn.execute(f"SELECT COUNT(*) FROM temp.{_TRADE_EVENTS_BACKUP_TABLE}").fetchone()[0]
            logger.info("Backed up %s rows to temp.%s", count_before, _TRADE_EVENTS_BACKUP_TABLE)

            # 2. Clear table
            conn.execute("DELETE FROM trade_events")

            # 3. Import
            full_success = True
            total_imported = 0

            for path in paths:
                try:
                    with open(path, "rb") as f:
                        content = f.read()

                    basename = os.path.basename(path)
                    detected_broker, detected_by = TradeRepository.detect_broker_from_bytes(content, basename)
                    if detected_broker in ("rakuten", "sbi"):
                        order = [detected_broker, "sbi" if detected_broker == "rakuten" else "rakuten"]
                    else:
                        order = ["rakuten", "sbi"]

                    parsed = False
                    for broker in order:
                        try:
                            if broker == "rakuten":
                                res = process_import_rakuten(content, replace_existing=False)
                            else:
                                res = process_import_sbi(content, replace_existing=False)
                            imported_count = int(res.get("inserted", 0) or 0)
                            received_count = int(res.get("received", 0) or 0)
                            if imported_count <= 0 and received_count <= 0:
                                continue
                            total_imported += imported_count
                            broker_label = "SBI" if broker == "sbi" else "Rakuten"
                            results["details"].append(
                                f"{broker_label}: {basename} (+{imported_count})"
                            )
                            parsed = True
                            break
                        except Exception:
                            continue

                    if parsed:
                        continue

                    # Failed both
                    logger.warning(f"Failed to parse {path}")
                    results["details"].append(
                        f"SKIP: {basename} (Parse Failed)"
                    )
                    full_success = False

                except Exception as e:
                    logger.error(f"Error reading {path}: {e}")
                    results["details"].append(f"ERROR: {os.path.basename(path)}")
                    full_success = False

            # 4. Verification / Rollback if disastrous (e.g. 0 rows imported when we had data)
            if total_imported == 0 and count_before > 0:
                logger.warning("Force sync resulted in 0 rows but backup had data. Rolling back.")
                conn.execute("DELETE FROM trade_events")
                conn.execute(f"INSERT INTO trade_events SELECT * FROM temp.{_TRADE_EVENTS_BACKUP_TABLE}")
                results["warnings"].append("Rolled back: No trades found in new files.")
                results["imported"] = count_before  # Restored count
            else:
                results["imported"] = total_imported

        except Exception as e:
            logger.error(f"Sync failed with DB error: {e}")
            # Try rollback
            try:
                conn.execute("DELETE FROM trade_events")
                conn.execute(f"INSERT INTO trade_events SELECT * FROM temp.{_TRADE_EVENTS_BACKUP_TABLE}")
                results["warnings"].append(f"Critical Error: {e}. Rolled back.")
            except Exception:
                results["warnings"].append(f"Critical Error: {e}. Rollback FAILED.")
        finally:
            try:
                conn.execute(f"DROP TABLE IF EXISTS temp.{_TRADE_EVENTS_BACKUP_TABLE}")
            except Exception:
                pass

    return results
