
import os
import logging
from pathlib import Path
try:
    from app.backend.import_positions import process_import_rakuten, process_import_sbi
    from app.db.session import get_conn
    from app.core.config import config
except ModuleNotFoundError:  # pragma: no cover - legacy tooling may import from app/backend on sys.path
    from import_positions import process_import_rakuten, process_import_sbi  # type: ignore
    from db import get_conn  # type: ignore
    from core.config import config  # type: ignore

logger = logging.getLogger(__name__)

def resolve_trade_csv_paths() -> list[str]:
    paths = []
    
    def _scan_dir(base: Path) -> None:
        if not base or not base.is_dir():
            return
        for filename in ("楽天証券取引履歴.csv", "SBI証券取引履歴.csv"):
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
                if "取引履歴" not in entry.name and "trade" not in name:
                    continue
                if any(key in entry.name for key in ("楽天", "ＳＢＩ", "SBI")) or any(key in name for key in ("rakuten", "sbi")):
                    paths.append(str(entry))
        except OSError:
            return

    _scan_dir(config.DATA_DIR)
    _scan_dir(config.DATA_DIR / "csv")

    trade_csv_dir = os.getenv("TRADE_CSV_DIR")
    if trade_csv_dir:
        try:
            _scan_dir(Path(trade_csv_dir))
        except Exception:
            pass

    # Env vars (legacy support)
    env = os.getenv("TRADE_CSV_PATH")
    if env:
        parts = [p.strip() for p in env.split(";") if p.strip()]
        paths.extend([os.path.abspath(part) for part in parts])

    # Dedup
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
            conn.execute("DROP TABLE IF EXISTS trade_events_bak")
            conn.execute("CREATE TABLE trade_events_bak AS SELECT * FROM trade_events")
            count_before = conn.execute("SELECT COUNT(*) FROM trade_events_bak").fetchone()[0]
            logger.info(f"Backed up {count_before} rows to trade_events_bak")

            # 2. Clear table
            conn.execute("DELETE FROM trade_events")

            # 3. Import
            full_success = True
            total_imported = 0

            for path in paths:
                try:
                    with open(path, "rb") as f:
                        content = f.read()

                    # Try Rakuten
                    try:
                        res = process_import_rakuten(content, replace_existing=False)
                        imported_count = res["inserted"]
                        total_imported += imported_count
                        results["details"].append(f"Rakuten: {os.path.basename(path)} (+{imported_count})")
                        continue
                    except Exception:
                        pass

                    # Try SBI
                    try:
                        res = process_import_sbi(content, replace_existing=False)
                        imported_count = res["inserted"]
                        total_imported += imported_count
                        results["details"].append(f"SBI: {os.path.basename(path)} (+{imported_count})")
                        continue
                    except Exception:
                        pass

                    # Failed both
                    logger.warning(f"Failed to parse {path}")
                    results["details"].append(f"SKIP: {os.path.basename(path)} (Parse Failed)")
                    full_success = False

                except Exception as e:
                    logger.error(f"Error reading {path}: {e}")
                    results["details"].append(f"ERROR: {os.path.basename(path)}")
                    full_success = False

            # 4. Verification / Rollback if disastrous (e.g. 0 rows imported when we had data)
            if total_imported == 0 and count_before > 0:
                logger.warning("Force sync resulted in 0 rows but backup had data. Rolling back.")
                conn.execute("DELETE FROM trade_events")
                conn.execute("INSERT INTO trade_events SELECT * FROM trade_events_bak")
                results["warnings"].append("Rolled back: No trades found in new files.")
                results["imported"] = count_before  # Restored count
            else:
                results["imported"] = total_imported

        except Exception as e:
            logger.error(f"Sync failed with DB error: {e}")
            # Try rollback
            try:
                conn.execute("DELETE FROM trade_events")
                conn.execute("INSERT INTO trade_events SELECT * FROM trade_events_bak")
                results["warnings"].append(f"Critical Error: {e}. Rolled back.")
            except Exception:
                results["warnings"].append(f"Critical Error: {e}. Rollback FAILED.")

    return results
