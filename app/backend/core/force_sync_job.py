
import os
import logging
from datetime import datetime

from .config import config
from .jobs import job_manager
from .code_ops import normalize_code_txt
from .txt_update_job import run_vbs_update, run_ingest

logger = logging.getLogger(__name__)

PAN_CODE_TXT_PATH = config.PAN_CODE_TXT_PATH
PAN_OUT_TXT_DIR = config.PAN_OUT_TXT_DIR

def handle_force_sync(job_id: str, payload: dict):
    """
    Job Handler for 'force_sync'.
    Steps:
    1. Normalize code.txt (sort/dedup)
    2. Run VBS Export (overwrite)
    3. Run Ingest (Full Mode - wipe and reload)
    """
    logger.info(f"Starting force_sync job {job_id}")
    job_manager._update_db(job_id, "force_sync", "running", message="Normalizing code.txt...", progress=5)

    try:
        if os.path.exists(PAN_CODE_TXT_PATH):
            changed = normalize_code_txt(PAN_CODE_TXT_PATH)
            msg = "code.txt normalized (updated)" if changed else "code.txt validated (no changes)"
            logger.info(msg)
            job_manager._update_db(job_id, "force_sync", "running", message=msg, progress=10)
        else:
            logger.warning("code.txt not found, skipping normalization")

        # Run VBS
        job_manager._update_db(job_id, "force_sync", "running", message="Running VBS Export...", progress=20)
        vbs_code, vbs_output = run_vbs_update(job_id, PAN_CODE_TXT_PATH, PAN_OUT_TXT_DIR)
        
        if vbs_code != 0:
            job_manager._update_db(job_id, "force_sync", "failed", error=f"VBS Failed: {vbs_code}", message="VBS Error")
            return

        # Run Ingest (Full)
        job_manager._update_db(job_id, "force_sync", "running", message="Ingesting (Full Mode)...", progress=60)
        ingest_out, ingest_err, stats = run_ingest(incremental=False)

        if ingest_err:
            job_manager._update_db(job_id, "force_sync", "failed", error="Ingest Failed", message=f"Ingest Error: {ingest_err}")
            return

        # CSV Sync
        from .csv_sync import sync_trade_csvs
        job_manager._update_db(job_id, "force_sync", "running", message="Syncing Trade CSVs...", progress=80)
        csv_res = sync_trade_csvs()
        
        # Build detailed CSV message
        csv_msg = f"Trades: {csv_res.get('imported')} rows."
        if csv_res.get('details'):
            # Only show first 3 details or summary
            details = csv_res.get('details')[:3]
            csv_msg += " [" + ", ".join(details) + "]"
            if len(csv_res.get('details')) > 3:
                csv_msg += "..."
        
        if csv_res.get('warnings'):
             csv_msg += " WARN: " + "; ".join(csv_res.get('warnings'))
            
        job_manager._update_db(job_id, "force_sync", "success", progress=100, message=f"Complete. {csv_msg}", finished_at=datetime.now())

    except Exception as e:
        logger.error(f"Force sync failed: {e}")
        job_manager._update_db(job_id, "force_sync", "failed", error=str(e), message="Internal Error", finished_at=datetime.now())
