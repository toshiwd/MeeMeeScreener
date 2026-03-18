import logging
import os
import datetime
from app.backend.infra.panrolling.client import PanRollingClient
from app.backend.infra.files.config_repo import ConfigRepository
from app.backend.core.legacy_analysis_control import is_legacy_analysis_disabled
# We will reuse the existing ingest logic for now as it's complex to refactor in one go
# Assuming ingest_txt is still available or moved. 
# For this step, we keep the import but might need to adjust path if we moved it.
# The user request said "logic/I/O... to infra". ingest_txt does DB I/O.
# Ideally ingest_txt logic should be here or in infra. 
# Let's assume we import the legacy module for now to maintain functionality.
logger = logging.getLogger(__name__)
try:
    from app.backend import ingest_txt
except Exception as exc:  # pragma: no cover - legacy path compatibility
    ingest_txt = None  # type: ignore
    logger.warning("ingest_txt import failed: %s", exc)

def _run_phase_batch_latest() -> int:
    try:
        from app.backend.db import get_conn
    except ModuleNotFoundError:
        from db import get_conn  # type: ignore
    try:
        from app.backend.jobs.phase_batch import run_batch
    except ModuleNotFoundError:
        from jobs.phase_batch import run_batch  # type: ignore

    with get_conn() as conn:
        row = conn.execute("SELECT MAX(dt) FROM feature_snapshot_daily").fetchone()
    if not row or row[0] is None:
        raise RuntimeError("feature_snapshot_daily is empty")
    max_dt = int(row[0])
    run_batch(max_dt, max_dt, dry_run=False)
    return max_dt


def run_txt_update_workflow(
    config_repo: ConfigRepository,
    pan_client: PanRollingClient,
    code_txt_path: str, # Passed from job runner or config
    out_txt_dir: str
):
    logger.info("Starting TXT Update Workflow")
    
    # 1. Export from Pan Rolling
    # We should ensure code_txt_path exists
    if not os.path.exists(code_txt_path):
        logger.error(f"code.txt not found at {code_txt_path}")
        raise FileNotFoundError(f"code.txt not found: {code_txt_path}")

    logger.info(f"Running VBS Export: {code_txt_path} -> {out_txt_dir}")
    exit_code = pan_client.run_export(code_txt_path, out_txt_dir)
    if exit_code != 0:
        raise RuntimeError(f"VBS Export failed with exit code {exit_code}")

    # 2. Ingest TXT files to Database
    # This part relies on ingest_txt logic. 
    # In Clean Arch, this should call a UseCase or Domain Service.
    # For now, we wrap the legacy ingest call.
    logger.info("Starting Ingest...")
    if ingest_txt is None:
        raise RuntimeError("ingest_txt module is unavailable")
    try:
        # We need to ensure environment variables are set for legacy ingest if it relies on them
        # Or better, pass arguments if we refactored ingest_txt properly.
        # Existing ingest_txt uses os.environ or config module.
        # Let's assume we can call a function `ingest(incremental=True)`.
        ingest_txt.ingest(incremental=True)
    except Exception as e:
        logger.error(f"Ingest failed: {e}")
        raise

    # 3. Update Phase predictions
    if is_legacy_analysis_disabled():
        logger.info("Skipping legacy phase batch because external analysis is active.")
    else:
        logger.info("Starting Phase batch...")
        phase_dt = _run_phase_batch_latest()
        logger.info("Phase batch completed (dt=%s)", phase_dt)
    
    # 4. Update State
    state = config_repo.load_update_state()
    now = datetime.datetime.now()
    state["last_txt_update_at"] = now.isoformat()
    state["last_txt_update_date"] = now.date().isoformat()
    config_repo.save_update_state(state)
    
    logger.info("TXT Update Workflow Completed Successfully")
