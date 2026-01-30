from fastapi import APIRouter, Depends, HTTPException, BackgroundTasks
from app.backend.api.dependencies import get_config_repo
from app.backend.infra.files.config_repo import ConfigRepository
from app.backend.jobs.txt_update import run_txt_update_workflow
from app.backend.infra.panrolling.client import PanRollingClient
import os

router = APIRouter(prefix="/api/system", tags=["system"])

@router.post("/update_data")
def trigger_update_data(
    background_tasks: BackgroundTasks,
    config: ConfigRepository = Depends(get_config_repo)
):
    """
    Triggers the VBS export and TXT ingest process.
    """
    # In a real app, we would inject the PanRollingClient and paths properly.
    # For now, we construct them here or get from config.
    
    # We need to know where the VBS is and where the code.txt is.
    # Legacy: `release/MeeMeeScreener/tools/PanRollingExport.vbs`
    # We should probably define these in ConfigRepo or env.
    
    # Resolve VBS path
    # In bundled EXE, it might be in _internal/tools or similar.
    # In dev, it's in tools/
    base_dir = os.path.dirname(os.path.abspath(__file__)) # routers/
    # We need to go up to root
    project_root = os.getcwd() # Assuming CWD is set correctly in main
    
    # Try multiple locations
    candidates = [
        os.path.join(project_root, "tools", "export_pan.vbs"),
        os.path.join(project_root, "_internal", "tools", "export_pan.vbs"), # PyInstaller default
        os.path.join(sys.prefix, "tools", "export_pan.vbs"),
    ]
    
    vbs_path = None
    for p in candidates:
        if os.path.exists(p):
            vbs_path = p
            break
            
    if not vbs_path:
        # Fallback for dev if CWD is wrong
        vbs_path = os.path.abspath("tools/export_pan.vbs")

    # Code txt path
    code_txt_path = os.path.join(project_root, "data", "code.txt")
    out_dir = os.path.join(project_root, "data", "txt_dump")
    
    # We need stock repo for scoring
    # In a real app config/dependencies should provide this context
    # We'll rely on the global init in dependencies for now or instantiate
    from app.backend.api.dependencies import get_stock_repo
    
    def _run_job():
        try:
            # 1. Update Data
            run_txt_update_workflow(config, client, code_txt_path, out_dir)
            
            # 2. Run Scoring
            repo = get_stock_repo()
            from app.backend.jobs.scoring_job import ScoringJob
            job = ScoringJob(repo)
            job.run()
            
        except Exception as e:
            print(f"Background Update Failed: {e}")

    background_tasks.add_task(_run_job)
    return {"status": "accepted", "message": "Update job started"}

@router.get("/status")
def get_system_status(config: ConfigRepository = Depends(get_config_repo)):
    state = config.load_update_state()
    return {
        "last_update": state.get("last_txt_update_at"),
        "version": "2.0.0-clean-arch"
    }
