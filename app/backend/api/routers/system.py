import logging
import os
from pathlib import Path

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel

from app.backend.api.dependencies import get_config_repo
from app.backend.core import config as backend_config
from app.backend.core.config import write_data_dir_override
from app.backend.api.routers.jobs import submit_txt_update_job
from app.backend.infra.files.config_repo import ConfigRepository

router = APIRouter(prefix="/api/system", tags=["system"])
logger = logging.getLogger(__name__)


class DataDirPayload(BaseModel):
    dataDir: str


@router.post("/update_data")
def trigger_update_data():
    return submit_txt_update_job(
        {},
        source="/api/system/update_data",
        legacy_endpoint="/api/system/update_data",
    )


@router.get("/data-dir")
def get_data_dir():
    current = backend_config.config.DATA_DIR
    return {
        "dataDir": str(current),
        "source": "env" if os.getenv("MEEMEE_DATA_DIR") else "config"
    }


@router.post("/data-dir")
def set_data_dir(payload: DataDirPayload):
    target = Path(payload.dataDir).expanduser().resolve()
    if not target:
        raise HTTPException(status_code=400, detail="dataDir is required")
    config_path = write_data_dir_override(target)
    os.environ["MEEMEE_DATA_DIR"] = str(target)
    return {
        "dataDir": str(target),
        "configPath": str(config_path),
        "restartRequired": True,
        "message": "Data directory override saved; restart the app for changes to fully apply."
    }

@router.get("/status")
def get_system_status(config: ConfigRepository = Depends(get_config_repo)):
    state = config.load_update_state()
    return {
        "last_update": state.get("last_txt_update_at"),
        "version": "2.0.0-clean-arch"
    }
