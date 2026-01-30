import os
import sys
from pathlib import Path
from unittest.mock import PropertyMock, patch

BACKEND_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "app", "backend"))
sys.path.append(BACKEND_DIR)

with patch("core.config.config.DATA_DIR", new=Path(".")), \
     patch("core.config.AppConfig.DB_PATH", new_callable=PropertyMock) as mock_db_path, \
     patch("core.config.AppConfig.PAN_CODE_TXT_PATH", new_callable=PropertyMock) as mock_code_path, \
     patch("core.config.AppConfig.PAN_EXPORT_VBS_PATH", new_callable=PropertyMock) as mock_vbs_path:
    mock_db_path.return_value = ":memory:"
    mock_code_path.return_value = Path("dummy_code.txt")
    mock_vbs_path.return_value = Path("dummy_vbs.vbs")
    import main


def test_txt_update_simple_flow():
    with patch.object(main, "run_vbs_export", return_value=(0, ["SUMMARY: total=2 ok=2 err=0 split=0"])), \
         patch.object(main, "_load_update_state", return_value={}), \
         patch.object(main, "_save_update_state") as mock_save:
        main._run_txt_update_job("dummy_code.txt", "dummy_out")

    mock_save.assert_called_once()
    saved_state = mock_save.call_args[0][0]
    assert "last_txt_update_at" in saved_state
    assert "last_txt_update_date" in saved_state
