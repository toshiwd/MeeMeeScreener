import os
import sys
from datetime import datetime
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


def test_txt_update_progress_streaming():
    snapshots: list[dict] = []

    def fake_run_streaming_command(cmd, timeout, on_line):
        lines = [
            "START: 1111",
            "OK   : 1111 : +1",
            "START: 2222",
            "ERROR: 2222 : prices.Read failed",
            "START: 3333",
            "OK   : 3333 : +2"
        ]
        for line in lines:
            on_line(line)
            snapshots.append(main._get_update_status_snapshot())
        output = "\n".join(lines + ["SUMMARY: total=3 ok=2 err=1 split=0"])
        return 0, output, False

    def fake_run_ingest_command():
        return 0, "ingest ok"

    main._set_update_status(
        running=True,
        phase="running",
        started_at=datetime.now().isoformat(),
        finished_at=None,
        processed=0,
        total=3,
        summary={},
        error=None,
        stdout_tail=[],
        status_message=None
    )

    with patch.object(main, "_run_streaming_command", side_effect=fake_run_streaming_command), \
         patch.object(main, "_run_ingest_command", side_effect=fake_run_ingest_command), \
         patch.object(main, "_load_update_state", return_value={}), \
         patch.object(main, "_save_update_state", return_value=None):
        main._run_txt_update_job("dummy_code.txt", "dummy_out")

    assert any(snapshot.get("processed") == 1 for snapshot in snapshots)
    assert any(snapshot.get("processed") == 2 for snapshot in snapshots)

    status = main._get_update_status_snapshot()
    assert status["phase"] == "done"
    assert status["running"] is False
    assert status["processed"] == 3
    assert status["summary"]["ok"] == 2
