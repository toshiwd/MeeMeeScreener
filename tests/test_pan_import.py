import os
import sys
from unittest.mock import patch

ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if ROOT_DIR not in sys.path:
    sys.path.append(ROOT_DIR)

from app.backend.infra.panrolling import pan_import


def test_wait_for_no_running_pandtmgr_returns_when_process_exits():
    seen = [[1200], [1200], []]

    def _list_pids():
        return seen.pop(0) if seen else []

    with (
        patch("app.backend.infra.panrolling.pan_import._list_running_pandtmgr_pids", side_effect=_list_pids),
        patch("app.backend.infra.panrolling.pan_import.time.sleep") as mock_sleep,
    ):
        remaining = pan_import._wait_for_no_running_pandtmgr(2.0)

    assert remaining == []
    assert mock_sleep.call_count == 2


def test_wait_for_no_running_pandtmgr_times_out_with_lingering_pids():
    monotonic_values = [0.0, 0.1, 0.4, 0.7]
    with (
        patch("app.backend.infra.panrolling.pan_import._list_running_pandtmgr_pids", return_value=[4321]),
        patch("app.backend.infra.panrolling.pan_import.time.sleep"),
        patch(
            "app.backend.infra.panrolling.pan_import.time.monotonic",
            side_effect=lambda: monotonic_values.pop(0) if monotonic_values else 1.0,
        ),
    ):
        remaining = pan_import._wait_for_no_running_pandtmgr(0.5)

    assert remaining == [4321]
