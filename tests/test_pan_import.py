from __future__ import annotations

from unittest.mock import patch

import pytest

from app.backend.infra.panrolling import pan_import


class _FakeWindow:
    def __init__(self, handle: int, *, visible: bool = True) -> None:
        self.handle = handle
        self._visible = visible

    def is_visible(self) -> bool:
        return self._visible


class _FakeApp:
    def __init__(self, windows: list[object]) -> None:
        self._windows = windows

    def windows(self) -> list[object]:
        return self._windows


def test_run_pan_import_fails_fast_when_pandtmgr_already_running() -> None:
    with (
        patch("app.backend.infra.panrolling.pan_import.os.path.isfile", return_value=True),
        patch("app.backend.infra.panrolling.pan_import._list_running_pandtmgr_pids", return_value=[111, 222]),
    ):
        with pytest.raises(pan_import.PanImportError, match="already running"):
            pan_import.run_pan_import(r"C:\Program Files (x86)\Pan\pandtmgr.exe")


def test_is_blocking_error_dialog_detects_libstock_message() -> None:
    with patch(
        "app.backend.infra.panrolling.pan_import._collect_dialog_texts",
        return_value=["Error", "libStock database is in use."],
    ):
        assert pan_import._is_blocking_error_dialog(object()) is True


def test_wait_for_import_completion_raises_on_blocking_error_dialog() -> None:
    main = _FakeWindow(1)
    blocking = _FakeWindow(2)
    app = _FakeApp([main, blocking])

    with (
        patch("app.backend.infra.panrolling.pan_import.time.sleep", return_value=None),
        patch("app.backend.infra.panrolling.pan_import._dismiss_embedded_completion_dialog", return_value=False),
        patch(
            "app.backend.infra.panrolling.pan_import._collect_dialog_texts",
            side_effect=[["libStock database is in use."], ["libStock database is in use."]],
        ),
        patch("app.backend.infra.panrolling.pan_import._dismiss_error_dialog", return_value=True) as mock_dismiss,
    ):
        with pytest.raises(pan_import.PanImportError, match="libStock database is in use"):
            pan_import._wait_for_import_completion(app, main, timeout=5)

    mock_dismiss.assert_called_once_with(blocking)
