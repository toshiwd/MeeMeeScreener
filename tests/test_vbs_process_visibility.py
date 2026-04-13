from __future__ import annotations

import subprocess

from app.backend.core import force_sync_job, txt_update_job


class _ReadyStdout:
    def __init__(self, lines: list[str]) -> None:
        self._lines = list(lines)
        self._index = 0

    def __iter__(self):
        return self

    def __next__(self) -> str:
        if self._index >= len(self._lines):
            raise StopIteration
        line = self._lines[self._index]
        self._index += 1
        return line


class _ReadyProcess:
    def __init__(self, lines: list[str], return_code: int = 0) -> None:
        self.stdout = _ReadyStdout(lines)
        self._return_code = return_code
        self.killed = False

    def poll(self):
        return -1 if self.killed else self._return_code

    def kill(self) -> None:
        self.killed = True

    def wait(self, timeout=None):
        return -1 if self.killed else self._return_code


def test_force_sync_vbs_launches_hidden_window(monkeypatch) -> None:
    captured: dict[str, object] = {}
    process = _ReadyProcess(["SUMMARY: ok=1\n"], return_code=0)

    def _popen(*args, **kwargs):  # noqa: ANN001
        captured.update(kwargs)
        return process

    monkeypatch.setattr(force_sync_job.subprocess, "Popen", _popen)

    code, output = force_sync_job._run_vbs_export("code.txt", "out", timeout=1)

    assert code == 0
    assert output[-1] == "[force_sync_job] VBS exit code 0"
    assert captured["creationflags"] == getattr(subprocess, "CREATE_NO_WINDOW", 0)
    if hasattr(subprocess, "STARTUPINFO"):
        assert "startupinfo" in captured
        startupinfo = captured["startupinfo"]
        assert startupinfo.wShowWindow == getattr(subprocess, "SW_HIDE", 0)


def test_txt_update_vbs_launches_hidden_window(monkeypatch) -> None:
    captured: dict[str, object] = {}
    process = _ReadyProcess(["SUMMARY: ok=1\n"], return_code=0)

    def _popen(*args, **kwargs):  # noqa: ANN001
        captured.update(kwargs)
        return process

    monkeypatch.setattr(txt_update_job.subprocess, "Popen", _popen)

    code, output = txt_update_job.run_vbs_export("code.txt", "out", timeout=1)

    assert code == 0
    assert any("SUMMARY" in line for line in output)
    assert captured["creationflags"] == getattr(subprocess, "CREATE_NO_WINDOW", 0)
    if hasattr(subprocess, "STARTUPINFO"):
        assert "startupinfo" in captured
        startupinfo = captured["startupinfo"]
        assert startupinfo.wShowWindow == getattr(subprocess, "SW_HIDE", 0)
