from __future__ import annotations

import threading
import time

from app.backend.core import force_sync_job


class _SilentStdout:
    def __init__(self, process) -> None:
        self._process = process

    def __iter__(self):
        return self

    def __next__(self) -> str:
        while not self._process.killed:
            time.sleep(0.01)
        raise StopIteration


class _SilentProcess:
    def __init__(self) -> None:
        self.killed = False
        self.stdout = _SilentStdout(self)

    def poll(self):
        return -1 if self.killed else None

    def kill(self) -> None:
        self.killed = True

    def wait(self, timeout=None):
        return -1 if self.killed else 0


class _BufferedStdout:
    def __init__(self, lines: list[str]) -> None:
        self._lines = list(lines)
        self._index = 0
        self._lock = threading.Lock()

    def __iter__(self):
        return self

    def __next__(self) -> str:
        with self._lock:
            if self._index >= len(self._lines):
                raise StopIteration
            line = self._lines[self._index]
            self._index += 1
            return line

    @property
    def done(self) -> bool:
        with self._lock:
            return self._index >= len(self._lines)


class _BufferedProcess:
    def __init__(self, lines: list[str], return_code: int = 0) -> None:
        self.stdout = _BufferedStdout(lines)
        self.killed = False
        self._return_code = return_code

    def poll(self):
        if self.killed:
            return -1
        return self._return_code if self.stdout.done else None

    def kill(self) -> None:
        self.killed = True

    def wait(self, timeout=None):
        return -1 if self.killed else self._return_code


def test_run_vbs_export_times_out_when_process_goes_silent(monkeypatch) -> None:
    process = _SilentProcess()
    monkeypatch.setattr(force_sync_job.subprocess, "Popen", lambda *args, **kwargs: process)

    code, output = force_sync_job._run_vbs_export("code.txt", "out", timeout=0.05)

    assert code == -1
    assert process.killed is True
    assert "Timeout expired" in output


def test_run_vbs_export_collects_stdout_and_exit_code(monkeypatch) -> None:
    process = _BufferedProcess(["START: 1001\n", "OK   : 1001 : +10\n"], return_code=0)
    monkeypatch.setattr(force_sync_job.subprocess, "Popen", lambda *args, **kwargs: process)

    code, output = force_sync_job._run_vbs_export("code.txt", "out", timeout=1)

    assert code == 0
    assert output[-1] == "[force_sync_job] VBS exit code 0"
    assert output[:-1] == ["START: 1001", "OK   : 1001 : +10"]
