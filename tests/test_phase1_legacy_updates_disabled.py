from __future__ import annotations

from types import SimpleNamespace

from app.backend.jobs import txt_update as txt_update_module


class _DummyPanClient:
    def run_export(self, code_txt_path: str, out_txt_dir: str) -> int:
        return 0


class _DummyConfigRepo:
    def __init__(self) -> None:
        self.saved_state: dict | None = None

    def load_update_state(self) -> dict:
        return {}

    def save_update_state(self, state: dict) -> None:
        self.saved_state = dict(state)


def test_legacy_txt_update_workflow_skips_phase_batch_when_disabled(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("MEEMEE_DISABLE_LEGACY_ANALYSIS", "1")
    code_txt = tmp_path / "code.txt"
    out_dir = tmp_path / "txt"
    code_txt.write_text("1301\n", encoding="utf-8")
    out_dir.mkdir()

    called = {"ingest": 0, "phase": 0}
    monkeypatch.setattr(
        txt_update_module,
        "ingest_txt",
        SimpleNamespace(ingest=lambda incremental=True: called.__setitem__("ingest", called["ingest"] + 1)),
    )
    monkeypatch.setattr(
        txt_update_module,
        "_run_phase_batch_latest",
        lambda: called.__setitem__("phase", called["phase"] + 1),
    )

    repo = _DummyConfigRepo()
    txt_update_module.run_txt_update_workflow(repo, _DummyPanClient(), str(code_txt), str(out_dir))

    assert called["ingest"] == 1
    assert called["phase"] == 0
    assert repo.saved_state is not None
    assert "last_txt_update_at" in repo.saved_state

