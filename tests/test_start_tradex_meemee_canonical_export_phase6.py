from __future__ import annotations

from pathlib import Path


def test_phase6_launcher_records_hidden_background_monitoring_contract() -> None:
    text = (Path(__file__).resolve().parents[1] / "tools" / "start_tradex_meemee_canonical_export_phase6.ps1").read_text(encoding="utf-8")
    assert "-WindowStyle Hidden" in text
    assert "-RedirectStandardOutput $stdoutPath" in text
    assert "-RedirectStandardError $stderrPath" in text
    assert "phase6_latest_launch.json" in text
    assert "monitor_progress_glob" in text
    assert "phase5_run_progress.json" in text
    assert "runtime DB write" in text
