from __future__ import annotations

from datetime import datetime

from external_analysis.runtime import load_control as load_control_module
from external_analysis.runtime.load_control import evaluate_research_load_control, resolve_research_runtime_budget


def test_load_control_defers_outside_heavy_window(monkeypatch) -> None:
    monkeypatch.setattr(load_control_module, "_active_window_snapshot", lambda: (None, None))
    decision = evaluate_research_load_control(now=datetime(2026, 3, 14, 22, 0, 0))
    assert decision.mode == "deferred"
    assert decision.reason == "outside_heavy_window"


def test_load_control_throttles_when_meemee_like_window_is_foreground(monkeypatch) -> None:
    monkeypatch.setattr(load_control_module, "_active_window_snapshot", lambda: ("MeeMee Screener", "python.exe"))
    decision = evaluate_research_load_control(now=datetime(2026, 3, 14, 10, 0, 0))
    assert decision.mode == "throttled"
    assert decision.reason == "meemee_foreground_active"


def test_load_control_runs_full_when_background_window_is_available(monkeypatch) -> None:
    monkeypatch.setattr(load_control_module, "_active_window_snapshot", lambda: ("Notepad", "notepad.exe"))
    decision = evaluate_research_load_control(now=datetime(2026, 3, 14, 10, 0, 0))
    assert decision.mode == "full"
    assert decision.reason == "background_window_available"


def test_load_control_runtime_budget_throttles_candidate_and_similarity_limits() -> None:
    budget = resolve_research_runtime_budget({"mode": "throttled"})
    assert budget["candidate_limit_per_side"] == 8
    assert budget["similarity_top_k"] == 3
