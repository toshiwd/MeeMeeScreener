from __future__ import annotations

import ctypes
from ctypes import wintypes
from dataclasses import dataclass
from datetime import datetime
from typing import Any

HEAVY_WINDOW_START_HOUR = 1
HEAVY_WINDOW_END_HOUR = 19
KNOWN_MEE_MEE_WINDOW_HINTS = ("meemee", "screener", "tradex")
KNOWN_MEE_MEE_PROCESS_HINTS = ("python", "meemee", "launcher", "msedgewebview2")
FULL_CANDIDATE_LIMIT_PER_SIDE = 20
THROTTLED_CANDIDATE_LIMIT_PER_SIDE = 8
FULL_SIMILARITY_TOP_K = 5
THROTTLED_SIMILARITY_TOP_K = 3
FULL_CHALLENGER_QUERY_CASE_LIMIT = 128
THROTTLED_CHALLENGER_QUERY_CASE_LIMIT = 48
FULL_CHALLENGER_CANDIDATE_POOL_LIMIT = 768
THROTTLED_CHALLENGER_CANDIDATE_POOL_LIMIT = 256


@dataclass(frozen=True)
class ResearchLoadDecision:
    mode: str
    reason: str
    active_window_title: str | None
    active_process_name: str | None
    within_heavy_window: bool
    interaction_detected: bool


def _within_heavy_window(now: datetime | None = None) -> bool:
    current = now or datetime.now()
    hour = int(current.hour)
    return HEAVY_WINDOW_START_HOUR <= hour < HEAVY_WINDOW_END_HOUR


def _active_window_snapshot() -> tuple[str | None, str | None]:
    try:
        user32 = ctypes.windll.user32
        kernel32 = ctypes.windll.kernel32
        psapi = ctypes.windll.psapi
        hwnd = user32.GetForegroundWindow()
        if not hwnd:
            return None, None
        length = user32.GetWindowTextLengthW(hwnd)
        title_buffer = ctypes.create_unicode_buffer(max(1, int(length) + 1))
        user32.GetWindowTextW(hwnd, title_buffer, len(title_buffer))
        pid = wintypes.DWORD()
        user32.GetWindowThreadProcessId(hwnd, ctypes.byref(pid))
        handle = kernel32.OpenProcess(0x1000 | 0x0400, False, pid.value)
        process_name = None
        if handle:
            try:
                image_buffer = ctypes.create_unicode_buffer(260)
                if psapi.GetModuleBaseNameW(handle, None, image_buffer, len(image_buffer)) > 0:
                    process_name = str(image_buffer.value)
            finally:
                kernel32.CloseHandle(handle)
        title = str(title_buffer.value) if title_buffer.value else None
        return title, process_name
    except Exception:
        return None, None


def _is_meemee_interaction(title: str | None, process_name: str | None) -> bool:
    title_text = str(title or "").strip().lower()
    process_text = str(process_name or "").strip().lower()
    return any(hint in title_text for hint in KNOWN_MEE_MEE_WINDOW_HINTS) or any(
        hint in process_text for hint in KNOWN_MEE_MEE_PROCESS_HINTS
    )


def evaluate_research_load_control(*, now: datetime | None = None) -> ResearchLoadDecision:
    title, process_name = _active_window_snapshot()
    within_heavy_window = _within_heavy_window(now)
    interaction_detected = _is_meemee_interaction(title, process_name)
    if interaction_detected:
        return ResearchLoadDecision(
            mode="throttled",
            reason="meemee_foreground_active",
            active_window_title=title,
            active_process_name=process_name,
            within_heavy_window=within_heavy_window,
            interaction_detected=True,
        )
    if not within_heavy_window:
        return ResearchLoadDecision(
            mode="deferred",
            reason="outside_heavy_window",
            active_window_title=title,
            active_process_name=process_name,
            within_heavy_window=False,
            interaction_detected=False,
        )
    return ResearchLoadDecision(
        mode="full",
        reason="background_window_available",
        active_window_title=title,
        active_process_name=process_name,
        within_heavy_window=True,
        interaction_detected=False,
    )


def load_decision_payload(decision: ResearchLoadDecision) -> dict[str, Any]:
    return {
        "mode": decision.mode,
        "reason": decision.reason,
        "active_window_title": decision.active_window_title,
        "active_process_name": decision.active_process_name,
        "within_heavy_window": bool(decision.within_heavy_window),
        "interaction_detected": bool(decision.interaction_detected),
    }


def resolve_research_runtime_budget(load_control: dict[str, Any] | None = None) -> dict[str, int]:
    mode = str((load_control or {}).get("mode") or "full").strip().lower()
    if mode == "throttled":
        return {
            "candidate_limit_per_side": THROTTLED_CANDIDATE_LIMIT_PER_SIDE,
            "similarity_top_k": THROTTLED_SIMILARITY_TOP_K,
            "challenger_query_case_limit": THROTTLED_CHALLENGER_QUERY_CASE_LIMIT,
            "challenger_candidate_pool_limit": THROTTLED_CHALLENGER_CANDIDATE_POOL_LIMIT,
        }
    return {
        "candidate_limit_per_side": FULL_CANDIDATE_LIMIT_PER_SIDE,
        "similarity_top_k": FULL_SIMILARITY_TOP_K,
        "challenger_query_case_limit": FULL_CHALLENGER_QUERY_CASE_LIMIT,
        "challenger_candidate_pool_limit": FULL_CHALLENGER_CANDIDATE_POOL_LIMIT,
    }
