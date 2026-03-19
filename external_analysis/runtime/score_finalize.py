from __future__ import annotations

from typing import Any


def build_tradex_score_reasons(core: dict[str, Any]) -> tuple[str, ...]:
    pattern_value = core.get("pattern_label")
    if pattern_value is None:
        pattern_value = core.get("patternLabel")
    environment_value = core.get("environment_label")
    if environment_value is None:
        environment_value = core.get("environmentLabel")
    return tuple(
        reason
        for reason in (
            f"tone={str(core.get('tone') or '').strip() or 'unknown'}",
            f"pattern={str(pattern_value or '').strip() or 'unknown'}",
            f"environment={str(environment_value or '').strip() or 'unknown'}",
            f"version={str(core.get('version') or '').strip() or 'unknown'}",
        )
        if reason
    )


def finalize_tradex_score_output(core: dict[str, Any]) -> dict[str, Any]:
    return {
        "tone": core.get("tone"),
        "sideLabel": core.get("side_label"),
        "patternLabel": core.get("pattern_label"),
        "environmentLabel": core.get("environment_label"),
        "confidence": core.get("confidence"),
        "buyProb": core.get("buy_prob"),
        "sellProb": core.get("sell_prob"),
        "neutralProb": core.get("neutral_prob"),
        "version": core.get("version"),
        "scenarios": [dict(item) for item in core.get("scenarios") or []],
    }
