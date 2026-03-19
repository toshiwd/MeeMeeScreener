from __future__ import annotations

import os
from typing import Any

from fastapi import HTTPException, Request, status

OPERATOR_CONSOLE_GATE_ENV = "MEEMEE_OPERATOR_CONSOLE_GATE_MODE"
OPERATOR_CONSOLE_GATE_HEADER = "X-MeeMee-Operator-Mode"
OPERATOR_CONSOLE_GATE_HEADER_VALUE = "operator"
OPERATOR_CONSOLE_GATE_MODE_OPEN = "open"
OPERATOR_CONSOLE_GATE_MODE_HEADER = "header"
OPERATOR_CONSOLE_GATE_MODE_DEV_ONLY = "dev_only"


def _normalize_text(value: Any) -> str | None:
    text = str(value or "").strip()
    return text or None


def resolve_operator_console_gate_mode() -> str:
    mode = _normalize_text(os.getenv(OPERATOR_CONSOLE_GATE_ENV)) or OPERATOR_CONSOLE_GATE_MODE_OPEN
    return mode.lower()


def describe_operator_console_gate(request: Request | None = None) -> dict[str, Any]:
    mode = resolve_operator_console_gate_mode()
    header_value = None if request is None else _normalize_text(request.headers.get(OPERATOR_CONSOLE_GATE_HEADER))
    return {
        "mode": mode,
        "enabled": mode != OPERATOR_CONSOLE_GATE_MODE_OPEN,
        "enforced": mode == OPERATOR_CONSOLE_GATE_MODE_HEADER,
        "header_name": OPERATOR_CONSOLE_GATE_HEADER,
        "header_value_present": bool(header_value),
        "header_matches": header_value == OPERATOR_CONSOLE_GATE_HEADER_VALUE,
    }


def require_operator_console_access(request: Request) -> dict[str, Any]:
    mode = resolve_operator_console_gate_mode()
    if mode in {OPERATOR_CONSOLE_GATE_MODE_OPEN, ""}:
        return describe_operator_console_gate(request)
    if mode == OPERATOR_CONSOLE_GATE_MODE_DEV_ONLY:
        app_env = _normalize_text(os.getenv("APP_ENV")).lower() if _normalize_text(os.getenv("APP_ENV")) else ""
        if app_env in {"prod", "production"}:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail={
                    "ok": False,
                    "reason": "operator_console_forbidden",
                    "gate_mode": mode,
                },
            )
        return describe_operator_console_gate(request)
    if mode == OPERATOR_CONSOLE_GATE_MODE_HEADER:
        header_value = _normalize_text(request.headers.get(OPERATOR_CONSOLE_GATE_HEADER))
        if header_value != OPERATOR_CONSOLE_GATE_HEADER_VALUE:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail={
                    "ok": False,
                    "reason": "operator_console_header_required",
                    "gate_mode": mode,
                    "header_name": OPERATOR_CONSOLE_GATE_HEADER,
                },
            )
        return describe_operator_console_gate(request)
    raise HTTPException(
        status_code=status.HTTP_403_FORBIDDEN,
        detail={
            "ok": False,
            "reason": "operator_console_forbidden",
            "gate_mode": mode,
        },
    )
