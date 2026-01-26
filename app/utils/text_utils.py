from __future__ import annotations

import re


def _normalize_code(value: str | None) -> str:
    if value is None:
        return ""
    text = str(value).strip()
    if not text:
        return ""
    match = re.search(r"\d{4}", text)
    if match:
        return match.group(0)
    return text.upper()
