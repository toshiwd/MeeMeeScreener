from __future__ import annotations

import hashlib
import json
import math
from typing import Any, Iterable


_DEFAULT_EXCLUDE = {
    "createdAt",
    "updatedAt",
    "runtime",
    "host",
    "path",
    "realPath",
}


def _round_float(value: float, digits: int = 6) -> float:
    rounded = round(float(value), int(digits))
    if rounded == -0.0:
        return 0.0
    return float(rounded)


def _normalize(value: Any, *, exclude: set[str], digits: int) -> Any:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, int):
        return int(value)
    if isinstance(value, float):
        if not math.isfinite(value):
            return None
        return _round_float(float(value), digits=digits)
    if isinstance(value, str):
        return value
    if isinstance(value, dict):
        out: dict[str, Any] = {}
        for key in sorted(value.keys(), key=lambda k: str(k)):
            key_text = str(key)
            if key_text in exclude:
                continue
            normalized = _normalize(value[key], exclude=exclude, digits=digits)
            if normalized is None:
                continue
            out[key_text] = normalized
        return out
    if isinstance(value, (list, tuple)):
        out_list: list[Any] = []
        for item in value:
            normalized = _normalize(item, exclude=exclude, digits=digits)
            if normalized is None:
                continue
            out_list.append(normalized)
        return out_list
    return str(value)


def canonical_payload(
    payload: Any,
    *,
    exclude_fields: Iterable[str] | None = None,
    digits: int = 6,
) -> Any:
    exclude = set(_DEFAULT_EXCLUDE)
    if exclude_fields:
        for name in exclude_fields:
            exclude.add(str(name))
    return _normalize(payload, exclude=exclude, digits=int(digits))


def canonical_json(
    payload: Any,
    *,
    exclude_fields: Iterable[str] | None = None,
    digits: int = 6,
) -> str:
    normalized = canonical_payload(payload, exclude_fields=exclude_fields, digits=digits)
    return json.dumps(normalized, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def hash_payload(
    payload: Any,
    *,
    exclude_fields: Iterable[str] | None = None,
    digits: int = 6,
) -> str:
    text = canonical_json(payload, exclude_fields=exclude_fields, digits=digits)
    return hashlib.sha256(text.encode("utf-8")).hexdigest()
