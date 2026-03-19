from __future__ import annotations

import threading
import time
from contextlib import contextmanager
from dataclasses import dataclass
from collections import Counter
from typing import Iterator


@dataclass(frozen=True)
class OperatorMutationLockState:
    active: bool
    active_action: str | None
    active_since_epoch: float | None
    active_since: str | None


class OperatorMutationBusyError(RuntimeError):
    def __init__(self, action: str, *, holder_action: str | None = None, holder_since: str | None = None) -> None:
        self.action = action
        self.holder_action = holder_action
        self.holder_since = holder_since
        message = "operator mutation is already running"
        if holder_action:
            message = f"{message}: {holder_action}"
        super().__init__(message)


_LOCK = threading.RLock()
_ACTIVE_COUNT = 0
_ACTIVE_ACTION: str | None = None
_ACTIVE_SINCE_EPOCH: float | None = None
_OBSERVED_REASONS = Counter()
_LAST_OBSERVED_REASON: str | None = None
_LAST_OBSERVED_AT_EPOCH: float | None = None


def _now_iso() -> str:
    from datetime import datetime, timezone

    return datetime.now(timezone.utc).isoformat()


def _epoch_to_iso(value: float | None) -> str | None:
    if value is None:
        return None
    from datetime import datetime, timezone

    try:
        return datetime.fromtimestamp(float(value), tz=timezone.utc).isoformat()
    except Exception:
        return None


def get_operator_mutation_state() -> OperatorMutationLockState:
    with _LOCK:
        return OperatorMutationLockState(
            active=_ACTIVE_COUNT > 0,
            active_action=_ACTIVE_ACTION,
            active_since_epoch=_ACTIVE_SINCE_EPOCH,
            active_since=_epoch_to_iso(_ACTIVE_SINCE_EPOCH),
        )


def is_operator_mutation_active() -> bool:
    return get_operator_mutation_state().active


def record_operator_mutation_observation(reason: str, *, action: str | None = None) -> None:
    normalized_reason = str(reason or "").strip() or "unknown"
    with _LOCK:
        _OBSERVED_REASONS[normalized_reason] += 1
        global _LAST_OBSERVED_REASON, _LAST_OBSERVED_AT_EPOCH
        _LAST_OBSERVED_REASON = normalized_reason
        _LAST_OBSERVED_AT_EPOCH = time.time()


def get_operator_mutation_observability() -> dict[str, object]:
    with _LOCK:
        return {
            "operator_mutation_busy_count": int(_OBSERVED_REASONS.get("operator_mutation_busy") or 0),
            "db_busy_count": int(_OBSERVED_REASONS.get("db_busy") or 0),
            "publish_state_refresh_conflict_count": int(_OBSERVED_REASONS.get("publish_state_refresh_conflict") or 0),
            "last_reason": _LAST_OBSERVED_REASON,
            "last_reason_at_epoch": _LAST_OBSERVED_AT_EPOCH,
            "last_reason_at": _epoch_to_iso(_LAST_OBSERVED_AT_EPOCH),
        }


@contextmanager
def operator_mutation_scope(action: str, *, timeout_sec: float = 1.5) -> Iterator[None]:
    acquired = _LOCK.acquire(timeout=max(0.0, float(timeout_sec)))
    if not acquired:
        state = get_operator_mutation_state()
        raise OperatorMutationBusyError(
            action,
            holder_action=state.active_action,
            holder_since=state.active_since,
        )

    global _ACTIVE_COUNT, _ACTIVE_ACTION, _ACTIVE_SINCE_EPOCH
    try:
        _ACTIVE_COUNT += 1
        if _ACTIVE_COUNT == 1:
            _ACTIVE_ACTION = str(action or "").strip() or "operator_mutation"
            _ACTIVE_SINCE_EPOCH = time.time()
        yield
    finally:
        try:
            _ACTIVE_COUNT = max(0, _ACTIVE_COUNT - 1)
            if _ACTIVE_COUNT == 0:
                _ACTIVE_ACTION = None
                _ACTIVE_SINCE_EPOCH = None
        finally:
            _LOCK.release()


@contextmanager
def try_operator_mutation_scope(action: str) -> Iterator[bool]:
    try:
        with operator_mutation_scope(action, timeout_sec=0.0):
            yield True
    except OperatorMutationBusyError:
        yield False
