from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import sys

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from app.backend.edinetdb.client import RateLimitStop
from app.backend.edinetdb.keyring import KeyRingClient


@dataclass
class _FakeClient:
    mode: str
    name: str
    calls: int = 0

    def get_json(self, path: str, params: dict | None = None):
        self.calls += 1
        if self.mode == "rate_limit":
            raise RateLimitStop("rate_limited", status=429, body=self.name)
        return {"client": self.name, "path": path, "params": params or {}}


def test_keyring_skips_rate_limited_client_after_first_429():
    c1 = _FakeClient(mode="rate_limit", name="k1")
    c2 = _FakeClient(mode="ok", name="k2")
    ring = KeyRingClient([c1, c2])  # type: ignore[arg-type]

    first = ring.get_json("/v1/companies")
    second = ring.get_json("/v1/companies")

    assert first["client"] == "k2"
    assert second["client"] == "k2"
    assert c1.calls == 1
    assert c2.calls == 2


def test_keyring_uses_first_key_until_rate_limited():
    c1 = _FakeClient(mode="ok", name="k1")
    c2 = _FakeClient(mode="ok", name="k2")
    ring = KeyRingClient([c1, c2])  # type: ignore[arg-type]

    a = ring.get_json("/v1/companies")
    b = ring.get_json("/v1/companies")
    c = ring.get_json("/v1/companies")

    assert a["client"] == "k1"
    assert b["client"] == "k1"
    assert c["client"] == "k1"
    assert c1.calls == 3
    assert c2.calls == 0


def test_keyring_raises_when_all_clients_rate_limited():
    c1 = _FakeClient(mode="rate_limit", name="k1")
    c2 = _FakeClient(mode="rate_limit", name="k2")
    ring = KeyRingClient([c1, c2])  # type: ignore[arg-type]

    with pytest.raises(RateLimitStop):
        ring.get_json("/v1/companies")
    assert c1.calls == 1
    assert c2.calls == 1
