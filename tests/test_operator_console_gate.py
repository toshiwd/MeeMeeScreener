from __future__ import annotations

from fastapi import Depends, FastAPI
from fastapi.testclient import TestClient

from app.backend.api.operator_console_gate import require_operator_console_access
from app.backend.services.operator_mutation_lock import get_operator_mutation_observability, record_operator_mutation_observation


def test_operator_console_gate_requires_header_when_enabled(monkeypatch) -> None:
    monkeypatch.setenv("MEEMEE_OPERATOR_CONSOLE_GATE_MODE", "header")

    app = FastAPI()

    @app.get("/guarded", dependencies=[Depends(require_operator_console_access)])
    def guarded() -> dict[str, bool]:
        return {"ok": True}

    client = TestClient(app)

    blocked = client.get("/guarded")
    assert blocked.status_code == 403
    assert blocked.json()["detail"]["reason"] == "operator_console_header_required"

    allowed = client.get("/guarded", headers={"X-MeeMee-Operator-Mode": "operator"})
    assert allowed.status_code == 200
    assert allowed.json() == {"ok": True}


def test_operator_console_gate_is_open_by_default(monkeypatch) -> None:
    monkeypatch.delenv("MEEMEE_OPERATOR_CONSOLE_GATE_MODE", raising=False)

    app = FastAPI()

    @app.get("/guarded", dependencies=[Depends(require_operator_console_access)])
    def guarded() -> dict[str, bool]:
        return {"ok": True}

    client = TestClient(app)
    response = client.get("/guarded")
    assert response.status_code == 200
    assert response.json() == {"ok": True}


def test_operator_mutation_observability_records_reason() -> None:
    before = get_operator_mutation_observability()
    record_operator_mutation_observation("db_busy", action="test")
    after = get_operator_mutation_observability()

    assert after["db_busy_count"] >= before["db_busy_count"] + 1
    assert after["last_reason"] == "db_busy"
