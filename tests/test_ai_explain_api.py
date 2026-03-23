from __future__ import annotations

from fastapi.testclient import TestClient

import app.backend.api.dependencies as dependencies
import app.backend.api.routers.ai_explain as ai_router
import app.main as main_module
from app.backend.infra.files.config_repo import ConfigRepository
from app.backend.services.ai_explain_service import OpenAICompatibleProvider, OpenAIStreamChunk


class FakeSecretStore:
    def __init__(self) -> None:
        self.values: dict[str, str] = {}

    def read_secret(self, credential_name: str) -> str | None:
        return self.values.get(credential_name)

    def write_secret(self, credential_name: str, secret: str) -> None:
        self.values[credential_name] = secret

    def delete_secret(self, credential_name: str) -> None:
        self.values.pop(credential_name, None)


def _build_client(tmp_path, monkeypatch, fake_store: FakeSecretStore) -> TestClient:
    monkeypatch.setattr(dependencies, "_config_repo", ConfigRepository(str(tmp_path)))
    monkeypatch.setattr(main_module, "init_resources", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(main_module, "cleanup_stale_jobs", lambda: None)
    monkeypatch.setattr(main_module, "start_yf_daily_ingest_scheduler", lambda: None)
    monkeypatch.setattr(main_module, "stop_yf_daily_ingest_scheduler", lambda timeout_sec=1.0: None)
    monkeypatch.setattr(main_module, "start_ranking_analysis_quality_scheduler", lambda: None)
    monkeypatch.setattr(main_module, "stop_ranking_analysis_quality_scheduler", lambda timeout_sec=1.0: None)
    monkeypatch.setattr(main_module, "start_analysis_prewarm_scheduler", lambda: None)
    monkeypatch.setattr(main_module, "stop_analysis_prewarm_scheduler", lambda timeout_sec=1.0: None)

    class _NoopThread:
        def __init__(self, *args, **kwargs):
            pass

        def start(self) -> None:
            return None

    monkeypatch.setattr(main_module.threading, "Thread", _NoopThread)
    app = main_module.create_app()
    app.dependency_overrides[ai_router.get_ai_explain_secret_store] = lambda: fake_store
    return TestClient(app)


def test_ai_explain_settings_and_request_route_roundtrip(monkeypatch, tmp_path) -> None:
    fake_store = FakeSecretStore()
    client = _build_client(tmp_path, monkeypatch, fake_store)

    default_response = client.get("/api/ai-explain/settings")
    assert default_response.status_code == 200
    default_payload = default_response.json()
    assert default_payload["canUse"] is False
    assert default_payload["settings"]["providerLabel"] == "sakura"

    saved = client.put(
        "/api/ai-explain/settings",
        json={
            "uiVisible": True,
            "enabled": True,
            "providerLabel": "sakura",
            "providerType": "openai_compatible",
            "endpointUrl": "https://example.invalid/v1",
            "model": "sakura-model",
            "credentialName": "sakura",
            "sendImages": False,
            "answerLength": "short",
            "dailyLimit": 5,
            "compareEnabled": True,
            "debugEnabled": False,
            "authSecret": "secret-token",
            "clearAuthSecret": False,
        },
    )
    assert saved.status_code == 200
    saved_payload = saved.json()
    assert saved_payload["canUse"] is True
    assert saved_payload["credentialConfigured"] is True

    async def fake_generate(self, *, system_prompt, user_prompt, images, max_tokens, temperature=0.2):
        return "短い説明です。", {"prompt_tokens": 7, "completion_tokens": 9, "total_tokens": 16}

    monkeypatch.setattr(OpenAICompatibleProvider, "generate", fake_generate)

    response = client.post(
        "/api/ai-explain",
        json={
            "mode": "explain",
            "screenType": "detail",
            "userQuestion": "この銘柄の注意点は？",
            "snapshot": {"symbol": "7203", "name": "トヨタ"},
            "images": [],
        },
    )
    assert response.status_code == 200
    payload = response.json()
    assert payload["cached"] is False
    assert payload["answer"] == "短い説明です。"
    assert payload["error"] is None

    cached = client.post(
        "/api/ai-explain",
        json={
            "mode": "explain",
            "screenType": "detail",
            "userQuestion": "この銘柄の注意点は？",
            "snapshot": {"name": "トヨタ", "symbol": "7203"},
            "images": [],
        },
    )
    assert cached.status_code == 200
    cached_payload = cached.json()
    assert cached_payload["cached"] is True
    assert cached_payload["answer"] == "短い説明です。"


def test_ai_explain_stream_route_emits_sse_events(monkeypatch, tmp_path) -> None:
    fake_store = FakeSecretStore()
    client = _build_client(tmp_path, monkeypatch, fake_store)

    client.put(
        "/api/ai-explain/settings",
        json={
            "uiVisible": True,
            "enabled": True,
            "providerLabel": "sakura",
            "providerType": "openai_compatible",
            "endpointUrl": "https://example.invalid/v1",
            "model": "sakura-model",
            "credentialName": "sakura",
            "sendImages": False,
            "answerLength": "short",
            "dailyLimit": 5,
            "compareEnabled": True,
            "debugEnabled": False,
            "authSecret": "secret-token",
            "clearAuthSecret": False,
        },
    )

    async def fake_generate_stream(self, *, system_prompt, user_prompt, images, max_tokens, temperature=0.2):
        yield OpenAIStreamChunk(text="最初の文。")
        yield OpenAIStreamChunk(text="続き。")
        yield OpenAIStreamChunk(done=True, usage={"prompt_tokens": 1, "completion_tokens": 2, "total_tokens": 3})

    monkeypatch.setattr(OpenAICompatibleProvider, "generate_stream", fake_generate_stream)

    response = client.post(
        "/api/ai-explain",
        json={
            "mode": "explain",
            "screenType": "detail",
            "userQuestion": "この銘柄の注意点は？",
            "snapshot": {"symbol": "7203", "name": "トヨタ"},
            "images": [],
            "stream": True,
        },
    )
    assert response.status_code == 200
    body = response.text
    assert "event: delta" in body
    assert "event: done" in body
    assert "最初の文。" in body
    assert "続き。" in body
