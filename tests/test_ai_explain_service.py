from __future__ import annotations

import asyncio
from pathlib import Path

import httpx
import pytest

from app.backend.infra.files.config_repo import ConfigRepository
from app.backend.services.ai_explain_service import (
    AiExplainRequestPayload,
    AiExplainSettingsUpdatePayload,
    AiExplainService,
    OpenAIStreamChunk,
    OpenAICompatibleProvider,
)


class FakeSecretStore:
    def __init__(self) -> None:
        self._values: dict[str, str] = {}

    def read_secret(self, credential_name: str) -> str | None:
        return self._values.get(credential_name)

    def write_secret(self, credential_name: str, secret: str) -> None:
        self._values[credential_name] = secret

    def delete_secret(self, credential_name: str) -> None:
        self._values.pop(credential_name, None)


def _build_service(tmp_path: Path, store: FakeSecretStore | None = None) -> AiExplainService:
    repo = ConfigRepository(str(tmp_path))
    return AiExplainService(config_repo=repo, secret_store=store or FakeSecretStore())


def test_ai_explain_settings_roundtrip_and_secret_visibility(tmp_path) -> None:
    store = FakeSecretStore()
    service = _build_service(tmp_path, store)

    payload = AiExplainSettingsUpdatePayload(
        uiVisible=True,
        enabled=True,
        providerLabel="sakura",
        providerType="openai_compatible",
        endpointUrl="https://example.invalid/v1",
        model="sakura-model",
        credentialName="sakura",
        sendImages=True,
        answerLength="short",
        dailyLimit=5,
        compareEnabled=True,
        debugEnabled=False,
        authSecret="super-secret-token",
    )
    saved = service.save_settings(payload)

    assert saved.canUse is True
    assert saved.canShowUi is True
    assert saved.credentialConfigured is True
    assert store.read_secret("sakura") == "super-secret-token"

    loaded = service.get_settings()
    assert loaded.settings.providerLabel == "sakura"
    assert loaded.settings.model == "sakura-model"
    assert loaded.providerReady is True
    assert loaded.credentialConfigured is True
    assert loaded.canShowUi is True
    assert loaded.canUse is True


def test_ai_explain_cache_hit_model_miss_and_compare_toggle(tmp_path, monkeypatch: pytest.MonkeyPatch) -> None:
    store = FakeSecretStore()
    service = _build_service(tmp_path, store)
    service.save_settings(
        AiExplainSettingsUpdatePayload(
            uiVisible=True,
            enabled=True,
            providerLabel="sakura",
            providerType="openai_compatible",
            endpointUrl="https://example.invalid/v1",
            model="model-a",
            credentialName="sakura",
            sendImages=False,
            answerLength="short",
            dailyLimit=10,
            compareEnabled=True,
            debugEnabled=False,
            authSecret="super-secret-token",
        )
    )

    calls: list[dict[str, object]] = []

    async def fake_generate(self, *, system_prompt, user_prompt, images, max_tokens, temperature=0.2):
        calls.append(
            {
                "system_prompt": system_prompt,
                "user_prompt": user_prompt,
                "images": list(images),
                "max_tokens": max_tokens,
                "temperature": temperature,
            }
        )
        return "上位理由の説明です。", {"prompt_tokens": 10, "completion_tokens": 12, "total_tokens": 22}

    monkeypatch.setattr(OpenAICompatibleProvider, "generate", fake_generate)

    request_a = AiExplainRequestPayload(
        mode="explain",
        screenType="ranking",
        userQuestion="なぜ上位？",
        snapshot={"b": 2, "a": 1},
        images=[],
    )
    first = asyncio.run(service.explain(request_a))
    second = asyncio.run(
        service.explain(
            AiExplainRequestPayload(
                mode="explain",
                screenType="ranking",
                userQuestion="なぜ上位？",
                snapshot={"a": 1, "b": 2},
                images=[],
            )
        )
    )

    assert first.cached is False
    assert first.error is None
    assert first.answer == "上位理由の説明です。"
    assert second.cached is True
    assert second.answer == "上位理由の説明です。"
    assert len(calls) == 1

    service.save_settings(
        AiExplainSettingsUpdatePayload(
            uiVisible=True,
            enabled=True,
            providerLabel="sakura",
            providerType="openai_compatible",
            endpointUrl="https://example.invalid/v1",
            model="model-b",
            credentialName="sakura",
            sendImages=False,
            answerLength="short",
            dailyLimit=10,
            compareEnabled=True,
            debugEnabled=False,
            authSecret="super-secret-token",
        )
    )

    third = asyncio.run(
        service.explain(
            AiExplainRequestPayload(
                mode="explain",
                screenType="ranking",
                userQuestion="なぜ上位？",
                snapshot={"a": 1, "b": 2},
                images=[],
            )
        )
    )

    assert third.cached is False
    assert len(calls) == 2
    assert calls[0]["max_tokens"] == 960
    assert "snapshot" in str(calls[0]["user_prompt"])


def test_ai_explain_strips_think_sections_from_model_output(tmp_path, monkeypatch: pytest.MonkeyPatch) -> None:
    store = FakeSecretStore()
    service = _build_service(tmp_path, store)
    service.save_settings(
        AiExplainSettingsUpdatePayload(
            uiVisible=True,
            enabled=True,
            providerLabel="sakura",
            providerType="openai_compatible",
            endpointUrl="https://example.invalid/v1",
            model="model-a",
            credentialName="sakura",
            sendImages=False,
            answerLength="short",
            dailyLimit=10,
            compareEnabled=True,
            debugEnabled=False,
            authSecret="super-secret-token",
        )
    )

    async def fake_generate(self, *, system_prompt, user_prompt, images, max_tokens, temperature=0.2):
        return "<think>internal reasoning</think>結論だけを返します。", {"prompt_tokens": 1, "completion_tokens": 2, "total_tokens": 3}

    monkeypatch.setattr(OpenAICompatibleProvider, "generate", fake_generate)

    result = asyncio.run(
        service.explain(
            AiExplainRequestPayload(
                mode="explain",
                screenType="detail",
                userQuestion="1514 住石HD の見どころを短く説明して",
                snapshot={"symbol": "1514"},
                images=[],
            )
        )
    )

    assert result.error is None
    assert result.answer == "結論だけを返します。"


def test_ai_explain_compacts_verbose_model_output(tmp_path, monkeypatch: pytest.MonkeyPatch) -> None:
    store = FakeSecretStore()
    service = _build_service(tmp_path, store)
    service.save_settings(
        AiExplainSettingsUpdatePayload(
            uiVisible=True,
            enabled=True,
            providerLabel="sakura",
            providerType="openai_compatible",
            endpointUrl="https://example.invalid/v1",
            model="model-a",
            credentialName="sakura",
            sendImages=False,
            answerLength="short",
            dailyLimit=10,
            compareEnabled=True,
            debugEnabled=False,
            authSecret="super-secret-token",
        )
    )

    verbose_answer = """
    画像の確認：
    1枚目は日足です。
    分析：
    直近は弱いです。
    結論：
    いまは様子見です。
    注意点：
    反発確認までは無理に追わないほうがよいです。
    """

    async def fake_generate(self, *, system_prompt, user_prompt, images, max_tokens, temperature=0.2):
        return verbose_answer, {"prompt_tokens": 1, "completion_tokens": 2, "total_tokens": 3}

    monkeypatch.setattr(OpenAICompatibleProvider, "generate", fake_generate)

    result = asyncio.run(
        service.explain(
            AiExplainRequestPayload(
                mode="explain",
                screenType="detail",
                userQuestion="1514 住石HD の見どころを短く説明して",
                snapshot={"symbol": "1514"},
                images=[],
            )
        )
    )

    assert result.error is None
    assert "画像の確認" not in result.answer
    assert "分析" not in result.answer
    assert "結論" in result.answer
    assert "注意点" in result.answer
    assert len(result.answer) > 40
    assert len(result.answer) <= 2400


def test_ai_explain_stream_emits_delta_and_done_events(tmp_path, monkeypatch: pytest.MonkeyPatch) -> None:
    store = FakeSecretStore()
    service = _build_service(tmp_path, store)
    service.save_settings(
        AiExplainSettingsUpdatePayload(
            uiVisible=True,
            enabled=True,
            providerLabel="sakura",
            providerType="openai_compatible",
            endpointUrl="https://example.invalid/v1",
            model="model-a",
            credentialName="sakura",
            sendImages=False,
            answerLength="short",
            dailyLimit=10,
            compareEnabled=True,
            debugEnabled=False,
            authSecret="super-secret-token",
        )
    )

    async def fake_generate_stream(self, *, system_prompt, user_prompt, images, max_tokens, temperature=0.2):
        yield OpenAIStreamChunk(text="最初の一文。")
        yield OpenAIStreamChunk(text="次の一文。")
        yield OpenAIStreamChunk(done=True, usage={"prompt_tokens": 1, "completion_tokens": 2, "total_tokens": 3})

    monkeypatch.setattr(OpenAICompatibleProvider, "generate_stream", fake_generate_stream)

    async def _collect():
        events = []
        async for event in service.explain_stream(
            AiExplainRequestPayload(
                mode="explain",
                screenType="detail",
                userQuestion="1514 住石HD の見どころを短く説明して",
                snapshot={"symbol": "1514"},
                images=[],
                stream=True,
            )
        ):
            events.append(event)
        return events

    events = asyncio.run(_collect())

    assert [event.type for event in events] == ["delta", "delta", "done"]
    assert events[-1].answer == "最初の一文。次の一文。"
    assert events[-1].cached is False
    assert events[-1].error is None


@pytest.mark.parametrize(
    ("screen_type", "question", "expected_hint"),
    [
        ("ranking", "上位銘柄を比較して", "上位2〜3銘柄を比較"),
        ("detail", "今後の見通しは？", "今後の展望"),
    ],
)
def test_ai_explain_prompt_includes_screen_specific_guidance(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
    screen_type: str,
    question: str,
    expected_hint: str,
) -> None:
    store = FakeSecretStore()
    service = _build_service(tmp_path, store)
    service.save_settings(
        AiExplainSettingsUpdatePayload(
            uiVisible=True,
            enabled=True,
            providerLabel="sakura",
            providerType="openai_compatible",
            endpointUrl="https://example.invalid/v1",
            model="model-a",
            credentialName="sakura",
            sendImages=False,
            answerLength="short",
            dailyLimit=10,
            compareEnabled=True,
            debugEnabled=False,
            authSecret="super-secret-token",
        )
    )

    captured: dict[str, str] = {}

    async def fake_generate(self, *, system_prompt, user_prompt, images, max_tokens, temperature=0.2):
        captured["user_prompt"] = user_prompt
        return "説明です。", {"prompt_tokens": 1, "completion_tokens": 2, "total_tokens": 3}

    monkeypatch.setattr(OpenAICompatibleProvider, "generate", fake_generate)

    result = asyncio.run(
        service.explain(
            AiExplainRequestPayload(
                mode="explain",
                screenType=screen_type,  # type: ignore[arg-type]
                userQuestion=question,
                snapshot={"symbol": "7203"},
                images=[],
            )
        )
    )

    assert result.error is None
    assert expected_hint in captured["user_prompt"]


def test_ai_explain_falls_back_to_text_only_when_images_are_rejected(
    tmp_path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = FakeSecretStore()
    service = _build_service(tmp_path, store)
    service.save_settings(
        AiExplainSettingsUpdatePayload(
            uiVisible=True,
            enabled=True,
            providerLabel="sakura",
            providerType="openai_compatible",
            endpointUrl="https://example.invalid/v1",
            model="model-a",
            credentialName="sakura",
            sendImages=True,
            answerLength="short",
            dailyLimit=10,
            compareEnabled=True,
            debugEnabled=False,
            authSecret="super-secret-token",
        )
    )

    calls: list[list[str]] = []

    async def fake_generate(self, *, system_prompt, user_prompt, images, max_tokens, temperature=0.2):
        calls.append(list(images))
        if images:
            request = httpx.Request("POST", "https://example.invalid/v1/chat/completions")
            response = httpx.Response(422, request=request, json={"error": {"message": "images unsupported"}})
            raise httpx.HTTPStatusError("unprocessable entity", request=request, response=response)
        return "画像なしで説明できました", {"prompt_tokens": 5, "completion_tokens": 8, "total_tokens": 13}

    monkeypatch.setattr(OpenAICompatibleProvider, "generate", fake_generate)

    result = asyncio.run(
        service.explain(
            AiExplainRequestPayload(
                mode="explain",
                screenType="detail",
                userQuestion="この銘柄の見どころを短く説明して",
                snapshot={"symbol": "7203"},
                images=["data:image/png;base64,AAAA"],
            )
        )
    )

    assert result.error is None
    assert result.answer == "画像なしで説明できました"
    assert result.cached is False
    assert calls == [["data:image/png;base64,AAAA"], []]


def test_ai_explain_compare_toggle_and_daily_limit(tmp_path, monkeypatch: pytest.MonkeyPatch) -> None:
    store = FakeSecretStore()
    service = _build_service(tmp_path, store)
    service.save_settings(
        AiExplainSettingsUpdatePayload(
            uiVisible=True,
            enabled=True,
            providerLabel="sakura",
            providerType="openai_compatible",
            endpointUrl="https://example.invalid/v1",
            model="model-a",
            credentialName="sakura",
            sendImages=False,
            answerLength="short",
            dailyLimit=1,
            compareEnabled=False,
            debugEnabled=False,
            authSecret="super-secret-token",
        )
    )

    async def fake_generate(self, *, system_prompt, user_prompt, images, max_tokens, temperature=0.2):
        return "説明です。", {"prompt_tokens": 10, "completion_tokens": 10, "total_tokens": 20}

    monkeypatch.setattr(OpenAICompatibleProvider, "generate", fake_generate)

    compare_error = asyncio.run(
        service.explain(
            AiExplainRequestPayload(
                mode="compare",
                screenType="compare",
                userQuestion="2銘柄の違いは？",
                snapshot={"symbols": ["7203", "7267"]},
                images=[],
            )
        )
    )
    assert compare_error.error is not None
    assert compare_error.error.kind == "compare_disabled"

    first = asyncio.run(
        service.explain(
            AiExplainRequestPayload(
                mode="explain",
                screenType="detail",
                userQuestion="この銘柄の注意点は？",
                snapshot={"symbol": "7203"},
                images=[],
            )
        )
    )
    second = asyncio.run(
        service.explain(
            AiExplainRequestPayload(
                mode="explain",
                screenType="detail",
                userQuestion="別の質問です",
                snapshot={"symbol": "7267"},
                images=[],
            )
        )
    )

    assert first.error is None
    assert second.error is not None
    assert second.error.kind == "daily_limit_reached"
