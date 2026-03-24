from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import re
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, AsyncIterator, Literal

import httpx
from pydantic import BaseModel, Field

from app.backend.infra.ai_explain_keyring import AiExplainSecretStore, get_default_ai_explain_secret_store
from app.backend.infra.files.config_repo import ConfigRepository

logger = logging.getLogger(__name__)

AI_EXPLAIN_SCHEMA_VERSION = "ai_explain_v3"
AI_EXPLAIN_CACHE_TTL_SEC = 24 * 60 * 60
AI_EXPLAIN_MAX_IMAGES = 3
AI_EXPLAIN_DEFAULT_PROVIDER_LABEL = "sakura"
AI_EXPLAIN_DEFAULT_PROVIDER_TYPE = "openai_compatible"
AI_EXPLAIN_DEFAULT_ANSWER_LENGTH: Literal["short", "medium", "long"] = "short"
AI_EXPLAIN_DEFAULT_DAILY_LIMIT = 20
AI_EXPLAIN_PROVIDER_TIMEOUT_SEC = 60.0
AI_EXPLAIN_SYSTEM_PROMPT = """あなたは MeeMee の AI解説です。
ランキングを決める役割ではなく、MeeMee が今見せている画面を説明する役割だけを担ってください。
MeeMee が渡した snapshot と画像だけを根拠にしてください。外部知識や推測で補わないでください。
confirmed を主根拠にし、provisional は補助扱いにしてください。
ranking では上位2〜3銘柄の比較と順位差の主因を、detail では今後の見通しと注意点を、compare では差分を先に述べてください。
回答は短く切りすぎず、必要な説明を省かず、3〜5文程度でまとめてください。
不要な前置き、長い雑談、投資判断の断定は避けてください。
内部の思考過程や <think> 形式は出力せず、最終回答だけを返してください。
"""


class AiExplainErrorPayload(BaseModel):
    kind: str
    message: str


class AiExplainRequestPayload(BaseModel):
    mode: Literal["explain", "compare", "summarize"]
    screenType: Literal["ranking", "detail", "compare"]
    userQuestion: str
    snapshot: dict[str, Any] = Field(default_factory=dict)
    images: list[str] = Field(default_factory=list)
    stream: bool = False


class AiExplainResponsePayload(BaseModel):
    answer: str
    cached: bool
    provider: str
    model: str
    latencyMs: int
    error: AiExplainErrorPayload | None = None


class AiExplainStreamEventPayload(BaseModel):
    type: Literal["delta", "done", "error"]
    delta: str | None = None
    answer: str = ""
    cached: bool = False
    provider: str = ""
    model: str = ""
    latencyMs: int = 0
    error: AiExplainErrorPayload | None = None


class AiExplainSettingsPayload(BaseModel):
    uiVisible: bool = False
    enabled: bool = False
    providerLabel: str = AI_EXPLAIN_DEFAULT_PROVIDER_LABEL
    providerType: str = AI_EXPLAIN_DEFAULT_PROVIDER_TYPE
    endpointUrl: str = ""
    model: str = ""
    credentialName: str = AI_EXPLAIN_DEFAULT_PROVIDER_LABEL
    sendImages: bool = True
    answerLength: Literal["short", "medium", "long"] = AI_EXPLAIN_DEFAULT_ANSWER_LENGTH
    dailyLimit: int = AI_EXPLAIN_DEFAULT_DAILY_LIMIT
    compareEnabled: bool = True
    debugEnabled: bool = False


class AiExplainSettingsUpdatePayload(AiExplainSettingsPayload):
    authSecret: str | None = None
    clearAuthSecret: bool = False


class AiExplainSettingsStatePayload(BaseModel):
    settings: AiExplainSettingsPayload
    providerReady: bool
    credentialConfigured: bool
    canShowUi: bool
    canUse: bool


@dataclass(frozen=True)
class AiExplainConfigSnapshot:
    settings: AiExplainSettingsPayload
    providerReady: bool
    credentialConfigured: bool
    canShowUi: bool
    canUse: bool
    authSecret: str | None


def _normalize_text(value: Any) -> str:
    return str(value or "").strip()


def _default_credential_name(provider_label: str) -> str:
    normalized = _normalize_text(provider_label)
    return normalized or AI_EXPLAIN_DEFAULT_PROVIDER_LABEL


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _canonical_json(payload: Any) -> str:
    return json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str)


def _sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _hash_json(payload: Any) -> str:
    return _sha256_text(_canonical_json(payload))


def _normalize_provider_label(value: Any) -> str:
    text = _normalize_text(value)
    return text or AI_EXPLAIN_DEFAULT_PROVIDER_LABEL


def _normalize_provider_type(value: Any) -> str:
    text = _normalize_text(value)
    return text or AI_EXPLAIN_DEFAULT_PROVIDER_TYPE


def _normalize_answer_length(value: Any) -> Literal["short", "medium", "long"]:
    text = _normalize_text(value).lower()
    if text in {"short", "medium", "long"}:
        return text  # type: ignore[return-value]
    return AI_EXPLAIN_DEFAULT_ANSWER_LENGTH


def _normalize_int(value: Any, fallback: int, *, minimum: int = 1, maximum: int = 1000) -> int:
    try:
        resolved = int(value)
    except Exception:
        return fallback
    return max(minimum, min(maximum, resolved))


def _normalize_settings_payload(payload: dict[str, Any] | None) -> AiExplainSettingsPayload:
    raw = dict(payload or {})
    return AiExplainSettingsPayload(
        uiVisible=bool(raw.get("uiVisible", False)),
        enabled=bool(raw.get("enabled", False)),
        providerLabel=_normalize_provider_label(raw.get("providerLabel")),
        providerType=_normalize_provider_type(raw.get("providerType")),
        endpointUrl=_normalize_text(raw.get("endpointUrl")),
        model=_normalize_text(raw.get("model")),
        credentialName=_normalize_text(raw.get("credentialName")) or _default_credential_name(raw.get("providerLabel")),
        sendImages=bool(raw.get("sendImages", True)),
        answerLength=_normalize_answer_length(raw.get("answerLength")),
        dailyLimit=_normalize_int(raw.get("dailyLimit"), AI_EXPLAIN_DEFAULT_DAILY_LIMIT, minimum=0, maximum=1000),
        compareEnabled=bool(raw.get("compareEnabled", True)),
        debugEnabled=bool(raw.get("debugEnabled", False)),
    )


def _settings_to_storage_payload(settings: AiExplainSettingsPayload) -> dict[str, Any]:
    return {
        "schema_version": AI_EXPLAIN_SCHEMA_VERSION,
        "uiVisible": bool(settings.uiVisible),
        "enabled": bool(settings.enabled),
        "providerLabel": _normalize_provider_label(settings.providerLabel),
        "providerType": _normalize_provider_type(settings.providerType),
        "endpointUrl": _normalize_text(settings.endpointUrl),
        "model": _normalize_text(settings.model),
        "credentialName": _normalize_text(settings.credentialName)
        or _default_credential_name(settings.providerLabel),
        "sendImages": bool(settings.sendImages),
        "answerLength": _normalize_answer_length(settings.answerLength),
        "dailyLimit": _normalize_int(settings.dailyLimit, AI_EXPLAIN_DEFAULT_DAILY_LIMIT, minimum=0, maximum=1000),
        "compareEnabled": bool(settings.compareEnabled),
        "debugEnabled": bool(settings.debugEnabled),
    }


def _current_secret_name(settings: AiExplainSettingsPayload) -> str:
    return _normalize_text(settings.credentialName) or _default_credential_name(settings.providerLabel)


def _answer_token_limit(answer_length: Literal["short", "medium", "long"]) -> int:
    return {"short": 960, "medium": 1440, "long": 2048}[answer_length]


def _screen_type_instruction(screen_type: Literal["ranking", "detail", "compare"]) -> str:
    if screen_type == "ranking":
        return (
            "ranking画面では、上位2〜3銘柄を比較し、順位差の主因、共通点、違いを先に述べてください。"
        )
    if screen_type == "detail":
        return (
            "detail画面では、対象銘柄の今後の展望を中心に、短期の見方、中期の見通し、注意点をまとめてください。"
        )
    if screen_type == "compare":
        return (
            "compare画面では、2銘柄の差分を先に述べ、どちらが相対的に強いか、何が違うかを整理してください。"
        )
    return ""

def _normalize_endpoint_url(value: str) -> str:
    endpoint = _normalize_text(value).rstrip("/")
    if not endpoint:
        return ""
    if endpoint.endswith("/chat/completions"):
        return endpoint
    return f"{endpoint}/chat/completions"


def _truncate_text(value: Any, limit: int = 120) -> str:
    text = _normalize_text(value)
    if len(text) <= limit:
        return text
    if limit <= 1:
        return text[:limit]
    return f"{text[: limit - 1]}…"


def _compact_visible_item(item: Any) -> dict[str, Any]:
    if not isinstance(item, dict):
        return {"value": _truncate_text(item)}
    keys = (
        "code",
        "name",
        "rank",
        "score",
        "chg1D",
        "chg1W",
        "entryPriorityLabel",
        "entryPriorityScore",
        "shortPriorityLabel",
        "stage",
        "buyPatternName",
        "buyState",
        "swingQualified",
        "swingScore",
    )
    compacted: dict[str, Any] = {}
    for key in keys:
        if key not in item:
            continue
        value = item.get(key)
        compacted[key] = _truncate_text(value, 80) if isinstance(value, str) else value
    return compacted


def _compact_snapshot_for_prompt(snapshot: Any) -> dict[str, Any]:
    if not isinstance(snapshot, dict):
        return {"value": _truncate_text(snapshot)}
    compacted: dict[str, Any] = {}
    for key in (
        "mode",
        "screenType",
        "asOfDate",
        "userQuestion",
        "selectedSymbols",
        "compareSymbols",
        "visibleTimeframe",
        "marketContext",
        "visibleItems",
        "selectedCount",
        "visibleCount",
        "main",
        "compare",
        "compareDifference",
    ):
        if key not in snapshot:
            continue
        value = snapshot.get(key)
        if key == "visibleItems" and isinstance(value, list):
            compacted[key] = [_compact_visible_item(item) for item in value[:5]]
            continue
        if key in {"main", "compare"} and isinstance(value, dict):
            compacted[key] = _compact_visible_item(value)
            continue
        if key == "marketContext" and isinstance(value, dict):
            allowed = {}
            for context_key in ("rankWhich", "rankMode", "riskMode", "dir", "fallback"):
                if context_key in value:
                    allowed[context_key] = value[context_key]
            compacted[key] = allowed
            continue
        if key == "compareDifference" and isinstance(value, dict):
            allowed = {}
            for diff_key in ("code", "targetLabel"):
                if diff_key in value:
                    allowed[diff_key] = _truncate_text(value[diff_key], 80)
            compacted[key] = allowed
            continue
        if key == "visibleTimeframe" and isinstance(value, dict):
            compacted[key] = {
                timeframe_key: value[timeframe_key]
                for timeframe_key in ("daily", "weekly", "monthly", "compareDaily", "compareMonthly")
                if timeframe_key in value
            }
            continue
        compacted[key] = _truncate_text(value, 120) if isinstance(value, str) else value
    return compacted


def _question_prompt(request: AiExplainRequestPayload) -> str:
    snapshot_json = _canonical_json(_compact_snapshot_for_prompt(request.snapshot))
    screen_instruction = _screen_type_instruction(request.screenType)
    return (
        f"mode: {request.mode}\n"
        f"screenType: {request.screenType}\n"
        f"userQuestion: {request.userQuestion.strip()}\n"
        f"snapshot:\n{snapshot_json}\n\n"
        f"{screen_instruction}\n"
        "以下を守ってください。\n"
        "- 画面にある情報だけで答えてください。\n"
        "- 結論を先に述べてください。\n"
        "- 原文の snapshot をそのまま繰り返さないでください。\n"
        "- 確認できないことは断定しないでください。\n"
    )

def _image_hash(image: str) -> str:
    return _sha256_text(image)


def _normalize_images(images: list[str]) -> list[str]:
    cleaned = [_normalize_text(image) for image in images if _normalize_text(image)]
    if len(cleaned) > AI_EXPLAIN_MAX_IMAGES:
        raise ValueError("too_many_images")
    return cleaned


def _cache_key_payload(
    *,
    settings: AiExplainSettingsPayload,
    request: AiExplainRequestPayload,
    images: list[str],
) -> dict[str, Any]:
    return {
        "schema_version": "ai_explain_cache_key_v3",
        "providerLabel": _normalize_provider_label(settings.providerLabel),
        "providerType": _normalize_provider_type(settings.providerType),
        "endpointUrl": _normalize_endpoint_url(settings.endpointUrl),
        "model": _normalize_text(settings.model),
        "mode": request.mode,
        "screenType": request.screenType,
        "userQuestion": request.userQuestion.strip(),
        "snapshotHash": _hash_json(request.snapshot),
        "imagesHash": [_image_hash(image) for image in images],
    }


def _prune_cache_entries(entries: dict[str, Any], *, now: datetime) -> dict[str, Any]:
    pruned: dict[str, Any] = {}
    for key, entry in entries.items():
        if not isinstance(entry, dict):
            continue
        expires_at = _normalize_text(entry.get("expiresAt"))
        if not expires_at:
            continue
        try:
            expires_dt = datetime.fromisoformat(expires_at.replace("Z", "+00:00"))
        except Exception:
            continue
        if expires_dt <= now:
            continue
        pruned[key] = entry
    return pruned


def _load_cache(config_repo: ConfigRepository) -> dict[str, Any]:
    payload = config_repo.load_ai_explain_cache_state()
    entries = payload.get("entries")
    if not isinstance(entries, dict):
        return {}
    now = datetime.now(timezone.utc)
    return _prune_cache_entries(entries, now=now)


def _save_cache(config_repo: ConfigRepository, entries: dict[str, Any]) -> None:
    config_repo.save_ai_explain_cache_state(
        {
            "schema_version": "ai_explain_cache_v3",
            "entries": entries,
        }
    )


def _load_usage(config_repo: ConfigRepository) -> dict[str, Any]:
    payload = config_repo.load_ai_explain_usage_state()
    if not isinstance(payload, dict):
        return {}
    return payload


def _save_usage(config_repo: ConfigRepository, usage: dict[str, Any]) -> None:
    config_repo.save_ai_explain_usage_state(usage)


def _can_use_today(config_repo: ConfigRepository, limit: int) -> bool:
    if limit <= 0:
        return True
    today = datetime.now(timezone.utc).date().isoformat()
    usage = _load_usage(config_repo)
    if usage.get("date") != today:
        usage = {"schema_version": "ai_explain_usage_v1", "date": today, "count": 0}
    count = int(usage.get("count") or 0)
    if count >= limit:
        return False
    return True


def _record_usage(config_repo: ConfigRepository) -> None:
    today = datetime.now(timezone.utc).date().isoformat()
    usage = _load_usage(config_repo)
    if usage.get("date") != today:
        usage = {"schema_version": "ai_explain_usage_v1", "date": today, "count": 0}
    count = int(usage.get("count") or 0)
    usage["date"] = today
    usage["count"] = count + 1
    usage["schema_version"] = "ai_explain_usage_v1"
    _save_usage(config_repo, usage)


def _restore_default_usage(config_repo: ConfigRepository) -> None:
    today = datetime.now(timezone.utc).date().isoformat()
    _save_usage(
        config_repo,
        {
            "schema_version": "ai_explain_usage_v1",
            "date": today,
            "count": 0,
        },
    )


def _build_settings_snapshot(
    config_repo: ConfigRepository,
    secret_store: AiExplainSecretStore | None = None,
) -> AiExplainConfigSnapshot:
    store = secret_store or get_default_ai_explain_secret_store()
    stored = config_repo.load_ai_explain_settings_state()
    settings = _normalize_settings_payload(stored)
    credential_name = _current_secret_name(settings)
    provider_ready = bool(settings.providerType == AI_EXPLAIN_DEFAULT_PROVIDER_TYPE and settings.endpointUrl and settings.model)
    auth_secret = None
    credential_configured = False
    try:
        auth_secret = store.read_secret(credential_name)
        credential_configured = bool(auth_secret)
    except Exception as exc:
        logger.warning("ai_explain secret lookup failed: %s", exc)
        auth_secret = None
        credential_configured = False
    can_show_ui = bool(settings.uiVisible and settings.enabled and provider_ready and credential_configured)
    can_use = bool(settings.enabled and provider_ready and credential_configured)
    return AiExplainConfigSnapshot(
        settings=AiExplainSettingsPayload(
            uiVisible=settings.uiVisible,
            enabled=settings.enabled,
            providerLabel=settings.providerLabel,
            providerType=settings.providerType,
            endpointUrl=settings.endpointUrl,
            model=settings.model,
            credentialName=credential_name,
            sendImages=settings.sendImages,
            answerLength=settings.answerLength,
            dailyLimit=settings.dailyLimit,
            compareEnabled=settings.compareEnabled,
            debugEnabled=settings.debugEnabled,
        ),
        providerReady=provider_ready,
        credentialConfigured=credential_configured,
        canShowUi=can_show_ui,
        canUse=can_use,
        authSecret=auth_secret,
    )


def get_ai_explain_settings(
    config_repo: ConfigRepository,
    secret_store: AiExplainSecretStore | None = None,
) -> AiExplainSettingsStatePayload:
    snapshot = _build_settings_snapshot(config_repo, secret_store)
    return AiExplainSettingsStatePayload(
        settings=snapshot.settings,
        providerReady=snapshot.providerReady,
        credentialConfigured=snapshot.credentialConfigured,
        canShowUi=snapshot.canShowUi,
        canUse=snapshot.canUse,
    )


def save_ai_explain_settings(
    *,
    config_repo: ConfigRepository,
    payload: AiExplainSettingsUpdatePayload,
    secret_store: AiExplainSecretStore | None = None,
) -> AiExplainSettingsStatePayload:
    store = secret_store or get_default_ai_explain_secret_store()
    settings = _normalize_settings_payload(payload.model_dump())
    secret_name = _current_secret_name(settings)

    if payload.clearAuthSecret:
        try:
            store.delete_secret(secret_name)
        except Exception as exc:
            logger.warning("ai_explain secret delete failed: %s", exc)
            raise
    elif _normalize_text(payload.authSecret):
        store.write_secret(secret_name, _normalize_text(payload.authSecret))

    config_repo.save_ai_explain_settings_state(_settings_to_storage_payload(settings))
    return get_ai_explain_settings(config_repo=config_repo, secret_store=store)


def _parse_usage_metadata(response_json: dict[str, Any]) -> dict[str, Any] | None:
    usage = response_json.get("usage")
    if not isinstance(usage, dict):
        return None
    return {
        "prompt_tokens": usage.get("prompt_tokens"),
        "completion_tokens": usage.get("completion_tokens"),
        "total_tokens": usage.get("total_tokens"),
    }


def _strip_reasoning_sections(text: str) -> str:
    cleaned = _normalize_text(text)
    if not cleaned:
        return ""
    cleaned = re.sub(r"<think>.*?</think>", "", cleaned, flags=re.IGNORECASE | re.DOTALL).strip()
    if not cleaned and "</think>" in text:
        cleaned = _normalize_text(text.rsplit("</think>", 1)[-1])
    cleaned = re.sub(r"\n{3,}", "\n\n", cleaned).strip()
    return cleaned


def _truncate_answer_text(text: str, limit: int = 240) -> str:
    cleaned = _normalize_text(text)
    if len(cleaned) <= limit:
        return cleaned
    cutoff = max(cleaned.rfind("。", 0, limit), cleaned.rfind("\n", 0, limit), cleaned.rfind("！", 0, limit), cleaned.rfind("?", 0, limit))
    if cutoff >= max(40, limit // 2):
        return cleaned[: cutoff + 1].strip()
    return cleaned[: max(1, limit - 1)].rstrip() + "…"


def _compact_ai_answer(text: str) -> str:
    cleaned = _strip_reasoning_sections(text)
    if not cleaned:
        return ""
    lines = [line.strip() for line in cleaned.splitlines() if line.strip()]
    filtered: list[str] = []
    for line in lines:
        normalized = re.sub(r"^[\-\*・•\d\.\)\s]+", "", line).strip()
        if not normalized:
            continue
        if any(
            keyword in normalized
            for keyword in (
                "userQuestion",
                "snapshot",
                "提供された情報",
                "チャート画像の分析",
                "画像の確認",
                "分析",
                "制約に従って",
                "内部の思考過程",
                "システムプロンプト",
            )
        ):
            continue
        filtered.append(normalized)

    if not filtered:
        filtered = lines

    compacted = "\n".join(filtered)
    compacted = re.sub(r"\n{3,}", "\n\n", compacted).strip()
    return _truncate_answer_text(compacted, 2400)

def _normalize_ai_answer(text: str) -> str:
    return _compact_ai_answer(text)


def _parse_openai_content(choice: dict[str, Any]) -> str:
    message = choice.get("message")
    if isinstance(message, dict):
        content = message.get("content")
        if isinstance(content, str):
            return _strip_reasoning_sections(content)
        if isinstance(content, list):
            parts: list[str] = []
            for part in content:
                if isinstance(part, dict) and isinstance(part.get("text"), str):
                    parts.append(part["text"])
            if parts:
                return _strip_reasoning_sections("".join(parts))
    text = choice.get("text")
    if isinstance(text, str):
        return _strip_reasoning_sections(text)
    return ""


@dataclass(frozen=True)
class OpenAIStreamChunk:
    text: str = ""
    usage: dict[str, Any] | None = None
    done: bool = False


async def _iter_sse_payloads(response: httpx.Response) -> AsyncIterator[str]:
    data_lines: list[str] = []
    async for line in response.aiter_lines():
        if line is None:
            continue
        text = line.strip("\r")
        if not text:
            if data_lines:
                yield "\n".join(data_lines)
                data_lines = []
            continue
        if text.startswith(":"):
            continue
        if text.startswith("event:") or text.startswith("id:") or text.startswith("retry:"):
            continue
        if text.startswith("data:"):
            data_lines.append(text[5:].lstrip())
            continue
        if data_lines:
            continue
    if data_lines:
        yield "\n".join(data_lines)


class OpenAICompatibleProvider:
    def __init__(
        self,
        *,
        endpoint_url: str,
        api_key: str,
        model: str,
        timeout_sec: float = AI_EXPLAIN_PROVIDER_TIMEOUT_SEC,
    ):
        self._endpoint_url = _normalize_endpoint_url(endpoint_url)
        self._api_key = api_key
        self._model = model
        self._timeout = timeout_sec

    async def generate(
        self,
        *,
        system_prompt: str,
        user_prompt: str,
        images: list[str],
        max_tokens: int,
        temperature: float = 0.2,
    ) -> tuple[str, dict[str, Any] | None]:
        if not self._endpoint_url:
            raise ValueError("endpoint_url_required")
        headers = {
            "Authorization": f"Bearer {self._api_key}",
            "Content-Type": "application/json",
        }
        user_content: Any
        if images:
            user_content = [{"type": "text", "text": user_prompt}] + [
                {"type": "image_url", "image_url": {"url": image, "detail": "auto"}} for image in images
            ]
        else:
            user_content = user_prompt
        body = {
            "model": self._model,
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_content},
            ],
            "temperature": temperature,
            "max_tokens": max_tokens,
        }
        timeout = httpx.Timeout(self._timeout, connect=min(self._timeout, 10.0))
        async with httpx.AsyncClient(timeout=timeout) as client:
            response = await client.post(self._endpoint_url, json=body, headers=headers)
            response.raise_for_status()
            response_json = response.json()
        choices = response_json.get("choices")
        if not isinstance(choices, list) or not choices:
            raise ValueError("missing_choices")
        answer = _parse_openai_content(choices[0])
        if not answer:
            raise ValueError("missing_answer")
        return answer, _parse_usage_metadata(response_json)

    async def generate_stream(
        self,
        *,
        system_prompt: str,
        user_prompt: str,
        images: list[str],
        max_tokens: int,
        temperature: float = 0.2,
    ) -> AsyncIterator[OpenAIStreamChunk]:
        if not self._endpoint_url:
            raise ValueError("endpoint_url_required")
        headers = {
            "Authorization": f"Bearer {self._api_key}",
            "Content-Type": "application/json",
            "Accept": "text/event-stream",
        }
        user_content: Any
        if images:
            user_content = [{"type": "text", "text": user_prompt}] + [
                {"type": "image_url", "image_url": {"url": image, "detail": "auto"}} for image in images
            ]
        else:
            user_content = user_prompt
        body = {
            "model": self._model,
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_content},
            ],
            "temperature": temperature,
            "max_tokens": max_tokens,
            "stream": True,
        }
        timeout = httpx.Timeout(self._timeout, connect=min(self._timeout, 10.0))
        async with httpx.AsyncClient(timeout=timeout) as client:
            async with client.stream("POST", self._endpoint_url, json=body, headers=headers) as response:
                response.raise_for_status()
                content_type = (response.headers.get("content-type") or "").lower()
                if "application/json" in content_type:
                    response_json = json.loads((await response.aread()).decode("utf-8"))
                    choices = response_json.get("choices")
                    if not isinstance(choices, list) or not choices:
                        raise ValueError("missing_choices")
                    answer = _parse_openai_content(choices[0])
                    if not answer:
                        raise ValueError("missing_answer")
                    yield OpenAIStreamChunk(text=answer, usage=_parse_usage_metadata(response_json), done=True)
                    return

                final_usage: dict[str, Any] | None = None
                async for payload in _iter_sse_payloads(response):
                    if not payload:
                        continue
                    if payload == "[DONE]":
                        break
                    try:
                        chunk_json = json.loads(payload)
                    except json.JSONDecodeError:
                        yield OpenAIStreamChunk(text=_strip_reasoning_sections(payload))
                        continue
                    if isinstance(chunk_json, dict):
                        usage = _parse_usage_metadata(chunk_json)
                        if usage is not None:
                            final_usage = usage
                        choices = chunk_json.get("choices")
                        if isinstance(choices, list):
                            for choice in choices:
                                if not isinstance(choice, dict):
                                    continue
                                delta = choice.get("delta")
                                if isinstance(delta, dict):
                                    content = delta.get("content")
                                    if isinstance(content, str) and content:
                                        yield OpenAIStreamChunk(text=_strip_reasoning_sections(content))
                                    elif isinstance(content, list):
                                        parts: list[str] = []
                                        for part in content:
                                            if isinstance(part, dict) and isinstance(part.get("text"), str):
                                                parts.append(part["text"])
                                        if parts:
                                            yield OpenAIStreamChunk(text=_strip_reasoning_sections("".join(parts)))
                                message = choice.get("message")
                                if isinstance(message, dict):
                                    content = message.get("content")
                                    if isinstance(content, str) and content:
                                        yield OpenAIStreamChunk(text=_strip_reasoning_sections(content))
                    continue
                yield OpenAIStreamChunk(done=True, usage=final_usage)


class AiExplainService:
    def __init__(
        self,
        *,
        config_repo: ConfigRepository,
        secret_store: AiExplainSecretStore | None = None,
    ):
        self._config_repo = config_repo
        self._secret_store = secret_store or get_default_ai_explain_secret_store()

    def get_settings(self) -> AiExplainSettingsStatePayload:
        return get_ai_explain_settings(self._config_repo, self._secret_store)

    def save_settings(self, payload: AiExplainSettingsUpdatePayload) -> AiExplainSettingsStatePayload:
        return save_ai_explain_settings(
            config_repo=self._config_repo,
            payload=payload,
            secret_store=self._secret_store,
        )

    async def explain(self, payload: AiExplainRequestPayload) -> AiExplainResponsePayload:
        start = asyncio.get_running_loop().time()
        try:
            settings_snapshot = _build_settings_snapshot(self._config_repo, self._secret_store)
            settings = settings_snapshot.settings
            snapshot_json = _canonical_json(payload.snapshot)
            snapshot_bytes = len(snapshot_json.encode("utf-8"))
            if not settings_snapshot.canUse:
                return self._error_response(
                    settings=settings,
                    latency_ms=int((asyncio.get_running_loop().time() - start) * 1000),
                    kind="settings_not_ready",
                    message="AI解説の設定が未完了です。",
                )
            if payload.mode == "compare" and not settings.compareEnabled:
                return self._error_response(
                    settings=settings,
                    latency_ms=int((asyncio.get_running_loop().time() - start) * 1000),
                    kind="compare_disabled",
                    message="比較モードは無効です。",
                )
            images = _normalize_images(payload.images if settings.sendImages else [])
            image_count = len(images)
            if payload.screenType == "compare" and payload.mode != "compare":
                # Keep compare screens available, but still allow explain/summarize if the user asks for it.
                pass
            cache_key = _hash_json(
                _cache_key_payload(settings=settings, request=payload, images=images)
            )
            cached_response = self._read_cached_response(cache_key)
            if cached_response is not None:
                elapsed_ms = int((asyncio.get_running_loop().time() - start) * 1000)
                logger.info(
                    "ai_explain cache_hit mode=%s screenType=%s provider=%s model=%s cached=%s latencyMs=%s snapshotBytes=%s imageCount=%s",
                    payload.mode,
                    payload.screenType,
                    settings.providerLabel,
                    settings.model,
                    True,
                    elapsed_ms,
                    snapshot_bytes,
                    image_count,
                )
                return AiExplainResponsePayload(
                    answer=cached_response["answer"],
                    cached=True,
                    provider=cached_response["provider"],
                    model=cached_response["model"],
                    latencyMs=elapsed_ms,
                    error=None,
                )
            if not _can_use_today(self._config_repo, settings.dailyLimit):
                return self._error_response(
                    settings=settings,
                    latency_ms=int((asyncio.get_running_loop().time() - start) * 1000),
                    kind="daily_limit_reached",
                    message="本日の AI解説 利用上限に達しました。",
                )
            auth_secret = settings_snapshot.authSecret
            if not auth_secret:
                return self._error_response(
                    settings=settings,
                    latency_ms=int((asyncio.get_running_loop().time() - start) * 1000),
                    kind="credential_missing",
                    message="認証情報が保存されていません。",
                )
            provider = OpenAICompatibleProvider(
                endpoint_url=settings.endpointUrl,
                api_key=auth_secret,
                model=settings.model,
            )
            image_fallback_used = False
            try:
                answer, usage = await provider.generate(
                    system_prompt=AI_EXPLAIN_SYSTEM_PROMPT,
                    user_prompt=_question_prompt(payload),
                    images=images,
                    max_tokens=_answer_token_limit(settings.answerLength),
                )
            except httpx.HTTPStatusError as exc:
                if not images or exc.response.status_code not in {400, 415, 422}:
                    raise
                logger.info(
                    "ai_explain image_fallback mode=%s screenType=%s provider=%s model=%s status=%s",
                    payload.mode,
                    payload.screenType,
                    settings.providerLabel,
                    settings.model,
                    exc.response.status_code,
                )
                image_fallback_used = True
                answer, usage = await provider.generate(
                    system_prompt=AI_EXPLAIN_SYSTEM_PROMPT,
                    user_prompt=_question_prompt(payload),
                    images=[],
                    max_tokens=_answer_token_limit(settings.answerLength),
                )
            answer = _normalize_ai_answer(answer)
            self._write_cached_response(
                cache_key=cache_key,
                answer=answer,
                provider=settings.providerLabel,
                model=settings.model,
                snapshot_hash=_hash_json(payload.snapshot),
                images_hash=[_image_hash(image) for image in images],
                usage=usage,
            )
            _record_usage(self._config_repo)
            elapsed_ms = int((asyncio.get_running_loop().time() - start) * 1000)
            logger.info(
                "ai_explain success mode=%s screenType=%s provider=%s model=%s cached=%s latencyMs=%s snapshotBytes=%s imageCount=%s",
                payload.mode,
                payload.screenType,
                settings.providerLabel,
                settings.model,
                False,
                elapsed_ms,
                snapshot_bytes,
                image_count,
            )
            if image_fallback_used:
                logger.info(
                    "ai_explain image_fallback_applied mode=%s screenType=%s provider=%s model=%s latencyMs=%s",
                    payload.mode,
                    payload.screenType,
                    settings.providerLabel,
                    settings.model,
                    elapsed_ms,
                )
            return AiExplainResponsePayload(
                answer=answer,
                cached=False,
                provider=settings.providerLabel,
                model=settings.model,
                latencyMs=elapsed_ms,
                error=None,
            )
        except ValueError as exc:
            kind = str(exc) or "invalid_request"
            return self._error_response(
                settings=self.get_settings().settings,
                latency_ms=int((asyncio.get_running_loop().time() - start) * 1000),
                kind=kind,
                message="リクエストを処理できませんでした。",
            )
        except httpx.TimeoutException:
            return self._error_response(
                settings=self.get_settings().settings,
                latency_ms=int((asyncio.get_running_loop().time() - start) * 1000),
                kind="provider_timeout",
                message="AI provider の応答がタイムアウトしました。",
            )
        except httpx.HTTPStatusError as exc:
            logger.warning("ai_explain provider http error: %s", exc)
            return self._error_response(
                settings=self.get_settings().settings,
                latency_ms=int((asyncio.get_running_loop().time() - start) * 1000),
                kind="provider_http_error",
                message=f"AI provider がエラーを返しました: {exc.response.status_code}",
            )
        except httpx.RequestError as exc:
            logger.warning("ai_explain provider request error: %s", exc)
            return self._error_response(
                settings=self.get_settings().settings,
                latency_ms=int((asyncio.get_running_loop().time() - start) * 1000),
                kind="provider_request_error",
                message="AI provider への接続に失敗しました。",
            )
        except Exception as exc:
            logger.exception("ai_explain unexpected error: %s", exc)
            return self._error_response(
                settings=self.get_settings().settings,
                latency_ms=int((asyncio.get_running_loop().time() - start) * 1000),
                kind="internal_error",
                message="AI解説の取得に失敗しました。",
            )

    async def explain_stream(self, payload: AiExplainRequestPayload) -> AsyncIterator[AiExplainStreamEventPayload]:
        start = asyncio.get_running_loop().time()
        try:
            settings_snapshot = _build_settings_snapshot(self._config_repo, self._secret_store)
            settings = settings_snapshot.settings
            snapshot_json = _canonical_json(payload.snapshot)
            snapshot_bytes = len(snapshot_json.encode("utf-8"))
            if not settings_snapshot.canUse:
                yield self._stream_error_response(
                    settings=settings,
                    latency_ms=int((asyncio.get_running_loop().time() - start) * 1000),
                    kind="settings_not_ready",
                    message="AI解説の設定が未完了です。",
                )
                return
            if payload.mode == "compare" and not settings.compareEnabled:
                yield self._stream_error_response(
                    settings=settings,
                    latency_ms=int((asyncio.get_running_loop().time() - start) * 1000),
                    kind="compare_disabled",
                    message="比較モードは無効です。",
                )
                return
            images = _normalize_images(payload.images if settings.sendImages else [])
            image_count = len(images)
            cache_key = _hash_json(
                _cache_key_payload(settings=settings, request=payload, images=images)
            )
            cached_response = self._read_cached_response(cache_key)
            if cached_response is not None:
                elapsed_ms = int((asyncio.get_running_loop().time() - start) * 1000)
                logger.info(
                    "ai_explain cache_hit mode=%s screenType=%s provider=%s model=%s cached=%s latencyMs=%s snapshotBytes=%s imageCount=%s",
                    payload.mode,
                    payload.screenType,
                    settings.providerLabel,
                    settings.model,
                    True,
                    elapsed_ms,
                    snapshot_bytes,
                    image_count,
                )
                yield AiExplainStreamEventPayload(
                    type="done",
                    answer=cached_response["answer"],
                    cached=True,
                    provider=cached_response["provider"],
                    model=cached_response["model"],
                    latencyMs=elapsed_ms,
                    error=None,
                )
                return
            if not _can_use_today(self._config_repo, settings.dailyLimit):
                yield self._stream_error_response(
                    settings=settings,
                    latency_ms=int((asyncio.get_running_loop().time() - start) * 1000),
                    kind="daily_limit_reached",
                    message="本日の AI解説 利用上限に達しました。",
                )
                return
            auth_secret = settings_snapshot.authSecret
            if not auth_secret:
                yield self._stream_error_response(
                    settings=settings,
                    latency_ms=int((asyncio.get_running_loop().time() - start) * 1000),
                    kind="credential_missing",
                    message="認証情報が保存されていません。",
                )
                return
            provider = OpenAICompatibleProvider(
                endpoint_url=settings.endpointUrl,
                api_key=auth_secret,
                model=settings.model,
                timeout_sec=AI_EXPLAIN_PROVIDER_TIMEOUT_SEC,
            )
            image_fallback_used = False
            answer_parts: list[str] = []
            usage: dict[str, Any] | None = None

            async def _stream_once(image_values: list[str]) -> AsyncIterator[AiExplainStreamEventPayload]:
                nonlocal usage
                async for chunk in provider.generate_stream(
                    system_prompt=AI_EXPLAIN_SYSTEM_PROMPT,
                    user_prompt=_question_prompt(payload),
                    images=image_values,
                    max_tokens=_answer_token_limit(settings.answerLength),
                ):
                    if chunk.text:
                        answer_parts.append(chunk.text)
                        yield AiExplainStreamEventPayload(type="delta", delta=chunk.text)
                    if chunk.done:
                        usage = chunk.usage

            try:
                async for event in _stream_once(images):
                    yield event
            except httpx.HTTPStatusError as exc:
                if not images or exc.response.status_code not in {400, 415, 422}:
                    raise
                logger.info(
                    "ai_explain image_fallback mode=%s screenType=%s provider=%s model=%s status=%s",
                    payload.mode,
                    payload.screenType,
                    settings.providerLabel,
                    settings.model,
                    exc.response.status_code,
                )
                image_fallback_used = True
                answer_parts = []
                usage = None
                async for event in _stream_once([]):
                    yield event

            answer = _normalize_ai_answer("".join(answer_parts))
            if not answer:
                raise ValueError("missing_answer")
            self._write_cached_response(
                cache_key=cache_key,
                answer=answer,
                provider=settings.providerLabel,
                model=settings.model,
                snapshot_hash=_hash_json(payload.snapshot),
                images_hash=[_image_hash(image) for image in images],
                usage=usage,
            )
            _record_usage(self._config_repo)
            elapsed_ms = int((asyncio.get_running_loop().time() - start) * 1000)
            logger.info(
                "ai_explain success mode=%s screenType=%s provider=%s model=%s cached=%s latencyMs=%s snapshotBytes=%s imageCount=%s",
                payload.mode,
                payload.screenType,
                settings.providerLabel,
                settings.model,
                False,
                elapsed_ms,
                snapshot_bytes,
                image_count,
            )
            if image_fallback_used:
                logger.info(
                    "ai_explain image_fallback_applied mode=%s screenType=%s provider=%s model=%s latencyMs=%s",
                    payload.mode,
                    payload.screenType,
                    settings.providerLabel,
                    settings.model,
                    elapsed_ms,
                )
            yield AiExplainStreamEventPayload(
                type="done",
                answer=answer,
                cached=False,
                provider=settings.providerLabel,
                model=settings.model,
                latencyMs=elapsed_ms,
                error=None,
            )
        except ValueError as exc:
            kind = str(exc) or "invalid_request"
            yield self._stream_error_response(
                settings=self.get_settings().settings,
                latency_ms=int((asyncio.get_running_loop().time() - start) * 1000),
                kind=kind,
                message="リクエストを処理できませんでした。",
            )
        except httpx.TimeoutException:
            yield self._stream_error_response(
                settings=self.get_settings().settings,
                latency_ms=int((asyncio.get_running_loop().time() - start) * 1000),
                kind="provider_timeout",
                message="AI provider の応答がタイムアウトしました。",
            )
        except httpx.HTTPStatusError as exc:
            logger.warning("ai_explain provider http error: %s", exc)
            yield self._stream_error_response(
                settings=self.get_settings().settings,
                latency_ms=int((asyncio.get_running_loop().time() - start) * 1000),
                kind="provider_http_error",
                message=f"AI provider がエラーを返しました: {exc.response.status_code}",
            )
        except httpx.RequestError as exc:
            logger.warning("ai_explain provider request error: %s", exc)
            yield self._stream_error_response(
                settings=self.get_settings().settings,
                latency_ms=int((asyncio.get_running_loop().time() - start) * 1000),
                kind="provider_request_error",
                message="AI provider への接続に失敗しました。",
            )
        except Exception as exc:
            logger.exception("ai_explain unexpected error: %s", exc)
            yield self._stream_error_response(
                settings=self.get_settings().settings,
                latency_ms=int((asyncio.get_running_loop().time() - start) * 1000),
                kind="internal_error",
                message="AI解説の取得に失敗しました。",
            )

    def _read_cached_response(self, cache_key: str) -> dict[str, Any] | None:
        entries = _load_cache(self._config_repo)
        entry = entries.get(cache_key)
        if not isinstance(entry, dict):
            return None
        expires_at = _normalize_text(entry.get("expiresAt"))
        if not expires_at:
            return None
        try:
            expires_dt = datetime.fromisoformat(expires_at.replace("Z", "+00:00"))
        except Exception:
            return None
        if expires_dt <= datetime.now(timezone.utc):
            return None
        return entry

    def _write_cached_response(
        self,
        *,
        cache_key: str,
        answer: str,
        provider: str,
        model: str,
        snapshot_hash: str,
        images_hash: list[str],
        usage: dict[str, Any] | None,
    ) -> None:
        entries = _load_cache(self._config_repo)
        entries[cache_key] = {
            "answer": answer,
            "provider": provider,
            "model": model,
            "snapshotHash": snapshot_hash,
            "imagesHash": images_hash,
            "usage": usage,
            "createdAt": _now_iso(),
            "expiresAt": (datetime.now(timezone.utc) + timedelta(seconds=AI_EXPLAIN_CACHE_TTL_SEC)).isoformat(),
        }
        _save_cache(self._config_repo, entries)

    def _error_response(
        self,
        *,
        settings: AiExplainSettingsPayload,
        latency_ms: int,
        kind: str,
        message: str,
    ) -> AiExplainResponsePayload:
        logger.info(
            "ai_explain error provider=%s model=%s kind=%s latencyMs=%s",
            settings.providerLabel,
            settings.model,
            kind,
            latency_ms,
        )
        return AiExplainResponsePayload(
            answer="",
            cached=False,
            provider=settings.providerLabel,
            model=settings.model,
            latencyMs=latency_ms,
            error=AiExplainErrorPayload(kind=kind, message=message),
        )

    def _stream_error_response(
        self,
        *,
        settings: AiExplainSettingsPayload,
        latency_ms: int,
        kind: str,
        message: str,
    ) -> AiExplainStreamEventPayload:
        logger.info(
            "ai_explain error provider=%s model=%s kind=%s latencyMs=%s",
            settings.providerLabel,
            settings.model,
            kind,
            latency_ms,
        )
        return AiExplainStreamEventPayload(
            type="error",
            answer="",
            cached=False,
            provider=settings.providerLabel,
            model=settings.model,
            latencyMs=latency_ms,
            error=AiExplainErrorPayload(kind=kind, message=message),
        )


def build_ai_explain_service(
    *,
    config_repo: ConfigRepository,
    secret_store: AiExplainSecretStore | None = None,
) -> AiExplainService:
    return AiExplainService(config_repo=config_repo, secret_store=secret_store)
