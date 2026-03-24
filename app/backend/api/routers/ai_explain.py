from __future__ import annotations

import json

from fastapi import APIRouter, Depends, Request
from fastapi.responses import StreamingResponse

from app.backend.api.dependencies import get_config_repo
from app.backend.infra.ai_explain_keyring import AiExplainSecretStore, get_default_ai_explain_secret_store
from app.backend.infra.files.config_repo import ConfigRepository
from app.backend.services.ai_explain_service import (
    AiExplainRequestPayload,
    AiExplainSettingsUpdatePayload,
    build_ai_explain_service,
)

router = APIRouter(prefix="/api/ai-explain", tags=["ai-explain"])


def get_ai_explain_secret_store() -> AiExplainSecretStore:
    return get_default_ai_explain_secret_store()


def get_ai_explain_service(
    config_repo: ConfigRepository = Depends(get_config_repo),
    secret_store: AiExplainSecretStore = Depends(get_ai_explain_secret_store),
):
    return build_ai_explain_service(config_repo=config_repo, secret_store=secret_store)


@router.get("/settings")
def get_ai_explain_settings(service=Depends(get_ai_explain_service)):
    return service.get_settings().model_dump(mode="json")


@router.put("/settings")
def update_ai_explain_settings(
    payload: AiExplainSettingsUpdatePayload,
    service=Depends(get_ai_explain_service),
):
    return service.save_settings(payload).model_dump(mode="json")


@router.post("")
async def explain_ai(
    payload: AiExplainRequestPayload,
    request: Request,
    service=Depends(get_ai_explain_service),
):
    if not payload.stream:
        result = await service.explain(payload)
        return result.model_dump(mode="json")

    async def _event_stream():
        async for event in service.explain_stream(payload):
            if await request.is_disconnected():
                break
            data = event.model_dump(mode="json")
            yield f"event: {data['type']}\n"
            yield f"data: {json.dumps(data, ensure_ascii=False, separators=(',', ':'))}\n\n"

    return StreamingResponse(
        _event_stream(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )
