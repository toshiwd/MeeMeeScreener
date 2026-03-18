from __future__ import annotations

from datetime import datetime

from app.backend.core.jobs import job_manager
from app.backend.services.toredex.toredex_self_improve import run_self_improve, run_self_improve_loop


def _to_int(value: object, default: int) -> int:
    try:
        return int(value)
    except Exception:
        return int(default)


def _to_float_or_none(value: object) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except Exception:
        return None


def _to_bool_or_none(value: object) -> bool | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    if text in {"1", "true", "yes", "on"}:
        return True
    if text in {"0", "false", "no", "off"}:
        return False
    return None


def _to_str_list(value: object) -> list[str] | None:
    if value is None:
        return None
    if isinstance(value, list):
        out = [str(item).strip() for item in value if str(item).strip()]
        return out if out else None
    text = str(value).strip()
    if not text:
        return None
    return [text]


def handle_toredex_self_improve(job_id: str, payload: dict) -> None:
    mode = str(payload.get("mode") or "challenger").strip().lower()
    iterations = _to_int(payload.get("iterations"), 0)
    stage2_topk = _to_int(payload.get("stage2_topk"), 0)
    seed = payload.get("seed")
    seed_value = _to_int(seed, 0) if seed is not None else None
    stage0_months = _to_int(payload.get("stage0_months"), 0)
    stage1_months = _to_int(payload.get("stage1_months"), 0)
    stage2_months = _to_int(payload.get("stage2_months"), 0)
    parallel_workers = _to_int(payload.get("parallel_workers"), 0)
    parallel_db_paths = _to_str_list(payload.get("parallel_db_paths"))
    if parallel_db_paths is None:
        parallel_db_paths = _to_str_list(payload.get("parallel_db_path"))
    max_cycles = _to_int(payload.get("max_cycles"), 0)
    target_net_return_pct = _to_float_or_none(payload.get("target_net_return_pct"))
    target_score_objective = _to_float_or_none(payload.get("target_score_objective"))
    require_stage2_pass = _to_bool_or_none(payload.get("require_stage2_pass"))
    loop_requested = (
        bool(payload.get("loop"))
        or max_cycles > 0
        or target_net_return_pct is not None
        or target_score_objective is not None
    )

    job_manager._update_db(
        job_id,
        "toredex_self_improve",
        "running",
        progress=10,
        message=(
            "TOREDEX self-improve starting "
            f"(mode={mode}, iterations={iterations or 'auto'}, stage2_topk={stage2_topk or 'auto'}, "
            f"loop={loop_requested})"
        ),
    )

    if loop_requested:
        result = run_self_improve_loop(
            mode=mode,
            iterations=iterations if iterations > 0 else None,
            stage2_topk=stage2_topk if stage2_topk > 0 else None,
            seed=seed_value,
            stage0_months=stage0_months if stage0_months > 0 else None,
            stage1_months=stage1_months if stage1_months > 0 else None,
            stage2_months=stage2_months if stage2_months > 0 else None,
            parallel_workers=parallel_workers if parallel_workers > 0 else None,
            parallel_db_paths=parallel_db_paths,
            max_cycles=max_cycles if max_cycles > 0 else None,
            target_net_return_pct=target_net_return_pct,
            target_score_objective=target_score_objective,
            require_stage2_pass=require_stage2_pass,
        )
    else:
        result = run_self_improve(
            mode=mode,
            iterations=iterations if iterations > 0 else None,
            stage2_topk=stage2_topk if stage2_topk > 0 else None,
            seed=seed_value,
            stage0_months=stage0_months if stage0_months > 0 else None,
            stage1_months=stage1_months if stage1_months > 0 else None,
            stage2_months=stage2_months if stage2_months > 0 else None,
            parallel_workers=parallel_workers if parallel_workers > 0 else None,
            parallel_db_paths=parallel_db_paths,
        )

    if loop_requested:
        best = result.get("best_overall") if isinstance(result.get("best_overall"), dict) else {}
        msg = (
            "TOREDEX self-improve loop completed "
            f"(cycles={result.get('completed_cycles')}/{result.get('max_cycles')}, "
            f"reached={result.get('reached')}, stop={result.get('stop_reason')}, "
            f"best={str(best.get('config_hash') or '')[:8]})"
        )
    else:
        best = result.get("best_stage2") if isinstance(result.get("best_stage2"), dict) else {}
        msg = (
            "TOREDEX self-improve completed "
            f"(stage0={result.get('counts', {}).get('stage0')}, "
            f"stage1_pass={result.get('counts', {}).get('stage1_pass')}, "
            f"stage2={result.get('counts', {}).get('stage2')}, "
            f"best={best.get('config_hash', '')[:8]})"
        )

    job_manager._update_db(
        job_id,
        "toredex_self_improve",
        "success",
        progress=100,
        message=msg,
        finished_at=datetime.now(),
    )
