from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from research.config import ResearchConfig, apply_variant
from research.evaluate import run_evaluate
from research.storage import ResearchPaths, now_utc_iso, write_json
from research.train import run_train


def run_loop(
    paths: ResearchPaths,
    config: ResearchConfig,
    snapshot_id: str,
    asof_date: str,
    cycles: int,
    workers: int = 1,
    chunk_size: int = 120,
) -> dict[str, Any]:
    if cycles < 1:
        raise ValueError("cycles must be >= 1")

    variants = list(config.loop_variants)
    if not variants:
        raise ValueError("loop variants are empty")

    started_at = now_utc_iso()
    results: list[dict[str, Any]] = []
    for idx in range(cycles):
        variant = variants[idx % len(variants)]
        variant_cfg = apply_variant(config, variant)
        run_id = datetime.now(timezone.utc).strftime(f"loop_{variant.name}_%Y%m%d%H%M%S_{idx+1:03d}")

        train_result = run_train(
            paths=paths,
            config=variant_cfg,
            snapshot_id=snapshot_id,
            asof_date=asof_date,
            run_id=run_id,
            workers=workers,
            chunk_size=chunk_size,
        )
        eval_result = run_evaluate(paths=paths, run_id=run_id)
        results.append(
            {
                "cycle": idx + 1,
                "variant": variant.name,
                "run_id": run_id,
                "train": train_result,
                "evaluate": eval_result,
            }
        )

    summary = {
        "ok": True,
        "started_at": started_at,
        "finished_at": now_utc_iso(),
        "asof_date": asof_date,
        "snapshot_id": snapshot_id,
        "cycles": cycles,
        "results": results,
    }
    summary_path = paths.evaluations_root / f"loop_{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}.json"
    write_json(summary_path, summary)
    return summary
