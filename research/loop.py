from __future__ import annotations

from datetime import datetime, timezone
from typing import Any
import pandas as pd

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

def run_loop_all(
    paths: ResearchPaths,
    config: ResearchConfig,
    snapshot_id: str,
    workers: int = 1,
    chunk_size: int = 120,
) -> dict[str, Any]:
    # snapshot_id 配下の calendar_month_ends.csv を読み込む
    snapshot_dir = paths.snapshot_dir(snapshot_id)
    calendar_path = snapshot_dir / "calendar_month_ends.csv"
    if not calendar_path.exists():
        raise FileNotFoundError(f"calendar not found in snapshot: {calendar_path}")
    
    calendar = pd.read_csv(calendar_path)
    asof_dates = calendar["asof_date"].astype(str).tolist()
    
    print(f"Starting loop_all for snapshot {snapshot_id} ({len(asof_dates)} months)")
    
    from research.features import build_features_for_asof
    from research.labels import build_labels_for_asof
    
    results = []
    for asof in asof_dates:
        print(f"--- Processing asof_date: {asof} ---")
        f_res = build_features_for_asof(
            paths=paths,
            config=config,
            snapshot_id=snapshot_id,
            asof_date=asof,
            force=False,
            workers=workers,
            chunk_size=chunk_size,
        )
        l_res = build_labels_for_asof(
            paths=paths,
            config=config,
            snapshot_id=snapshot_id,
            asof_date=asof,
            force=False,
            workers=workers,
            chunk_size=chunk_size,
        )
        results.append({"asof": asof, "features": f_res, "labels": l_res})
        
    return {"ok": True, "snapshot_id": snapshot_id, "months": len(asof_dates), "results_count": len(results)}
