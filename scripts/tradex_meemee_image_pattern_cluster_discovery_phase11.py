from __future__ import annotations

import argparse
import hashlib
import json
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
from PIL import Image
from sklearn.cluster import MiniBatchKMeans


AXIS_ID = "meemee_image_pattern_cluster_discovery_phase11"
DEFAULT_PHASE7_DIR = Path(r"G:\Tradex\meemee_training_preparation_phase7\20260602T080906Z-meemee_training_preparation_phase7")
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\meemee_image_pattern_cluster_discovery_phase11")
SCALES = ("macro", "structure", "short", "micro")
IMAGE_SIZE = 8
CLUSTER_COUNT = 12
RANDOM_SEED = 20260602
MIN_TRAIN_COUNT = 100
MIN_EVAL_COUNT = 30
MIN_RET20_LIFT = 0.005
CACHE_CHUNK_SIZE = 512


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _tag() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


def _write_json(path: Path, payload: Any) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _image_vector(path: str) -> np.ndarray:
    resample = getattr(getattr(Image, "Resampling", Image), "BILINEAR")
    with Image.open(path) as image:
        array = np.asarray(image.convert("L").resize((IMAGE_SIZE, IMAGE_SIZE), resample), dtype=np.float32)
    return array.reshape(-1) / 255.0


def _bundle_rows(manifest: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[str, dict[str, dict[str, Any]]] = defaultdict(dict)
    for row in manifest:
        grouped[str(row["image_sample_key"])][str(row["scale"])] = row
    bundles: list[dict[str, Any]] = []
    for key, by_scale in grouped.items():
        if any(scale not in by_scale for scale in SCALES):
            continue
        reference = by_scale[SCALES[0]]
        bundles.append({
            "image_sample_key": key,
            "code": str(reference["code"]),
            "as_of": int(reference["as_of"]),
            "split": str(reference["split"]),
            "ret20": float(reference["ret20"]),
            "future_top15_by_ret20": bool(reference["future_top15_by_ret20"]),
            "future_bottom15_by_ret20": bool(reference["future_bottom15_by_ret20"]),
            "image_paths": {scale: str(by_scale[scale]["image_path"]) for scale in SCALES},
        })
    return sorted(bundles, key=lambda row: (row["as_of"], row["code"]))


def _bundle_vector(row: dict[str, Any]) -> np.ndarray:
    return np.concatenate([_image_vector(row["image_paths"][scale]) for scale in SCALES])


def _cache_key(manifest_path: Path, rows: list[dict[str, Any]]) -> str:
    digest = hashlib.sha256()
    digest.update(str(manifest_path.resolve()).encode("utf-8"))
    stat = manifest_path.stat()
    digest.update(f":{stat.st_size}:{stat.st_mtime_ns}:{len(rows)}:{IMAGE_SIZE}:{','.join(SCALES)}".encode("utf-8"))
    return digest.hexdigest()[:20]


def _features(
    rows: list[dict[str, Any]],
    *,
    workers: int,
    cache_dir: Path,
    cache_key: str,
    chunk_size: int = CACHE_CHUNK_SIZE,
) -> np.ndarray:
    cache_dir.mkdir(parents=True, exist_ok=True)
    chunks: list[np.ndarray] = []
    for chunk_index, start in enumerate(range(0, len(rows), chunk_size)):
        chunk_path = cache_dir / f"{cache_key}-{chunk_index:05d}.npy"
        if chunk_path.exists():
            chunk = np.load(chunk_path)
        else:
            subset = rows[start:start + chunk_size]
            with ThreadPoolExecutor(max_workers=workers) as executor:
                chunk = np.vstack(list(executor.map(_bundle_vector, subset))).astype(np.float32)
            temp_path = chunk_path.with_suffix(".tmp.npy")
            np.save(temp_path, chunk)
            temp_path.replace(chunk_path)
        chunks.append(chunk)
    return np.vstack(chunks).astype(np.float32)


def _summary(rows: list[dict[str, Any]]) -> dict[str, Any]:
    if not rows:
        return {"count": 0}
    ret20 = np.asarray([row["ret20"] for row in rows], dtype=np.float64)
    return {
        "count": len(rows),
        "ret20_mean": float(ret20.mean()),
        "ret20_median": float(np.median(ret20)),
        "positive_ret20_rate": float((ret20 > 0).mean()),
        "top15_rate": float(np.mean([row["future_top15_by_ret20"] for row in rows])),
        "bottom15_rate": float(np.mean([row["future_bottom15_by_ret20"] for row in rows])),
    }


def _cluster_report(rows: list[dict[str, Any]], labels: np.ndarray) -> dict[str, Any]:
    by_split = {split: _summary([row for row in rows if row["split"] == split]) for split in ("train", "validation", "test")}
    clusters: dict[str, Any] = {}
    for cluster_id in sorted(set(labels.tolist())):
        cluster_rows = [row for row, label in zip(rows, labels) if int(label) == int(cluster_id)]
        clusters[str(cluster_id)] = {
            split: _summary([row for row in cluster_rows if row["split"] == split])
            for split in ("train", "validation", "test")
        }
    return {"overall_by_split": by_split, "clusters": clusters}


def _stable_candidates(report: dict[str, Any]) -> list[dict[str, Any]]:
    overall = report["overall_by_split"]
    candidates: list[dict[str, Any]] = []
    for cluster_id, splits in report["clusters"].items():
        train, validation, test = (splits[name] for name in ("train", "validation", "test"))
        if train["count"] < MIN_TRAIN_COUNT or validation["count"] < MIN_EVAL_COUNT or test["count"] < MIN_EVAL_COUNT:
            continue
        lifts = {
            split: splits[split]["ret20_mean"] - overall[split]["ret20_mean"]
            for split in ("train", "validation", "test")
        }
        if all(value >= MIN_RET20_LIFT for value in lifts.values()):
            candidates.append({"cluster_id": int(cluster_id), "direction": "favorable", "ret20_mean_lift": lifts})
        elif all(value <= -MIN_RET20_LIFT for value in lifts.values()):
            candidates.append({"cluster_id": int(cluster_id), "direction": "unfavorable", "ret20_mean_lift": lifts})
    return candidates


def run(
    *,
    phase7_dir: Path,
    output_root: Path,
    cluster_count: int = CLUSTER_COUNT,
    max_examples_per_cluster: int = 8,
    workers: int = 16,
    cache_dir: Path | None = None,
) -> Path:
    manifest_path = phase7_dir / "training_manifest.jsonl"
    manifest = _read_jsonl(manifest_path)
    rows = _bundle_rows(manifest)
    train_rows = [row for row in rows if row["split"] == "train"]
    if len(train_rows) < cluster_count:
        raise RuntimeError("Not enough train bundles for requested cluster count")
    model = MiniBatchKMeans(n_clusters=cluster_count, random_state=RANDOM_SEED, batch_size=1024, n_init=10)
    resolved_cache_dir = cache_dir or (output_root / "feature_cache")
    feature_cache_key = _cache_key(manifest_path, rows)
    train_features = _features(train_rows, workers=workers, cache_dir=resolved_cache_dir / "train", cache_key=feature_cache_key)
    model.fit(train_features)
    labels = model.predict(_features(rows, workers=workers, cache_dir=resolved_cache_dir / "all", cache_key=feature_cache_key))
    report = _cluster_report(rows, labels)
    stable = _stable_candidates(report)
    output_dir = output_root / f"{_tag()}-{AXIS_ID}"
    output_dir.mkdir(parents=True, exist_ok=False)
    compare = {
        "schema_version": "tradex_meemee_image_pattern_cluster_compare_phase11_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "cluster_count": cluster_count,
        "bundle_count": len(rows),
        "feature_cache_key": feature_cache_key,
        "feature_cache_dir": str(resolved_cache_dir),
        "feature_contract": {
            "scales": list(SCALES),
            "image_size_per_scale": IMAGE_SIZE,
            "labels_used_for_clustering": False,
            "labels_used_for_evaluation_only": True,
        },
        **report,
        "stable_candidates": stable,
    }
    _write_json(output_dir / "image_pattern_cluster_compare.json", compare)
    with (output_dir / "cluster_examples.jsonl").open("w", encoding="utf-8") as handle:
        for cluster_id in sorted(set(labels.tolist())):
            examples = [row for row, label in zip(rows, labels) if int(label) == int(cluster_id)]
            examples.sort(key=lambda row: abs(row["ret20"]), reverse=True)
            for row in examples[:max_examples_per_cluster]:
                handle.write(json.dumps({"cluster_id": int(cluster_id), **row}, ensure_ascii=False) + "\n")
    decision = {
        "schema_version": "tradex_meemee_image_pattern_cluster_discovery_phase11_decision_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "boundary_owner": "TRADEX",
        "authoritative_rollup_decision": "keep_stable_image_pattern_clusters_for_manual_review" if stable else "drop_no_stable_image_pattern_cluster",
        "reason_type": "stable_cluster_direction_replicates" if stable else "no_cluster_direction_replicates_across_train_validation_test",
        "stable_candidate_count": len(stable),
        "stable_candidates": stable,
        "production_ranking_changed": False,
        "runtime_db_write": False,
        "meemee_unchanged": True,
        "meemee_reflectable": False,
        "automatic_trade_action": False,
        "non_scope": ["daily ReentryReady join", "pattern naming", "MeeMee reflection", "production ranking", "automatic trade action"],
    }
    _write_json(output_dir / "research_decision.json", decision)
    _write_json(output_root / "latest_research_decision.json", {"run_root": str(output_dir), **decision})
    return output_dir


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--phase7-dir", type=Path, default=DEFAULT_PHASE7_DIR)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--cluster-count", type=int, default=CLUSTER_COUNT)
    parser.add_argument("--workers", type=int, default=16)
    parser.add_argument("--cache-dir", type=Path)
    args = parser.parse_args()
    print(run(phase7_dir=args.phase7_dir, output_root=args.output_root, cluster_count=args.cluster_count, workers=args.workers, cache_dir=args.cache_dir))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
