from __future__ import annotations

import numpy as np


def normalize_scores(values: list[float]) -> np.ndarray:
    array = np.asarray(values, dtype=np.float32)
    if array.size == 0:
        return array
    mean = float(array.mean())
    std = float(array.std())
    if std <= 0.0:
        return np.zeros_like(array)
    return (array - mean) / std


def fuse_rank_improver_scores(
    *,
    base_scores: list[float],
    image_scores: list[float],
    base_weight: float = 0.7,
    image_weight: float = 0.3,
) -> np.ndarray:
    base_norm = normalize_scores(base_scores)
    image_arr = np.asarray(image_scores, dtype=np.float32)
    if image_arr.size == 0:
        return image_arr
    image_norm = (image_arr * 2.0) - 1.0
    return (float(base_weight) * base_norm) + (float(image_weight) * image_norm)


def fuse_veto_helper_scores(
    *,
    base_scores: list[float],
    image_scores: list[float],
    base_weight: float = 0.85,
    veto_weight: float = 0.15,
    veto_floor: float = 0.35,
) -> np.ndarray:
    base_norm = normalize_scores(base_scores)
    image_arr = np.asarray(image_scores, dtype=np.float32)
    if image_arr.size == 0:
        return image_arr
    image_norm = (image_arr * 2.0) - 1.0
    penalty = np.clip(float(veto_floor) - image_norm, 0.0, None)
    return (float(base_weight) * base_norm) - (float(veto_weight) * penalty)


def build_fusion_sweep(
    *,
    base_scores: list[float],
    image_scores: list[float],
    base_weight: float = 0.7,
    image_weight: float = 0.3,
    veto_base_weight: float = 0.85,
    veto_weight: float = 0.15,
    veto_floor: float = 0.35,
) -> dict[str, np.ndarray]:
    return {
        "rank_improver": fuse_rank_improver_scores(
            base_scores=base_scores,
            image_scores=image_scores,
            base_weight=base_weight,
            image_weight=image_weight,
        ),
        "veto_helper": fuse_veto_helper_scores(
            base_scores=base_scores,
            image_scores=image_scores,
            base_weight=veto_base_weight,
            veto_weight=veto_weight,
            veto_floor=veto_floor,
        ),
    }


def fuse_scores(
    *,
    base_scores: list[float],
    image_scores: list[float],
    base_weight: float = 0.7,
    image_weight: float = 0.3,
) -> np.ndarray:
    return fuse_rank_improver_scores(
        base_scores=base_scores,
        image_scores=image_scores,
        base_weight=base_weight,
        image_weight=image_weight,
    )
