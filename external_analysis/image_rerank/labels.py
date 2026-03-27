from __future__ import annotations

from typing import Any


def _percentile(values: list[float], percent: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(float(item) for item in values)
    if len(ordered) == 1:
        return ordered[0]
    position = (len(ordered) - 1) * max(0.0, min(1.0, float(percent)))
    lower = int(position)
    upper = min(len(ordered) - 1, lower + 1)
    if lower == upper:
        return ordered[lower]
    fraction = position - lower
    return ordered[lower] + (ordered[upper] - ordered[lower]) * fraction


def build_label_manifest(
    *,
    positive_quantile: float,
    negative_quantile: float,
    positive_threshold: float,
    negative_threshold: float,
    neutral_weight: float,
    liquidity_min_average_volume: float,
    label_horizon_days: int,
    sample_weight_policy: dict[str, float],
    counts: dict[str, Any],
) -> dict[str, Any]:
    return {
        "positive_bucket_rule": {"kind": "quantile", "quantile": float(positive_quantile), "threshold": float(positive_threshold)},
        "negative_bucket_rule": {"kind": "quantile", "quantile": float(negative_quantile), "threshold": float(negative_threshold)},
        "neutral_handling": {"weight": float(neutral_weight), "policy": "ambiguous labels are kept with low weight"},
        "liquidity_exclusion": {"min_average_volume": float(liquidity_min_average_volume)},
        "missing_bar_exclusion": {"policy": "exclude incomplete feature or label windows"},
        "sample_weight_policy": dict(sample_weight_policy),
        "label_horizon_days": int(label_horizon_days),
        "counts": counts,
    }


def label_samples(
    *,
    samples: list[dict[str, Any]],
    positive_quantile: float = 0.85,
    negative_quantile: float = 0.15,
    neutral_weight: float = 0.25,
    liquidity_min_average_volume: float = 1.0,
    label_horizon_days: int = 20,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    eligible_future_returns = [
        float(sample["future_return"])
        for sample in samples
        if float(sample.get("liquidity_proxy") or 0.0) >= float(liquidity_min_average_volume)
    ]
    positive_threshold = _percentile(eligible_future_returns, positive_quantile)
    negative_threshold = _percentile(eligible_future_returns, negative_quantile)
    sample_weight_policy = {"positive": 1.0, "negative": 1.0, "neutral": float(neutral_weight), "excluded": 0.0}
    labeled: list[dict[str, Any]] = []
    counts = {"sample_count": len(samples), "positive_count": 0, "negative_count": 0, "neutral_count": 0, "excluded_count": 0}
    for sample in samples:
        future_return = float(sample.get("future_return") or 0.0)
        liquidity_proxy = float(sample.get("liquidity_proxy") or 0.0)
        feature_row_count = int(sample.get("feature_row_count") or 0)
        future_row_count = int(sample.get("future_row_count") or 0)
        if feature_row_count <= 0 or future_row_count <= 0 or liquidity_proxy < float(liquidity_min_average_volume):
            counts["excluded_count"] += 1
            labeled.append({**sample, "label_bucket": "excluded", "label_value": None, "sample_weight": 0.0, "label_reason": "excluded"})
            continue
        if future_return >= positive_threshold:
            label_bucket = "positive"
        elif future_return <= negative_threshold:
            label_bucket = "negative"
        else:
            label_bucket = "neutral"
        counts[f"{label_bucket}_count"] += 1
        labeled.append(
            {
                **sample,
                "label_bucket": label_bucket,
                "label_value": 1 if label_bucket == "positive" else 0,
                "sample_weight": float(sample_weight_policy[label_bucket]),
                "label_reason": "quantile_bucket",
            }
        )
    manifest = build_label_manifest(
        positive_quantile=positive_quantile,
        negative_quantile=negative_quantile,
        positive_threshold=float(positive_threshold),
        negative_threshold=float(negative_threshold),
        neutral_weight=neutral_weight,
        liquidity_min_average_volume=liquidity_min_average_volume,
        label_horizon_days=label_horizon_days,
        sample_weight_policy=sample_weight_policy,
        counts=counts,
    )
    return labeled, manifest
