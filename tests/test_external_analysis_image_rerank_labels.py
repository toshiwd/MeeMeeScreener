from __future__ import annotations

from external_analysis.image_rerank.labels import label_samples


def test_image_rerank_labels_keep_ambiguous_rows_lightly_weighted() -> None:
    samples = [
        {"future_return": 0.30, "liquidity_proxy": 10.0, "feature_row_count": 80, "future_row_count": 20},
        {"future_return": 0.10, "liquidity_proxy": 10.0, "feature_row_count": 80, "future_row_count": 20},
        {"future_return": -0.15, "liquidity_proxy": 10.0, "feature_row_count": 80, "future_row_count": 20},
        {"future_return": 0.00, "liquidity_proxy": 0.0, "feature_row_count": 80, "future_row_count": 20},
    ]
    labeled, manifest = label_samples(samples=samples, positive_quantile=0.75, negative_quantile=0.25, neutral_weight=0.25)
    buckets = [row["label_bucket"] for row in labeled]
    assert buckets[0] == "positive"
    assert buckets[2] == "negative"
    assert buckets[1] == "neutral"
    assert buckets[3] == "excluded"
    assert manifest["sample_weight_policy"]["neutral"] == 0.25
