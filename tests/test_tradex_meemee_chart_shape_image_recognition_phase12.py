from __future__ import annotations

import numpy as np

from scripts import tradex_meemee_chart_shape_image_recognition_phase12 as mod


def test_metrics_reports_multiclass_quality() -> None:
    metrics = mod._metrics(np.asarray(["a", "a", "b", "b"]), np.asarray(["a", "a", "b", "a"]))
    assert metrics["count"] == 4
    assert metrics["accuracy"] == 0.75
    assert 0.0 < metrics["macro_f1"] < 1.0
