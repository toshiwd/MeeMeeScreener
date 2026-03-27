from __future__ import annotations

from external_analysis.image_rerank.artifacts import sha256_file
from external_analysis.image_rerank.renderer import RendererConfig, render_day80_chart


def test_image_rerank_renderer_is_deterministic(tmp_path) -> None:
    bars = []
    for idx in range(80):
        close = 100.0 + (idx * 0.5)
        bars.append({"o": close - 0.3, "h": close + 0.8, "l": close - 1.0, "c": close, "v": 1000 + idx})
    config = RendererConfig(backend="auto")
    path_1 = tmp_path / "chart-1.png"
    path_2 = tmp_path / "chart-2.png"
    manifest_1 = render_day80_chart(bars=bars, path=path_1, config=config)
    manifest_2 = render_day80_chart(bars=bars, path=path_2, config=config)
    assert manifest_1["canvas_size"] == [224, 224]
    assert manifest_1["renderer_version"] == "day80_ohlcv_v1"
    assert manifest_1["image_sha256"] == manifest_2["image_sha256"]
    assert sha256_file(path_1) == sha256_file(path_2)
