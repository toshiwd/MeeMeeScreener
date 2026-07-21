from __future__ import annotations

import json
import sys
from pathlib import Path

import pandas as pd

import tradex_short_denial_composite_state_family_v1 as base


def assign_feature_labels(centroids: pd.DataFrame) -> dict[int, str]:
    remaining = set(int(index) for index in centroids.index)
    labels: dict[int, str] = {}
    buy_score = (
        centroids["next_ret1"] + centroids["next_higher_low"]
        + centroids["next_close_above_rebound_high"] + centroids["next_close_position"]
        + centroids["rebound_close_vs_ma20"] - centroids["rebound_upper_wick_ratio"]
    )
    buy = int(buy_score.idxmax()); labels[buy] = "買い加速型"; remaining.remove(buy)
    volume = int(centroids.loc[list(remaining), "rebound_volume_ratio20"].idxmax())
    labels[volume] = "大出来高反発型"; remaining.remove(volume)
    deep = int(centroids.loc[list(remaining), "rebound_close_vs_ma20"].idxmin())
    labels[deep] = "深押し混合型"; remaining.remove(deep)
    resignal = int(centroids.loc[list(remaining), "next_day_resignal"].idxmax())
    labels[resignal] = "再シグナル押し戻し型"; remaining.remove(resignal)
    high_test = int(centroids.loc[list(remaining), "next_higher_high"].idxmax())
    labels[high_test] = "高値試し型"; remaining.remove(high_test)
    labels[int(next(iter(remaining)))] = "弱含み停滞型"
    return labels


def output_path() -> Path:
    index = sys.argv.index("--output")
    return Path(sys.argv[index + 1])


def normalize_role_labels(output: Path) -> None:
    replacements = {
        "買い本命候補": "開発期買い候補",
        "試し玉候補": "開発期試し玉候補",
        "売り再監視": "開発期売り再監視",
    }
    for name in ["composite_state_ledger.parquet", "composite_state_family_metrics.parquet", "composite_state_yearly_metrics.parquet"]:
        path = output / name
        frame = pd.read_parquet(path)
        if "operational_role" in frame.columns:
            frame["operational_role"] = frame["operational_role"].replace(replacements)
        frame.to_parquet(path, index=False)
    compare_path = output / "compare.json"
    payload = json.loads(compare_path.read_text(encoding="utf-8"))
    payload["schema_version"] = "tradex_short_denial_composite_state_family_v2.compare.v1"
    payload["authoritative_result"]["role_map"] = {
        key: replacements.get(value, value)
        for key, value in payload["authoritative_result"]["role_map"].items()
    }
    for section in ["families", "buy_family_validation_years"]:
        for row in payload["authoritative_result"][section]:
            row["operational_role"] = replacements.get(row.get("operational_role"), row.get("operational_role"))
    compare_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


if __name__ == "__main__":
    base.assign_feature_labels = assign_feature_labels
    target = output_path()
    base.main()
    normalize_role_labels(target)
