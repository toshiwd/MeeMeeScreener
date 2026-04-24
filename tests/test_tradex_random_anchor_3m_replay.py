from __future__ import annotations

import json
from pathlib import Path

from scripts.tradex_random_anchor_3m_replay import run_random_anchor_replay


def test_random_anchor_replay_smoke(tmp_path: Path) -> None:
    output_dir = tmp_path / "random_anchor"
    payload = run_random_anchor_replay(
        output_dir=output_dir,
        anchor_count=1,
        pool_limit=20,
    )
    summary_path = Path(payload["paths"]["random_anchor_replay_summary_json"])
    summary = json.loads(summary_path.read_text(encoding="utf-8"))

    assert payload["ok"] is True
    assert payload["decision"] in {"keep", "hold", "drop"}
    assert len(payload["anchors"]) == 1
    assert summary["anchor_count"] == 1

    paths = payload["paths"]
    for key in (
        "random_anchor_dates_json",
        "random_anchor_candidate_snapshots_json",
        "selection_only_replay_ledger_json",
        "selection_only_replay_ledger_parquet",
        "policy_trade_replay_ledger_json",
        "policy_trade_replay_ledger_parquet",
        "champion_vs_challenger_random_anchor_compare_json",
        "random_anchor_replay_summary_json",
    ):
        assert Path(paths[key]).exists(), key
