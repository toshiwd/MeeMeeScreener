from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from scripts.tradex_pullback_retest_confirmation_contract_preflight_v1 import SNAPSHOT_COLUMNS, build


def test_preflight_blocks_candidate_generation_until_sequence_ledger_exists(tmp_path: Path) -> None:
    source = tmp_path / "source"
    source.mkdir()
    row = {column: 0 for column in SNAPSHOT_COLUMNS}
    row.update({"as_of_date": 20250101, "code": "1001"})
    pd.DataFrame([row]).to_parquet(source / "pattern_family_source_rows.parquet", index=False)
    (source / "no_lookahead_audit.json").write_text('{"audit_result":"pass"}\n', encoding="utf-8")

    output = build(source_root=source, output_root=tmp_path / "out")
    decision = json.loads((output / "research_decision.json").read_text(encoding="utf-8"))
    contract = json.loads((output / "pullback_retest_confirmation_contract.json").read_text(encoding="utf-8"))

    assert decision["decision_class"] == "BLOCKED"
    assert decision["candidate_generation_changed"] is False
    assert "confirmation_as_of" in contract["sequence_fields_missing"]
