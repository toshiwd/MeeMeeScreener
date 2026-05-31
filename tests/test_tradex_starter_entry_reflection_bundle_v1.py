from __future__ import annotations

import json

import pandas as pd

from scripts import tradex_starter_entry_reflection_bundle_v1 as mod


def test_reflection_bundle_blocks_non_keep_decision(tmp_path) -> None:
    root = tmp_path / "action"
    root.mkdir()
    (root / "research_decision.json").write_text(json.dumps({"research_decision": "hold_for_model_refinement"}), encoding="utf-8")
    (root / "no_lookahead_audit.json").write_text(json.dumps({"audit_result": "pass"}), encoding="utf-8")
    (root / "topk_actionability_comparison_summary.json").write_text(json.dumps({"rows": []}), encoding="utf-8")
    pd.DataFrame(columns=["code"]).to_csv(root / "candidate_actionability_rows.csv", index=False)
    out = mod.run(root, tmp_path / "out")
    decision = json.loads((out / "reflection_readiness_decision.json").read_text(encoding="utf-8"))
    assert decision["meemee_reflectable_candidate"] is False
    assert decision["blocker_reasons"]
