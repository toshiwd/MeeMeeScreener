from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from scripts import faster_risk_on_reentry_pretest_v1 as mod
from tests.test_market_regime_gated_risk_off_pretest_v1 import _make_gate


def test_faster_risk_on_reentry_outputs_required_artifacts(tmp_path: Path) -> None:
    gate = _make_gate(tmp_path)

    result = mod.run_pretest(gate)

    out = gate / "faster_risk_on_reentry_pretest_v1"
    assert result["complete"] is True
    for artifact in mod.REQUIRED_ARTIFACTS:
        assert (out / artifact).exists(), artifact
    complete = json.loads((out / "_ARTIFACT_COMPLETE.json").read_text(encoding="utf-8"))
    assert complete["required_artifacts_all_present"] is True
    assert complete["exact_next_open_replay"] is True
    assert complete["max_days_sweep"] is False

    summary = json.loads((out / "faster_risk_on_reentry_summary.json").read_text(encoding="utf-8"))
    assert summary["rule"]["risk_off_max_days"] == mod.FAST_RISK_OFF_MAX_DAYS
    assert summary["rule"]["max_days_sweep"] is False

    events = pd.read_csv(out / "faster_reentry_events.csv")
    assert "risk_off" in set(events["event_type"])

    decision = json.loads((out / "next_axis_decision.json").read_text(encoding="utf-8"))
    assert decision["decision"] in mod.DECISIONS
    assert decision["decision_count"] == 1
