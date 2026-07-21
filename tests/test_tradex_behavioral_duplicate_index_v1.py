import json
from pathlib import Path

from scripts.tradex_behavioral_duplicate_index_v1 import build_index, run


def _write(root: Path, name: str, payload: dict) -> Path:
    p = root / name; p.write_text(json.dumps(payload), encoding="utf-8"); return p


def test_zero_change_count_does_not_infer_noop_or_membership(tmp_path: Path) -> None:
    p = _write(tmp_path, "candidate_gate_decision.json", {"unit_decisions": [{"unit_name": "x", "decision": "drop", "metrics": {"changed_top10_members_count": 0}}]})
    rows, _ = build_index([p]); row = rows[0]
    assert row["top10_membership_hash"] == "unknown" and row["changed_top10"] == "unknown"
    assert row["behavioral_duplicate"] == "unknown" and row["ranking_no_op_explicit"] == "unknown"


def test_explicit_membership_allows_jaccard_and_behavioral_duplicate(tmp_path: Path) -> None:
    p = _write(tmp_path, "candidate_gate_decision.json", {"unit_decisions": [
        {"unit_name": "a", "family_name": "f", "conditions": {"x": 1}, "side": "short", "top_k": 10, "evaluation_units": ["m1"], "comparison_contract": {"id": "fixed"}, "top10_codes": [str(i) for i in reversed(range(10))]},
        {"unit_name": "b", "family_name": "f", "conditions": {"x": 2}, "side": "short", "top_k": 10, "evaluation_units": ["m1"], "comparison_contract": {"id": "fixed"}, "top10_codes": [str(i) for i in range(9)] + ["z"]},
    ]})
    rows, _ = build_index([p]); a = next(r for r in rows if r["candidate_id"] == "a")
    assert a["top10_jaccard"] == 9 / 11
    assert a["behavioral_duplicate"] is False and a["changed_top10"] == 2
    assert a["top10_membership_hash"] != a["top10_ranked_hash"]


def test_exact_duplicate_requires_complete_identical_fingerprint(tmp_path: Path) -> None:
    a = _write(tmp_path, "bp_a.json", {"candidate_name": "same", "family_name": "f", "features": ["x"], "decision": "hold"})
    b = _write(tmp_path, "bp_b.json", {"candidate_name": "same", "family_name": "f", "features": ["x"], "decision": "hold"})
    rows, _ = build_index([a, b])
    assert rows[0]["exact_duplicate"] is True


def test_run_writes_three_outputs_without_mutation(tmp_path: Path) -> None:
    source = tmp_path / "src"; source.mkdir()
    p = _write(source, "bp_x.json", {"candidate_name": "x", "decision": "keep"}); before = p.read_bytes()
    root = run(source, tmp_path / "out")
    assert p.read_bytes() == before
    assert {x.name for x in root.iterdir()} == {"behavioral_duplicate_index.jsonl", "manifest.json", "duplicate_gate_summary.json"}
