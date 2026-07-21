import json
from pathlib import Path

from scripts.tradex_short_entry_family_cards_v1 import build_cards, run


def _write(root: Path, name: str, payload: dict) -> Path:
    p = root / name
    p.write_text(json.dumps(payload), encoding="utf-8")
    return p


def test_compare_alone_never_infers_decision(tmp_path: Path) -> None:
    p = _write(tmp_path, "entry_precision_short_trend_compare.json", {"decision_rollup": {"winner": "x"}})
    cards, warnings = build_cards([p])
    assert cards == []
    assert warnings[0]["warning"] == "no_explicit_decision_no_card"


def test_decision_priority_and_axis_bundle(tmp_path: Path) -> None:
    compare = _write(tmp_path, "entry_precision_short_broad_down_monthlybreak_quality_compare.json", {"comparison_contract": {"period": "fixed"}, "variants": [1]})
    fix = _write(tmp_path, "entry_precision_short_broad_down_monthlybreak_fix_decision.json", {"session_id": "s", "baseline_id": "b", "challenger_id": "c", "overall_decision": "hold"})
    explicit = _write(tmp_path, "entry_precision_short_broad_down_monthlybreak_audit.json", {"authoritative_rollup_decision": "keep"})
    cards, _ = build_cards([compare, fix, explicit])
    assert len(cards) == 1
    card = cards[0]
    assert (card["family"], card["axis"]) == ("broad_down", "monthlybreak")
    assert card["raw_decision"] == "hold" and card["normalized_decision"] == "hold"
    assert len(card["sources"]) == 3
    assert card["fingerprint_status"] == "incomplete"
    assert set(card["fingerprint_missing_inputs"]) == {
        "code_version", "data_version", "universe", "regime",
        "entry", "exit", "target", "features_or_conditions",
    }


def test_run_preserves_sources_and_writes_manifest(tmp_path: Path) -> None:
    source = tmp_path / "src"; source.mkdir()
    p = _write(source, "entry_precision_short_trend_regime_decision.json", {"session_id": "s", "baseline_id": "b", "challenger_id": "c", "overall_decision": "drop", "comparison_contract": {"fixed": True}})
    before = p.read_bytes()
    root = run(source, tmp_path / "out")
    assert p.read_bytes() == before
    lines = (root / "short_entry_family_cards.jsonl").read_text().splitlines()
    assert len(lines) == 1 and json.loads(lines[0])["axis"] == "regime"
    manifest = json.loads((root / "manifest.json").read_text())
    assert manifest["card_count"] == 1 and manifest["runtime_db_write"] is False
    card = json.loads(lines[0])
    assert card["fingerprint_status"] == "incomplete"
    assert "code_version" in card["fingerprint_missing_inputs"]
