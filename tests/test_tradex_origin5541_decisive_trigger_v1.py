from __future__ import annotations

import json
from pathlib import Path

import duckdb
import pandas as pd

from scripts import tradex_origin5541_decisive_trigger_v1 as mod


def test_fixed_runtime_episode_matches_both_article_decisive_dates() -> None:
    runtime_db = Path(r"C:\Users\enish\AppData\Local\MeeMeeScreener-dev\data\stocks.duckdb")
    if not runtime_db.exists():
        return
    classified = mod.classify(mod.build_features(mod.load_bars(runtime_db)))
    compare, events = mod.build_compare(classified)
    assert compare["authoritative_result"]["buy_decisive_dates"] == ["2025-11-04", "2025-11-28"]
    assert compare["authoritative_result"]["buy_initial_decisive_dates"] == ["2025-11-04"]
    assert compare["authoritative_result"]["buy_continuation_decisive_dates"] == ["2025-11-28"]
    assert compare["authoritative_result"]["sell_decisive_dates"] == ["2025-12-11"]
    assert compare["authoritative_result"]["false_trigger_count_in_fixed_period"] == 0
    assert set(events["decision"]) == {"BUY_DECISIVE_INITIAL", "BUY_DECISIVE_CONTINUATION", "SELL_DECISIVE_RETURN_SELL"}


def test_run_writes_review_only_complete_artifact(tmp_path: Path) -> None:
    source_db = Path(r"C:\Users\enish\AppData\Local\MeeMeeScreener-dev\data\stocks.duckdb")
    if not source_db.exists():
        return
    with duckdb.connect(str(source_db), read_only=True) as source:
        rows = source.execute(
            "SELECT * FROM daily_bars WHERE code='5541' AND date BETWEEN epoch(TIMESTAMP '2025-07-01') AND epoch(TIMESTAMP '2025-12-26') ORDER BY date"
        ).fetchall()
    db_path = tmp_path / "stocks.duckdb"
    with duckdb.connect(str(db_path)) as target:
        target.execute("CREATE TABLE daily_bars(code VARCHAR, date INTEGER, o DOUBLE, h DOUBLE, l DOUBLE, c DOUBLE, v BIGINT, source VARCHAR)")
        target.executemany("INSERT INTO daily_bars VALUES (?, ?, ?, ?, ?, ?, ?, ?)", rows)
    output = tmp_path / "output"
    mod.run(db_path, output)
    compare = json.loads((output / "compare.json").read_text(encoding="utf-8"))
    complete = json.loads((output / "_ARTIFACT_COMPLETE.json").read_text(encoding="utf-8"))
    assert compare["review_only"] is True
    assert compare["judgment"]["candidate_local_decision"] == "keep_for_full_universe_validation"
    assert compare["not_changed"] == ["MeeMee", "ranking", "runtime DB", "production trading logic", "position sizing", "hedge ratios"]
    assert complete["complete"] is True
    assert complete["authoritative"] == "compare.json"
