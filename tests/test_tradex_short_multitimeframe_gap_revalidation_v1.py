import json
from pathlib import Path

import pandas as pd
import pytest

from scripts.tradex_short_multitimeframe_gap_revalidation_v1 import NORMALIZED_DATE_SQL, bars_content_sha, build_context_lookup, build_or_load_cache, states_at_signal


def _bars(n=500):
    dates = pd.bdate_range("2023-01-02", periods=n)
    close = pd.Series(range(n), dtype=float) * 0.1 + 100
    return pd.DataFrame({"code": "X", "date": dates.strftime("%Y%m%d").astype(int), "o": close, "h": close+1,
                         "l": close-1, "c": close, "v": 1000})


def test_future_bar_mutation_does_not_change_signal_states():
    bars = _bars(); signal = int(bars.iloc[350]["date"])
    before = states_at_signal(bars, signal)
    mutated = bars.copy(); mutated.loc[mutated["date"] > signal, ["o", "h", "l", "c"]] *= 50
    after = states_at_signal(mutated, signal)
    assert before == after
    assert before["source_max_date"] == signal


def test_future_rows_can_be_absent_entirely():
    bars = _bars(); signal = int(bars.iloc[350]["date"])
    assert states_at_signal(bars, signal) == states_at_signal(bars[bars["date"] <= signal], signal)


def test_bulk_context_lookup_is_future_mutation_invariant():
    bars = _bars(); signals = [int(bars.iloc[300]["date"]), int(bars.iloc[350]["date"])]
    before = build_context_lookup(bars, signals)
    mutated = bars.copy(); mutated.loc[mutated["date"] > max(signals), ["o", "h", "l", "c"]] *= 50
    after = build_context_lookup(mutated, signals)
    pd.testing.assert_frame_equal(before, after)
    assert (before["source_max_date"] <= before["signal_date"]).all()


def test_month_end_context_does_not_use_unfinished_month_or_future_mutation():
    bars = _bars(); signal = int(bars.iloc[350]["date"])
    lookup = build_context_lookup(bars, [signal])
    assert int(lookup.iloc[0]["monthly_source_date"]) <= signal
    mutated = bars.copy(); mutated.loc[mutated["date"] > signal, ["o", "h", "l", "c"]] *= 100
    pd.testing.assert_frame_equal(lookup, build_context_lookup(mutated, [signal]))


def test_cache_result_exactly_matches_noncache(tmp_path: Path):
    bars = _bars(); signals = [int(bars.iloc[300]["date"]), int(bars.iloc[350]["date"])]
    db = tmp_path / "db.duckdb"; db.write_bytes(b"test-db")
    expected = build_context_lookup(bars, signals); expected.insert(0, "code", "X")
    first = build_or_load_cache(db, tmp_path / "cache", bars, {"X": signals})
    second = build_or_load_cache(db, tmp_path / "cache", bars * 1, {"X": signals})
    pd.testing.assert_frame_equal(first.reset_index(drop=True), expected.reset_index(drop=True))
    pd.testing.assert_frame_equal(second.reset_index(drop=True), expected.reset_index(drop=True))


def test_stale_cache_is_rejected(tmp_path: Path):
    bars = _bars(); signals = [int(bars.iloc[300]["date"])]
    db = tmp_path / "db.duckdb"; db.write_bytes(b"test-db")
    cache = tmp_path / "cache"; build_or_load_cache(db, cache, bars, {"X": signals})
    manifest_path = next(cache.glob("*/manifest.json"))
    manifest = json.loads(manifest_path.read_text())
    manifest["dataset_key"]["confirmed_date"] = 19990101
    manifest_path.write_text(json.dumps(manifest))
    with pytest.raises(RuntimeError, match="dataset key mismatch"):
        build_or_load_cache(db, cache, bars, {"X": signals})


def test_bar_content_fingerprint_changes_on_value_mutation():
    bars = _bars(); changed = bars.copy(); changed.loc[0, "c"] += 1
    assert bars_content_sha(bars) != bars_content_sha(changed)


def test_sql_mixed_date_normalization_and_range_filter():
    con = __import__("duckdb").connect(":memory:")
    con.execute("create table daily_bars(date varchar)")
    epoch = int(pd.Timestamp("2026-07-10", tz="UTC").timestamp())
    con.executemany("insert into daily_bars values (?)", [("20260710",), (str(epoch),), ("20181231",), ("bad",)])
    got = con.execute(f"select normalized_date from (select {NORMALIZED_DATE_SQL} normalized_date from daily_bars) where normalized_date between 20190101 and 20260710 order by normalized_date").fetchall()
    assert got == [(20260710,), (20260710,)]
