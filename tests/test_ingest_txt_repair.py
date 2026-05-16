from __future__ import annotations

import os

import duckdb
import pandas as pd

from app.backend import ingest_txt


def test_repair_concatenated_records_splits_one_boundary() -> None:
    raw = (
        "2170,2026/01/20,530,530,518,523,218.199996948242"
        "2170,2026/01/20,530,530,518,523,218.199996948242"
    )
    repaired, count = ingest_txt._repair_concatenated_records(raw)

    assert count == 1
    assert repaired.count("\n") == 1
    assert repaired.splitlines()[0].endswith("218.199996948242")
    assert repaired.splitlines()[1].startswith("2170,2026/01/20")


def test_repair_concatenated_records_handles_multiple_boundaries() -> None:
    raw = (
        "1111,2026/01/01,1,1,1,1,1"
        "2222,2026/01/02,2,2,2,2,2"
        "3333,2026/01/03,3,3,3,3,3"
    )
    repaired, count = ingest_txt._repair_concatenated_records(raw)

    assert count == 2
    assert repaired.splitlines() == [
        "1111,2026/01/01,1,1,1,1,1",
        "2222,2026/01/02,2,2,2,2,2",
        "3333,2026/01/03,3,3,3,3,3",
    ]


def test_read_csv_with_fallback_repairs_parser_error(tmp_path) -> None:
    path = tmp_path / "broken.txt"
    path.write_text(
        "\n".join(
            [
                "2170,2026/01/19,526,526,517,523,337",
                (
                    "2170,2026/01/20,530,530,518,523,218.199996948242"
                    "2170,2026/01/21,516,519,511,516,219"
                ),
                "2170,2026/01/22,520,527,518,524,200.699996948242",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    df, repaired = ingest_txt.read_csv_with_fallback(str(path))

    assert repaired == 1
    assert len(df) == 4
    assert df.iloc[1]["date"] == "2026/01/20"
    assert df.iloc[2]["date"] == "2026/01/21"


def test_incremental_selection_skips_mtime_only_rewrite_with_same_content_hash(tmp_path) -> None:
    path = tmp_path / "1301_sample.txt"
    path.write_text("1301,2026/01/20,1,2,1,2,100\n", encoding="utf-8")
    current_sha = ingest_txt._file_sha256(str(path))
    current_size = path.stat().st_size
    os.utime(path, (2000.0, 2000.0))

    selection = ingest_txt._select_txt_files_for_ingest(
        [str(path)],
        {"1301_sample.txt": {"mtime": 1000.0, "size": current_size, "sha256": current_sha}},
        incremental=True,
        now_ts=3000.0,
    )

    assert selection["files"] == []
    assert selection["changed_files"] == []
    assert selection["skipped_count"] == 1
    assert selection["content_hash_checks"] == 1
    assert selection["content_hash_skips"] == 1


def test_incremental_selection_reprocesses_same_size_rewrite_when_content_hash_differs(tmp_path) -> None:
    path = tmp_path / "1301_sample.txt"
    old_text = "1301,2026/01/20,1,2,1,2,100\n"
    new_text = "1301,2026/01/20,1,2,1,3,100\n"
    assert len(old_text) == len(new_text)
    path.write_text(old_text, encoding="utf-8")
    old_sha = ingest_txt._file_sha256(str(path))
    path.write_text(new_text, encoding="utf-8")
    os.utime(path, (2000.0, 2000.0))

    selection = ingest_txt._select_txt_files_for_ingest(
        [str(path)],
        {"1301_sample.txt": {"mtime": 1000.0, "size": path.stat().st_size, "sha256": old_sha}},
        incremental=True,
        now_ts=3000.0,
    )

    assert selection["files"] == [(str(path), 0)]
    assert selection["changed_files"] == [(str(path), 0)]
    assert selection["skipped_count"] == 0
    assert selection["content_hash_checks"] == 1
    assert selection["content_hash_skips"] == 0


def test_incremental_history_guard_rejects_tail_only_update() -> None:
    conn = duckdb.connect(":memory:")
    conn.execute(
        """
        CREATE TABLE daily_bars (
            code TEXT,
            date BIGINT,
            o DOUBLE,
            h DOUBLE,
            l DOUBLE,
            c DOUBLE,
            v DOUBLE,
            source TEXT
        )
        """
    )
    rows = [
        ("1306", 1_700_000_000 + idx, 100.0, 101.0, 99.0, 100.5, 1000.0, "pan")
        for idx in range(120)
    ]
    conn.executemany("INSERT INTO daily_bars VALUES (?, ?, ?, ?, ?, ?, ?, ?)", rows)

    incremental_daily = pd.DataFrame({"code": ["1306"], "date": [1_800_000_000]})

    try:
        ingest_txt._validate_incremental_history_integrity(conn, incremental_daily=incremental_daily)
    except RuntimeError as exc:
        assert "Incremental history validation failed" in str(exc)
        assert "1306" in str(exc)
    else:
        raise AssertionError("history guard did not reject tail-only incremental input")


def test_incremental_history_guard_allows_full_history_refresh() -> None:
    conn = duckdb.connect(":memory:")
    conn.execute(
        """
        CREATE TABLE daily_bars (
            code TEXT,
            date BIGINT,
            o DOUBLE,
            h DOUBLE,
            l DOUBLE,
            c DOUBLE,
            v DOUBLE,
            source TEXT
        )
        """
    )
    rows = [
        ("8729", 1_700_000_000 + idx, 100.0, 101.0, 99.0, 100.5, 1000.0, "pan")
        for idx in range(60)
    ]
    conn.executemany("INSERT INTO daily_bars VALUES (?, ?, ?, ?, ?, ?, ?, ?)", rows)

    incremental_daily = pd.DataFrame(
        {"code": ["8729"] * 60, "date": [1_700_000_000 + idx for idx in range(60)]}
    )

    ingest_txt._validate_incremental_history_integrity(conn, incremental_daily=incremental_daily)
