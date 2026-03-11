from __future__ import annotations

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
