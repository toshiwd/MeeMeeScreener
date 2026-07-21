from __future__ import annotations

import json

import pandas as pd

from app.backend.ingest_txt import filter_delisted_daily_rows, load_delisted_codes
from scripts.update_delisted_codes import (
    effective_codes,
    merge_registry,
    parse_jpx_delisted_html,
    remove_codes_from_code_list,
)


def test_load_delisted_codes_reads_registry(tmp_path):
    registry = tmp_path / "delisted_codes.json"
    registry.write_text(
        json.dumps(
            {
                "schema_version": "meemee_delisted_codes_v1",
                "codes": {
                    "5727": {
                        "delisted_on": "2026-05-28",
                        "reason": "share_exchange",
                        "source": "company_ir",
                    }
                },
            }
        ),
        encoding="utf-8",
    )

    assert load_delisted_codes(registry) == {"5727"}


def test_filter_delisted_daily_rows_removes_registered_code_only():
    daily = pd.DataFrame(
        [
            {"code": "5727", "date": 1779840000, "o": 1, "h": 2, "l": 1, "c": 2, "v": 100},
            {"code": "5016", "date": 1783382400, "o": 3, "h": 4, "l": 3, "c": 4, "v": 200},
        ]
    )
    name_map = {"5727": "東邦チタニウム", "5016": "JX金属"}

    filtered_daily, filtered_name_map, stats = filter_delisted_daily_rows(
        daily,
        name_map,
        {"5727"},
    )

    assert filtered_daily["code"].tolist() == ["5016"]
    assert filtered_name_map == {"5016": "JX金属"}
    assert stats == {"removed_rows": 1, "removed_codes": 1}


def test_filter_delisted_daily_rows_noops_without_match():
    daily = pd.DataFrame(
        [{"code": "5016", "date": 1783382400, "o": 3, "h": 4, "l": 3, "c": 4, "v": 200}]
    )
    name_map = {"5016": "JX金属"}

    filtered_daily, filtered_name_map, stats = filter_delisted_daily_rows(
        daily,
        name_map,
        {"5727"},
    )

    assert filtered_daily.equals(daily)
    assert filtered_name_map == name_map
    assert stats == {"removed_rows": 0, "removed_codes": 0}


def test_parse_jpx_delisted_html_extracts_rows():
    html = """
    <table>
      <tr><th>上場廃止日</th><th>銘柄名</th><th>コード</th><th>市場区分</th><th>上場廃止理由</th></tr>
      <tr><td>2026/07/09</td><td>ウェーブロックホールディングス（株）</td><td>7940</td><td>スタンダード</td><td>他社による買収</td></tr>
    </table>
    """

    assert parse_jpx_delisted_html(html) == [
        {
            "code": "7940",
            "name": "ウェーブロックホールディングス（株）",
            "delisted_on": "2026-07-09",
            "market": "スタンダード",
            "reason": "他社による買収",
            "source": "JPX",
        }
    ]


def test_merge_registry_preserves_existing_and_adds_jpx_rows():
    existing = {
        "schema_version": "meemee_delisted_codes_v1",
        "codes": {
            "5727": {
                "delisted_on": "2026-05-28",
                "reason": "share_exchange",
                "source": "company_ir",
            }
        },
    }

    merged = merge_registry(
        existing,
        [
            {
                "code": "7940",
                "name": "ウェーブロックホールディングス（株）",
                "delisted_on": "2026-07-09",
                "market": "スタンダード",
                "reason": "他社による買収",
                "source": "JPX",
            }
        ],
    )

    assert set(merged["codes"]) == {"5727", "7940"}
    assert merged["codes"]["5727"]["reason"] == "share_exchange"
    assert merged["codes"]["7940"]["source"] == "JPX"


def test_effective_codes_excludes_future_delistings():
    registry = {
        "codes": {
            "7940": {"delisted_on": "2026-07-09"},
            "5903": {"delisted_on": "2026-07-22"},
        }
    }

    assert effective_codes(registry, as_of=pd.Timestamp("2026-07-09").date()) == {"7940"}


def test_remove_codes_from_code_list_deletes_effective_codes(tmp_path):
    code_list = tmp_path / "code.txt"
    code_list.write_text("1301\n7940\n5903\n", encoding="utf-8")

    result = remove_codes_from_code_list(code_list, {"7940"}, dry_run=False)

    assert code_list.read_text(encoding="utf-8").splitlines() == ["1301", "5903"]
    assert result["removed_codes"] == ["7940"]
