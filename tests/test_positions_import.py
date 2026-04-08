from __future__ import annotations

import app.backend.positions as positions


def test_rakuten_header_detection_skips_preamble():
    header_row = [
        positions.RAKUTEN_HASH_KEYS[0],
        positions.RAKUTEN_HASH_KEYS[1],
        positions.RAKUTEN_HASH_KEYS[2],
        "dummy",
    ]
    rows = [
        ["この行は前文です"],
        header_row,
        ["2026/04/03", "2026/04/06", "8086", "ニプロ", "東証", "特定", "買付", "100", "1500", "150000", "0", "0"],
    ]

    assert positions._find_rakuten_header_index(rows) == 1


def test_sbi_header_detection_skips_preamble():
    header_row = [
        "邏・ｮ壽律",
        "驫俶氛繧ｳ繝ｼ繝・",
        "謨ｰ驥擾ｼｻ譬ｪ・ｽ",
        "蜿門ｼ・",
    ]
    rows = [
        ["取引履歴の前文"],
        header_row,
        ["2026/04/03", "8086", "100", "買付"],
    ]

    assert positions._find_sbi_header_index(rows) == 1


def test_parse_rakuten_csv_uses_detected_header_row(monkeypatch):
    header_row = [
        positions.RAKUTEN_HASH_KEYS[0],
        positions.RAKUTEN_HASH_KEYS[1],
        positions.RAKUTEN_HASH_KEYS[2],
        "dummy",
    ]
    csv_text = "\n".join(
        [
            "この行は前文です",
            ",".join(header_row),
            "2026/04/03,2026/04/06,8086,ニプロ,東証,特定,買付,100,1500,150000,0,0",
        ]
    )

    def fake_parse(rows_all, encoding_used=""):
        assert rows_all[0] == header_row
        return {
            "rows": [
                {
                    "broker": "RAKUTEN",
                    "tradeDate": "2026-04-03",
                    "code": "8086",
                    "kind": "BUY_OPEN",
                    "position_action": "SPOT_BUY",
                    "qty": 100,
                    "price": 1500,
                    "memo": "買付",
                    "buySell": "買",
                    "side": "buy",
                    "row_hash": "hash-1",
                }
            ],
            "warnings": [{"message": "parsed"}],
        }

    monkeypatch.setattr(positions.TradeParser, "parse_rakuten_rows", fake_parse)

    events, warnings = positions.parse_rakuten_csv(csv_text.encode("cp932"))

    assert len(events) == 1
    assert events[0].symbol == "8086"
    assert warnings == ["parsed"]


def test_parse_sbi_csv_uses_detected_header_row(monkeypatch):
    header_row = [
        "邏・ｮ壽律",
        "驫俶氛繧ｳ繝ｼ繝・",
        "謨ｰ驥擾ｼｻ譬ｪ・ｽ",
        "蜿門ｼ・",
    ]
    csv_text = "\n".join(
        [
            "取引履歴の前文",
            ",".join(header_row),
            "2026/04/03,8086,100,買付",
        ]
    )

    def fake_parse(rows_all, encoding_used=""):
        assert rows_all[0] == header_row
        return {
            "rows": [
                {
                    "broker": "SBI",
                    "tradeDate": "2026-04-03",
                    "code": "8086",
                    "kind": "BUY_OPEN",
                    "qty": 100,
                    "price": 1500,
                    "memo": "買付",
                    "side": "buy",
                    "action": "open",
                    "row_hash": "hash-2",
                }
            ],
            "warnings": [{"message": "parsed"}],
        }

    monkeypatch.setattr(positions.TradeParser, "parse_sbi_rows", fake_parse)

    events, warnings = positions.parse_sbi_csv(csv_text.encode("cp932"))

    assert len(events) == 1
    assert events[0].symbol == "8086"
    assert warnings == ["parsed"]
