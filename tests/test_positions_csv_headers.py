from app.backend.positions import _find_sbi_header_index, parse_sbi_csv
from app.backend.trade_parser import TradeParser


TRADE_DATE = "\u7d04\u5b9a\u65e5"
SETTLE_DATE = "\u53d7\u6e21\u65e5"
CODE = "\u9298\u67c4\u30b3\u30fc\u30c9"
NAME = "\u9298\u67c4"
MARKET = "\u5e02\u5834"
ACCOUNT = "\u9810\u308a"
TRADE = "\u53d6\u5f15"
QUANTITY = "\u6570\u91cf"
PRICE = "\u5358\u4fa1"
FEE = "\u624b\u6570\u6599"
TAX = "\u7a0e\u91d1"
AMOUNT = "\u53d7\u6e21\u91d1\u984d"
BUY_OPEN = "\u4fe1\u7528\u65b0\u898f\u8cb7"


def test_find_sbi_header_index_detects_trade_date_and_code_header():
    rows = [
        ["SBI\u8a3c\u5238", "\u53d6\u5f15\u5c65\u6b74"],
        ["CSV\u4f5c\u6210\u65e5", "2026/04/10"],
        [],
        [TRADE_DATE, SETTLE_DATE, CODE, NAME, TRADE],
        ["2026/04/09", "2026/04/11", "4385", "\u30e1\u30eb\u30ab\u30ea", BUY_OPEN],
    ]

    assert _find_sbi_header_index(rows) == 3
    assert TradeParser.find_sbi_header_index(rows) == 3


def test_find_sbi_header_index_detects_header_near_top():
    rows = [
        [TRADE_DATE, SETTLE_DATE, CODE, NAME, TRADE],
        ["2026/04/09", "2026/04/11", "4385", "\u30e1\u30eb\u30ab\u30ea", BUY_OPEN],
    ]

    assert _find_sbi_header_index(rows) == 0
    assert TradeParser.find_sbi_header_index(rows) == 0


def test_parse_sbi_csv_accepts_normal_japanese_headers():
    rows = [
        [TRADE_DATE, SETTLE_DATE, CODE, NAME, MARKET, TRADE, ACCOUNT, QUANTITY, PRICE, FEE, TAX, AMOUNT],
        [
            "2026/04/09",
            "2026/04/11",
            "4385",
            "\u30e1\u30eb\u30ab\u30ea",
            "\u6771\u8a3c",
            BUY_OPEN,
            "\u7279\u5b9a",
            "100",
            "2500",
            "0",
            "0",
            "-250000",
        ],
    ]
    text = "\n".join(",".join(row) for row in rows)

    events, warnings = parse_sbi_csv(text.encode("cp932"))

    assert warnings == []
    assert len(events) == 1
    assert events[0].broker == "sbi"
    assert events[0].symbol == "4385"
