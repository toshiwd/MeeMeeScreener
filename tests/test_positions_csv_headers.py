from app.backend.positions import _find_sbi_header_index


def test_find_sbi_header_index_detects_trade_date_and_code_header():
    rows = [
        ["SBI証券", "取引履歴"],
        ["CSV作成日", "2026/04/10"],
        [],
        ["約定日", "受渡日", "銘柄コード", "銘柄", "取引"],
        ["2026/04/09", "2026/04/11", "4385", "メルカリ", "現物買"],
    ]

    assert _find_sbi_header_index(rows) == 3
