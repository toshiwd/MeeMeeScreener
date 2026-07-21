from app.backend.core.yahoo_history_rows import _code_to_symbol


def test_code_to_symbol_accepts_numeric_and_jpx_alphanumeric_codes():
    assert _code_to_symbol("5803") == "5803.T"
    assert _code_to_symbol("285a") == "285A.T"
    assert _code_to_symbol("543A") == "543A.T"


def test_code_to_symbol_rejects_non_jpx_codes():
    assert _code_to_symbol("AAPL") is None
    assert _code_to_symbol("12A4") is None
