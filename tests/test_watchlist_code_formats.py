from pathlib import Path

from app.backend.core.code_ops import normalize_code_txt
from app.backend.services.watchlist import load_watchlist_codes, normalize_watch_code


def test_normalize_watch_code_accepts_three_digit_plus_letter_codes() -> None:
    assert normalize_watch_code("285A") == "285A"
    assert normalize_watch_code("543a") == "543A"
    assert normalize_watch_code(" ５４３ａ ") == "543A"


def test_normalize_code_txt_keeps_three_digit_plus_letter_codes(tmp_path: Path) -> None:
    path = tmp_path / "code.txt"
    path.write_text("543a\n285A\n1306\n# memo\n", encoding="utf-8")

    changed = normalize_code_txt(str(path))

    assert changed is True
    assert load_watchlist_codes(str(path)) == ["1306", "285A", "543A"]
