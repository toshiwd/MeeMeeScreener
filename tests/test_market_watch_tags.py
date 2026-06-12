from app.backend.services.market_watch_tags import (
    NIKKEI_225_CODES,
    NIKKEI_225_TAG,
    build_market_watch_tags_by_code,
    get_market_watch_tags,
)


def test_nikkei_225_registry_contains_current_constituent_count() -> None:
    assert len(NIKKEI_225_CODES) == 225


def test_build_market_watch_tags_by_code_marks_nikkei_constituents() -> None:
    tags_by_code = build_market_watch_tags_by_code(["285A", "8035", "1306"])

    assert tags_by_code["285A"] == [NIKKEI_225_TAG]
    assert tags_by_code["8035"] == [NIKKEI_225_TAG]
    assert "1306" not in tags_by_code
    assert get_market_watch_tags("543A") == [NIKKEI_225_TAG]
