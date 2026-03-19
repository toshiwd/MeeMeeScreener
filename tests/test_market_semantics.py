from __future__ import annotations

from shared.market_semantics import is_confirmed_market_semantics


def test_display_only_overlay_is_rejected() -> None:
    assert (
        is_confirmed_market_semantics(
            confirmation_state="provisional",
            quality="provisional",
            display_only=True,
        )
        is False
    )


def test_confirmed_semantics_are_accepted() -> None:
    assert (
        is_confirmed_market_semantics(
            confirmation_state="confirmed",
            quality="high",
            display_only=False,
        )
        is True
    )

