from __future__ import annotations

from typing import Any


MARKET_BASKET_DEFS: list[dict[str, Any]] = [
    {
        "theme_id": "nikkei_entertainment_content",
        "name": "Nikkei Entertainment Content",
        "basket_type": "official_index",
        "source": "Nikkei Japan Entertainment Content Stock Index",
        "codes": {
            "2432", "3632", "3635", "3659", "3668", "3765", "4816", "6460",
            "6758", "7832", "7867", "7974", "8136", "9468", "9601", "9602",
            "9605", "9684", "9697", "9766",
        },
        "sector_codes": set(),
        "keywords": ("entertainment", "content", "game", "anime", "movie"),
    },
    {
        "theme_id": "semiconductor_core",
        "name": "Semiconductor Core",
        "basket_type": "proxy_index",
        "source": "MeeMee representative basket",
        "codes": {"3436", "4063", "6146", "6266", "6323", "6525", "6723", "6857", "6920", "6963", "7735", "8035"},
        "sector_codes": {"3650", "3600", "3750"},
        "keywords": ("semiconductor", "electronics", "silicon"),
    },
    {
        "theme_id": "game_content",
        "name": "Game Content",
        "basket_type": "proxy_index",
        "source": "MeeMee entertainment sub-basket",
        "codes": {"2432", "3632", "3635", "3659", "3668", "3765", "6460", "7832", "7974", "9684", "9697", "9766"},
        "sector_codes": set(),
        "keywords": ("game", "gaming"),
    },
    {
        "theme_id": "anime_film_content",
        "name": "Anime Film Content",
        "basket_type": "proxy_index",
        "source": "MeeMee entertainment sub-basket",
        "codes": {"4816", "6758", "9468", "9601", "9602", "9605"},
        "sector_codes": set(),
        "keywords": ("anime", "film", "movie", "content"),
    },
    {
        "theme_id": "leisure_live_entertainment",
        "name": "Leisure Live Entertainment",
        "basket_type": "proxy_index",
        "source": "MeeMee entertainment sub-basket",
        "codes": {"4301", "4680", "4681", "6191", "7803", "7867", "8136", "9616"},
        "sector_codes": set(),
        "keywords": ("leisure", "live", "toy", "character"),
    },
    {
        "theme_id": "bank_rate_sensitive",
        "name": "Bank Rate Sensitive",
        "basket_type": "proxy_index",
        "source": "MeeMee representative basket",
        "codes": {"7182", "8306", "8308", "8309", "8316", "8411", "8354", "7167", "8377"},
        "sector_codes": {"7050"},
        "keywords": ("bank", "financial"),
    },
    {
        "theme_id": "trading_house_resources",
        "name": "Trading House Resources",
        "basket_type": "proxy_index",
        "source": "MeeMee representative basket",
        "codes": {"1605", "5020", "8001", "8002", "8015", "8031", "8053", "8058"},
        "sector_codes": {"3300", "3450", "6050"},
        "keywords": ("trading", "resource", "commodity"),
    },
    {
        "theme_id": "defense_heavy_industry",
        "name": "Defense Heavy Industry",
        "basket_type": "proxy_index",
        "source": "MeeMee representative basket",
        "codes": {"6208", "7011", "7012", "7013", "7224", "7721"},
        "sector_codes": {"3600", "3700"},
        "keywords": ("defense", "heavy industry"),
    },
    {
        "theme_id": "power_grid_utilities",
        "name": "Power Grid Utilities",
        "basket_type": "proxy_index",
        "source": "MeeMee representative basket",
        "codes": {"9501", "9502", "9503", "9504", "9505", "9506", "9507", "9508", "9509", "9511", "9513"},
        "sector_codes": {"4050"},
        "keywords": ("power", "utility", "grid"),
    },
    {
        "theme_id": "autos_mobility",
        "name": "Autos Mobility",
        "basket_type": "proxy_index",
        "source": "MeeMee representative basket",
        "codes": {"5101", "5105", "5108", "6902", "7201", "7203", "7267", "7269"},
        "sector_codes": {"3350", "3700"},
        "keywords": ("auto", "mobility", "tire"),
    },
    {
        "theme_id": "inbound_transport_leisure",
        "name": "Inbound Transport Leisure",
        "basket_type": "proxy_index",
        "source": "MeeMee representative basket",
        "codes": {"3099", "4661", "8233", "9001", "9005", "9020", "9021", "9022", "9041", "9201", "9202", "9603", "9706"},
        "sector_codes": {"5050", "5100", "5150", "5200", "6100"},
        "keywords": ("inbound", "transport", "leisure"),
    },
    {
        "theme_id": "healthcare_defensive",
        "name": "Healthcare Defensive",
        "basket_type": "proxy_index",
        "source": "MeeMee representative basket",
        "codes": {"2413", "4502", "4503", "4506", "4507", "4519", "4523", "4543", "4568", "6869", "7741"},
        "sector_codes": {"3250", "3750"},
        "keywords": ("healthcare", "pharma", "medical"),
    },
    {
        "theme_id": "ai_datacenter_infra",
        "name": "AI Datacenter Infra",
        "basket_type": "proxy_index",
        "source": "MeeMee representative basket",
        "codes": {"3778", "3856", "4816", "5574", "5801", "5802", "5803", "6701", "6702", "6758", "9432", "9433"},
        "sector_codes": {"3650", "5250"},
        "keywords": ("ai", "datacenter", "cloud", "fiber"),
    },
]


def get_market_basket_defs() -> list[dict[str, Any]]:
    def priority(basket: dict[str, Any]) -> tuple[int, str]:
        basket_id = str(basket.get("theme_id") or "")
        if basket_id in {"game_content", "anime_film_content", "leisure_live_entertainment"}:
            return (0, basket_id)
        if basket_id == "nikkei_entertainment_content":
            return (1, basket_id)
        if basket_id == "semiconductor_core":
            return (2, basket_id)
        return (3, basket_id)

    return [
        {
            **basket,
            "codes": set(basket.get("codes") or set()),
            "sector_codes": set(basket.get("sector_codes") or set()),
            "keywords": tuple(basket.get("keywords") or ()),
        }
        for basket in sorted(MARKET_BASKET_DEFS, key=priority)
    ]


def market_basket_catalog() -> list[dict[str, Any]]:
    return [
        {
            "themeId": str(basket["theme_id"]),
            "name": str(basket["name"]),
            "basketType": str(basket["basket_type"]),
            "source": str(basket["source"]),
            "codes": sorted(str(code) for code in basket.get("codes") or set()),
            "sectorCodes": sorted(str(code) for code in basket.get("sector_codes") or set()),
        }
        for basket in MARKET_BASKET_DEFS
    ]
