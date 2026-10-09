
from __future__ import annotations

import math
from datetime import datetime, timezone
from typing import Any, Iterable, Mapping

import requests

from fashion_resale_trends import fixtures
from fashion_resale_trends.config import Settings, sources_config
from fashion_resale_trends.keywords import normalize_keyword

_SEARCH_URL = "https://www.marktplaats.nl/lrp/api/search"

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json",
    "Accept-Language": "nl-NL,nl;q=0.9,en;q=0.8",
}

_MARKTPLAATS_BASE = "https://www.marktplaats.nl"


def extract_price_value(price_info: Mapping[str, Any] | None) -> float | None:
    
    if not price_info:
        return None
    cents = price_info.get("priceCents")
    if cents is None:
        return None
    try:
        cents_int = int(cents)
    except (TypeError, ValueError):
        return None
    if cents_int <= 0:
        return None
    return round(cents_int / 100.0, 2)


def extract_condition(listing: Mapping[str, Any]) -> str:
   
    for attr in listing.get("attributes") or []:
        if str(attr.get("key", "")).lower() == "condition":
            value = attr.get("value")
            if value:
                return str(value).strip()
    return ""


def parse_listing(keyword: str, listing: Mapping[str, Any], source_is_fixture: bool = False) -> dict[str, Any]:
    
    price_info = listing.get("priceInfo") or {}
    price_value = extract_price_value(price_info)

    vip_url = str(listing.get("vipUrl") or "")
    item_url = vip_url if vip_url.startswith("http") else f"{_MARKTPLAATS_BASE}{vip_url}"

    seller_info = listing.get("sellerInformation") or {}
    seller_name = str(seller_info.get("sellerName") or "")

    return {
        "keyword": normalize_keyword(keyword),
        "item_id": str(listing.get("itemId") or ""),
        "title": str(listing.get("title") or ""),
        "price_value": price_value,
        "currency": "EUR",
        "condition": extract_condition(listing),
        "price_type": str(price_info.get("priceType") or ""),
        "item_url": item_url,
        "seller_username": seller_name,
        "seller_feedback_score": 0,
        "seller_feedback_percentage": 0.0,
        "shipping_cost": 0.0,
        "marketplace_id": "MARKTPLAATS_NL",
        "collected_at": datetime.now(timezone.utc).isoformat(),
        "source": "marktplaats",
        "source_is_fixture": source_is_fixture,
    }


# Marktplaats top-level categories searched: Kleding | Dames, Kleding | Heren and
# Sieraden, Tassen en Uiterlijk. Without them, "robe" returns stage lights (ROBE brand)
# and "zara" returns 1943 stamps from the city of Zara.
_DEFAULT_CATEGORY_IDS = [621, 1776, 1826]


def _category_ids() -> list[int]:
    configured = sources_config().get("marktplaats", {}).get("category_ids")
    return [int(value) for value in configured] if configured else _DEFAULT_CATEGORY_IDS


def _fetch_keyword(keyword: str, limit: int) -> list[dict[str, Any]]:
    categories = _category_ids()
    per_category = max(1, math.ceil(limit / len(categories)))
    parsed: list[dict[str, Any]] = []
    seen: set[str] = set()
    for category_id in categories:
        response = requests.get(
            _SEARCH_URL,
            headers=_HEADERS,
            params={"query": keyword, "l1CategoryId": category_id, "limit": min(per_category * 2, 100)},
            timeout=30,
        )
        response.raise_for_status()
        listings = response.json().get("listings") or []
        rows = [parse_listing(keyword, item) for item in listings]
        rows = [row for row in rows if row["item_id"] not in seen]
        rows.sort(key=lambda row: row.get("price_value") is None)
        for row in rows[:per_category]:
            seen.add(row["item_id"])
            parsed.append(row)

    with_price = [row for row in parsed if row.get("price_value") is not None]
    without_price = [row for row in parsed if row.get("price_value") is None]
    prioritized = with_price[:limit]
    if len(prioritized) < limit:
        prioritized.extend(without_price[: limit - len(prioritized)])
    return prioritized


def discover_marktplaats_keywords(
    cfg: Settings,
    keywords: Iterable[str],
    limit_per_keyword: int = 20,
    discover_limit: int = 50,
) -> list[dict[str, Any]]:
    
    from fashion_resale_trends.keywords import discover_keywords_from_offers

    offers = fetch_marktplaats_offers(keywords, cfg, limit_per_keyword=limit_per_keyword)
    return discover_keywords_from_offers(
        offers,
        limit=discover_limit,
        discovery_source="marktplaats_keyword_discovery",
        marketplace="marktplaats",
    )


def fetch_marktplaats_offers(
    keywords: Iterable[str],
    cfg: Settings,
    limit_per_keyword: int = 50,
) -> list[dict[str, Any]]:
    
    selected = [normalize_keyword(str(k)) for k in keywords]
    selected = [k for k in selected if k]

    try:
        records: list[dict[str, Any]] = []
        for keyword in selected:
            records.extend(_fetch_keyword(keyword, limit_per_keyword))
        if records:
            return records
        raise RuntimeError("Marktplaats returned no listings.")
    except Exception:
        if not cfg.use_fixtures_if_source_fail:
            raise
        return fixtures.marktplaats_offers(selected)
