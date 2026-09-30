from __future__ import annotations

import base64
from datetime import datetime, timezone
from typing import Any, Iterable, Mapping

import requests

from fashion_resale_trends import fixtures
from fashion_resale_trends.config import Settings
from fashion_resale_trends.keywords import normalize_keyword, useful_keyword

PRODUCTION_TOKEN_URL = "https://api.ebay.com/identity/v1/oauth2/token"
PRODUCTION_SEARCH_URL = "https://api.ebay.com/buy/browse/v1/item_summary/search"
SANDBOX_TOKEN_URL = "https://api.sandbox.ebay.com/identity/v1/oauth2/token"
SANDBOX_SEARCH_URL = "https://api.sandbox.ebay.com/buy/browse/v1/item_summary/search"


def ebay_urls(env: str) -> tuple[str, str]:
    if env.strip().lower() == "sandbox":
        return SANDBOX_TOKEN_URL, SANDBOX_SEARCH_URL
    return PRODUCTION_TOKEN_URL, PRODUCTION_SEARCH_URL


def access_token(cfg: Settings) -> str:
    if not cfg.ebay_client_id or not cfg.ebay_client_secret:
        raise RuntimeError("Missing EBAY_CLIENT_ID or EBAY_CLIENT_SECRET.")
    token_url, _ = ebay_urls(cfg.ebay_env)
    credentials = base64.b64encode(f"{cfg.ebay_client_id}:{cfg.ebay_client_secret}".encode()).decode()
    response = requests.post(
        token_url,
        headers={
            "Authorization": f"Basic {credentials}",
            "Content-Type": "application/x-www-form-urlencoded",
        },
        data={"grant_type": "client_credentials", "scope": "https://api.ebay.com/oauth/api_scope"},
        timeout=30,
    )
    response.raise_for_status()
    return str(response.json()["access_token"])


def parse_item(keyword: str, item: Mapping[str, Any], source_is_fixture: bool = False) -> dict[str, Any]:
    price = item.get("price") or {}
    seller = item.get("seller") or {}
    shipping_options = item.get("shippingOptions") or []
    shipping_cost = None
    if shipping_options:
        shipping_cost = ((shipping_options[0] or {}).get("shippingCost") or {}).get("value")
    return {
        "keyword": normalize_keyword(keyword),
        "item_id": item.get("itemId"),
        "title": item.get("title"),
        "price_value": float(price["value"]) if price.get("value") not in (None, "") else None,
        "currency": price.get("currency"),
        "condition": item.get("condition"),
        "item_url": item.get("itemWebUrl") or item.get("itemHref"),
        "seller_username": seller.get("username"),
        "seller_feedback_score": int(seller.get("feedbackScore") or 0),
        "seller_feedback_percentage": float(seller.get("feedbackPercentage") or 0.0),
        "shipping_cost": float(shipping_cost) if shipping_cost not in (None, "") else 0.0,
        "marketplace_id": item.get("listingMarketplaceId"),
        "collected_at": datetime.now(timezone.utc).isoformat(),
        "source": "ebay",
        "source_is_fixture": source_is_fixture,
    }


def fetch_items(keyword: str, token: str, cfg: Settings, limit: int = 50) -> list[dict[str, Any]]:
    _, search_url = ebay_urls(cfg.ebay_env)
    response = requests.get(
        search_url,
        headers={
            "Authorization": f"Bearer {token}",
            "X-EBAY-C-MARKETPLACE-ID": cfg.ebay_marketplace_id,
        },
        params={"q": keyword, "limit": min(limit, 200)},
        timeout=30,
    )
    response.raise_for_status()
    return [parse_item(keyword, item) for item in response.json().get("itemSummaries", [])]


def fetch_offers(keywords: Iterable[str], cfg: Settings, limit_per_keyword: int = 30) -> list[dict[str, Any]]:
    selected = [normalize_keyword(keyword) for keyword in keywords if useful_keyword(str(keyword))]
    try:
        token = access_token(cfg)
        records: list[dict[str, Any]] = []
        for keyword in selected:
            records.extend(fetch_items(keyword, token, cfg, limit_per_keyword))
        if records:
            return records
        raise RuntimeError("eBay returned no records.")
    except Exception:
        if not cfg.use_fixtures_if_source_fail:
            raise
        return fixtures.ebay_offers(selected)

