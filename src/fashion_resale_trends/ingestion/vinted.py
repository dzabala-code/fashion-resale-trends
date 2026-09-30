
from __future__ import annotations

import time
from datetime import datetime, timezone
from typing import Any, Iterable

import requests

from fashion_resale_trends import fixtures
from fashion_resale_trends.config import Settings
from fashion_resale_trends.keywords import normalize_keyword

_VINTED_BASE = "https://www.vinted.fr"
_CATALOG_URL = f"{_VINTED_BASE}/api/v2/catalog/items"

# User-Agent réaliste pour éviter les blocages anti-bot basiques
_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "fr-FR,fr;q=0.9,en;q=0.8",
    "Referer": "https://www.vinted.fr/",
}

# Vinted returns HTTP 429 when queried too quickly (especially across many keywords).
_REQUEST_DELAY_SECONDS = 2.5
_MAX_LIVE_KEYWORDS = 20
_MAX_REQUEST_RETRIES = 4


def _make_session() -> requests.Session:
    
    session = requests.Session()
    session.get(_VINTED_BASE, headers=_HEADERS, timeout=30)
    return session


def _parse_item(keyword: str, item: dict[str, Any]) -> dict[str, Any]:
    
    user = item.get("user") or {}

    # Prix
    price_raw = item.get("price") or item.get("total_item_price") or {}
    if isinstance(price_raw, dict):
        price_value = float(price_raw.get("amount") or price_raw.get("value") or 0)
        currency = str(price_raw.get("currency_code") or price_raw.get("currency") or "EUR")
    else:
        try:
            price_value = float(price_raw)
        except (TypeError, ValueError):
            price_value = 0.0
        currency = "EUR"

    
    url_raw = str(item.get("url") or "")
    item_url = url_raw if url_raw.startswith("http") else f"{_VINTED_BASE}{url_raw}"

    
    feedback_rep = float(user.get("feedback_reputation") or 0)

    return {
        "keyword": normalize_keyword(keyword),
        "item_id": str(item.get("id") or ""),
        "title": str(item.get("title") or ""),
        "brand": str(item.get("brand_title") or ""),
        "price_value": price_value,
        "currency": currency,
        "condition": str(item.get("status") or ""),
        "size": str(item.get("size_title") or ""),
        "item_url": item_url,
        "seller_username": str(user.get("login") or ""),
        "seller_feedback_percentage": round(feedback_rep * 100, 1),
        "collected_at": datetime.now(timezone.utc).isoformat(),
        "source": "vinted",
        "source_is_fixture": False,
    }


def _fetch_keyword(
    session: requests.Session,
    keyword: str,
    limit: int = 50,
) -> list[dict[str, Any]]:
    
    params = {
        "search_text": keyword,
        "per_page": min(limit, 96),
        "page": 1,
        "order": "relevance",
    }
    delay = _REQUEST_DELAY_SECONDS
    response: requests.Response | None = None
    for _ in range(_MAX_REQUEST_RETRIES):
        response = session.get(_CATALOG_URL, params=params, headers=_HEADERS, timeout=30)
        if response.status_code == 429:
            retry_after = response.headers.get("Retry-After")
            time.sleep(float(retry_after) if retry_after else delay)
            delay = min(delay * 2, 60)
            continue
        response.raise_for_status()
        items = response.json().get("items") or []
        return [_parse_item(keyword, item) for item in items[:limit]]
    assert response is not None
    response.raise_for_status()
    return []


def fetch_vinted_offers(
    keywords: Iterable[str],
    cfg: Settings,
    limit_per_keyword: int = 50,
) -> list[dict[str, Any]]:
   
    selected = [normalize_keyword(str(k)) for k in keywords]
    selected = [k for k in selected if k]
    selected = selected[:_MAX_LIVE_KEYWORDS]

    try:
        session = _make_session()
        records: list[dict[str, Any]] = []
        for i, keyword in enumerate(selected):
            try:
                records.extend(_fetch_keyword(session, keyword, limit_per_keyword))
            except requests.HTTPError:
                # Skip a throttled keyword but keep offers collected so far.
                continue
            if i < len(selected) - 1:
                time.sleep(_REQUEST_DELAY_SECONDS)
        if records:
            return records
        raise RuntimeError("Vinted API returned no records.")
    except Exception:
        if not cfg.use_fixtures_if_source_fail:
            raise
        return fixtures.vinted_offers(selected, limit_per_keyword=limit_per_keyword)
