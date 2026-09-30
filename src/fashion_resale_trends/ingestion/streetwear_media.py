"""Ingestion streetwear culture — Hypebeast + Highsnobiety RSS feeds."""
from __future__ import annotations

import hashlib
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from typing import Any, Iterable
from urllib.parse import urlparse

import requests

from fashion_resale_trends import fixtures
from fashion_resale_trends.config import Settings, sources_config
from fashion_resale_trends.keywords import normalize_keyword

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "application/rss+xml, application/xml, text/xml",
}

_DEFAULT_FEEDS = {
    "hypebeast": "https://hypebeast.com/feed",
    "highsnobiety": "https://www.highsnobiety.com/feed/",
}


def _configured_feeds() -> list[tuple[str, str]]:
    configured = sources_config().get("streetwear_media", {})
    feeds: list[tuple[str, str]] = []
    for name, info in configured.items():
        if not isinstance(info, dict):
            continue
        url = str(info.get("rss_url") or "").strip()
        if url:
            feeds.append((name, url))
    if feeds:
        return feeds
    return list(_DEFAULT_FEEDS.items())


def parse_article(keyword: str, article: dict[str, Any], source_is_fixture: bool = False) -> dict[str, Any]:
    return {
        "keyword": normalize_keyword(keyword),
        "article_id": article.get("id"),
        "title": article.get("title") or article.get("description") or "",
        "description": article.get("description") or "",
        "link": article.get("link") or "",
        "feed_source": article.get("feed_source") or "",
        "collected_at": datetime.now(timezone.utc).isoformat(),
        "source": "streetwear_media",
        "source_is_fixture": source_is_fixture,
    }


def _feed_source_name(url: str) -> str:
    host = urlparse(url).netloc.lower()
    if "hypebeast" in host:
        return "hypebeast"
    if "highsnobiety" in host:
        return "highsnobiety"
    return host.removeprefix("www.")


def _parse_rss_item(item: ET.Element, feed_source: str) -> dict[str, Any]:
    title = item.findtext("title") or ""
    link = item.findtext("link") or ""
    description = item.findtext("description") or ""
    guid = item.findtext("guid") or link
    article_id = hashlib.sha1(guid.encode()).hexdigest()[:16]  # noqa: S324 — id only

    return {
        "id": article_id,
        "title": title,
        "description": description,
        "link": link,
        "feed_source": feed_source,
    }


def _fetch_feed(feed_source: str, url: str) -> list[dict[str, Any]]:
    response = requests.get(url, headers=_HEADERS, timeout=30)
    response.raise_for_status()
    root = ET.fromstring(response.text)
    channel = root.find("channel")
    items = channel.findall("item") if channel is not None else root.findall("item")
    return [_parse_rss_item(item, feed_source) for item in items]


def _fetch_all_feeds() -> list[dict[str, Any]]:
    all_articles: list[dict[str, Any]] = []
    errors: list[Exception] = []
    for feed_source, url in _configured_feeds():
        try:
            all_articles.extend(_fetch_feed(feed_source, url))
        except Exception as exc:
            errors.append(exc)
    if not all_articles and errors:
        raise errors[0]
    return all_articles


def fetch_streetwear_articles(
    keywords: Iterable[str],
    cfg: Settings,
    limit_per_query: int = 25,
) -> list[dict[str, Any]]:
    """Return Hypebeast / Highsnobiety articles matching the given keywords."""
    selected = [normalize_keyword(k) for k in keywords if normalize_keyword(str(k))]

    try:
        articles = _fetch_all_feeds()
        if not articles:
            raise RuntimeError("Hypebeast/Highsnobiety returned no articles.")
        records: list[dict[str, Any]] = []
        for keyword in selected:
            kw_lower = keyword.lower()
            matched = [
                art for art in articles
                if kw_lower in art.get("title", "").lower()
                or kw_lower in art.get("description", "").lower()
            ]
            if not matched:
                words = [w for w in kw_lower.split() if len(w) > 3]
                matched = [
                    art for art in articles
                    if any(w in art.get("title", "").lower() for w in words)
                ]
            if not matched:
                matched = articles
            for art in matched[:limit_per_query]:
                records.append(parse_article(keyword, art))
        return records
    except Exception:
        if not cfg.use_fixtures_if_source_fail:
            raise
        return fixtures.streetwear_media_articles(selected)
