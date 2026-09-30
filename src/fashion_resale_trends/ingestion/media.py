from __future__ import annotations

import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from typing import Any

import requests
from bs4 import BeautifulSoup

from fashion_resale_trends import fixtures

_HEADERS = {"User-Agent": "fashion-resale-trends/1.0"}


def parse_media_rss(source: str, rss_url: str, limit: int = 40) -> list[dict[str, Any]]:
    
    response = requests.get(rss_url, timeout=30, headers=_HEADERS)
    response.raise_for_status()
    root = ET.fromstring(response.content)
    
    channel = root.find("channel")
    items = channel.findall("item") if channel is not None else root.findall("{http://www.w3.org/2005/Atom}entry")
    collected_at = datetime.now(timezone.utc).isoformat()
    records: list[dict[str, Any]] = []
    for item in items[:limit]:
        _t = item.find("title")
        title_el = _t if _t is not None else item.find("{http://www.w3.org/2005/Atom}title")
        _l = item.find("link")
        link_el = _l if _l is not None else item.find("{http://www.w3.org/2005/Atom}link")
        _p = item.find("pubDate")
        pub_el = _p if _p is not None else item.find("{http://www.w3.org/2005/Atom}published")
        title = (title_el.text or "").strip() if title_el is not None else ""
        # Atom <link> stocke l'URL dans l'attribut href, pas dans le texte
        link = (link_el.get("href") or link_el.text or "").strip() if link_el is not None else ""
        pub = (pub_el.text or "").strip() if pub_el is not None else None
        if len(title) < 8:
            continue
        records.append(
            {
                "source": source,
                "title": title,
                "url": link,
                "published_at": pub,
                "collected_at": collected_at,
                "source_is_fixture": False,
            }
        )
    return records


def parse_media_html(source: str, url: str, html: str, limit: int = 40) -> list[dict[str, Any]]:
    
    soup = BeautifulSoup(html, "html.parser")
    collected_at = datetime.now(timezone.utc).isoformat()
    records: list[dict[str, Any]] = []
    seen: set[str] = set()

    for link in soup.select("a[href]"):
        title = " ".join(link.get_text(" ", strip=True).split())
        href = str(link.get("href") or "")
        if len(title) < 8:
            continue
        lower_href = href.lower()
        lower_title = title.lower()
        if not any(marker in lower_href or marker in lower_title for marker in ("mode", "fashion", "tendance", "style")):
            continue
        absolute_url = href if href.startswith("http") else f"{url.rstrip('/')}/{href.lstrip('/')}"
        dedupe_key = f"{title}|{absolute_url}"
        if dedupe_key in seen:
            continue
        seen.add(dedupe_key)
        records.append(
            {
                "source": source,
                "title": title,
                "url": absolute_url,
                "published_at": None,
                "collected_at": collected_at,
                "source_is_fixture": False,
            }
        )
        if len(records) >= limit:
            break
    return records


def scrape_media(
    source: str,
    url: str,
    use_fixture_on_fail: bool = True,
    rss_url: str | None = None,
) -> list[dict[str, Any]]:
   
    if rss_url:
        try:
            records = parse_media_rss(source, rss_url)
            if records:
                return records
        except Exception:
            pass  

   
    try:
        response = requests.get(url, timeout=30, headers=_HEADERS)
        response.raise_for_status()
        records = parse_media_html(source, url, response.text)
        if records:
            return records
        raise RuntimeError(f"No media records parsed from {url}")
    except Exception:
        if not use_fixture_on_fail:
            raise
        return fixtures.media_articles(source)

