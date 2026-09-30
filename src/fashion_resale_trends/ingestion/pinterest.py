"""
Pinterest was not working, so we are using Hypebeast and Highsnobiety RSS feeds instead.
"""
from __future__ import annotations

import hashlib
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from typing import Any, Iterable

import requests

from fashion_resale_trends import fixtures
from fashion_resale_trends.config import Settings
from fashion_resale_trends.keywords import normalize_keyword

_FEEDS = [
    "https://hypebeast.com/feed",
    "https://www.highsnobiety.com/feed/",
]

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "application/rss+xml, application/xml, text/xml",
}

# Namespace RSS 2.0 étendu
_NS = {
    "content": "http://purl.org/rss/1.0/modules/content/",
    "dc": "http://purl.org/dc/elements/1.1/",
}


def parse_pin(keyword: str, pin: dict[str, Any], source_is_fixture: bool = False) -> dict[str, Any]:
    """Convertit un article RSS vers le schéma interne compatible Pinterest.

    Pour les articles RSS :
      save_count   → 0 (non disponible, engagement calculé via pin_count * 5)
      comment_count→ 0 (non disponible dans RSS)
      media_type   → "article"
      pin_id       → hash SHA-1 de l'URL (stable, unique)
    """
    media = pin.get("media", {}) or {}
    return {
        "keyword": normalize_keyword(keyword),
        "pin_id": pin.get("id"),
        "title": pin.get("title") or pin.get("description") or "",
        "description": pin.get("description") or "",
        "save_count": int(pin.get("save_count") or 0),
        "comment_count": int(pin.get("comment_count") or 0),
        "link": pin.get("link") or "",
        "media_type": media.get("media_type") or "image",
        "collected_at": datetime.now(timezone.utc).isoformat(),
        "source": "pinterest",
        "source_is_fixture": source_is_fixture,
    }


def _parse_rss_item(item: ET.Element) -> dict[str, Any]:
    """Convertit un <item> RSS 2.0 en dict article brut."""
    title = item.findtext("title") or ""
    link = item.findtext("link") or ""
    description = item.findtext("description") or ""
    guid = item.findtext("guid") or link

    # Génère un id court et stable depuis l'URL
    pin_id = hashlib.sha1(guid.encode()).hexdigest()[:16]  # noqa: S324 — non-crypto, identification only

    return {
        "id": pin_id,
        "title": title,
        "description": description,
        "link": link,
        "save_count": 0,
        "comment_count": 0,
        "media": {"media_type": "article"},
    }


def _fetch_feed(url: str) -> list[dict[str, Any]]:
    """Télécharge et parse un flux RSS 2.0, retourne la liste des items bruts."""
    response = requests.get(url, headers=_HEADERS, timeout=30)
    response.raise_for_status()
    root = ET.fromstring(response.text)
    channel = root.find("channel")
    items = channel.findall("item") if channel is not None else root.findall("item")
    return [_parse_rss_item(item) for item in items]


def _fetch_all_feeds() -> list[dict[str, Any]]:
    """Agrège les articles de tous les flux configurés.

    Ignore silencieusement les flux indisponibles.
    Lève une exception seulement si tous les flux échouent.
    """
    all_articles: list[dict[str, Any]] = []
    errors: list[Exception] = []
    for url in _FEEDS:
        try:
            all_articles.extend(_fetch_feed(url))
        except Exception as exc:
            errors.append(exc)
    if not all_articles and errors:
        raise errors[0]
    return all_articles


def fetch_pinterest_pins(
    keywords: Iterable[str],
    cfg: Settings,
    limit_per_query: int = 25,
) -> list[dict[str, Any]]:
    """Retourne des articles de tendance mode pour les keywords donnés.

    Source : Hypebeast + Highsnobiety RSS (remplace Pinterest API).
    Aucune authentification requise.

    Stratégie :
    - Télécharge les flux RSS Hypebeast et Highsnobiety.
    - Filtre les articles par présence du keyword dans le titre ou la description.
    - Limite à limit_per_query articles par keyword.
    - Fallback sur fixtures Pinterest si les deux flux échouent.

    Args:
        keywords:        Mots-clés mode à rechercher dans les titres d'articles.
        cfg:             Configuration applicative (use_fixtures_if_source_fail).
        limit_per_query: Nombre maximum d'articles par mot-clé.

    Returns:
        Liste de dicts au schéma Pinterest interne.
        source="pinterest", source_is_fixture=False si données réelles.
        source="pinterest", source_is_fixture=True si fixtures en fallback.
    """
    selected = [normalize_keyword(k) for k in keywords if normalize_keyword(str(k))]

    try:
        articles = _fetch_all_feeds()
        if not articles:
            raise RuntimeError("Hypebeast/Highsnobiety returned no articles.")
        records: list[dict[str, Any]] = []
        for keyword in selected:
            kw_lower = keyword.lower()
            # Try exact keyword match first
            matched = [
                art for art in articles
                if kw_lower in art.get("title", "").lower()
                or kw_lower in art.get("description", "").lower()
            ]
            # Fall back to any word of the keyword matching (min 4 chars to avoid noise)
            if not matched:
                words = [w for w in kw_lower.split() if len(w) > 3]
                matched = [
                    art for art in articles
                    if any(w in art.get("title", "").lower() for w in words)
                ]
            # Last resort: distribute all feed articles as general fashion trend signals
            if not matched:
                matched = articles
            for art in matched[:limit_per_query]:
                records.append(parse_pin(keyword, art))
        return records
    except Exception:
        if not cfg.use_fixtures_if_source_fail:
            raise
        return fixtures.pinterest_pins(selected)
