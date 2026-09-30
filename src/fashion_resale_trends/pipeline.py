from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from typing import Iterable

from fashion_resale_trends.config import settings, sources_config
from fashion_resale_trends.config import seed_keywords
from fashion_resale_trends.ingestion.ebay import fetch_offers
from fashion_resale_trends.ingestion.marktplaats import discover_marktplaats_keywords, fetch_marktplaats_offers
from fashion_resale_trends.ingestion.google_trends import fetch_google_trends
from fashion_resale_trends.ingestion.media import scrape_media
from fashion_resale_trends.ingestion.streetwear_media import fetch_streetwear_articles
from fashion_resale_trends.ingestion.reddit import fetch_reddit_posts
from fashion_resale_trends.ingestion.vinted import fetch_vinted_offers
from fashion_resale_trends.keywords import extract_keywords
from fashion_resale_trends.storage import ObjectStore, utc_run_id


def write_records(layer: str, group: str, entity: str, filename: str, records: Iterable[dict]) -> None:
    store = ObjectStore()
    run_id = utc_run_id()
    ref = store.dataset_file(layer, group, entity, run_id, filename)
    count = store.write_jsonl(ref, records)
    print(json.dumps({"event": "write_records", "count": count, "uri": ref.uri, "local_path": str(ref.local_path)}))


def latest_keywords(limit: int = 50) -> list[str]:
    store = ObjectStore()
    records = store.read_jsonl("raw", "discovery", "extracted_keywords")
    if not records:
        return seed_keywords()[:limit]
    return [str(row["keyword"]) for row in records[:limit]]


def task_scrape_vogue() -> None:
    cfg = settings()
    source = sources_config().get("media", {}).get("vogue", {})
    records = scrape_media("vogue", source.get("url", "https://www.vogue.fr/mode"), cfg.use_fixtures_if_source_fail)
    write_records("raw", "media", "vogue", "articles.jsonl", records)


def task_scrape_elle() -> None:
    cfg = settings()
    source = sources_config().get("media", {}).get("elle", {})
    records = scrape_media("elle", source.get("url", "https://www.elle.fr/Mode"), cfg.use_fixtures_if_source_fail)
    write_records("raw", "media", "elle", "articles.jsonl", records)


def task_scrape_all_media() -> None:
    
    cfg = settings()
    media_cfg = sources_config().get("media", {})
    for source_name, source_info in media_cfg.items():
        url = source_info.get("url", "")
        if not url:
            continue
        rss_url = source_info.get("rss_url")
        try:
            records = scrape_media(
                source_name, url, cfg.use_fixtures_if_source_fail, rss_url=rss_url
            )
            write_records("raw", "media", source_name, "articles.jsonl", records)
        except Exception as exc:
            print(f"[media/{source_name}] SKIP — {exc}")


def task_discover_marketplace_keywords() -> None:
    """Discover fashion keywords from live marketplace listings (Marktplaats in sandbox)."""
    cfg = settings()
    seeds = seed_keywords()[:20]
    if cfg.ebay_env != "sandbox":
        from fashion_resale_trends.keywords import discover_keywords_from_offers

        offers = list(fetch_offers(seeds, cfg, limit_per_keyword=20))
        records = discover_keywords_from_offers(
            offers,
            discovery_source="ebay_keyword_discovery",
            marketplace="ebay",
        )
    else:
        records = discover_marktplaats_keywords(cfg, seeds, limit_per_keyword=20)
    write_records("raw", "discovery", "marketplace_keywords", "keywords.jsonl", records)


def task_extract_keywords() -> None:
    store = ObjectStore()
    media_cfg = sources_config().get("media", {})
    media_records: list[dict] = []
    for source_name in media_cfg:
        media_records.extend(store.read_jsonl("raw", "media", source_name))
    if not media_records:
        media_records = store.read_jsonl("raw", "media", "vogue") + store.read_jsonl("raw", "media", "elle")
    marketplace_keyword_records = store.read_jsonl("raw", "discovery", "marketplace_keywords")
    if not marketplace_keyword_records:
        marketplace_keyword_records = store.read_jsonl("raw", "discovery", "ebay_keywords")
    records = extract_keywords(media_records, marketplace_keyword_records, limit=100)
    collected_at = datetime.now(timezone.utc).isoformat()
    for record in records:
        record["collected_at"] = collected_at
        record["source"] = "keyword_extraction"
        record["source_is_fixture"] = False
    write_records("raw", "discovery", "extracted_keywords", "keywords.jsonl", records)


def task_ingest_reddit() -> None:
    records = list(fetch_reddit_posts(latest_keywords(50), settings(), limit_per_query=100))
    write_records("raw", "social", "reddit", "posts.jsonl", records)


def task_ingest_streetwear_media() -> None:
    records = list(fetch_streetwear_articles(latest_keywords(50), settings(), limit_per_query=100))
    write_records("raw", "social", "streetwear_media", "articles.jsonl", records)


def task_ingest_google_trends() -> None:
    records = list(fetch_google_trends(latest_keywords(50), settings()))
    write_records("raw", "search", "google_trends", "interest.jsonl", records)


def task_ingest_ebay_offers() -> None:
    """Ingestion marketplace : eBay production ou Marktplaats en fallback sandbox.

    Quand EBAY_ENV=sandbox, le sandbox eBay ne contient aucune donnée réelle
    (0 résultats). Dans ce cas, Marktplaats (filiale eBay, Pays-Bas) est utilisé
    à la place — API publique, pas d'auth requise, données réelles de seconde main.
    Les deux sources produisent le même schéma de sortie.
    """
    cfg = settings()
    keywords = latest_keywords(50)
    if cfg.ebay_env != "sandbox":
        records = list(fetch_offers(keywords, cfg, limit_per_keyword=200))
    else:
        # eBay sandbox = 0 résultats → Marktplaats comme source de données réelles
        records = list(fetch_marktplaats_offers(keywords, cfg, limit_per_keyword=50))
    write_records("raw", "marketplace", "ebay", "offers.jsonl", records)


def task_ingest_vinted() -> None:
    
    records = list(fetch_vinted_offers(latest_keywords(50), settings(), limit_per_keyword=50))
    write_records("raw", "marketplace", "vinted", "offers.jsonl", records)


def run_all() -> None:
    task_scrape_all_media()
    task_discover_marketplace_keywords()
    task_extract_keywords()
    task_ingest_reddit()
    task_ingest_streetwear_media()
    task_ingest_google_trends()
    task_ingest_ebay_offers()
    task_ingest_vinted()


TASKS = {
    "scrape_vogue": task_scrape_vogue,
    "scrape_elle": task_scrape_elle,
    "scrape_all_media": task_scrape_all_media,
    "discover_marketplace_keywords": task_discover_marketplace_keywords,
    "extract_keywords": task_extract_keywords,
    "ingest_reddit": task_ingest_reddit,
    "ingest_streetwear_media": task_ingest_streetwear_media,
    "ingest_google_trends": task_ingest_google_trends,
    "ingest_ebay_offers": task_ingest_ebay_offers,
    "ingest_vinted": task_ingest_vinted,
    "all": run_all,
}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("task", choices=sorted(TASKS))
    args = parser.parse_args()
    TASKS[args.task]()


if __name__ == "__main__":
    main()
