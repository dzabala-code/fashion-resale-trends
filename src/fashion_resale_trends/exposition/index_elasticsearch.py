from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from elasticsearch import Elasticsearch, helpers

from fashion_resale_trends.config import settings
from fashion_resale_trends.export_results import first_csv
from fashion_resale_trends.storage import ObjectStore
from fashion_resale_trends.tabular_io import read_csv_dicts

_BOOL_FIELDS = {"source_is_fixture"}
_INT_FIELDS = {
    "keyword_rank",
    "offer_rank",
    "seller_feedback_score",
    "reddit_mentions",
    "google_peak_interest",
    "ebay_offer_count",
    "vinted_offer_count",
    "media_source_count",
}
_FLOAT_FIELDS = {
    "trend_score",
    "price_value",
    "model_score",
    "seller_feedback_percentage",
    "google_avg_interest",
    "reddit_engagement",
    "google_trends_score",
    "reddit_engagement_score",
    "media_score",
    "ebay_market_score",
    "vinted_market_score",
    "ebay_avg_price",
    "vinted_avg_price",
    "media_mentions_score",
    "streetwear_score",
    "pinterest_score",
}


def _parse_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    text = str(value).strip().lower()
    return text in {"true", "1", "yes", "y"}


def _coerce_value(key: str, value: Any) -> Any:
    if key in _BOOL_FIELDS:
        return _parse_bool(value)
    if key in _INT_FIELDS:
        return int(float(value))
    if key in _FLOAT_FIELDS:
        return float(value)
    return value


def _clean_document(row: dict[str, Any]) -> dict[str, Any]:
    document: dict[str, Any] = {}
    for key, value in row.items():
        if value is None or value == "":
            continue
        document[key] = _coerce_value(key, value)
    return document


def actions_from_rows(index_name: str, rows: list[dict[str, Any]], id_column: str | None = None):
    for index, row in enumerate(rows):
        document = _clean_document(row)
        action = {"_index": index_name, "_source": document}
        if id_column and id_column in document:
            action["_id"] = str(document[id_column])
        else:
            action["_id"] = str(index)
        yield action


def _prepare_keyword_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    if not rows:
        return rows
    if "collected_at_utc" in rows[0]:
        return rows
    stamp = datetime.now(timezone.utc).isoformat()
    return [{**row, "collected_at_utc": stamp} for row in rows]


def _prepare_offer_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    prepared: list[dict[str, Any]] = []
    for row in rows:
        updated = dict(row)
        condition = updated.get("condition")
        if condition is None or str(condition).strip() == "":
            updated["condition"] = "unknown"
        prepared.append(updated)
    return prepared


def main() -> None:
    cfg = settings()
    store = ObjectStore(cfg)
    client = Elasticsearch(cfg.elasticsearch_url)

    top_keywords_csv = first_csv(store.layer_dir("combined", "keyword_trend_scores", "top10"))
    offers_csv = store.layer_dir("ml", "ebay_offer_recommendations", "top3") / "top3_ebay_offers.csv"
    keyword_rows = _prepare_keyword_rows(read_csv_dicts(top_keywords_csv))
    offer_rows = _prepare_offer_rows(read_csv_dicts(offers_csv))

    helpers.bulk(client, actions_from_rows(cfg.keyword_index, keyword_rows, "keyword"))
    helpers.bulk(client, actions_from_rows(cfg.offer_index, offer_rows, "item_id"))
    print(f"Indexed {len(keyword_rows)} keyword rows into {cfg.keyword_index}")
    print(f"Indexed {len(offer_rows)} offer rows into {cfg.offer_index}")


if __name__ == "__main__":
    main()
