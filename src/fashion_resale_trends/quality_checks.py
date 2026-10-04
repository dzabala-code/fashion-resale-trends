"""Data quality checks run between the combine step and the export.

Errors stop the DAG so bad data never reaches the export or Elasticsearch.
Warnings (such as datasets made of fixtures) are reported but do not fail the run.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq

from fashion_resale_trends.storage import ObjectStore

FORMATTED_DATASETS = [
    ("discovery", "extracted_keywords"),
    ("search", "google_trends"),
    ("social", "reddit"),
    ("social", "streetwear_media"),
    ("marketplace", "ebay"),
    ("marketplace", "vinted"),
]


def read_rows(path: Path) -> list[dict[str, Any]] | None:
    parts = sorted(path.glob("*.parquet"))
    if not parts:
        return None
    return [row for part in parts for row in pq.read_table(part).to_pylist()]


def check_keywords(rows: list[dict[str, Any]] | None) -> list[str]:
    if not rows:
        return ["combined/keyword_trend_scores/top10 is missing or empty"]
    errors = []
    if len(rows) > 10:
        errors.append(f"top10 has {len(rows)} rows, expected at most 10")
    for row in rows:
        keyword = row.get("keyword")
        if not keyword or not str(keyword).strip():
            errors.append("top10 contains an empty keyword")
        score = row.get("trend_score")
        if score is None or not 0.0 <= float(score) <= 1.0:
            errors.append(f"trend_score out of [0, 1] for {keyword!r}: {score}")
    return errors


def check_offers(offers: list[dict[str, Any]] | None, keywords: list[dict[str, Any]] | None) -> list[str]:
    if offers is None:
        return ["combined/ebay_offers/top3 is missing"]
    errors = []
    top_keywords = {row.get("keyword") for row in keywords or []}
    per_keyword: dict[str, int] = {}
    for offer in offers:
        keyword = offer.get("keyword")
        per_keyword[keyword] = per_keyword.get(keyword, 0) + 1
        if keyword not in top_keywords:
            errors.append(f"offer {offer.get('item_id')} has keyword {keyword!r} outside the top 10")
        price = offer.get("price_value")
        if price is None or float(price) <= 0:
            errors.append(f"offer {offer.get('item_id')} has invalid price {price}")
    for keyword, count in per_keyword.items():
        if count > 3:
            errors.append(f"{count} offers for {keyword!r}, expected at most 3")
    return errors


def fixture_share(rows: list[dict[str, Any]]) -> float:
    if not rows:
        return 0.0
    fixtures = sum(1 for row in rows if str(row.get("source_is_fixture")).lower() == "true")
    return fixtures / len(rows)


def run_checks(store: ObjectStore) -> dict[str, Any]:
    keywords = read_rows(store.layer_dir("combined", "keyword_trend_scores", "top10") / "parquet")
    offers = read_rows(store.layer_dir("combined", "ebay_offers", "top3") / "parquet")
    errors = check_keywords(keywords) + check_offers(offers, keywords)

    warnings = []
    datasets = {}
    for group, entity in FORMATTED_DATASETS:
        rows = read_rows(store.layer_dir("formatted", group, entity) / "parquet")
        name = f"{group}/{entity}"
        if rows is None:
            warnings.append(f"{name} has no formatted data")
            continue
        share = fixture_share(rows)
        datasets[name] = {"rows": len(rows), "fixture_share": round(share, 3)}
        if share > 0:
            warnings.append(f"{name}: {share:.0%} of rows are fixtures, not live data")

    return {"errors": errors, "warnings": warnings, "datasets": datasets}


def main() -> None:
    report = run_checks(ObjectStore())
    print(json.dumps(report, indent=2, ensure_ascii=False))
    if report["errors"]:
        sys.exit(f"Data quality checks failed with {len(report['errors'])} error(s).")


if __name__ == "__main__":
    main()
