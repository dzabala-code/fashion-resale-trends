from pathlib import Path

import pytest

from fashion_resale_trends.config import Settings
from fashion_resale_trends.storage import ObjectStore

pyspark = pytest.importorskip("pyspark")


def test_spark_combine_produces_top10(tmp_path: Path, monkeypatch):
    """PySpark combine_sources_spark produces a top-10 CSV with trend_score."""
    monkeypatch.setenv("LOCAL_DATA_DIR", str(tmp_path / "lake"))
    monkeypatch.setenv("STORAGE_BACKEND", "local")
    monkeypatch.setenv("SPARK_MASTER", "local[1]")

    from fashion_resale_trends import fixtures
    from fashion_resale_trends.spark_jobs.format_sources_spark import main as format_main
    from fashion_resale_trends.spark_jobs.combine_sources_spark import main as combine_main

    cfg = Settings(
        project_root=tmp_path,
        local_data_dir=tmp_path / "lake",
        storage_backend="local",
        minio_bucket="fashion-trends",
        minio_endpoint_url="http://minio:9000",
        minio_access_key="minioadmin",
        minio_secret_key="minioadmin",
        use_fixtures_if_source_fail=True,
        ebay_env="sandbox",
        ebay_client_id="",
        ebay_client_secret="",
        ebay_marketplace_id="EBAY_FR",
        reddit_client_id="",
        reddit_client_secret="",
        reddit_user_agent="test",
        google_trends_geo="FR",
        elasticsearch_url="http://localhost:9200",
        keyword_index="fashion_keyword_scores",
        offer_index="fashion_offer_recommendations",
    )
    store = ObjectStore(cfg)
    run_id = "20260603T000000Z"

    store.write_jsonl(
        store.dataset_file("raw", "discovery", "extracted_keywords", run_id, "keywords.jsonl"),
        [{"keyword": kw, "keyword_score": 10 - i, "source_count": 2} for i, kw in enumerate(fixtures.KEYWORDS)],
    )
    store.write_jsonl(
        store.dataset_file("raw", "search", "google_trends", run_id, "interest.jsonl"),
        fixtures.google_trends(fixtures.KEYWORDS),
    )
    store.write_jsonl(
        store.dataset_file("raw", "social", "reddit", run_id, "posts.jsonl"),
        fixtures.reddit_posts(fixtures.KEYWORDS),
    )
    store.write_jsonl(
        store.dataset_file("raw", "marketplace", "ebay", run_id, "offers.jsonl"),
        fixtures.ebay_offers(fixtures.KEYWORDS),
    )

    format_main()
    combine_main()

    top_dir = ObjectStore().layer_dir("combined", "keyword_trend_scores", "top10") / "parquet"
    parts = sorted(top_dir.glob("*.parquet"))
    assert parts, "No Parquet produced in combined/top10"
    lines = parts[0].read_bytes()
    assert len(lines) > 0
    import pyarrow.parquet as pq

    table = pq.read_table(parts[0])
    assert "trend_score" in table.column_names
    assert table.num_rows == 10
