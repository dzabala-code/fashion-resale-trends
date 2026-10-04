import json
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
        fixtures.ebay_offers(fixtures.KEYWORDS)
        + [
            # Fuzzy marketplace matches that must be filtered out.
            {**fixtures.ebay_offers(fixtures.KEYWORDS)[0], "item_id": "junk-1", "title": "Dakpannen zwart", "price_value": 1.0},
            {**fixtures.ebay_offers(fixtures.KEYWORDS)[0], "item_id": "junk-2", "title": "Rob Kemps tickets", "price_value": 2.0},
        ],
    )

    format_main()
    monkeypatch.setenv("RUN_DATE", "2026-06-02")
    combine_main()
    monkeypatch.setenv("RUN_DATE", "2026-06-03")
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

    offers_dir = ObjectStore().layer_dir("combined", "ebay_offers", "top3") / "parquet"
    offers = pq.read_table(sorted(offers_dir.glob("*.parquet"))[0]).to_pylist()
    assert len(offers) == 30
    assert max(row["offer_rank"] for row in offers) == 3
    assert "model_score" not in offers[0]
    assert not {"junk-1", "junk-2"} & {row["item_id"] for row in offers}
    assert sorted(ObjectStore().layer_dir("combined", "keyword_trend_scores", "top10").glob("csv/*.csv"))

    history_dir = ObjectStore().layer_dir("combined", "keyword_trend_scores", "history")
    assert sorted(p.name for p in history_dir.glob("run_date=*")) == ["run_date=2026-06-02", "run_date=2026-06-03"]
    latest = pq.read_table(sorted(top_dir.glob("*.parquet"))[0]).to_pylist()
    assert all(row["run_date"] == "2026-06-03" for row in latest)
    assert all(row["trend_score_delta"] == 0.0 for row in latest)
    assert "google_momentum" in latest[0]
    assert latest[0]["score_sources"] == "google,reddit,media,ebay"
    assert max(row["trend_score"] for row in latest) <= 1.0

    from fashion_resale_trends.quality_checks import run_checks

    report = run_checks(ObjectStore())
    assert report["errors"] == []
    assert report["datasets"]["marketplace/ebay"]["fixture_share"] == 1.0


def test_spark_format_clears_dataset_when_latest_run_is_empty(tmp_path: Path, monkeypatch):
    """An empty latest run (e.g. Vinted blocked) must not leave stale formatted data."""
    monkeypatch.setenv("LOCAL_DATA_DIR", str(tmp_path / "lake"))
    monkeypatch.setenv("SPARK_MASTER", "local[1]")

    from fashion_resale_trends import fixtures
    from fashion_resale_trends.spark_jobs import format_sources_spark

    monkeypatch.setattr(format_sources_spark, "DATA_ROOT", tmp_path / "lake" / "fashion-trends")
    raw = tmp_path / "lake" / "fashion-trends" / "raw" / "marketplace" / "vinted"
    (raw / "20260601T000000Z").mkdir(parents=True)
    (raw / "20260601T000000Z" / "offers.jsonl").write_text(
        "\n".join(json.dumps(row) for row in fixtures.vinted_offers(["cardigan"], limit_per_keyword=3)) + "\n",
        encoding="utf-8",
    )
    format_sources_spark.main()
    formatted = tmp_path / "lake" / "fashion-trends" / "formatted" / "marketplace" / "vinted" / "parquet"
    assert list(formatted.glob("*.parquet"))

    (raw / "20260602T000000Z").mkdir()
    (raw / "20260602T000000Z" / "offers.jsonl").write_text("", encoding="utf-8")
    format_sources_spark.main()
    assert not formatted.exists()


def test_spark_combine_scores_without_google_trends(tmp_path: Path, monkeypatch):
    """When Google Trends is blocked, the score is built from the remaining sources."""
    monkeypatch.setenv("SPARK_MASTER", "local[1]")
    monkeypatch.setenv("RUN_DATE", "2026-06-02")

    from fashion_resale_trends import fixtures
    from fashion_resale_trends.spark_jobs import combine_sources_spark, format_sources_spark

    data_root = tmp_path / "lake" / "fashion-trends"
    monkeypatch.setattr(format_sources_spark, "DATA_ROOT", data_root)
    monkeypatch.setattr(combine_sources_spark, "DATA_ROOT", data_root)
    for group, entity, rows in [
        ("discovery", "extracted_keywords", [{"keyword": kw, "keyword_score": 10 - i, "source_count": 2} for i, kw in enumerate(fixtures.KEYWORDS)]),
        ("marketplace", "ebay", fixtures.ebay_offers(fixtures.KEYWORDS)),
    ]:
        run_dir = data_root / "raw" / group / entity / "20260602T000000Z"
        run_dir.mkdir(parents=True)
        (run_dir / "records.jsonl").write_text("\n".join(json.dumps(row) for row in rows) + "\n", encoding="utf-8")

    format_sources_spark.main()
    combine_sources_spark.main()

    import pyarrow.parquet as pq

    top_dir = data_root / "combined" / "keyword_trend_scores" / "top10" / "parquet"
    rows = pq.read_table(sorted(top_dir.glob("*.parquet"))[0]).to_pylist()
    assert rows[0]["score_sources"] == "media,ebay"
    assert max(row["trend_score"] for row in rows) > 0.5
