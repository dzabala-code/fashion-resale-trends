from pathlib import Path

from fashion_resale_trends.config import Settings
from fashion_resale_trends.storage import ObjectStore


def test_storage_builds_clean_keys_and_writes_jsonl(tmp_path: Path):
    cfg = Settings(
        project_root=tmp_path,
        local_data_dir=tmp_path / "lake",
        storage_backend="local",
        minio_bucket="fashion-trends",
        minio_endpoint_url="http://minio:9000",
        minio_access_key="minioadmin",
        minio_secret_key="minioadmin",
        use_fixtures_if_source_fail=True,
        ebay_env="production",
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

    ref = store.dataset_file("raw", "media", "vogue", "20260603T000000Z", "articles.jsonl")
    count = store.write_jsonl(ref, [{"title": "Trench coat"}])

    assert count == 1
    assert ref.key == "raw/media/vogue/20260603T000000Z/articles.jsonl"
    assert ref.uri == "s3://fashion-trends/raw/media/vogue/20260603T000000Z/articles.jsonl"
    assert ref.local_path.read_text(encoding="utf-8").strip() == '{"title": "Trench coat"}'

