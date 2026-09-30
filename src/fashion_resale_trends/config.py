from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml
from dotenv import load_dotenv


PROJECT_ROOT = Path(os.getenv("PROJECT_ROOT", Path(__file__).resolve().parents[2]))
load_dotenv(PROJECT_ROOT / ".env")


@dataclass(frozen=True)
class Settings:
    project_root: Path
    local_data_dir: Path
    storage_backend: str
    minio_bucket: str
    minio_endpoint_url: str
    minio_access_key: str
    minio_secret_key: str
    use_fixtures_if_source_fail: bool
    ebay_env: str
    ebay_client_id: str
    ebay_client_secret: str
    ebay_marketplace_id: str
    reddit_client_id: str
    reddit_client_secret: str
    reddit_user_agent: str
    google_trends_geo: str
    elasticsearch_url: str
    keyword_index: str
    offer_index: str


def bool_env(name: str, default: str = "false") -> bool:
    return os.getenv(name, default).strip().lower() in {"1", "true", "yes", "on"}


def settings() -> Settings:
    return Settings(
        project_root=PROJECT_ROOT,
        local_data_dir=Path(os.getenv("LOCAL_DATA_DIR", PROJECT_ROOT / "data_lake")),
        storage_backend=os.getenv("STORAGE_BACKEND", "local").strip().lower(),
        minio_bucket=os.getenv("MINIO_BUCKET", "fashion-trends"),
        minio_endpoint_url=os.getenv("MINIO_ENDPOINT_URL", "http://localhost:9000"),
        minio_access_key=os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
        minio_secret_key=os.getenv("MINIO_SECRET_KEY", "minioadmin"),
        use_fixtures_if_source_fail=bool_env("USE_FIXTURES_IF_SOURCE_FAIL", "true"),
        ebay_env=os.getenv("EBAY_ENV", "production"),
        ebay_client_id=os.getenv("EBAY_CLIENT_ID", ""),
        ebay_client_secret=os.getenv("EBAY_CLIENT_SECRET", ""),
        ebay_marketplace_id=os.getenv("EBAY_MARKETPLACE_ID", "EBAY_FR"),
        reddit_client_id=os.getenv("REDDIT_CLIENT_ID", ""),
        reddit_client_secret=os.getenv("REDDIT_CLIENT_SECRET", ""),
        reddit_user_agent=os.getenv("REDDIT_USER_AGENT", "fashion-resale-trends/1.0 by local"),
        google_trends_geo=os.getenv("GOOGLE_TRENDS_GEO", "FR"),
        elasticsearch_url=os.getenv("ELASTICSEARCH_URL", "http://localhost:9200"),
        keyword_index=os.getenv("KEYWORD_INDEX", "fashion_keyword_scores"),
        offer_index=os.getenv("OFFER_INDEX", "fashion_offer_recommendations"),
    )


def load_yaml_config(name: str) -> dict[str, Any]:
    path = PROJECT_ROOT / "config" / name
    if not path.exists():
        return {}
    with path.open(encoding="utf-8") as input_file:
        return yaml.safe_load(input_file) or {}


def seed_keywords() -> list[str]:
    configured = load_yaml_config("seed_keywords.yml").get("seed_keywords", [])
    return [str(value).strip().lower() for value in configured if str(value).strip()]


def stopwords() -> set[str]:
    configured = load_yaml_config("stopwords.yml").get("stopwords", [])
    return {str(value).strip().lower() for value in configured if str(value).strip()}


def fashion_vocabulary() -> set[str]:
    """Vocabulaire fashion large pour la découverte libre des tendances."""
    configured = load_yaml_config("fashion_vocabulary.yml").get("fashion_vocabulary", [])
    return {str(value).strip().lower() for value in configured if str(value).strip()}


def sources_config() -> dict[str, Any]:
    return load_yaml_config("sources.yml")

