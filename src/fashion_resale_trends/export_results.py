from __future__ import annotations

import json
import shutil
import zipfile
from datetime import datetime, timezone
from pathlib import Path

from fashion_resale_trends.config import settings
from fashion_resale_trends.storage import ObjectStore
from fashion_resale_trends.tabular_io import count_csv_rows


def first_csv(path: Path) -> Path:
    matches = sorted((path / "csv").glob("*.csv"))
    if not matches:
        raise FileNotFoundError(f"No CSV part found under {path / 'csv'}")
    return matches[0]


def create_export_zip() -> Path:
    cfg = settings()
    store = ObjectStore(cfg)
    export_dir = cfg.project_root / "exports" / "latest"
    export_dir.mkdir(parents=True, exist_ok=True)

    top_keywords_src = first_csv(store.layer_dir("combined", "keyword_trend_scores", "top10"))
    offers_src = store.layer_dir("ml", "ebay_offer_recommendations", "top3") / "top3_ebay_offers.csv"
    if not offers_src.exists():
        raise FileNotFoundError(f"Missing recommendations file: {offers_src}")

    top_keywords_out = export_dir / "top_keywords.csv"
    shutil.copy2(top_keywords_src, top_keywords_out)

    offers_out = export_dir / "top3_ebay_offers.csv"
    shutil.copy2(offers_src, offers_out)

    metadata = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "top_keyword_count": count_csv_rows(top_keywords_out),
        "offer_count": count_csv_rows(offers_out),
        "bucket": cfg.minio_bucket,
        "market": "France/Europe",
    }
    metadata_path = export_dir / "run_metadata.json"
    metadata_path.write_text(json.dumps(metadata, indent=2), encoding="utf-8")

    zip_path = export_dir / "fashion_recommendations.zip"
    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as archive:
        archive.write(top_keywords_out, "top_keywords.csv")
        archive.write(offers_out, "top3_ebay_offers.csv")
        archive.write(metadata_path, "run_metadata.json")

    ref = store.dataset_file("exports", "recommendations", "latest", "latest", "fashion_recommendations.zip")
    ref.local_path.parent.mkdir(parents=True, exist_ok=True)
    ref.local_path.write_bytes(zip_path.read_bytes())
    store._upload_if_enabled(ref)
    print(f"Wrote export zip to {zip_path}")
    return zip_path


def main() -> None:
    create_export_zip()


if __name__ == "__main__":
    main()
