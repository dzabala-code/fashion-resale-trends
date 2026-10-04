from __future__ import annotations


import os
import shutil
import sys
from pathlib import Path

PROJECT_ROOT = Path(os.getenv("PROJECT_ROOT", Path(__file__).resolve().parents[4]))
DATA_ROOT = (
    Path(os.getenv("LOCAL_DATA_DIR", PROJECT_ROOT / "data_lake"))
    / os.getenv("MINIO_BUCKET", "fashion-trends")
)


if str(PROJECT_ROOT / "src") not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT / "src"))


def _media_datasets() -> list[tuple[str, str, str, str]]:
    from fashion_resale_trends.config import sources_config

    return [("media", name, "media", name) for name in sources_config().get("media", {})]


DATASETS = [
    ("discovery", "marketplace_keywords", "discovery", "marketplace_keywords"),
    ("discovery", "extracted_keywords", "discovery", "extracted_keywords"),
    ("social", "reddit", "social", "reddit"),
    ("social", "streetwear_media", "social", "streetwear_media"),
    ("search", "google_trends", "search", "google_trends"),
    ("marketplace", "ebay", "marketplace", "ebay"),
    ("marketplace", "vinted", "marketplace", "vinted"),
]


_TIMESTAMP_COLS = ("collected_at", "published_at", "created_utc", "scraped_at")


def latest_run_dir(layer: str, group: str, entity: str) -> Path | None:
   
    root = DATA_ROOT / layer / group / entity
    if not root.exists():
        return None
    runs = sorted(p for p in root.iterdir() if p.is_dir())
    return runs[-1] if runs else None


def normalize_df(df):
    """Normalise timestamps en UTC-string et price_value en Double."""
    from pyspark.sql import functions as F
    from pyspark.sql.types import DoubleType, StringType

  
    for col_name in _TIMESTAMP_COLS:
        if col_name in df.columns:
            df = df.withColumn(
                col_name,
                F.when(
                    F.col(col_name).isNotNull(),
                    F.date_format(
                        F.to_utc_timestamp(
                            F.to_timestamp(F.col(col_name).cast(StringType())),
                            "UTC",
                        ),
                        "yyyy-MM-dd'T'HH:mm:ss'Z'",
                    ),
                ).otherwise(F.lit(None).cast(StringType())),
            )

    
    if "price_value" in df.columns:
        df = df.withColumn("price_value", F.col("price_value").cast(DoubleType()))

    return df


def format_dataset(spark, raw_group: str, raw_entity: str, fmt_group: str, fmt_entity: str) -> None:
    
    run_dir = latest_run_dir("raw", raw_group, raw_entity)
    if run_dir is None:
        print(f"[SKIP] Aucun run trouvé pour raw/{raw_group}/{raw_entity}")
        return

    jsonl_files = list(run_dir.glob("*.jsonl"))
    if not jsonl_files:
        print(f"[SKIP] Aucun fichier JSONL dans {run_dir}")
        return

    
    df = spark.read.option("inferSchema", "true").json(str(run_dir / "*.jsonl"))

    output_dir = DATA_ROOT / "formatted" / fmt_group / fmt_entity / "parquet"

    if df.rdd.isEmpty():
        # The latest run is empty: drop the previous output so stale data is not reused.
        shutil.rmtree(output_dir, ignore_errors=True)
        print(f"[EMPTY] Aucun enregistrement dans {run_dir}, données formatées supprimées")
        return

    df = normalize_df(df)

    output_dir.mkdir(parents=True, exist_ok=True)

    df.coalesce(1).write.mode("overwrite").parquet(str(output_dir))
    count = df.count()
    print(f"[OK] {raw_group}/{raw_entity} → {count} enregistrements → {output_dir}")


def main() -> None:
    from pyspark.sql import SparkSession

    spark_master = os.getenv("SPARK_MASTER", "local[2]")

    spark = (
        SparkSession.builder.appName("FashionResale-FormatSources")
        .master(spark_master)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.driver.bindAddress", "0.0.0.0")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    print(f"[Spark] master={spark_master}  data_root={DATA_ROOT}")

    for raw_group, raw_entity, fmt_group, fmt_entity in _media_datasets() + DATASETS:
        try:
            format_dataset(spark, raw_group, raw_entity, fmt_group, fmt_entity)
        except Exception as exc:
            print(f"[WARN] format_dataset({raw_group}/{raw_entity}) a échoué : {exc}")

    spark.stop()
    print("[Spark] FormatSources terminé.")


if __name__ == "__main__":
    main()
