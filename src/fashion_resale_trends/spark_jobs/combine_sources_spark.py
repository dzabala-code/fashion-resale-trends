from __future__ import annotations


import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(os.getenv("PROJECT_ROOT", Path(__file__).resolve().parents[4]))
DATA_ROOT = (
    Path(os.getenv("LOCAL_DATA_DIR", PROJECT_ROOT / "data_lake"))
    / os.getenv("MINIO_BUCKET", "fashion-trends")
)

if str(PROJECT_ROOT / "src") not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT / "src"))


def _load_fashion_vocab() -> list[str]:
    
    from fashion_resale_trends.config import fashion_vocabulary
    return list(fashion_vocabulary())


def read_parquet(group: str, entity: str, spark):
   
    path = DATA_ROOT / "formatted" / group / entity / "parquet"
    if not path.exists() or not list(path.glob("*.parquet")):
        return None
    df = spark.read.parquet(str(path))
    if df.rdd.isEmpty():
        return None
    return df


def minmax_col(df, src_col: str, dst_col: str):
    
    from pyspark.sql import Window
    from pyspark.sql import functions as F
    from pyspark.sql.types import DoubleType

    w = Window.partitionBy(F.lit(1))
    col_min = F.min(F.col(src_col).cast(DoubleType())).over(w)
    col_max = F.max(F.col(src_col).cast(DoubleType())).over(w)
    return df.withColumn(
        dst_col,
        F.when(
            col_max - col_min > F.lit(0.0),
            (F.col(src_col).cast(DoubleType()) - col_min) / (col_max - col_min),
        ).otherwise(F.lit(0.0)),
    )


def main() -> None:
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F
    from pyspark.sql.types import BooleanType, DoubleType

    spark_master = os.getenv("SPARK_MASTER", "local[2]")

    spark = (
        SparkSession.builder.appName("FashionResale-CombineSources")
        .master(spark_master)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.driver.bindAddress", "0.0.0.0")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    print(f"[Spark] master={spark_master}  data_root={DATA_ROOT}")
    vocab_bc = spark.sparkContext.broadcast(set(_load_fashion_vocab()))

    def is_fashion(keyword: str) -> bool:
        if not keyword:
            return False
        vocab = vocab_bc.value
        kw = keyword.lower().strip()
        if kw in vocab:
            return True
        parts = kw.split()
        if len(parts) > 1:
            if any(p in vocab for p in parts):
                return True
            for i in range(len(parts) - 1):
                if f"{parts[i]} {parts[i + 1]}" in vocab:
                    return True
            for i in range(len(parts) - 2):
                if f"{parts[i]} {parts[i + 1]} {parts[i + 2]}" in vocab:
                    return True
        return False

    is_fashion_udf = F.udf(is_fashion, BooleanType())

    keywords_df = read_parquet("discovery", "extracted_keywords", spark)
    trends_df = read_parquet("search", "google_trends", spark)
    reddit_df = read_parquet("social", "reddit", spark)
    streetwear_df = read_parquet("social", "streetwear_media", spark)
    ebay_df = read_parquet("marketplace", "ebay", spark)
    vinted_df = read_parquet("marketplace", "vinted", spark)

    if keywords_df is None:
        raise RuntimeError("Formatted extracted keywords dataset est requis.")
    if trends_df is None:
        raise RuntimeError("Formatted Google Trends dataset est requis.")

   
  
    keywords_df = keywords_df.filter(is_fashion_udf(F.col("keyword")))
    trends_df = trends_df.filter(is_fashion_udf(F.col("keyword")))

    combined = keywords_df.groupBy("keyword").agg(
        F.mean("keyword_score").alias("media_mentions_score"),
        F.mean("source_count").alias("media_source_count"),
    )

    trends_agg = trends_df.groupBy("keyword").agg(
        F.mean("interest").alias("google_avg_interest"),
        F.max("interest").alias("google_peak_interest"),
    )
    combined = combined.join(trends_agg, on="keyword", how="outer")

   
    if reddit_df is not None and "post_id" in reddit_df.columns:
        reddit_agg = reddit_df.groupBy("keyword").agg(
            F.count("post_id").cast(DoubleType()).alias("reddit_mentions"),
            F.mean(F.col("score").cast(DoubleType())).alias("reddit_avg_score"),
            F.mean(F.col("num_comments").cast(DoubleType())).alias("reddit_avg_comments"),
        )
        reddit_agg = reddit_agg.withColumn(
            "reddit_engagement",
            F.col("reddit_avg_score") + F.col("reddit_avg_comments"),
        )
        combined = combined.join(
            reddit_agg.select("keyword", "reddit_mentions", "reddit_engagement"),
            on="keyword",
            how="left",
        )
    else:
        combined = combined.withColumn("reddit_mentions", F.lit(0.0))
        combined = combined.withColumn("reddit_engagement", F.lit(0.0))

   
    if streetwear_df is not None and "article_id" in streetwear_df.columns:
        streetwear_agg = streetwear_df.groupBy("keyword").agg(
            F.count("article_id").cast(DoubleType()).alias("streetwear_article_count"),
        )
        streetwear_agg = streetwear_agg.withColumn(
            "streetwear_engagement",
            F.col("streetwear_article_count"),
        )
        combined = combined.join(
            streetwear_agg.select("keyword", "streetwear_engagement"),
            on="keyword",
            how="left",
        )
    else:
        combined = combined.withColumn("streetwear_engagement", F.lit(0.0))

    if ebay_df is not None and "item_id" in ebay_df.columns:
        ebay_agg = ebay_df.groupBy("keyword").agg(
            F.count("item_id").cast(DoubleType()).alias("ebay_offer_count"),
            F.mean(F.col("price_value").cast(DoubleType())).alias("ebay_avg_price"),
        )
        combined = combined.join(ebay_agg, on="keyword", how="left")
    else:
        combined = combined.withColumn("ebay_offer_count", F.lit(0.0))
        combined = combined.withColumn("ebay_avg_price", F.lit(0.0))

    if vinted_df is not None and "item_id" in vinted_df.columns:
        vinted_agg = vinted_df.groupBy("keyword").agg(
            F.count("item_id").cast(DoubleType()).alias("vinted_offer_count"),
            F.mean(F.col("price_value").cast(DoubleType())).alias("vinted_avg_price"),
        )
        combined = combined.join(vinted_agg, on="keyword", how="left")
    else:
        combined = combined.withColumn("vinted_offer_count", F.lit(0.0))
        combined = combined.withColumn("vinted_avg_price", F.lit(0.0))

    
    null_cols = [
        "media_mentions_score", "media_source_count", "google_avg_interest",
        "google_peak_interest", "reddit_mentions", "reddit_engagement",
        "streetwear_engagement", "ebay_offer_count", "ebay_avg_price",
        "vinted_offer_count", "vinted_avg_price",
    ]
    fill_map = {c: 0.0 for c in null_cols if c in combined.columns}
    combined = combined.fillna(fill_map)

    
    combined = minmax_col(combined, "google_avg_interest", "google_trends_score")
    combined = minmax_col(combined, "reddit_engagement", "reddit_engagement_score")
    combined = minmax_col(combined, "streetwear_engagement", "streetwear_score")
    combined = minmax_col(combined, "media_mentions_score", "media_score")
    combined = minmax_col(combined, "ebay_offer_count", "ebay_market_score")
    combined = minmax_col(combined, "vinted_offer_count", "vinted_market_score")

    
    combined = combined.withColumn(
        "trend_score",
        F.col("google_trends_score") * F.lit(0.30)
        + F.col("reddit_engagement_score") * F.lit(0.20)
        + F.col("vinted_market_score") * F.lit(0.20)
        + F.col("media_score") * F.lit(0.15)
        + F.col("streetwear_score") * F.lit(0.10)
        + F.col("ebay_market_score") * F.lit(0.05),
    )

   
    from pyspark.sql import Window as W
    rank_w = W.orderBy(F.col("trend_score").desc())
    combined = combined.withColumn("keyword_rank", F.rank().over(rank_w))

    top10 = combined.filter(F.col("keyword_rank") <= 10)

    
    for label, df_out in [("all", combined), ("top10", top10)]:
        output_dir = DATA_ROOT / "combined" / "keyword_trend_scores" / label / "parquet"
        output_dir.mkdir(parents=True, exist_ok=True)
        df_out.coalesce(1).write.mode("overwrite").parquet(str(output_dir))

    total = combined.count()
    top_rows = (
        top10.select("keyword_rank", "keyword", "trend_score")
        .orderBy("keyword_rank")
        .collect()
    )
    print(f"[Spark] CombineSources : {total} keywords scorés.")
    print("\nTop 10 keywords par trend_score :")
    for row in top_rows:
        print(f"  #{int(row['keyword_rank'])} {row['keyword']} ({row['trend_score']:.3f})")

    spark.stop()
    print("[Spark] CombineSources terminé.")


if __name__ == "__main__":
    main()
