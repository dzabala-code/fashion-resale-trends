from __future__ import annotations


import os
import sys
from datetime import datetime, timezone
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


OFFER_COLUMNS = [
    "keyword",
    "keyword_rank",
    "trend_score",
    "offer_rank",
    "item_id",
    "title",
    "price_value",
    "currency",
    "condition",
    "item_url",
    "seller_feedback_score",
    "seller_feedback_percentage",
    "source_is_fixture",
]


def read_previous_run(spark, history_dir: Path, current_date: str):
    """trend_score per keyword from the most recent run before current_date."""
    from pyspark.sql import functions as F

    if not history_dir.exists():
        return None
    dates = sorted(
        p.name.split("=", 1)[1]
        for p in history_dir.glob("run_date=*")
        if p.name.split("=", 1)[1] < current_date
    )
    if not dates:
        return None
    return (
        spark.read.parquet(str(history_dir / f"run_date={dates[-1]}"))
        .select("keyword", F.col("trend_score").alias("previous_trend_score"))
    )


def write_outputs(df, output_dir: Path) -> None:
    single = df.coalesce(1)
    single.write.mode("overwrite").parquet(str(output_dir / "parquet"))
    single.write.mode("overwrite").option("header", True).csv(str(output_dir / "csv"))


# (score column, source, weight). Weights add up to 1 and are re-normalised over
# the sources that returned data. Momentum gets the largest Google weight because
# the goal is to catch rising trends, not terms that are already popular.
SCORE_WEIGHTS = [
    ("google_momentum_score", "google", 0.20),
    ("google_trends_score", "google", 0.10),
    ("reddit_score", "reddit", 0.20),
    ("media_score", "media", 0.20),
    ("streetwear_score", "streetwear", 0.10),
    ("vinted_market_score", "vinted", 0.15),
    ("ebay_market_score", "ebay", 0.05),
]

# Google Trends returns weekly points: momentum is the growth of the last 13 weeks
# (a quarter) over the 52 weeks before them, as a ratio (0.25 = +25 %). A ratio, not
# a difference, because Google scales each batch of 5 keywords to its own maximum,
# so raw interest values are not comparable across batches.
RECENT_WEEKS = 13
BASELINE_WEEKS = 52


def google_aggregates(trends_df):
    """Average, peak and momentum of search interest per keyword."""
    from pyspark.sql import Window
    from pyspark.sql import functions as F
    from pyspark.sql.types import DoubleType

    interest = F.col("interest").cast(DoubleType())
    recent_first = Window.partitionBy("keyword").orderBy(F.col("date").desc())
    ranked = trends_df.withColumn("week_rank", F.row_number().over(recent_first))
    is_recent = F.col("week_rank") <= RECENT_WEEKS
    is_baseline = (F.col("week_rank") > RECENT_WEEKS) & (F.col("week_rank") <= RECENT_WEEKS + BASELINE_WEEKS)
    aggregated = ranked.groupBy("keyword").agg(
        F.mean(interest).alias("google_avg_interest"),
        F.max(interest).alias("google_peak_interest"),
        F.mean(F.when(is_recent, interest)).alias("google_recent_interest"),
        F.mean(F.when(is_baseline, interest)).alias("google_baseline_interest"),
    )
    growth = F.when(
        F.col("google_baseline_interest") > 0,
        F.col("google_recent_interest") / F.col("google_baseline_interest") - F.lit(1.0),
    )
    # Keywords without enough history get 0: neutral, neither rising nor falling.
    return aggregated.withColumn("google_momentum", F.coalesce(growth, F.lit(0.0))).drop(
        "google_recent_interest", "google_baseline_interest"
    )


def relevant_offers(offers_df):
    """Keep listings whose title contains the searched keyword as a whole word."""
    from pyspark.sql import functions as F

    if offers_df is None or "title" not in offers_df.columns:
        return offers_df
    # \Q...\E quotes the keyword so characters like "+" are matched literally.
    relevant = offers_df.filter(F.expr(
        r"lower(title) RLIKE concat('(^|[^\\p{L}0-9])\\Q', lower(keyword), '\\E($|[^\\p{L}0-9])')"
    ))
    return None if relevant.rdd.isEmpty() else relevant


def run_date() -> str:
    """Logical date of the run (Airflow's ds), defaulting to today in UTC."""
    configured = os.getenv("RUN_DATE", "").strip()
    return configured or datetime.now(timezone.utc).strftime("%Y-%m-%d")


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
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
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

    keywords_df = keywords_df.filter(is_fashion_udf(F.col("keyword")))

    combined = keywords_df.groupBy("keyword").agg(
        F.mean("keyword_score").alias("media_mentions_score"),
        F.mean("source_count").alias("media_source_count"),
    )

    # Google Trends is optional: Google often blocks the unofficial API.
    if trends_df is not None:
        trends_df = trends_df.filter(is_fashion_udf(F.col("keyword")))
        combined = combined.join(google_aggregates(trends_df), on="keyword", how="left")
    else:
        for column in ("google_avg_interest", "google_peak_interest", "google_momentum"):
            combined = combined.withColumn(column, F.lit(0.0))

    # Marketplace searches are fuzzy ("robe" also returns "Rob" tickets on Marktplaats):
    # only listings whose title contains the keyword as a whole word are kept.
    ebay_df = relevant_offers(ebay_df)
    vinted_df = relevant_offers(vinted_df)

   
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
        "google_peak_interest", "google_momentum", "reddit_mentions", "reddit_engagement",
        "streetwear_engagement", "ebay_offer_count", "ebay_avg_price",
        "vinted_offer_count", "vinted_avg_price",
    ]
    fill_map = {c: 0.0 for c in null_cols if c in combined.columns}
    combined = combined.fillna(fill_map)

    
    combined = minmax_col(combined, "google_avg_interest", "google_trends_score")
    combined = minmax_col(combined, "google_momentum", "google_momentum_score")
    # Number of posts rather than engagement: without Reddit API keys the pipeline reads
    # the RSS feed, which has no score or comment count (both are always 0).
    combined = minmax_col(combined, "reddit_mentions", "reddit_score")
    combined = minmax_col(combined, "streetwear_engagement", "streetwear_score")
    combined = minmax_col(combined, "media_mentions_score", "media_score")
    combined = minmax_col(combined, "ebay_offer_count", "ebay_market_score")
    combined = minmax_col(combined, "vinted_offer_count", "vinted_market_score")

    
    # Weighted average over the sources that returned data in this run, so a blocked
    # source (Google, Vinted...) does not drag every keyword's score towards 0.
    available = {
        "google": trends_df is not None,
        "reddit": reddit_df is not None,
        "media": True,
        "streetwear": streetwear_df is not None,
        "vinted": vinted_df is not None,
        "ebay": ebay_df is not None,
    }
    used = [(column, weight) for column, source, weight in SCORE_WEIGHTS if available[source]]
    total_weight = sum(weight for _, weight in used)
    combined = combined.withColumn(
        "trend_score",
        sum(F.col(column) * F.lit(weight / total_weight) for column, weight in used),
    ).withColumn(
        "score_sources",
        F.lit(",".join(source for source, ok in available.items() if ok)),
    )
    print(f"[Spark] Sources utilisées pour le trend_score : {[s for s, ok in available.items() if ok]}")

   
    from pyspark.sql import Window as W
    rank_w = W.orderBy(F.col("trend_score").desc())
    combined = combined.withColumn("keyword_rank", F.rank().over(rank_w))

    # Daily history: one partition per run date, so each day's scores are kept
    # and compared with the previous run instead of being overwritten.
    current_date = run_date()
    combined = combined.withColumn("run_date", F.lit(current_date))
    history_dir = DATA_ROOT / "combined" / "keyword_trend_scores" / "history"
    previous = read_previous_run(spark, history_dir, current_date)
    if previous is not None:
        combined = combined.join(previous, on="keyword", how="left")
        combined = combined.withColumn(
            "trend_score_delta",
            F.when(
                F.col("previous_trend_score").isNotNull(),
                F.col("trend_score") - F.col("previous_trend_score"),
            ),
        ).drop("previous_trend_score")
    else:
        combined = combined.withColumn("trend_score_delta", F.lit(None).cast(DoubleType()))
    combined.coalesce(1).write.mode("overwrite").partitionBy("run_date").parquet(str(history_dir))

    top10 = combined.filter(F.col("keyword_rank") <= 10)

    
    for label, df_out in [("all", combined), ("top10", top10)]:
        write_outputs(df_out, DATA_ROOT / "combined" / "keyword_trend_scores" / label)

    if ebay_df is not None and "item_id" in ebay_df.columns:
        offers = ebay_df.join(
            top10.select("keyword", "keyword_rank", "trend_score"), on="keyword", how="inner"
        )
        # Drop placeholder prices (1 euro "to negotiate" listings, accessories...):
        # keep offers priced at least a quarter of the keyword's median price.
        medians = offers.groupBy("keyword").agg(
            F.percentile_approx(F.col("price_value").cast(DoubleType()), 0.5).alias("median_price")
        )
        offers = (
            offers.join(medians, on="keyword")
            .filter(F.col("price_value").cast(DoubleType()) >= F.col("median_price") * F.lit(0.25))
            .filter(F.col("price_value").cast(DoubleType()) > 0)
        )
        offer_w = W.partitionBy("keyword").orderBy(F.col("price_value").cast(DoubleType()).asc(), F.col("item_id"))
        offers = offers.withColumn("offer_rank", F.row_number().over(offer_w)).filter(F.col("offer_rank") <= 3)
        offer_columns = [c for c in OFFER_COLUMNS if c in offers.columns]
        offers = offers.select(*offer_columns).orderBy("keyword_rank", "offer_rank")
        write_outputs(offers, DATA_ROOT / "combined" / "ebay_offers" / "top3")
        print(f"[Spark] {offers.count()} offres eBay retenues (top 3 par keyword).")

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
