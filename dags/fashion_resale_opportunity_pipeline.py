from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator


PROJECT_ROOT = os.getenv("PROJECT_ROOT", "/opt/airflow/project")
PYTHONPATH = f"{PROJECT_ROOT}/src"

ENV = {
    **os.environ,
    "PROJECT_ROOT": PROJECT_ROOT,
    "PYTHONPATH": PYTHONPATH,
}


def python_task(task: str) -> str:
    return f"cd {PROJECT_ROOT} && python -m fashion_resale_trends.pipeline {task}"


def python_module(module: str) -> str:
    return f"cd {PROJECT_ROOT} && python -m {module}"


def spark_submit(script: str) -> str:
    """Run a PySpark job via spark-submit, passing the DAG run's logical date."""
    submit_bin = (
        "$(python3 -c \"import pyspark,os; "
        "print(os.path.join(pyspark.__path__[0],'bin','spark-submit'))\")"
    )
    return (
        f"cd {PROJECT_ROOT} && "
        f"RUN_DATE={{{{ ds }}}} {submit_bin} "
        f"--master ${{SPARK_MASTER:-local[2]}} "
        f"src/fashion_resale_trends/spark_jobs/{script}"
    )


with DAG(
    dag_id="fashion_resale_opportunity_pipeline",
    description="Ingest fashion trend signals, combine them per keyword and expose the results.",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    # Runs share the same data lake folders: two runs at once would overwrite each other.
    max_active_runs=1,
    # Scraped sites and APIs drop connections from time to time: retry before failing.
    default_args={"retries": 2, "retry_delay": timedelta(minutes=5)},
    tags=["big-data", "fashion", "resale", "minio"],
) as dag:
    scrape_media = BashOperator(
        task_id="scrape_media",
        bash_command=python_task("scrape_all_media"),
        env=ENV,
    )

    discover_marketplace_keywords = BashOperator(
        task_id="discover_marketplace_keywords",
        bash_command=python_task("discover_marketplace_keywords"),
        env=ENV,
    )

    extract_keywords = BashOperator(
        task_id="extract_keywords",
        bash_command=python_task("extract_keywords"),
        env=ENV,
    )

    ingest_reddit = BashOperator(
        task_id="ingest_reddit",
        bash_command=python_task("ingest_reddit"),
        env=ENV,
    )

    ingest_streetwear_media = BashOperator(
        task_id="ingest_streetwear_media",
        bash_command=python_task("ingest_streetwear_media"),
        env=ENV,
    )

    ingest_google_trends = BashOperator(
        task_id="ingest_google_trends",
        bash_command=python_task("ingest_google_trends"),
        env=ENV,
    )

    ingest_ebay_offers = BashOperator(
        task_id="ingest_ebay_offers",
        bash_command=python_task("ingest_ebay_offers"),
        env=ENV,
    )

    ingest_vinted = BashOperator(
        task_id="ingest_vinted",
        bash_command=python_task("ingest_vinted"),
        env=ENV,
    )

    format_sources = BashOperator(
        task_id="format_sources",
        bash_command=spark_submit("format_sources_spark.py"),
        env=ENV,
    )

    combine_scores = BashOperator(
        task_id="combine_keyword_scores",
        bash_command=spark_submit("combine_sources_spark.py"),
        env=ENV,
    )

    check_quality = BashOperator(
        task_id="check_data_quality",
        bash_command=python_module("fashion_resale_trends.quality_checks"),
        env=ENV,
    )

    export_zip = BashOperator(
        task_id="export_recommendations_zip",
        bash_command=python_module("fashion_resale_trends.export_results"),
        env=ENV,
    )

    index_elasticsearch = BashOperator(
        task_id="index_to_elasticsearch",
        bash_command=python_module("fashion_resale_trends.exposition.index_elasticsearch"),
        env=ENV,
    )

    [scrape_media, discover_marketplace_keywords] >> extract_keywords
    extract_keywords >> [ingest_reddit, ingest_streetwear_media, ingest_google_trends, ingest_ebay_offers, ingest_vinted]
    [ingest_reddit, ingest_streetwear_media, ingest_google_trends, ingest_ebay_offers, ingest_vinted] >> format_sources
    format_sources >> combine_scores >> check_quality >> export_zip >> index_elasticsearch

