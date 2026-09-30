from pathlib import Path
import zipfile

from fashion_resale_trends import fixtures
from fashion_resale_trends.ml.recommend_offers import OUTPUT_COLUMNS, recommend


def test_random_forest_recommends_max_three_offers_per_keyword():
    top_keywords = [
        {"keyword": keyword, "keyword_rank": index + 1, "trend_score": 1.0 - index * 0.05}
        for index, keyword in enumerate(fixtures.KEYWORDS)
    ]
    offers = fixtures.ebay_offers(fixtures.KEYWORDS)

    recommendations = recommend(top_keywords, offers)

    assert recommendations
    assert list(recommendations[0].keys()) == OUTPUT_COLUMNS
    assert max(sum(1 for row in recommendations if row["keyword"] == kw) for kw in fixtures.KEYWORDS) <= 3
    assert len(recommendations) == 30


def test_elasticsearch_document_coercion_parses_csv_booleans_and_numbers():
    from fashion_resale_trends.exposition.index_elasticsearch import _clean_document

    document = _clean_document(
        {
            "keyword_rank": "1",
            "trend_score": "0.68",
            "source_is_fixture": "False",
            "title": "Adidas Campus",
            "condition": "",
        }
    )

    assert document["keyword_rank"] == 1
    assert document["trend_score"] == 0.68
    assert document["source_is_fixture"] is False
    assert "condition" not in document


def test_zip_contains_expected_files(tmp_path: Path):
    top_keywords = tmp_path / "top_keywords.csv"
    offers = tmp_path / "top3_ebay_offers.csv"
    metadata = tmp_path / "run_metadata.json"
    zip_path = tmp_path / "fashion_recommendations.zip"
    top_keywords.write_text("keyword,trend_score\ntrench coat,1.0\n", encoding="utf-8")
    offers.write_text("keyword,item_id\ntrench coat,item1\n", encoding="utf-8")
    metadata.write_text('{"offer_count": 1}', encoding="utf-8")

    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as archive:
        archive.write(top_keywords, "top_keywords.csv")
        archive.write(offers, "top3_ebay_offers.csv")
        archive.write(metadata, "run_metadata.json")

    with zipfile.ZipFile(zip_path) as archive:
        assert set(archive.namelist()) == {"top_keywords.csv", "top3_ebay_offers.csv", "run_metadata.json"}
