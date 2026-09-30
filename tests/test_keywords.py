from fashion_resale_trends import fixtures
from fashion_resale_trends.keywords import extract_keywords


def test_extract_keywords_filters_stopwords_and_keeps_fashion_terms():
    media = fixtures.media_articles("vogue") + fixtures.media_articles("elle")
    marketplace_keywords = [
        {"keyword": "the", "mention_count": 10, "marketplace": "marktplaats"},
        {"keyword": "trench coat", "mention_count": 4, "marketplace": "marktplaats"},
    ]

    records = extract_keywords(media, marketplace_keywords, limit=20)
    keywords = {record["keyword"] for record in records}

    assert "the" not in keywords
    assert "new" not in keywords
    assert "trench coat" in keywords
    assert "ballet flats" in keywords
    assert len(records) == len(keywords)

