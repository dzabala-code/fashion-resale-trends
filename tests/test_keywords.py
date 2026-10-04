from fashion_resale_trends import fixtures
from fashion_resale_trends.keywords import extract_keywords


def test_extract_keywords_filters_stopwords_and_keeps_fashion_terms():
    media = fixtures.media_articles("vogue") + fixtures.media_articles("stylist")
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



def test_marketplace_discovery_ignores_the_words_of_the_search_query():
    from fashion_resale_trends.keywords import discover_keywords_from_offers

    offers = [
        {"keyword": "samba adidas", "title": f"Adidas Samba veste en cuir {size}"}
        for size in ("38", "40", "42")
    ]

    keywords = {record["keyword"] for record in discover_keywords_from_offers(offers)}

    assert "adidas" not in keywords
    assert "samba" not in keywords
    assert not any("samba" in keyword or "adidas" in keyword for keyword in keywords)
    assert "veste cuir" in keywords
