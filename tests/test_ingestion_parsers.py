from fashion_resale_trends.ingestion.ebay import parse_item
from fashion_resale_trends.ingestion.google_trends import summarize_interest
from fashion_resale_trends.ingestion.reddit import parse_post


def test_parse_ebay_item_extracts_offer_fields():
    item = {
        "itemId": "abc",
        "title": "Trench coat vintage",
        "price": {"value": "49.90", "currency": "EUR"},
        "condition": "Occasion",
        "itemWebUrl": "https://www.ebay.fr/itm/abc",
        "seller": {"username": "seller", "feedbackScore": 1234, "feedbackPercentage": "99.2"},
        "shippingOptions": [{"shippingCost": {"value": "4.90"}}],
        "listingMarketplaceId": "EBAY_FR",
    }

    parsed = parse_item("Trench Coat", item)

    assert parsed["keyword"] == "trench coat"
    assert parsed["price_value"] == 49.9
    assert parsed["currency"] == "EUR"
    assert parsed["seller_feedback_score"] == 1234
    assert parsed["shipping_cost"] == 4.9


def test_parse_reddit_post_extracts_engagement():
    child = {"data": {"id": "p1", "title": "Ballet flats?", "score": 42, "num_comments": 11, "created_utc": 1780416000}}

    parsed = parse_post("Ballet Flats", "fashion", child)

    assert parsed["keyword"] == "ballet flats"
    assert parsed["subreddit"] == "fashion"
    assert parsed["score"] == 42
    assert parsed["num_comments"] == 11


def test_summarize_google_trends_calculates_average_peak_and_growth():
    rows = [
        {"date": "2026-01-01", "interest": 10},
        {"date": "2026-01-02", "interest": 20},
        {"date": "2026-01-03", "interest": 40},
    ]

    summary = summarize_interest("Mocassins", rows)

    assert summary["keyword"] == "mocassins"
    assert summary["google_avg_interest"] == 70 / 3
    assert summary["google_peak_interest"] == 40
    assert summary["google_growth"] == 30


def test_marktplaats_parse_fixed_price_and_condition():
    from fashion_resale_trends.ingestion.marktplaats import parse_listing

    listing = {
        "itemId": "m123",
        "title": "Adidas Campus",
        "vipUrl": "/v/kleding-heren/schoenen/m123-adidas-campus",
        "priceInfo": {"priceCents": 6500, "priceType": "FIXED"},
        "sellerInformation": {"sellerName": "seller1"},
        "attributes": [{"key": "condition", "value": "Zo goed als nieuw"}],
    }
    parsed = parse_listing("Adidas Campus", listing)

    assert parsed["price_value"] == 65.0
    assert parsed["condition"] == "Zo goed als nieuw"
    assert parsed["price_type"] == "FIXED"


def test_marktplaats_fast_bid_zero_price_is_none():
    from fashion_resale_trends.ingestion.marktplaats import parse_listing

    listing = {
        "itemId": "m999",
        "title": "Adidas Campus",
        "vipUrl": "/v/kleding-heren/schoenen/m999",
        "priceInfo": {"priceCents": 0, "priceType": "FAST_BID"},
        "sellerInformation": {"sellerName": "seller1"},
        "attributes": [],
    }
    parsed = parse_listing("adidas", listing)

    assert parsed["price_value"] is None
    assert parsed["price_type"] == "FAST_BID"


def test_parse_streetwear_article_extracts_feed_source():
    from fashion_resale_trends.ingestion.streetwear_media import parse_article

    parsed = parse_article(
        "adidas",
        {
            "id": "abc123",
            "title": "Adidas x Wales Bonner",
            "description": "New collaboration",
            "link": "https://hypebeast.com/adidas-wales-bonner",
            "feed_source": "hypebeast",
        },
    )

    assert parsed["keyword"] == "adidas"
    assert parsed["article_id"] == "abc123"
    assert parsed["feed_source"] == "hypebeast"
    assert parsed["source"] == "streetwear_media"
    assert parsed["source_is_fixture"] is False


def test_discover_marktplaats_keywords_marks_real_source():
    from fashion_resale_trends.keywords import discover_keywords_from_offers

    offers = [
        {
            "title": "Adidas Samba vintage sneakers maat 40",
            "source": "marktplaats",
            "source_is_fixture": False,
        },
        {
            "title": "Adidas Samba classic white",
            "source": "marktplaats",
            "source_is_fixture": False,
        },
    ]
    records = discover_keywords_from_offers(
        offers,
        discovery_source="marktplaats_keyword_discovery",
        marketplace="marktplaats",
    )

    assert records
    assert records[0]["source"] == "marktplaats_keyword_discovery"
    assert records[0]["marketplace"] == "marktplaats"
    assert records[0]["source_is_fixture"] is False
    assert "adidas" in {row["keyword"] for row in records}


def test_marktplaats_min_bid_uses_starting_price():
    from fashion_resale_trends.ingestion.marktplaats import parse_listing

    listing = {
        "itemId": "m456",
        "title": "Vintage trench",
        "vipUrl": "/v/test/m456",
        "priceInfo": {"priceCents": 7500, "priceType": "MIN_BID"},
        "sellerInformation": {"sellerName": "seller2"},
        "attributes": [],
    }
    parsed = parse_listing("trench coat", listing)

    assert parsed["price_value"] == 75.0
    assert parsed["price_type"] == "MIN_BID"



def test_ingest_vinted_writes_empty_run_when_vinted_is_blocked(monkeypatch):
    from fashion_resale_trends import pipeline

    written = {}

    def blocked(*args, **kwargs):
        raise RuntimeError("Vinted API returned no records (last HTTP status: 403).")

    monkeypatch.setattr(pipeline, "fetch_vinted_offers", blocked)
    monkeypatch.setattr(pipeline, "latest_keywords", lambda limit=50: ["cardigan"])
    monkeypatch.setattr(pipeline, "write_records", lambda *args: written.update(args=args))

    pipeline.task_ingest_vinted()

    assert written["args"][:3] == ("raw", "marketplace", "vinted")
    assert list(written["args"][4]) == []


def test_streetwear_keyword_without_matching_article_gets_no_articles(monkeypatch):
    from fashion_resale_trends.config import settings
    from fashion_resale_trends.ingestion import streetwear_media

    articles = [{"title": "Carhartt debuts a workwear jacket", "description": "", "link": "https://x/1"}]
    monkeypatch.setattr(streetwear_media, "_fetch_all_feeds", lambda: articles)

    records = streetwear_media.fetch_streetwear_articles(["carhartt", "mocassins"], settings())

    assert [record["keyword"] for record in records] == ["carhartt"]
