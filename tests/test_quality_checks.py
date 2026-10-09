from fashion_resale_trends.quality_checks import check_keywords, check_offers, fixture_share


def test_check_keywords_flags_empty_and_out_of_range_rows():
    assert check_keywords(None) == ["combined/keyword_trend_scores/top10 is missing or empty"]
    errors = check_keywords([{"keyword": "", "trend_score": 0.5}, {"keyword": "cardigan", "trend_score": 1.4}])
    assert len(errors) == 2


def test_check_offers_flags_bad_price_unknown_keyword_and_too_many_offers():
    keywords = [{"keyword": "cardigan", "trend_score": 0.8}]
    offers = [{"keyword": "cardigan", "item_id": str(i), "price_value": 20.0} for i in range(4)]
    offers.append({"keyword": "trench coat", "item_id": "x", "price_value": 0.0})

    errors = check_offers(offers, keywords)

    assert any("outside the top 10" in error for error in errors)
    assert any("invalid price" in error for error in errors)
    assert any("4 offers" in error for error in errors)
    assert check_offers(offers[:3], keywords) == []


def test_fixture_share_handles_bools_and_csv_strings():
    rows = [{"source_is_fixture": True}, {"source_is_fixture": "False"}, {"source_is_fixture": "true"}, {}]
    assert fixture_share(rows) == 0.5
