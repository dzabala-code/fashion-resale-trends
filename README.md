# Fashion Resale Trends

Big Data project for the II.2314 Advanced Database course at ISEP (June 2026), by Danae Zabala.

**Stack:** Python · Apache Airflow · PySpark · MinIO (S3) · Elasticsearch · Kibana · Docker

Fashion trends change very fast, and today they are not only created by big brands but also by vintage resellers on second-hand platforms. Detecting emerging trends is getting harder and harder. With this project we built a data pipeline that collects fashion signals from many sources, combines them per keyword and exposes the trending items with their best-priced resale offers.

The question we tried to answer:

**Can we spot which fashion trend is rising on the resale market by combining social media metrics and search interest?**

## Data sources

Good fashion data is hard to collect, especially when you want emerging trends and not the ones that are already everywhere. We did not want data from big brands because they are already late on trends, so we looked for sources where trends appear early:

- Fashion magazines: we scrape article titles from French and European media (Vogue, Stylist, Grazia, Madame Figaro...) plus streetwear media (Hypebeast, Highsnobiety)
- Reddit: posts from 8 subreddits (r/fashion, r/streetwear, r/sneakers, r/femalefashionadvice, r/malefashionadvice, r/frugalmalefashion, r/femalefashion, r/womensfashion)
- Google Trends: search interest over time for the 30 best keywords. This source is optional: Google rate-limits the unofficial Trends API (HTTP 429). When it does, the pipeline carries on without Google data. We don't reuse old cached results, because scoring today's trends with months-old search data would be misleading.
- eBay API: listing titles and prices. The eBay sandbox returns no real listings, so when `EBAY_ENV=sandbox` (the default in `.env.example`) the pipeline uses Marktplaats instead, eBay's Dutch second-hand marketplace, which has a public API with the same listing data. Its offers are in Dutch. Searches are limited to the fashion categories (women's clothing, men's clothing, jewellery and bags), otherwise "robe" returns ROBE stage lights and "zara" returns 1943 stamps from the city of Zara. With production eBay keys (`EBAY_ENV=production`) the pipeline uses eBay France.
- Vinted: second-hand listings. This source is optional: Vinted's API is behind Cloudflare bot protection and often answers 403. When it does, the pipeline logs a warning and carries on without Vinted data, and its weight in the trend score is shared out among the other sources.

All these APIs are pretty unstable and often change their policy or rate limits. That's why we have fixtures (fake sample data) that the pipeline can use when an API call fails. They are off by default (`USE_FIXTURES_IF_SOURCE_FAIL=false`), and every row carries a `source_is_fixture` flag so fixture data is never mistaken for live data.

## Pipeline

Everything is orchestrated by one Airflow DAG, `dags/fashion_resale_opportunity_pipeline.py`, that runs daily. It works in 5 phases:

1. **Raw ingestion**: we scrape media titles and marketplace listing titles. Everything is stored as JSON in the raw layer of the data lake.
2. **Keyword extraction**: our whole work depends on having relevant keywords, so we extract them from the raw text with a custom process that only keeps fashion keywords.
3. **Multi-source ingestion**: with these keywords we fetch more data (Google Trends interest, Reddit post counts, eBay prices...) to get more parameters to score the trends.
4. **Formatting and aggregation**: Spark normalises the raw JSON into Parquet and CSV (timestamps in UTC), then combines all the source scores per keyword (see [How we score the trends](#how-we-score-the-trends)) and selects 3 offers for each top-10 keyword. Each day's scores are also saved in a history partitioned by run date, and `trend_score_delta` shows how much a keyword moved since the previous run.
5. **Quality checks, export and indexing**: `quality_checks.py` checks the combined data (no empty keywords, scores between 0 and 1, valid prices, at most 3 offers per keyword) and stops the DAG if something is wrong. It also reports the share of fixture rows in each dataset. Final results are then exported as a ZIP, whose `run_metadata.json` includes that fixture share, and indexed into Elasticsearch for the Kibana dashboard.

The data lake is local and mirrored in MinIO:

| Layer     | Content                                  |
|-----------|------------------------------------------|
| raw       | Original ingested JSON                   |
| formatted | Cleaned and normalised Parquet and CSV   |
| combined  | Aggregated multi-source scores and top offers, plus a daily history partitioned by `run_date` |
| exports   | Final ZIP and CSV files                  |

## How we find the trends

**Seeds are just a bootstrap.** `config/seed_keywords.yml` contains 16 fashion terms we picked by hand. They are only used as marketplace search queries on the first run, when the data lake is empty. After that, each run searches the marketplace with the keywords extracted by the previous run. Seeds keep a small +3 bonus, so they can only stay on top if the media and the listings keep mentioning them.

**Automatic discovery.** Then the pipeline finds new terms by itself:

1. It collects article titles from the fashion media, where new aesthetics, garments and micro-trends often show up first.
2. In parallel it takes eBay listing titles, which are good for product-level trends (mesh flats, Samba Adidas, cardigan...). Phrases containing a word of the search query are ignored: a listing found by searching "samba adidas" always contains "samba", so counting it would only measure what we searched for. Only the other terms in the titles count.
3. `candidate_phrases()` in `keywords.py` generates all unigrams, bigrams and trigrams from each title. For example from "gorpcore s'invite dans les défilés printemps 2026" we get `gorpcore`, `défilés printemps`, `printemps 2026`...
4. `useful_keyword()` keeps a candidate only if at least one word is in our fashion vocabulary (`config/fashion_vocabulary.yml`, garments, materials, aesthetics, silhouettes, brands). This removes the noise like verbs, dates or generic words.
5. Each keyword gets a score: +2 per media article mention, +1 per eBay title mention, +3 for seeds.

Example: if "gorpcore" appears in 8 articles it gets 8 × 2 = 16 points, while a seed like "mesh flats" that is in no article only keeps its +3. So gorpcore goes above mesh flats even if it was never in the seed list. Seeds help the pipeline start, but they don't decide the trends.

## How we score the trends

Each source gives a score between 0 and 1 per keyword (min-max normalised across keywords), and the `trend_score` is their weighted average:

| Signal | Source | Weight |
|---|---|---|
| Search momentum: growth of the average interest over the last 13 weeks vs the 52 weeks before | Google Trends | 0.20 |
| Average search interest | Google Trends | 0.10 |
| Number of posts (score and comments are not available without Reddit API keys) | Reddit | 0.20 |
| Mentions in fashion media | Media | 0.20 |
| Articles | Streetwear media | 0.10 |
| Number of relevant listings | Vinted | 0.15 |
| Number of relevant listings | eBay / Marktplaats | 0.05 |

- **Momentum over popularity.** The goal is to catch rising trends, not terms that are already everywhere, so momentum gets the largest Google weight. The day-to-day change, `trend_score_delta`, adds a second growth signal once the history covers several days.
- **Missing sources don't count as zero.** When a source is blocked (often Google Trends or Vinted), its weight is shared out among the sources that returned data. The `score_sources` column lists which sources were used for that run.
- **Only relevant listings count.** A listing is kept only if its title contains the keyword as a whole word, so "robe" does not match "Rob Kemps tickets".

**Offer selection.** For each top-10 keyword we keep the 3 cheapest relevant listings, ignoring prices below a quarter of that keyword's median price (placeholder 1 € listings, accessories). This gives the best deals that are actually the item.

## Results

On our runs, cardigan, mocassins, trench coat and Samba Adidas were in the top positions on most metrics. Samba Adidas had a very high Google Trends interest and good resale prices, and leopard print had the highest resale price even with a moderate search interest (niche but valuable demand). Brands dominate media mentions, while products dominate search and resale.

## Run it

You need Docker. Copy the env file and fill in your eBay and Reddit keys:

```bash
cp .env.example .env
docker compose up -d --build
```

Then:

- Airflow: http://localhost:8085 (admin / admin), trigger the `fashion_resale_opportunity_pipeline` DAG
- MinIO: http://localhost:9003 (minioadmin / minioadmin)
- Kibana: http://localhost:5602, the dashboard can be created with `python scripts/create_kibana_dashboard.py`
- Spark master UI: http://localhost:8090

If you don't have API keys, set `USE_FIXTURES_IF_SOURCE_FAIL=true` to run with the fixtures. The `check_data_quality` task logs how much of each dataset came from fixtures.

Tests run outside Docker. PySpark 3.5 needs **Python 3.11 or 3.12** and **Java 17** (it does not work with Python 3.13+ or Java 21+):

```bash
python3.11 -m venv .venv && source .venv/bin/activate
pip install -r requirements-dev.txt
export JAVA_HOME=$(/usr/libexec/java_home -v 17)   # macOS; on Linux point JAVA_HOME to a JDK 17
export PYSPARK_PYTHON=$(which python)
pytest
```

## Limitations and future work

- No TikTok or Instagram data. Today that's where micro-trends are born, so it's the biggest blind spot of the project.
- Media scraping depends on HTML structures that change often, and many fashion sites have no stable RSS feed, so ingestion can break without warning.
- Without production eBay keys, the marketplace data comes from Marktplaats (Netherlands), not from the French market.
- Elle (elle.fr) blocks scraping, so we use Stylist instead.
- Vinted blocks automated requests most of the time, so Vinted data is often missing.
- Google Trends is queried through its unofficial web API, which can change or rate-limit without warning. The official Google Trends API (in alpha) would be the proper fix.

Next steps would be to extend the fashion vocabulary to catch more niche aesthetics, and add real-time monitoring and alerting to react to API failures or sudden spikes in trend signals.

## Sources

- Vogue France: https://www.vogue.fr
- Stylist: https://www.stylist.fr
- Marktplaats: https://www.marktplaats.nl
- PyTrends: https://github.com/GeneralMills/pytrends
- ThredUp 2026 Resale Report: https://cf-assets-tup.thredup.com/resale_report/2026/ThredUp_Resale_Report_2026.pdf
