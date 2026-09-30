from __future__ import annotations

import json
import time
from datetime import datetime, timezone
from statistics import mean
from typing import Any, Callable, Iterable, Mapping

import requests

from fashion_resale_trends import fixtures
from fashion_resale_trends.config import Settings, sources_config
from fashion_resale_trends.keywords import normalize_keyword
from fashion_resale_trends.storage import ObjectStore

_BASE_URL = "https://trends.google.com/trends"
_USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)
_REQUEST_DELAY_SECONDS = 8.0
_MAX_LIVE_KEYWORDS = 10
_BATCH_SIZE = 5
_MAX_RETRIES = 3
_SINGLE_KEYWORD_RETRIES = 2


def summarize_interest(keyword: str, rows: Iterable[Mapping[str, Any]]) -> dict[str, Any]:
    ordered = sorted(rows, key=lambda row: str(row.get("date", "")))
    values = [int(row.get("interest") or 0) for row in ordered]
    if not values:
        return {
            "keyword": normalize_keyword(keyword),
            "google_avg_interest": 0.0,
            "google_peak_interest": 0,
            "google_growth": 0.0,
        }
    first_window = values[: max(1, len(values) // 3)]
    last_window = values[-max(1, len(values) // 3):]
    return {
        "keyword": normalize_keyword(keyword),
        "google_avg_interest": float(mean(values)),
        "google_peak_interest": max(values),
        "google_growth": float(mean(last_window) - mean(first_window)),
    }


def _is_retryable_error(exc: Exception) -> bool:
    text = str(exc)
    return "429" in text or "500" in text


class _GoogleTrendsClient:
    """Minimal Google Trends HTTP client (no pandas / pytrends)."""

    def __init__(self, hl: str = "fr-FR", tz: int = 0, geo: str = "FR") -> None:
        self.hl = hl
        self.tz = tz
        self.geo = geo
        self.session = requests.Session()
        self.session.headers.update(
            {
                "accept-language": hl,
                "User-Agent": _USER_AGENT,
            }
        )
        self._interest_widget: dict[str, Any] = {}

    def _get_json(self, url: str, *, method: str = "GET", trim_chars: int = 0, **kwargs: Any) -> dict[str, Any]:
        response = self.session.request(method, url, timeout=30, **kwargs)
        if response.status_code in (429, 500):
            raise RuntimeError(f"Google Trends returned HTTP {response.status_code}")
        response.raise_for_status()
        content = response.text[trim_chars:]
        return json.loads(content)

    def _ensure_cookie(self) -> None:
        self.session.get(f"{_BASE_URL}/explore/?geo={self.geo or self.hl[-2:]}", timeout=30)

    def _build_payload(self, keywords: list[str], timeframe: str) -> None:
        payload = {
            "hl": self.hl,
            "tz": self.tz,
            "req": json.dumps(
                {
                    "comparisonItem": [
                        {"keyword": keyword, "time": timeframe, "geo": self.geo}
                        for keyword in keywords
                    ],
                    "category": 0,
                    "property": "",
                }
            ),
        }
        widgets = self._get_json(
            f"{_BASE_URL}/api/explore",
            method="POST",
            trim_chars=4,
            data=payload,
        ).get("widgets", [])
        for widget in widgets:
            if widget.get("id") == "TIMESERIES":
                self._interest_widget = widget
                return
        raise RuntimeError("Google Trends TIMESERIES widget missing.")

    def interest_over_time(self, keywords: list[str], timeframe: str) -> list[dict[str, Any]]:
        self._ensure_cookie()
        self._build_payload(keywords, timeframe)
        params = {
            "req": json.dumps(self._interest_widget["request"]),
            "token": self._interest_widget["token"],
            "tz": self.tz,
        }
        payload = self._get_json(
            f"{_BASE_URL}/api/widgetdata/multiline",
            trim_chars=5,
            params=params,
        )
        timeline = payload.get("default", {}).get("timelineData") or []
        collected_at = datetime.now(timezone.utc).isoformat()
        records: list[dict[str, Any]] = []
        for point in timeline:
            date_str = datetime.fromtimestamp(int(point["time"]), tz=timezone.utc).date().isoformat()
            values = point.get("value") or []
            if isinstance(values, str):
                values = [int(part) for part in values.strip("[]").split(",") if part.strip()]
            for keyword, interest in zip(keywords, values):
                records.append(
                    {
                        "keyword": keyword,
                        "date": date_str,
                        "interest": int(interest),
                        "collected_at": collected_at,
                        "source": "google_trends",
                        "source_is_fixture": False,
                    }
                )
        return records


def _new_client(cfg: Settings) -> _GoogleTrendsClient:
    return _GoogleTrendsClient(hl="fr-FR", tz=0, geo=cfg.google_trends_geo)


def _fetch_batch_with_retry(
    client_factory: Callable[[], _GoogleTrendsClient],
    batch: list[str],
    timeframe: str,
    *,
    max_retries: int = _MAX_RETRIES,
) -> list[dict[str, Any]]:
    delay = _REQUEST_DELAY_SECONDS
    last_error: Exception | None = None
    for _ in range(max_retries):
        try:
            return client_factory().interest_over_time(batch, timeframe)
        except Exception as exc:
            last_error = exc
            if not _is_retryable_error(exc):
                raise
            time.sleep(delay)
            delay = min(delay * 2, 60)
    if last_error is not None:
        raise last_error
    return []


def _fetch_live_records(
    client_factory: Callable[[], _GoogleTrendsClient],
    keywords: list[str],
    timeframe: str,
) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    batches = [keywords[i : i + _BATCH_SIZE] for i in range(0, len(keywords), _BATCH_SIZE)]
    for batch_index, batch in enumerate(batches):
        batch_records: list[dict[str, Any]] = []
        try:
            batch_records = _fetch_batch_with_retry(client_factory, batch, timeframe)
        except Exception:
            for keyword in batch:
                try:
                    batch_records.extend(
                        _fetch_batch_with_retry(
                            client_factory,
                            [keyword],
                            timeframe,
                            max_retries=_SINGLE_KEYWORD_RETRIES,
                        )
                    )
                except Exception:
                    continue
                time.sleep(_REQUEST_DELAY_SECONDS)
        records.extend(batch_records)
        if batch_records:
            if batch_index < len(batches) - 1:
                time.sleep(_REQUEST_DELAY_SECONDS)
            continue
        # Google is blocking or rate-limiting; avoid waiting through every remaining batch.
        break
    return records


def _load_cached_raw_records(keywords: list[str], store: ObjectStore) -> list[dict[str, Any]]:
    prior = store.read_jsonl("raw", "search", "google_trends")
    if not prior:
        return []
    wanted = {normalize_keyword(keyword) for keyword in keywords}
    filtered = [
        row
        for row in prior
        if normalize_keyword(str(row.get("keyword", ""))) in wanted and not row.get("source_is_fixture")
    ]
    if not filtered:
        return []
    collected_at = datetime.now(timezone.utc).isoformat()
    return [
        {
            **row,
            "collected_at": collected_at,
            "source": "google_trends",
            "source_is_fixture": False,
            "source_is_cached": True,
        }
        for row in filtered
    ]


def fetch_google_trends(keywords: Iterable[str], cfg: Settings) -> list[dict[str, Any]]:
    selected = [normalize_keyword(keyword) for keyword in keywords if normalize_keyword(str(keyword))]
    selected = selected[:_MAX_LIVE_KEYWORDS]
    try:
        timeframe = sources_config().get("google_trends", {}).get("timeframe", "today 5-y")
        records = _fetch_live_records(lambda: _new_client(cfg), selected, timeframe)
        if records:
            return records
        cached = _load_cached_raw_records(selected, ObjectStore())
        if cached:
            print(
                json.dumps(
                    {
                        "event": "google_trends_cached_fallback",
                        "count": len(cached),
                        "keywords": sorted({row["keyword"] for row in cached}),
                    }
                )
            )
            return cached
        raise RuntimeError("Google Trends returned no records.")
    except Exception:
        if not cfg.use_fixtures_if_source_fail:
            cached = _load_cached_raw_records(selected, ObjectStore())
            if cached:
                print(
                    json.dumps(
                        {
                            "event": "google_trends_cached_fallback",
                            "count": len(cached),
                            "keywords": sorted({row["keyword"] for row in cached}),
                        }
                    )
                )
                return cached
            raise
        return fixtures.google_trends(selected)
