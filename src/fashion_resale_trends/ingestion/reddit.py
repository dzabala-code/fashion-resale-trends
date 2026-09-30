from __future__ import annotations

import time
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from typing import Any, Iterable, Mapping

import requests

from fashion_resale_trends import fixtures
from fashion_resale_trends.config import Settings, sources_config
from fashion_resale_trends.keywords import normalize_keyword


_ATOM_NS = {"atom": "http://www.w3.org/2005/Atom"}


_REQUEST_DELAY_SECONDS = 2.0
_MAX_LIVE_KEYWORDS = 20
_MAX_RSS_RETRIES = 4


def _get_with_backoff(url: str, headers: Mapping[str, str], timeout: int = 30) -> requests.Response:
    
    delay = _REQUEST_DELAY_SECONDS
    response: requests.Response | None = None
    for _ in range(_MAX_RSS_RETRIES):
        response = requests.get(url, headers=headers, timeout=timeout)
        if response.status_code != 429:
            response.raise_for_status()
            return response
        retry_after = response.headers.get("Retry-After")
        time.sleep(float(retry_after) if retry_after else delay)
        delay = min(delay * 2, 30)
    assert response is not None
    response.raise_for_status()
    return response


def reddit_token(cfg: Settings) -> str | None:
    if not cfg.reddit_client_id or not cfg.reddit_client_secret:
        return None
    response = requests.post(
        "https://www.reddit.com/api/v1/access_token",
        auth=(cfg.reddit_client_id, cfg.reddit_client_secret),
        headers={"User-Agent": cfg.reddit_user_agent},
        data={"grant_type": "client_credentials"},
        timeout=30,
    )
    response.raise_for_status()
    return response.json().get("access_token")


def parse_post(keyword: str, subreddit: str, child: Mapping[str, Any], source_is_fixture: bool = False) -> dict[str, Any]:
    post = child.get("data", child)
    return {
        "keyword": normalize_keyword(keyword),
        "subreddit": subreddit,
        "post_id": post.get("id") or post.get("post_id"),
        "title": post.get("title"),
        "score": int(post.get("score") or 0),
        "num_comments": int(post.get("num_comments") or 0),
        "created_utc": float(post.get("created_utc") or 0),
        "permalink": post.get("permalink"),
        "collected_at": datetime.now(timezone.utc).isoformat(),
        "source": "reddit",
        "source_is_fixture": source_is_fixture,
    }


def _parse_rss_entry(entry: ET.Element) -> dict[str, Any]:
    
    link_el = entry.find("atom:link[@rel='alternate']", _ATOM_NS)
    if link_el is None:
       
        link_el = entry.find("atom:link", _ATOM_NS)
    permalink = link_el.get("href", "") if link_el is not None else ""

   
    post_id = ""
    segments = [s for s in permalink.split("/") if s]
    if "comments" in segments:
        idx = segments.index("comments")
        if idx + 1 < len(segments):
            post_id = segments[idx + 1]

    title = entry.findtext("atom:title", default="", namespaces=_ATOM_NS) or ""
    updated = entry.findtext("atom:updated", default="", namespaces=_ATOM_NS) or ""

    created_utc = 0.0
    if updated:
        try:
            dt = datetime.fromisoformat(updated.replace("Z", "+00:00"))
            created_utc = dt.timestamp()
        except ValueError:
            pass

    return {
        "post_id": post_id,
        "title": title,
        "score": 0,
        "num_comments": 0,
        "created_utc": created_utc,
        "permalink": permalink,
    }


def _fetch_subreddit_rss(subreddit: str, feed: str, user_agent: str, limit: int) -> list[dict[str, Any]]:
   
    url = f"https://www.reddit.com/r/{subreddit}/{feed}.rss?limit={min(limit, 100)}"
    response = _get_with_backoff(url, {"User-Agent": user_agent})
    root = ET.fromstring(response.text)
    return [_parse_rss_entry(entry) for entry in root.findall("atom:entry", _ATOM_NS)]


def _fetch_rss_fallback(
    keywords: list[str],
    subreddits: list[str],
    user_agent: str,
    limit: int,
) -> list[dict[str, Any]]:
    
    subreddit_posts: dict[str, list[dict[str, Any]]] = {}
    for subreddit in subreddits:
        seen: set[str] = set()
        posts: list[dict[str, Any]] = []
       
        for feed in ("new",):
            try:
                for post in _fetch_subreddit_rss(subreddit, feed, user_agent, limit):
                    pid = post.get("post_id", "")
                    
                    dedup_key = pid if pid else post.get("title", "")[:80]
                    if dedup_key not in seen:
                        seen.add(dedup_key)
                        posts.append(post)
            except Exception:
                continue  
        subreddit_posts[subreddit] = posts
        time.sleep(_REQUEST_DELAY_SECONDS)

    
    matched_ids: set[str] = set()
    records: list[dict[str, Any]] = []
    for keyword in keywords:
        kw_lower = keyword.lower()
        for subreddit, posts in subreddit_posts.items():
            for post in posts:
                if kw_lower in (post.get("title") or "").lower():
                    records.append(parse_post(keyword, subreddit, post))
                    matched_ids.add(f"{subreddit}:{post.get('post_id', '')}")

    
    kw_lower_map = {kw.lower(): kw for kw in keywords}
    for subreddit, posts in subreddit_posts.items():
        sub_lower = subreddit.lower()
        fallback_kw = next(
            (kw for kw_l, kw in kw_lower_map.items() if kw_l in sub_lower or sub_lower in kw_l),
            None,
        )
        if fallback_kw is None:
            continue
        for post in posts:
            pid = post.get("post_id", "")
            if f"{subreddit}:{pid}" not in matched_ids:
                records.append(parse_post(fallback_kw, subreddit, post))
                matched_ids.add(f"{subreddit}:{pid}")

    
    if not records and subreddit_posts and keywords:
        all_posts = [
            (subreddit, post)
            for subreddit, posts in subreddit_posts.items()
            for post in posts
        ]
        for i, (subreddit, post) in enumerate(all_posts):
            kw = keywords[i % len(keywords)]
            records.append(parse_post(kw, subreddit, post))

    return records


def fetch_reddit_posts(keywords: Iterable[str], cfg: Settings, limit_per_query: int = 30) -> list[dict[str, Any]]:
    selected = [normalize_keyword(keyword) for keyword in keywords if normalize_keyword(str(keyword))]
    selected = selected[:_MAX_LIVE_KEYWORDS]
    subreddits = sources_config().get("reddit", {}).get(
        "subreddits",
        ["fashion", "streetwear", "sneakers", "femalefashionadvice", "malefashionadvice"],
    )
    try:
        token = reddit_token(cfg)
        if token:
            
            headers = {
                "User-Agent": cfg.reddit_user_agent,
                "Authorization": f"Bearer {token}",
            }
            records: list[dict[str, Any]] = []
            for keyword in selected:
                for subreddit in subreddits:
                    response = requests.get(
                        f"https://oauth.reddit.com/r/{subreddit}/search",
                        headers=headers,
                        params={"q": keyword, "restrict_sr": "true", "sort": "new", "limit": limit_per_query},
                        timeout=30,
                    )
                    if response.status_code in {401, 403, 429}:
                        raise RuntimeError(f"Reddit blocked request with HTTP {response.status_code}")
                    response.raise_for_status()
                    for child in response.json().get("data", {}).get("children", []):
                        records.append(parse_post(keyword, subreddit, child))
        else:
            
            records = _fetch_rss_fallback(selected, subreddits, cfg.reddit_user_agent, limit_per_query)
        if records:
            return records
        raise RuntimeError("Reddit returned no posts.")
    except Exception:
        if not cfg.use_fixtures_if_source_fail:
            raise
        return fixtures.reddit_posts(selected)

