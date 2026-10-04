from __future__ import annotations

import re
from collections import Counter, defaultdict
from typing import Iterable, Mapping, Any

from fashion_resale_trends.config import fashion_vocabulary, seed_keywords, stopwords as _stopwords_fn


def stopwords() -> set[str]:
    return _stopwords_fn()


TOKEN_RE = re.compile(r"[a-zA-ZÀ-ÿ0-9][a-zA-ZÀ-ÿ0-9' -]{1,}")


def normalize_keyword(value: str) -> str:
    normalized = re.sub(r"\s+", " ", value.lower().strip())
    normalized = normalized.replace("’", "'")
    return normalized.strip(" -'")


def title_contains_keyword(title: str, keyword: str) -> bool:
    return normalize_keyword(keyword) in normalize_keyword(title)


def candidate_phrases(text: str, blocked_words: set[str] | None = None) -> list[str]:
    blocked = blocked_words if blocked_words is not None else stopwords()
    tokens = [
        normalize_keyword(match.group(0))
        for match in TOKEN_RE.finditer(text)
        if normalize_keyword(match.group(0))
    ]
    words: list[str] = []
    for token in tokens:
        words.extend(part for part in re.split(r"[^a-zA-ZÀ-ÿ0-9']+", token) if part)
    words = [word for word in words if len(word) > 2 and word not in blocked]

    phrases: list[str] = []
    phrases.extend(words)
    phrases.extend(" ".join(words[index:index + 2]) for index in range(max(0, len(words) - 1)))
    phrases.extend(" ".join(words[index:index + 3]) for index in range(max(0, len(words) - 2)))
    return [phrase for phrase in phrases if useful_keyword(phrase, blocked)]


def _contains_fashion_term(keyword: str, vocab: set[str]) -> bool:
   
    normalized = keyword.lower().strip()
    if normalized in vocab:
        return True
    parts = normalized.split()
    if len(parts) > 1:
        # Vérifie les mots individuels
        if any(p in vocab for p in parts):
            return True
        # Vérifie les bigrammes
        for i in range(len(parts) - 1):
            if f"{parts[i]} {parts[i + 1]}" in vocab:
                return True
        # Vérifie les trigrammes
        for i in range(len(parts) - 2):
            if f"{parts[i]} {parts[i + 1]} {parts[i + 2]}" in vocab:
                return True
    return False


def useful_keyword(keyword: str, blocked_words: set[str] | None = None) -> bool:
   
    blocked = blocked_words if blocked_words is not None else stopwords()
    normalized = normalize_keyword(keyword)
    if not normalized or normalized in blocked:
        return False
    if len(normalized) < 4:
        return False
    
    if normalized.startswith(("l'", "d'", "s'", "c'", "j'", "n'", "m'")):
        return False

    parts = normalized.split()
    vocab = fashion_vocabulary()

    if len(parts) == 1:
        
        return _contains_fashion_term(normalized, vocab)

    
    meaningful = [p for p in parts if len(p) > 3 and p not in blocked]
    if not meaningful:
        return False
    return _contains_fashion_term(normalized, vocab)


def discover_keywords_from_offers(
    offers: Iterable[Mapping[str, Any]],
    limit: int = 50,
    *,
    discovery_source: str = "marktplaats_keyword_discovery",
    marketplace: str = "marktplaats",
) -> list[dict[str, Any]]:
    """Extract recurring fashion phrases from marketplace listing titles.

    Phrases containing a word of the search query that found a listing are not
    counted: a listing found by searching "samba adidas" always contains "samba" and
    "adidas", so counting them would only measure what we searched for, not what is
    trending. Only the other terms that appear in the titles are kept.
    """
    from datetime import datetime, timezone

    offer_list = list(offers)
    counter: Counter[str] = Counter()
    for offer in offer_list:
        title = str(offer.get("title", ""))
        query_words = set(normalize_keyword(str(offer.get("keyword", ""))).split())
        for phrase in candidate_phrases(title):
            if query_words & set(phrase.split()):
                continue
            counter[phrase] += 1
    records = []
    is_fixture = bool(offer_list) and all(bool(offer.get("source_is_fixture")) for offer in offer_list)
    for keyword, count in counter.most_common(limit):
        if useful_keyword(keyword):
            records.append(
                {
                    "keyword": keyword,
                    "mention_count": count,
                    "source": discovery_source,
                    "marketplace": marketplace,
                    "collected_at": datetime.now(timezone.utc).isoformat(),
                    "source_is_fixture": is_fixture,
                }
            )
    return records


def extract_keywords(
    media_records: Iterable[Mapping[str, Any]],
    marketplace_keyword_records: Iterable[Mapping[str, Any]],
    limit: int = 50,
) -> list[dict[str, Any]]:
    blocked = stopwords()
    configured_seeds = seed_keywords()
    scores: Counter[str] = Counter()
    sources: dict[str, set[str]] = defaultdict(set)

    for seed in configured_seeds:
        if useful_keyword(seed, blocked):
            scores[normalize_keyword(seed)] += 3
            sources[normalize_keyword(seed)].add("seed")

    for record in media_records:
        source = str(record.get("source", "media"))
        title = str(record.get("title", ""))
        for phrase in candidate_phrases(title, blocked):
            scores[phrase] += 2
            sources[phrase].add(source)

    for record in marketplace_keyword_records:
        keyword = normalize_keyword(str(record.get("keyword", "")))
        if useful_keyword(keyword, blocked):
            scores[keyword] += int(record.get("mention_count", 1) or 1)
            marketplace = str(record.get("marketplace") or "marktplaats")
            sources[keyword].add(marketplace)

    ranked = []
    for keyword, score in scores.most_common():
        ranked.append(
            {
                "keyword": keyword,
                "keyword_score": float(score),
                "source_count": len(sources[keyword]),
                "sources": sorted(sources[keyword]),
            }
        )
        if len(ranked) >= limit:
            break
    return ranked

