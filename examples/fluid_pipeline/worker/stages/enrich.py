"""Stage B — Enrich: TF-IDF keyword extraction, entity tagging, topic clustering."""

import re
import time


PLANT_LEXICON = {
    "plant-based", "vegan", "vegetarian", "meat", "dairy", "livestock",
    "agriculture", "emissions", "protein", "diet", "climate", "carbon",
    "greenhouse", "sustainability", "animal", "factory", "food", "crops",
}

TOPIC_CLUSTERS = {
    "climate":       {"climate", "carbon", "emissions", "greenhouse", "warming", "droughts"},
    "food":          {"food", "diet", "protein", "plant-based", "meat", "dairy", "crops"},
    "policy":        {"policy", "government", "regulation", "legislation", "subsidies"},
    "health":        {"health", "nutrition", "study", "research", "cancer", "obesity"},
    "economics":     {"prices", "market", "sector", "funding", "farmer", "supply"},
    "animal-welfare":{"animal", "welfare", "factory", "livestock", "cattle", "pigs"},
}


def _tfidf_keywords(text: str, top_n: int = 10) -> list[str]:
    words = re.findall(r"\b[a-z]{4,}\b", text.lower())
    freq: dict[str, int] = {}
    for w in words:
        freq[w] = freq.get(w, 0) + 1
    # Simple IDF proxy: penalise very common words
    stopwords = {"this", "that", "with", "from", "have", "been", "they", "their",
                 "will", "into", "also", "more", "some", "than", "would", "could"}
    return [w for w in sorted(freq, key=freq.get, reverse=True) if w not in stopwords][:top_n]


def enrich_article(payload: dict) -> dict:
    text    = (payload.get("title", "") + " " + payload.get("body", "")).lower()
    keywords = _tfidf_keywords(text)
    kw_set   = set(keywords)

    entities = sorted(kw_set & PLANT_LEXICON)
    topics   = [t for t, words in TOPIC_CLUSTERS.items() if kw_set & words]

    return {
        **payload,
        "keywords":    keywords,
        "entities":    entities,
        "topics":      topics,
        "enriched_at": time.time(),
    }
