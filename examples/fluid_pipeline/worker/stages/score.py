"""Stage C — Score: relevance, urgency, credibility. Configurable sleep simulates LLM.

Set STAGE_C_SLEEP to a higher value to make the bottleneck clearly visible
in the flow diagram — all 10 workers will pile up here, demonstrating how
the fluid pool naturally concentrates resources at the slowest stage.
"""

import os
import time


STAGE_C_SLEEP = float(os.environ.get("STAGE_C_SLEEP", "0.3"))

# Credibility tier by known source quality
SOURCE_RANK = {
    "carbonbrief.org": 0.95, "ft.com": 0.92, "reuters.com": 0.90,
    "guardian.com": 0.88,    "bbc.co.uk": 0.87, "theconversation.com": 0.85,
    "independent.co.uk": 0.75, "foodnavigator.com": 0.70,
    "plantbasednews.org": 0.65, "vegnews.com": 0.60,
}


def score_article(payload: dict) -> dict:
    # Simulate LLM / NLP model inference latency
    time.sleep(STAGE_C_SLEEP)

    entities = payload.get("entities", [])
    topics   = payload.get("topics", [])
    keywords = payload.get("keywords", [])
    source   = payload.get("source", "unknown")

    # Relevance: fraction of Plant-Based Treaty lexicon entities found
    relevance = min(1.0, len(entities) / 6.0)

    # Urgency: topic density + keyword richness
    urgency = min(1.0, len(topics) * 0.15 + len(keywords) * 0.04)

    # Credibility: source-tier lookup with fallback hash
    credibility = SOURCE_RANK.get(source, (abs(hash(source)) % 60 + 30) / 100.0)

    composite = round(0.5 * relevance + 0.3 * urgency + 0.2 * credibility, 4)

    return {
        **payload,
        "scores": {
            "relevance":   round(relevance, 4),
            "urgency":     round(urgency, 4),
            "credibility": round(credibility, 4),
            "composite":   composite,
        },
        "scored_at": time.time(),
    }
