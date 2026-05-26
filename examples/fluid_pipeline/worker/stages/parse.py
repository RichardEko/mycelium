"""Stage A — Parse: strip markup, normalise whitespace, extract title and body."""

import html
import re
import time


def parse_article(payload: dict) -> dict:
    raw = payload.get("raw", "")

    # Strip any HTML tags
    text = re.sub(r"<[^>]+>", " ", raw)
    text = html.unescape(text)
    text = re.sub(r"\s+", " ", text).strip()

    # First sentence → title; remainder → body
    sentences = [s.strip() for s in re.split(r"(?<=[.!?])\s+", text) if len(s.strip()) > 10]
    title = sentences[0][:200] if sentences else text[:200]
    body  = " ".join(sentences[1:])[:2000] if len(sentences) > 1 else text

    return {
        "id":        payload["id"],
        "title":     title,
        "body":      body,
        "source":    payload.get("source", "unknown"),
        "date":      payload.get("date", ""),
        "tag":       payload.get("tag", ""),
        "parsed_at": time.time(),
    }
