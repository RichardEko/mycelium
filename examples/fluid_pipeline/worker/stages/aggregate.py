"""Stage D — Aggregate: write final record to PostgreSQL, return summary."""

import json
import logging
import os
import time

log = logging.getLogger("aggregate")

POSTGRES_DSN = os.environ.get(
    "POSTGRES_DSN",
    "postgresql://pipeline:pipeline@postgres:5432/pipeline",
)


def aggregate_article(payload: dict) -> dict:
    scores = payload.get("scores", {})

    try:
        import psycopg2
        conn = psycopg2.connect(POSTGRES_DSN)
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO articles
                        (id, title, source, topics, composite_score, processed_at)
                    VALUES (%s, %s, %s, %s, %s, NOW())
                    ON CONFLICT (id) DO UPDATE
                        SET composite_score = EXCLUDED.composite_score,
                            processed_at    = EXCLUDED.processed_at
                    """,
                    (
                        payload["id"],
                        payload.get("title", "")[:500],
                        payload.get("source", ""),
                        json.dumps(payload.get("topics", [])),
                        scores.get("composite", 0.0),
                    ),
                )
        conn.close()
    except Exception as exc:
        log.warning("postgres write skipped for %s: %s", payload.get("id"), exc)

    return {
        "id":              payload["id"],
        "title":           payload.get("title", "")[:120],
        "topics":          payload.get("topics", []),
        "composite_score": scores.get("composite", 0.0),
        "done_at":         time.time(),
    }
