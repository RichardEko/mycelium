CREATE TABLE IF NOT EXISTS articles (
    id              TEXT PRIMARY KEY,
    title           TEXT,
    source          TEXT,
    topics          JSONB DEFAULT '[]',
    composite_score DOUBLE PRECISION DEFAULT 0,
    processed_at    TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS articles_composite_score_idx ON articles (composite_score DESC);
CREATE INDEX IF NOT EXISTS articles_processed_at_idx    ON articles (processed_at DESC);
