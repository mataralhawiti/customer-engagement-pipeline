-- Content catalogue
CREATE TABLE IF NOT EXISTS content (
    id              UUID PRIMARY KEY,
    slug            TEXT UNIQUE NOT NULL,
    title           TEXT        NOT NULL,
    content_type    TEXT CHECK (content_type IN ('podcast', 'newsletter', 'video')),
    length_seconds  INTEGER,
    publish_ts      TIMESTAMPTZ NOT NULL
);
 
-- Raw engagement telemetry
CREATE TABLE IF NOT EXISTS engagement_events (
    id           BIGSERIAL PRIMARY KEY,
    content_id   UUID REFERENCES content(id),
    user_id      UUID,
    event_type   TEXT CHECK (event_type IN ('play', 'pause', 'finish', 'click')),
    event_ts     TIMESTAMPTZ NOT NULL,
    duration_ms  INTEGER,      -- nullable for events without duration
    device       TEXT,         -- e.g. "ios", "web‑safari"
    raw_payload  JSONB         -- anything extra the client sends
);