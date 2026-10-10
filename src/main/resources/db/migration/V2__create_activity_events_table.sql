-- V2: Create activity_events table
-- Purpose: Persists every validated activity event received by the ingestion API.
-- This is the source-of-truth record in PostgreSQL.
-- Elasticsearch holds a derived, denormalized copy for analytics queries.
--
-- Design notes:
-- - event_id is client-provided or server-generated. UNIQUE constraint = idempotency guard.
-- - metadata uses JSONB because event-type-specific fields vary widely.
-- - processed flag is set to TRUE after the Kafka consumer successfully indexes to Elasticsearch.
-- - received_at records when PulseStream received it; event_timestamp is the client-reported time.

CREATE TABLE activity_events (
    id              UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    event_id        VARCHAR(36) NOT NULL,
    user_id         VARCHAR(255) NOT NULL,
    session_id      VARCHAR(255) NOT NULL,
    event_type      VARCHAR(50)  NOT NULL,
    event_timestamp TIMESTAMPTZ  NOT NULL,
    source          VARCHAR(50)  NOT NULL,
    metadata        JSONB,
    processed       BOOLEAN      NOT NULL DEFAULT FALSE,
    received_at     TIMESTAMPTZ  NOT NULL DEFAULT NOW(),

    CONSTRAINT uq_activity_events_event_id UNIQUE (event_id),
    CONSTRAINT chk_activity_events_source  CHECK (source IN ('web', 'mobile', 'api')),
    CONSTRAINT chk_activity_events_type    CHECK (
        event_type IN (
            'LOGIN', 'LOGOUT', 'PAGE_VIEW', 'PRODUCT_VIEW',
            'SEARCH', 'ADD_TO_CART', 'REMOVE_FROM_CART',
            'PURCHASE', 'PROFILE_UPDATE'
        )
    )
);

-- Queries we expect to run frequently:
CREATE INDEX idx_ae_user_id        ON activity_events(user_id);
CREATE INDEX idx_ae_event_type     ON activity_events(event_type);
CREATE INDEX idx_ae_event_ts_desc  ON activity_events(event_timestamp DESC);
CREATE INDEX idx_ae_session_id     ON activity_events(session_id);
-- Partial index: find unprocessed events for reprocessing / admin
CREATE INDEX idx_ae_unprocessed    ON activity_events(received_at) WHERE processed = FALSE;

COMMENT ON TABLE activity_events IS 'Canonical record of every validated user activity event.';
COMMENT ON COLUMN activity_events.event_id IS 'Idempotency key — duplicate event_id is rejected.';
COMMENT ON COLUMN activity_events.metadata IS 'Event-type-specific fields stored as JSONB. Avoids schema explosion for varying event shapes.';
COMMENT ON COLUMN activity_events.processed IS 'TRUE after the Kafka consumer successfully writes the event to Elasticsearch.';
