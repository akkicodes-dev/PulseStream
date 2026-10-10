-- V3: Create user_sessions table
-- Purpose: Tracks aggregated session state per user.
-- A session is defined by a sessionId provided by the client.
-- Sessions are closed either by an explicit LOGOUT event or by a configurable
-- inactivity timeout (default: 30 minutes) managed by Kafka Streams.
--
-- Design notes:
-- - session_id is client-provided (matches sessionId in activity_events).
-- - event_count is incremented as events arrive in this session.
-- - is_active flips to FALSE when the session is closed.
-- - ended_at is NULL while the session is still active.

CREATE TABLE user_sessions (
    id           UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    session_id   VARCHAR(255) NOT NULL,
    user_id      VARCHAR(255) NOT NULL,
    started_at   TIMESTAMPTZ  NOT NULL,
    last_seen_at TIMESTAMPTZ  NOT NULL,
    ended_at     TIMESTAMPTZ,
    event_count  INTEGER      NOT NULL DEFAULT 0,
    is_active    BOOLEAN      NOT NULL DEFAULT TRUE,
    created_at   TIMESTAMPTZ  NOT NULL DEFAULT NOW(),

    CONSTRAINT uq_user_sessions_session_id UNIQUE (session_id)
);

CREATE INDEX idx_us_user_id    ON user_sessions(user_id);
CREATE INDEX idx_us_session_id ON user_sessions(session_id);
CREATE INDEX idx_us_active     ON user_sessions(user_id) WHERE is_active = TRUE;
CREATE INDEX idx_us_started_at ON user_sessions(started_at DESC);

COMMENT ON TABLE user_sessions IS 'Aggregated session records. Updated as events arrive. Closed on LOGOUT or inactivity timeout.';
COMMENT ON COLUMN user_sessions.last_seen_at IS 'Timestamp of the most recent event in this session. Used to detect inactivity.';
COMMENT ON COLUMN user_sessions.event_count  IS 'Running count of events in this session. Incremented by the Kafka consumer.';
COMMENT ON COLUMN user_sessions.is_active    IS 'FALSE once the session has ended. Used to filter active sessions efficiently.';
