-- V4: Add compound and partial indexes for M4 real-time analytics and session aggregation

-- Compound indexes for analytical time-window aggregations on activity_events
CREATE INDEX idx_ae_ts_type   ON activity_events(event_timestamp DESC, event_type);
CREATE INDEX idx_ae_ts_source ON activity_events(event_timestamp DESC, source);

-- Partial index for active sessions: optimizes active session queries and 60-second inactivity expiration sweep
CREATE INDEX idx_us_active_last_seen ON user_sessions(last_seen_at DESC) WHERE is_active = TRUE;
