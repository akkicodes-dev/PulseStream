-- V5: Add index-only scan optimization for user activity time-series and user history
-- Enables Index-Only Scan (Heap Fetches: 0) on time-series queries aggregating unique users
CREATE INDEX idx_ae_ts_user ON activity_events(event_timestamp DESC, user_id);

-- Optimizes user activity history retrieval and time-bounded user filtering
CREATE INDEX idx_ae_user_ts ON activity_events(user_id, event_timestamp DESC);
