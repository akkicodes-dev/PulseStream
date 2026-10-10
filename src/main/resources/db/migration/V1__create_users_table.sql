-- V1: Create users table
-- Purpose: Stores a lightweight reference to users that have generated activity events.
-- external_id is the client-provided user identifier (e.g., "user-123", a UUID from the client system).
-- We do not force clients to use our internal ID format.

CREATE TABLE users (
    id          UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    external_id VARCHAR(255) NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT uq_users_external_id UNIQUE (external_id)
);

COMMENT ON TABLE users IS 'Lightweight user reference table populated lazily as activity events arrive.';
COMMENT ON COLUMN users.external_id IS 'Client-provided user identifier. Must be unique across the platform.';
COMMENT ON COLUMN users.updated_at IS 'Updated by application logic on each activity. Not database-trigger-managed to keep infra simple.';
