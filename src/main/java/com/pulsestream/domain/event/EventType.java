package com.pulsestream.domain.event;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

import java.util.Arrays;

/**
 * Enumeration of all supported user activity event types.
 *
 * <p>This enum is the single source of truth for valid event types.
 * The PostgreSQL CHECK constraint in V2 migration mirrors these values.
 * If you add a new event type here, update the migration accordingly.
 *
 * <p>Interview note: keeping the enum here in the domain package (rather than in the
 * API layer) means the domain model is not coupled to HTTP serialization concerns.
 */
public enum EventType {
    LOGIN,
    LOGOUT,
    PAGE_VIEW,
    PRODUCT_VIEW,
    SEARCH,
    ADD_TO_CART,
    REMOVE_FROM_CART,
    PURCHASE,
    PROFILE_UPDATE;

    @JsonCreator
    public static EventType fromString(String value) {
        if (value == null) {
            return null;
        }
        for (EventType type : EventType.values()) {
            if (type.name().equalsIgnoreCase(value.trim())) {
                return type;
            }
        }
        throw new IllegalArgumentException(
                "Invalid eventType: '" + value + "'. Supported types: " + Arrays.toString(EventType.values())
        );
    }

    @JsonValue
    public String toValue() {
        return name();
    }
}
