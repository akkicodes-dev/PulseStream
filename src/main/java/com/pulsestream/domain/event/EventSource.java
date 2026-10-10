package com.pulsestream.domain.event;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

import java.util.Arrays;

/**
 * The originating source platform for an activity event.
 *
 * <p>Supported platforms: WEB, MOBILE, API.
 * Mirrors the CHECK constraint in V2 Flyway migration ('web', 'mobile', 'api')
 * via {@link com.pulsestream.infrastructure.persistence.converter.EventSourceConverter}.
 */
public enum EventSource {
    WEB,
    MOBILE,
    API;

    @JsonCreator
    public static EventSource fromString(String value) {
        if (value == null) {
            return null;
        }
        for (EventSource source : EventSource.values()) {
            if (source.name().equalsIgnoreCase(value.trim())) {
                return source;
            }
        }
        throw new IllegalArgumentException(
                "Invalid source: '" + value + "'. Supported sources: " + Arrays.toString(EventSource.values())
        );
    }

    @JsonValue
    public String toValue() {
        return name();
    }
}
