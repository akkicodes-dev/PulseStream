package com.pulsestream.infrastructure.persistence.converter;

import com.pulsestream.domain.event.EventSource;
import jakarta.persistence.AttributeConverter;
import jakarta.persistence.Converter;

import java.util.Locale;

/**
 * JPA AttributeConverter mapping {@link EventSource} enum to lowercase database string ('web', 'mobile', 'api').
 *
 * <p>Ensures compatibility with PostgreSQL check constraint:
 * {@code chk_activity_events_source CHECK (source IN ('web', 'mobile', 'api'))}.
 */
@Converter(autoApply = true)
public class EventSourceConverter implements AttributeConverter<EventSource, String> {

    @Override
    public String convertToDatabaseColumn(EventSource attribute) {
        return attribute == null ? null : attribute.name().toLowerCase(Locale.ROOT);
    }

    @Override
    public EventSource convertToEntityAttribute(String dbData) {
        return dbData == null ? null : EventSource.fromString(dbData);
    }
}
