package com.pulsestream.infrastructure.persistence.entity;

import com.pulsestream.domain.event.EventSource;
import com.pulsestream.domain.event.EventType;
import jakarta.persistence.*;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.hibernate.annotations.CreationTimestamp;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

import java.time.OffsetDateTime;
import java.util.Map;
import java.util.UUID;

/**
 * JPA entity mapping to the {@code activity_events} table.
 *
 * <p>The {@code eventId} field carries a UNIQUE constraint that serves as the
 * idempotency key. Attempting to insert a duplicate eventId will throw a
 * {@link org.springframework.dao.DataIntegrityViolationException} which the
 * service layer catches and maps to a {@code 409 Conflict} response.
 *
 * <p>JSONB metadata is mapped via the Hypersistence Utils library's
 * {@link JsonBinaryType}, which correctly handles serialization/deserialization
 * of Map&lt;String, Object&gt; to/from PostgreSQL JSONB without manual conversion.
 */
@Entity
@Table(name = "activity_events",
        uniqueConstraints = @UniqueConstraint(name = "uq_activity_events_event_id", columnNames = "event_id"),
        indexes = {
                @Index(name = "idx_ae_user_id",       columnList = "user_id"),
                @Index(name = "idx_ae_event_type",    columnList = "event_type"),
                @Index(name = "idx_ae_event_ts_desc", columnList = "event_timestamp DESC"),
                @Index(name = "idx_ae_session_id",    columnList = "session_id")
        })
@Getter
@Setter
@NoArgsConstructor
public class ActivityEventEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    @Column(name = "id", updatable = false, nullable = false)
    private UUID id;

    @Column(name = "event_id", nullable = false, unique = true, length = 36)
    private String eventId;

    @Column(name = "user_id", nullable = false, length = 255)
    private String userId;

    @Column(name = "session_id", nullable = false, length = 255)
    private String sessionId;

    @Enumerated(EnumType.STRING)
    @Column(name = "event_type", nullable = false, length = 50)
    private EventType eventType;

    @Column(name = "event_timestamp", nullable = false)
    private OffsetDateTime eventTimestamp;

    @Column(name = "source", nullable = false, length = 50)
    private EventSource source;

    /**
     * Free-form event-specific metadata stored as JSONB.
     * Examples: productId, searchQuery, pageUrl, cartItemId.
     */
    @JdbcTypeCode(SqlTypes.JSON)
    @Column(name = "metadata", columnDefinition = "jsonb")
    private Map<String, Object> metadata;

    @Column(name = "processed", nullable = false)
    private boolean processed = false;

    @CreationTimestamp
    @Column(name = "received_at", nullable = false, updatable = false)
    private OffsetDateTime receivedAt;
}
