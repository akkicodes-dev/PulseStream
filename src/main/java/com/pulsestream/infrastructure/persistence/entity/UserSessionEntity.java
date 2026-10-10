package com.pulsestream.infrastructure.persistence.entity;

import jakarta.persistence.*;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.hibernate.annotations.CreationTimestamp;

import java.time.OffsetDateTime;
import java.util.UUID;

/**
 * JPA entity mapping to the {@code user_sessions} table.
 *
 * <p>Sessions are created when the first event with a new sessionId arrives.
 * The session remains active ({@code isActive = true}) until:
 * <ul>
 *   <li>A LOGOUT event is received for this session.</li>
 *   <li>The Kafka Streams session window expires (inactivity timeout).</li>
 * </ul>
 *
 * <p>The {@code eventCount} is incremented by the Kafka consumer on each event.
 * {@code lastSeenAt} is updated on every event in this session.
 */
@Entity
@Table(name = "user_sessions",
        uniqueConstraints = @UniqueConstraint(name = "uq_user_sessions_session_id", columnNames = "session_id"),
        indexes = {
                @Index(name = "idx_us_user_id",    columnList = "user_id"),
                @Index(name = "idx_us_session_id", columnList = "session_id"),
                @Index(name = "idx_us_started_at", columnList = "started_at DESC")
        })
@Getter
@Setter
@NoArgsConstructor
public class UserSessionEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    @Column(name = "id", updatable = false, nullable = false)
    private UUID id;

    @Column(name = "session_id", nullable = false, unique = true, length = 255)
    private String sessionId;

    @Column(name = "user_id", nullable = false, length = 255)
    private String userId;

    @Column(name = "started_at", nullable = false)
    private OffsetDateTime startedAt;

    @Column(name = "last_seen_at", nullable = false)
    private OffsetDateTime lastSeenAt;

    @Column(name = "ended_at")
    private OffsetDateTime endedAt;

    @Column(name = "event_count", nullable = false)
    private int eventCount = 0;

    @Column(name = "is_active", nullable = false)
    private boolean isActive = true;

    @CreationTimestamp
    @Column(name = "created_at", nullable = false, updatable = false)
    private OffsetDateTime createdAt;
}
