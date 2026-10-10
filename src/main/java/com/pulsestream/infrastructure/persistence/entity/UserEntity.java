package com.pulsestream.infrastructure.persistence.entity;

import jakarta.persistence.*;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.hibernate.annotations.CreationTimestamp;
import org.hibernate.annotations.UpdateTimestamp;

import java.time.OffsetDateTime;
import java.util.UUID;

/**
 * JPA entity mapping to the {@code users} table.
 *
 * <p>This entity is intentionally lightweight — it is a reference record,
 * not a full user profile. Full user management belongs in a separate
 * identity/auth service. PulseStream creates user records lazily when
 * the first activity event arrives for an unknown userId.
 */
@Entity
@Table(name = "users",
        uniqueConstraints = @UniqueConstraint(name = "uq_users_external_id", columnNames = "external_id"))
@Getter
@Setter
@NoArgsConstructor
public class UserEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    @Column(name = "id", updatable = false, nullable = false)
    private UUID id;

    @Column(name = "external_id", nullable = false, unique = true, length = 255)
    private String externalId;

    @CreationTimestamp
    @Column(name = "created_at", nullable = false, updatable = false)
    private OffsetDateTime createdAt;

    @UpdateTimestamp
    @Column(name = "updated_at", nullable = false)
    private OffsetDateTime updatedAt;

    public UserEntity(String externalId) {
        this.externalId = externalId;
    }
}
