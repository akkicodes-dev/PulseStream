package com.pulsestream.infrastructure.persistence.repository;

import com.pulsestream.infrastructure.persistence.entity.UserSessionEntity;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.time.OffsetDateTime;
import java.util.Optional;
import java.util.UUID;

/**
 * Repository for {@link UserSessionEntity}.
 */
@Repository
public interface UserSessionRepository extends JpaRepository<UserSessionEntity, UUID> {

    Optional<UserSessionEntity> findBySessionId(String sessionId);

    boolean existsBySessionId(String sessionId);

    Page<UserSessionEntity> findByUserIdOrderByStartedAtDesc(String userId, Pageable pageable);

    long countByUserId(String userId);

    long countByUserIdAndIsActiveTrue(String userId);

    Optional<UserSessionEntity> findFirstByUserIdAndIsActiveTrueOrderByLastSeenAtDesc(String userId);

    long countByStartedAtBetween(OffsetDateTime from, OffsetDateTime to);

    long countByIsActiveTrueAndLastSeenAtAfter(OffsetDateTime activeCutoff);

    Page<UserSessionEntity> findByIsActiveTrueAndLastSeenAtAfterOrderByLastSeenAtDesc(
            OffsetDateTime activeCutoff, Pageable pageable);

    @Modifying
    @Query("UPDATE UserSessionEntity u SET u.isActive = false, u.endedAt = u.lastSeenAt " +
           "WHERE u.isActive = true AND u.lastSeenAt <= :cutoff")
    int deactivateSessionsOlderThan(@Param("cutoff") OffsetDateTime cutoff);
}
