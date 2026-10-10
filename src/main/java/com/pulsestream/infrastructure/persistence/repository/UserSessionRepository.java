package com.pulsestream.infrastructure.persistence.repository;

import com.pulsestream.infrastructure.persistence.entity.UserSessionEntity;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

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

    long countByUserIdAndIsActiveTrue(String userId);
}
