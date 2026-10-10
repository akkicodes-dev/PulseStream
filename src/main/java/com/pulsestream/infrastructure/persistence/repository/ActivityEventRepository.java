package com.pulsestream.infrastructure.persistence.repository;

import com.pulsestream.domain.event.EventType;
import com.pulsestream.infrastructure.persistence.entity.ActivityEventEntity;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.Optional;
import java.util.UUID;

/**
 * Repository for {@link ActivityEventEntity}.
 *
 * <p>The unique constraint on {@code eventId} is enforced at the database level.
 * This repository provides application-level duplicate detection via
 * {@link #existsByEventId(String)}.
 *
 * <p>Pagination is used for all user-facing queries to prevent unbounded result sets.
 */
@Repository
public interface ActivityEventRepository extends JpaRepository<ActivityEventEntity, UUID> {

    boolean existsByEventId(String eventId);

    Optional<ActivityEventEntity> findByEventId(String eventId);

    Page<ActivityEventEntity> findByUserIdOrderByEventTimestampDesc(String userId, Pageable pageable);

    Page<ActivityEventEntity> findByUserIdAndEventTypeOrderByEventTimestampDesc(
            String userId, EventType eventType, Pageable pageable);
}
