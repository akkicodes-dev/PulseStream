package com.pulsestream.infrastructure.persistence.repository;

import com.pulsestream.domain.event.EventType;
import com.pulsestream.infrastructure.persistence.entity.ActivityEventEntity;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.time.OffsetDateTime;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

/**
 * Repository for {@link ActivityEventEntity}.
 */
@Repository
public interface ActivityEventRepository extends JpaRepository<ActivityEventEntity, UUID> {

    boolean existsByEventId(String eventId);

    Optional<ActivityEventEntity> findByEventId(String eventId);

    Page<ActivityEventEntity> findByUserIdOrderByEventTimestampDesc(String userId, Pageable pageable);

    Page<ActivityEventEntity> findByUserIdAndEventTypeOrderByEventTimestampDesc(
            String userId, EventType eventType, Pageable pageable);

    long countByEventTimestampBetween(OffsetDateTime from, OffsetDateTime to);

    @Query("SELECT COUNT(DISTINCT e.userId) FROM ActivityEventEntity e " +
           "WHERE e.eventTimestamp >= :from AND e.eventTimestamp <= :to")
    long countDistinctUserIdByEventTimestampBetween(
            @Param("from") OffsetDateTime from,
            @Param("to") OffsetDateTime to);

    @Query("SELECT e.eventType, COUNT(e) FROM ActivityEventEntity e " +
           "WHERE e.eventTimestamp >= :from AND e.eventTimestamp <= :to " +
           "GROUP BY e.eventType")
    List<Object[]> countByEventTypeBetween(
            @Param("from") OffsetDateTime from,
            @Param("to") OffsetDateTime to);

    @Query("SELECT e.source, COUNT(e) FROM ActivityEventEntity e " +
           "WHERE e.eventTimestamp >= :from AND e.eventTimestamp <= :to " +
           "GROUP BY e.source")
    List<Object[]> countBySourceBetween(
            @Param("from") OffsetDateTime from,
            @Param("to") OffsetDateTime to);

    @Query("SELECT e.eventType, COUNT(e) FROM ActivityEventEntity e " +
           "WHERE e.userId = :userId GROUP BY e.eventType")
    List<Object[]> countByEventTypeForUser(@Param("userId") String userId);

    @Query("SELECT MIN(e.eventTimestamp), MAX(e.eventTimestamp), COUNT(e) FROM ActivityEventEntity e " +
           "WHERE e.userId = :userId")
    List<Object[]> findUserSummary(@Param("userId") String userId);

    // Static parameterized queries for time-series aggregation without dynamic SQL concatenation
    @Query(value = "SELECT (date_trunc('minute', event_timestamp AT TIME ZONE 'UTC') AT TIME ZONE 'UTC') AS bucket, " +
                   "COUNT(*) AS total_events, COUNT(DISTINCT user_id) AS unique_users " +
                   "FROM activity_events " +
                   "WHERE event_timestamp >= :from AND event_timestamp <= :to " +
                   "GROUP BY bucket ORDER BY bucket ASC", nativeQuery = true)
    List<Object[]> aggregateTimeSeriesMinute(
            @Param("from") OffsetDateTime from,
            @Param("to") OffsetDateTime to);

    @Query(value = "SELECT (date_trunc('hour', event_timestamp AT TIME ZONE 'UTC') AT TIME ZONE 'UTC') AS bucket, " +
                   "COUNT(*) AS total_events, COUNT(DISTINCT user_id) AS unique_users " +
                   "FROM activity_events " +
                   "WHERE event_timestamp >= :from AND event_timestamp <= :to " +
                   "GROUP BY bucket ORDER BY bucket ASC", nativeQuery = true)
    List<Object[]> aggregateTimeSeriesHour(
            @Param("from") OffsetDateTime from,
            @Param("to") OffsetDateTime to);

    @Query(value = "SELECT (date_trunc('day', event_timestamp AT TIME ZONE 'UTC') AT TIME ZONE 'UTC') AS bucket, " +
                   "COUNT(*) AS total_events, COUNT(DISTINCT user_id) AS unique_users " +
                   "FROM activity_events " +
                   "WHERE event_timestamp >= :from AND event_timestamp <= :to " +
                   "GROUP BY bucket ORDER BY bucket ASC", nativeQuery = true)
    List<Object[]> aggregateTimeSeriesDay(
            @Param("from") OffsetDateTime from,
            @Param("to") OffsetDateTime to);
}
