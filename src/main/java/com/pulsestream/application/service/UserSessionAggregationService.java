package com.pulsestream.application.service;

import com.pulsestream.domain.event.ActivityEvent;
import com.pulsestream.domain.event.EventType;
import com.pulsestream.infrastructure.persistence.entity.UserSessionEntity;
import com.pulsestream.infrastructure.persistence.repository.UserSessionRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Duration;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Optional;

/**
 * Service managing user session lifecycle and real-time session aggregation.
 *
 * <p>Key Invariants:
 * <ul>
 *   <li><b>Atomic Event-Session Aggregation:</b> Updates session state within the same transaction as event persistence.</li>
 *   <li><b>Closed-Session Safeguard:</b> A session closed by LOGOUT or inactivity expiration is NEVER silently reopened.
 *       Late-arriving events increment {@code eventCount} and update {@code lastSeenAt} without mutating {@code isActive=false}
 *       or corrupting {@code endedAt}.</li>
 *   <li><b>Inactivity Expiration:</b> A scheduled background task closes sessions whose {@code lastSeenAt} exceeds the
 *       configured inactivity threshold (default: 30 minutes).</li>
 * </ul>
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class UserSessionAggregationService {

    private final UserSessionRepository userSessionRepository;

    @Value("${pulsestream.session.inactivity-timeout-minutes:30}")
    private long inactivityTimeoutMinutes = 30;

    /**
     * Records an activity event in its associated session, creating or updating the session state.
     *
     * <p>Must be invoked ONLY after a genuinely new activity event has been persisted.
     *
     * @param event Validated activity event
     */
    @Transactional
    public void recordEventInSession(ActivityEvent event) {
        if (event == null || event.sessionId() == null || event.sessionId().isBlank()) {
            log.debug("Skipping session aggregation for event with null/blank sessionId: {}",
                    event != null ? event.eventId() : "null");
            return;
        }

        String sessionId = event.sessionId();
        OffsetDateTime eventTime = event.timestamp().atOffset(ZoneOffset.UTC);
        Optional<UserSessionEntity> existingSessionOpt = userSessionRepository.findBySessionId(sessionId);

        if (existingSessionOpt.isEmpty()) {
            UserSessionEntity newSession = new UserSessionEntity();
            newSession.setSessionId(sessionId);
            newSession.setUserId(event.userId());
            newSession.setStartedAt(eventTime);
            newSession.setLastSeenAt(eventTime);
            newSession.setEventCount(1);

            if (event.eventType() == EventType.LOGOUT) {
                newSession.setActive(false);
                newSession.setEndedAt(eventTime);
                log.info("Created pre-closed session (started with LOGOUT): sessionId={}, userId={}", sessionId, event.userId());
            } else {
                newSession.setActive(true);
                newSession.setEndedAt(null);
                log.debug("Created active session: sessionId={}, userId={}", sessionId, event.userId());
            }

            userSessionRepository.save(newSession);
            return;
        }

        UserSessionEntity session = existingSessionOpt.get();

        // 1. Always increment running event count for this session
        session.setEventCount(session.getEventCount() + 1);

        // 2. Adjust chronological boundaries
        if (eventTime.isBefore(session.getStartedAt())) {
            session.setStartedAt(eventTime);
        }
        if (eventTime.isAfter(session.getLastSeenAt())) {
            session.setLastSeenAt(eventTime);
        }

        // 3. Enforce lifecycle rules
        if (session.isActive()) {
            if (event.eventType() == EventType.LOGOUT) {
                session.setActive(false);
                session.setEndedAt(eventTime);
                log.info("Closed active session on LOGOUT: sessionId={}, userId={}", sessionId, event.userId());
            }
        } else {
            // SAFEGUARD: Session is already closed (via LOGOUT or inactivity expiration).
            // Do NOT silently reopen. Preserve isActive = false and keep endedAt intact.
            log.info("Late event {} arrived for closed session {}. Incremented eventCount ({}) without reopening session.",
                    event.eventId(), sessionId, session.getEventCount());
        }

        userSessionRepository.save(session);
    }

    /**
     * Periodic background sweep that deactivates idle user sessions.
     * Runs every 60 seconds by default.
     *
     * @return Number of sessions closed in this sweep
     */
    @Scheduled(fixedDelayString = "${pulsestream.session.expiration-check-interval-ms:60000}")
    @Transactional
    public int expireInactiveSessions() {
        OffsetDateTime cutoff = OffsetDateTime.now(ZoneOffset.UTC).minus(Duration.ofMinutes(inactivityTimeoutMinutes));
        int deactivatedCount = userSessionRepository.deactivateSessionsOlderThan(cutoff);
        if (deactivatedCount > 0) {
            log.info("Inactivity sweep: closed {} sessions idle since {}", deactivatedCount, cutoff);
        }
        return deactivatedCount;
    }
}
