package com.pulsestream.application.service;

import com.pulsestream.domain.event.ActivityEvent;
import com.pulsestream.domain.event.EventSource;
import com.pulsestream.domain.event.EventType;
import com.pulsestream.infrastructure.persistence.entity.UserSessionEntity;
import com.pulsestream.infrastructure.persistence.repository.UserSessionRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class UserSessionAggregationServiceTest {

    @Mock
    private UserSessionRepository userSessionRepository;

    @InjectMocks
    private UserSessionAggregationService sessionAggregationService;

    private static final String USER_ID = "usr-100";
    private static final String SESSION_ID = "sess-200";

    @BeforeEach
    void setUp() {
    }

    private ActivityEvent createEvent(String eventId, EventType type, Instant timestamp) {
        return new ActivityEvent(
                eventId,
                USER_ID,
                SESSION_ID,
                type,
                timestamp,
                EventSource.WEB,
                Map.of("page", "/dashboard")
        );
    }

    @Test
    @DisplayName("Should create new active session on first non-logout event")
    void shouldCreateNewActiveSessionOnFirstEvent() {
        when(userSessionRepository.findBySessionId(SESSION_ID)).thenReturn(Optional.empty());

        Instant eventTime = Instant.parse("2026-10-10T10:00:00Z");
        ActivityEvent event = createEvent("evt-1", EventType.PAGE_VIEW, eventTime);

        sessionAggregationService.recordEventInSession(event);

        ArgumentCaptor<UserSessionEntity> captor = ArgumentCaptor.forClass(UserSessionEntity.class);
        verify(userSessionRepository).save(captor.capture());
        UserSessionEntity saved = captor.getValue();

        assertThat(saved.getSessionId()).isEqualTo(SESSION_ID);
        assertThat(saved.getUserId()).isEqualTo(USER_ID);
        assertThat(saved.getStartedAt()).isEqualTo(eventTime.atOffset(ZoneOffset.UTC));
        assertThat(saved.getLastSeenAt()).isEqualTo(eventTime.atOffset(ZoneOffset.UTC));
        assertThat(saved.getEndedAt()).isNull();
        assertThat(saved.getEventCount()).isEqualTo(1);
        assertThat(saved.isActive()).isTrue();
    }

    @Test
    @DisplayName("Should create closed session if the very first event received is LOGOUT")
    void shouldCreateClosedSessionWhenFirstEventIsLogout() {
        when(userSessionRepository.findBySessionId(SESSION_ID)).thenReturn(Optional.empty());

        Instant eventTime = Instant.parse("2026-10-10T10:00:00Z");
        ActivityEvent event = createEvent("evt-1", EventType.LOGOUT, eventTime);

        sessionAggregationService.recordEventInSession(event);

        ArgumentCaptor<UserSessionEntity> captor = ArgumentCaptor.forClass(UserSessionEntity.class);
        verify(userSessionRepository).save(captor.capture());
        UserSessionEntity saved = captor.getValue();

        assertThat(saved.getSessionId()).isEqualTo(SESSION_ID);
        assertThat(saved.isActive()).isFalse();
        assertThat(saved.getEndedAt()).isEqualTo(eventTime.atOffset(ZoneOffset.UTC));
        assertThat(saved.getEventCount()).isEqualTo(1);
    }

    @Test
    @DisplayName("Should update active session by incrementing count and updating lastSeenAt")
    void shouldIncrementCountAndUpdateLastSeenAtForActiveSession() {
        OffsetDateTime startedAt = OffsetDateTime.parse("2026-10-10T10:00:00Z");
        UserSessionEntity existing = new UserSessionEntity();
        existing.setSessionId(SESSION_ID);
        existing.setUserId(USER_ID);
        existing.setStartedAt(startedAt);
        existing.setLastSeenAt(startedAt);
        existing.setEventCount(1);
        existing.setActive(true);

        when(userSessionRepository.findBySessionId(SESSION_ID)).thenReturn(Optional.of(existing));

        Instant secondEventTime = Instant.parse("2026-10-10T10:05:00Z");
        ActivityEvent event = createEvent("evt-2", EventType.SEARCH, secondEventTime);

        sessionAggregationService.recordEventInSession(event);

        assertThat(existing.getEventCount()).isEqualTo(2);
        assertThat(existing.getStartedAt()).isEqualTo(startedAt);
        assertThat(existing.getLastSeenAt()).isEqualTo(secondEventTime.atOffset(ZoneOffset.UTC));
        assertThat(existing.isActive()).isTrue();
        assertThat(existing.getEndedAt()).isNull();
        verify(userSessionRepository).save(existing);
    }

    @Test
    @DisplayName("Should close active session when LOGOUT event is received")
    void shouldCloseActiveSessionOnLogout() {
        OffsetDateTime startedAt = OffsetDateTime.parse("2026-10-10T10:00:00Z");
        UserSessionEntity existing = new UserSessionEntity();
        existing.setSessionId(SESSION_ID);
        existing.setUserId(USER_ID);
        existing.setStartedAt(startedAt);
        existing.setLastSeenAt(startedAt);
        existing.setEventCount(3);
        existing.setActive(true);

        when(userSessionRepository.findBySessionId(SESSION_ID)).thenReturn(Optional.of(existing));

        Instant logoutTime = Instant.parse("2026-10-10T10:15:00Z");
        ActivityEvent logoutEvent = createEvent("evt-3", EventType.LOGOUT, logoutTime);

        sessionAggregationService.recordEventInSession(logoutEvent);

        assertThat(existing.getEventCount()).isEqualTo(4);
        assertThat(existing.isActive()).isFalse();
        assertThat(existing.getEndedAt()).isEqualTo(logoutTime.atOffset(ZoneOffset.UTC));
        assertThat(existing.getLastSeenAt()).isEqualTo(logoutTime.atOffset(ZoneOffset.UTC));
        verify(userSessionRepository).save(existing);
    }

    @Test
    @DisplayName("CRITICAL SAFEGUARD: Session closed by LOGOUT must NEVER be silently reopened by late-arriving event")
    void shouldNeverReopenClosedSessionOnLateEvent() {
        OffsetDateTime startedAt = OffsetDateTime.parse("2026-10-10T10:00:00Z");
        OffsetDateTime logoutEndedAt = OffsetDateTime.parse("2026-10-10T10:15:00Z");

        UserSessionEntity closedSession = new UserSessionEntity();
        closedSession.setSessionId(SESSION_ID);
        closedSession.setUserId(USER_ID);
        closedSession.setStartedAt(startedAt);
        closedSession.setLastSeenAt(logoutEndedAt);
        closedSession.setEndedAt(logoutEndedAt);
        closedSession.setEventCount(4);
        closedSession.setActive(false); // Closed by LOGOUT

        when(userSessionRepository.findBySessionId(SESSION_ID)).thenReturn(Optional.of(closedSession));

        Instant lateTime = Instant.parse("2026-10-10T10:20:00Z");
        ActivityEvent lateEvent = createEvent("evt-late", EventType.PRODUCT_VIEW, lateTime);

        sessionAggregationService.recordEventInSession(lateEvent);

        // Safeguard assertions
        assertThat(closedSession.isActive()).isFalse(); // MUST REMAIN FALSE
        assertThat(closedSession.getEndedAt()).isEqualTo(logoutEndedAt); // MUST PRESERVE ORIGINAL endedAt
        assertThat(closedSession.getEventCount()).isEqualTo(5); // Event count accurately reflects late arrival
        assertThat(closedSession.getLastSeenAt()).isEqualTo(lateTime.atOffset(ZoneOffset.UTC));
        verify(userSessionRepository).save(closedSession);
    }

    @Test
    @DisplayName("CRITICAL SAFEGUARD: Session closed by inactivity expiration must NEVER be silently reopened")
    void shouldNeverReopenExpiredSessionOnLateEvent() {
        OffsetDateTime startedAt = OffsetDateTime.parse("2026-10-10T10:00:00Z");
        OffsetDateTime expiredLastSeen = OffsetDateTime.parse("2026-10-10T10:05:00Z");

        UserSessionEntity expiredSession = new UserSessionEntity();
        expiredSession.setSessionId(SESSION_ID);
        expiredSession.setUserId(USER_ID);
        expiredSession.setStartedAt(startedAt);
        expiredSession.setLastSeenAt(expiredLastSeen);
        expiredSession.setEndedAt(expiredLastSeen);
        expiredSession.setEventCount(2);
        expiredSession.setActive(false); // Closed by inactivity sweep

        when(userSessionRepository.findBySessionId(SESSION_ID)).thenReturn(Optional.of(expiredSession));

        Instant lateEventTime = Instant.parse("2026-10-10T10:10:00Z");
        ActivityEvent lateEvent = createEvent("evt-late-2", EventType.PAGE_VIEW, lateEventTime);

        sessionAggregationService.recordEventInSession(lateEvent);

        assertThat(expiredSession.isActive()).isFalse(); // MUST REMAIN FALSE
        assertThat(expiredSession.getEndedAt()).isEqualTo(expiredLastSeen); // PRESERVED
        assertThat(expiredSession.getEventCount()).isEqualTo(3);
        verify(userSessionRepository).save(expiredSession);
    }

    @Test
    @DisplayName("Should adjust startedAt if out-of-order event is earlier than current startedAt")
    void shouldAdjustStartedAtForOutOfOrderEarlyEvent() {
        OffsetDateTime startedAt = OffsetDateTime.parse("2026-10-10T10:05:00Z");
        UserSessionEntity existing = new UserSessionEntity();
        existing.setSessionId(SESSION_ID);
        existing.setUserId(USER_ID);
        existing.setStartedAt(startedAt);
        existing.setLastSeenAt(startedAt);
        existing.setEventCount(1);
        existing.setActive(true);

        when(userSessionRepository.findBySessionId(SESSION_ID)).thenReturn(Optional.of(existing));

        Instant earlierTime = Instant.parse("2026-10-10T10:01:00Z");
        ActivityEvent earlyEvent = createEvent("evt-early", EventType.LOGIN, earlierTime);

        sessionAggregationService.recordEventInSession(earlyEvent);

        assertThat(existing.getStartedAt()).isEqualTo(earlierTime.atOffset(ZoneOffset.UTC));
        assertThat(existing.getLastSeenAt()).isEqualTo(startedAt);
        assertThat(existing.getEventCount()).isEqualTo(2);
        verify(userSessionRepository).save(existing);
    }

    @Test
    @DisplayName("Should trigger scheduled inactivity expiration sweep")
    void shouldExpireInactiveSessions() {
        when(userSessionRepository.deactivateSessionsOlderThan(any(OffsetDateTime.class))).thenReturn(3);

        int count = sessionAggregationService.expireInactiveSessions();

        assertThat(count).isEqualTo(3);
        verify(userSessionRepository).deactivateSessionsOlderThan(any(OffsetDateTime.class));
    }
}
