package com.pulsestream.messaging.consumer;

import com.pulsestream.application.service.ActivityEventPersistenceService;
import com.pulsestream.domain.event.ActivityEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Kafka Consumer responsible for reading activity events from {@code user.activity.events}
 * and driving persistence into PostgreSQL.
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class ActivityEventConsumer {

    private final ActivityEventPersistenceService activityEventPersistenceService;

    @KafkaListener(
            topics = "${pulsestream.kafka.topics.activity-events:user.activity.events}",
            groupId = "${spring.kafka.consumer.group-id:ps-activity-consumer}",
            containerFactory = "activityEventKafkaListenerContainerFactory"
    )
    public void onMessage(ConsumerRecord<String, ActivityEvent> record) {
        ActivityEvent event = record.value();
        if (event == null) {
            log.warn("Received null payload on topic={}, partition={}, offset={}",
                    record.topic(), record.partition(), record.offset());
            return;
        }

        log.info("Consuming ActivityEvent from Kafka [topic={}, partition={}, offset={}, key={}]: eventId={}, type={}",
                record.topic(), record.partition(), record.offset(), record.key(), event.eventId(), event.eventType());

        activityEventPersistenceService.persist(event);
    }
}
