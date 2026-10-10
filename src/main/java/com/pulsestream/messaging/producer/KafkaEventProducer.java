package com.pulsestream.messaging.producer;

import com.pulsestream.config.PulseStreamKafkaProperties;
import com.pulsestream.domain.event.ActivityEvent;
import com.pulsestream.exception.EventPublishingException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Dedicated Apache Kafka producer for streaming user activity events.
 *
 * <p><b>Delivery Guarantees:</b>
 * <ul>
 *   <li><b>At-Least-Once Delivery:</b> Combined with producer idempotence ({@code enable.idempotence=true})
 *       and {@code acks=all}, records are reliably written to all in-sync replicas without duplicate entries
 *       per partition caused by producer-side network retries.</li>
 *   <li><b>Per-User Ordering:</b> Records use {@code userId} as the Kafka partition key, guaranteeing
 *       strict chronological ordering for any given user within a single Kafka topic partition.</li>
 *   <li><b>Controlled Bounded Latency:</b> Calls block synchronously waiting for broker acknowledgment
 *       up to {@link PulseStreamKafkaProperties#getPublishTimeoutMs()}. If the broker does not acknowledge
 *       within this window, a {@link EventPublishingException} is raised, allowing the HTTP endpoint to return
 *       a {@code 503 Service Unavailable} rather than hanging indefinitely.</li>
 * </ul>
 *
 * <p><b>Limitations:</b>
 * <ul>
 *   <li>No local transactional outbox pattern is implemented in this milestone; if Kafka is down,
 *       incoming requests are rejected immediately without being spooled to disk.</li>
 *   <li>Cross-user total ordering is not guaranteed across partitions.</li>
 * </ul>
 */
@Component
@Slf4j
public class KafkaEventProducer implements EventProducer {

    private final KafkaTemplate<String, ActivityEvent> kafkaTemplate;
    private final PulseStreamKafkaProperties kafkaProperties;

    public KafkaEventProducer(
            KafkaTemplate<String, ActivityEvent> kafkaTemplate,
            PulseStreamKafkaProperties kafkaProperties) {
        this.kafkaTemplate = kafkaTemplate;
        this.kafkaProperties = kafkaProperties;
    }

    @Override
    public SendResult<String, ActivityEvent> sendEvent(ActivityEvent event) {
        String topic = kafkaProperties.getTopics().getActivityEvents();
        String partitionKey = event.userId();

        // Note: Event metadata is explicitly NOT logged to prevent leaking sensitive or PII attributes.
        log.debug("Initiating Kafka publication: eventId={}, userId={}, eventType={}, topic={}",
                event.eventId(), partitionKey, event.eventType(), topic);

        try {
            CompletableFuture<SendResult<String, ActivityEvent>> future =
                    kafkaTemplate.send(topic, partitionKey, event);

            long timeoutMs = kafkaProperties.getPublishTimeoutMs();
            SendResult<String, ActivityEvent> sendResult = future.get(timeoutMs, TimeUnit.MILLISECONDS);

            log.info("Successfully published event to Kafka: eventId={}, topic={}, partition={}, offset={}",
                    event.eventId(),
                    sendResult.getRecordMetadata().topic(),
                    sendResult.getRecordMetadata().partition(),
                    sendResult.getRecordMetadata().offset());

            return sendResult;
        } catch (TimeoutException e) {
            log.error("Kafka publication timed out after {}ms for eventId={}, topic={}",
                    kafkaProperties.getPublishTimeoutMs(), event.eventId(), topic);
            throw new EventPublishingException(
                    "Timed out waiting for Kafka broker acknowledgment after " + kafkaProperties.getPublishTimeoutMs() + "ms",
                    e
            );
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("Kafka publication interrupted for eventId={}, topic={}", event.eventId(), topic);
            throw new EventPublishingException("Thread interrupted while publishing event to Kafka", e);
        } catch (ExecutionException e) {
            Throwable rootCause = e.getCause() != null ? e.getCause() : e;
            log.error("Kafka publication failed for eventId={}, topic={}: {}",
                    event.eventId(), topic, rootCause.getMessage());
            throw new EventPublishingException("Kafka broker rejected or failed to publish event: " + rootCause.getMessage(), rootCause);
        } catch (Exception e) {
            log.error("Unexpected error publishing event for eventId={}, topic={}: {}",
                    event.eventId(), topic, e.getMessage());
            throw new EventPublishingException("Unexpected error during event publication: " + e.getMessage(), e);
        }
    }
}
