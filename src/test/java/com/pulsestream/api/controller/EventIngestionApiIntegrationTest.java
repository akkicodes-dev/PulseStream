package com.pulsestream.api.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.pulsestream.api.exception.GlobalExceptionHandler;
import com.pulsestream.api.filter.RequestIdFilter;
import com.pulsestream.application.service.EventIngestionService;
import com.pulsestream.config.JacksonConfig;
import com.pulsestream.config.KafkaProducerConfig;
import com.pulsestream.config.PulseStreamKafkaProperties;
import com.pulsestream.messaging.producer.KafkaEventProducer;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.http.converter.json.MappingJackson2HttpMessageConverter;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest(classes = {
        JacksonConfig.class,
        KafkaProducerConfig.class,
        PulseStreamKafkaProperties.class,
        KafkaEventProducer.class,
        EventIngestionService.class,
        EventController.class,
        GlobalExceptionHandler.class,
        RequestIdFilter.class
})
@EmbeddedKafka(
        partitions = 1,
        topics = {"user.activity.events"},
        bootstrapServersProperty = "spring.kafka.bootstrap-servers"
)
@TestPropertySource(properties = {
        "pulsestream.kafka.topics.activity-events=user.activity.events",
        "pulsestream.kafka.publish-timeout-ms=5000",
        "spring.kafka.producer.acks=all"
})
class EventIngestionApiIntegrationTest {

    @Autowired
    private EventController eventController;

    @Autowired
    private GlobalExceptionHandler globalExceptionHandler;

    @Autowired
    private RequestIdFilter requestIdFilter;

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private EmbeddedKafkaBroker embeddedKafkaBroker;

    private MockMvc mockMvc;
    private Consumer<String, String> testConsumer;

    @BeforeEach
    void setUp() {
        mockMvc = MockMvcBuilders.standaloneSetup(eventController)
                .setControllerAdvice(globalExceptionHandler)
                .addFilters(requestIdFilter)
                .setMessageConverters(new MappingJackson2HttpMessageConverter(objectMapper))
                .build();

        Map<String, Object> consumerProps = KafkaTestUtils.consumerProps(
                "api-test-group-" + UUID.randomUUID(), "true", embeddedKafkaBroker
        );
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        DefaultKafkaConsumerFactory<String, String> consumerFactory =
                new DefaultKafkaConsumerFactory<>(consumerProps);
        testConsumer = consumerFactory.createConsumer();
        testConsumer.subscribe(Collections.singletonList("user.activity.events"));
    }

    @AfterEach
    void tearDown() {
        if (testConsumer != null) {
            testConsumer.close();
        }
    }

    @Test
    @DisplayName("End-to-End: POST /api/v1/events successfully validates, publishes to Kafka, and returns 202")
    void postEvent_endToEnd_returns202AndPublishesRecord() throws Exception {
        String requestJson = """
                {
                    "userId": "user_end_to_end",
                    "sessionId": "sess_live_123",
                    "eventType": "LOGIN",
                    "timestamp": "2026-10-09T10:15:30Z",
                    "source": "WEB",
                    "metadata": {
                        "ip": "192.168.1.1"
                    }
                }
                """;

        mockMvc.perform(post("/api/v1/events")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(requestJson))
                .andExpect(status().isAccepted())
                .andExpect(jsonPath("$.status").value("ACCEPTED"))
                .andExpect(jsonPath("$.eventId").isNotEmpty());

        ConsumerRecord<String, String> record =
                KafkaTestUtils.getSingleRecord(testConsumer, "user.activity.events", Duration.ofSeconds(5));

        assertThat(record).isNotNull();
        assertThat(record.key()).isEqualTo("user_end_to_end");
        assertThat(record.value()).contains("user_end_to_end");
        assertThat(record.value()).contains("sess_live_123");
        assertThat(record.value()).contains("LOGIN");
        assertThat(record.value()).contains("WEB");
        assertThat(record.value()).contains("192.168.1.1");
    }
}
