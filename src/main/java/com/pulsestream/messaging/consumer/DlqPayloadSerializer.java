package com.pulsestream.messaging.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serializer;

import java.nio.charset.StandardCharsets;
import java.util.Map;

/**
 * Universal Kafka serializer for Dead Letter Queue (DLQ) messages.
 *
 * <p>Handles:
 * <ul>
 *   <li>Raw byte arrays (from malformed deserialization errors)</li>
 *   <li>Strings (e.g. invalid JSON payloads)</li>
 *   <li>Domain POJOs / records (serialized to JSON via Jackson)</li>
 * </ul>
 */
public class DlqPayloadSerializer implements Serializer<Object> {

    private final ObjectMapper objectMapper;

    public DlqPayloadSerializer() {
        this.objectMapper = new ObjectMapper();
    }

    public DlqPayloadSerializer(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper != null ? objectMapper : new ObjectMapper();
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        // No custom config required
    }

    @Override
    public byte[] serialize(String topic, Object data) {
        if (data == null) {
            return null;
        }
        if (data instanceof byte[] bytes) {
            return bytes;
        }
        if (data instanceof String str) {
            return str.getBytes(StandardCharsets.UTF_8);
        }
        try {
            return objectMapper.writeValueAsBytes(data);
        } catch (Exception ex) {
            return data.toString().getBytes(StandardCharsets.UTF_8);
        }
    }

    @Override
    public void close() {
        // No resources to close
    }
}
