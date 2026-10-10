#!/bin/bash
# PulseStream — Kafka Topic Initialization Script
# Runs inside the Kafka container to create all required topics.
#
# Topics:
#   user.activity.events    — Raw validated events from the ingestion API (6 partitions)
#   user.activity.processed — Enriched events after consumer processing (6 partitions)
#   user.activity.dlq       — Dead-letter queue for unprocessable events (1 partition)
#
# Partitioning strategy:
#   6 partitions for main topics because we expect 6 consumers max in a consumer group.
#   Partition key = userId, ensuring per-user event ordering within a partition.
#   DLQ has 1 partition — low volume, ordering not critical.
#
# Retention:
#   Main topics: 7 days (604800000 ms) — allows consumer replay after failures.
#   DLQ: 30 days — more time to investigate and reprocess failed events.

KAFKA_BIN=/opt/kafka/bin
BOOTSTRAP=localhost:9092

echo "[PulseStream] Waiting for Kafka to be ready..."
until $KAFKA_BIN/kafka-broker-api-versions.sh --bootstrap-server $BOOTSTRAP > /dev/null 2>&1; do
    sleep 2
done
echo "[PulseStream] Kafka is ready. Creating topics..."

create_topic() {
    local TOPIC=$1
    local PARTITIONS=$2
    local RETENTION_MS=$3

    if $KAFKA_BIN/kafka-topics.sh --bootstrap-server $BOOTSTRAP --describe --topic "$TOPIC" > /dev/null 2>&1; then
        echo "[PulseStream] Topic '$TOPIC' already exists — skipping."
    else
        $KAFKA_BIN/kafka-topics.sh \
            --bootstrap-server $BOOTSTRAP \
            --create \
            --topic "$TOPIC" \
            --partitions "$PARTITIONS" \
            --replication-factor 1 \
            --config retention.ms="$RETENTION_MS" \
            --config cleanup.policy=delete

        echo "[PulseStream] Created topic '$TOPIC' ($PARTITIONS partitions, retention=${RETENTION_MS}ms)"
    fi
}

# 7 days = 604800000 ms
create_topic "user.activity.events"    6 604800000

# 7 days
create_topic "user.activity.processed" 6 604800000

# 30 days = 2592000000 ms
create_topic "user.activity.dlq"       1 2592000000

echo "[PulseStream] All topics created successfully."
$KAFKA_BIN/kafka-topics.sh --bootstrap-server $BOOTSTRAP --list
