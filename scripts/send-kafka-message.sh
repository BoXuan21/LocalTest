#!/usr/bin/env bash
# Send a single message to a Kafka topic from inside the broker pod.
# Usage: scripts/send-kafka-message.sh [topic] [message]
set -euo pipefail
TOPIC=${1:-test-topic}
MESSAGE=${2:-"hello from send-kafka-message.sh at $(date +%H:%M:%S)"}
echo "$MESSAGE" | kubectl -n kafka exec -i kafka-0 -- \
  /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic "$TOPIC"
echo "sent to $TOPIC: $MESSAGE"
