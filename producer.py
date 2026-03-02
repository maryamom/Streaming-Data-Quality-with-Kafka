#!/usr/bin/env python3
"""Read flights_summary.json and send each line to Kafka topic 'flights'. Do not modify."""
import os
import sys
import time

from kafka import KafkaProducer
from kafka.errors import KafkaError

ROOT = os.path.dirname(os.path.abspath(__file__))
DATA_PATH = os.path.join(ROOT, "flights_summary.json")
BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = os.environ.get("FLIGHTS_TOPIC_IN", "flights")

def main():
    if not os.path.isfile(DATA_PATH):
        print(f"File not found: {DATA_PATH}", file=sys.stderr)
        sys.exit(1)
    try:
        producer = KafkaProducer(
            bootstrap_servers=BOOTSTRAP.split(","),
            value_serializer=lambda v: v if isinstance(v, bytes) else v.encode("utf-8"),
        )
    except KafkaError as e:
        print(f"Kafka error: {e}", file=sys.stderr)
        sys.exit(1)
    with open(DATA_PATH, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                producer.send(TOPIC, value=line)
                time.sleep(0.05)
    producer.flush()
    producer.close()
    print("Done.")

if __name__ == "__main__":
    main()
