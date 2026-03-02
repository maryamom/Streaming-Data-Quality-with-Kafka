#!/usr/bin/env python3
"""
Consumer: parse JSON safely, validate schema and types, detect invalid records,
route to flights-valid or flights-invalid. Does not crash on bad data.
"""
import json
import os
import sys
from typing import Optional, Tuple

from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import KafkaError

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC_IN = os.environ.get("FLIGHTS_TOPIC_IN", "flights")
TOPIC_VALID = os.environ.get("FLIGHTS_TOPIC_VALID", "flights-valid")
TOPIC_INVALID = os.environ.get("FLIGHTS_TOPIC_INVALID", "flights-invalid")
GROUP = os.environ.get("FLIGHTS_CONSUMER_GROUP", "flights-quality-group")


def parse_json_safe(raw: bytes) -> Tuple[Optional[dict], bool]:
    """Parse JSON without raising. Returns (parsed_dict, ok)."""
    if not raw or not raw.strip():
        return None, False
    try:
        obj = json.loads(raw.decode("utf-8") if isinstance(raw, bytes) else raw)
        return (obj, True) if isinstance(obj, dict) else (None, False)
    except (json.JSONDecodeError, UnicodeDecodeError, TypeError):
        return None, False


def validate_record(obj: dict) -> bool:
    """Validate schema and data types: ORIGIN_COUNTRY_NAME, DEST_COUNTRY_NAME (non-empty str), count (int >= 0)."""
    if not isinstance(obj, dict):
        return False
    origin = obj.get("ORIGIN_COUNTRY_NAME")
    dest = obj.get("DEST_COUNTRY_NAME")
    count = obj.get("count")
    if origin is None or dest is None or count is None:
        return False
    if not isinstance(origin, str) or not origin.strip():
        return False
    if not isinstance(dest, str) or not dest.strip():
        return False
    if not isinstance(count, int):
        return False
    if count < 0:
        return False
    return True


def main():
    try:
        consumer = KafkaConsumer(
            TOPIC_IN,
            bootstrap_servers=BOOTSTRAP.split(","),
            group_id=GROUP,
            value_deserializer=lambda v: v,
            auto_offset_reset="earliest",
        )
    except KafkaError as e:
        print(f"Consumer error: {e}", file=sys.stderr)
        sys.exit(1)

    try:
        producer = KafkaProducer(
            bootstrap_servers=BOOTSTRAP.split(","),
            value_serializer=lambda v: v if isinstance(v, bytes) else json.dumps(v).encode("utf-8"),
        )
    except KafkaError as e:
        print(f"Producer error: {e}", file=sys.stderr)
        sys.exit(1)

    try:
        for msg in consumer:
            raw = msg.value
            parsed, parse_ok = parse_json_safe(raw)
            if not parse_ok:
                producer.send(TOPIC_INVALID, value=raw if isinstance(raw, bytes) else raw.encode("utf-8"))
                continue
            if validate_record(parsed):
                producer.send(TOPIC_VALID, value=parsed)
            else:
                producer.send(TOPIC_INVALID, value=parsed)
        producer.flush()
    except KeyboardInterrupt:
        pass
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
    finally:
        consumer.close()
        producer.close()


if __name__ == "__main__":
    main()
