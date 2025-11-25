# ingestion/kafka_producer.py
import os
import json
from kafka import KafkaProducer

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "credit_applications")

_producer = None


def get_producer() -> KafkaProducer:
    global _producer
    if _producer is None:
        _producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            key_serializer=lambda v: str(v).encode("utf-8"),
        )
    return _producer


def send_event(event: dict, key=None):
    """
    Gửi 1 bản ghi (event) lên Kafka.
    """
    producer = get_producer()
    producer.send(KAFKA_TOPIC, key=key, value=event)
