import json
import uuid

from kafka import KafkaConsumer


TOPIC = "green-trips"
KAFKA_BOOTSTRAP = "localhost:9092"


def deserialize(data: bytes) -> dict:
    return json.loads(data.decode("utf-8"))


def main() -> None:
    # Use a fresh group id so each run can consume from the beginning.
    group_id = f"hw7-q3-{uuid.uuid4()}"

    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=[KAFKA_BOOTSTRAP],
        auto_offset_reset="earliest",
        group_id=group_id,
        enable_auto_commit=False,
        consumer_timeout_ms=8000,
        value_deserializer=deserialize,
    )

    gt_5_count = 0
    total = 0

    for msg in consumer:
        total += 1
        trip_distance = msg.value.get("trip_distance")
        if trip_distance is not None and float(trip_distance) > 5.0:
            gt_5_count += 1

    consumer.close()

    print(f"Total messages read: {total}")
    print(f"Trips with trip_distance > 5.0: {gt_5_count}")


if __name__ == "__main__":
    main()
