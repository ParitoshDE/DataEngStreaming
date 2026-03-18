import json
from time import time

import pandas as pd
from kafka import KafkaProducer


URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2025-10.parquet"
TOPIC = "green-trips"
KAFKA_BOOTSTRAP = "localhost:9092"

COLUMNS = [
    "lpep_pickup_datetime",
    "lpep_dropoff_datetime",
    "PULocationID",
    "DOLocationID",
    "passenger_count",
    "trip_distance",
    "tip_amount",
    "total_amount",
]


def json_serializer(data: dict) -> bytes:
    return json.dumps(data).encode("utf-8")


def clean_record(row: pd.Series) -> dict:
    record = row.to_dict()

    # Homework asks to serialize datetimes as strings.
    for dt_col in ("lpep_pickup_datetime", "lpep_dropoff_datetime"):
        value = record.get(dt_col)
        if pd.isna(value):
            record[dt_col] = None
        else:
            record[dt_col] = value.strftime("%Y-%m-%d %H:%M:%S")

    # Convert NaN values from pandas to JSON null.
    for k, v in record.items():
        if pd.isna(v):
            record[k] = None

    return record


def main() -> None:
    print("Loading parquet data...")
    df = pd.read_parquet(URL, columns=COLUMNS)
    print(f"Rows loaded: {len(df)}")

    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_BOOTSTRAP],
        value_serializer=json_serializer,
    )

    t0 = time()
    for _, row in df.iterrows():
        producer.send(TOPIC, value=clean_record(row))

    producer.flush()
    t1 = time()

    print(f"Sent {len(df)} records to topic '{TOPIC}'")
    print(f"took {(t1 - t0):.2f} seconds")


if __name__ == "__main__":
    main()
