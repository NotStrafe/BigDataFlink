import csv
import glob
import json
import os
import sys
import time
from pathlib import Path

from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable


BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
TOPIC = os.getenv("KAFKA_TOPIC", "pet-sales")
CSV_DIR = os.getenv("CSV_DIR", "../исходные данные")
SEND_DELAY_MS = int(os.getenv("SEND_DELAY_MS", "0"))


def wait_for_kafka() -> KafkaProducer:
    last_error = None
    for _ in range(60):
        try:
            return KafkaProducer(
                bootstrap_servers=BOOTSTRAP_SERVERS,
                value_serializer=lambda value: json.dumps(value, ensure_ascii=False).encode("utf-8"),
                acks="all",
                retries=5,
                linger_ms=20,
            )
        except NoBrokersAvailable as exc:
            last_error = exc
            time.sleep(2)
    raise RuntimeError(f"Kafka is not available at {BOOTSTRAP_SERVERS}") from last_error


def csv_files() -> list[str]:
    files = glob.glob(str(Path(CSV_DIR) / "*.csv"))
    return sorted(files, key=lambda path: (Path(path).stem.replace("MOCK_DATA", ""), path))


def publish_file(producer: KafkaProducer, file_path: str) -> int:
    sent = 0
    source_file = Path(file_path).name
    with open(file_path, newline="", encoding="utf-8") as csv_file:
        reader = csv.DictReader(csv_file)
        for row_number, row in enumerate(reader, start=1):
            row["_source_file"] = source_file
            row["_source_row_number"] = row_number
            producer.send(TOPIC, row)
            sent += 1
            if SEND_DELAY_MS > 0:
                time.sleep(SEND_DELAY_MS / 1000)
    producer.flush()
    print(f"{source_file}: sent {sent} messages", flush=True)
    return sent


def main() -> int:
    files = csv_files()
    if not files:
        print(f"No CSV files found in {CSV_DIR}", file=sys.stderr)
        return 1

    producer = wait_for_kafka()
    total = 0
    for file_path in files:
        total += publish_file(producer, file_path)
    producer.close()
    print(f"Done: sent {total} messages to Kafka topic {TOPIC}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
