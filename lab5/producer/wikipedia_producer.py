"""Stream Wikipedia recent changes into Azure Event Hub, acting as an external producer."""

import argparse
import logging
import os
from collections.abc import Iterator

import requests
from azure.eventhub import (
    EventData,
    EventDataBatch,
    EventHubProducerClient,
    TransportType,
)

WIKIPEDIA_STREAM_URL = "https://stream.wikimedia.org/v2/stream/recentchange"
CONNECTION_STRING_ENV_VAR = "EVENTHUB_SEND_CONNECTION_STRING"
USER_AGENT = "lab5-wikipedia-producer/1.0 (data engineering course project)"
REQUEST_TIMEOUT_SECONDS = 30

logger = logging.getLogger("wikipedia_producer")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--eventhub-name", required=True, help="Target Event Hub")
    parser.add_argument(
        "--max-events", type=int, default=300, help="Number of events to send"
    )
    return parser.parse_args()


def read_wikipedia_events(max_events: int) -> Iterator[str]:
    with requests.get(
        WIKIPEDIA_STREAM_URL,
        headers={"User-Agent": USER_AGENT},
        stream=True,
        timeout=REQUEST_TIMEOUT_SECONDS,
    ) as response:
        response.raise_for_status()
        received = 0
        for line in response.iter_lines(decode_unicode=True):
            if not line or not line.startswith("data:"):
                continue
            yield line.removeprefix("data:").strip()
            received += 1
            if received >= max_events:
                return


def send_events(
    connection_string: str, eventhub_name: str, events: Iterator[str]
) -> int:
    producer = EventHubProducerClient.from_connection_string(
        connection_string,
        eventhub_name=eventhub_name,
        transport_type=TransportType.AmqpOverWebsocket,
    )
    sent = 0
    with producer:
        batch = producer.create_batch()
        for payload in events:
            try:
                batch.add(EventData(payload))
            except ValueError:
                sent += _send_batch(producer, batch)
                batch = producer.create_batch()
                batch.add(EventData(payload))
        if len(batch) > 0:
            sent += _send_batch(producer, batch)
    return sent


def _send_batch(producer: EventHubProducerClient, batch: EventDataBatch) -> int:
    producer.send_batch(batch)
    logger.info("Sent batch of %d events", len(batch))
    return len(batch)


def main() -> None:
    logging.basicConfig(
        level=logging.WARNING,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    logger.setLevel(logging.INFO)
    args = parse_args()

    connection_string = os.environ.get(CONNECTION_STRING_ENV_VAR)
    if not connection_string:
        raise SystemExit(f"Environment variable {CONNECTION_STRING_ENV_VAR} is not set")

    sent = send_events(
        connection_string, args.eventhub_name, read_wikipedia_events(args.max_events)
    )
    logger.info("Sent %d events to Event Hub '%s'", sent, args.eventhub_name)


if __name__ == "__main__":
    main()
