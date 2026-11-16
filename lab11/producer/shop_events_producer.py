"""Replay shop clickstream events from events.csv into a Unity Catalog table through Zerobus Ingest.

The script plays the shop backend: every csv row is sent as one event, at a fixed rate,
straight to the Delta table. no message bus in between.
"""

import argparse
import csv
import json
import logging
import os
import time
from collections.abc import Iterator
from datetime import datetime, timezone
from itertools import islice
from pathlib import Path

from dotenv import load_dotenv
from zerobus.sdk.shared import NonRetriableException, TableProperties, ZerobusException
from zerobus.sdk.sync import ZerobusSdk

ENV_FILE = Path(__file__).resolve().parent / ".env"
PRODUCER_NAME = "shop-backend"
CHECKPOINT_EVERY = 200

logger = logging.getLogger("shop_events_producer")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--source-file", type=Path, required=True, help="Path to events.csv"
    )
    parser.add_argument(
        "--start-row", type=int, default=1, help="First csv data row to send, 1-based"
    )
    parser.add_argument(
        "--max-events", type=int, default=1000, help="How many events to send"
    )
    parser.add_argument(
        "--rate",
        type=float,
        default=20,
        help="Events per second, 0 sends without pause",
    )
    return parser.parse_args()


def require_env(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        raise SystemExit(f"Environment variable {name} is not set")
    return value


def read_rows(
    path: Path, start_row: int, max_events: int
) -> Iterator[tuple[int, dict]]:
    with path.open(newline="", encoding="utf-8") as f:
        rows = islice(csv.DictReader(f), start_row - 1, start_row - 1 + max_events)
        yield from enumerate(rows, start=start_row)


def to_micros(value: str) -> int:
    # source times have no zone, they are treated as UTC
    moment = datetime.fromisoformat(value).replace(tzinfo=timezone.utc)
    return int(moment.timestamp() * 1_000_000)


def to_float(value: str) -> float | None:
    return float(value) if value else None


def build_record(row: dict, producer_id: str, producer_seq: int) -> str:
    record = {
        "event_id": int(row["event_id"]),
        "session_id": int(row["session_id"]) if row["session_id"] else None,
        "event_ts": to_micros(row["timestamp"]) if row["timestamp"] else None,
        "event_type": row["event_type"] or None,
        "product_id": to_float(row["product_id"]),
        "qty": to_float(row["qty"]),
        "cart_size": to_float(row["cart_size"]),
        "payment": row["payment"] or None,
        "discount_pct": to_float(row["discount_pct"]),
        "amount_usd": to_float(row["amount_usd"]),
        # VARIANT goes over the JSON api as a json string
        "raw_payload": json.dumps(row),
        "producer_id": producer_id,
        "producer_seq": producer_seq,
        "producer_sent_at": time.time_ns() // 1_000,
    }
    # fields that are not sent land in the table as NULL
    return json.dumps(
        {key: value for key, value in record.items() if value is not None}
    )


def main() -> None:
    logging.basicConfig(
        level=logging.WARNING,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    logger.setLevel(logging.INFO)
    args = parse_args()
    load_dotenv(ENV_FILE)

    table_name = require_env("ZEROBUS_TABLE_NAME")
    sdk = ZerobusSdk(
        require_env("ZEROBUS_SERVER_ENDPOINT"), require_env("DATABRICKS_WORKSPACE_URL")
    )
    stream = sdk.create_stream(
        require_env("ZEROBUS_CLIENT_ID"),
        require_env("ZEROBUS_CLIENT_SECRET"),
        TableProperties(table_name),
    )

    producer_id = f"{PRODUCER_NAME}-{datetime.now(timezone.utc):%Y%m%dT%H%M%SZ}"
    pause = 1 / args.rate if args.rate > 0 else 0
    sent = 0
    confirmed_row = args.start_row - 1
    logger.info(
        "Producer %s sends up to %d events from row %d to %s",
        producer_id,
        args.max_events,
        args.start_row,
        table_name,
    )

    started = time.monotonic()
    try:
        for row_number, row in read_rows(
            args.source_file, args.start_row, args.max_events
        ):
            offset = stream.ingest_record_offset(
                build_record(row, producer_id, sent + 1)
            )
            sent += 1
            # a confirmed row is the position a restarted producer would continue from
            if sent % CHECKPOINT_EVERY == 0:
                stream.wait_for_offset(offset)
                confirmed_row = row_number
                logger.info("Durable up to row %d, %d events sent", confirmed_row, sent)
            if pause:
                time.sleep(pause)
        stream.flush()
        confirmed_row = args.start_row - 1 + sent
    except KeyboardInterrupt:
        logger.warning(
            "Stopped by user. Last confirmed row %d, resume with --start-row %d",
            confirmed_row,
            confirmed_row + 1,
        )
    except NonRetriableException:
        logger.exception(
            "Zerobus rejected the stream, check credentials, grants and the table schema"
        )
        raise
    except ZerobusException:
        logger.exception("Zerobus ingestion failed after the SDK retries")
        raise
    finally:
        stream.close()

    logger.info(
        "Sent %d events in %.1f s, confirmed up to row %d",
        sent,
        time.monotonic() - started,
        confirmed_row,
    )


if __name__ == "__main__":
    main()
