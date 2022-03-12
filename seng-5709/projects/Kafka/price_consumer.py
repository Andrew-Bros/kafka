#!/usr/bin/env python3
"""
SENG-5709 Kafka project
Andrew Bros bros0164@umn.edu

Create a simple Kafka consumer.
Read 1s reference rates for a cryptocurrency from Kafka
and calculate 1 minute candles (high, low, median).
"""

import argparse
import json
import sys
from collections import namedtuple
from statistics import median
from time import sleep
from typing import Any, Dict, List

from kafka import KafkaConsumer

Candle = namedtuple("Candle", ["low", "high", "median"])


def get_args():
    """Get command-line arguments"""

    parser = argparse.ArgumentParser(
        description="Calculate 1 minute candles from Kafka topic",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--kafka",
        help="Kafka bootstrap server",
        metavar="str",
        type=str,
        default="kafka:9092",
    )
    parser.add_argument(
        "--topic",
        help="Kafka topic to subscribe to",
        metavar="str",
        type=str,
        default="crypto-ref-rates",
    )
    args = parser.parse_args()
    return args


def calculate_candle(values: List[float]) -> Candle:
    """Return high, low and median price for a list of prices"""
    ordered = sorted(values)
    return Candle(low=ordered[0], high=ordered[-1], median=median(ordered))


def time_to_minute(timestamp: str) -> str:
    """Truncate the time stamp to the minute"""
    return ":".join(timestamp.split(":")[0:2])


window = []


def process_event(event: Dict[str, Any]):
    """Process a Kafka event"""
    if window:
        start = window[0]
        if start["asset"] != event["asset"]:
            print(f"Unexpected asset in event stream {event['asset']}", file=sys.stderr)
            return
        start_minute = time_to_minute(start.get("time"))
        this_minute = time_to_minute(event.get("time"))
        if start_minute != this_minute:
            candle = calculate_candle([e["price"] for e in window])
            print(
                f"{start_minute} {start['asset']} low: {candle.low:.6f} "
                + f"high: {candle.high:.6f} median {candle.median:.6f}",
                flush=True,
            )
            window.clear()
    window.append(event)


def main():
    """Read crypto prices from Kafka topic and calculate candles"""
    args = get_args()
    print("Sleeping 30 seconds", flush=True)
    sleep(30)
    kafka = KafkaConsumer(
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        bootstrap_servers=args.kafka,
        client_id="ref-rates-consumer",
    )

    kafka.subscribe(args.topic)

    for event in kafka:
        if event.topic != args.topic:
            print(f"Unexpected topic event {event.topic}", file=sys.stderr)
            return
        process_event(event.value)


if __name__ == "__main__":
    main()
