#!/usr/bin/env python3
"""
SENG-5709 Kafka project
Andrew Bros bros0164@umn.edu

Create a simple Kafka producer.
Use Coin Metrics community API to get 1s reference rates for a cryptocurrency.
"""

import argparse
import json
import sys
from datetime import datetime, timedelta
from time import sleep
from typing import Any, Dict, List

import requests
from kafka import KafkaProducer


def get_args():
    """Get command-line arguments"""

    parser = argparse.ArgumentParser(
        description="Write cryptocurrency reference rates to Kafka",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--crypto",
        help="A cryptocurrency ticker",
        metavar="str",
        type=str,
        default="btc",
    )
    parser.add_argument(
        "--kafka",
        help="Kafka bootstrap server",
        metavar="str",
        type=str,
        default="kafka:9092",
    )
    args = parser.parse_args()
    if args.kafka.upper() == "NONE":
        args.kafka = None
    return args


def get_ref_rates(coin: str, start: datetime, end: datetime) -> List[Dict[str, Any]]:
    """Get timeseries of reference rates"""
    endpoint = "https://community-api.coinmetrics.io/v4/timeseries/asset-metrics?"
    args = [
        f"assets={coin}",
        "metrics=ReferenceRateUSD",
        "frequency=1s",
        "page_size=1000",
        f"start_time={start.strftime('%Y-%m-%dT%H:%M:%S')}",
        f"end_time={end.strftime('%Y-%m-%dT%H:%M:%S')}",
    ]
    response = requests.get(endpoint + "&".join(args))
    if not response.ok:
        print(f"API failure: {response.status_code} {response.reason}")
        sys.exit(1)
    data = response.json().get("data")
    rates = []
    for rate in data:
        rate["price"] = float(rate["ReferenceRateUSD"])
        rate.pop("ReferenceRateUSD")
        rates.append(rate)
    return rates


def write_rates(kafka: KafkaProducer, series: List[Dict[str, Any]]):
    """Write rates to Kafka"""
    for rate in series:
        print(f"{rate['asset']}  {rate['time']}  {rate['price']:.6f}")
        if kafka:
            kafka.send("crypto-ref-rates", value=rate)


def main():
    """Write BTC prices to Kafka topic"""
    args = get_args()

    start = datetime.utcnow().replace(second=0, microsecond=0)
    print("Sleeping 10 seconds", flush=True)
    sleep(10)

    kafka = None
    if args.kafka:
        kafka = KafkaProducer(
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            bootstrap_servers=args.kafka,
            client_id="ref-rates-producer",
        )

    while True:
        end = start + timedelta(minutes=1)
        sleeptime = (end - datetime.utcnow()).total_seconds()
        if sleeptime > 0:
            print(f"Sleeping {sleeptime:.3f} seconds", flush=True)
            sleep(sleeptime)
        series = get_ref_rates(args.crypto, start, end)
        write_rates(kafka, series)
        start = end


if __name__ == "__main__":
    main()
