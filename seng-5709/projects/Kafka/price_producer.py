#!/usr/bin/env python3
"""
SENG-5709 Kafka project
Andrew Bros bros0164@umn.edu

Create a simple Kafka producer.
Use Coin Metrics community API to get 1s reference rates for bitcoin (BTC).
"""

import sys
from datetime import datetime, timedelta
from time import sleep
from typing import List, Tuple

import requests
from kafka import KafkaProducer


def get_btc_rates(start: datetime, end: datetime) -> List[Tuple[str, float]]:
    """Get timeseries of BTC rates as (time, price) tuples"""
    endpoint = "https://community-api.coinmetrics.io/v4/timeseries/asset-metrics?"
    args = [
        "assets=btc",
        "metrics=ReferenceRateUSD",
        "frequency=1s",
        "page_size=1000",
        f"start_time={start.strftime('%Y-%m-%dT%H:%M:%S')}",
        f"end_time={end.strftime('%Y-%m-%dT%H:%M:%S')}",
    ]
    print(f"start={args[4]} end={args[5]}")
    response = requests.get(endpoint + "&".join(args))
    if not response.ok:
        print(f"API failure: {response.status_code} {response.reason}")
        sys.exit(1)
    data = response.json().get("data")
    rates = []
    for rate in data:
        rates.append((rate["time"], rate["ReferenceRateUSD"]))
    return rates


def write_rates(kafka: KafkaProducer, series: List[Tuple[str, float]]):
    """Write rates to Kafka"""
    for rate in series:
        time, price = rate
        print(f"{time}  {price}")
        kafka.send("btc", key=time, value=price)


def main():
    """Write BTC prices to Kafka topic"""
    kafka = KafkaProducer(
        key_serializer=str.encode,
        value_serializer=str.encode,
        bootstrap_servers="localhost:2181",
        client_id="btc-rates",
    )
    start = datetime.utcnow().replace(second=0, microsecond=0)

    while True:
        end = start + timedelta(minutes=1)
        sleeptime = end - datetime.utcnow()
        print(f"sleeping {sleeptime.total_seconds()}")
        sleep(sleeptime.total_seconds())
        series = get_btc_rates(start, end)
        write_rates(kafka, series)
        start = end


if __name__ == "__main__":
    main()
