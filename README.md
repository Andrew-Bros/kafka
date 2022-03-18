# SENG-5709 Kafka Project

## Overview

The project objective is to create and demonstrate a functional kafka
producer and consumer.  I chose to use Python for the implementation.

For my producer, I chose to get interesting data by getting Coin
Metrics 1 second reference rate (prices) for Bitcoin (or another
cryptocurrency) using their free community API.  Every minute, the
producer requests the reference rates for the preceeding minute,
extracts the time and price and writes to the data to kafka.

For the consumer application, I read the kafka topic and summarize the
data by creating market candles of the high, low and median value for
each one minute window.

## Prerequisites

This project has been implemented to run Kafka (with Zookeeper) as
well as the producer and consumer applications in docker containers
using docker compose.

The producer and consumer applications take a command line option to
override the default bootstrap-server that should make it easy to run
in a different configuration.  In that case, however, Kafka and Python
(v3.9) and python modules listed in requirements.txt are needed.

## Instructions

To run the demo, simply `gmake demo` to start up the producer and
consumer applications.

Use `docker compose logs --follow producer` or `docker compose logs
--follow consumer` to see the applications at work.

The demonstration can be stopped with `gmake stop`.

## Video

The demonstration video can be found on YouTube at
https://youtu.be/Q4ik2W751OY

## Sources

- Kafka docker container: https://hub.docker.com/r/bitnami/kafka
- Python kafka module: https://pypi.org/project/kafka-python/
- Coin Metrics community API: https://docs.coinmetrics.io/api
