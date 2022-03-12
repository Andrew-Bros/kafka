# SENG-5709 Kafka Project

## Overview

The project objective is to create and demonstrate a functional kafka
producer and consumer.

For my producer, I chose to get interesting data by getting Coin
Metrics 1 second reference rate (prices) for Bitcoin (or another
cryptocurrency) using their free community API.  Every minute, the
producer requests the reference rates for the preceeding minute,
extracts the time and price and writes to the data to kafka.

For the consumer application, I read the kafka topic and summarize the
data by creating market candles of the high, low and median value for
each one minute window.

## Instructions

## Sources

- Kafka docker container: https://hub.docker.com/r/bitnami/kafka

- Coin Metrics community API: https://docs.coinmetrics.io/api
