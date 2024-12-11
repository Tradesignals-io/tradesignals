# Equity Trades Pipeline

## Overview

This pipeline is responsible for ingesting equity trades from the Databento API and writing them to Kafka.

## Requirements

- Python 3.11
- Poetry
- Docker
- Docker Compose

## Installation

```bash
poetry install
```

## Running the Pipeline

```bash
poetry run python -m src.pipelines.equity_trades.main
```
