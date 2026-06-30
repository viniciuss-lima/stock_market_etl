# Stock Market ETL Pipeline

This project is an ETL pipeline built with **PySpark** and **Delta Lake** to process U.S. stock market data from multiple public APIs.

The goal is to build a small data warehouse following the **Bronze → Silver → Gold** architecture, applying data engineering best practices for data ingestion, transformation, and modeling.

## Technologies

- Python
- Apache Spark (PySpark)
- Delta Lake
- PostgreSQL (work in progress)
- Polygon (Massive) API
- Nasdaq Screener API

## Current Progress

### Bronze Layer
- Extracts raw data from different APIs.
- Stores the original JSON files.
- Loads raw datasets into Delta Lake without modifications.

Data sources include:
- Daily OHLC market data
- Common Stocks
- American Depositary Receipts (ADR)
- Nasdaq Stock Screener

### Silver Layer
- Cleans and transforms the raw datasets.
- Flattens nested JSON structures.
- Integrates data from multiple sources.
- Filters supported exchanges (NYSE, NASDAQ and NYSE American).
- Creates normalized datasets such as:
  - Security
  - Industry
  - Sector
  - Market Data

### Gold Layer
- Builds analytical datasets using a star schema.
- Creates:
  - Fact Market Data
  - Dim Security
  - Dim Date

## Project Structure

```
data/
│
├── staging/
├── bronze/
├── silver/
└── gold/

src/
├── extraction.py
├── transform.py
└── connection.py
```

## Next Steps

- Load the Gold layer into PostgreSQL.
- Create the relational schema with primary and foreign keys.
- Perform SQL-based analytical queries.
- Develop dashboards for data visualization.

## Status

This project is still under development.
The current version focuses on building the ETL pipeline and the Lakehouse architecture. PostgreSQL loading and analytical reporting will be added in the next stage.