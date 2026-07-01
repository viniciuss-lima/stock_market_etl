# Stock Market ETL Pipeline

This project is an ETL pipeline built with **PySpark** and **Delta Lake** to process U.S. stock market data from multiple public APIs.

The goal is to build a small data warehouse following the **Bronze → Silver → Gold** architecture, applying data engineering best practices for data ingestion, transformation, and modeling.

## Technologies

- Python
- Apache Spark (PySpark)
- Delta Lake
- PostgreSQL
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

### Load Layer
- Creates the PostgreSQL data warehouse schema.
- Loads Gold layer tables into PostgreSQL.
- Uses staging tables and `ON CONFLICT` to avoid duplicate records.

## Project Structure

```
data/
│
├── staging/
├── bronze/
├── silver/
└── gold/

src/
├── extract.py
├── transform.py
├── load.py
├── connection.py
└── create_tables.sql
```

## Next Steps

- Automate the pipeline to run daily.
- Keep the data warehouse continuously updated.
- Build SQL analytical queries.

## Status

This project is still under development.

The ETL pipeline, Lakehouse architecture, and PostgreSQL loading are already implemented. The next stage is to automate daily data ingestion and build analytical reports.