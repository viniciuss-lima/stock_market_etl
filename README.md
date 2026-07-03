# Stock Market ETL Pipeline

This project is an end-to-end ETL pipeline built with **PySpark**, **Delta Lake**, and **PostgreSQL** to process U.S. stock market data.

The pipeline follows the **Medallion Architecture (Bronze → Silver → Gold)**, transforming raw financial data into a dimensional **Star Schema** for analytical workloads. It integrates data from multiple public APIs and loads the processed datasets into a PostgreSQL data warehouse.

# Architecture

## ETL Pipeline

<p align="center">
  <img src="images/etl_pipeline.png" alt="ETL Pipeline" width="900"/>
</p>

## Silver Layer (Normalized Model)

<p align="center">
  <img src="images/silver_normalized.png" alt="Silver Layer" width="900"/>
</p>

## Gold Layer (Star Schema)

<p align="center">
  <img src="images/star_schema.png" alt="Gold Layer" width="900"/>
</p>

# Technologies

- Python
- Apache Spark (PySpark)
- Delta Lake
- PostgreSQL
- SQLAlchemy
- Massive API
- Nasdaq Screener API

# About the Project

This pipeline automatically collects stock market data from the previous trading day, ensuring the dataset is always updated with the most recent closed market session.

It retrieves Common Stocks (CS) and American Depositary Receipts (ADRC) listed on the three major U.S. stock exchanges (NYSE, NASDAQ, and NYSE American) using the Massive API.

To enrich the dataset, additional company metadata such as industry, sector, IPO year, and market capitalization is retrieved from the Nasdaq Screener API.

The data is processed through the Medallion Architecture, where it is stored as raw data in the Bronze layer, cleaned and normalized in the Silver layer, modeled into a Star Schema in the Gold layer, and finally loaded into a PostgreSQL data warehouse.

# Project Structure

```text
data
  staging
  bronze
  silver
  gold

images
  etl_pipeline.png
  silver_normalized.png
  star_schema.png

src
  extract.py
  transform.py
  load.py
  connection.py
  create_tables.sql

.env.example
.gitignore
requirements.txt
main.py
README.md
```

# Running the Pipeline

Install dependencies:

```bash
pip install -r requirements.txt
```

Configure your environment variables based on `.env.example`.

Run the pipeline:

```bash
python main.py
```

# Current Status

The project currently includes:

- End-to-end ETL pipeline
- Medallion Architecture (Bronze → Silver → Gold)
- Delta Lake storage
- Normalized Silver data model
- Star Schema modeling
- PostgreSQL data warehouse
- Incremental loading with append strategy (no overwrite)
- Single-command execution via `main.py`

# Next Steps

The ETL pipeline and data warehouse are already implemented.

The next stage of the project is to run SQL queries on the data warehouse and use them to generate insights and visualizations from the processed stock market data.

> **Note:** This project is designed with a hybrid architecture:
> - The **Delta Lake layers (Bronze, Silver, Gold)** can be fully reprocessed and overwritten safely, since they are derived from raw source data.
> - The **PostgreSQL data warehouse** is incremental and append-based, preserving historical records while avoiding duplicates through `ON CONFLICT` logic.

This ensures both reprocessing flexibility in the lake layer and historical consistency in the warehouse layer.