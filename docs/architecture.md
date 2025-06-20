
#  Project Architecture

## Overview

This investment backtester project fetches historical **price** and **dividend** data for a variety of asset types (e.g., stocks, ETFs, crypto, gold), transforms it into a unified format, and stores it as partitioned Parquet files on an external SSD. 

The backend engine will lazy query this data using polar commands to filter the necessary data for performating a financial backtest for a given period of time. It will use this subset of data and calcualate key metrics and financial indicators.

The frontend will receive the results of the backtest and display them in a user friendly manner, with several interactive graphs and visuals.

---

## Folder Structure

investment_backtester/
├── backend/
│   ├── data/               # Local data storage (eg. metadata file)
│   ├── pipelines/          # Data pipeline logic
│   └── config.py           # Configuration constants and methods                 
├── docs/                   # Architecture, schema, and pipeline documentation
├── scripts/                
├── tests/                  # Unit tests , folder structure within replicates project 
├── requirements.txt        # Project dependencies
├── README.md               # Project overview and setup
├── setup.sh                # Project setup file : sets python path and activates virtual environment
└── .gitignore              

---

## Main Components

### `DataPipeline`

- Downloads historical price or action data for one or more tickers
- Combines multiple dataframes from different ingestors into a single standard Polars DataFrame
- Saves data as partitioned `.parquet` files by ticker and stores on external SSD

### `YFinanceBaseIngestor`

- Abstract class for ingesting data from yfinance API
- Retrieves data from yfinance based on list of tickers and set of dates input
- Uses batches with a short delay (2 second) to prevent API throttling or bans
- Child classes include YFinancePriceIngestor and YFinanceCorporateActionIngestor

### `CSVPriceIngestor`

- Reads a local csv, cleans the data and converts to a Polars DataFrame
- Renames and casts columns to ensure it adheres to specified schema
- If only one price column is present, it will duplicate that for both a `close` and `adj_close` 

---

## Testing Strategy

- Use **`pytest`** with fixtures for isolated, repeatable unit tests
- **Patch external APIs** like `yfinance.download()` to avoid network calls
- **Parameterise** pytest to perform repeated tests with different inputs.

---

## Tools & Libraries

 Purpose          | Tool / Library       
------------------|--------------------------------
 Data Download    | `yfinance`           
 DataFrame Engine | `pandas`, `polars`             
 File Format      | `.parquet`           
 Testing          | `pytest`, `unittest.mock` 

