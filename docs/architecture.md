
#  Project Architecture

## Overview

This investment backtester project fetches historical **price** and **dividend** data for a variety of asset types (e.g., stocks, ETFs, crypto, gold), transforms it into a unified format, and stores it as partitioned Parquet files on an external SSD. 

The backend engine will lazy query this data using polar commands to filter the necessary data for performating a financial backtest for a given period of time. It will use this subset of data and calcualate key metricas and financial indicators.

The frontend will receive the results of the backtest and display them in a user friendly manner, wiith several interactive graphs and visuals.

---

## Folder Structure

investment_backtester/
├── backend/
│   ├── pipelines/          # Data pipeline logic
│   └── tests/              # Unit tests
├── config/                   
├── docs/                   # Architecture, schema, and pipeline documentation
├── scripts/                  
├── requirements.txt        # Project dependencies
├── README.md               # Project overview and setup
└── .gitignore              


---

## Main Components

### `PriceDataPipeline`

- Downloads historical price data for one or more tickers
- Transforms raw API output into a standard Polars DataFrame
- Saves data as partitioned `.parquet` files by ticker and stores on external SSD

### `DividendDataPipeline`

- Similar to `PriceDataPipeline` but for dividend data
- Also writes partitioned Parquet files

---

## Testing Strategy

- Use **`pytest`** with fixtures for isolated, repeatable unit tests
- **Patch external APIs** like `yfinance.download()` to avoid network calls
- Write tests for:
  - download logic
  - transformation logic

---

## Tools & Libraries

 Purpose          | Tool / Library       
------------------|--------------------------------
 Data Download    | `yfinance`           
 DataFrame Engine | `pandas`, `polars`             
 File Format      | `.parquet`           
 Testing          | `pytest`, `unittest.mock` 
 Temp Files       | `tempfile` (for test isolation) 
