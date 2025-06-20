# Data Pipelines

## 1. DataPipeline

**Purpose**:  
Generic class : Fetches historical data for stocks tickers contained on an external file
Supports retrieving price or corporate action data based on the ingestors provided in the arguments

**Inputs**:
- `ingestors`: List of ingestor objects (e.g., `YFinanceIngestor`, `CSVIngestor`)
- `save_dir`: Output directory path

**Steps**:
1. Download raw price data from each specialised ingestor
2. Transform to unified schema: flatten, clean, filter, tag with ticker and combine.
3. Convert to Polars and store as partitioned Parquet files.

**Output**:
- Partitioned `.parquet` files under `/processed/parquet/price`, split by ticker.

/processed/parquet/price/
├── ticker=AAPL/
│ └── data.parquet
├── ticker=GOOG/
│ └── data.parquet

---

