# Data Pipelines

## 1. PriceDataPipeline

**Purpose**:  
Generic class : Fetches historical price data for stocks tickers contained on an external file

**Inputs**:
- Ticker list
- Start date
- End date
- Metadata file (for asset source/type if needed)

**Steps**:
1. Download raw price data using `yfinance` (or other APIs per asset type).
2. Transform to unified schema: flatten, clean, filter, tag with ticker.
3. Convert to Polars and store as partitioned Parquet.

**Output**:
- Partitioned `.parquet` files under `/data/price_data/`, split by ticker.

---

<!-- ## 2. DividendDataPipeline

**Purpose**:  
Fetch and store dividend events per asset.

**Inputs**:
- Ticker list
- Start/end date
- Metadata source (to identify dividend-capable assets)

**Steps**:
1. Download dividend data (likely from `yfinance`).
2. Clean and unify date/ticker columns.
3. Save as `.parquet`, potentially not partitioned.

**Output**:
- Single `.parquet` or partitioned by ticker under `/data/dividends/`.

---

## 3. Update Pipelines

**Purpose**:  
Append new data (daily) to existing files.

**Strategy**:
- Determine last date in saved Parquet file.
- Query only for the missing date range.
- Merge and overwrite with updated file. -->

