# Data Schema

## Price Data

| Column       | Type     | Description                     |
|--------------|----------|---------------------------------|
| Date         | Date     | Trading date                    |
| Adj Close    | Float    | Adjusted close price            |
| Close        | Float    | Raw closing price               |
| Ticker       | String   | Asset symbol                    |

Partitioned by: `Ticker`

---

## Corporate Action Data

| Column       | Type     | Description                     |
|--------------|----------|---------------------------------|
| Date         | Date     | Date of dividend payout         |
| Dividends    | Float    | Dividend amount                 |
| Stock Splits | Float    | Stock split ratio               |
| Ticker       | String   | Asset symbol                    |

Partitioned by: `Ticker`

---

## Metadata

# Metadata File Schema

This file defines metadata for each asset.

| Column           | Type     | Description                                                           |
|------------------|----------|-----------------------------------------------------------------------|
| ticker           | String   | Unique symbol or identifier (e.g. `AAPL`)                             |
| name             | String   | Full asset name                                                       |
| asset_type       | String   | Type of asset (e.g. `mutual fund`, `ETF`, `stock`, `crypto`)          |
| has_dividends    | Boolean  | `Y` if the asset distributes dividends, otherwise `N`                 |
| has_stock_splits | Boolean  | `Y` if asset has undergone stock splits, otherwise `N`                |
| currency         | String   | Native currency in which the asset is traded (e.g. `GBP`, `USD`)      |
| source           | String   | Data source used to fetch this asset’s price (e.g. `yfinance`, `csv`) |
| source_file_path | String   | Optional: path to source file (used if source is local like `csv`)    |
| active           | Boolean  | `Y` if asset is currently included in backtester, otherwise `N`       |
| start_date       | Date     | Optional: first known or relevant data date (may be blank)            |
| end_date         | Date     | Optional: last known or relevant data date (may be blank)             |
| notes            | String   | Free-form notes                                                       |

Stored in: `/backend/data/asset_metadata.csv`
