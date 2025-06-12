# Data Sources

## 1. Yahoo Finance (`yfinance`)

Used for:
- Historical prices
- Dividend data

Notes:
- Supports multiple asset types.
- Crypto handled with suffixes like `BTC-USD`.

---

## 2. Metadata File

Location: `/backend/pipelines/asset_info.csv`

Purpose:
- Stores key information per asset.
- Dictates which pipeline/API is used per asset.

---

## 3. External APIs (Optional Future Use)

- CoinGecko: richer crypto info
- LSE APIs (if needed for UK ETFs)
