# Data Sources

## 1. Yahoo Finance (`yfinance`)

Used for:
- Historical prices
- Corporate actions (Dividends and Stock Splits)

Notes:
- Supports multiple asset types : stocks, funds, crypto 
- Crypto handled with suffixes like `BTC-GBP`.
- Securities sold on the London Stock Exchange (LSE) will have a .L suffix added to their ticker

---

## 2. Metadata File

Location: `/backend/data/asset_metadata.csv`

Purpose:
- Stores key information per asset:
    - ticker
    - name
    - asset_type
    - has_dividends
    - has_stock_splits
    - source
    - etc...
- Used to determine where the source of data, and therefore which ingestor is used.

---

## 3. Local CSV files

CSVs have been downloaded from online sources when a suitable free API is not available

Used for:
- Commodities : Gold and Silver price history downloaded from Investing.com 
    - https://uk.investing.com/commodities/gold-historical-data
    - https://uk.investing.com/commodities/silver-historical-data

- *Gilts (UK Bonds) : Average yields downloaded from UK Debt Management Office (DMO)
    - website : https://www.dmo.gov.uk/data/gilt-market/aggregated-yields/
    - document : https://www.dmo.gov.uk/data/ExportReport?reportCode=D4H

- *Treasury bills : Average yeilds downloaded from UK DMO 
    - website : https://www.dmo.gov.uk/data/pdfdatareport?reportCode=D2.2D

Notes:
- *Gilt and treasury bills are not considered core assets and have not been implemented.
- The London Bullion Market Association (LBMA) set global prices for gold and silver market, however, they do not openly share their historical prices, therefore we have had to rely on spot prices in USD from Investing.com and then convert for GBP. Yahoo finance data only dated back to early 2000s. 
- There was a noticable difference between the average LBMA gold price based on data from https://www.gold.org/goldhub/data/gold-prices and the spot price on investing.com. This difference is notable before 2000, after which they align.
However, Investing.com spot price agrees with https://www.bullionbypost.co.uk/gold-price/20-year-gold-price-chart-ounce-gbp/ so it is assumed that the gold.org prices before 2000 are inaccurate.
