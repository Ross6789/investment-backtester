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

## Dividend Data

| Column       | Type     | Description                     |
|--------------|----------|---------------------------------|
| Date         | Date     | Date of dividend payout         |
| Dividend     | Float    | Dividend amount                 |
| Ticker       | String   | Asset symbol                    |

---

## Metadata

| Column        | Type     | Description                              |
|---------------|----------|------------------------------------------|
| Ticker        | String   | Unique symbol                            |
| Asset Name    | String   | Full name                                |
| Asset Type    | String   | e.g. 'ETF', 'Stock', 'Crypto'            |
| Source        | String   | API/data provider                        |
| Currency      | String   | Native trading currency                  |

Stored in: `/backend/pipelines/asset_info.csv`
