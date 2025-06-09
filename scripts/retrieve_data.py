import yfinance as yf
import os

# CONFIGURATION
tickers = ["AAPL","GOOG"]
start_date = "2024-12-01"
end_date = "2024-12-31"

# DOWNLOAD PRICE DATA
data = yf.download(tickers, start = start_date, end = end_date, auto_adjust= False, group_by='tickers')
print(data)

