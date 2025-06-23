import backend.config as config
import polars as pl
from datetime import date, timedelta
from typing import List


# config
parquet_price_path = config.get_parquet_price_base_path()
adjusted_days = 10
start_date = date(2024,1,1)
end_date = date(2025,1,1)
tickers = ['AAPL','GOOG']

# Adjust date range for forward filling (incase start date falls on a non trading day)
start_date_extended = start_date - timedelta(days=adjusted_days)
end_date_extended = end_date + timedelta(days=adjusted_days)

# dataframe with complete date range
all_dates_df = pl.DataFrame({
    'date': pl.date_range(start=start_date_extended, end=end_date_extended,interval='1d', eager=True)
})

# lazy query prices, filtering by ticker and date
prices = pl.scan_parquet((parquet_price_path)).filter((pl.col('date')>= start_date_extended)&(pl.col('date')<= end_date_extended)&(pl.col('ticker').is_in(tickers))).collect()

# pivot dataframes to wide format
pivot_df = prices.pivot(on='ticker',index='date', values=['adj_close','close'])

# Join all dates df to price df
joined_df = all_dates_df.join(pivot_df,on='date',how='left')

# Forward fill all missing values and refilter
filled_df = joined_df.fill_null(strategy='forward').filter((pl.col('date')>= start_date)&(pl.col('date')<= end_date))

print(prices)
print(all_dates_df)
print(pivot_df)
print(filled_df)