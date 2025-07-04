import polars as pl
from backend.pipelines.loader import get_backtest_data
from backend.config import get_backtest_data_path
from datetime import date

def generate_master_calendar(backtest_data) -> pl.DataFrame:
    master_calendar = (
        backtest_data
        .group_by('date')
        .agg([
            pl.col('ticker').filter(pl.col('is_trading_day')==True).unique().sort().alias('trading_tickers')
        ])
        .sort('date')
    )
    return master_calendar

def generate_ticker_active_dates(backtest_data) -> pl.DataFrame:
    ticker_active_dates = (
        backtest_data
        .filter(pl.col('is_trading_day')==True)
        .group_by('ticker')
        .agg([
            pl.col('date').min().alias('first_active_date'),
            pl.col('date').max().alias('last_active_date')
        ])
        .sort('ticker')
    )
    return ticker_active_dates

tickers = ['AAPL','GOOG','MSFT','AU','SI']
start_date = date(1980,1,1)
print(start_date)
end_date = date(2025,1,1)
path = get_backtest_data_path()
save_path = '/Volumes/T7/investment_backtester_data/calendar.csv'
print(path)
backtest_data = get_backtest_data(path,tickers,start_date,end_date)
print(backtest_data.filter(pl.col('ticker')=='AU'))
calendar = generate_master_calendar(backtest_data)
print(calendar)

ticker_active_dates = generate_ticker_active_dates(backtest_data)
print(ticker_active_dates)

# csv_calendar = calendar.with_columns(
#     pl.col("trading_tickers").cast(pl.List(pl.String)).map_elements(lambda x: ",".join(x)).alias("trading_tickers")
# )

# csv_calendar.write_csv(save_path)