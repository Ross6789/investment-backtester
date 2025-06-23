import polars as pl
from datetime import timedelta
from typing import List
from backend.utils import parse_date

def get_price_data(price_data_path: str,tickers : List[str], start_date: str, end_date: str, buffer_days: int = 10) -> pl.DataFrame:
    
    # parse dates
    try:
        start_date = parse_date(start_date)
        end_date = parse_date(end_date)
    except ValueError as err:
        print(f'Error: {err}')

    # adjust date range for forward filling (incase start date falls on a non trading day)
    start_date_extended = start_date - timedelta(days=buffer_days)
    end_date_extended = end_date + timedelta(days=buffer_days)

    # dataframe with complete date range
    all_dates_df = pl.DataFrame({
        'date': pl.date_range(start=start_date_extended, end=end_date_extended,interval='1d', eager=True)
    })

    # lazy query prices, filtering by ticker and date
    prices = (
        pl.scan_parquet((price_data_path))
        .filter(
            (pl.col('date')>= start_date_extended)&
            (pl.col('date')<= end_date_extended)&
            (pl.col('ticker').is_in(tickers))
        )
    )

    # pivot price dataframe to wide format
    pivoted_df = (
        prices.collect()
        .pivot(on='ticker',index='date', values=['adj_close','close'])
    )

    # Join all dates df to price df
    joined_df = all_dates_df.join(pivoted_df,on='date',how='left')

    # Join prices with full date df, forward fill, refilter and sort
    filled_df = (
        all_dates_df.join(pivoted_df,on='date',how='left')
        .fill_null(strategy='forward')
        .filter((pl.col('date')>= start_date)&(pl.col('date')<= end_date))
        .sort('date')
    )
    return filled_df
