import polars as pl
from typing import List
from datetime import date

def get_backtest_data(backtest_data_path: str,tickers : List[str], start_date: date, end_date: date) -> pl.DataFrame:


    # lazy query prices, filtering by ticker and date
    data = (
        pl.scan_parquet((backtest_data_path))
        .filter(
            (pl.col('date')>= start_date)&
            (pl.col('date')<= end_date)&
            (pl.col('ticker').is_in(tickers))
        )
        .collect()
    )

    return data
