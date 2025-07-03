import polars as pl
from typing import List
from datetime import date
from backend.config import get_backtest_data_path
from backend.choices import BacktestMode

def get_backtest_data(backtest_mode : BacktestMode, tickers : List[str], start_date: date, end_date: date) -> pl.DataFrame:

    backtest_data_path = get_backtest_data_path()

    # Retrieve columns based on backtest mode
    match backtest_mode:
        case 'adjusted':
            columns_required = ['date','ticker','adj_close']
            column_rename_mapping = {'adj_close' : 'price'}
        case 'manual':
            columns_required = ['date','ticker','close','dividend']
            column_rename_mapping = {'close': 'price'}
        case _:
            raise ValueError (f'Invalid backtest mode : {backtest_mode}')
    
    # lazy query prices, filtering by ticker and date
    data = (
        pl.scan_parquet((backtest_data_path))
        .filter(
            (pl.col('date')>= start_date)&
            (pl.col('date')<= end_date)&
            (pl.col('ticker').is_in(tickers))
        )
        .select(columns_required)
        .rename(column_rename_mapping)
        .collect()
    )

    return data


