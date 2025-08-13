import polars as pl
from typing import List
from datetime import date
from backend.core.paths import get_asset_data_path, get_asset_metadata_csv_path, get_fx_data_path, get_benchmark_metadata_csv_path, get_benchmark_data_path
from backend.core.enums import BacktestMode, BaseCurrency

def get_backtest_data(backtest_mode : BacktestMode, base_currency: BaseCurrency, tickers : List[str], start_date: date, end_date: date) -> pl.DataFrame:
    """
    Load backtest data for given tickers and date range, with prices converted to a specified base currency.

    This function:
    - Loads price and related data based on the selected backtest mode.
    - Filters data by ticker and date.
    - Retrieves the native currency for each ticker.
    - Loads foreign exchange (FX) rates to convert native prices to the base currency.
    - Applies a static conversion for GBX (pence) prices to GBP.
    - Calculates the 'base_price' by converting native prices using FX rates where necessary.
    
    Args:
        backtest_mode (BacktestMode): Mode specifying which columns to load and use.
        base_currency (BaseCurrency): The target currency to which all prices should be converted.
        tickers (List[str]): List of ticker symbols to include.
        start_date (date): Start date for filtering the backtest data.
        end_date (date): End date for filtering the backtest data.

    Returns:
        pl.DataFrame: Polars DataFrame containing at least the following columns:
            - 'date': Date of the price data.
            - 'ticker': Ticker symbol.
            - 'native_price': Price in the ticker's native currency (adjusted for GBX to GBP where applicable).
            - 'base_price': Price converted to the specified base currency.
            - 'native_currency': The original currency of the price after static conversion.
            - 'exchange_rate': The FX rate used for conversion to the base currency.
            - Other columns depending on backtest mode (e.g., 'dividend', 'is_trading_day').

    Note:
        The function collects the lazy Polars DataFrame to eager mode before returning. 
        For very large datasets, consider modifying to return a LazyFrame for downstream lazy processing.
    """
    backtest_data_path = get_asset_data_path()
    metadata_path = get_asset_metadata_csv_path()
    fx_data_path = get_fx_data_path()

    # Retrieve columns based on backtest mode
    match backtest_mode:
        case BacktestMode.BASIC:
            columns_required = ['date','ticker','adj_close','is_trading_day']
            column_rename_mapping = {'adj_close' : 'native_price'}
        case BacktestMode.REALISTIC:
            columns_required = ['date','ticker','close','is_trading_day','dividend']
            column_rename_mapping = {'close': 'native_price'}
        case _:
            raise ValueError (f'Invalid backtest mode : {backtest_mode}')
    
    # Filter backtest data by dates and tickers
    filtered_backtest_data = (
        pl.scan_parquet(backtest_data_path)
        .filter(
            (pl.col('date')>= start_date)&
            (pl.col('date')<= end_date)&
            (pl.col('ticker').is_in(tickers))
        )
        .select(columns_required)
        .rename(column_rename_mapping)
    )

    # Get native currencies from metadata file
    ticker_currencies = (
        pl.scan_csv(metadata_path)
        .filter(pl.col('ticker').is_in(tickers))
        .select(['ticker','currency'])
    )

    currencies_used = ticker_currencies.select('currency').unique().collect().to_series().to_list()

    # Get FX rates
    fx_rates = (
        pl.scan_parquet(fx_data_path)
        .filter(
            (pl.col('from_currency').is_in(currencies_used))&
            (pl.col('to_currency')== base_currency.value)
        )
    )


    # Join backtest data to metadata and fx rates
    joined_data = (
        filtered_backtest_data
        .join(ticker_currencies, on='ticker',how='left')
        .join(fx_rates, left_on=['date','currency'], right_on=['date','from_currency'], how='left') # from_currency col on right will simply become currency after join
    )

    # Static conversions : convert from GBX (pence) to GBP
    if 'GBX' in currencies_used:
        joined_data = (
            joined_data.with_columns([
                pl.when(pl.col('currency')=='GBX')
                .then(pl.col('native_price') / 100)
                .otherwise(pl.col('native_price'))
                .alias('native_price'),

                pl.when(pl.col('currency')=='GBX')
                .then(pl.lit('GBP'))
                .otherwise(pl.col('currency'))
                .alias('currency')
            ])
        )

    # Dynamic conversions : calculate base_price using rate if necessary
    converted_backtest_data = (
        joined_data.with_columns([
            # For tickers already in the currency, keep the price otherwise convert
            pl.when(pl.col('currency') == base_currency.value)
            .then(pl.col('native_price'))
            .otherwise(pl.col('native_price') * pl.col('rate'))
            .alias('base_price'),

            pl.col('currency').alias('native_currency'), # rename
            pl.col('rate').alias('exchange_rate') # rename
        ])
        .drop(['currency','to_currency','rate'])
    )

    return converted_backtest_data.collect()  # Convert backtest data from lazy to eager (can be reverted back to lazy if memory issues are encountered)


def get_benchmark_data(base_currency: BaseCurrency, tickers: list[str], start_date: date, end_date: date) -> pl.LazyFrame:

    benchmark_data_path = get_benchmark_data_path()
    
    # Filter backtest data by dates and tickers
    filtered_benchmark_data = (
        pl.scan_parquet(benchmark_data_path)
        .filter(
            (pl.col('date')>= start_date)&
            (pl.col('date')<= end_date)&
            (pl.col('ticker').is_in(tickers))&
            (pl.col('currency')==(base_currency))
        )
        .select('date','ticker','price')
    )

    return filtered_benchmark_data 