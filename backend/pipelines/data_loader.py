import polars as pl
from datetime import timedelta
from typing import List, Tuple
from datetime import date

def get_price_data(price_data_path: str,tickers : List[str], start_date: date, end_date: date, buffer_days: int = 10) -> Tuple[pl.DataFrame, List[date]]:
    """
    Loads and processes historical price data for given tickers within a specified date range, 
    including a buffer period for forward-filling missing prices on non-trading days.

    Steps performed:
    - Extends the requested date range by `buffer_days` before and after to ensure continuous price data for forward-filling.
    - Reads price data from a Parquet file, filtered by tickers and extended date range.
    - Pivots the data to a wide format with tickers as columns for 'adj_close' and 'close' prices.
    - Extracts and returns the sorted list of actual trading dates from the data.
    - Joins the complete date range with the price data and forward-fills missing prices.
    - Filters the final dataframe to the original requested date range and sorts by date.

    Args:
        price_data_path (str): Path to the Parquet file containing historical price data.
        tickers (List[str]): List of ticker symbols to load data for.
        start_date (date): Start date of the desired price data range.
        end_date (date): End date of the desired price data range.
        buffer_days (int, optional): Number of days to extend the date range on each side for forward-filling.
                                     Defaults to 10.

    Returns:
        Tuple[pl.DataFrame, List[date]]: 
            - A Polars DataFrame containing forward-filled price data for the requested tickers and date range. 
              Columns include 'date', and for each ticker, 'adj_close' and 'close' prices.
            - A sorted list of trading dates (date objects) present in the original price data within the extended range.
    """
        
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

    # extract trading dates
    trading_dates = sorted(pivoted_df.select('date').to_series().to_list())

    # Join all dates df to price df
    joined_df = all_dates_df.join(pivoted_df,on='date',how='left')

    # Join prices with full date df, forward fill, refilter and sort
    filled_df = (
        all_dates_df.join(pivoted_df,on='date',how='left')
        .fill_null(strategy='forward')
        .filter((pl.col('date')>= start_date)&(pl.col('date')<= end_date))
        .sort('date')
    )
    return filled_df, trading_dates
