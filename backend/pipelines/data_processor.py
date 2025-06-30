import polars as pl

def process_price_data(price_data_path: str) -> pl.DataFrame:
 
    # scan price data
    prices = pl.read_parquet(price_data_path)    

    # Get min and max dates
    start_date = prices.select('date').min().to_series()[0]
    end_date = prices.select('date').max().to_series()[0]

    # Create dataframe using daterange
    dates = pl.DataFrame({'date':pl.date_range(start_date,end_date,interval='1d', eager=True)})

    # Add trading dates column
    prices = prices.with_columns(pl.lit(True).alias('is_trading_day'))

    # Join dates df to prices df and forward fill
    filled_prices = (
        dates.join(prices, on='date', how='left')
        .with_columns([
            pl.col('adj_close').fill_null(strategy='forward'),
            pl.col('close').fill_null(strategy='forward'),
            pl.col('is_trading_day').fill_null(False)
        ])
        .sort('date')
    )

    return filled_prices