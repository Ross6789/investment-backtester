import polars as pl
import pandas as pd
from typing import List

class PriceProcessor:
    @staticmethod
    # Method to remove unnecessary columns, combine data for each ticker and convert to polars dataframe
    def clean_yfinance_data(raw_data: pd.DataFrame, tickers: List[str]) -> pl.DataFrame:
        
        print('Starting to clean YFinance price data ...')
        
        dfs = []
        
        for ticker in tickers:

            try:
                # Filter relevant columns using pandas
                filtered_df = raw_data.loc[:, [('Adj Close', ticker), ('Close', ticker)]] 
            except KeyError:
                raise KeyError(f"Required columns for ticker {ticker} not found in raw_data")
            
            # Rename column names
            filtered_df.columns = ['adj_close', 'close']

            # Drop rows with missing price data
            filtered_df_clean = filtered_df.dropna(subset=['adj_close', 'close'])

            if filtered_df_clean.empty:
                print(f"Downloaded data for ticker {ticker} is empty")

            # Reset index to return date to a regular column
            filtered_df_clean.index.name = 'date'
            filtered_df_reset = filtered_df_clean.reset_index()

            # Add ticker column
            filtered_df_reset['ticker'] = ticker

            # Add df to list
            dfs.append(filtered_df_reset)

        try:
            # Concatenate all tickers into a single pandas DataFrame
            combined_data_pd = pd.concat(dfs, ignore_index=True)
        except Exception as e:
            raise Exception(f"Failed to concatenate pandas dfs: {e}")

        try:
            # Convert to Polars
            combined_data_pl = pl.from_pandas(combined_data_pd)
        except Exception as e:
            raise Exception(f"Failed to convert to polars df: {e}")
        
        try:
            # Convert datetime to date
            combined_data_pl = combined_data_pl.with_columns(pl.col('date').cast(pl.Date))
        except Exception as e:
            raise Exception(f"Failed to cast date column: {e}")
        
        print('YFinance price data cleaned')
        return combined_data_pl.sort('date')

    @staticmethod
    # Method to remove unnecessary columns, combine data for each ticker and convert to polars dataframe
    def clean_csv_data(raw_data: pd.DataFrame, ticker: str) -> pl.DataFrame: 
        
        print('Starting to clean CSV price data ...')

        # Clean csv files based on column count      
        if len(raw_data.columns)==3:
            transformed_data = raw_data.rename({
                raw_data.columns[0]:'date',
                raw_data.columns[1]:'adj_close',
                raw_data.columns[2]:'close',
            })
        elif len(raw_data.columns)==2:
            transformed_data = raw_data.select([
                pl.col(raw_data.columns[0]).alias('date'),
                pl.col(raw_data.columns[1]).alias('adj_close'),
                pl.col(raw_data.columns[1]).alias('close')
            ])
        else:
            raise ValueError("Invalid number of columns in CSV file")
        
        # Add ticker column
        transformed_data = transformed_data.with_columns(pl.lit(ticker).alias("ticker"))

        try:
            # Convert date column to date
            transformed_data = transformed_data.with_columns(pl.col('date').str.strptime(pl.Date,"%d/%m/%Y"))
        except Exception as e:
            raise Exception(f"Error while trying to parse the csv date column, ensure it is in the format 'dd/mm/yyyy': {e}")

        print('CSV price data cleaned')
        return transformed_data.sort('date')

    @staticmethod
    def forward_fill(cleaned_prices: pl.DataFrame) -> pl.DataFrame:

        if cleaned_prices.is_empty():
            print('Warning : cleaned prices dataframe is empty')
            return cleaned_prices
        
        # Get min and max dates
        start_date = cleaned_prices.select('date').min().to_series()[0]
        end_date = cleaned_prices.select('date').max().to_series()[0]

        # Create dataframe using daterange
        dates = pl.DataFrame({'date':pl.date_range(start_date,end_date,interval='1d', eager=True)})

        # Add trading dates column
        cleaned_prices = cleaned_prices.with_columns(pl.lit(True).alias('is_trading_day'))

        # Join dates df to prices df and forward fill
        filled_prices = (
            dates.join(cleaned_prices, on='date', how='left')
            .with_columns([
                pl.col('adj_close').fill_null(strategy='forward'),
                pl.col('close').fill_null(strategy='forward'),
                pl.col('is_trading_day').fill_null(False)
            ])
            .sort('date')
        )

        return filled_prices 

class CorporateActionProcessor:

    @staticmethod
    # Method to remove unnecessary columns, combine data for each ticker and convert to polars dataframe
    def clean_yfinance_data(raw_data: pd.DataFrame, tickers: List[str]) -> pl.DataFrame:
        dfs = []
        
        for ticker in tickers:

            try:
                # Filter relevant columns using pandas
                filtered_df = raw_data.loc[:, [('Dividends', ticker), ('Stock Splits', ticker)]] 
            except KeyError:
                raise KeyError(f"Required columns for ticker {ticker} not found in raw_data")
            
            # Rename column names
            filtered_df.columns = ['dividends', 'stock_splits']

            # Drop rows with missing price data
            filtered_df_clean = filtered_df.replace(0.0, None)
            filtered_df_clean = filtered_df_clean.dropna(subset=['dividends', 'stock_splits'],how='all')

            if filtered_df_clean.empty:
                print(f"Downloaded data for ticker {ticker} is empty")

            # Reset index to return date to a regular column
            filtered_df_clean.index.name = 'date'
            filtered_df_reset = filtered_df_clean.reset_index()

            # Add ticker column
            filtered_df_reset['ticker'] = ticker

            # Add df to list
            dfs.append(filtered_df_reset)

        try:
            # Concatenate all tickers into a single pandas DataFrame
            combined_data_pd = pd.concat(dfs, ignore_index=True)
        except Exception as e:
            raise Exception(f"Failed to concatenate pandas dfs: {e}")

        try:
            # Convert to Polars
            combined_data_pl = pl.from_pandas(combined_data_pd)
        except Exception as e:
            raise Exception(f"Failed to convert to polars df: {e}")
        
        try:
            # Convert datetime to date
            combined_data_pl = combined_data_pl.with_columns(pl.col('date').cast(pl.Date))
        except Exception as e:
            raise Exception(f"Failed to cast date column: {e}")
        
        # Enforce column schema
        df_cols_cast = combined_data_pl.with_columns([
            pl.col('dividends').cast(pl.Float64),
            pl.col('stock_splits').cast(pl.Float64)
            ])

        print('Corporate action data cleaned')
        return df_cols_cast.sort('date')


