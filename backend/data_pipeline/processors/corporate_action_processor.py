import polars as pl
import pandas as pd
import warnings

class CorporateActionProcessor:
    """
    Handles cleaning and transformation of raw corporate action data for use in backtesting pipelines.
    """

    @staticmethod
    def clean_yfinance_data(raw_data: pd.DataFrame, tickers: list[str]) -> pl.DataFrame:
        """
        Cleans raw corporate action data downloaded from yFinance for multiple tickers.

        Replaces 0.0 values with nulls, removes rows with no actions, and adds 'date' and 'ticker' columns.
        Returns a Polars DataFrame sorted by ticker and date.

        Args:
            raw_data (pd.DataFrame): MultiIndex pandas DataFrame from yFinance with ('Dividends', ticker) and 
                                     ('Stock Splits', ticker) columns.
            tickers (List[str]): List of ticker symbols to clean data for.

        Returns:
            pl.DataFrame: Cleaned and structured corporate action data with columns 
                          ['date', 'dividend', 'stock_split', 'ticker'].
        
        Raises:
            KeyError: If required columns are missing for a ticker.
            RuntimeError: If no valid rows remain after cleaning.
            Exception: If concatenation or conversion fails.
        """
        print('Cleaning YFinance corporate action data ...')
        dfs = []
        
        for ticker in tickers:
            try:
                # Filter and rename relevant columns (pandas)
                filtered_df = raw_data[[('Dividends', ticker), ('Stock Splits', ticker)]].copy()
                filtered_df.columns = ['dividend', 'stock_split']
            

                # Drop rows with missing action data
                filtered_df = filtered_df.replace(0.0, None)
                filtered_df = filtered_df.dropna(subset=['dividend', 'stock_split'],how='all')

                if filtered_df.empty:
                    warnings.warn(f"No valid rows found after cleaning for ticker: {ticker}")
                    continue

                # Add date and ticker columns (date is currently index so needs reset to create column)
                filtered_df.index.name = 'date'
                filtered_df = filtered_df.reset_index()
                filtered_df['ticker'] = ticker

                dfs.append(filtered_df)

            except KeyError:
                raise KeyError(f"Missing required columns for {ticker}")
            
        if not dfs:
            raise RuntimeError("No valid rows found after cleaning.")
            
        try:
            # Concatenate all tickers into a single pandas DataFrame
            combined_data_pd = pd.concat(dfs, ignore_index=True)
            combined_data_pl = pl.from_pandas(combined_data_pd)

            # Enforce column schema
            combined_data_pl = combined_data_pl.with_columns([
                pl.col('date').cast(pl.Date),
                pl.col('dividend').cast(pl.Float64),
                pl.col('stock_split').cast(pl.Float64)
                ])
        
        except Exception as e:
            raise Exception(f"Failed while concatenating and converting to polars: {e}")
        
        print('YFinance corporate action data cleaned.')
        return combined_data_pl.sort(['ticker','date'])