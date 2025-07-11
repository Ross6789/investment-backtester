import polars as pl
import pandas as pd
import warnings

class PriceProcessor:
    """
    Handles cleaning and transformation of raw price data for use in backtesting pipelines.
    """

    @staticmethod
    def clean_yfinance_data(raw_data: pd.DataFrame, tickers: list[str]) -> pl.DataFrame:
        """
        Clean raw YFinance price data and return a unified Polars DataFrame.

        Extracts 'Adj Close' and 'Close' for each ticker, drops missing values,
        adds 'date' and 'ticker' columns, converts to Polars, and sorts by date.

        Args:
            raw_data (pd.DataFrame): MultiIndex DataFrame from yfinance download.
            tickers (list[str]): List of ticker symbols to process.

        Returns:
            pl.DataFrame: Cleaned Polars DataFrame with columns:
                          ['date', 'adj_close', 'close', 'ticker']

        Raises:
            KeyError: If expected columns are missing for a ticker.
            RuntimeError: If there is no valid data after cleaning.
            Exception: If concatenation or Polars conversion fails.
        """
        print('Cleaning YFinance price data ...')
        dfs = []
        
        for ticker in tickers:

            try:
                # Filter and rename relevant columns (pandas)
                filtered_df = raw_data[[('Adj Close', ticker), ('Close', ticker)]].copy()
                filtered_df.columns = ['adj_close', 'close']

                # Drop rows with missing price data
                filtered_df = filtered_df.dropna(subset=['adj_close', 'close'])
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

            # Convert datetime to date
            combined_data_pl = combined_data_pl.with_columns(pl.col('date').cast(pl.Date))
        
        except Exception as e:
            raise Exception(f"Failed while concatenating and converting to polars: {e}")
        
        print('YFinance price data cleaned.')
        return combined_data_pl.sort(['ticker','date'])
    

    @staticmethod
    def clean_csv_data(raw_data: pd.DataFrame, ticker: str) -> pl.DataFrame: 
        """
        Clean raw CSV price data and return a Polars DataFrame.

        Handles both 2-column (date, price) and 3-column (date, adj_close, close) CSV formats.
        Adds a 'ticker' column, casts 'date' to proper type, and sorts by ticker and date.

        Args:
            raw_data (pd.DataFrame): Raw data loaded from CSV.
            ticker (str): Ticker symbol inferred from filename.

        Returns:
            pl.DataFrame: Cleaned Polars DataFrame with columns:
                          ['date', 'adj_close', 'close', 'ticker']

        Raises:
            ValueError: If the number of columns is not 2 or 3.
            Exception: If conversion to Polars or casting date fails.
        """
        print('Cleaning CSV price data ...')
           
        # Convert to Polars dataframe
        try:
            raw_data_pl = pl.from_pandas(raw_data)
        except Exception as e:
            raise Exception(f"Failed to convert to Polars Dataframe: {e}")
        
        # 3 col csv : assume date | adj_close | close
        if len(raw_data_pl.columns)==3:
            transformed_data = raw_data_pl.rename({
                raw_data_pl.columns[0]:'date',
                raw_data_pl.columns[1]:'adj_close',
                raw_data_pl.columns[2]:'close',
            })

        # 2 col csv : assume date | close and adj_close will be a duplicated using close
        elif len(raw_data_pl.columns)==2:
            transformed_data = raw_data_pl.select([
                pl.col(raw_data_pl.columns[0]).alias('date'),
                pl.col(raw_data_pl.columns[1]).alias('adj_close'),
                pl.col(raw_data_pl.columns[1]).alias('close')
            ])
        else:
            raise ValueError("Invalid number of columns in CSV file")
        
        # Add ticker column
        transformed_data = transformed_data.with_columns(pl.lit(ticker.upper()).alias("ticker"))

        # Cast date column 
        try:
            transformed_data = transformed_data.with_columns(pl.col('date').cast(pl.Date))
        except Exception as e:
            raise Exception(f"Error while trying to cast the date column : {e}")

        print('CSV price data cleaned.')
        return transformed_data.sort('date')


    @staticmethod
    def forward_fill(cleaned_prices: pl.DataFrame) -> pl.DataFrame:
        """
        Forward-fill missing price data for each ticker over its full date range.

        Creates a complete daily date range per ticker, joins with the input price data,
        and forward-fills missing 'adj_close' and 'close' prices for non-trading days.
        Also flags whether each row corresponds to a trading day.

        Args:
            cleaned_prices (pl.DataFrame): Polars DataFrame with columns ['date', 'ticker', 'adj_close', 'close'] containing cleaned trading data.

        Returns:
            pl.DataFrame: Polars DataFrame with forward-filled price data and an 'is_trading_day' boolean column.
        """
        print('Forward filling price data ...')

        # Find relevant date ranges for each ticker
        ticker_date_ranges = (
            cleaned_prices.group_by('ticker').agg([
                pl.col('date').min().alias('min_date'),
                pl.col('date').max().alias('max_date')
            ])
        )

        # Create dataframe of all dates (including non trading) for each ticker using its daterange
        date_dfs = []
        for ticker, min_date, max_date in ticker_date_ranges.iter_rows():
            date_df = pl.DataFrame({'date':pl.date_range(min_date,max_date,interval='1d', eager=True), 'ticker':ticker})
            date_dfs.append(date_df)
        full_date_df = pl.concat(date_dfs)

        # Add trading dates column to cleaned prices df (trading day = true, since cleaned data is only present on trading dates)
        cleaned_prices = cleaned_prices.with_columns(pl.lit(True).alias('is_trading_day'))

        # Join full dates df to prices df and forward fill
        filled_prices = (
            full_date_df.join(cleaned_prices, on=['date','ticker'], how='left')
            .sort(['ticker','date'])
            .with_columns([
                pl.col('adj_close').fill_null(strategy='forward'),
                pl.col('close').fill_null(strategy='forward'),
                pl.col('is_trading_day').fill_null(False)
            ])
        )
        print('Price data forward filled.')
        return filled_prices 
