import yfinance as yf
import pandas as pd
import polars as pl
import time

class YFinanceDividendIngestor:
    def __init__(self,tickers,batch_size,start_date,end_date):
        if not isinstance(batch_size, int):
            raise TypeError("Batch size must be an integer")
        if batch_size < 1:
            raise ValueError("Batch size must be a positive integer")
        if start_date > end_date:
            raise ValueError("Start date must be after the end date")
        self.tickers = tickers
        self.start_date = start_date
        self.batch_size = batch_size
        self.end_date = end_date
        self.data = None

    # Method to batch the tickers to prevent API ban or freezing (_ is convention for private methods)
    def _batch_tickers(self):
        for i in range(0, len(self.tickers), self.batch_size):
            yield self.tickers[i:i + self.batch_size]

    # Method to download data using yfinance
    def download_data(self, tickers_batch) -> pd.DataFrame:
        print('Starting download...')
        try:
            raw_data = yf.download(tickers_batch, self.start_date, self.end_date, auto_adjust= False, actions=True)
        except Exception as e:
            raise RuntimeError(f"Failed to download data for batch {tickers_batch}: {e}")
        
        if raw_data.empty:
            raise ValueError(f"Downloaded data for batch {tickers_batch} is empty.")
        print('Download complete.')
        return raw_data
    

    # Method to remove unnecessary columns, combine data for each ticker and convert to polars dataframe
    def transform_data(self, raw_data: pd.DataFrame):
        dfs = []
        
        for ticker in self.tickers:

            try:
                # Filter relevant columns using pandas
                filtered_df = raw_data.loc[:, [('Dividends', ticker), ('Stock Splits', ticker)]] 
            except KeyError:
                raise KeyError(f"Required columns for ticker {ticker} not found in raw_data")
            
            # Rename column names
            filtered_df.columns = ['Dividends', 'Stock Splits']

            # Drop rows with missing price data
            filtered_df_clean = filtered_df.replace(0.0, None)
            filtered_df_clean = filtered_df_clean.dropna(subset=['Dividends', 'Stock Splits'],how='all')

            if filtered_df_clean.empty:
                print(f"Downloaded data for ticker {ticker} is empty")

            # Reset index to return date to a regular column
            filtered_df_clean.index.name = 'Date'
            filtered_df_reset = filtered_df_clean.reset_index()

            # Add ticker column
            filtered_df_reset['Ticker'] = ticker

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
            combined_data_pl = combined_data_pl.with_columns(pl.col('Date').cast(pl.Date))
        except Exception as e:
            raise Exception(f"Failed to cast date column: {e}")
        
        self.data = combined_data_pl

        print('Data cleaned')
        
    def run(self) -> pl.DataFrame:
        batch_data = []
        for batch in self._batch_tickers():
            print(f"Downloading batch {batch}")
            try:
                batch_data.append(self.download_data(batch))
            except Exception as e:
                print (f"Error downloading batch {batch} : {e}")
                continue
            # add 2 second delay to prevent api restrictions
            time.sleep(2)
        try:
            combined_data = pd.concat(batch_data)
        except Exception as e:
            raise RuntimeError(f"Error during batch concatenation: {e}")
        self.transform_data(combined_data)
        return self.data

tickers = ['AAPL','GOOG','MSFT','TSLA','AMZN']
batch_size = 100
start_date = '1970-01-01'
end_date = '2025-01-01'    

ingestor = YFinanceDividendIngestor(tickers,batch_size,start_date,end_date)
actions_df = ingestor.run()

file_path = '/Volumes/T7/investment_backtester_data/processed/actions.csv'
actions_df.write_csv(file_path)
print(f"File save to {file_path}")
