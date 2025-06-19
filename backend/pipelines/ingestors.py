import yfinance as yf
import time
import polars as pl
import pandas as pd

class YFinanceIngestor:
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
            raw_data = yf.download(tickers_batch, self.start_date, self.end_date, auto_adjust= False)
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
                filtered_df = raw_data.loc[:, [('Adj Close', ticker), ('Close', ticker)]] 
            except KeyError:
                raise KeyError(f"Required columsn for ticker {ticker} not found in raw_data")
            
            # Rename column names
            filtered_df.columns = ['Adj Close', 'Close']

            # Drop rows with missing price data
            filtered_df_clean = filtered_df.dropna(subset=['Adj Close', 'Close'])

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
    
class CSVIngestor:
    def __init__(self, ticker, source_path, start_date, end_date):
        if start_date > end_date:
            raise ValueError("Start date must be after the end date")
        self.ticker = ticker
        self.source_path = source_path
        self.start_date = start_date
        self.end_date = end_date
        self.data = None

    # Method to download data from csv
    def read_data(self) -> pl.DataFrame:
        print(f"Reading file : {self.source_path}...")
        try:
            raw_data = pl.read_csv(self.source_path)
        except FileNotFoundError:
            raise FileNotFoundError(f"CSV file not found at path: {self.source_path}") 
        except Exception as e:
            raise RuntimeError(f"Error occured when trying to read csv: {e}")
        print("Read complete.")
        if raw_data.is_empty():
            raise ValueError(f"CSV file at {self.source_path} is empty.")
        return raw_data

    # Method to remove unnecessary columns, combine data for each ticker and convert to polars dataframe
    def transform_data(self, raw_data: pl.DataFrame): 

        # Clean csv files based on column count      
        if len(raw_data.columns)==3:
            transformed_data = raw_data.rename({
                raw_data.columns[0]:'Date',
                raw_data.columns[1]:'Adj Close',
                raw_data.columns[2]:'Close',
            })
        elif len(raw_data.columns)==2:
            transformed_data = raw_data.select([
                pl.col(raw_data.columns[0]).alias('Date'),
                pl.col(raw_data.columns[1]).alias('Adj Close'),
                pl.col(raw_data.columns[1]).alias('Close')
            ])
        else:
            raise ValueError("Invalid number of columns in CSV file")
        
        # Add ticker column
        transformed_data = transformed_data.with_columns(pl.lit(self.ticker).alias("Ticker"))

        try:
            # Convert date column to date
            transformed_data = transformed_data.with_columns(pl.col('Date').str.strptime(pl.Date,"%d/%m/%Y"))
        except Exception as e:
            raise Exception(f"Error while trying to parse the csv date column, ensure it is in the format 'dd/mm/yyyy': {e}")

        # Filter based on start date
        if self.start_date:
            transformed_data = transformed_data.filter(
                pl.col('Date') >= pl.lit(self.start_date).cast(pl .Date)
                )

        # Filter based on end date
        if self.end_date:
            transformed_data = transformed_data.filter(
                pl.col('Date') <= pl.lit(self.end_date).cast(pl.Date)
                )

        self.data = transformed_data.sort('Date')
        print('Data cleaned')
        
    def run(self) -> pl.DataFrame:
        raw_data = self.read_data()
        self.transform_data(raw_data)
        return self.data

        