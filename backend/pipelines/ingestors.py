import yfinance as yf
import polars as pl
import pandas as pd

class YFinanceIngestor:
    def __init__(self,tickers,start_date,end_date):
        self.tickers = tickers
        self.start_date = start_date
        self.end_date = end_date
        self.data = None

    # Method to download data using yfinance
    def download_data(self) -> pd.DataFrame:
        print('Starting download...')
        raw_data = yf.download(self.tickers, self.start_date, self.end_date, auto_adjust= False)
        print('Download complete.')
        return raw_data
    

    # Method to remove unnecessary columns, combine data for each ticker and convert to polars dataframe
    def transform_data(self, raw_data: pd.DataFrame):
        dfs = []
        
        for ticker in self.tickers:

            # Filter relevant columns using pandas
            filtered_df = raw_data.loc[:, [('Adj Close', ticker), ('Close', ticker)]] 

            # Rename column names
            filtered_df.columns = ['Adj Close', 'Close']

            # Drop rows with missing price data
            filtered_df_clean = filtered_df.dropna(subset=['Adj Close', 'Close'])

            # Reset index to return date to a regular column
            filtered_df_reset = filtered_df_clean.reset_index()

            # Add ticker column
            filtered_df_reset['Ticker'] = ticker

            # Add df to list
            dfs.append(filtered_df_reset)

        # Concatenate all tickers into a single pandas DataFrame
        combined_data_pd = pd.concat(dfs, ignore_index=True)

        # Convert to Polars
        combined_data_pl = pl.from_pandas(combined_data_pd)

        # Convert datetime to date
        combined_data_pl = combined_data_pl.with_columns(pl.col('Date').cast(pl.Date))

        self.data = combined_data_pl

        print('Data cleaned')
        
    def run(self) -> pl.DataFrame:
        raw_data = self.download_data()
        self.transform_data(raw_data)
        return self.data

class CSVIngestor:
    def __init__(self, ticker, source_file_path):
        self.ticker = ticker
        self.csv_path = source_file_path
        self.data = None

    # Method to download data from csv
    def read_data(self) -> pl.DataFrame:
        print(f"Reading file : {self.source_file_path}...")
        raw_data = pl.read_csv(self.source_file_path)
        print("Read complete.")
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
        transformed_data['Ticker'] = self.ticker

        # Convert date column to date
        transformed_data = transformed_data.with_columns(pl.col('Date').cast(pl.Date))

        self.data = transformed_data
        print('Data cleaned')
        
    def run(self) -> pl.DataFrame:
        raw_data = self.read_data()
        self.transform_data(raw_data)
        return self.data


# class PriceDataPipeline:
#     def __init__(self,tickers,start_date,end_date,save_path):
#         self.tickers = tickers
#         self.start_date = start_date
#         self.end_date = end_date
#         self.save_path = save_path

#     # Method to download data using yfinance
#     def download_data(self):
#         print("Starting download...")
#         self.raw_data = yf.download(self.tickers, self.start_date, self.end_date, auto_adjust= False)
#         print("Download complete.")
    

#     # Method to remove unnecessary columns, combine data for each ticker and convert to polars dataframe
#     def transform_data(self):
#         dfs = []
        
#         for ticker in self.tickers:

#             # Filter relevant columns using pandas
#             df_filtered = self.raw_data.loc[:, [('Adj Close', ticker), ('Close', ticker)]] 

#             # Flatten column names
#             df_filtered.columns = ['Adj Close', 'Close']

#             # Drop rows with missing price data
#             df_filtered = df_filtered.dropna(subset=['Adj Close', 'Close'])

#             # Reset index to return date to a regular column
#             df_filtered = df_filtered.reset_index()

#             # Add ticker column
#             df_filtered['Ticker'] = ticker

#             # Add df to list
#             dfs.append(df_filtered)

#         # Concatenate all tickers into a single pandas DataFrame
#         all_data_pd = pd.concat(dfs, ignore_index=True)

#         # Convert to Polars
#         all_data_pl = pl.from_pandas(all_data_pd)

#         # Convert datetime to date
#         all_data_pl = all_data_pl.with_columns(pl.col("Date").cast(pl.Date))

#         self.transformed_data = all_data_pl

#         print("Data filtered")

#     # Method to save the data as a partitioned parquet file
#     def save_data(self):
#         self.transformed_data.write_parquet(
#         self.save_path,
#         use_pyarrow=True,
#         partition_by=["Ticker"]
#         )
#         print("Data saved.")
        
#     def run(self):
#         self.download_data()
#         self.transform_data()
#         self.save_data()

        