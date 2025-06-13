import yfinance as yf
import polars as pl
import pandas as pd

class MetalDataPipeline:
    def __init__(self,tickers,start_date,end_date,save_path):
        self.tickers = tickers
        self.start_date = start_date
        self.end_date = end_date
        self.save_path = save_path

    # Method to download data using yfinance
    def download_data(self):
        print("Starting download...")
        self.raw_data = yf.download(self.tickers, self.start_date, self.end_date, auto_adjust= False)
        print("Download complete.")
    

    # Method to remove unnecessary columns, combine data for each ticker and convert to polars dataframe
    def transform_data(self):
        dfs = []
        
        for ticker in self.tickers:

            # Filter relevant columns using pandas
            df_filtered = self.raw_data.loc[:, [('Adj Close', ticker), ('Close', ticker)]] 

            # Flatten column names
            df_filtered.columns = ['Adj Close', 'Close']

            # Drop rows with missing price data
            df_filtered = df_filtered.dropna(subset=['Adj Close', 'Close'])

            # Reset index to return date to a regular column
            df_filtered = df_filtered.reset_index()

            # Add ticker column
            df_filtered['Ticker'] = ticker

            # Add df to list
            dfs.append(df_filtered)

        # Concatenate all tickers into a single pandas DataFrame
        all_data_pd = pd.concat(dfs, ignore_index=True)

        # Convert to Polars
        all_data_pl = pl.from_pandas(all_data_pd)

        # Convert datetime to date
        all_data_pl = all_data_pl.with_columns(pl.col("Date").cast(pl.Date))

        # Pivot dataframe on date
        transformed_df = all_data_pl.pivot(index="Date",columns="Ticker",values="Close")
        
        # Sort by date
        transformed_df = transformed_df.sort("Date",descending=True)

        self.transformed_data = transformed_df

        print("Data filtered")

    # Method to save the data as a csv file
    def save_data(self):
        self.transformed_data.write_csv(
        self.save_path#,
        # use_pyarrow=True,
        # partition_by=["Ticker"]
        )
        print("Data saved.")
        
    def run(self):
        self.download_data()
        self.transform_data()
        self.save_data()