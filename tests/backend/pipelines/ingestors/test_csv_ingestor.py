# class CSVIngestor:
#     def __init__(self, ticker, source_path, start_date, end_date):
#         self.ticker = ticker
#         self.source_path = source_path
#         self.start_date = start_date
#         self.end_date = end_date
#         self.data = None

#     # Method to download data from csv
#     def read_data(self) -> pl.DataFrame:
#         print(f"Reading file : {self.source_path}...")
#         raw_data = pl.read_csv(self.source_path)
#         print("Read complete.")
#         return raw_data

#     # Method to remove unnecessary columns, combine data for each ticker and convert to polars dataframe
#     def transform_data(self, raw_data: pl.DataFrame): 

#         # Clean csv files based on column count      
#         if len(raw_data.columns)==3:
#             transformed_data = raw_data.rename({
#                 raw_data.columns[0]:'Date',
#                 raw_data.columns[1]:'Adj Close',
#                 raw_data.columns[2]:'Close',
#             })
#         elif len(raw_data.columns)==2:
#             transformed_data = raw_data.select([
#                 pl.col(raw_data.columns[0]).alias('Date'),
#                 pl.col(raw_data.columns[1]).alias('Adj Close'),
#                 pl.col(raw_data.columns[1]).alias('Close')
#             ])
#         else:
#             raise ValueError("Invalid number of columns in CSV file")
        
#         # Add ticker column
#         transformed_data = transformed_data.with_columns(pl.lit(self.ticker).alias("Ticker"))

#         # Convert date column to date
#         transformed_data = transformed_data.with_columns(pl.col('Date').str.strptime(pl.Date,"%d/%m/%Y"))

#         # Filter based on start date
#         if self.start_date:
#             transformed_data = transformed_data.filter(
#                 pl.col('Date') >= pl.lit(self.start_date).cast(pl.Date)
#                 )

#         # Filter based on end date
#         if self.end_date:
#             transformed_data = transformed_data.filter(
#                 pl.col('Date') <= pl.lit(self.end_date).cast(pl.Date)
#                 )

#         self.data = transformed_data
#         print('Data cleaned')
        
#     def run(self) -> pl.DataFrame:
#         raw_data = self.read_data()
#         self.transform_data(raw_data)
#         return self.data
