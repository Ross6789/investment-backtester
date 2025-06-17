import polars as pl

class PriceDataPipeline:
    def __init__(self, price_ingestors, save_file_path):
        self.price_ingestors = price_ingestors    
        self.save_path = save_file_path

    # Method to compile data from different sources (ingestors)
    def combine_data(self):
        
        dfs = []
        
        for ingestor in self.price_ingestors:
            df = ingestor.run()
            dfs.append(df)
        
        self.combined_data = pl.concat(dfs)
    
    # Method to save the data as a partitioned parquet file
    def save_data(self):
        self.combined_data.write_parquet(
        self.save_path,
        use_pyarrow=True,
        partition_by=["Ticker"]
        )
        print("Data saved.")
        
    def run(self):
        self.combine_data()
        self.save_data()
