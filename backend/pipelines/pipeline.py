import polars as pl

class PriceDataPipeline:
    def __init__(self, price_ingestors, save_file_path):
        self.price_ingestors = price_ingestors    
        self.save_path = save_file_path

    # Method to compile data from different sources (ingestors)
    def combine_data(self):
        dfs = []
        
        for ingestor in self.price_ingestors:
            try:
                df = ingestor.run()
                if not df.is_empty():
                    dfs.append(df)
                else: 
                    print(f"Warning: no data returned from ingestor")
            except Exception as e:
                print(f"Error running ingestor : {e}")
                continue
        
        self.combined_data = pl.concat(dfs)
    
    # Method to save the data as a partitioned parquet file
    def save_data(self):
        self.combined_data.write_parquet(
        self.save_path,
        use_pyarrow=True,
        partition_by=["Ticker"]
        )
        print(f"Data saved to {self.save_path}.")
        
    def run(self):
        try:
            self.combine_data()
            self.save_data()
        except Exception as e:
            raise Exception(f"Pricedata pipeline failed : {e}")