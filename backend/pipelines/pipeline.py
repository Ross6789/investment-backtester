import polars as pl

class DataPipeline:
    def __init__(self, ingestors, save_file_path):
        self.ingestors = ingestors    
        self.save_path = save_file_path

    # Method to compile data from different sources (ingestors)
    def combine_data(self):
        dfs = []
        
        for ingestor in self.ingestors:
            try:
                df = ingestor.run()
                if not df.is_empty():
                    dfs.append(df)
                else: 
                    print(f"Warning: no data returned from ingestor")
            except Exception as e:
                print(f"Error running ingestor : {e}")
                continue
        
        for df in dfs:
            print(df.head())
        
        if dfs:
            self.combined_data = pl.concat(dfs)
        else:
            raise ValueError("No data ingested, therefore file save has been aborted")
        
    
    # Method to save the data as a partitioned parquet file
    def save_data(self):
        self.combined_data.write_parquet(
        self.save_path,
        use_pyarrow=True,
        partition_by=["ticker"]
        )
        print(f"Data saved to {self.save_path}.")
        
    def run(self):
        try:
            self.combine_data()
            self.save_data()
        except Exception as e:
            raise Exception(f"data pipeline failed : {e}")
        