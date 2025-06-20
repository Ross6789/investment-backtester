import polars as pl
from pathlib import Path

class DataPipeline:
    def __init__(self, ingestors, save_dir_base_path):
        self.ingestors = ingestors    
        self.save_dir_base_path = save_dir_base_path

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
        save_dir_base_path = Path(self.save_dir_base_path)
        for (ticker,) , ticker_df in self.combined_data.group_by("ticker"):
            folder = save_dir_base_path / f"ticker={ticker}"
            folder.mkdir(parents=True, exist_ok=True)
            ticker_df.write_parquet(folder / "data.parquet")
        print(f"Data saved to {save_dir_base_path}.")
        
    def run(self):
        try:
            self.combine_data()
            self.save_data()
        except Exception as e:
            raise Exception(f"data pipeline failed : {e}")
        