from backend.pipelines.ingestors import YFinanceIngestor

# Configuration
tickers = ["GC=F","SI=F"]
batch_size = 100
start_date = "1970-01-01"
end_date = "2025-06-12"
save_path = "/Volumes/T7/investment_backtester_data/raw/metals/yfinance/yfinance_metals.csv"

# Ingest yfinance data
ingestor = YFinanceIngestor(tickers,batch_size,start_date,end_date)
metal_df = ingestor.run()

# Drop adj close column (same as close since no corporate actions)
metal_df.drop("Adj Close")

# Pivot dataframe on date
transformed_df = metal_df.pivot(index="Date",on="Ticker",values="Close")
        
# Sort by date
sorted_df = transformed_df.sort("Date",descending=True)

# Rename columns
renamed_df = sorted_df.rename({"GC=F":"Gold(USD)","SI=F":"Silver(USD)"})

# Save to file
renamed_df.write_csv(save_path)
print("File saved")
