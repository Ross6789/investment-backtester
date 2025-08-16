from backend.utils.metadata import update_asset_metadata_csv
import yfinance as yf
import polars as pl


actions = yf.download('^STOXX50E','1999-01-01','2025-08-01',auto_adjust=False, actions=True)
no_actions = yf.download('^STOXX50E','1999-01-01','2025-08-01',auto_adjust=False, actions=False)

print(actions.info)

print(no_actions.info)