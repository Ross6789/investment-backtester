# import sys
# import os
# sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..","..")))

# import pytest
# from unittest.mock import patch
# import tempfile
# import os
# import pandas as pd
# import polars as pl
# from backend.pipelines.ingestors import PriceDataPipeline

# TEST_TICKERS = ["AAPL","GOOG"]
# TEST_START_DATE = "2024-01-01"
# TEST_END_DATE = "2025-01-01"

# # Sample fake yfinance data shaped like yf.download output
# def create_fake_yf_data():
#     # MultiIndex columns like: ('Adj Close', 'AAPL'), ('Close', 'AAPL'), etc.
#     dates = pd.date_range(TEST_START_DATE, TEST_END_DATE)
#     tuples = []
#     data = {}
#     for ticker in TEST_TICKERS:
#         for col in ["Adj Close", "Close"]:
#             tuples.append((col, ticker))
#             # just some dummy data
#             data[(col, ticker)] = [100 + i for i in range(len(dates))]

#     # Create multiindex columns
#     cols = pd.MultiIndex.from_tuples(tuples)
#     df = pd.DataFrame(data, index=dates)
#     df.columns = cols
#     df.index.name = "Date"
#     return df

# @pytest.fixture
# def temp_dir():
#     with tempfile.TemporaryDirectory() as tmpdir:
#         yield tmpdir

# @patch("backend.pipelines.price_pipeline.yf.download")
# def test_download_data(mock_yf_download):
#     fake_data = create_fake_yf_data()
#     mock_yf_download.return_value = fake_data

#     pipeline = PriceDataPipeline(TEST_TICKERS, TEST_START_DATE, TEST_END_DATE, "some_path")
#     pipeline.download_data()

#     mock_yf_download.assert_called_once_with(TEST_TICKERS, TEST_START_DATE, TEST_END_DATE, auto_adjust=False)
#     pd.testing.assert_frame_equal(pipeline.raw_data, fake_data)


# def test_transform_data():
#     pipeline = PriceDataPipeline(TEST_TICKERS, TEST_START_DATE, TEST_END_DATE, "some_path")

#     # Assign fake raw_data (same structure as yf.download output)
#     pipeline.raw_data = create_fake_yf_data()

#     pipeline.transform_data()

#     # Check type
#     assert isinstance(pipeline.transformed_data, pl.DataFrame)

#     # Check columns exist
#     cols = pipeline.transformed_data.columns
#     for col in ["Date", "Adj Close", "Close", "Ticker"]:
#         assert col in cols

#     # Check no missing values in Adj Close or Close
#     assert not pipeline.transformed_data.select(pl.col("Adj Close").is_null().any()).item()
#     assert not pipeline.transformed_data.select(pl.col("Close").is_null().any()).item()

# # def test_save_data(temp_dir):
# #     pipeline = PriceDataPipeline([], "", "", temp_dir)

# #     # Create simple polars DataFrame like your transform_data output
# #     df = pl.DataFrame({
# #         "Date": [pl.date(2023, 1, 1), pl.date(2023, 1, 2)],
# #         "Adj Close": [100.0, 101.0],
# #         "Close": [101.0, 102.0],
# #         "Ticker": ["AAPL", "GOOG"]
# #     })
# #     pipeline.transformed_data = df

# #     print("starting test")
# #     pipeline.save_data()

# #     # Check at least one parquet file was created
# #     files = os.listdir(temp_dir)
# #     print(files)
# #     assert any(f.endswith(".parquet") for f in files)

# # @patch("backend.pipelines.price_pipeline.yf.download")
# # def test_run(mock_yf_download, temp_dir):
# #     fake_data = create_fake_yf_data()
# #     mock_yf_download.return_value = fake_data

# #     pipeline = PriceDataPipeline(TEST_TICKERS, TEST_START_DATE, TEST_END_DATE, temp_dir)
# #     pipeline.run()

# #     # Confirm all stages complete and file saved
# #     files = os.listdir(temp_dir)
# #     assert any(f.endswith(".parquet") for f in files)
    