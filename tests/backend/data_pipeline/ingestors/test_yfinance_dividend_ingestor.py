from unittest.mock import patch
import pytest
import pandas as pd
import polars as pl
from backend.data_pipeline.ingestors import YFinanceCorporateActionsIngestor
from datetime import date
from pandas.testing import assert_frame_equal as pd_assert_frame_equal
from polars.testing import assert_frame_equal as pl_assert_frame_equal

@pytest.fixture
def sample_yf_download_df():
    return pd.DataFrame({
        ('Dividends','AAPL'):[0.25,0.24],
        ('Stock Splits','AAPL'):[ 0.0,2.0],
        ('Another Column','AAPL'):[0,0],
        ('Dividends','GOOG'):[0.54,0.0],
        ('Stock Splits','GOOG'):[0.0,4.0],
        ('Another Column','GOOG'):[0,0]
    }, index=pd.to_datetime(['2025-01-01','2025-01-02']))

@pytest.fixture
def sample_pl_transformed_df():
    return pl.DataFrame({
        'date':[date(2025,1,1),date(2025,1,2),date(2025,1,1),date(2025,1,2)],
        'dividends':[0.25,0.24,0.54, None],
        'stock_splits':[None,2.0,None,4.0],
        'ticker':['AAPL','AAPL','GOOG','GOOG']
    })

@pytest.fixture
def sample_ingestor():
    return YFinanceCorporateActionsIngestor(['AAPL','GOOG'],2,'2025-01-01','2025-01-03')

@patch('backend.pipelines.ingestors.yf.download')
def test_download_data(mock_download,sample_yf_download_df,sample_ingestor):
    expected_df = sample_yf_download_df
    mock_download.return_value = expected_df

    ingestor = sample_ingestor

    result = ingestor.download_data(['AAPL','GOOG'])

    mock_download.assert_called_once_with(['AAPL','GOOG'],'2025-01-01','2025-01-03',auto_adjust=False, actions=True)

    pd_assert_frame_equal(result,expected_df)

def test_transform_data(sample_yf_download_df,sample_pl_transformed_df,sample_ingestor):
    raw_data = sample_yf_download_df
    expected_df = sample_pl_transformed_df

    ingestor = sample_ingestor

    ingestor.transform_data(raw_data)

    result = ingestor.data

    pl_assert_frame_equal(result,expected_df)
