from unittest.mock import patch
import pytest
import pandas as pd
import polars as pl
from backend.pipelines.ingestors import YFinancePriceIngestor
from datetime import date
from pandas.testing import assert_frame_equal as pd_assert_frame_equal
from polars.testing import assert_frame_equal as pl_assert_frame_equal

@pytest.fixture
def sample_pandas_df():
    return pd.DataFrame({
        ('Dividends','AAPL'):[0.25,0.24],
        ('Stock Splits','AAPL'):[ 0.0,2.0],
        ('Another Column','AAPL'):[0,0],
        ('Dividends','GOOG'):[0.54,0.0],
        ('Stock Splits','GOOG'):[0.0,4.0],
        ('Another Column','GOOG'):[0,0]
    }, index=pd.to_datetime(['2025-01-01','2025-01-02']))

@pytest.fixture
def sample_polars_df():
    return pl.DataFrame({
        'date':[date(2025,1,1),date(2025,1,2),date(2025,1,1),date(2025,1,2)],
        'dividends':[0.25,0.24,0.54, pl.Null],
        'stock_splits':[pl.Null,2.0,pl.Null,4.0],
        'ticker':['AAPL','AAPL','GOOG','GOOG']
    })

@pytest.fixture
def sample_ingestor():
    return YFinancePriceIngestor(['AAPL','GOOG'],2,'2025-01-01','2025-01-03')

@pytest.mark.parametrize('tickers,batch_size,expected_batches',[
    # Large set of tickers
    (['AAPL', 'GOOG', 'TSLA', 'MSFT', 'AMZN'],2,[['AAPL','GOOG'],['TSLA','MSFT'],['AMZN']]), 
    # tickers less than batch size
    (['AAPL'],2,[['AAPL']]),
    # Empty ticker list
    ([],2,[]) 
])
def test_batch_tickers(tickers,batch_size,expected_batches):
    ingestor = YFinancePriceIngestor(tickers,batch_size,'2025-01-01','2025-01-03')
    assert list(ingestor._batch_tickers()) == expected_batches


@patch('backend.pipelines.ingestors.yf.download')
def test_download_data(mock_download,sample_pandas_df,sample_ingestor):
    expected_df = sample_pandas_df
    mock_download.return_value = expected_df

    ingestor = sample_ingestor

    result = ingestor.download_data(['AAPL','GOOG'])

    mock_download.assert_called_once_with(['AAPL','GOOG'],'2025-01-01','2025-01-03',auto_adjust=False)

    pd_assert_frame_equal(result,expected_df)

def test_transform_data(sample_pandas_df,sample_polars_df,sample_ingestor):
    raw_data = sample_pandas_df
    expected_df = sample_polars_df

    ingestor = sample_ingestor

    ingestor.transform_data(raw_data)

    result = ingestor.data

    pl_assert_frame_equal(result,expected_df)

def test_run(monkeypatch, sample_pandas_df,sample_polars_df,sample_ingestor):

    monkeypatch.setattr(sample_ingestor,"_batch_tickers", lambda: iter([['AAPL','GOOG']]))
    monkeypatch.setattr(sample_ingestor,"download_data",lambda batch: sample_pandas_df)
    monkeypatch.setattr(sample_ingestor,"transform_data", lambda df: setattr(sample_ingestor,"data",sample_polars_df))

    result = sample_ingestor.run()
    pl_assert_frame_equal(result, sample_polars_df)