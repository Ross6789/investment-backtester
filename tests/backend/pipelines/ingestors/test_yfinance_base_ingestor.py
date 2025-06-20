from unittest.mock import patch
import pytest
import pandas as pd
import polars as pl
from backend.pipelines.ingestors import YFinancePriceIngestor
from datetime import date
from polars.testing import assert_frame_equal as pl_assert_frame_equal

@pytest.fixture
def sample_yf_download_df():
    return pd.DataFrame({
        ('Close','AAPL'):[1000,1002],
        ('Adj Close','AAPL'):[1003,1005],
        ('Another Column','AAPL'):[0,0],
        ('Close','GOOG'):[500,502],
        ('Adj Close','GOOG'):[503,505],
        ('Another Column','GOOG'):[0,0]
    }, index=pd.to_datetime(['2025-01-01','2025-01-02']))

@pytest.fixture
def sample_pl_transformed_df():
    return pl.DataFrame({
        'date':[date(2025,1,1),date(2025,1,2),date(2025,1,1),date(2025,1,2)],
        'adj_close':[1003,1005,503,505],
        'close':[1000,1002,500,502],
        'ticker':['AAPL','AAPL','GOOG','GOOG']
    })

@pytest.fixture
def sample_ingestor():
    return YFinancePriceIngestor(['AAPL','GOOG'],2,'2025-01-01','2025-01-03')

@pytest.mark.parametrize('tickers,batch_size,start_date,end_date,expected_error_type, expected_error_message',[
    # Invalid batch size type
    (['AAPL', 'GOOG'],'two','2025-01-01','2025-01-02',TypeError,'Batch size must be an integer'),
    # Invalid batch size number
    (['AAPL', 'GOOG'],0,'2025-01-01','2025-01-02',ValueError,'Batch size must be greater than zero'),
    # Invalid dates
    (['AAPL', 'GOOG'],2,'2025-01-02','2025-01-01',ValueError,'Start date must be after the end date')
])
def test_constructor_invalid(tickers,batch_size,start_date,end_date,expected_error_type, expected_error_message):
    with pytest.raises(expected_error_type, match=expected_error_message):
        ingestor = YFinancePriceIngestor(tickers,batch_size,start_date,end_date)


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

def test_run(monkeypatch, sample_yf_download_df,sample_pl_transformed_df,sample_ingestor):

    monkeypatch.setattr(sample_ingestor,"_batch_tickers", lambda: iter([['AAPL','GOOG']]))
    monkeypatch.setattr(sample_ingestor,"download_data",lambda batch: sample_yf_download_df)
    monkeypatch.setattr(sample_ingestor,"transform_data", lambda df: setattr(sample_ingestor,"data",sample_pl_transformed_df))

    result = sample_ingestor.run()
    pl_assert_frame_equal(result, sample_pl_transformed_df)
