from unittest.mock import patch
import pytest
import pandas as pd
from backend.pipelines.ingestors import YFinanceIngestor

@patch('backend.pipelines.ingestors.yf.download')
def test_download_data(mock_download):
    expected_df = pd.DataFrame({
        ('Close','AAPL'):[1000,1002],
        ('Adj Close','AAPL'):[1003,1005],
        ('Close','GOOG'):[500,502],
        ('Adj Close','GOOG'):[503,505]
    }, index=pd.to_datetime(['2025-01-01','2025-01-02']))

    mock_download.return_value = expected_df

    ingestor = YFinanceIngestor(['AAPL','GOOG'],2,'2025-01-01','2025-01-03')

    result = ingestor.download_data(['AAPL','GOOG'])

    mock_download.assert_called_once_with(['AAPL','GOOG'],'2025-01-01','2025-01-03',auto_adjust=False)

    pd.testing.assert_frame_equal(result,expected_df)

# def test_transform_data():

# def test_run():