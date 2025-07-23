import pytest
from datetime import date
import polars as pl

from backend.backtest.engines import BaseEngine
from backend.core.models import BacktestConfig, BacktestResult, TargetPortfolio, Strategy,RecurringInvestment


# --- Dummy subclass to test abstract BaseEngine ---
class DummyEngine(BaseEngine):
    def rebalance(self, current_date, prices, normalized_target_weights):
        pass

    def run(self) -> BacktestResult:
        pass


# --- Fixtures ---

@pytest.fixture
def mock_backtest_data():
    return pl.DataFrame({
        "date": [date(2025, 1, 2), date(2025, 1, 3), date(2025, 1, 4), date(2025, 1, 2)],
        "ticker": ["AAPL", "AAPL", "AAPL", "MSFT"],
        "base_price": [100.0, 101.0, 102.0, 200.0],
        "is_trading_day": [True, True, False, True],
    })


@pytest.fixture
def mock_config():

    return BacktestConfig(
        start_date=date(2025, 1, 1),
        end_date=date(2025, 1, 5),
        target_portfolio=TargetPortfolio({"AAPL": 0.6, "MSFT": 0.4}),
    )


@pytest.fixture
def dummy_engine(mock_config, mock_backtest_data):
    return DummyEngine(config=mock_config, backtest_data=mock_backtest_data)


# --- Tests ---

def test_generate_master_calendar_structure(dummy_engine):
    calendar_df = dummy_engine.calendar_df
    calendar_dict = dummy_engine.calendar_dict

    assert isinstance(calendar_df, pl.DataFrame)
    assert isinstance(calendar_dict, dict)
    assert "active_tickers" in calendar_df.columns
    assert "trading_tickers" in calendar_df.columns
    assert date(2025, 1, 2) in calendar_dict
    assert isinstance(calendar_dict[date(2025, 1, 2)]["active_tickers"], set)


def test_get_first_active_date(dummy_engine):
    assert dummy_engine.first_active_date == date(2025, 1, 2)


def test_find_active_tickers(dummy_engine):
    result = dummy_engine._find_active_tickers(date(2025, 1, 2))
    assert result == {"AAPL", "MSFT"}


def test_normalize_portfolio_targets_sums_to_one(dummy_engine):
    weights = dummy_engine._normalize_portfolio_targets(date(2025, 1, 2))
    assert round(sum(weights.values()), 5) == 1.0
    assert set(weights.keys()).issubset({"AAPL", "MSFT"})


def test_get_ticker_allocations_by_target(dummy_engine):
    weights = {"AAPL": 0.6, "MSFT": 0.4}
    total = 1000
    allocs = dummy_engine._get_ticker_allocations_by_target(weights, total)
    assert allocs == {"AAPL": 600.0, "MSFT": 400.0}


def test_get_prices_on_date(dummy_engine):
    prices = dummy_engine._get_prices_on_date(date(2025, 1, 2))
    assert prices == {"AAPL": 100.0, "MSFT": 200.0}
