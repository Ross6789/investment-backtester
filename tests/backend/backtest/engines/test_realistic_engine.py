import pytest
import polars as pl
from enum import Enum
from datetime import date, timedelta
from dateutil.relativedelta import relativedelta

from backend.backtest.engines import RealisticEngine
from backend.core.models import TargetPortfolio, BacktestConfig, Strategy, RecurringInvestment, RebalanceFrequency

# --- Fixtures ---

@pytest.fixture
def mock_config():
    return BacktestConfig(
        start_date=date(2025, 1, 1),
        end_date=date(2025, 1, 6),
        target_portfolio=TargetPortfolio({"AAPL": 0.5, "MSFT": 0.5}),
        strategy=Strategy(
            allow_fractional_shares=True,
            reinvest_dividends=True,
            rebalance_frequency='monthly'
        ),
        initial_investment=10000,
        recurring_investment=RecurringInvestment(
            amount=100,
            frequency='weekly'
        )
    )

@pytest.fixture
def mock_backtest_data():
    return pl.DataFrame({
        "date": [date(2025, 1, d) for d in range(2, 7)] * 2,
        "ticker": ["AAPL"] * 5 + ["MSFT"] * 5,
        "base_price": [150 + i for i in range(5)] + [50 + i for i in range(5)],
        "is_trading_day":[True, True, False, False, True] * 2,
        "dividend":[None, None, 0.20, None, None, None, None, 0.05, None, 0.06]
    })


@pytest.fixture
def realistic_engine(mock_config, mock_backtest_data):
    return RealisticEngine(config=mock_config, backtest_data=mock_backtest_data)

# --- Tests ---

def test_engine_initializes(realistic_engine):
    assert realistic_engine.portfolio.cash_balance == 0
    assert realistic_engine.previous_rebalance_date == date(2025, 1, 2)
    assert realistic_engine.dividend_dates == {date(2025, 1, 4),date(2025, 1, 6)}

@pytest.mark.parametrize(
    "ticker, target_date, expected_date",
    [
        ("AAPL", date(2025, 1, 1), date(2025, 1, 2)),  # Before active
        ("AAPL", date(2025, 1, 2), date(2025, 1, 2)),  # Exact match
        ("MSFT", date(2025, 1, 4), date(2025, 1, 6)),  # Non trading day
        ("TSLA", date(2025, 1, 1), None),              # Never trades
        ("AAPL", date(2026, 1, 1), None),              # After range
    ]
)
def test_next_trading_date(realistic_engine, ticker, target_date, expected_date):
    result = realistic_engine._next_trading_date(ticker, target_date)
    assert result == expected_date


@pytest.mark.parametrize(
    "date, allocations, side, expected_execution_date",
    [
        (date(2025, 1, 2), {"AAPL": 1000.0, "MSFT": 1500.0},'buy',date(2025, 1, 2)),  # trading day
        (date(2025, 1, 4), {"AAPL": 1000.0, "MSFT": 1500.0},'sell',date(2025, 1, 6)),  # non-trading day
        (date(2025, 1, 7), {"AAPL": 1000.0, "MSFT": 1500.0},'sell',None),  # non-executable trade (no further trading days)
    ]
)
def test_queue_orders(realistic_engine, date, allocations, side, expected_execution_date):
    realistic_engine._queue_orders(date, allocations, side)
    orders = realistic_engine.pending_orders
    assert orders.shape == (2,8)
    assert all(status == "pending" for status in orders["status"])
    assert all(date_placed == date for date_placed in orders["date_placed"])
    assert all(date_executed == expected_execution_date for date_executed in orders["date_executed"])
    assert all(order_side == side for order_side in orders["side"])


@pytest.mark.parametrize(
    "side, units_returned, executed_date_a, executed_date_b, expected_executed_shape, expected_pending_shape, expected_executions", 
    [
        ("buy",10,date(2025,1,2),date(2025,1,2),(2,8),(0,8),2), # multiple succesful buys
        ("sell", 5, date(2025,1,2),date(2025,1,2),(2,8),(0,8),2), # multiple successful sells
        ("buy", 0,date(2025,1,2),date(2025,1,2),(2,8),(0,8),2), # failed orders
        ("buy",10,date(2025,1,2),date(2025,1,3),(1,8),(1,8),1), # partial execution
        ("buy",10,date(2025,1,3),date(2025,1,3),(0,0),(2,8),0), # future execution , shape = (0,0) because the executed df does not exist yet
    ]
)
def test_execute_orders(realistic_engine, mocker, side, units_returned, executed_date_a, executed_date_b, expected_executed_shape, expected_pending_shape,expected_executions):
    date_ = date(2025, 1, 2)
    realistic_engine.pending_orders = pl.DataFrame(
        {
            "ticker": ["AAPL","MSFT"],
            "target_value": [1000.0]*2,
            "date_placed": [date(2025, 1, 2)]*2,
            "date_executed": [executed_date_a,executed_date_b],
            "side": [side]*2,
            "base_price": [None]*2,
            "units": [None]*2,
            "status": ["pending"]*2
        }
    )

    # Mock portfolio methods
    mock_method = mocker.patch.object(
        realistic_engine.portfolio,
        "invest" if side == "buy" else "sell",
        return_value=units_returned
    )

    # Run
    realistic_engine._execute_orders(date_, prices={"AAPL": 150.0,"MSFT": 100.0})

    pending = realistic_engine.pending_orders
    executed = realistic_engine.executed_orders
    assert pending.shape == expected_pending_shape
    assert executed.shape == expected_executed_shape
    if not executed.is_empty(): 
        assert executed[0, "ticker"] == "AAPL"
        assert executed[0, "base_price"] == 150.0
        assert executed[0, "units"] == units_returned
        assert executed[0, "status"] == "fulfilled" if units_returned > 0 else "failed"

    # Assert portfolio method was called correctly
    assert mock_method.call_count == expected_executions
    if expected_executions >= 1:
        mock_method.assert_any_call("AAPL", 1000.0, 150.0, True)
    if expected_executions == 2:
        mock_method.assert_any_call("MSFT", 1000.0, 100.0, True)


@pytest.mark.parametrize(
    "date_,expected",
    [
        (date(2025,1,2),True), # 2 tickers trading
        (date(2025,1,3),False), # 1 ticker trading
        (date(2025,1,4),False) # 0 tickers trading
    ]
)
def test_all_active_tickers_trading(realistic_engine,date_,expected):

    # Arrange - modify master calendar to have day where only 1 ticker is trading
    realistic_engine.calendar_dict[date(2025,1,3)]["trading_tickers"] = {"AAPL"}

    result = realistic_engine._all_active_tickers_trading(date_)
    assert result == expected


@pytest.mark.parametrize(
    "freq, delta, expected",
    [
        (RebalanceFrequency.DAILY, timedelta(days=1), True),
        (RebalanceFrequency.WEEKLY, timedelta(weeks=1), True),
        (RebalanceFrequency.MONTHLY, relativedelta(months=1), True),
        (RebalanceFrequency.QUARTERLY, relativedelta(months=3), True),
        (RebalanceFrequency.YEARLY, relativedelta(years=1), True),
        (RebalanceFrequency.WEEKLY, timedelta(days=6), False),
        (RebalanceFrequency.MONTHLY, relativedelta(days=27), False),
        (RebalanceFrequency.QUARTERLY, relativedelta(days=87), False),
        (RebalanceFrequency.YEARLY, relativedelta(days=364), False),
    ]
)
def test_should_rebalance_valid_frequencies(realistic_engine, mocker, freq, delta, expected):
    # Mock all tickers trading
    mocker.patch.object(realistic_engine, "_all_active_tickers_trading", return_value=True)

    last_date = date(2025, 1, 1)
    current_date = last_date + delta

    result = realistic_engine._should_rebalance(current_date, last_date, freq)
    assert result == expected

def test_should_rebalance_tickers_not_trading(realistic_engine, mocker):
    mocker.patch.object(realistic_engine, "_all_active_tickers_trading", return_value=False)
    result = realistic_engine._should_rebalance(date(2025,1,2), date(2025,1,1), RebalanceFrequency.DAILY)
    assert result is False

def test_should_rebalance_invalid_frequency(realistic_engine, mocker):
    mocker.patch.object(realistic_engine, "_all_active_tickers_trading", return_value=True)

    class FakeFrequency(Enum):
        BAD = "bad"

    with pytest.raises(ValueError, match="Invalid rebalance frequency"):
        realistic_engine._should_rebalance(date(2025,1,2), date(2025,1,1), FakeFrequency.BAD)

                
    # def rebalance(self, current_date: date, prices: dict[str, float], normalized_target_weights: dict[str, float]) -> None:

    # def run(self) -> RealisticBacktestResult:



