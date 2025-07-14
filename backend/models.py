from dataclasses import dataclass, field
from backend.enums import RebalanceFrequency,ReinvestmentFrequency, BacktestMode, BaseCurrency
from backend.utils import parse_enum,validate_positive_amount, validate_date_order
import polars as pl
from datetime import date

@dataclass
class TargetPortfolio:
    """
    Represents a validated dictionary of portfolio asset weightings.

    This class ensures:
    - The weight dictionary is not empty.
    - All individual weights are greater than zero.
    - The total of all weights sums to 1.0 (within a small tolerance).

    Attributes:
        weights (Dict[str, float]): A mapping of ticker symbols to their portfolio weights.

    Raises:
        ValueError: If the weight dictionary is empty, contains weights <= 0 or > 1.0,
                    or the weights do not sum to 1.0.
    """
    weights: dict[str,float] 

    def __post_init__(self):
        if not self.weights:
            raise ValueError("Empty weight dictionary")
        
        for ticker, weight in self.weights.items():
            if weight <= 0 or weight > 1.0:
                raise ValueError(f"Invalid weight for '{ticker}': must be greater than 0 and less than or equal to 1.")
            
        total = sum(self.weights.values())
        if not abs(total-1.0) < 1e-6:
            raise ValueError(f"Portfolio weightings add to {total}. Must equal 1.0")
    
    def get_tickers(self) -> list[str]:
        return list(self.weights.keys())


@dataclass
class RecurringInvestment:
    """
    Defines recurring investment details.

    Attributes:
        amount (float): Investment amount, must be positive.
        frequency (ReinvestmentFrequency or str): Investment interval.

    Raises:
        ValueError: If frequency string is invalid or amount is non-positive.
    """
    amount: float              
    frequency: ReinvestmentFrequency = ReinvestmentFrequency.MONTHLY

    def __post_init__(self):
        if isinstance(self.frequency, str):
            self.frequency = parse_enum(ReinvestmentFrequency, self.frequency)
        validate_positive_amount(self.amount,'recurring investment amount')


@dataclass
class Strategy:
    """
    Configuration for portfolio investment strategy in backtesting.

    Attributes:
        allow_fractional_shares (bool): Enable fractional share purchases.
        reinvest_dividends (bool): Enable dividend reinvestment.
        rebalance_frequency (RebalanceFrequency or str): Portfolio rebalance interval.

    Raises:
        ValueError: If rebalance_frequency string is invalid.
    """
    allow_fractional_shares: bool = True
    reinvest_dividends: bool = True
    rebalance_frequency: RebalanceFrequency = RebalanceFrequency.NEVER

    def __post_init__(self):
        if isinstance(self.rebalance_frequency, str):
            self.rebalance_frequency = parse_enum(RebalanceFrequency, self.rebalance_frequency)


@dataclass
class BacktestConfig:
    """
    Configuration for running a backtest, including parameters defining the
    backtest mode, portfolio, investment schedule, and time period.

    Attributes:
        start_date (date): The start date of the backtest period.
        end_date (date): The end date of the backtest period.
        target_portfolio (TargetPortfolio): The target allocation of assets for the strategy.
        mode (BacktestMode): The backtest mode to use (e.g., basic, realistic).
        base_currency (BaseCurrency): The currency in which the portfolio is denominated (e.g., GBP, USD).
        strategy (Strategy): Parameters defining the investment strategy.
        initial_investment (float): The initial capital amount for the backtest (must be positive).
        recurring_investment (RecurringInvestment | None): Optional recurring investment schedule.

    Raises:
        ValueError: If `mode` or `base_currency` values are invalid.
        ValueError: If `start_date` is after `end_date`.
        ValueError: If `initial_investment` is not a positive value.
    """
    start_date : date
    end_date : date
    target_portfolio : TargetPortfolio
    mode : BacktestMode = BacktestMode.REALISTIC
    base_currency : BaseCurrency = BaseCurrency.GBP
    strategy: Strategy = field(default_factory=Strategy)
    initial_investment : float = 10000
    recurring_investment : RecurringInvestment | None = None

    def __post_init__(self):
        if isinstance(self.mode, str):
            self.mode = parse_enum(BacktestMode,self.mode)
        if isinstance(self.base_currency, str):
            self.base_currency = parse_enum(BaseCurrency,self.base_currency)
        validate_date_order(self.start_date,self.end_date)
        validate_positive_amount(self.initial_investment, 'initial investment')

    def to_flat_dict(self) -> dict[str, str]:
        """
        Returns a flat dictionary representation of the backtest config.

        This method formats all configuration values as strings, ensuring the output
        is suitable for display or export (e.g., CSV metadata headers). Nested fields
        like target portfolio weights and recurring investment are flattened.

        Returns:
            dict[str, str]: A flat dictionary of configuration values.
        """
        return {
            "Start date": str(self.start_date),
            "End date": str(self.end_date),
            "Target portfolio": " ; ".join(f"{k} = {v}" for k, v in self.target_portfolio.weights.items()),
            "Backtest mode": str(self.mode.value),
            "Base currency": str(self.base_currency.value),
            "Allow fractional shares": str(self.strategy.allow_fractional_shares),
            "Reinvest dividends": str(self.strategy.reinvest_dividends),
            "Rebalance frequency": str(self.strategy.rebalance_frequency.value),
            "Initial investment": f"{self.initial_investment:.2f}",
            "Recurring investment amount": f"{self.recurring_investment.amount:.2f}" if self.recurring_investment else "0.00",
            "Recurring investment frequency": str(self.recurring_investment.frequency.value) if self.recurring_investment else "N/A",
        }


@dataclass
class BacktestResult:
    """
    Container for storing the results of a backtest in basic mode.

    Attributes:
        data (pl.DataFrame): The raw price and dividend data used during the backtest.
        calendar (pl.DataFrame): The calendar of all backtest dates with asset activity flags.
        cash (pl.DataFrame): Daily snapshots of the portfolio's cash balance.
        holdings (pl.DataFrame): Daily snapshots of the holdings across all assets.
    """
    data : pl.DataFrame
    calendar : pl.DataFrame
    cash : pl.DataFrame
    holdings : pl.DataFrame


@dataclass
class RealisticBacktestResult(BacktestResult):
    """
    Extended backtest result class for realistic mode, including additional trading information.

    Attributes:
        dividends (pl.DataFrame): Dividend payments received during the backtest.
        orders (pl.DataFrame): Executed trade orders including buys, sells, and rebalances.
    """
    dividends : pl.DataFrame
    orders : pl.DataFrame

@dataclass
class CSVReport:
    """
    Represents a CSV report with optional comments, column headers, and data rows.

    Attributes:
        comments (list[str]): Lines of comments to include at the top of the CSV, 
            typically prefixed by a comment character (e.g., '#').
        headers (list[str]): The list of column headers for the CSV.
        rows (list[tuple]): The data rows of the CSV, where each tuple corresponds to a row.
    """
    comments: list[str]
    headers: list[str]
    rows: list[tuple]