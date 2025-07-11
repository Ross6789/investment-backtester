from dataclasses import dataclass, field
from backend.enums import RebalanceFrequency,ReinvestmentFrequency, BacktestMode, BaseCurrency
from backend.utils import parse_enum,validate_positive_amount


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
    Configuration for the backtest.

    Attributes:
        mode: BacktestMode, e.g. basic or realistic.
        base_currency: BaseCurrency eg.  GBP, USD
        strategy: Investment strategy parameters.
        initial_investment: Initial capital amount for backtesting (must be positive).
        recurring_investment: Optional recurring investment schedule.

    Raises:
        ValueError: If mode or base currency is an invalid value or initial_investment is not positive.
    """
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
        validate_positive_amount(self.initial_investment, 'initial investment')
