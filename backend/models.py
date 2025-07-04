from dataclasses import dataclass, field
from typing import Dict, List, Optional
from backend.choices import RebalanceFrequency,ReinvestmentFrequency, BacktestMode
from backend.utils import validate_choice,validate_positive_amount

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
    weights: Dict[str,float] 

    def __post_init__(self):
        if not self.weights:
            raise ValueError("Empty weight dictionary")
        
        for ticker, weight in self.weights.items():
            if weight <= 0 or weight > 1.0:
                raise ValueError(f"Invalid weight for '{ticker}': must be greater than 0 and less than or equal to 1.")
            
        total = sum(self.weights.values())
        if not abs(total-1.0) < 1e-6:
            raise ValueError(f"Portfolio weightings add to {total}. Must equal 1.0")
    
    def get_tickers(self) -> List[str]:
        return list(self.weights.keys())

@dataclass
class RecurringInvestment:

    amount: float              
    frequency: ReinvestmentFrequency 

    def __post_init__(self):
        validate_choice(self.frequency,ReinvestmentFrequency,"reinvestment frequency")
        validate_positive_amount(self.amount,'recurring investment amount')

@dataclass
class Strategy:
    """
    Represents a portfolio investment strategy used during backtesting.

    Encapsulates key configuration options:
    - Whether fractional shares can be purchased.
    - How dividends are treated.
    - How often to rebalance the portfolio.
    """
    allow_fractional_shares: bool = True
    reinvest_dividends: bool = True
    rebalance_frequency: RebalanceFrequency = 'never'

    def __post_init__(self):
        validate_choice(self.rebalance_frequency,RebalanceFrequency, "rebalance frequency")

@dataclass
class BacktestConfig:
    mode : BacktestMode = 'adjusted'
    strategy: Strategy = field(default_factory=Strategy)
    initial_investment : float = 10000
    recurring_investment : Optional[RecurringInvestment] = None

    def __post_init__(self):
        validate_choice(self.mode,BacktestMode, "backtest mode")
        validate_positive_amount(self.initial_investment, 'initial investment')
