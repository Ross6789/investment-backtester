from dataclasses import dataclass
from typing import Dict

@dataclass
class PortfolioWeights:
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

@dataclass
class RecurringInvestment:
    """
    Represents a recurring investment schedule for adding cash to the portfolio.

    Attributes:
        amount (float): The amount of cash to invest at each scheduled interval. Must be positive.
        frequency (str): The frequency at which the investment occurs. 
                         Options: 'daily', 'weekly', 'monthly', 'quarterly', 'yearly'.
    """
    amount: float              
    frequency: str          

    def __post_init__(self):
        valid_frequencies = {"daily", "weekly", "monthly", "quarterly", "yearly"}
        if self.frequency.lower() not in valid_frequencies:
            raise ValueError(f"Invalid frequency: '{self.frequency}'. Must be one of {valid_frequencies}.")
        if self.amount <= 0:
            raise ValueError("Investment amount must be greater than zero.")

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
    rebalance_frequency: str = "never"
