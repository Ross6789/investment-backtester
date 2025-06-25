from dataclasses import dataclass


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

