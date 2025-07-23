from enum import Enum

# Include str inheritance within the method signature to allow enums to behave like strings

class BacktestMode(str, Enum):
    BASIC = "basic"
    REALISTIC = "realistic"

class RebalanceFrequency(str, Enum):
    NEVER = "never"
    DAILY = "daily"
    WEEKLY = "weekly"
    MONTHLY = "monthly"
    QUARTERLY = "quarterly"
    YEARLY = "yearly"

class ReinvestmentFrequency(str, Enum):
    DAILY = "daily"
    WEEKLY = "weekly"
    MONTHLY = "monthly"
    QUARTERLY = "quarterly"
    YEARLY = "yearly"

class OrderSide(str, Enum):
    BUY = "buy"
    SELL = "sell"

class BaseCurrency(str, Enum):
    GBP = "GBP"
    USD = "USD"
    EUR = "EUR"