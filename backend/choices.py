from typing import Literal, Any, get_args

BacktestMode = Literal["adjusted", "manual"]
RebalanceFrequency = Literal["never","daily","weekly","monthly","quarterly","yearly"]
ReinvestmentFrequency = Literal["daily", "weekly", "monthly", "quarterly", "yearly"]
OrderSide = Literal["buy","sell"]

def validate_choice(value: str,  choice_literal: Any, choice_field: str = 'value') -> None:
    valid_choices = set(get_args(choice_literal))
    if value not in valid_choices:
        raise ValueError(f"Invalid {choice_field}: '{value}'. Must be one of {valid_choices}.")
