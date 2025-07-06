from typing import Literal

BacktestMode = Literal["adjusted", "manual"]
RebalanceFrequency = Literal["never","daily","weekly","monthly","quarterly","yearly"]
ReinvestmentFrequency = Literal["daily", "weekly", "monthly", "quarterly", "yearly"]
OrderSide = Literal["buy","sell"]
RoundMethod = Literal["nearest","up","down"]


