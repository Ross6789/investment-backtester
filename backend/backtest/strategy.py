class Strategy:
    def __init__(
        self,
        allow_fractional_shares: bool = True,
        reinvest_dividends: bool = True,
        rebalance_frequency: str = "never"
    ):
        """
        Initialize a backtest strategy configuration.

        Args:
            allow_fractional_shares (bool): 
                Whether the portfolio allows buying fractional shares.
                
            reinvest_dividends (bool): 
                Whether dividends should be automatically reinvested into the portfolio.
                If False, dividends are added to the cash balance.
                
            rebalance_frequency (str): 
                Frequency of portfolio rebalancing. Options might include:
                - "never": No rebalancing : assets will always be purchased based on chosen allocations 
                - "monthly": Rebalance at the start of each month.
                - "quarterly": Rebalance at the start of each quarter.
                - "yearly": Rebalance at the start of each quarter.

        """
        self.allow_fractional_shares = allow_fractional_shares
        self.reinvest_dividends = reinvest_dividends
        self.rebalance_frequency = rebalance_frequency
