


    # --- Data Generation & Loading ---

            # Instantiate last rebalance day and order book
            self.last_rebalance_date = self.master_calendar.select(pl.col('date').min()).item()
            self.pending_orders = None
            self.executed_orders = None

            # Optional : dividends (if in manual mode)
            if self.config.mode == BacktestMode.REALISTIC:
                self.dividend_dates = self._load_dividend_dates()

            # Optional : recurring cashflow
            if self.config.recurring_investment:
                self.recurring_cashflow_dates = self._generate_recurring_cashflow_dates(start_date,end_date, self.config.recurring_investment.frequency)



    def _load_dividend_dates(self) -> set[date]:
        """
        Extract all unique dates from the backtest data where dividends were issued.

        Returns:
            set[date]: A set of dates on which at least one ticker paid a dividend.
        """
        dividend_dates = (
            self.backtest_data
            .filter(pl.col('dividend').is_not_null())
            .get_column('date')
            .to_list()
        )
        return set(dividend_dates)

