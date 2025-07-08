
            # Optional : recurring cashflow
            if self.config.recurring_investment:
                self.recurring_cashflow_dates = self._generate_recurring_cashflow_dates(start_date,end_date, self.config.recurring_investment.frequency)