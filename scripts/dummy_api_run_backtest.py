from backend.api.run_backtest import run_backtest

input_data = {
  "mode": "basic",
  "base_currency": "GBP",
  "start_date": "2020-01-01",
  "end_date": "2025-01-01",
  "target_weights":{
    "AAPL":0.5,
    "GOOG":0.5
  },
  "initial_investment":10000,
  "strategy":{
    "fractional_shares":True,
    "reinvest_dividends":True,
    "rebalance_frequency":"never"
  },
  "recurring_investment":{
    "amount":100,
    "frequency":"monthly"
  },
  "export_excel":True
}

result = run_backtest(input_data, dev_mode=True)
