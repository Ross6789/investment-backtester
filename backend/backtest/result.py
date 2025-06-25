from typing import List, Dict
import polars as pl

class BacktestResult:
    def __init__(self, history: List[Dict[str, object]]):
        self.history = history
    
    def to_csv(self, save_path: str):
        # Extract all tickers
        tickers = set()
        for row in self.history:
            tickers.update(row.get('holdings', {}).keys())
        tickers = sorted(tickers)

        # Flatten data rows
        flat_rows = []
        for row in self.history:
            flat_row = {
                'date': row['date'],
                'cash balance': row['cash balance'],
                'total_value': row['total_value']
            }
            holdings = row.get('holdings', {})
            # Add each ticker column, default to 0.0 if missing
            for ticker in tickers:
                flat_row[ticker] = holdings.get(ticker, 0.0)
            flat_rows.append(flat_row)

        # Create DataFrame
        df = pl.DataFrame(flat_rows)

        # Write CSV
        df.write_csv(save_path)
        print(f'Result exported to {save_path}')