import csv
from typing import List, Dict
import polars as pl
from pathlib import Path
from datetime import datetime

class BacktestResult:
    """
    A container for the historical results of a portfolio backtest.

    Attributes:
        history (List[Dict[str, object]]): A list of snapshots, each representing the portfolio state on a specific date.
    """
    def __init__(self, history: List[Dict[str, object]], pending_orders: pl.DataFrame, executed_orders : pl.DataFrame):
        """
        Initialize the BacktestResult with a list of portfolio snapshots.
        """
        self.history = history
        self.pending_orders = pending_orders
        self.executed_orders = executed_orders

    @staticmethod
    def _create_run_folder(base_path: Path) -> Path:
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        run_folder_path = base_path / timestamp
        run_folder_path.mkdir(parents = True, exist_ok=False)
        return run_folder_path
    
    def to_csv(self, base_path: Path, backtest_configuration: Dict[str, object]):
        """
        Export the backtest history to a CSV file, including configuration metadata as comments.

        The CSV will include a header with column names, and each row will represent the portfolio
        state on a specific date. Configuration metadata will be written as comment lines before
        the actual data table.

        Args:
            save_path (str): File path to save the CSV output.
            backtest_congfiguration (Dict[str, object]): A dictionary of the backtest settings 
                (e.g., start date, end date, initial balance, target weights) to be included as comments 
                at the top of the file.
        """
        # Create timestamped run directory and result paths
        run_folder_path = self._create_run_folder(base_path)
        history_path = run_folder_path / 'history.csv'
        orders_path = run_folder_path / 'orders.csv'

        # Extract all tickers
        tickers = set()
        for row in self.history:
            tickers.update(row.get('holdings', {}).keys())
        tickers = sorted(tickers)

        # Ticker columns
        ticker_cols = []
        for ticker in tickers:
            ticker_cols.extend([f'{ticker} units',f'{ticker} price',f'{ticker} total value',f'{ticker} dividend per unit',f'{ticker} total dividend'])

        # CSV headers
        headers = ['Date','Cash balance','Cash inflow','Dividend income','Total value','Did receive dividends','Did rebalance','Did buy','Did sell'] + ticker_cols

        with open(history_path, mode='w') as f:
            writer = csv.writer(f)

            # Write backtest_configuration as comments at top of csv file
            for key, value in backtest_configuration.items():
                writer.writerow([f'# {key}: {value}'])
            writer.writerow([])  # Empty line between configuration and results

            # Write data rows
            dict_writer = csv.DictWriter(f, fieldnames=headers)
            dict_writer.writeheader()

            for row in self.history:
                flat_row = {
                    'Date': row['date'],
                    'Cash balance': row['cash_balance'],
                    'Cash inflow': row['cash_inflow'],
                    'Dividend income': row['dividend_income'],
                    'Total value': row['total_value'],
                    'Did receive dividends': row['did_receive_dividends'],
                    'Did rebalance': row['did_rebalance'],
                    'Did buy': row['did_buy'],
                    'Did sell': row['did_sell']
                }

                units = row.get('holdings', {})
                prices = row.get('prices',{})
                values = row.get('holding_values',{})
                dividends = row.get('dividends') or []
                
                for ticker in tickers:
                    
                    dividend_per_unit = next((d['dividend_per_unit'] for d in dividends if d['ticker'] == ticker), 0.0)
                    total_dividend = next((d['total_dividend'] for d in dividends if d['ticker'] == ticker), 0.0)
                    
                    flat_row[f'{ticker} units'] = units.get(ticker, 0.0)
                    flat_row[f'{ticker} price'] = prices.get(ticker, 0.0)
                    flat_row[f'{ticker} total value'] = values.get(ticker, 0.0)
                    flat_row[f'{ticker} dividend per unit'] = dividend_per_unit
                    flat_row[f'{ticker} total dividend'] = total_dividend

                dict_writer.writerow(flat_row)
    
        print(f'Exported history to csv : {history_path}')

        # export order books
        orders = pl.concat([self.executed_orders,self.pending_orders])
        orders.write_csv(orders_path)

        print(f'Exported orders to csv : {orders_path}')