import csv
import polars as pl
from pathlib import Path
from datetime import datetime

class BacktestResult:
    """
    A container for the historical results of a portfolio backtest.

    Attributes:
        history (list[dict[str, object]]): A list of snapshots, each representing the portfolio state on a specific date.
    """
    def __init__(self, history: dict[str, pl.DataFrame]):
        """
        Initialize the BacktestResult with a list of portfolio snapshots.
        """
        self.cash_history = history['cash']
        self.holding_history = history['holdings']
        self.dividend_history = history['dividends']
        self.order_history = history['orders']

    @staticmethod
    def _create_run_folder(base_path: Path) -> Path:
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        run_folder_path = base_path / timestamp
        run_folder_path.mkdir(parents = True, exist_ok=False)
        return run_folder_path
    
    
    def compute_daily_summary(self) -> pl.DataFrame:

        # Convert dataframes to lazyframes
        cash_history_lf = self.cash_history.lazy()
        holding_history_lf = self.holding_history.lazy()
        dividend_history_lf = self.dividend_history.lazy()
        order_history_lf = self.order_history.lazy()

        # Join holdings with dividends
        holding_div_lf = (
            holding_history_lf
            .join(dividend_history_lf, on=["date", "ticker"], how="left")
            .with_columns([
                (pl.col("units") * pl.col("price")).alias("value")
            ])
        )

        # Aggregate total holding value and dividend per date
        total_holding_value_lf = (
            holding_div_lf
            .group_by("date")
            .agg([
                pl.sum("value").alias("total_holding_value"),
                pl.sum("total_dividend").alias("all_dividends_received")
            ])
        )

        # Join with cash and compute total portfolio value
        total_portfolio_value_lf = (
            cash_history_lf
            .join(total_holding_value_lf, on="date", how="left")
            .with_columns([
                (pl.col("cash_balance") + pl.col("total_holding_value")).alias("total_portfolio_value")
            ])
        )

        # Compute buy sell flags
        order_flags_lf = (
            order_history_lf
            .filter(pl.col('status') == 'fulfilled')
            .select([
                pl.col('date_executed').alias('date'),
                (pl.col('side') == 'buy').alias('did_buy'),
                (pl.col('side') == 'sell').alias('did_sell'),
            ])
        )

        # Join bool flags to portfolio summary and fill necessary nulls
        portfolio_value_with_flags_lf = (
            total_portfolio_value_lf.join(order_flags_lf, on="date", how="left")
            .with_columns([
                pl.col('did_buy').fill_null(False),
                pl.col('did_sell').fill_null(False)
            ])
        )

        #  Pivot holding data for reporting (need to collect to convert from lazy to eager)
        pivoted_holdings_lf = (
            holding_div_lf
            .collect()
            .pivot(values=["units", "price", "value", "dividend_per_unit", "total_dividend"], 
                index="date", 
                on="ticker")
                .lazy()
        )

        # Final summary table
        summary_lf = portfolio_value_with_flags_lf.join(pivoted_holdings_lf, on="date", how="left")

        # Collect and return
        return summary_lf.sort('date').collect()

    def to_csv(self, base_path: Path, backtest_configuration: dict[str, object]):

        """
        Export the backtest history to a CSV file, including configuration metadata as comments.

        The CSV will include a header with column names, and each row will represent the portfolio
        state on a specific date. Configuration metadata will be written as comment lines before
        the actual data table.

        Args:
            save_path (str): File path to save the CSV output.
            backtest_congfiguration (dict[str, object]): A dictionary of the backtest settings 
                (e.g., start date, end date, initial balance, target weights) to be included as comments 
                at the top of the file.
        """
        # Create timestamped run directory and result paths
        run_folder_path = self._create_run_folder(base_path)
        daily_portfolio_summary_path = run_folder_path / 'daily_portfolio_summary.csv'
        cash_history_path = run_folder_path / 'cash_history.csv'
        holding_history_path = run_folder_path / 'holding_history.csv'
        dividend_history_path = run_folder_path / 'dividend_history.csv'
        orders_path = run_folder_path / 'order_history.csv'

        # export cash history
        self.cash_history.write_csv(cash_history_path)
        print(f'Exported cash history to csv : {cash_history_path}')

        # export holding history
        self.holding_history.write_csv(holding_history_path)
        print(f'Exported holding history to csv : {holding_history_path}')

        # export dividend history
        self.dividend_history.write_csv(dividend_history_path)
        print(f'Exported dividend history to csv : {dividend_history_path}')

        # export order books
        self.order_history.write_csv(orders_path)
        print(f'Exported orders to csv : {orders_path}')

        # #export daily summary
        # self.compute_daily_summary().write_csv(daily_portfolio_summary_path)
        # print(f'Exported summary to csv : {orders_path}')

        # Extract all tickers
        tickers = set()
        for row in self.holding_history.iter_rows(named=True):
            tickers.add(row['ticker'])
        tickers = sorted(tickers)

        # Ticker columns
        ticker_cols = []
        for ticker in tickers:
            ticker_cols.extend([f'{ticker} units',f'{ticker} price',f'{ticker} total value',f'{ticker} dividend per unit',f'{ticker} total dividend'])

        # CSV headers
        headers = ['Date','Dividend income','All dividends received','Cash inflow','Cash balance','Total holding value','Total portfolio value','Did buy','Did sell','Did rebalance'] + ticker_cols

        with open(daily_portfolio_summary_path, mode='w') as f:
            writer = csv.writer(f)

            # Write backtest_configuration as comments at top of csv file
            for key, value in backtest_configuration.items():
                writer.writerow([f'# {key}: {value}'])
            writer.writerow([])  # Empty line between configuration and results

            # Write data rows
            dict_writer = csv.DictWriter(f, fieldnames=headers)
            dict_writer.writeheader()

            for row in self.compute_daily_summary().iter_rows(named=True):
                flat_row = {
                    'Date': row['date'],
                    'Dividend income': row['dividend_income'],
                    'All dividends received': row['all_dividends_received'],
                    'Cash inflow': row['cash_inflow'],
                    'Cash balance': row['cash_balance'],
                    'Total holding value': row['total_holding_value'],
                    'Total portfolio value': row['total_portfolio_value'],
                    'Did buy': row['did_buy'],
                    'Did sell': row['did_sell'],
                    'Did rebalance': row['did_rebalance'],
                }
                
                for ticker in tickers:

                    flat_row[f'{ticker} units'] = row[f'units_{ticker}']
                    flat_row[f'{ticker} price'] = row[f'price_{ticker}']
                    flat_row[f'{ticker} total value'] = row[f'value_{ticker}']
                    flat_row[f'{ticker} dividend per unit'] = row[f'dividend_per_unit_{ticker}']
                    flat_row[f'{ticker} total dividend'] = row[f'total_dividend_{ticker}']

                dict_writer.writerow(flat_row)
    
        print(f'Exported daily summmary to csv : {daily_portfolio_summary_path}')
        

        # # Extract all tickers
        # tickers = set()
        # for row in self.history:
        #     tickers.update(row.get('holdings', {}).keys())
        # tickers = sorted(tickers)

        # # Ticker columns
        # ticker_cols = []
        # for ticker in tickers:
        #     ticker_cols.extend([f'{ticker} units',f'{ticker} price',f'{ticker} total value',f'{ticker} dividend per unit',f'{ticker} total dividend'])

        # # CSV headers
        # headers = ['Date','Cash balance','Cash inflow','Dividend income','Total value','Did receive dividends','Did rebalance','Did buy','Did sell'] + ticker_cols

        # with open(daily_portfolio_summary_path, mode='w') as f:
        #     writer = csv.writer(f)

        #     # Write backtest_configuration as comments at top of csv file
        #     for key, value in backtest_configuration.items():
        #         writer.writerow([f'# {key}: {value}'])
        #     writer.writerow([])  # Empty line between configuration and results

        #     # Write data rows
        #     dict_writer = csv.DictWriter(f, fieldnames=headers)
        #     dict_writer.writeheader()

        #     for row in self.history:
        #         flat_row = {
        #             'Date': row['date'],
        #             'Cash balance': row['cash_balance'],
        #             'Cash inflow': row['cash_inflow'],
        #             'Dividend income': row['dividend_income'],
        #             'Total value': row['total_value'],
        #             'Did receive dividends': row['did_receive_dividends'],
        #             'Did rebalance': row['did_rebalance'],
        #             'Did buy': row['did_buy'],
        #             'Did sell': row['did_sell']
        #         }

        #         units = row.get('holdings', {})
        #         prices = row.get('prices',{})
        #         values = row.get('holding_values',{})
        #         dividends = row.get('dividends') or []
                
        #         for ticker in tickers:
                    
        #             dividend_per_unit = next((d['dividend_per_unit'] for d in dividends if d['ticker'] == ticker), 0.0)
        #             total_dividend = next((d['total_dividend'] for d in dividends if d['ticker'] == ticker), 0.0)
                    
        #             flat_row[f'{ticker} units'] = units.get(ticker, 0.0)
        #             flat_row[f'{ticker} price'] = prices.get(ticker, 0.0)
        #             flat_row[f'{ticker} total value'] = values.get(ticker, 0.0)
        #             flat_row[f'{ticker} dividend per unit'] = dividend_per_unit
        #             flat_row[f'{ticker} total dividend'] = total_dividend

        #         dict_writer.writerow(flat_row)
    
        # print(f'Exported history to csv : {daily_portfolio_summary_path}')
        
