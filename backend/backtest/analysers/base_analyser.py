from abc import ABC
import polars as pl
import pandas as pd
from quantstats import stats
from backend.core.models import BacktestResult
from backend.utils.reporting import generate_suffixed_col_names, build_drop_col_list


class BaseAnalyser(ABC):
    """
    Basic analyser class for processing backtest results.

    Initializes lazy dataframes for key backtest components and caches unique tickers for efficient access in analysis methods.

    Attributes:
        data_lf (pl.LazyFrame): LazyFrame of backtest price data.
        calendar_lf (pl.LazyFrame): LazyFrame of the calendar data.
        cash_lf (pl.LazyFrame): LazyFrame of cash balances over time.
        holdings_lf (pl.LazyFrame): LazyFrame of holdings over time.
        tickers (list[str]): List of unique ticker symbols from holdings.
    """

    def __init__(self, backtest_results : BacktestResult):
        """
        Initialize the BaseAnalyser with backtest result data.

        Args:
            backtest_results (BacktestResult): Object containing backtest dataframes.
        """
        # Initialize lazy dataframes
        self.data_lf = backtest_results.data.lazy()
        self.calendar_lf = backtest_results.calendar.lazy()
        self.cash_lf = backtest_results.cash.lazy()
        self.holdings_lf = backtest_results.holdings.lazy()

        # Cache tickers for use in future methods
        self.tickers = self._get_all_tickers()

        # Perform essential enrichments
        self._compile_enriched_data()


    def _get_all_tickers(self) -> list[str]:       
        """
        Retrieve a list of unique ticker symbols from the holdings data.

        Returns:
            list[str]: Unique tickers present in holdings_lf.
        """
        return (
            self.holdings_lf
            .select('ticker')
            .unique()
            .collect()
            .to_series()
            .to_list()
        )
   

    def _compile_enriched_data(self) -> None:
        """
        Compile enriched holdings and portfolio data.

        Enhances holdings with value calculations, computes portfolio totals, and enriches holdings with portfolio weighting, storing results as instance attributes.
        """
        self.enriched_cash_lf= self.cash_lf.with_columns(BaseAnalyser.get_cumulative_cashflow_expr())

        holdings_with_values = self.holdings_lf.with_columns(BaseAnalyser.get_values_expr())
        portfolio_lf = self._compute_portfolio_totals(holdings_with_values,self.enriched_cash_lf)

        self.enriched_holdings_lf = self._enrich_holdings_with_portfolio_weighting(holdings_with_values,portfolio_lf)
        self.enriched_portfolio_lf = portfolio_lf.with_columns(*BaseAnalyser.get_gain_exprs(), *BaseAnalyser.get_return_exprs())
 

    # --- Simple single column enrichments expression --- # 

    # === GENERIC === #

    @staticmethod
    def get_year_expr(date_col: str) -> pl.Expr:
        """
        Generate an expression to extract the year from a date column.

        This expression can be used with `.with_columns()` to add a 'year' column
        derived from the specified date column in a LazyFrame or DataFrame.

        Args:
            date_col (str): Name of the date column from which to extract the year.

        Returns:
            pl.Expr: Polars expression that extracts the year and labels it as 'year'.
        """
        return pl.col(date_col).dt.year().alias('year')
    

    # === CASH === #

    @staticmethod
    def get_cumulative_cashflow_expr() -> pl.Expr:
        """
        Add a 'cumulative_cashflow' column to the LazyFrame by computing the cumulative sum of 'cash_inflow' over time.

        Returns:
            pl.Expr: Polars expression that calculated the 'cumulative_cashflow' column

        """
        return pl.col("cash_inflow").cum_sum().alias("cumulative_cashflow")
    

    @staticmethod
    def get_gain_exprs() -> list[pl.Expr]:
        """
        Generate expressions to calculate net gains, accounting for cash inflows.

        Returns:
            list[pl.Expr]: 
                - 'net_daily_gain': The change in total portfolio value from the previous day, minus any cash inflow on the current day.
                - 'net_cumulative_gain': The total portfolio value minus the cumulative cash invested up to the current day.
        """
        return [
            ((pl.col("total_portfolio_value").diff())-(pl.col("cash_inflow"))).alias("net_daily_gain"),
            (pl.col("total_portfolio_value") - pl.col("cumulative_cashflow")).alias("net_cumulative_gain")
        ]
    

    @staticmethod
    def get_return_exprs() -> list[pl.Expr]:
        """
        Generate expressions to calculate net returns, accounting for cash inflows.

        Returns:
            list[pl.Expr]: 
                - 'net_daily_return': The net daily return as the portfolio value (adjusted for cash inflow) divided by the previous day's value, minus 1.
                - 'net_cumulative_return': The cumulative return calculated as the portfolio value divided by cumulative cash inflows, minus 1.
        """
        return [
            (((pl.col("total_portfolio_value")-pl.col("cash_inflow"))/ pl.col("total_portfolio_value").shift(1)) - 1).alias("net_daily_return"),
            ((pl.col("total_portfolio_value") / pl.col("cumulative_cashflow")) - 1).alias("net_cumulative_return"),
        ]
    

    # === HOLDINGS === #

    @staticmethod
    def get_values_expr() -> pl.Expr:  
        """
        Calculate the total value of each holding by multiplying units by base price.

        Returns:
            pl.Expr: Polars expression that calculates the 'value' column from 'units' x 'base_price'
            
        """     
        return (pl.col("units") * pl.col("base_price")).alias("value")


    # --- Multi-stage Enrichment methods --- # 
    
    @staticmethod
    def _enrich_holdings_with_portfolio_weighting(holdings_lf: pl.LazyFrame, portfolio_lf: pl.LazyFrame) -> pl.LazyFrame:  
        """
        Add portfolio weighting (%) to each holding based on its value proportion of total holdings.

        Args:
            holdings_lf (pl.LazyFrame): Holdings data with 'value' per asset per date.
            portfolio_lf (pl.LazyFrame): Portfolio totals data with 'total_holding_value' per date.

        Returns:
            pl.LazyFrame: Holdings enriched with 'portfolio_weighting' column representing the proportion of each holding relative to total holdings on that date.
        """
        drop_cols = build_drop_col_list(['date'], portfolio_lf.collect_schema().keys())

        holdings_with_weighting = (
            holdings_lf
            .join(portfolio_lf, on='date')
            .with_columns((pl.col('value') / pl.col('total_holding_value')).alias('portfolio_weighting'))
            .drop(drop_cols)
        )
        return holdings_with_weighting
    

    @staticmethod
    def _compute_portfolio_totals(holdings_lf: pl.LazyFrame, cash_lf: pl.LazyFrame) -> pl.LazyFrame:
        """
        Calculate daily portfolio totals by combining holdings and cash balances.

        Args:
            holdings_lf (pl.LazyFrame): Holdings data with 'value' column per asset per date.
            cash_lf (pl.LazyFrame): Cash balance data per date.

        Returns:
            pl.LazyFrame: DataFrame with columsn from cash_lf and 'total_holding_value' and 'total_portfolio_value'
        """
        # Add total holdings value column
        total_holdings_value = (
            holdings_lf
            .group_by('date')
            .agg(pl.sum('value').alias('total_holding_value'))
        )

        # Add total portfolio value column
        total_portfolio_value = (
            cash_lf.join(total_holdings_value, on='date',how='left')
            .fill_null(0) # Fill any empty holding totals
            .with_columns(
                (pl.col('cash_balance')+pl.col('total_holding_value')).alias('total_portfolio_value')
            )
        )
        return total_portfolio_value
        

    # --- Pivoting--- #
    @staticmethod
    def _format_wide_holdings_summary(enriched_holdings_lf : pl.LazyFrame, tickers : list[str]) -> pl.LazyFrame:
        """
        Pivot enriched holdings data to a wide format with separate columns for each ticker's 'value' and 'portfolio_weighting'.

        Collects and pivots the data so that each ticker has its own set of 'value' and 'portfolio_weighting' columns and ensures columns are ordered consistently.

        Args:
            enriched_holdings_lf (pl.LazyFrame): LazyFrame containing at least 'date', 'ticker', 'value', and 'portfolio_weighting' columns.
            tickers (list[str]): List of expected tickers, used to order the resulting pivoted columns.

        Returns:
            pl.LazyFrame: Pivoted LazyFrame in wide format with one row per date and ordered columns per ticker and metric.

        """
        PIVOT_VALUES = ["value","portfolio_weighting"] 

        wide_holdings_total_value = (
            enriched_holdings_lf
            .select(["date","ticker", *PIVOT_VALUES])
            .collect()
            .pivot(values=PIVOT_VALUES, 
                index="date", 
                on="ticker")
                .lazy()
        )

        # Order columns
        pivot_cols = generate_suffixed_col_names(PIVOT_VALUES, tickers)
        wide_holdings_total_value_ordered = wide_holdings_total_value.select(['date'] + pivot_cols)

        return wide_holdings_total_value_ordered


    # --- Final report generation --- #
    
    def generate_daily_summary(self) -> pl.DataFrame:
        """
        Generate a daily summary by joining portfolio and holdings data.

       Formats holdings in wide format, then joins enriched portfolio data on date.

        Returns:
            pl.DataFrame: Combined daily summary with portfolio and holdings info.
        """
        wide_holdings_summary = self._format_wide_holdings_summary(self.enriched_holdings_lf,self.tickers)

        daily_summary = (
            self.enriched_portfolio_lf
            .join(wide_holdings_summary, on='date',how='left')
            .fill_null(0)
        )

        return daily_summary.collect()


    def generate_holdings_summary(self) -> pl.DataFrame:
        """
        Generate a pivoted summary of holdings with FX and portfolio data.

        Joins enriched holdings with FX and portfolio data, then pivots specified value columns by date and ticker to create a wide-format summary. 

        Returns:
            pl.DataFrame: Pivoted holdings summary with one column per (ticker, value type) combination, plus the date column.
        """
        PIVOT_VALUES = ['units','native_currency','native_price','exchange_rate','value','portfolio_weighting']

        holdings_fx = (
            self.enriched_holdings_lf
            .join(self.data_lf, on=['date','ticker','base_price'])
            .join(self.enriched_portfolio_lf, on='date')
            .select(['date','ticker', *PIVOT_VALUES])
        )
        
        holdings_summary = (
            holdings_fx.collect()
            .pivot(values=PIVOT_VALUES, 
                index="date", 
                on="ticker")
        )

        # Order columns
        pivot_cols = generate_suffixed_col_names(PIVOT_VALUES, self.tickers)
        holdings_summary_ordered = holdings_summary.select(['date', *pivot_cols])
        
        return holdings_summary_ordered


    # --- Calculating overall metrics --- # 

    def calculate_overall_metrics(self) -> dict:
        
        # Remove non trading days from portfolio valuations - this need 
        trading_days = self.calendar_lf.filter(pl.col('trading_tickers').list.len() >0)
        trading_portfolio = self.enriched_portfolio_lf.join(trading_days, on='date', how='semi')
        
        # Collect returns as pandas series to use with quantstats
        returns_df = trading_portfolio.select(['date','net_daily_return']).collect()
        returns = pd.Series(returns_df['net_daily_return'],index=pd.DatetimeIndex(returns_df['date']))
  
        # Collect key metrics from enriched portfolio lf
        key_metrics_df = (
            self.enriched_portfolio_lf.select([
                pl.col("cash_inflow").sum().alias("total_contributions"),
                pl.col("total_portfolio_value").tail(1).alias("final_value"),
                pl.col("net_cumulative_gain").tail(1).alias("cumulative_gain"),
                pl.col("net_cumulative_return").tail(1).alias("cumulative_return"),
            ])
            .collect()
        )

        # Unpack metrics from dict
        total_contributions, final_value, cumulative_gain, cumulative_return = key_metrics_df.row(0)

        # CAGR
        calc_cagr = stats.cagr(returns)

        # # Sharpe
        calc_sharpe = stats.sharpe(returns)

        # # Sharpe
        calc_volatility = stats.volatility(returns)

        # Aggregated returns
        agg_returns_pd = stats.monthly_returns(returns)

        # Monthly returns - quantstats 
        monthly_returns_pd = agg_returns_pd.drop(columns=['EOY'])
        calc_monthly_returns_dict = monthly_returns_pd.T.reset_index().rename(columns={'index': 'month'}).to_dict(orient='records')

        # Annual returns - quantstats
        yearly_returns_pd = agg_returns_pd['EOY']
        calc_yearly_returns_dict = yearly_returns_pd.to_dict()

        # Drawdown
        drawdown = stats.to_drawdown_series(returns)
        all_drawdown_details = stats.drawdown_details(drawdown)
        formatted_drawdown_details = all_drawdown_details[['start','end','days','max drawdown','valley']].rename(columns={'max drawdown':'max_drawdown','valley':'max_drawdown_date'}) 
        calc_drawdown_dict = formatted_drawdown_details.to_dict(orient='records')
       
        # calc_max_drawdown = stats.max_drawdown(returns) #quantstat max drawdown used to check accuracy of dict retrieval : results match (although one is float and other is percentage) 
        calc_max_drawdown_dict = min(calc_drawdown_dict, key=lambda d: d['max_drawdown'], default={})

        # Period returns
        period_returns = self._aggregate_returns_by_periods(returns_df)

        # Likely move to Exporter (JSON) adn add handler method to adhere to DRY
        # daily_df = period_returns.get('daily')
        # daily_dates = daily_df['day'].to_list()
        # daily_returns = daily_df['return'].to_list()
        # formatted_daily_rows = [{"period": date.strftime('%Y-%m-%d'), "return": ret, "period_start": date.strftime('%Y-%m-%d')} for date, ret in zip(daily_dates, daily_returns)]

        # weekly_df = period_returns.get('weekly')
        # weekly_start_dates = weekly_df['week'].to_list()
        # weekly_returns = weekly_df['return'].to_list()
        # formatted_weekly_rows = [{"period": date.strftime('%Y-%m-%d'), "return": ret, "period_start": date.strftime('%Y-%m-%d')} for date, ret in zip(weekly_start_dates, weekly_returns)]
        
        monthly_df = period_returns.get('monthly')
        monthly_start_dates = monthly_df['month'].to_list()
        monthly_returns = monthly_df['return'].to_list()
        formatted_monthly_rows = [{"period": date.strftime('%Y_%m'), "return": ret, "period_start": date.strftime('%Y-%m-%d')} for date, ret in zip(monthly_start_dates, monthly_returns)]
        
        quarterly_df = period_returns.get('quarterly')
        quarterly_start_dates = quarterly_df['quarter'].to_list()
        quarterly_returns = quarterly_df['return'].to_list()
        formatted_quarterly_rows = [{"period": f"{date.year}-Q{((date.month - 1) // 3) + 1}", "return": ret, "period_start": date.strftime('%Y-%m-%d')} for date, ret in zip(quarterly_start_dates, quarterly_returns)]
        
        yearly_df = period_returns.get('yearly')
        yearly_start_dates = yearly_df['year'].to_list()
        yearly_returns = yearly_df['return'].to_list()
        formatted_yearly_rows = [{"period": date.strftime('%Y'), "return": ret, "period_start": date.strftime('%Y-%m-%d')} for date, ret in zip(yearly_start_dates, yearly_returns)]
        
        agg_returns = {
                # "daily": formatted_daily_rows,
                # "weekly": formatted_weekly_rows,
                # "monthly": formatted_monthly_rows,
                # "quarterly": formatted_quarterly_rows,
                "yearly": formatted_yearly_rows
            }

        # Best periods
        best_periods = []

        for period, df in period_returns.items():
            best_row = df.filter(pl.col('return') == pl.col('return').max()).row(0)
            best_periods.append({
                'aggregation':period,
                'period_start':best_row[0],
                'return':best_row[1]
            })

        # Worst periods
        worst_periods = []

        for period, df in period_returns.items():
            worst_row = df.filter(pl.col('return') == pl.col('return').min()).row(0)
            worst_periods.append({
                'aggregation':period,
                'period_start':worst_row[0],
                'return':worst_row[1]
            })


        # Monthly return analysis
        monthly_return_summary = BaseAnalyser._calculate_monthly_win_rate(period_returns.get("monthly"))
        monthly_return_buckets = BaseAnalyser._categorize_monthly_return_buckets(period_returns.get("monthly"))

        return {
            "metrics":{
                "total_contributions": total_contributions,
                "final_value": final_value,
                "cumulative_gain": cumulative_gain,
                "cumulative_return":cumulative_return,
                "cagr": calc_cagr,
                "sharpe": calc_sharpe,
                "volatility": calc_volatility,
            },
            "max_drawdown": calc_max_drawdown_dict,
            "monthly_return_analysis": {
                "summary": monthly_return_summary,
                "buckets": monthly_return_buckets
            },
            "best_periods":best_periods,
            "worst_periods": worst_periods
            # "period_returns": period_returns,
            # "agg_returns": agg_returns,
            # "yearly_returns": calc_yearly_returns_dict,
            # "yearly_returns_polars": period_returns.get('yearly').to_dicts(),
            # "monthly_returns": calc_monthly_returns_dict,
            # "monthly_returns_polars": period_returns.get('monthly').to_dicts(),
            # "drawdown": calc_drawdown_dict,

        }


    def _aggregate_returns_by_periods(self, net_daily_returns_df : pl.DataFrame) -> dict[str, pl.DataFrame]:
        
        # Add period columns to returns df
        returns_with_period_cols = net_daily_returns_df.with_columns(
            pl.col('date').dt.truncate('1w').alias('week'),
            pl.col('date').dt.truncate('1mo').alias('month'),
            pl.col('date').dt.truncate('1q').alias('quarter'),
            pl.col('date').dt.truncate('1y').alias('year')
        )
        
        daily_returns = net_daily_returns_df.rename({'date':'day','net_daily_return':"return"})
        weekly_returns = BaseAnalyser._aggregate_return_for_period(returns_with_period_cols,'week')
        monthly_returns = BaseAnalyser._aggregate_return_for_period(returns_with_period_cols,'month')
        quarterly_returns = BaseAnalyser._aggregate_return_for_period(returns_with_period_cols,'quarter')
        yearly_returns = BaseAnalyser._aggregate_return_for_period(returns_with_period_cols,'year')
        
        return {
        "daily": daily_returns,
        "weekly": weekly_returns,
        "monthly": monthly_returns,
        "quarterly": quarterly_returns,
        "yearly": yearly_returns        
        }
    
    @staticmethod
    def _aggregate_return_for_period(returns_with_periods: pl.DataFrame, period: str):
        return (
            returns_with_periods
            .group_by(period)
            .agg(
                ((1 + pl.col('net_daily_return')).product() - 1).alias('return')
            )
            .sort(period)
        )
    
    # @staticmethod
    # def _bucket_monthly_returns(monthly_returns: pl.DataFrame) -> dict:
        
    #     bucket_order = ["< -10%", "-10% to -5%", "-5% to 0%", "0% to 5%", "5% to 10%", "10%+"]

    #     bucketed_returns = monthly_returns.with_columns(
    #         pl.when(pl.col("return") < -0.10).then(pl.lit("< -10%"))
    #         .when(pl.col("return") < -0.05).then(pl.lit("-10% to -5%"))
    #         .when(pl.col("return") < 0).then(pl.lit("-5% to 0%"))
    #         .when(pl.col("return") < 0.05).then(pl.lit("0% to 5%"))
    #         .when(pl.col("return") < 0.10).then(pl.lit("5% to 10%"))
    #         .otherwise(pl.lit("10%+"))
    #         .alias("performance_bucket")
    #     )

    #     counts = (
    #         bucketed_returns
    #         .group_by("performance_bucket")
    #         .agg(pl.len().alias("count"))
    #     )
        
    #     unordered_counts = dict(zip(counts["performance_bucket"], counts["count"]))
        
    #     ordered_counts = {}
    #     for bucket in bucket_order:
    #         ordered_counts[bucket]=unordered_counts[bucket]

    #     return ordered_counts
    
    # @staticmethod
    # def _bucket_monthly_returns_summary(monthly_returns: pl.DataFrame) -> dict:
        
    #     bucketed_returns = monthly_returns.with_columns(
    #         pl.when(pl.col("return") < 0).then(pl.lit("loss"))
    #         .otherwise(pl.lit("win"))
    #         .alias("performance")
    #     )

    #     counts = (
    #         bucketed_returns
    #         .group_by("performance")
    #         .agg(pl.len().alias("count"))
    #     )
        
    #     counts = dict(zip(counts["performance_bucket"], counts["count"]))

    #     win = counts["win"]
    #     loss = counts["loss"]
    #     total = win + loss
    #     rate = win / total 

    #     return {
    #         "win":win,
    #         "loss":loss,
    #         "rate": rate
    #     }

    @staticmethod
    def _categorize_monthly_return_buckets(monthly_returns: pl.DataFrame) -> dict:
        """
        Categorize monthly returns into fixed percentage buckets.

        Args:
            monthly_returns (pl.DataFrame): DataFrame with a "return" column of monthly returns as decimals.

        Returns:
            dict: Mapping of performance buckets to counts. Missing buckets default to 0.
        """
        bucket_order = ["< -10%", "-10% to -5%", "-5% to 0%", "0% to 5%", "5% to 10%", "10%+"]

        # Tag each return with a bucket label
        bucketed = monthly_returns.with_columns(
            pl.when(pl.col("return") < -0.10).then(pl.lit("< -10%"))
            .when(pl.col("return") < -0.05).then(pl.lit("-10% to -5%"))
            .when(pl.col("return") < 0).then(pl.lit("-5% to 0%"))
            .when(pl.col("return") < 0.05).then(pl.lit("0% to 5%"))
            .when(pl.col("return") < 0.10).then(pl.lit("5% to 10%"))
            .otherwise(pl.lit("10%+"))
            .alias("performance_bucket")
        )

        # Get counts per bucket
        counts = (
            bucketed
            .group_by("performance_bucket")
            .agg(pl.len().alias("count"))
        )

        counts_dict = {row["performance_bucket"]: row["count"] for row in counts.iter_rows(named=True)}

        # Ensure all buckets are included (even if zero) and in correct order
        return {bucket: counts_dict.get(bucket, 0) for bucket in bucket_order}


    @staticmethod
    def _calculate_monthly_win_rate(monthly_returns: pl.DataFrame) -> dict:
        """
        Calculate win/loss counts and win rate from monthly returns.

        Args:
            monthly_returns (pl.DataFrame): DataFrame with a "return" column.

        Returns:
            dict: Contains "win", "loss", and "rate" (win percentage as a float).
        """
        # Label each return as 'win' or 'loss'
        labeled = monthly_returns.with_columns(
            pl.when(pl.col("return") < 0).then(pl.lit("loss"))
            .otherwise(pl.lit("win"))
            .alias("performance")
        )
        
        # Get counts win / loss
        counts_dict = (
            labeled
            .group_by("performance")
            .agg(pl.len().alias("count"))
        )

        
        counts_dict = {row["performance"]: row["count"] for row in counts_dict.iter_rows(named=True)}
        
        win = counts_dict.get("win", 0)
        loss = counts_dict.get("loss", 0)
        total = win + loss
        
        return {
            "win": win,
            "loss": loss,
            "rate": win / total if total > 0 else 0
        }
