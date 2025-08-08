import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";

import { Badge } from "@/components/ui/badge";

import {
  StrongText,
  SecondaryText,
  MutedText,
} from "@/components/ui/typography";
import {
  formatCurrency,
  formatPercentage,
  formatDate,
  capitalizeFirst,
  getRiskRating,
  getSharpeRating,
} from "@/lib/utils";

import { useLocation, useNavigate } from "react-router-dom";
import { useEffect } from "react";
import { PortfolioGrowthChart } from "@/components/charts/portfolio-growth";

export function ResultsPage() {
  const location = useLocation();
  const navigate = useNavigate();
  const backtestResult = location.state?.backtestResult;
  const riskRating = getRiskRating(backtestResult.results.metrics.volatility);
  const sharpeRating = getSharpeRating(backtestResult.results.metrics.sharpe);

  // Define types
  type TargetWeights = {
    [ticker: string]: number;
  };

  interface BestOrWorstPeriod {
    period: string;
    period_start: string;
    return: number;
  }

  type BestOrWorstPeriods = {
    [key: string]: BestOrWorstPeriod;
  };

  const targetWeights: TargetWeights = backtestResult.settings.target_weights;
  const bestPeriods: BestOrWorstPeriods = backtestResult.results.best_periods;
  const worstPeriods: BestOrWorstPeriods = backtestResult.results.worst_periods;

  useEffect(() => {
    // If no result data (e.g. direct access to /results), redirect to settings
    if (!backtestResult) {
      navigate("/settings");
    }
  }, [backtestResult, navigate]);

  if (!backtestResult) return null;

  return (
    <div className="grid grid-cols-12 gap-8 p-6">
      <div className="col-span-12">
        <Card>
          <CardContent>
            <div className="grid grid-cols-1 sm:grid-cols-3 gap-8">
              <section className="flex flex-col items-center sm:items-start text-center sm:text-left gap-y-1">
                <MutedText>Your Portfolio Ended At</MutedText>
                <StrongText>
                  {formatCurrency(
                    backtestResult.results.metrics.final_value,
                    backtestResult.settings.base_currency
                  )}
                </StrongText>
                <Badge>
                  {formatPercentage(
                    backtestResult.results.metrics.cumulative_return
                  )}{" "}
                  Total Return
                </Badge>
              </section>
              <section className="flex flex-col items-center text-center gap-y-1">
                <MutedText>You Invested</MutedText>
                <StrongText>
                  {formatCurrency(
                    backtestResult.results.metrics.total_contributions,
                    backtestResult.settings.base_currency
                  )}
                </StrongText>
                <MutedText>Initial + Contributions</MutedText>
              </section>
              <section className="flex flex-col items-center sm:items-end text-center sm:text-end gap-y-1">
                <MutedText>Your Profit</MutedText>
                <StrongText>
                  {formatCurrency(
                    backtestResult.results.metrics.cumulative_gain,
                    backtestResult.settings.base_currency
                  )}
                </StrongText>
                <MutedText>
                  {formatPercentage(backtestResult.results.metrics.cagr)} per
                  year
                </MutedText>
              </section>
            </div>
          </CardContent>
        </Card>
      </div>

      <div className="col-span-12 sm:col-span-6 lg:col-span-3">
        <Card>
          <CardHeader>
            <CardTitle>Annual Growth</CardTitle>
          </CardHeader>
          <CardContent>
            <StrongText>
              {formatPercentage(backtestResult.results.metrics.cagr)}
            </StrongText>
            <CardDescription>Average yearly return</CardDescription>
          </CardContent>
        </Card>
      </div>

      <div className="col-span-12 sm:col-span-6 lg:col-span-3">
        <Card>
          <CardHeader>
            <CardTitle>Risk Level</CardTitle>
          </CardHeader>
          <CardContent>
            <StrongText className={riskRating.colorClass}>
              {riskRating.label}
            </StrongText>
            <CardDescription>
              {formatPercentage(backtestResult.results.metrics.volatility)}{" "}
              volatility
            </CardDescription>
          </CardContent>
        </Card>
      </div>

      <div className="col-span-12 sm:col-span-6 lg:col-span-3">
        <Card>
          <CardHeader>
            <CardTitle>Biggest Drop</CardTitle>
          </CardHeader>
          <CardContent>
            <StrongText>
              {backtestResult.results.max_drawdown.max_drawdown.toFixed(1)}%
            </StrongText>
            <CardDescription>Maximum decline from peak</CardDescription>
          </CardContent>
        </Card>
      </div>

      <div className="col-span-12 sm:col-span-6 lg:col-span-3">
        <Card>
          <CardHeader>
            <CardTitle>Risk-Adjusted Score</CardTitle>
          </CardHeader>
          <CardContent>
            <StrongText className={sharpeRating.colorClass}>
              {backtestResult.results.metrics.sharpe.toFixed(2)}
            </StrongText>
            <CardDescription>{sharpeRating.label}</CardDescription>
          </CardContent>
        </Card>
      </div>

      <div className="col-span-12 lg:col-span-6">
        <Card>
          <CardHeader>
            <CardTitle>Best & Worst Periods</CardTitle>
            <CardDescription>
              Your highest and lowest performing periods
            </CardDescription>
          </CardHeader>
          <CardContent>
            <div className="grid grid-cols-2 gap-12 text-sm">
              <section className="flex flex-col gap-y-4">
                <SecondaryText className=" text-green-600">
                  Best Periods
                </SecondaryText>
                {["day", "week", "month", "quarter", "year"].map(
                  (periodType) => {
                    const data = bestPeriods[periodType];
                    if (!data) return null; // skip if missing

                    return (
                      <div
                        key={periodType}
                        className="flex items-center justify-between"
                      >
                        <MutedText>{capitalizeFirst(periodType)}</MutedText>
                        <div className="text-right">
                          <p>{data.period}</p>
                          <p className=" text-green-600">
                            {formatPercentage(data.return, 1)}
                          </p>
                        </div>
                      </div>
                    );
                  }
                )}
              </section>
              <section className="flex flex-col gap-y-4">
                <SecondaryText className=" text-red-600">
                  Worst Periods
                </SecondaryText>

                {["day", "week", "month", "quarter", "year"].map(
                  (periodType) => {
                    const data = worstPeriods[periodType];
                    if (!data) return null; // skip if missing

                    return (
                      <div
                        key={periodType}
                        className="flex items-center justify-between"
                      >
                        <MutedText>{capitalizeFirst(periodType)}</MutedText>
                        <div className="text-right">
                          <p>{data.period}</p>
                          <p className=" text-red-600">
                            {formatPercentage(data.return, 1)}
                          </p>
                        </div>
                      </div>
                    );
                  }
                )}
              </section>
            </div>
          </CardContent>
        </Card>
      </div>

      <div className="col-span-12">
        <PortfolioGrowthChart
          chartData={backtestResult.results.chart_data.portfolio_growth}
          currency_code={backtestResult.settings.base_currency}
        ></PortfolioGrowthChart>
      </div>

      <div className="col-span-12">
        <Card>
          <CardHeader>
            <CardTitle>Investment Settings</CardTitle>
            <CardDescription>
              The configuration used for this backtest
            </CardDescription>
          </CardHeader>
          <CardContent>
            <div className="grid grid-cols-1 md:grid-cols-3 gap-12 text-sm">
              <section className="flex flex-col gap-y-2">
                <SecondaryText>Investment Details</SecondaryText>

                {[
                  {
                    label: "Start Date",
                    value: formatDate(backtestResult.settings.start_date),
                  },
                  {
                    label: "End Date",
                    value: formatDate(backtestResult.settings.end_date),
                  },
                  {
                    label: "Currency",
                    value: backtestResult.settings.base_currency,
                  },
                  {
                    label: "Starting Amount",
                    value: formatCurrency(
                      backtestResult.settings.initial_investment,
                      backtestResult.settings.base_currency
                    ),
                  },
                  {
                    label: "Recurring Amount",
                    value: backtestResult.settings.recurring_investment
                      ? formatCurrency(
                          backtestResult.settings.recurring_investment.amount,
                          backtestResult.settings.base_currency
                        )
                      : "N/A",
                  },
                  // Recurring Frequency is only pushed if recurring investment exists ...[] does not insert element
                  ...(backtestResult.settings.recurring_investment
                    ? [
                        {
                          label: "Recurring Frequency",
                          value: capitalizeFirst(
                            backtestResult.settings.recurring_investment
                              .frequency
                          ),
                        },
                      ]
                    : []),
                ].map((item, idx) => (
                  <div key={idx} className="flex items-center justify-between">
                    <MutedText>{item.label}</MutedText>
                    <p>{item.value}</p>
                  </div>
                ))}
              </section>
              <section className="flex flex-col gap-y-2">
                <SecondaryText>Strategy settings</SecondaryText>

                {[
                  {
                    label: "Simulation Mode",
                    value: capitalizeFirst(backtestResult.settings.mode),
                  },
                  {
                    label: "Rebalancing",
                    value: capitalizeFirst(
                      backtestResult.settings.strategy.rebalance_frequency
                    ),
                  },
                  {
                    label: "Dividends",
                    value: capitalizeFirst(
                      backtestResult.settings.strategy.reinvest_dividends
                        ? "reinvest"
                        : "income"
                    ),
                  },
                  {
                    label: "Fractional Shares",
                    value: capitalizeFirst(
                      backtestResult.settings.strategy.allow_fractional_shares
                        ? "allowed"
                        : "disallowed"
                    ),
                  },
                ].map((item, idx) => (
                  <div key={idx} className="flex items-center justify-between">
                    <MutedText>{item.label}</MutedText>
                    <p>{item.value}</p>
                  </div>
                ))}
              </section>
              <section className="flex flex-col gap-y-1">
                <SecondaryText>Asset Allocation</SecondaryText>
                {Object.entries(targetWeights).map(([ticker, weight]) => (
                  <div
                    key={ticker}
                    className="flex items-center justify-between"
                  >
                    <MutedText>{ticker}</MutedText>
                    <p>{formatPercentage(weight, 1, false)}</p>
                  </div>
                ))}
              </section>
            </div>
          </CardContent>
        </Card>
      </div>
    </div>
  );
}
