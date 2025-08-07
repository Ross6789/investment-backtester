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

      {/* <div className="col-span-12 sm:col-span-6 lg:col-span-3">
        <Card>
          <CardHeader>
            <CardTitle>Success Rate</CardTitle>
          </CardHeader>
          <CardContent>
            <StrongText>
              {formatPercentage(
                backtestResult.results.monthly_return_analysis.summary.rate,
                0,
                false
              )}
            </StrongText>
            <CardDescription>Months with positive returns</CardDescription>
          </CardContent>
        </Card>
      </div> */}

      <div className="col-span-12">
        <Card>
          <CardHeader>
            <CardTitle>Card 6</CardTitle>
            <CardDescription>Description</CardDescription>
          </CardHeader>
          <CardContent>
            <PortfolioGrowthChart></PortfolioGrowthChart>
          </CardContent>
        </Card>
      </div>

      <div className="col-span-12">
        <Card>
          <CardHeader>
            <CardTitle>Card 7</CardTitle>
            <CardDescription>Description</CardDescription>
          </CardHeader>
          <CardContent>content</CardContent>
        </Card>
      </div>
    </div>

    // <div className="m-4 p-4 flex lg:flex-row flex-col gap-4">
    //   {/* LEFT SIDE - add items-star to make box shrink */}
    //   <div className="flex-1">
    // <Card>
    //   <CardHeader>
    //     <CardTitle>Result</CardTitle>
    //     <CardDescription>JSON results</CardDescription>
    //   </CardHeader>
    //   <CardContent>
    //     <pre>{JSON.stringify(backtestResult, null, 2)}</pre>
    //   </CardContent>
    // </Card>
    //   </div>

    //   {/* RIGHT SIDE */}
    //   <div className="flex flex-col flex-1 gap-4">
    //     <Card>
    //       <CardHeader>
    //         <CardTitle>Backtest Mode</CardTitle>
    //         <CardDescription>
    //           Choose your simulation complexity level
    //         </CardDescription>
    //       </CardHeader>
    //       <CardContent></CardContent>
    //     </Card>
    //     <Card>
    //       <CardHeader>
    //         <CardTitle>Investment Settings</CardTitle>
    //       </CardHeader>
    //       <CardContent></CardContent>
    //     </Card>
    //     <Card>
    //       <CardHeader>
    //         <CardTitle>Strategy Settings</CardTitle>
    //       </CardHeader>
    //       <CardContent className="space-y-4"></CardContent>
    //     </Card>
    //   </div>
    // </div>
  );
}
