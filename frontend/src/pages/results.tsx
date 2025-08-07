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

  const chartData = [
    { date: "2024-04-01", value: 2220, mobile: 150 },
    { date: "2024-04-02", value: 97000, mobile: 180 },
    { date: "2024-04-03", value: 167000, mobile: 120 },
    { date: "2024-04-04", value: 2420000, mobile: 260 },
    { date: "2024-04-05", value: 373, mobile: 290 },
    { date: "2024-04-06", value: 301, mobile: 340 },
    { date: "2024-04-07", value: 245, mobile: 180 },
    { date: "2024-04-08", value: 409, mobile: 320 },
    { date: "2024-04-09", value: 59, mobile: 110 },
    { date: "2024-04-10", value: 261, mobile: 190 },
    { date: "2024-04-11", value: 327, mobile: 350 },
    { date: "2024-04-12", value: 292, mobile: 210 },
    { date: "2024-04-13", value: 342, mobile: 380 },
    { date: "2024-04-14", value: 137, mobile: 220 },
    { date: "2024-04-15", value: 120, mobile: 170 },
    { date: "2024-04-16", value: 138, mobile: 190 },
    { date: "2024-04-17", value: 446, mobile: 360 },
    { date: "2024-04-18", value: 364, mobile: 410 },
    { date: "2024-04-19", value: 243, mobile: 180 },
    { date: "2024-04-20", value: 89, mobile: 150 },
    { date: "2024-04-21", value: 137, mobile: 200 },
    { date: "2024-04-22", value: 224, mobile: 170 },
    { date: "2024-04-23", value: 138, mobile: 230 },
    { date: "2024-04-24", value: 387, mobile: 290 },
    { date: "2024-04-25", value: 215, mobile: 250 },
    { date: "2024-04-26", value: 75, mobile: 130 },
    { date: "2024-04-27", value: 383, mobile: 420 },
    { date: "2024-04-28", value: 122, mobile: 180 },
    { date: "2024-04-29", value: 315, mobile: 240 },
    { date: "2024-04-30", value: 454, mobile: 380 },
    { date: "2024-05-01", value: 165, mobile: 220 },
    { date: "2024-05-02", value: 293, mobile: 310 },
    { date: "2024-05-03", value: 247, mobile: 190 },
    { date: "2024-05-04", value: 385, mobile: 420 },
    { date: "2024-05-05", value: 481, mobile: 390 },
    { date: "2024-05-06", value: 498, mobile: 520 },
    { date: "2024-05-07", value: 388, mobile: 300 },
    { date: "2024-05-08", value: 149, mobile: 210 },
    { date: "2024-05-09", value: 227, mobile: 180 },
    { date: "2024-05-10", value: 293, mobile: 330 },
    { date: "2024-05-11", value: 335, mobile: 270 },
    { date: "2024-05-12", value: 197, mobile: 240 },
    { date: "2024-05-13", value: 197, mobile: 160 },
    { date: "2024-05-14", value: 448, mobile: 490 },
    { date: "2024-05-15", value: 473, mobile: 380 },
    { date: "2024-05-16", value: 338, mobile: 400 },
    { date: "2024-05-17", value: 499, mobile: 420 },
    { date: "2024-05-18", value: 315, mobile: 350 },
    { date: "2024-05-19", value: 235, mobile: 180 },
    { date: "2024-05-20", value: 177, mobile: 230 },
    { date: "2024-05-21", value: 82, mobile: 140 },
    { date: "2024-05-22", value: 81, mobile: 120 },
    { date: "2024-05-23", value: 252, mobile: 290 },
    { date: "2024-05-24", value: 294, mobile: 220 },
    { date: "2024-05-25", value: 201, mobile: 250 },
    { date: "2024-05-26", value: 213, mobile: 170 },
    { date: "2024-05-27", value: 420, mobile: 460 },
    { date: "2024-05-28", value: 233, mobile: 190 },
    { date: "2024-05-29", value: 78, mobile: 130 },
    { date: "2024-05-30", value: 340, mobile: 280 },
    { date: "2024-05-31", value: 178, mobile: 230 },
    { date: "2024-06-01", value: 178, mobile: 200 },
    { date: "2024-06-02", value: 470, mobile: 410 },
    { date: "2024-06-03", value: 103, mobile: 160 },
    { date: "2024-06-04", value: 439, mobile: 380 },
    { date: "2024-06-05", value: 88, mobile: 140 },
    { date: "2024-06-06", value: 294, mobile: 250 },
    { date: "2024-06-07", value: 323, mobile: 370 },
    { date: "2024-06-08", value: 385, mobile: 320 },
    { date: "2024-06-09", value: 438, mobile: 480 },
    { date: "2024-06-10", value: 155, mobile: 200 },
    { date: "2024-06-11", value: 92, mobile: 150 },
    { date: "2024-06-12", value: 492, mobile: 420 },
    { date: "2024-06-13", value: 81, mobile: 130 },
    { date: "2024-06-14", value: 426, mobile: 380 },
    { date: "2024-06-15", value: 307, mobile: 350 },
    { date: "2024-06-16", value: 371, mobile: 310 },
    { date: "2024-06-17", value: 475, mobile: 520 },
    { date: "2024-06-18", value: 107, mobile: 170 },
    { date: "2024-06-19", value: 341, mobile: 290 },
    { date: "2024-06-20", value: 408, mobile: 450 },
    { date: "2024-06-21", value: 169, mobile: 210 },
    { date: "2024-06-22", value: 317, mobile: 270 },
    { date: "2024-06-23", value: 480, mobile: 530 },
    { date: "2024-06-24", value: 132, mobile: 180 },
    { date: "2024-06-25", value: 141, mobile: 190 },
    { date: "2024-06-26", value: 434, mobile: 380 },
    { date: "2024-06-27", value: 448, mobile: 490 },
    { date: "2024-06-28", value: 149, mobile: 200 },
    { date: "2024-06-29", value: 103, mobile: 160 },
    { date: "2024-06-30", value: 446, mobile: 400 },
  ];

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
            <PortfolioGrowthChart chartData={chartData}></PortfolioGrowthChart>
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
