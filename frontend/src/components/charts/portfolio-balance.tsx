import * as React from "react";
import { Area, AreaChart, CartesianGrid, XAxis, YAxis } from "recharts";

import { differenceInDays } from "date-fns";
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";
import {
  ChartContainer,
  ChartTooltip,
  ChartTooltipContent,
} from "@/components/ui/chart";

import type { ChartConfig } from "@/components/ui/chart";

import { formatCurrency, formatPercentage } from "@/lib/utils";

import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";

// Helper Functions

function generateChartConfig(tickers: string[]): ChartConfig {
  const colors = [
    "var(--chart-1)",
    "var(--chart-2)",
    "var(--chart-3)",
    "var(--chart-4)",
    "var(--chart-5)",
  ];

  const config: ChartConfig = {};

  tickers.forEach((ticker, idx) => {
    config[ticker] = {
      label: ticker,
      color: colors[idx % colors.length],
    };
  });
  console.log(config);
  return config;
}

type holdingData = {
  ticker: string;
  value: number;
  weight: number;
};

// Transform data into a flat format based on the active mode eg {"date": "2020-06-19","AAPL": 6733.19, "GOOG": 6019.44, "MSFT": 0}
function transformData(
  chartData: PortfolioBalanceChartProps["chartData"],
  mode: "value" | "weight",
  allTickers: string[]
) {
  return chartData.map(({ date, holdings }) => {
    const obj: Record<string, number | string> = { date };
    holdings.forEach((h) => {
      obj[h.ticker] = mode === "value" ? h.value : h.weight;
    });
    allTickers.forEach((ticker) => {
      if (!(ticker in obj)) obj[ticker] = 0;
    });
    return obj;
  });
}

interface PortfolioBalanceChartProps {
  chartData: {
    date: string;
    holdings: holdingData[];
  }[];
  currency_code: string;
}

export function PortfolioBalanceStackedChart({
  chartData,
  currency_code,
}: PortfolioBalanceChartProps) {
  // Dynamically filter the available periods based on backtest length
  const startDate = new Date(chartData[0]?.date);
  const endDate = new Date(chartData[chartData.length - 1]?.date);
  const daysDiff = differenceInDays(endDate, startDate);

  // only show option if approx 4 data points can be formulated
  const availableOptions = ["daily"];
  if (daysDiff >= 30) availableOptions.push("weekly");
  if (daysDiff >= 120) availableOptions.push("monthly");
  if (daysDiff >= 365) availableOptions.push("quarterly");
  if (daysDiff >= 1461) availableOptions.push("yearly");

  // set default to the smallest aggretion possible without exceeding approx 100 datapoints
  let defaultTimeRange = "daily";
  if (daysDiff >= 100) defaultTimeRange = "weekly";
  if (daysDiff >= 700) defaultTimeRange = "monthly";
  if (daysDiff >= 3000) defaultTimeRange = "quarterly";
  if (daysDiff >= 9100) defaultTimeRange = "yearly";

  // Set default time aggregation
  const [timeRange, setTimeRange] = React.useState(defaultTimeRange);

  const aggregatedData = React.useMemo(() => {
    if (timeRange === "daily") return chartData;

    const result = [];
    const seen = new Set();

    for (const item of chartData) {
      const date = new Date(item.date);
      let key = "";

      if (timeRange === "weekly") {
        // Use ISO week number (year + week)
        const monday = new Date(date);
        monday.setDate(date.getDate() - date.getDay() + 1); // Monday of the week
        key = monday.toISOString().split("T")[0];
      } else if (timeRange === "monthly") {
        key = `${date.getFullYear()}-${String(date.getMonth() + 1).padStart(
          2,
          "0"
        )}`;
      } else if (timeRange === "quarterly") {
        const quarter = Math.floor(date.getMonth() / 3) + 1;
        key = `${date.getFullYear()}-Q${quarter}`;
      } else if (timeRange === "yearly") {
        key = `${date.getFullYear()}`;
      }

      if (!seen.has(key)) {
        result.push(item);
        seen.add(key);
      }
    }

    return result;
  }, [chartData, timeRange]);

  // Set default view
  const [activeView, setActiveView] = React.useState<"value" | "weight">(
    "weight"
  );

  // Create set containing all tickers
  const allTickers = Array.from(
    new Set(chartData.flatMap(({ holdings }) => holdings.map((h) => h.ticker)))
  );

  // Generate flattened data array with chosen field (value vs percentage) and time aggregation performed
  const transformedData = transformData(aggregatedData, activeView, allTickers);

  // Generate chart config dynamically
  const chartConfig = generateChartConfig(allTickers);

  return (
    <Card className="pt-0">
      <CardHeader className="flex items-center gap-2 space-y-0 border-b py-5 sm:flex-row">
        <div className="grid flex-1 gap-1">
          <CardTitle>Portfolio Balance</CardTitle>
          <CardDescription>
            Shows how each asset's share of your portfolio changes over time.
          </CardDescription>
        </div>
        <Select
          value={activeView}
          onValueChange={(val) => setActiveView(val as "value" | "weight")}
        >
          <SelectTrigger
            className="hidden w-[160px] rounded-lg sm:ml-auto sm:flex"
            aria-label="Select a value"
          >
            <SelectValue placeholder="Choose chart" />
          </SelectTrigger>
          <SelectContent className="rounded-xl">
            <SelectItem value="value" className="rounded-lg">
              Value
            </SelectItem>
            <SelectItem value="weight" className="rounded-lg">
              Weight
            </SelectItem>
          </SelectContent>
        </Select>
        <Select value={timeRange} onValueChange={setTimeRange}>
          <SelectTrigger
            className="hidden w-[160px] rounded-lg sm:ml-auto sm:flex"
            aria-label="Select a value"
          >
            <SelectValue placeholder="Choose period" />
          </SelectTrigger>
          <SelectContent className="rounded-xl">
            <SelectItem value="daily" className="rounded-lg">
              Daily
            </SelectItem>
            {availableOptions.includes("weekly") && (
              <SelectItem value="weekly" className="rounded-lg">
                Weekly
              </SelectItem>
            )}
            {availableOptions.includes("monthly") && (
              <SelectItem value="monthly" className="rounded-lg">
                Monthly
              </SelectItem>
            )}
            {availableOptions.includes("quarterly") && (
              <SelectItem value="quarterly" className="rounded-lg">
                Quarterly
              </SelectItem>
            )}
            {availableOptions.includes("yearly") && (
              <SelectItem value="yearly" className="rounded-lg">
                Yearly
              </SelectItem>
            )}
          </SelectContent>
        </Select>
      </CardHeader>
      <CardContent className="px-2 sm:px-6 ">
        <ChartContainer
          config={chartConfig}
          className="aspect-auto h-[250px] w-full"
        >
          <AreaChart
            accessibilityLayer
            data={transformedData}
            margin={{
              left: 12,
              right: 12,
            }}
            stackOffset="expand"
          >
            <CartesianGrid vertical={false} />
            <XAxis
              dataKey="date"
              tickLine={false}
              axisLine={false}
              tickMargin={8}
              minTickGap={32}
              tickFormatter={(value) =>
                new Date(value).toLocaleDateString("en-GB", {
                  day: "numeric",
                  month: "short",
                  year: "2-digit",
                })
              }
            />
            {/* <ChartTooltip
              cursor={false}
              content={<ChartTooltipContent indicator="line" />}
            /> */}

            <ChartTooltip
              cursor={false}
              content={
                <ChartTooltipContent
                  labelFormatter={(value) => {
                    return new Date(value).toLocaleDateString("en-GB", {
                      month: "short",
                      day: "numeric",
                      year: "numeric",
                    });
                  }}
                  formatter={(value, name) => {
                    return (
                      <div className="text-muted-foreground flex gap-3 items-center text-xs">
                        <div
                          className="h-2.5 w-2.5 shrink-0 rounded-[2px]"
                          style={{ backgroundColor: chartConfig[name].color }}
                        />

                        {chartConfig[name as keyof typeof chartConfig]?.label ||
                          name}

                        <div className="text-foreground ml-auto flex items-baseline gap-0.5 font-mono font-medium tabular-nums">
                          {activeView === "value"
                            ? formatCurrency(Number(value), currency_code)
                            : formatPercentage(Number(value), 1, false, true)}
                        </div>
                      </div>
                    );
                  }}
                />
              }
            />

            {Object.entries(chartConfig)
              .reverse()
              .map(([key, config]) => (
                <Area
                  key={key}
                  dataKey={key}
                  type="natural"
                  fill={config.color}
                  fillOpacity={0.3}
                  stroke={config.color}
                  stackId="a"
                />
              ))}
          </AreaChart>
        </ChartContainer>
      </CardContent>
    </Card>
  );
}
