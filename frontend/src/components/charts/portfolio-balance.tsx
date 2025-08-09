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
  // Create set containing all tickers
  const allTickers = Array.from(
    new Set(chartData.flatMap(({ holdings }) => holdings.map((h) => h.ticker)))
  );

  // Generate chart config dynamically
  const chartConfig = generateChartConfig(allTickers);

  // Set default view
  const [activeChart, setActiveChart] = React.useState<"value" | "weight">(
    "weight"
  );

  // Dynamically filter the axis ticks based on range of data
  const startDate = new Date(chartData[0]?.date);
  const endDate = new Date(chartData[chartData.length - 1]?.date);
  const daysDiff = differenceInDays(endDate, startDate);

  const availableOptions = ["daily"];
  if (daysDiff >= 30) availableOptions.push("weekly");
  if (daysDiff >= 120) availableOptions.push("monthly");
  if (daysDiff >= 365) availableOptions.push("quarterly");
  if (daysDiff >= 1461) availableOptions.push("yearly");

  // Generate flattened data array with chosen field (value vs percentage)
  const transformedData = transformData(chartData, activeChart, allTickers);

  return (
    <Card className="pt-0">
      <CardHeader className="flex items-center gap-2 space-y-0 border-b py-5 sm:flex-row">
        <div className="grid flex-1 gap-1">
          <CardTitle>Portfolio Contributions vs Growth</CardTitle>
          <CardDescription>
            Understand the balance between what you've put in and what your
            investments have earned.
          </CardDescription>
        </div>
        <Select
          value={activeChart}
          onValueChange={(val) => setActiveChart(val as "value" | "weight")}
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
      </CardHeader>
      <CardContent className="px-2 pt-4 sm:px-6 sm:pt-6">
        <ChartContainer
          config={chartConfig}
          className="aspect-auto h-[400px] w-full"
        >
          <AreaChart
            accessibilityLayer
            data={transformedData}
            margin={{
              left: 12,
              right: 12,
              top: 12,
            }}
            stackOffset="expand"
          >
            <CartesianGrid vertical={false} />
            <XAxis
              dataKey="date"
              tickLine={false}
              axisLine={false}
              tickMargin={8}
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
                      <div className="text-muted-foreground flex gap-2 items-center text-xs">
                        <div
                          className="h-2.5 w-2.5 shrink-0 rounded-[2px] bg-(--color-bg)"
                          style={
                            {
                              "--color-bg": `var(--color-${name})`,
                            } as React.CSSProperties
                          }
                        />

                        {chartConfig[name as keyof typeof chartConfig]?.label ||
                          name}

                        <div className="text-foreground ml-auto flex items-baseline gap-0.5 font-mono font-medium tabular-nums">
                          {activeChart === "value"
                            ? formatCurrency(Number(value), currency_code)
                            : formatPercentage(Number(value), 1, false, true)}
                        </div>
                      </div>
                    );
                  }}
                />
              }
              // cursor={false}
            />

            {Object.entries(chartConfig).map(([key, config]) => (
              <Area
                key={key}
                dataKey={key}
                type="natural"
                fill={config.color}
                fillOpacity={0.1}
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
