import * as React from "react";
import { Area, AreaChart, CartesianGrid, XAxis, YAxis } from "recharts";

import { InfoTooltip } from "../info_tooltip";
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
import { tooltipTexts } from "@/constants/ui_text";

import { formatCurrency } from "@/lib/utils";

import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";

const chartConfig = {
  portfolio: {
    label: "Portfolio",
    color: "var(--chart-1)",
  },
  benchmark: {
    label: "Benchmark",
    color: "var(--chart-4)",
  },
} satisfies ChartConfig;

interface BenchmarkDataPoint {
  date: string;
  [ticker: string]: number | string;
}

interface PortfolioGrowthChartProps {
  portfolioChartData: {
    date: string;
    contributions: number;
    gain: number;
    value: number;
  }[];
  benchmarkChartData: BenchmarkDataPoint[];
  benchmarkLabels: { [ticker: string]: string };
  currency_code: string;
}

export function PortfolioGrowthChart({
  portfolioChartData,
  benchmarkChartData,
  benchmarkLabels,
  currency_code,
}: PortfolioGrowthChartProps) {
  // Dynamically filter the available periods based on backtest length
  const startDate = new Date(portfolioChartData[0]?.date);
  const endDate = new Date(
    portfolioChartData[portfolioChartData.length - 1]?.date
  );
  const daysDiff = differenceInDays(endDate, startDate);

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

  // Create state variables to manage selected time period and benchmark
  const [timeRange, setTimeRange] = React.useState(defaultTimeRange);
  const [selectedBenchmark, setSelectedBenchmark] = React.useState("none"); // default to no benchmark

  // Filter portfolio data to selected time period
  const filteredPortfolioData = React.useMemo(() => {
    const visibleData = [];

    for (const item of portfolioChartData) {
      const date = new Date(item.date);

      if (timeRange === "weekly" && date.getDay() !== 1) continue;
      if (timeRange === "monthly" && date.getDate() !== 1) continue;
      if (
        timeRange === "quarterly" &&
        !(date.getDate() === 1 && [0, 3, 6, 9].includes(date.getMonth()))
      )
        continue;
      if (
        timeRange === "yearly" &&
        !(date.getDate() === 1 && date.getMonth() === 0)
      )
        continue;

      visibleData.push({ date: item.date, portfolio: item.value });
    }

    return visibleData;
  }, [portfolioChartData, timeRange]);

  // Filter benchark data to selected benchmark and selected time period
  const filteredBenchmarkData = React.useMemo(() => {
    if (!selectedBenchmark) return [];

    const visibleData = [];

    for (const item of benchmarkChartData) {
      const date = new Date(item.date);

      if (timeRange === "weekly" && date.getDay() !== 1) continue;
      if (timeRange === "monthly" && date.getDate() !== 1) continue;
      if (
        timeRange === "quarterly" &&
        !(date.getDate() === 1 && [0, 3, 6, 9].includes(date.getMonth()))
      )
        continue;
      if (
        timeRange === "yearly" &&
        !(date.getDate() === 1 && date.getMonth() === 0)
      )
        continue;

      visibleData.push({ date: item.date, benchmark: item[selectedBenchmark] });
    }

    return visibleData;
  }, [benchmarkChartData, timeRange, selectedBenchmark]);

  const combinedChartData = React.useMemo(() => {
    const benchmarkMap = new Map(
      filteredBenchmarkData.map((item) => [item.date, item.benchmark])
    );

    return filteredPortfolioData.map((item) => ({
      date: item.date,
      portfolio: item.portfolio,
      benchmark: selectedBenchmark ? benchmarkMap.get(item.date) ?? null : null,
    }));
  }, [filteredPortfolioData, filteredBenchmarkData]);

  const maxY = React.useMemo(() => {
    return Math.max(
      ...combinedChartData.map((d) =>
        Math.max(d.portfolio ?? 0, Number(d.benchmark ?? 0))
      )
    );
  }, [combinedChartData]);

  return (
    <Card className="pt-0">
      <CardHeader className="flex items-center gap-3 space-y-0 border-b py-5 sm:flex-row">
        <div className="grid flex-1 gap-1">
          <CardTitle>Portfolio Value History</CardTitle>
          <CardDescription>
            Track how the total value of your portfolio changes over time.
          </CardDescription>
        </div>

        <InfoTooltip content={tooltipTexts.benchmarks} />
        <Select value={selectedBenchmark} onValueChange={setSelectedBenchmark}>
          <SelectTrigger className="w-[170px] " aria-label="Select benchmark">
            <SelectValue />
          </SelectTrigger>
          <SelectContent className="rounded-xl">
            <SelectItem value="none">No benchmark</SelectItem>
            {Object.keys(benchmarkLabels).map((ticker) => (
              <SelectItem key={ticker} value={ticker} className="rounded-lg">
                {benchmarkLabels[ticker]}
              </SelectItem>
            ))}
          </SelectContent>
        </Select>

        <Select value={timeRange} onValueChange={setTimeRange}>
          <SelectTrigger
            className="hidden w-[160px] rounded-lg sm:ml-auto md:flex"
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
      <CardContent className="px-2 pt-4 sm:px-6 sm:pt-6">
        <ChartContainer
          config={chartConfig}
          className="aspect-auto h-[250px] w-full"
        >
          <AreaChart data={combinedChartData}>
            <defs>
              <linearGradient id="fillPortfolio" x1="0" y1="0" x2="0" y2="1">
                <stop
                  offset="5%"
                  stopColor="var(--color-portfolio)"
                  stopOpacity={0.8}
                />
                <stop
                  offset="95%"
                  stopColor="var(--color-portfolio)"
                  stopOpacity={0.1}
                />
              </linearGradient>
              <linearGradient id="fillBenchmark" x1="0" y1="0" x2="0" y2="1">
                <stop
                  offset="5%"
                  stopColor="var(--color-benchmark)"
                  stopOpacity={0.8}
                />
                <stop
                  offset="95%"
                  stopColor="var(--color-benchmark)"
                  stopOpacity={0.1}
                />
              </linearGradient>
            </defs>
            <CartesianGrid vertical={false} />
            <XAxis
              dataKey="date"
              tickLine={false}
              axisLine={false}
              tickMargin={8}
              minTickGap={32}
              tickFormatter={(value) => {
                const date = new Date(value);

                switch (
                  timeRange // filter could be "day", "week", "month", "quarter", "year"
                ) {
                  case "daily":
                    return date.toLocaleDateString("en-GB", {
                      day: "numeric",
                      month: "short",
                      year: "2-digit",
                    }); // e.g. 7 Aug '25
                  case "weekly":
                    return date.toLocaleDateString("en-GB", {
                      day: "numeric",
                      month: "short",
                      year: "2-digit",
                    }); // e.g. 7 Aug '25
                  case "monthly":
                    return date.toLocaleDateString("en-GB", {
                      month: "short",
                      year: "2-digit",
                    }); // e.g. Aug '25
                  case "quarterly":
                    const quarter = Math.floor(date.getMonth() / 3) + 1;
                    return `Q${quarter} '${date
                      .getFullYear()
                      .toString()
                      .slice(-2)}`; // e.g. Q3 '25
                  case "yearly":
                    return date.getFullYear().toString(); // e.g. 2025
                  default:
                    return date.toLocaleDateString("en-GB");
                }
              }}
            />
            <YAxis
              domain={[0, maxY]}
              tickLine={false}
              axisLine={false}
              tickMargin={8}
              minTickGap={24}
              tickFormatter={(value) => {
                return formatCurrency(value, currency_code, "compact");
              }}
            />

            <ChartTooltip
              cursor={false}
              content={
                <ChartTooltipContent
                  labelFormatter={(value) =>
                    new Date(value).toLocaleDateString("en-GB", {
                      month: "short",
                      day: "numeric",
                      year: "numeric",
                    })
                  }
                  formatter={(value, name) => {
                    const config =
                      chartConfig[name as keyof typeof chartConfig];

                    return (
                      <div className="text-muted-foreground flex gap-3 items-center text-xs">
                        <div
                          className="h-2.5 w-2.5 shrink-0 rounded-[2px]"
                          style={{ backgroundColor: config?.color ?? "grey" }}
                        />

                        {config?.label ?? name}

                        <div className="text-foreground ml-auto flex items-baseline gap-0.5 font-mono font-medium tabular-nums">
                          {formatCurrency(Number(value), currency_code)}
                        </div>
                      </div>
                    );
                  }}
                />
              }
            />

            <Area
              dataKey="portfolio"
              type="natural"
              fill="url(#fillPortfolio)"
              stroke="var(--color-Portfolio)"
            />
            <Area
              dataKey="benchmark"
              type="natural"
              fill="url(#fillBenchmark)"
              stroke="var(--color-benchmark)"
            />
          </AreaChart>
        </ChartContainer>
      </CardContent>
    </Card>
  );
}
