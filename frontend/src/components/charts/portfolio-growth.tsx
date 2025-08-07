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

import { formatCurrency } from "@/lib/utils";

import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";

export const description = "An interactive area chart";

const chartConfig = {
  value: {
    label: "Value",
    color: "var(--chart-1)",
  },
} satisfies ChartConfig;

interface PortfolioGrowthChartProps {
  chartData: {
    date: string;
    contributions: number;
    gain: number;
    value: number;
  }[];
  currency_code: string;
}

export function PortfolioGrowthChart({
  chartData,
  currency_code,
}: PortfolioGrowthChartProps) {
  const [timeRange, setTimeRange] = React.useState("daily");

  // Dynamically filter the available periods based on backtest length
  const startDate = new Date(chartData[0]?.date);
  const endDate = new Date(chartData[chartData.length - 1]?.date);
  const daysDiff = differenceInDays(endDate, startDate);

  const availableOptions = ["daily"];
  if (daysDiff >= 30) availableOptions.push("weekly");
  if (daysDiff >= 120) availableOptions.push("monthly");
  if (daysDiff >= 365) availableOptions.push("quarterly");
  if (daysDiff >= 1461) availableOptions.push("yearly");

  //   const filteredData = chartData.filter((item) => {
  //     const date = new Date(item.date);
  //     const referenceDate = new Date("2024-06-30");
  //     let daysToSubtract = 90;
  //     if (timeRange === "30d") {
  //       daysToSubtract = 30;
  //     } else if (timeRange === "7d") {
  //       daysToSubtract = 7;
  //     }
  //     const startDate = new Date(referenceDate);
  //     startDate.setDate(startDate.getDate() - daysToSubtract);
  //     return date >= startDate;
  //   });

  const filteredData = React.useMemo(() => {
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

  return (
    <Card className="pt-0">
      <CardHeader className="flex items-center gap-2 space-y-0 border-b py-5 sm:flex-row">
        <div className="grid flex-1 gap-1">
          <CardTitle>Area Chart - Interactive</CardTitle>
          <CardDescription>
            Showing total visitors for the last 3 months
          </CardDescription>
        </div>
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
      <CardContent className="px-2 pt-4 sm:px-6 sm:pt-6">
        <ChartContainer
          config={chartConfig}
          className="aspect-auto h-[250px] w-full"
        >
          <AreaChart data={filteredData}>
            <defs>
              <linearGradient id="fillValue" x1="0" y1="0" x2="0" y2="1">
                <stop
                  offset="5%"
                  stopColor="var(--color-value)"
                  stopOpacity={0.8}
                />
                <stop
                  offset="95%"
                  stopColor="var(--color-value)"
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
              dataKey="value"
              tickLine={false}
              axisLine={false}
              tickMargin={8}
              minTickGap={24}
              tickFormatter={(value) => {
                return formatCurrency(value, currency_code, "compact");
              }}
            />

            {/* <ChartTooltip
              cursor={true}
              content={
                <ChartTooltipContent
                  labelFormatter={(value) => {
                    return new Date(value).toLocaleDateString("en-GB", {
                      month: "short",
                      day: "numeric",
                      year: "numeric",
                    });
                  }}
                  formatter={(value) => {
                    return `${formatCurrency(Number(value), currency_code)}`;
                  }}
                  indicator="dot"
                />
              }
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
                  formatter={(value, name) => (
                    <div className="text-muted-foreground flex gap-2 items-center text-xs">
                      {chartConfig[name as keyof typeof chartConfig]?.label ||
                        name}
                      <div className="text-foreground ml-auto flex items-baseline gap-0.5 font-mono font-medium tabular-nums">
                        {formatCurrency(Number(value), currency_code)}
                      </div>
                    </div>
                  )}
                />
              }
              cursor={true}
            />

            <Area
              dataKey="value"
              type="natural"
              fill="url(#fillValue)"
              stroke="var(--color-value)"
              stackId="a"
            />
          </AreaChart>
        </ChartContainer>
      </CardContent>
    </Card>
  );
}
