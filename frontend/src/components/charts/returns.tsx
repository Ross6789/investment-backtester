import * as React from "react";
import { Bar, BarChart, CartesianGrid, Cell, LabelList } from "recharts";
import { differenceInDays } from "date-fns";
import {
  Card,
  CardContent,
  CardDescription,
  CardFooter,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";

import type { ChartConfig } from "@/components/ui/chart";

import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";

import {
  ChartContainer,
  ChartTooltip,
  ChartTooltipContent,
} from "@/components/ui/chart";

const chartConfig = {
  return: {
    label: "Return",
  },
} satisfies ChartConfig;

interface PeriodReturn {
  period: string;
  period_start: string;
  return: number;
}

interface ReturnChartProps {
  chartData: {
    daily: PeriodReturn[];
    weekly: PeriodReturn[];
    monthly: PeriodReturn[];
    quarterly: PeriodReturn[];
    yearly: PeriodReturn[];
  };
}

type PeriodKey = "daily" | "weekly" | "monthly" | "quarterly" | "yearly";

export function ReturnBarChart({ chartData }: ReturnChartProps) {
  const [timeRange, setTimeRange] = React.useState("monthly");

  // Dynamically filter the available periods based on backtest length
  const startDate = new Date(chartData["daily"][0]?.period_start);
  const endDate = new Date(
    chartData["daily"][chartData["daily"].length - 1]?.period_start
  );
  const daysDiff = differenceInDays(endDate, startDate);

  const availableOptions = ["daily"];
  if (daysDiff >= 30) availableOptions.push("weekly");
  if (daysDiff >= 120) availableOptions.push("monthly");
  if (daysDiff >= 365) availableOptions.push("quarterly");
  if (daysDiff >= 1461) availableOptions.push("yearly");

  const filteredData = React.useMemo(() => {
    return chartData[timeRange as PeriodKey] ?? [];
  }, [chartData, timeRange]);

  return (
    <Card>
      <CardHeader className="flex items-center gap-2 space-y-0 border-b py-5 sm:flex-row">
        <div className="grid flex-1 gap-1">
          <CardTitle>Returns Chart</CardTitle>
          <CardDescription>Track your returns for each period</CardDescription>
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

      <CardContent>
        <ChartContainer config={chartConfig}>
          <BarChart accessibilityLayer data={filteredData}>
            <CartesianGrid vertical={false} />
            <ChartTooltip
              cursor={false}
              content={<ChartTooltipContent hideLabel hideIndicator />}
            />
            {/* <Bar dataKey="visitors">
              <LabelList position="top" dataKey="month" fillOpacity={1} />
              {chartData.map((item) => (
                <Cell
                  key={item.month}
                  fill={item.visitors > 0 ? "var(--chart-1)" : "var(--chart-2)"}
                />
              ))}
            </Bar> */}

            <Bar dataKey="return">
              {/* <LabelList position="top" dataKey="period" fillOpacity={1} /> */}
              {filteredData.map((item) => (
                <Cell
                  key={item.period}
                  fill={item.return > 0 ? "var(--chart-1)" : "var(--chart-2)"}
                />
              ))}
            </Bar>
          </BarChart>
        </ChartContainer>
      </CardContent>
    </Card>
  );
}
