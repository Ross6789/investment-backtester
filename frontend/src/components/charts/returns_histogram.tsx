import { Bar, BarChart, CartesianGrid, XAxis, YAxis } from "recharts";
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";

import type { ChartConfig } from "@/components/ui/chart";

import {
  ChartContainer,
  ChartTooltip,
  ChartTooltipContent,
} from "@/components/ui/chart";

const chartConfig = {
  bucket: {
    label: "Return range",
  },
  count: {
    label: "Number of months",
  },
} satisfies ChartConfig;

interface ReturnHistogramChartProps {
  chartData: {
    bucket: string;
    count: number;
  }[];
}

export function ReturnHistogramChart({ chartData }: ReturnHistogramChartProps) {
  return (
    <Card>
      <CardHeader className="flex items-center gap-2 space-y-0 border-b py-5 sm:flex-row">
        <div className="grid flex-1 gap-1">
          <CardTitle>Monthly Return Pattern</CardTitle>
          <CardDescription>
            How often your monthly returns fall into each range.
          </CardDescription>
        </div>
      </CardHeader>

      <CardContent>
        <ChartContainer config={chartConfig}>
          <BarChart accessibilityLayer data={chartData}>
            <CartesianGrid vertical={false} />
            <XAxis dataKey="bucket" />
            <YAxis
              dataKey="count"
              tickLine={false}
              axisLine={false}
              tickMargin={8}
              minTickGap={24}
            />
            <ChartTooltip
              content={
                <ChartTooltipContent
                // hideIndicator
                // formatter={(value, name) => (
                //   <div className="text-muted-foreground flex gap-2 items-center text-xs">
                //     {chartConfig[name as keyof typeof chartConfig]?.label ||
                //       name}
                //     <div className="text-foreground ml-auto flex items-baseline gap-0.5 font-mono font-medium tabular-nums">
                //       {formatPercentage(Number(value), 2)}
                //     </div>
                //   </div>
                // )}
                />
              }
            />

            <Bar dataKey="count" fill="var(--chart-1)" radius={8} />
          </BarChart>
        </ChartContainer>
      </CardContent>
    </Card>
  );
}
