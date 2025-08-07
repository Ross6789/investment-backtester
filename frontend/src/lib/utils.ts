import { clsx, type ClassValue } from "clsx"
import { twMerge } from "tailwind-merge"

export function cn(...inputs: ClassValue[]) {
  return twMerge(clsx(inputs))
}

// Format utility functions

export function formatCurrency(value: number, currencyCode: string): string {
  return new Intl.NumberFormat('en-GB', {
    style: 'currency',
    currency: currencyCode,
  }).format(value);
}

export function formatPercentage(
  value: number,
  decimals: number = 1,
  showSign: boolean = true
): string {
  const sign = showSign ? (value > 0 ? "+" : value < 0 ? "−" : "") : "";
  const percentage = (value * 100).toFixed(decimals);
  return `${sign}${percentage}%`;
}


// Metric rating utility functions

type MetricRating = {
  label: string;
  colorClass: string;
  tooltip?: string;
};


export function getRiskRating(volatility: number): MetricRating {
  if (volatility <= 0.03)
    return { label: "Very Low", colorClass: "text-dark-green" };
  if (volatility <= 0.05) 
    return { label: "Low", colorClass: "text-green" };
  if (volatility <= 0.10) 
    return { label: "Medium", colorClass: "text-yellow-600" };
  if (volatility <= 0.20) 
    return { label: "High", colorClass: "text-orange-600" };
  return { label: "Very High", colorClass: "text-red-700" };
}


export function getSharpeRating(sharpe: number): MetricRating {
  if (sharpe > 1.5)
    return { label: "Excellent risk-adjusted return", colorClass: "text-dark-green", tooltip:"Your investment achieved a high return for the amount of risk taken, meaning it performed very well compared to typical investments." };
  if (sharpe >= 1.0) 
    return { label: "Good risk-adjusted return", colorClass: "text-green", tooltip:"Your investment earned a solid return relative to the risk involved, showing a balanced and favorable outcome." };
  if (sharpe >= 0.50) 
    return { label: "Average risk-adjusted return", colorClass: "text-yellow-600", tooltip:"Your investment’s return is about what you would expect for the level of risk taken — a typical performance." };
  if (sharpe >= 0) 
    return { label: "Below average risk-adjusted return", colorClass: "text-orange-600", tooltip:"Your investment’s returns were somewhat low compared to the risk taken, indicating there may be room for improvement." };
  return { label: "Poor risk-adjusted return", colorClass: "text-red-700", tooltip: "Your investment took on risk but did not generate sufficient returns, suggesting it underperformed compared to typical investments"
};
}
