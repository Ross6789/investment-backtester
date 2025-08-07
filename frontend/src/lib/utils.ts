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
