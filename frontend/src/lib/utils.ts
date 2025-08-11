import { clsx, type ClassValue } from "clsx"
import { twMerge } from "tailwind-merge"
import {DollarSign, Euro, PoundSterling, CreditCard} from "lucide-react";
import type { LucideIcon } from "lucide-react";

export function cn(...inputs: ClassValue[]) {
  return twMerge(clsx(inputs))
}

// Fetch symbol / icon

const currencyIcons: Record<string, LucideIcon> = {
  USD: DollarSign,
  EUR: Euro,
  GBP: PoundSterling,
};

const currencySymbols: Record<string, string> = {
  USD: "$",
  EUR: "€",
  GBP: "£",
};

export function getCurrencyIcon(currencyCode: string): LucideIcon {
  return currencyIcons[currencyCode.toUpperCase()] || CreditCard;
}

export function getCurrencySymbol(currencyCode: string): string {
  return currencySymbols[currencyCode.toUpperCase()] || "";
}


// Format utility functions

export function formatCurrency(value: number, currencyCode: string,notation: "standard" | "compact" = "standard", decimals: number=2 ): string {
  return new Intl.NumberFormat('en-US', {
    style: 'currency',
    currency: currencyCode,
    notation: notation,
    maximumFractionDigits: decimals
  }).format(value);
}

export function formatPercentage(
  value: number,
  decimals: number = 1,
  showSign: boolean = true,
  showTrailingZeros: boolean = false
): string {
  return new Intl.NumberFormat("en-US", {
    style: "percent",
    maximumFractionDigits: decimals,
    minimumFractionDigits: showTrailingZeros ? decimals : 0,
    signDisplay: showSign ? "exceptZero" : "never",
  }).format(value);
}


export function formatDate(dateString: string) {
  if (!dateString) return "";

  return new Date(dateString).toLocaleDateString("en-GB", {
    day: "numeric",
    month: "short",
    year: "numeric",
  });
}

export function capitalizeFirst(str: string) {
  if (!str) return "";
  return str.charAt(0).toUpperCase() + str.slice(1).toLowerCase();
}

// Metric rating utility functions

type MetricRating = {
  label?: string;
  ratingClass: string;
  caption: string;
};

export function getGrowthRating(cagr: number) {
  if (cagr < 0) 
    return { ratingClass: "text-rating-1", caption: "Value is shrinking over time" };
  if (cagr < 0.03) 
    return { ratingClass: "text-rating-2", caption: "Small yearly growth" };
  if (cagr < 0.07) 
    return { ratingClass: "text-rating-3", caption: "Steady yearly growth" };
  if (cagr < 0.12) 
    return { ratingClass: "text-rating-4", caption: "Strong yearly growth" };
  return { ratingClass: "text-rating-5", caption: "Outstanding yearly growth" };
}


export function getRiskRating(volatility: number): MetricRating {
  if (volatility <= 0.03)
    return { 
      label: "Very Low", 
      ratingClass: "text-rating-5", 
      caption: "Very minor ups and downs over time" 
    };
  if (volatility <= 0.05) 
    return { 
      label: "Low", 
      ratingClass: "text-rating-4", 
      caption: "Small changes, generally stable" 
    };
  if (volatility <= 0.10) 
    return { 
      label: "Medium", 
      ratingClass: "text-rating-3", 
      caption: "Noticeable swings but still manageable" 
    };
  if (volatility <= 0.20) 
    return { 
      label: "High", 
      ratingClass: "text-rating-2", 
      caption: "Large fluctuations, can rise or fall quickly" 
    };
  return { 
    label: "Very High", 
    ratingClass: "text-rating-1", 
    caption: "Extreme ups and downs, highly unpredictable" 
  };
}

export function getDrawdownRating(maxDrawdown: number) {
  if (maxDrawdown >= -5) 
    return { ratingClass: "text-rating-5", caption: "Hardly any major drops" };
  if (maxDrawdown >= -10) 
    return { ratingClass: "text-rating-4", caption: "Small drops during downturns" };
  if (maxDrawdown >= -20) 
    return { ratingClass: "text-rating-3", caption: "Noticeable drops but recoverable" };
  if (maxDrawdown >= -35) 
    return { ratingClass: "text-rating-2", caption: "Large drops in value" };
  return { ratingClass: "text-rating-1", caption: "Very large and steep drops in value" };
}


export function getSharpeRating(sharpe: number): MetricRating {
  if (sharpe < 0)
    return { 
      label: "Poor", 
      ratingClass: "text-rating-1", 
      caption: "Losing money compared to a safe investment" 
    };
  if (sharpe < 0.5) 
    return { 
      label: "Low", 
      ratingClass: "text-rating-2", 
      caption: "Returns don’t justify the risk taken" 
    };
  if (sharpe < 1.0) 
    return { 
      label: "Fair", 
      ratingClass: "text-rating-3", 
      caption: "Moderate reward for the risk" 
    };
  if (sharpe < 2.0) 
    return { 
      label: "Good", 
      ratingClass: "text-rating-4", 
      caption: "Strong reward compared to the risk" 
    };
  return { 
    label: "Excellent", 
    ratingClass: "text-rating-5", 
    caption: "Outstanding returns for the amount of risk" 
  };
}


