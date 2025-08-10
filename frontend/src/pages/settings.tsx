import { zodResolver } from "@hookform/resolvers/zod";
import { useForm, useFieldArray } from "react-hook-form";
import { z } from "zod";
import { useNavigate } from "react-router-dom";
import { useEffect, useState } from "react";

import { format } from "date-fns";
import { CalendarIcon, Trash2, Check, ChevronDown, Plus } from "lucide-react";
import { getCurrencySymbol } from "@/lib/utils";
import { cn } from "@/lib/utils";
import { Button } from "@/components/ui/button";
import { Calendar } from "@/components/ui/calendar";
import { AssetClassFilterDropdown } from "@/components/asset_filter_dropdown";

import {
  Popover,
  PopoverContent,
  PopoverTrigger,
} from "@/components/ui/popover";

import {
  Command,
  CommandEmpty,
  CommandGroup,
  CommandInput,
  CommandItem,
  CommandList,
} from "@/components/ui/command";

import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";

import {
  Form,
  FormControl,
  FormDescription,
  FormField,
  FormItem,
  FormLabel,
  FormMessage,
} from "@/components/ui/form";
import { Input } from "@/components/ui/input";
import { Switch } from "@/components/ui/switch";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";

const assetClassLabels: Record<string, string> = {
  "us stock": "US Stock",
  "uk stock": "UK Stock",
  etf: "ETF",
  "mutual fund": "Mutual Fund",
  cryptocurrency: "Cryptocurrency",
  commodity: "Commodity",
};

const formSchema = z
  .object({
    mode: z.enum(["basic", "realistic"]),
    base_currency: z.enum(["GBP", "USD", "EUR"]),
    start_date: z
      .date()
      .min(new Date("1970-01-01"), { message: "Date must be after Jan 1 1970" })
      .max(new Date("2025-05-31"), {
        message: "Date must be before June 1 2025",
      }),
    end_date: z
      .date()
      .min(new Date("1970-01-01"), { message: "Date must be after Jan 1 1970" })
      .max(new Date("2025-05-31"), {
        message: "Date must be before June 1 2025",
      }),
    target_weights: z.array(
      z.object({
        ticker: z.string(),
        percentage: z.preprocess((val) => {
          if (val === "" || val === null || val === undefined) return undefined;
          return typeof val === "string" ? Number(val) : val;
        }, z.number().min(0, { message: "Minimum is 0" }).max(100, { message: "Maximum is 100" })),
      })
    ),
    initial_investment: z.preprocess(
      (val) => {
        if (val === "" || val === null || val === undefined) return undefined;
        return typeof val === "string" ? Number(val) : val;
      },
      z
        .number({
          error: "Initial investment is required",
        })
        .min(0.01, { message: "Minimum is 0.01" })
        .max(1000000000, { message: "Maximum is 1,000,000,000" })
        .refine(
          (val) => {
            const str = val.toString();
            const parts = str.split(".");
            return parts.length === 1 || parts[1].length <= 2;
          },
          {
            message: "Cannot have more than 2 decimal places",
          }
        )
    ),
    recurring_investment: z
      .object({
        amount: z.preprocess(
          (val) => {
            if (val === "" || val === null || val === undefined)
              return undefined;
            return typeof val === "string" ? Number(val) : val;
          },
          z
            .number()
            .min(0, { message: "Must not be negative" })
            .max(1000000000, { message: "Maximum is 1,000,000,000" })
            .refine(
              (val) => {
                const str = val.toString();
                const parts = str.split(".");
                return parts.length === 1 || parts[1].length <= 2;
              },
              {
                message: "Cannot have more than 2 decimal places",
              }
            )
            .optional()
        ),
        frequency: z.enum([
          "never",
          "daily",
          "weekly",
          "monthly",
          "quarterly",
          "yearly",
        ]),
      })
      .partial()
      .optional()
      .nullable(),
    strategy: z.object({
      fractional_shares: z.boolean().default(true).optional(),
      reinvest_dividends: z.boolean().default(true).optional(),
      rebalance_frequency: z.enum([
        "never",
        "daily",
        "weekly",
        "monthly",
        "quarterly",
        "yearly",
      ]),
    }),
  })
  .refine((data) => data.start_date < data.end_date, {
    message: "Start date must be before end date",
    path: ["end_date"],
  })
  .refine((data) => {
    const total = data.target_weights.reduce(
      (sum, item) => sum + (item.percentage ?? 0),
      0
    );
    return Math.abs(total - 100) < 0.0001; // account for floating point precision
  });

type Asset = {
  ticker: string;
  name: string;
  asset_type: string;
  start_date: string;
  end_date: string;
  currency: string;
};

export function SettingsPage() {
  const navigate = useNavigate();

  const [allAssets, setAllAssets] = useState<Asset[]>([]);

  // Fetch asset data once when page loads
  useEffect(() => {
    fetch("http://localhost:5002/api/assets")
      .then((res) => res.json())
      .then((data: Asset[]) => {
        setAllAssets(data);
      })
      .catch(console.error);
  }, []);

  // Allow filtering of the assets in combo box - set default to us stock
  const [enabledAssetClasses, setEnabledAssetClasses] = useState<string[]>([
    "us stock",
  ]);

  // Filter assets based on selected asset classes:
  const filteredAssets = allAssets.filter((a) =>
    enabledAssetClasses.includes(a.asset_type)
  );

  // Configure combo box options from filtered asset list
  const assetOptions = filteredAssets.map(({ ticker, name }) => ({
    label: `${ticker} - ${name}`,
    value: ticker,
  }));

  // 1. Define your form.
  const form = useForm({
    resolver: zodResolver(formSchema),
    defaultValues: {
      mode: "basic",
      base_currency: "GBP",
      start_date: new Date("2020-01-01"),
      end_date: new Date("2025-05-31"),
      target_weights: [
        { ticker: "AAPL", percentage: 50 },
        { ticker: "GOOG", percentage: 30 },
        { ticker: "AMZN", percentage: 20 },
      ],
      initial_investment: 1000,
      recurring_investment: {
        amount: 0,
        frequency: "never",
      },
      strategy: {
        fractional_shares: true,
        reinvest_dividends: true,
        rebalance_frequency: "never",
      },
    },
  });

  const { fields, append, remove } = useFieldArray({
    control: form.control,
    name: "target_weights",
  });

  // Determine which currency symbol matches the selected dropdown
  const selectedCurrency = form.watch("base_currency");
  const symbol = getCurrencySymbol(selectedCurrency) || "";

  // 2. Define a submit handler.
  async function onSubmit(values: z.infer<typeof formSchema>) {
    // Clean recurring investment
    const recurringInvestment =
      !values.recurring_investment ||
      values.recurring_investment.frequency === "never" ||
      !values.recurring_investment.amount
        ? null
        : {
            amount: Number(values.recurring_investment.amount),
            frequency: values.recurring_investment.frequency,
          };

    // Convert target weights array to object
    const weightsObject = Object.fromEntries(
      values.target_weights
        .filter(
          (item): item is { ticker: string; percentage: number } =>
            item.percentage !== undefined && item.percentage !== 0
        )
        .map(({ ticker, percentage }) => [ticker, percentage / 100])
    );

    // Format dates
    const formatDate = (d: Date) =>
      d instanceof Date && !isNaN(d.getTime())
        ? d.toISOString().split("T")[0]
        : null;

    // Final cleaned payload
    const payload = {
      mode: values.mode,
      base_currency: values.base_currency,
      start_date: formatDate(values.start_date),
      end_date: formatDate(values.end_date),
      initial_investment: Number(values.initial_investment),
      strategy: {
        fractional_shares: values.strategy.fractional_shares,
        reinvest_dividends: values.strategy.reinvest_dividends,
        rebalance_frequency: values.strategy.rebalance_frequency,
      },
      recurring_investment: recurringInvestment,
      target_weights: weightsObject,
    };

    console.log("Input settings:", payload);

    try {
      const response = await fetch("http://localhost:5002/api/run-backtest", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      });

      if (!response.ok) {
        throw new Error("Backtest failed");
      }

      const result = await response.json();

      // Navigate to results page and pass result data
      navigate("/results", { state: { backtestResult: result } });
    } catch (error) {
      console.error(error);
      alert("Failed to run backtest");
    }
  }

  // Handler to update enabled classes from dropdown:
  const handleAssetClassChange = (newEnabledClasses: string[]) => {
    setEnabledAssetClasses(newEnabledClasses);
  };

  return (
    <Form {...form}>
      <form onSubmit={form.handleSubmit(onSubmit)} className="space-y-8">
        <div className="m-4 p-4 flex lg:flex-row flex-col gap-4">
          {/* LEFT SIDE - add items-star to make box shrink */}
          <div className="flex-1">
            <Card>
              <CardHeader className="flex items-center gap-2 space-y-0 border-b sm:flex-row">
                <div className="grid flex-1 gap-1">
                  <CardTitle>Portfolio Assets</CardTitle>
                  <CardDescription>
                    Select assets and assign weights (must total 100%)
                  </CardDescription>
                </div>
                <AssetClassFilterDropdown
                  enabledClasses={enabledAssetClasses}
                  onChange={handleAssetClassChange}
                  classLabels={assetClassLabels}
                />
              </CardHeader>

              {/* <CardHeader>
                <CardTitle>Portfolio Assets</CardTitle>
                <CardDescription>
                  Select assets and assign weights (must total 100%)
                </CardDescription>
                <AssetClassFilterDropdown
                  enabledClasses={enabledAssetClasses}
                  onChange={handleAssetClassChange}
                  classLabels={assetClassLabels}
                />
              </CardHeader> */}
              <CardContent>
                {fields.map((field, index) => (
                  <div key={field.id} className="flex items-center gap-4 mb-4">
                    {/* Ticker input */}

                    <FormField
                      control={form.control}
                      name={`target_weights.${index}.ticker`}
                      render={({ field }) => {
                        const selectedTickers = form
                          .watch("target_weights")
                          ?.map((item) => item.ticker);

                        return (
                          <FormItem className="flex-1">
                            <Popover>
                              <PopoverTrigger asChild>
                                <FormControl>
                                  <Button
                                    variant="outline"
                                    role="combobox"
                                    className={cn(
                                      "w-full justify-between",
                                      !field.value && "text-muted-foreground"
                                    )}
                                  >
                                    {field.value
                                      ? assetOptions.find(
                                          (asset) => asset.value === field.value
                                        )?.label
                                      : "Select asset"}
                                    <ChevronDown className="opacity-50" />
                                  </Button>
                                </FormControl>
                              </PopoverTrigger>
                              <PopoverContent
                                className="w-full p-0"
                                align="start"
                              >
                                <Command>
                                  <CommandInput
                                    placeholder="Search assets..."
                                    className="h-9"
                                  />
                                  <CommandList>
                                    <CommandEmpty>No asset found.</CommandEmpty>
                                    <CommandGroup>
                                      {assetOptions.map((asset) => {
                                        const isSelectedInOtherField =
                                          selectedTickers?.includes(
                                            asset.value
                                          ) && asset.value !== field.value;

                                        return (
                                          <CommandItem
                                            key={asset.value}
                                            value={asset.value}
                                            disabled={isSelectedInOtherField}
                                            onSelect={() => {
                                              if (!isSelectedInOtherField) {
                                                field.onChange(asset.value);
                                              }
                                            }}
                                          >
                                            {asset.label}
                                            <Check
                                              className={cn(
                                                "ml-auto",
                                                asset.value === field.value
                                                  ? "opacity-100"
                                                  : "opacity-0"
                                              )}
                                            />
                                          </CommandItem>
                                        );
                                      })}
                                    </CommandGroup>
                                  </CommandList>
                                </Command>
                              </PopoverContent>
                            </Popover>
                            <FormMessage />
                          </FormItem>
                        );
                      }}
                    />

                    {/* Percentage input - 1/4 width */}
                    <FormField
                      control={form.control}
                      name={`target_weights.${index}.percentage`}
                      render={({ field }) => (
                        <FormItem className="w-20">
                          <FormControl>
                            <Input
                              type="number"
                              placeholder="%" // optional, can be removed
                              step="1"
                              {...field}
                              value={
                                field.value === undefined ||
                                field.value === null
                                  ? ""
                                  : Number(field.value)
                              }
                              onChange={(e) => {
                                const raw = e.target.value;
                                field.onChange(raw === "" ? null : raw);
                              }}
                            />
                          </FormControl>
                          <FormMessage />
                        </FormItem>
                      )}
                    />

                    {/* % symbol */}
                    <span className="text-muted-foreground">%</span>

                    {/* Delete button with trash icon */}
                    <Button
                      type="button"
                      variant="ghost"
                      size="icon"
                      onClick={() => remove(index)}
                    >
                      <Trash2 className="h-4 w-4  text-red-500" />
                    </Button>
                  </div>
                ))}

                <Button
                  type="button"
                  variant="outline"
                  onClick={() => append({ ticker: "", percentage: 0 })}
                >
                  <Plus className="h-4 w-4" />
                  Add Asset
                </Button>
              </CardContent>
            </Card>
          </div>

          {/* RIGHT SIDE */}
          <div className="flex flex-col flex-1 gap-4">
            <Card>
              <CardHeader>
                <CardTitle>Backtest Mode</CardTitle>
                <CardDescription>
                  Choose your simulation complexity level
                </CardDescription>
              </CardHeader>
              <CardContent>
                <FormField
                  control={form.control}
                  name="mode"
                  render={({ field }) => (
                    <FormItem>
                      <Select
                        onValueChange={field.onChange}
                        defaultValue={field.value}
                      >
                        <FormControl>
                          <SelectTrigger className="w-full">
                            <SelectValue placeholder="Select a backtest mode" />
                          </SelectTrigger>
                        </FormControl>
                        <SelectContent>
                          <SelectItem value="basic">Basic Mode</SelectItem>
                          <SelectItem value="realistic">
                            Realistic Mode
                          </SelectItem>
                        </SelectContent>
                      </Select>
                      <FormMessage />
                    </FormItem>
                  )}
                />
              </CardContent>
            </Card>
            <Card>
              <CardHeader>
                <CardTitle>Investment Settings</CardTitle>
              </CardHeader>
              <CardContent>
                <div className="grid grid-cols-1 sm:grid-cols-2 gap-4">
                  <div className="sm:col-span-2">
                    <FormField
                      control={form.control}
                      name="base_currency"
                      render={({ field }) => (
                        <FormItem>
                          <FormLabel>Currency</FormLabel>
                          <Select
                            onValueChange={field.onChange}
                            defaultValue={field.value}
                          >
                            <FormControl>
                              <SelectTrigger className="w-full">
                                <SelectValue placeholder="Select a currency" />
                              </SelectTrigger>
                            </FormControl>
                            <SelectContent>
                              <SelectItem value="GBP">
                                £ GBP - British Pound
                              </SelectItem>
                              <SelectItem value="EUR">€ EUR - Euro</SelectItem>
                              <SelectItem value="USD">
                                $ USD - US Dollar
                              </SelectItem>
                            </SelectContent>
                          </Select>
                          <FormMessage />
                        </FormItem>
                      )}
                    />
                  </div>
                  <FormField
                    control={form.control}
                    name="start_date"
                    render={({ field }) => (
                      <FormItem>
                        <FormLabel>Start Date</FormLabel>
                        <Popover>
                          <PopoverTrigger asChild>
                            <FormControl>
                              <Button
                                variant={"outline"}
                                className={cn(
                                  "w-full pl-3 text-left font-normal",
                                  !field.value && "text-muted-foreground"
                                )}
                              >
                                {field.value ? (
                                  format(field.value, "PPP")
                                ) : (
                                  <span>Pick a date</span>
                                )}
                                <CalendarIcon className="ml-auto h-4 w-4 opacity-50" />
                              </Button>
                            </FormControl>
                          </PopoverTrigger>
                          <PopoverContent className="w-auto p-0" align="start">
                            <Calendar
                              mode="single"
                              selected={field.value}
                              onSelect={field.onChange}
                              disabled={(date) =>
                                date >= new Date("2025-06-01") ||
                                date <= new Date("1969-12-31")
                              }
                              captionLayout="dropdown"
                              defaultMonth={field.value}
                              startMonth={new Date("1970-01-01")}
                              endMonth={new Date("2025-05-31")}
                            />
                          </PopoverContent>
                        </Popover>
                        <FormMessage />
                      </FormItem>
                    )}
                  />
                  <FormField
                    control={form.control}
                    name="end_date"
                    render={({ field }) => (
                      <FormItem>
                        <FormLabel>End Date</FormLabel>
                        <Popover>
                          <PopoverTrigger asChild>
                            <FormControl className="w-full">
                              <Button
                                variant={"outline"}
                                className={cn(
                                  "w-full px-3 text-left font-normal",
                                  !field.value && "text-muted-foreground"
                                )}
                              >
                                {field.value ? (
                                  format(field.value, "PPP")
                                ) : (
                                  <span>Pick a date</span>
                                )}
                                <CalendarIcon className="ml-auto h-4 w-4 opacity-50" />
                              </Button>
                            </FormControl>
                          </PopoverTrigger>
                          <PopoverContent className="w-auto p-0" align="start">
                            <Calendar
                              mode="single"
                              selected={field.value}
                              onSelect={field.onChange}
                              disabled={(date) =>
                                date >= new Date("2025-06-01") ||
                                date <= new Date("1969-12-31")
                              }
                              captionLayout="dropdown"
                              defaultMonth={field.value}
                              startMonth={new Date("1970-01-01")}
                              endMonth={new Date("2025-05-31")}
                            />
                          </PopoverContent>
                        </Popover>
                        <FormMessage />
                      </FormItem>
                    )}
                  />
                  <div className="sm:col-span-2">
                    <FormField
                      control={form.control}
                      name="initial_investment"
                      render={({ field }) => (
                        <FormItem>
                          <FormLabel>Initial Investment Amount</FormLabel>
                          <FormControl>
                            <div className="relative">
                              <span className="absolute left-3 top-1/2 -translate-y-1/2 text-muted-foreground text-sm pointer-events-none">
                                {symbol}
                              </span>
                              <Input
                                type="number"
                                placeholder="Enter amount"
                                step="100"
                                {...field}
                                className="pl-8"
                                value={
                                  field.value === undefined ||
                                  field.value === null
                                    ? ""
                                    : Number(field.value)
                                }
                                onChange={(e) => {
                                  const raw = e.target.value;
                                  field.onChange(raw === "" ? null : raw);
                                }}
                              />
                            </div>
                          </FormControl>
                          <FormMessage />
                        </FormItem>
                      )}
                    />
                  </div>

                  <FormField
                    control={form.control}
                    name="recurring_investment.amount"
                    render={({ field }) => (
                      <FormItem>
                        <FormLabel>
                          Recurring Contributions (Optional)
                        </FormLabel>
                        <FormControl>
                          <div className="relative">
                            <span className="absolute left-3 top-1/2 -translate-y-1/2 text-muted-foreground text-sm pointer-events-none">
                              {symbol}
                            </span>
                            <Input
                              type="number"
                              placeholder="Enter amount"
                              step="10"
                              {...field}
                              className="pl-8"
                              value={
                                field.value === undefined ||
                                field.value === null
                                  ? ""
                                  : Number(field.value)
                              }
                              onChange={(e) => {
                                const raw = e.target.value;
                                field.onChange(raw === "" ? null : raw);
                              }}
                            />
                          </div>
                        </FormControl>
                        <FormMessage />
                      </FormItem>
                    )}
                  />
                  <FormField
                    control={form.control}
                    name="recurring_investment.frequency"
                    render={({ field }) => (
                      <FormItem>
                        <Select
                          onValueChange={field.onChange}
                          defaultValue={field.value ?? ""}
                        >
                          <FormLabel>Frequency</FormLabel>
                          <FormControl>
                            <SelectTrigger className="w-full">
                              <SelectValue placeholder="Select frequency" />
                            </SelectTrigger>
                          </FormControl>
                          <SelectContent>
                            <SelectItem value="never">
                              No recurring contributions
                            </SelectItem>
                            <SelectItem value="daily">Daily</SelectItem>
                            <SelectItem value="weekly">Weekly</SelectItem>
                            <SelectItem value="monthly">Monthly</SelectItem>
                            <SelectItem value="quarterly">Quarterly</SelectItem>
                            <SelectItem value="yearly">Yearly</SelectItem>
                          </SelectContent>
                        </Select>
                        <FormMessage />
                      </FormItem>
                    )}
                  />
                </div>
              </CardContent>
            </Card>
            <Card>
              <CardHeader>
                <CardTitle>Strategy Settings</CardTitle>
              </CardHeader>
              <CardContent className="space-y-4">
                <FormField
                  control={form.control}
                  name="strategy.rebalance_frequency"
                  render={({ field }) => (
                    <FormItem>
                      <Select
                        onValueChange={field.onChange}
                        defaultValue={field.value}
                      >
                        <FormLabel>Rebalancing Frequency</FormLabel>
                        <FormControl>
                          <SelectTrigger className="w-full">
                            <SelectValue placeholder="Select rebalancing frequency" />
                          </SelectTrigger>
                        </FormControl>
                        <SelectContent>
                          <SelectItem value="never">Never</SelectItem>
                          <SelectItem value="daily">Daily</SelectItem>
                          <SelectItem value="weekly">Weekly</SelectItem>
                          <SelectItem value="monthly">Monthly</SelectItem>
                          <SelectItem value="quarterly">Quarterly</SelectItem>
                          <SelectItem value="yearly">Yearly</SelectItem>
                        </SelectContent>
                      </Select>
                      <FormMessage />
                    </FormItem>
                  )}
                />
                <FormField
                  control={form.control}
                  name="strategy.reinvest_dividends"
                  render={({ field }) => (
                    <FormItem className="flex flex-row items-center justify-between rounded-lg shadow-s">
                      <div className="space-y-0.5">
                        <FormLabel>Reinvest Dividends</FormLabel>
                        <FormDescription>
                          Choose whether to use dividends to buy more shares or
                          take as cash.
                        </FormDescription>
                      </div>
                      <FormControl>
                        <Switch
                          checked={field.value}
                          onCheckedChange={field.onChange}
                        />
                      </FormControl>
                    </FormItem>
                  )}
                />
                <FormField
                  control={form.control}
                  name="strategy.fractional_shares"
                  render={({ field }) => (
                    <FormItem className="flex flex-row items-center justify-between rounded-lg shadow-s">
                      <div className="space-y-0.5">
                        <FormLabel>Allow Fractional Shares</FormLabel>
                        <FormDescription>
                          Whether you can buy partial shares (e.g., 0.5 shares
                          of a stock)
                        </FormDescription>
                      </div>
                      <FormControl>
                        <Switch
                          checked={field.value}
                          onCheckedChange={field.onChange}
                        />
                      </FormControl>
                    </FormItem>
                  )}
                />
              </CardContent>
            </Card>
          </div>
        </div>
        {/* Full-width row for submit button */}
        <div className="flex justify-center mb-4">
          <Button type="submit">Run Backtest</Button>
        </div>
      </form>
    </Form>
  );
}
