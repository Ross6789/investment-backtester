import { zodResolver } from "@hookform/resolvers/zod";
import { useForm, useFieldArray } from "react-hook-form";
import { z } from "zod";
import { useNavigate } from "react-router-dom";
import { useEffect, useState } from "react";
import { InfoTooltip } from "@/components/info_tooltip";
import { format } from "date-fns";
import { CalendarIcon, Trash2, Check, ChevronDown, Plus } from "lucide-react";
import { getCurrencySymbol } from "@/lib/utils";
import { cn } from "@/lib/utils";
import { Button } from "@/components/ui/button";
import { Calendar } from "@/components/ui/calendar";
import { AssetClassFilterDropdown } from "@/components/asset_filter_dropdown";
import { tooltipTexts } from "@/constants/ui_text";
import { AssetHoverCard } from "@/components/asset_hovercard";
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
import { LoadingScreen } from "@/components/loading_screen";

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
      fractional_shares: z.boolean().default(true),
      reinvest_dividends: z.boolean().default(true),
      rebalance_frequency: z.enum([
        "never",
        "daily",
        "weekly",
        "monthly",
        "quarterly",
        "yearly",
      ]),
    }),
    export_excel: z.boolean().default(false),
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
  dividends: string;
};

export function SettingsPage() {
  const navigate = useNavigate();

  // Create state control for monitoring loading
  const [loading, setLoading] = useState<boolean>(false);

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
  const assetOptions = filteredAssets.map((a) => ({
    label: `${a.ticker} - ${a.name}`,
    ticker: a.ticker,
    name: a.name,
    asset_type: a.asset_type,
    start_date: a.start_date,
    end_date: a.end_date,
    currency: a.currency,
    dividends: a.dividends,
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
      export_excel: false,
    },
  });

  const { fields, append, remove } = useFieldArray({
    control: form.control,
    name: "target_weights",
  });

  const CURRENCY_START_DATES: Record<string, Date> = {
    EUR: new Date("1999-01-03"),
    GBP: new Date("1970-01-01"),
    USD: new Date("1970-01-01"),
  };

  // Determine which currency symbol matches the selected dropdown
  const selectedCurrency = form.watch("base_currency");
  const symbol = getCurrencySymbol(selectedCurrency) || "";
  const currencyStart = CURRENCY_START_DATES[selectedCurrency];

  // Watch mode to determine state of dividend/fractional share toggles
  const selectedMode = form.watch("mode");

  // Add live tally to show weighting totals
  const targetWeights = form.watch("target_weights");
  const totalPercentage =
    targetWeights?.reduce((sum, item) => {
      const value = Number(item?.percentage);
      return sum + (isNaN(value) ? 0 : value);
    }, 0) ?? 0;

  // *--- Original submit button handler - before concurrent jobs
  // // 2. Define a submit handler.
  // async function onSubmit(values: z.infer<typeof formSchema>) {
  //   // Clean recurring investment
  //   const recurringInvestment =
  //     !values.recurring_investment ||
  //     values.recurring_investment.frequency === "never" ||
  //     !values.recurring_investment.amount
  //       ? null
  //       : {
  //           amount: Number(values.recurring_investment.amount),
  //           frequency: values.recurring_investment.frequency,
  //         };

  //   // Convert target weights array to object
  //   const weightsObject = Object.fromEntries(
  //     values.target_weights
  //       .filter(
  //         (item): item is { ticker: string; percentage: number } =>
  //           item.percentage !== undefined && item.percentage !== 0
  //       )
  //       .map(({ ticker, percentage }) => [ticker, percentage / 100])
  //   );

  //   // Format dates
  //   const formatDate = (d: Date) =>
  //     d instanceof Date && !isNaN(d.getTime())
  //       ? d.toISOString().split("T")[0]
  //       : null;

  //   // Final cleaned payload
  //   const payload = {
  //     mode: values.mode,
  //     base_currency: values.base_currency,
  //     start_date: formatDate(values.start_date),
  //     end_date: formatDate(values.end_date),
  //     initial_investment: Number(values.initial_investment),
  //     strategy: {
  //       fractional_shares: values.strategy.fractional_shares,
  //       reinvest_dividends: values.strategy.reinvest_dividends,
  //       rebalance_frequency: values.strategy.rebalance_frequency,
  //     },
  //     recurring_investment: recurringInvestment,
  //     target_weights: weightsObject,
  //     export_excel: values.export_excel,
  //   };

  //   console.log("Input settings:", payload);

  //   try {
  //     const response = await fetch("http://localhost:5002/api/run-backtest", {
  //       method: "POST",
  //       headers: { "Content-Type": "application/json" },
  //       body: JSON.stringify(payload),
  //     });

  //     if (!response.ok) {
  //       throw new Error("Backtest failed");
  //     }

  //     const result = await response.json();

  //     // Navigate to results page and pass result data
  //     navigate("/results", { state: { backtestResult: result } });
  //   } catch (error) {
  //     console.error(error);
  //     alert("Failed to run backtest");
  //   }
  // }

  async function onSubmit(values: z.infer<typeof formSchema>) {
    setLoading(true); // Show loading screen

    // 1. Clean recurring investment
    const recurringInvestment =
      !values.recurring_investment ||
      values.recurring_investment.frequency === "never" ||
      !values.recurring_investment.amount
        ? null
        : {
            amount: Number(values.recurring_investment.amount),
            frequency: values.recurring_investment.frequency,
          };

    // 2. Convert target weights array to object
    const weightsObject = Object.fromEntries(
      values.target_weights
        .filter(
          (item): item is { ticker: string; percentage: number } =>
            item.percentage !== undefined && item.percentage !== 0
        )
        .map(({ ticker, percentage }) => [ticker, percentage / 100])
    );

    // 3. Format dates
    const formatDate = (d: Date) =>
      d instanceof Date && !isNaN(d.getTime())
        ? d.toISOString().split("T")[0]
        : null;

    // 4. Final cleaned payload
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
      export_excel: values.export_excel,
    };

    console.log("Input settings:", payload);

    try {
      // 5. Start backtest, get job_id
      const response = await fetch("http://localhost:5002/api/run-backtest", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      });

      if (!response.ok) throw new Error("Failed to start backtest");

      const data = await response.json();
      const jobId = data.job_id;
      if (!jobId) throw new Error("No job ID returned");

      // 6. Poll for status
      let backtestResult: any = null;
      while (true) {
        await new Promise((r) => setTimeout(r, 2000)); // 2 sec delay
        const statusResp = await fetch(
          `http://localhost:5002/api/backtest-status/${jobId}`
        );
        if (!statusResp.ok) throw new Error("Failed to get job status");
        const statusData = await statusResp.json();

        if (statusData.status === "done") {
          backtestResult = statusData.result;
          break;
        } else if (statusData.status === "error") {
          throw new Error(statusData.result?.error || "Backtest error");
        }
        // else continue polling
      }

      // 7. Navigate to results page with result
      navigate("/results", { state: { backtestResult } });
    } catch (error) {
      console.error(error);
      alert((error as Error).message || "Failed to run backtest");
    } finally {
      setLoading(false); // Hide loading screen
    }
  }

  // Handler to update enabled classes from dropdown:
  const handleAssetClassChange = (newEnabledClasses: string[]) => {
    setEnabledAssetClasses(newEnabledClasses);
  };

  return (
    <Form {...form}>
      <form onSubmit={form.handleSubmit(onSubmit)}>
        <LoadingScreen visible={loading} />
        <div className="m-4 p-4 flex lg:flex-row flex-col gap-4">
          {/* LEFT SIDE - add items-star to make box shrink */}
          <div className="flex-1">
            <Card>
              <CardHeader className="flex items-center justify-between sm:flex-row">
                <div className="flex flex-col gap-1">
                  <div className="flex items-center gap-2">
                    <CardTitle>Portfolio Assets</CardTitle>
                    <InfoTooltip content={tooltipTexts.portfolioAssets} />
                  </div>

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

                        // Find the asset info for this specific combobox
                        const selectedAsset = assetOptions.find(
                          (asset) => asset.ticker === field.value
                        );

                        return (
                          <FormItem className="flex-1">
                            <Popover>
                              <PopoverTrigger asChild>
                                <FormControl>
                                  <Button
                                    variant="outline"
                                    role="combobox"
                                    className={cn(
                                      "flex items-center truncate gap-3",
                                      !field.value && "text-muted-foreground"
                                    )}
                                  >
                                    {selectedAsset && (
                                      <AssetHoverCard
                                        ticker={selectedAsset.ticker}
                                        name={selectedAsset.name}
                                        asset_type={selectedAsset.asset_type}
                                        currency={selectedAsset.currency}
                                        start_date={selectedAsset.start_date}
                                        end_date={selectedAsset.end_date}
                                        dividends={selectedAsset.dividends}
                                      />
                                    )}
                                    <span className="flex-1 truncate text-left">
                                      {field.value
                                        ? assetOptions.find(
                                            (asset) =>
                                              asset.ticker === field.value
                                          )?.label
                                        : "Select asset"}
                                    </span>
                                    <ChevronDown className="opacity-50 flex-shrink-0" />
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
                                            asset.ticker
                                          ) && asset.ticker !== field.value;

                                        return (
                                          <CommandItem
                                            key={asset.ticker}
                                            value={asset.ticker}
                                            disabled={isSelectedInOtherField}
                                            onSelect={() => {
                                              if (!isSelectedInOtherField) {
                                                field.onChange(asset.ticker);
                                              }
                                            }}
                                          >
                                            {asset.label}
                                            <Check
                                              className={cn(
                                                "ml-auto",
                                                asset.ticker === field.value
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
                      <Trash2 className="text-negative" />
                    </Button>
                  </div>
                ))}

                <div className="flex justify-between items-center">
                  <Button
                    type="button"
                    variant="outline"
                    onClick={() => append({ ticker: "", percentage: 0 })}
                  >
                    <Plus />
                    Add Asset
                  </Button>
                  <span
                    className={cn(
                      "font-medium text-sm",
                      totalPercentage === 100
                        ? "text-positive"
                        : "text-negative"
                    )}
                  >
                    Total: {totalPercentage}%
                  </span>
                </div>

                {/* <Button
                  type="button"
                  variant="outline"
                  onClick={() => append({ ticker: "", percentage: 0 })}
                >
                  <Plus />
                  Add Asset
                </Button> */}
              </CardContent>
            </Card>
          </div>

          {/* RIGHT SIDE */}
          <div className="flex flex-col flex-1 gap-4">
            <Card>
              <CardHeader>
                <div className="flex items-center gap-2">
                  <CardTitle>Backtest Mode</CardTitle>
                  <InfoTooltip content={tooltipTexts.mode} />
                </div>
                <CardDescription>
                  Choose your simulation complexity level
                </CardDescription>
              </CardHeader>
              <CardContent>
                <div className="grid grid-cols-1 gap-4">
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

                  <FormField
                    control={form.control}
                    name="export_excel"
                    render={({ field }) => (
                      <FormItem className="flex flex-row items-center justify-between rounded-lg shadow-s">
                        <div className="space-y-0.5">
                          <div className="flex items-center gap-2">
                            <FormLabel>Export to Excel</FormLabel>
                            <InfoTooltip content={tooltipTexts.exportExcel} />
                          </div>
                          <FormDescription>
                            Must be manually downloaded in results page.
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
                </div>
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
                          <div className="flex items-center gap-2">
                            <FormLabel>Currency</FormLabel>
                            <InfoTooltip content={tooltipTexts.currency} />
                          </div>
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
                                date <= currencyStart
                              }
                              captionLayout="dropdown"
                              defaultMonth={field.value}
                              startMonth={currencyStart}
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
                                date <= currencyStart
                              }
                              captionLayout="dropdown"
                              defaultMonth={field.value}
                              startMonth={currencyStart}
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
                          <div className="flex items-center gap-2">
                            <FormLabel>Initial Investment Amount</FormLabel>
                            <InfoTooltip
                              content={tooltipTexts.initialInvestment}
                            />
                          </div>
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
                        <div className="flex items-center gap-2">
                          <FormLabel>
                            Recurring Contributions (Optional)
                          </FormLabel>
                          <InfoTooltip
                            content={tooltipTexts.recurringInvestment}
                          />
                        </div>
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
                        <div className="flex items-center gap-2">
                          <FormLabel>Rebalancing Frequency</FormLabel>
                          <InfoTooltip
                            content={tooltipTexts.rebalanceFrequency}
                          />
                        </div>
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
                        <div className="flex items-center gap-2">
                          <FormLabel>Reinvest Dividends</FormLabel>
                          <InfoTooltip
                            content={tooltipTexts.reinvestDividends}
                          />
                        </div>
                        <FormDescription hidden={selectedMode !== "basic"}>
                          Dividends are automatically reinvested in basic mode.
                        </FormDescription>
                      </div>
                      <FormControl>
                        <Switch
                          disabled={selectedMode === "basic"}
                          checked={
                            selectedMode === "basic" ? true : field.value
                          }
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
                        <div className="flex items-center gap-2">
                          <FormLabel>Allow Fractional Shares</FormLabel>
                          <InfoTooltip
                            content={tooltipTexts.fractionalShares}
                          />
                        </div>
                        <FormDescription hidden={selectedMode !== "basic"}>
                          Fractional shares are automatically allowed in basic
                          mode.
                        </FormDescription>
                      </div>
                      <FormControl>
                        <Switch
                          disabled={selectedMode === "basic"}
                          checked={
                            selectedMode === "basic" ? true : field.value
                          }
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
          <Button type="submit" disabled={totalPercentage !== 100}>
            Run Backtest
          </Button>
        </div>
      </form>
    </Form>
  );
}
