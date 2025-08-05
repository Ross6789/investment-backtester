import { zodResolver } from "@hookform/resolvers/zod";
import { useForm } from "react-hook-form";
import { optional, z } from "zod";

import { format } from "date-fns";
import { CalendarIcon } from "lucide-react";

import { toast } from "sonner";

import { cn } from "@/lib/utils";
import { Button } from "@/components/ui/button";
import { Calendar } from "@/components/ui/calendar";

import {
  Popover,
  PopoverContent,
  PopoverTrigger,
} from "@/components/ui/popover";

import {
  Card,
  CardAction,
  CardContent,
  CardDescription,
  CardFooter,
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

const formSchema = z
  .object({
    mode: z.enum(["basic", "realistic"]),
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
  });

export function ProfileForm() {
  // 1. Define your form.
  const form = useForm({
    resolver: zodResolver(formSchema),
    defaultValues: {
      mode: "basic",
      start_date: new Date("2020-01-01"),
      end_date: new Date("2025-05-31"),
      initial_investment: undefined,
      recurring_investment: undefined,
      strategy: {
        fractional_shares: true,
        reinvest_dividends: true,
        rebalance_frequency: "never",
      },
    },
  });

  // 2. Define a submit handler.
  function onSubmit(values: z.infer<typeof formSchema>) {
    if (
      !values.recurring_investment ||
      values.recurring_investment.frequency === "never" ||
      !values.recurring_investment.amount
    ) {
      values.recurring_investment = null;
    }
    // Do something with the form values.
    // ✅ This will be type-safe and validated.

    console.log(values);
  }

  return (
    <Form {...form}>
      <form onSubmit={form.handleSubmit(onSubmit)} className="space-y-8">
        <div className="m-4 p-4 flex lg:flex-row flex-col gap-4">
          {/* LEFT SIDE - add items-star to make box shrink */}
          <div className="flex-1">
            <FormField
              control={form.control}
              name="mode"
              render={({ field }) => (
                <Card>
                  <CardHeader>
                    <CardTitle>Backtest Mode</CardTitle>
                    <CardDescription>
                      Choose your simulation complexity level
                    </CardDescription>
                  </CardHeader>
                  <CardContent>
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
                  </CardContent>
                </Card>
              )}
            />
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
                            <Input
                              type="number"
                              placeholder="Enter amount"
                              step="100"
                              {...field}
                              value={
                                field.value === undefined ||
                                field.value === null
                                  ? ""
                                  : Number(field.value)
                              }
                              onChange={(e) => {
                                const raw = e.target.value;
                                field.onChange(raw === "" ? undefined : raw);
                              }}
                            />
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
                          <Input
                            type="number"
                            placeholder="Enter amount"
                            step="10"
                            {...field}
                            value={
                              field.value === undefined || field.value === null
                                ? ""
                                : Number(field.value)
                            }
                            onChange={(e) => {
                              const raw = e.target.value;
                              field.onChange(raw === "" ? undefined : raw);
                            }}
                          />
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
                          defaultValue={field.value}
                        >
                          <FormLabel>Frequency</FormLabel>
                          <FormControl>
                            <SelectTrigger className="w-full">
                              <SelectValue placeholder="Select rebalancing frequency" />
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
