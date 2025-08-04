import { zodResolver } from "@hookform/resolvers/zod";
import { useForm } from "react-hook-form";
import { z } from "zod";

import {
  Card,
  CardAction,
  CardContent,
  CardDescription,
  CardFooter,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";

import { Button } from "@/components/ui/button";
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
      .max(new Date("2025-06-01"), {
        message: "Date must be before June 1 2025",
      }),
    end_date: z
      .date()
      .min(new Date("1970-01-01"), { message: "Date must be after Jan 1 1970" })
      .max(new Date("2025-06-01"), {
        message: "Date must be before June 1 2025",
      }),
    strategy: z.object({
      fractional_shares: z.boolean().default(false).optional(),
      reinvest_dividends: z.boolean().default(false).optional(),
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
  const form = useForm<z.infer<typeof formSchema>>({
    resolver: zodResolver(formSchema),
    defaultValues: {
      mode: "basic",
      start_date: new Date("2020-01-01"),
      end_date: new Date("2025-06-01"),
      strategy: {
        fractional_shares: true,
        reinvest_dividends: true,
        rebalance_frequency: "never",
      },
    },
  });

  // 2. Define a submit handler.
  function onSubmit(values: z.infer<typeof formSchema>) {
    // Do something with the form values.
    // ✅ This will be type-safe and validated.
    console.log(values);
  }

  return (
    <Form {...form}>
      <form onSubmit={form.handleSubmit(onSubmit)} className="space-y-8">
        <div className="min-h-screen m-4 p-4 grid grid-cols-1 lg:grid-cols-2 gap-4 bg-amber-400">
          {/* LEFT SIDE */}
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
                        <SelectTrigger>
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

          {/* RIGHT SIDE */}
          <div className="flex flex-col gap-4">
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
                          <SelectTrigger>
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
                <div className="grid grid-cols-2 gap-4 bg-blue-300">
                  <FormField
                    control={form.control}
                    name="strategy.rebalance_frequency"
                    render={({ field }) => (
                      <FormItem className="col-span-1">
                        <Select
                          onValueChange={field.onChange}
                          defaultValue={field.value}
                        >
                          <FormLabel>Rebalancing Frequency</FormLabel>
                          <FormControl>
                            <SelectTrigger>
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
                    name="strategy.rebalance_frequency"
                    render={({ field }) => (
                      <FormItem className="col-span-1">
                        <Select
                          onValueChange={field.onChange}
                          defaultValue={field.value}
                        >
                          <FormLabel>Rebalancing Frequency</FormLabel>
                          <FormControl>
                            <SelectTrigger>
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
                    name="strategy.rebalance_frequency"
                    render={({ field }) => (
                      <FormItem className="col-span-2 items-center">
                        <Select
                          onValueChange={field.onChange}
                          defaultValue={field.value}
                        >
                          <FormLabel>Rebalancing Frequency</FormLabel>
                          <FormControl>
                            <SelectTrigger>
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
                          <SelectTrigger>
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

          {/* Full-width row for submit button */}
          <div className="lg:col-span-2">
            <Button type="submit">Submit</Button>
          </div>
        </div>
      </form>
    </Form>
  );
}

//   return (
//     <Form {...form}>
//       <form onSubmit={form.handleSubmit(onSubmit)} className="w-full space-y-6">
//         <div>
//           <h3 className="mb-4 text-lg font-medium">Email Notifications</h3>
//           <div className="space-y-4">
//             <FormField
//               control={form.control}
//               name="marketing_emails"
//               render={({ field }) => (
//                 <FormItem className="flex flex-row items-center justify-between rounded-lg border p-3 shadow-sm">
//                   <div className="space-y-0.5">
//                     <FormLabel>Marketing emails</FormLabel>
//                     <FormDescription>
//                       Receive emails about new products, features, and more.
//                     </FormDescription>
//                   </div>
//                   <FormControl>
//                     <Switch
//                       checked={field.value}
//                       onCheckedChange={field.onChange}
//                     />
//                   </FormControl>
//                 </FormItem>
//               )}
//             />
//             <FormField
//               control={form.control}
//               name="security_emails"
//               render={({ field }) => (
//                 <FormItem className="flex flex-row items-center justify-between rounded-lg border p-3 shadow-sm">
//                   <div className="space-y-0.5">
//                     <FormLabel>Security emails</FormLabel>
//                     <FormDescription>
//                       Receive emails about your account security.
//                     </FormDescription>
//                   </div>
//                   <FormControl>
//                     <Switch
//                       checked={field.value}
//                       onCheckedChange={field.onChange}
//                       disabled
//                       aria-readonly
//                     />
//                   </FormControl>
//                 </FormItem>
//               )}
//             />
//           </div>
//         </div>
//         <Button type="submit">Submit</Button>
//       </form>
//     </Form>
//   )
// }
