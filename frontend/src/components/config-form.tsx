import { useForm, useFieldArray } from "react-hook-form";
import { zodResolver } from "@hookform/resolvers/zod";
import { z } from "zod";
import { PortfolioAssets } from "@/components/config-sections/portfolio-asset-section";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";

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

const schema = z.object({
  target_weights: z.record(z.string(), z.number().min(0).max(100)),
});

export function ConfigForm() {
  const form = useForm<z.infer<typeof schema>>({
    resolver: zodResolver(schema),
    defaultValues: {
      target_weights: {
        AAPL: 50,
        GOOG: 50,
      },
    },
  });

  // 2. Define a submit handler.
  function onSubmit(data: z.infer<typeof schema>) {
    // Do something with the form values.
    // ✅ This will be type-safe and validated.
    console.log(data);
  }

  return (
    <div className="min-h-screen m-4 grid grid-cols-1 lg:grid-cols-2 gap-4">
      <Form {...form}>
        <form
          onSubmit={form.handleSubmit(onSubmit)}
          className="space-y-8 contents"
        >
          <PortfolioAssets control={form.control} register={form.register} />

          <div className="flex flex-col gap-4">
            <Card>
              <CardHeader>
                <CardTitle>Backtest Mode</CardTitle>
                <CardDescription>
                  Choose your simulation complexity level
                </CardDescription>
              </CardHeader>
              <CardContent>Card content</CardContent>
            </Card>
            <Card>
              <CardHeader>
                <CardTitle>Investment Settings</CardTitle>
              </CardHeader>
              <CardContent>
                <p>Card Content</p>
              </CardContent>
            </Card>
            <Card>
              <CardHeader>
                <CardTitle>Advanced Settings</CardTitle>
              </CardHeader>
              <CardContent>
                <p>Card Content</p>
              </CardContent>
            </Card>
          </div>

          {/* <div className="h-full rounded-lg bg-gray-400 shadow">
            <FormField
              control={form.control}
              name="username"
              render={({ field }) => (
                <FormItem>
                  <FormLabel>Username</FormLabel>
                  <FormControl>
                    <Input placeholder="shadcn" {...field} />
                  </FormControl>
                  <FormDescription>
                    This is your public display name.
                  </FormDescription>
                  <FormMessage />
                </FormItem>
              )}
            />
            <Button type="submit">Submit</Button>
          </div>
          <div className="h-full rounded-lg bg-blue-400 shadow">
            <FormField
              control={form.control}
              name="username"
              render={({ field }) => (
                <FormItem>
                  <FormLabel>Username</FormLabel>
                  <FormControl>
                    <Input placeholder="shadcn" {...field} />
                  </FormControl>
                  <FormDescription>
                    This is your public display name.
                  </FormDescription>
                  <FormMessage />
                </FormItem>
              )}
            />
            <Button type="submit">Submit</Button>
          </div> */}
        </form>
      </Form>
    </div>
  );
}
