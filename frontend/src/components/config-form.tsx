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

import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";

const formSchema = z.object({
  mode: z.enum(["basic", "realistic"]),
});

export function ProfileForm() {
  // 1. Define your form.
  const form = useForm<z.infer<typeof formSchema>>({
    resolver: zodResolver(formSchema),
    defaultValues: {
      mode: "basic",
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

          {/* Full-width row for submit button */}
          <div className="lg:col-span-2">
            <Button type="submit">Submit</Button>
          </div>
        </div>
      </form>
    </Form>
  );
}
