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

const formSchema = z.object({
  username: z.string().min(2, {
    message: "Username must be at least 2 characters.",
  }),
});

export function SettingsForm() {
  // 1. Define your form.
  const form = useForm<z.infer<typeof formSchema>>({
    resolver: zodResolver(formSchema),
    defaultValues: {
      username: "",
    },
  });

  // 2. Define a submit handler.
  function onSubmit(values: z.infer<typeof formSchema>) {
    // Do something with the form values.
    // ✅ This will be type-safe and validated.
    console.log(values);
  }

  return (
    <div className="grid grid-cols-1 lg:grid-cols-2 gap-4 p-4 items-start">
      <Card>
        <CardHeader>
          <CardTitle>Portfolio Assets</CardTitle>
          <CardDescription>
            Select assets and assign weights (must total 100%)
          </CardDescription>
        </CardHeader>
        <CardContent>
          <div className="grid grid-cols-12 gap-4 items-center">
            <div className="col-span-6 bg-fuchsia-200">combobox</div>
            <div className="col-span-4 bg-amber-200">input</div>
            <div className="col-span-1 bg-blue-300">%</div>
            <div className="col-span-1 bg-green-300">icon</div>
          </div>
        </CardContent>
      </Card>

      {/* Right side with variable height boxes */}
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
    </div>

    // <div className="min-h-screen m-4 grid grid-cols-1 lg:grid-cols-2 gap-4">
    //   <Form {...form}>
    //     <form
    //       onSubmit={form.handleSubmit(onSubmit)}
    //       className="space-y-8 contents"
    //     >
    //       <div className="h-full rounded-lg bg-gray-400 shadow">
    //         <FormField
    //           control={form.control}
    //           name="username"
    //           render={({ field }) => (
    //             <FormItem>
    //               <FormLabel>Username</FormLabel>
    //               <FormControl>
    //                 <Input placeholder="shadcn" {...field} />
    //               </FormControl>
    //               <FormDescription>
    //                 This is your public display name.
    //               </FormDescription>
    //               <FormMessage />
    //             </FormItem>
    //           )}
    //         />
    //         <Button type="submit">Submit</Button>
    //       </div>
    //       <div className="h-full rounded-lg bg-blue-400 shadow">
    //         <FormField
    //           control={form.control}
    //           name="username"
    //           render={({ field }) => (
    //             <FormItem>
    //               <FormLabel>Username</FormLabel>
    //               <FormControl>
    //                 <Input placeholder="shadcn" {...field} />
    //               </FormControl>
    //               <FormDescription>
    //                 This is your public display name.
    //               </FormDescription>
    //               <FormMessage />
    //             </FormItem>
    //           )}
    //         />
    //         <Button type="submit">Submit</Button>
    //       </div>
    //     </form>
    //   </Form>
    // </div>
  );
}
