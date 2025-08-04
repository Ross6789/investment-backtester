import { useFieldArray } from "react-hook-form";
import type { Control, UseFormRegister } from "react-hook-form";
import { Trash2 } from "lucide-react";
import { Input } from "@/components/ui/input";
import { Button } from "@/components/ui/button";

import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";
import { FormField } from "../ui/form";

type PortfolioAssetsProps = {
  control: Control<any>;
  register: UseFormRegister<any>;
};

export function PortfolioSection({ control, register }: PortfolioAssetsProps) {
  const { fields, append, remove } = useFieldArray({
    control,
    name: "assets",
  });

  return <FormField />;
}
