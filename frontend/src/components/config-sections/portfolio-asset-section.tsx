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

type PortfolioAssetsSectionProps = {
  control: Control<any>;
  register: UseFormRegister<any>;
};

export function PortfolioAssets({
  control,
  register,
}: PortfolioAssetsSectionProps) {
  const { fields, append, remove } = useFieldArray({
    control,
    name: "assets",
  });

  return (
    <Card>
      <CardHeader>
        <CardTitle>Portfolio Assets</CardTitle>
        <CardDescription>
          Select assets and assign weights (must total 100%)
        </CardDescription>
      </CardHeader>
      <CardContent>
        <div className="space-y-4">
          {fields.map((field, index) => (
            <div
              key={field.id}
              className="grid grid-cols-12 gap-4 items-center"
            >
              <div className="col-span-6">
                <Input
                  placeholder={`Asset ${index + 1}`}
                  {...register(`assets.${index}.name`)}
                />
              </div>
              <div className="col-span-4">
                <Input
                  placeholder="Weight (%)"
                  type="number"
                  {...register(`assets.${index}.weight`, {
                    valueAsNumber: true,
                  })}
                />
              </div>
              <div className="col-span-1 text-sm text-center font-medium">
                %
              </div>
              <div className="col-span-1">
                <Button
                  variant="ghost"
                  size="icon"
                  type="button"
                  onClick={() => remove(index)}
                >
                  <Trash2 className="w-4 h-4" />
                </Button>
              </div>
            </div>
          ))}

          <Button type="button" onClick={() => append({ name: "", weight: 0 })}>
            + Add Asset
          </Button>
        </div>
      </CardContent>
    </Card>
  );
}
