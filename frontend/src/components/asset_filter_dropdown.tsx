import { Button } from "@/components/ui/button";
import { Filter } from "lucide-react";
import {
  DropdownMenu,
  DropdownMenuCheckboxItem,
  DropdownMenuContent,
  DropdownMenuLabel,
  DropdownMenuSeparator,
  DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu";

type Props = {
  enabledClasses: string[];
  onChange: (newEnabledClasses: string[]) => void;
  classLabels: Record<string, string>;
};

export function AssetClassFilterDropdown({
  enabledClasses,
  onChange,
  classLabels,
}: Props) {
  const assetClasses = Object.keys(classLabels);

  const toggleClass = (cls: string) => {
    if (enabledClasses.includes(cls)) {
      onChange(enabledClasses.filter((c) => c !== cls));
    } else {
      onChange([...enabledClasses, cls]);
    }
  };

  return (
    <DropdownMenu>
      <DropdownMenuTrigger asChild>
        <Button variant="outline">
          <Filter className="w-4 h-4"></Filter>
        </Button>
      </DropdownMenuTrigger>
      <DropdownMenuContent className="w-48">
        <DropdownMenuLabel>Asset Class</DropdownMenuLabel>
        <DropdownMenuSeparator />
        {assetClasses.map((cls) => (
          <DropdownMenuCheckboxItem
            key={cls}
            checked={enabledClasses.includes(cls)}
            onCheckedChange={() => toggleClass(cls)}
          >
            {classLabels[cls]}
          </DropdownMenuCheckboxItem>
        ))}
      </DropdownMenuContent>
    </DropdownMenu>
  );
}
