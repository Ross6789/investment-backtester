import type { LucideIcon } from "lucide-react";

import {
  HoverCard,
  HoverCardContent,
  HoverCardTrigger,
} from "@/components/ui/hover-card";

import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";

type Props = {
  icon: LucideIcon;
  title: string;
  description: string;
  value: string;
  valueRatingClass: string;
  ratings: {
    [range: string]: string;
  };
};

export function MetricHoverCard({
  icon,
  title,
  description,
  value,
  valueRatingClass,
  ratings,
}: Props) {
  const Icon = icon; // Capitalize first letter to treat as component
  return (
    <HoverCard>
      <HoverCardTrigger asChild>
        {/* {<Icon size={16} />} */}
        <span className="inline-block text-muted-foreground hover:text-foreground">
          <Icon size={16} />
        </span>
      </HoverCardTrigger>
      <HoverCardContent className="w-120 ">
        <div className="flex justify-between gap-8">
          <div className="space-y-2">
            <h4 className="text-lg font-semibold">{title}</h4>
            <p className="text-sm">{description}</p>
            <div className="flex flex-col items-center justify-center gap-2 my-6">
              <p className="text-sm text-muted-foreground">This portfolio : </p>
              <h3 className={`text-3xl font-semibold ${valueRatingClass}`}>
                {value}
              </h3>
            </div>
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead className="text-right w-1/2 pr-6">Range</TableHead>
                  <TableHead className="w-1/2 pl-4">Rating</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {Object.entries(ratings).map(([range, caption]) => (
                  <TableRow key={range}>
                    <TableCell className="text-right pr-6">{range}</TableCell>
                    <TableCell className="pl-4">{caption}</TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          </div>
        </div>
      </HoverCardContent>
    </HoverCard>
  );
}
