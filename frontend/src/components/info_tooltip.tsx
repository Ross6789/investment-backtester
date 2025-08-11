import { Info } from "lucide-react";

import {
  Tooltip,
  TooltipContent,
  TooltipTrigger,
} from "@/components/ui/tooltip";

export function InfoTooltip({ content }: { content: string }) {
  return (
    <Tooltip>
      <TooltipTrigger className="text-muted-foreground">
        <Info size={16}></Info>
      </TooltipTrigger>
      <TooltipContent style={{ whiteSpace: "pre-wrap" }}>
        <p>{content}</p>
      </TooltipContent>
    </Tooltip>
  );
}
