import {
  HoverCard,
  HoverCardContent,
  HoverCardTrigger,
} from "@/components/ui/hover-card";

import { Table, TableBody, TableCell, TableRow } from "@/components/ui/table";

type Props = {
  ticker: string;
  name: string;
  asset_type: string;
  start_date: string;
  end_date: string;
  currency: string;
  dividends: string;
};

export function AssetHoverCard({
  ticker,
  name,
  asset_type,
  start_date,
  // end_date,
  currency,
  dividends,
}: Props) {
  return (
    <HoverCard>
      <HoverCardTrigger asChild>
        <span className="inline-block text-muted-foreground hover:text-foreground">
          ?
        </span>
      </HoverCardTrigger>
      <HoverCardContent className="w-auto" align="start">
        <Table>
          <TableBody>
            {[
              { label: "Ticker", value: ticker },
              { label: "Name", value: name },
              { label: "Type", value: asset_type },
              { label: "Available", value: start_date },
              // { label: "End", value: end_date }, // Show end date if desired
              { label: "Currency", value: currency },
              { label: "Dividends", value: dividends },
            ].map((field) => (
              <TableRow key={field.label}>
                <TableCell>{field.label}</TableCell>
                <TableCell>{field.value}</TableCell>
              </TableRow>
            ))}
          </TableBody>
        </Table>
      </HoverCardContent>
    </HoverCard>
  );
}
