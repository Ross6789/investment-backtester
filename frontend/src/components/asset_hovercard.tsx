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
  end_date,
  currency,
  dividends,
}: Props) {
  return (
    <HoverCard>
      <HoverCardTrigger asChild>
        <span className="inline-block text-muted-foreground hover:text-foreground">
          {/* <Eye size={16} /> */}?
        </span>
      </HoverCardTrigger>
      <HoverCardContent className="w-auto" align="start">
        <Table>
          <TableBody>
            <TableRow>
              <TableCell>Ticker</TableCell>
              <TableCell>{ticker}</TableCell>
            </TableRow>

            <TableRow>
              <TableCell>Name</TableCell>
              <TableCell>{name}</TableCell>
            </TableRow>

            <TableRow>
              <TableCell>Type</TableCell>
              <TableCell>{asset_type}</TableCell>
            </TableRow>

            <TableRow>
              <TableCell>Active</TableCell>
              <TableCell>{start_date}</TableCell>
            </TableRow>

            {/* <TableRow>
              <TableCell>End</TableCell>
              <TableCell>{end_date}</TableCell>
            </TableRow> */}

            <TableRow>
              <TableCell>Currency</TableCell>
              <TableCell>{currency}</TableCell>
            </TableRow>

            <TableRow>
              <TableCell>Dividends</TableCell>
              <TableCell>{dividends}</TableCell>
            </TableRow>
          </TableBody>
        </Table>
      </HoverCardContent>
    </HoverCard>
  );
}
