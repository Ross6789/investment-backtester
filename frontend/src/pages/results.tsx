import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";

import { useLocation, useNavigate } from "react-router-dom";
import { useEffect } from "react";

export function ResultsPage() {
  const location = useLocation();
  const navigate = useNavigate();
  const backtestResult = location.state?.backtestResult;

  useEffect(() => {
    // If no result data (e.g. direct access to /results), redirect to settings
    if (!backtestResult) {
      navigate("/settings");
    }
  }, [backtestResult, navigate]);

  if (!backtestResult) return null;

  return (
    <div className="m-4 p-4 flex lg:flex-row flex-col gap-4">
      {/* LEFT SIDE - add items-star to make box shrink */}
      <div className="flex-1">
        <Card>
          <CardHeader>
            <CardTitle>Result</CardTitle>
            <CardDescription>JSON results</CardDescription>
          </CardHeader>
          <CardContent>
            <pre>{JSON.stringify(backtestResult, null, 2)}</pre>
          </CardContent>
        </Card>
      </div>

      {/* RIGHT SIDE */}
      <div className="flex flex-col flex-1 gap-4">
        <Card>
          <CardHeader>
            <CardTitle>Backtest Mode</CardTitle>
            <CardDescription>
              Choose your simulation complexity level
            </CardDescription>
          </CardHeader>
          <CardContent></CardContent>
        </Card>
        <Card>
          <CardHeader>
            <CardTitle>Investment Settings</CardTitle>
          </CardHeader>
          <CardContent></CardContent>
        </Card>
        <Card>
          <CardHeader>
            <CardTitle>Strategy Settings</CardTitle>
          </CardHeader>
          <CardContent className="space-y-4"></CardContent>
        </Card>
      </div>
    </div>
  );
}
