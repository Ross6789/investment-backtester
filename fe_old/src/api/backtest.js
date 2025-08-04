export async function runBacktest(inputData) {
  const response = await fetch("/api/backtest", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(inputData),
  });

  const result = await response.json();
  return result;
}
