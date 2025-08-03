import { useState } from 'react';
import { runBacktest } from './api/backtest';

function App() {
  const [result, setResult] = useState(null);

  const handleRun = async () => {
    const config = {
      start_date: "2020-01-01",
      end_date: "2020-01-02",
      target_weights: { AAPL: 0.6, GOOG: 0.4 },
      initial_investment: 10000,
      fractional_shares: true,
      reinvest_dividends: true,
      rebalance_frequency: "MONTHLY",
      mode: "BASIC",
      base_currency: "GBP",
      recurring: {
        amount: 500,
        frequency: "MONTHLY"
      }
    };

    const response = await runBacktest(config);
    setResult(response.result || response.message);
  };

  return (
    <div>
      <h1>Portfolio Backtester</h1>
      <button onClick={handleRun}>Run Backtest</button>
      {result && <pre>{JSON.stringify(result, null, 2)}</pre>}
    </div>
  );
}

export default App;

