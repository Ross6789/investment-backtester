export const tooltipTexts = {
  portfolioAssets: "Choose which stocks, ETFs, crypto or other assets to include in your portfolio.\nThe weights should add up to 100%.",
  mode: "Basic: Simple, fast calculations assuming perfect trading conditions.\nRealistic: Detailed modeling with trading restrictions, order tracking, and dividend handling.",
  currency: "Choose the currency for your investment amounts and results.",
  initialInvestment: "The amount of money you start investing with.",
  recurringInvestment: "Regular additional investments made over time.",
  rebalanceFrequency: "How often to adjust your portfolio back to target weights.",
  reinvestDividends: "Choose whether to use dividends to buy more shares or take as cash.",
  fractionalShares:"Whether you can buy partial shares (e.g., 0.5 shares of a stock).",
};

export const metricHoverTexts = {
  cagr: {
    title: "Compound Annual Growth Rate (CAGR)",
    description : "This shows how much your investment grew each year on average over a certain period. It smooths out the ups and downs to give you a simple number that tells you the steady growth rate, making it easier to understand how your money increased over time.",
    ratings: {
      "< 0 %": "Losing value",
      "0 - 3 %": "Weak growth",
      "3 - 7 %": "Moderate growth",
      "7 - 12 %": "Strong growth",
      "12 % +": "Exceptional growth",
    }
    
  },
  volatility:{
    title: "Volatility",
    description : "This measures how much the value of your investment goes up and down over time. A higher volatility means bigger swings in price, which can mean more risk but also more chance for reward. Lower volatility means the investment price is steadier and less risky.",
    ratings: {
      "30 % +": "Very high",
      "20 to 30 %": "High",
      "10 to 20 %": "Moderate",
      "5 to 10 %": "Low",
      "< 5 %": "Very low",
    }
  },
    max_drawdown:{
    title: "Maximum Drawdown",
    description : "This shows the biggest drop in your investment's value from its highest point to its lowest point during a certain time. It helps you understand the worst loss you might face before the value starts going back up.",
    ratings : {"< -35 %": "Very poor",
      "-35 % to -20 %": "Poor",
      "-20 % to -10 %": "Moderate",
      "-10 % to -5 %": "Good",
      "> -5 %": "Excellent"
}
  },
    sharpe:{
    title: "Sharpe Ratio",
    description : "This tells you how much extra return you're getting for the amount of risk you're taking with your investment. A higher Sharpe Ratio means you’re being rewarded well for the risk, while a lower number means the risk might not be worth it.",
    ratings : {"<0.0": "Very Poor",
      "0.0 to 0.5": "Poor",
      "0.5 to 1.0": "Moderate",
      "1.0 to 2.0": "Good",
      "2.0+": "Excellent",
    }
  }
}