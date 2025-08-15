from datetime import date

# Rounding constants
PRICE_PRECISION = 4
CURRENCY_PRECISION = 2
PERCENTAGE_PRECISION = 3
GENERAL_PRECISION = 4

# Currency start dates
CURRENCY_START_DATES = {
    "GBP": date.fromisoformat("1970-01-01"),
    "USD": date.fromisoformat("1970-01-01"),
    "EUR": date.fromisoformat("1999-01-03")
}