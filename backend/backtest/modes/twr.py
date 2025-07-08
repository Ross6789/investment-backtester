from datetime import date


# --- Ticker Lookup & Filtering ---

def is_fully_active_date(self, date: date) -> bool:
    day_info = self.master_calendar.get(date, {})
    return day_info.get("active_tickers", set()) == self.tickers