from config import SECTORS
from fetcher import fetch_market_cap

def compute_sector_value(sector_tickers):
    total = 0
    for ticker in sector_tickers:
        try:
            market_cap = fetch_market_cap(ticker)
            if market_cap:
                total += market_cap
        except:
            continue
    return round(total, 2)

def calculate_ema(values, span=20):
    ema_values = []
    alpha = 2 / (span + 1)
    for i, val in enumerate(values):
        if i == 0:
            ema_values.append(val)
        else:
            ema = alpha * val + (1 - alpha) * ema_values[-1]
            ema_values.append(ema)
    return ema_values