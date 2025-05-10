"""
Calculate AdTech Market Cap from Provided Ticker Values
"""

# Ticker values provided by user
adtech_tickers = {
    "APP": 111_173_569_309,
    "APPS": 436_828_271,
    "CRTO": 1_512_417_571,
    "DV": 2_167_452_542,
    "GOOGL": 1_861_565_777_500,
    "META": 1_489_713_579_676,
    "MGNI": 2_130_768_603,
    "PUBM": 536_115_338,
    "TTD": 34_921_076_418
}

# Calculate total market cap
total_market_cap = sum(adtech_tickers.values())

# Convert to billions for readability
total_market_cap_billions = total_market_cap / 1_000_000_000

print(f"AdTech Market Cap Calculation")
print(f"=============================")
print(f"Individual Ticker Market Caps:")
for ticker, market_cap in adtech_tickers.items():
    market_cap_billions = market_cap / 1_000_000_000
    print(f"  {ticker}: ${market_cap_billions:.2f} billion")

print(f"\nTotal AdTech Market Cap: ${total_market_cap_billions:.2f} billion")
print(f"Total AdTech Market Cap: ${total_market_cap_billions/1000:.2f} trillion")

# Calculate how much of GOOGL and META should be attributed to AdTech
# Note: These companies have multiple business units, so only a portion should be counted
googl_percentage = 0.81  # 81% of Google's revenue is from advertising
meta_percentage = 0.98   # 98% of Meta's revenue is from advertising

# Recalculate with percentages applied
adjusted_market_caps = adtech_tickers.copy()
adjusted_market_caps["GOOGL"] = adtech_tickers["GOOGL"] * googl_percentage
adjusted_market_caps["META"] = adtech_tickers["META"] * meta_percentage

adjusted_total = sum(adjusted_market_caps.values())
adjusted_total_billions = adjusted_total / 1_000_000_000

print(f"\nAdjusted Calculation (applying business segment percentages):")
print(f"  GOOGL: Counting {googl_percentage*100:.0f}% (advertising business only)")
print(f"  META: Counting {meta_percentage*100:.0f}% (advertising business only)")
print(f"Adjusted AdTech Market Cap: ${adjusted_total_billions:.2f} billion")
print(f"Adjusted AdTech Market Cap: ${adjusted_total_billions/1000:.2f} trillion")