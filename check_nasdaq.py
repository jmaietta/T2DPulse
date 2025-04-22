from app import nasdaq_data, calculate_sector_sentiment
import json

print('Latest NASDAQ data:')
if not nasdaq_data.empty:
    latest = nasdaq_data.sort_values('date', ascending=False).iloc[0]
    print(f'Value: {latest["value"]:.2f}, Gap: {latest["gap_pct"]:.2f}% from 20-day EMA')

# Get the macros dictionary being passed to sentiment_engine
print("\nSector sentiment calculation:")
scores = calculate_sector_sentiment()

print("\nAdTech sector score details:")
adtech_score = [s for s in scores if s['sector'] == 'AdTech']
print(json.dumps(adtech_score, indent=2))