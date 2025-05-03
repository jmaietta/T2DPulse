import requests
from config import FINNHUB_API_KEY

BASE_URL = "https://finnhub.io/api/v1"

def fetch_eod_price(ticker):
    url = f"{BASE_URL}/quote?symbol={ticker}&token={FINNHUB_API_KEY}"
    res = requests.get(url).json()
    return res.get("c")

def fetch_market_cap(ticker):
    url = f"{BASE_URL}/stock/profile2?symbol={ticker}&token={FINNHUB_API_KEY}"
    res = requests.get(url).json()
    return res.get("marketCapitalization")