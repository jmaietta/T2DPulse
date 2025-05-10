# T2D Pulse API Dependencies

This document provides detailed information about the external APIs used by T2D Pulse and how they are integrated into the codebase.

## Overview of API Integrations

T2D Pulse relies on several external APIs to collect economic data, market information, and financial metrics. These APIs provide the authentic data required for market cap calculations, sector sentiment analysis, and economic indicators.

## Required API Keys

The following API keys must be configured as environment variables:

| API Name | Environment Variable | Purpose |
|----------|---------------------|---------|
| Finnhub | `FINNHUB_API_KEY` | Real-time stock data and market information |
| Alpha Vantage | `ALPHAVANTAGE_API_KEY` | Stock market data, technical indicators |
| FRED (Federal Reserve) | `FRED_API_KEY` | Economic indicators (GDP, inflation, etc.) |
| BEA (Bureau of Economic Analysis) | `BEA_API_KEY` | GDP and economic growth data |
| BLS (Bureau of Labor Statistics) | `BLS_API_KEY` | Employment and labor market data |
| NASDAQ Data Link | `NASDAQ_DATA_LINK_API_KEY` | Financial and economic datasets |
| Polygon.io | `POLYGON_API_KEY` | Comprehensive stock market data | 

## API Integration Details

### Finnhub API
**Primary use**: Real-time stock data and market information

**Key files**:
- `batch_ticker_collector.py` - Collects ticker data
- `authentic_marketcap_updater.py` - Updates market cap data

**Sample usage**:
```python
import finnhub
import os

# Setup client
finnhub_client = finnhub.Client(api_key=os.environ.get("FINNHUB_API_KEY"))

# Get stock quote
quote = finnhub_client.quote('AAPL')

# Get company profile
profile = finnhub_client.company_profile2(symbol='AAPL')
```

**Usage limits**: 60 API calls/minute for free tier

### Alpha Vantage API
**Primary use**: Historical stock data and financial metrics

**Key files**:
- `data_helpers.py` - Contains helper functions for Alpha Vantage API
- `check_ticker_coverage.py` - Uses Alpha Vantage for ticker validation

**Sample usage**:
```python
import requests
import os

API_KEY = os.environ.get("ALPHAVANTAGE_API_KEY")
url = f'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol=MSFT&apikey={API_KEY}'
r = requests.get(url)
data = r.json()
```

**Usage limits**: 5 API calls per minute, 500 calls per day for free tier

### FRED API
**Primary use**: Economic indicators (GDP, unemployment, inflation, etc.)

**Key files**:
- `app.py` - Contains `fetch_fred_data()` function
- `config.py` - Contains FRED series IDs

**Sample usage**:
```python
import requests
import os
import pandas as pd

def fetch_fred_data(series_id, start_date=None, end_date=None):
    """Fetch data from FRED API for a given series"""
    api_key = os.environ.get("FRED_API_KEY")
    url = f"https://api.stlouisfed.org/fred/series/observations"
    params = {
        "series_id": series_id,
        "api_key": api_key,
        "file_type": "json",
        "frequency": "m",  # Monthly data
        "observation_start": start_date or "2020-01-01",
        "observation_end": end_date or "9999-12-31"
    }
    
    response = requests.get(url, params=params)
    data = response.json()
    
    # Convert to DataFrame
    df = pd.DataFrame(data["observations"])
    df = df[["date", "value"]].copy()
    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    return df
```

**Usage limits**: Generous limits for free tier, typically not a concern

### BEA API
**Primary use**: GDP and economic analysis data

**Key files**:
- `app.py` - Contains `fetch_bea_data()` function

**Sample usage**:
```python
import requests
import os
import json

def fetch_bea_data(table_name, frequency, start_year, end_year):
    """Fetch data from BEA API"""
    api_key = os.environ.get("BEA_API_KEY")
    url = "https://apps.bea.gov/api/data"
    params = {
        "UserID": api_key,
        "method": "GetData",
        "datasetname": "NIPA",
        "TableName": table_name,
        "Frequency": frequency,
        "Year": f"{start_year},{end_year}",
        "ResultFormat": "JSON"
    }
    
    response = requests.get(url, params=params)
    return response.json()
```

**Usage limits**: 100 queries per day for free key

### BLS API
**Primary use**: Employment and inflation data

**Key files**:
- `app.py` - Contains `fetch_bls_data()` function

**Sample usage**:
```python
import requests
import os
import json

def fetch_bls_data(series_id, start_year, end_year):
    """Fetch data from BLS API"""
    api_key = os.environ.get("BLS_API_KEY")
    url = "https://api.bls.gov/publicAPI/v2/timeseries/data/"
    
    headers = {'Content-type': 'application/json'}
    data = json.dumps({
        "seriesid": [series_id],
        "startyear": str(start_year),
        "endyear": str(end_year),
        "registrationkey": api_key
    })
    
    response = requests.post(url, data=data, headers=headers)
    return response.json()
```

**Usage limits**: 500 queries per day with registered key

### NASDAQ Data Link API
**Primary use**: Financial and economic datasets

**Key files**:
- `data_helpers.py` - Contains helper functions

**Sample usage**:
```python
import os
import nasdaqdatalink

# Set API key
nasdaqdatalink.ApiConfig.api_key = os.environ.get("NASDAQ_DATA_LINK_API_KEY")

# Get data
data = nasdaqdatalink.get("FRED/GDP")
```

**Usage limits**: 50 calls per day for free tier

### Polygon.io API
**Primary use**: Comprehensive stock market data

**Key files**:
- `check_polygon_details.py` - Validates stock details
- `direct_ticker_check.py` - Direct ticker validation

**Sample usage**:
```python
import requests
import os

def get_ticker_details(ticker):
    """Get details for a specific ticker"""
    api_key = os.environ.get("POLYGON_API_KEY")
    url = f"https://api.polygon.io/v3/reference/tickers/{ticker}"
    params = {
        "apiKey": api_key
    }
    
    response = requests.get(url, params=params)
    return response.json()
```

**Usage limits**: 5 API calls per minute for free tier

## API Fallback Strategy

T2D Pulse implements fallback strategies in case of API failures:

1. If Finnhub API fails, the system attempts to use Alpha Vantage API for stock data
2. If real-time data cannot be retrieved, historical cached data is used
3. The `data_cache.py` module manages caching and retrieval of data when APIs are unavailable

## Best Practices for API Key Management

1. Always store API keys in environment variables, never in code
2. Use separate API keys for development and production environments
3. Monitor API usage to stay within limits
4. Implement proper error handling for API failures

## API Call Optimization

T2D Pulse optimizes API calls through:

1. Caching results to minimize repeat calls
2. Batch processing requests when possible
3. Scheduling heavy API calls during low-usage periods
4. Using bulk endpoints when available

This documentation should help with migrating the T2D Pulse application to a new platform by understanding the API dependencies and integration points.