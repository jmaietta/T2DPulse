# T2D Pulse Data Collection Architecture

This document provides a comprehensive overview of the data collection and processing architecture for T2D Pulse, including the background processes, data sources, and data flow.

## Overview

T2D Pulse relies on a robust data collection system that:

1. Regularly fetches stock data from financial APIs
2. Collects economic indicators from authoritative sources
3. Processes and normalizes data for sector sentiment analysis
4. Maintains historical records for trend analysis
5. Ensures data authenticity and reliability

## Key Components

### Background Data Collector

**Core file**: `background_data_collector.py`

This is the primary background service responsible for collecting ticker data at regular intervals. It runs independently from the main application and updates the database with fresh data.

**Key features**:
- Scheduled collection of ticker data
- Validation and normalization of financial data
- Error recovery and retry mechanisms
- Logging of data collection activities

**Usage**:
```bash
python background_data_collector.py --check 15 --update 30
```
This runs the collector with:
- 15-minute interval for checking data freshness
- 30-minute interval for full data updates

### Batch Ticker Collector

**Core file**: `batch_ticker_collector.py`

This component handles bulk collection of ticker data from multiple sources. It's designed for efficiency when fetching data for many tickers at once.

**Key methods**:
- `collect_tickers_batch()`: Collects data for a batch of tickers
- `validate_ticker_data()`: Validates received ticker data
- `store_ticker_data()`: Stores validated data in the database

### Market Cap Calculation

**Core files**:
- `authentic_marketcap_reader.py`
- `authentic_marketcap_updater.py`
- `calculate_adtech_marketcap.py`

These components handle the calculation of market capitalization for sectors based on authentic ticker data. They ensure accurate representation of sector sizes.

**Market cap calculation process**:
1. Fetch latest ticker prices from financial APIs
2. Retrieve authentic share count data
3. Calculate market cap = share count × price
4. Aggregate by sector based on ticker classifications
5. Store in the database for historical records

### Sector Sentiment Analysis

**Core files**:
- `sentiment_engine.py`
- `ema_calculator.py`

These components analyze economic indicators and market data to generate sentiment scores for each technology sector.

**Sentiment calculation process**:
1. Collect all relevant economic indicators
2. Apply sector-specific weighting to indicators
3. Calculate EMA (Exponential Moving Average) for smoothing
4. Generate sentiment score on a 0-100 scale
5. Persist scores in the sector history database

## Data Flow

The overall data flow in the T2D Pulse system works as follows:

```
                 +----------------+
                 | Financial APIs |
                 +----------------+
                         |
                         v
+------------------+  +-----------------------+  +---------------+
| Background Data  |->| Batch Ticker          |->| Market Cap    |
| Collector        |  | Collector             |  | Calculation   |
+------------------+  +-----------------------+  +---------------+
                                                        |
                 +----------------+                     |
                 | Economic APIs  |                     |
                 +----------------+                     |
                         |                              |
                         v                              v
                  +---------------------+    +--------------------+
                  | Economic Indicator  |    | Sector History     |
                  | Collection          |    | Database           |
                  +---------------------+    +--------------------+
                         |                              |
                         v                              v
                  +---------------------+    +--------------------+
                  | Sentiment Engine    |--->| T2D Pulse Score    |
                  |                     |    | Calculation        |
                  +---------------------+    +--------------------+
                                                      |
                                                      v
                                              +------------------+
                                              | Web Dashboard    |
                                              | Display          |
                                              +------------------+
```

## Data Sources

### Stock Market Data

- **Finnhub API**: Primary source for real-time stock data
- **Alpha Vantage API**: Secondary source for historical stock data
- **Polygon.io API**: Used for detailed company information
- **Yahoo Finance (yfinance)**: Used for VIX, Treasury Yield, and NASDAQ data

### Economic Indicators

- **FRED API**: Federal Reserve Economic Data for indicators like:
  - GDP
  - Unemployment rate
  - Inflation (CPI)
  - Personal Consumption Expenditures (PCE)
  - Interest rates
  
- **BEA API**: Bureau of Economic Analysis for detailed GDP components
- **BLS API**: Bureau of Labor Statistics for employment data and PPI

## Data Authenticity Mechanisms

T2D Pulse implements several mechanisms to ensure data authenticity:

1. **Cross-validation**: Data is validated across multiple sources when possible
2. **Completeness checks**: Ensures all required fields are present
3. **Range validation**: Checks that values are within expected ranges
4. **Temporal consistency**: Verifies that data points follow a logical timeline
5. **Audit logging**: Records all data updates with timestamps and sources

## Error Handling and Recovery

The data collection system has robust error handling:

1. **Automatic retries**: Failed API requests are retried with exponential backoff
2. **Fallback sources**: If a primary source fails, secondary sources are used
3. **Data caching**: Successfully retrieved data is cached to prevent data loss
4. **Alert system**: Critical failures trigger alerts (via logs)
5. **Self-healing**: The system attempts to recover from partial failures

## Migration Considerations

When migrating this architecture to a new platform:

1. **Schedule background processes**: Set up cron jobs or scheduled tasks for:
   ```bash
   python background_data_collector.py --check 15 --update 30
   ```

2. **Configure API keys**: Ensure all necessary API keys are configured in environment variables

3. **Set up proper error handling**: Forward logs to a monitoring system

4. **Database configuration**: Ensure the database is properly configured for storing collected data

5. **File system access**: Check that all file system paths are correctly set for the new environment

## Common Troubleshooting

### Missing or Incomplete Data

If you encounter missing or incomplete data:

1. Check the logs in `background_data_collector.log`
2. Verify API key configurations
3. Check for API rate limiting issues
4. Run a manual collection process:
   ```bash
   python batch_ticker_collector.py --force
   ```

### Incorrect Sector Scores

If sector scores seem incorrect:

1. Review the economic indicator values in the database
2. Check the sector weight configurations
3. Verify the sentiment calculation logic in `sentiment_engine.py`
4. Validate market cap data by running:
   ```bash
   python check_market_caps.py
   ```

This document should help in understanding and migrating the data collection architecture of T2D Pulse to a new platform.