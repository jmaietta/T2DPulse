# T2D Pulse Market Cap Data Management

This documentation explains how market cap data is managed in the T2D Pulse system.

## Data Sources

All market cap data comes from authentic sources:

1. **Polygon API**: Primary source for historical ticker price and market cap data
2. **Yahoo Finance API**: Backup source for ticker data when Polygon API is unavailable
3. **Database**: SQLite database storing historical market cap data for tickers and sectors

## Database Schema

The database schema for market cap data consists of:

1. **sectors**: Stores sector information (id, name)
2. **tickers**: Stores ticker information (id, symbol, name)
3. **ticker_sectors**: Many-to-many relationship between tickers and sectors
4. **ticker_market_caps**: Historical market cap data for individual tickers
5. **sector_market_caps**: Historical market cap data for sectors

## Data Collection Process

1. `Polygon 30-Day History Collector` workflow runs continuously to fetch 30-day historical market cap data for all 96 tickers
2. Data is saved to `T2D_Pulse_Full_Ticker_History.csv`
3. `fix_sector_market_caps.py` loads this data into the database and calculates sector market caps
4. Market cap data is used by the dashboard to display sector performance

## Updating Market Cap Data

To update market cap data:

1. Ensure the `Polygon 30-Day History Collector` workflow is running
2. Wait for it to fetch data for all tickers (monitor the logs)
3. Run `fix_sector_market_caps.py` to update the database
4. Verify the data with `verify_market_caps.py`

## Verification Steps

To verify market cap data integrity:

1. Run `verify_market_caps.py` to check database consistency
2. Examine the Excel files (`authentic_sector_market_caps.xlsx`) for manual verification
3. Check that all dates have data and there are no gaps in the time series
4. Ensure all sectors have proper market cap values, with no placeholders or missing data

## Important Scripts

- `fix_sector_market_caps.py`: Updates the database from CSV data
- `verify_market_caps.py`: Verifies data integrity in the database
- `display_all_sector_marketcaps.py`: Displays sector market cap data in tabular format
- `background_data_collector.py`: Daily data collection for prices and market caps

## Troubleshooting

If market cap data appears inconsistent or incomplete:

1. Check that the Polygon API key is valid and working
2. Verify that `T2D_Pulse_Full_Ticker_History.csv` contains the expected data
3. Run `fix_sector_market_caps.py` to reload data into the database
4. If needed, manually trigger data collection for specific tickers

## File Locations

- **Database**: `market_cap_data.db` 
- **CSV Data**: `T2D_Pulse_Full_Ticker_History.csv`
- **Excel Reports**: `authentic_sector_market_caps.xlsx`, `sector_market_caps_history.xlsx`

## Data Integrity Rules

1. Never use placeholder or synthetic data
2. Always load authentic data from reliable sources 
3. Do not display charts or metrics if data is missing
4. Maintain consistent historical records with no gaps
5. Record all data processing steps for transparency