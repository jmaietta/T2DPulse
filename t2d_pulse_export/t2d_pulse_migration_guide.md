# T2D Pulse Migration Guide

## Project Overview
T2D Pulse is an advanced economic analytics platform delivering comprehensive technological and software industry economic insights through intelligent multi-source data collection and visualization.

This guide provides instructions for migrating the T2D Pulse project from Replit to another hosting platform.

## Core Components

### Main Application Files
- `app.py` - The main Dash application that serves the economic dashboard
- `config.py` - Configuration settings and constants
- `wsgi.py` - WSGI entry point for production deployment
- `background_data_collector.py` - Background service that collects ticker data

### Data Collection
- `authentic_marketcap_reader.py` - Reads authentic market cap data
- `authentic_marketcap_updater.py` - Updates market cap data from authentic sources
- `batch_ticker_collector.py` - Collects ticker data in batches
- `fix_sector_export.py` - Generates sector export files
- `calculate_adtech_marketcap.py` - Calculates AdTech sector market cap

### Data Storage
- `data/` - Directory containing all historical and current data files
- `data/sector_sentiment_history.json` - Authentic sector sentiment history data
- `data/sector_weights_latest.json` - Latest sector weights

### Static Assets
- `assets/` - Directory for static assets like CSS and images
- `assets/pulse_logo.png` - T2D Pulse logo
- `assets/styles.css` - Custom styling

## API Dependencies

The application relies on several external APIs:

1. **Finnhub API**
   - Used for: Real-time and historical stock data
   - Required environment variable: `FINNHUB_API_KEY`
   - API documentation: https://finnhub.io/docs/api

2. **Alpha Vantage API**
   - Used for: Financial data, market statistics
   - Required environment variable: `ALPHAVANTAGE_API_KEY`
   - API documentation: https://www.alphavantage.co/documentation/

3. **Federal Reserve Economic Data (FRED) API**
   - Used for: Economic indicators
   - Required environment variable: `FRED_API_KEY`
   - API documentation: https://fred.stlouisfed.org/docs/api/fred/

4. **Bureau of Economic Analysis (BEA) API**
   - Used for: GDP and economic analysis data
   - Required environment variable: `BEA_API_KEY`
   - API documentation: https://apps.bea.gov/API/signup/

5. **Bureau of Labor Statistics (BLS) API**
   - Used for: Employment and inflation data
   - Required environment variable: `BLS_API_KEY`
   - API documentation: https://www.bls.gov/developers/

6. **NASDAQ Data Link API (formerly Quandl)**
   - Used for: Financial and economic datasets
   - Required environment variable: `NASDAQ_DATA_LINK_API_KEY`
   - API documentation: https://docs.data.nasdaq.com/

7. **Polygon.io API**
   - Used for: Stock market data
   - Required environment variable: `POLYGON_API_KEY`
   - API documentation: https://polygon.io/docs/

## Database Requirements

The application uses a PostgreSQL database for data storage:

- Required environment variables:
  - `DATABASE_URL` - Full connection string
  - `PGHOST`, `PGPORT`, `PGUSER`, `PGPASSWORD`, `PGDATABASE` - Individual connection parameters

## Environment Variables

Create a `.env` file with these environment variables:

```
# API Keys
FINNHUB_API_KEY=your_key_here
ALPHAVANTAGE_API_KEY=your_key_here
FRED_API_KEY=your_key_here
BEA_API_KEY=your_key_here
BLS_API_KEY=your_key_here
NASDAQ_DATA_LINK_API_KEY=your_key_here
POLYGON_API_KEY=your_key_here
SENDGRID_API_KEY=your_key_here

# Database Configuration
DATABASE_URL=postgres://username:password@host:port/database
PGHOST=host
PGPORT=5432
PGUSER=username
PGPASSWORD=password
PGDATABASE=database
```

## Dependencies

The application requires the following Python packages:

```
dash
dash-bootstrap-components
docx
fastparquet
filelock
finnhub-python
flask
matplotlib
nasdaq-data-link
numpy
openai
openpyxl
pandas
plotly
pyarrow
pypdf2
python-docx
pytz
requests
sendgrid
tabulate
tenacity
torch
tqdm
trafilatura
twilio
yfinance
```

Create a `requirements.txt` file with these dependencies.

## Migration Steps

1. **Set up a new environment**:
   - Python 3.9 or higher
   - PostgreSQL database
   - Web server (Nginx/Apache) if deploying to production

2. **Copy all essential files**:
   ```bash
   # Create export directory
   mkdir -p t2d_pulse_export
   
   # Copy main application files
   cp app.py config.py wsgi.py background_data_collector.py t2d_pulse_export/
   
   # Copy data collection scripts
   cp authentic_*.py batch_ticker_collector.py fix_sector_*.py calculate_*.py t2d_pulse_export/
   
   # Copy data directory
   cp -r data t2d_pulse_export/
   
   # Copy assets
   cp -r assets t2d_pulse_export/
   
   # Copy other utility scripts
   cp *.py t2d_pulse_export/
   
   # Create requirements.txt
   pip freeze > t2d_pulse_export/requirements.txt
   ```

3. **Configure environment variables**:
   - Create a `.env` file as described above
   - Configure your new hosting platform to use these environment variables

4. **Set up the database**:
   - Create a PostgreSQL database
   - Import any existing data from Replit

5. **Deploy the application**:
   - Install dependencies: `pip install -r requirements.txt`
   - Run the application: `python wsgi.py`
   - For production, consider using Gunicorn or uWSGI

## Running on the New Platform

1. **Start the main application**:
   ```bash
   python wsgi.py
   ```

2. **Start the background data collector**:
   ```bash
   python background_data_collector.py --check 15 --update 30
   ```

## Troubleshooting

- **Missing Data**: If historical data is missing, check the `data/` directory and ensure all CSV files are properly copied.
- **API Connection Issues**: Verify that all API keys are correctly set in environment variables.
- **Database Connection**: Ensure PostgreSQL connection details are correct and the database is accessible.

For additional support, refer to the comprehensive codebase documentation or contact the development team.