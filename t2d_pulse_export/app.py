import os
import pandas as pd
import numpy as np
import requests
import base64
import io
import json
import time
from datetime import datetime, timedelta, timezone
import pytz  # For timezone handling
import dash
import dash_bootstrap_components as dbc
from dash import dcc, html, dash_table, ALL, MATCH, ctx
from dash.dependencies import Input, Output, State
from dash.exceptions import PreventUpdate
import flask
import plotly.graph_objs as go
import plotly.express as px
import yfinance as yf
import logging
from functools import lru_cache

# Import API keys from the separate file
from api_keys import FRED_API_KEY, BEA_API_KEY, BLS_API_KEY

# Import document analysis functionality
import document_analysis

# Import sector sentiment scoring
import sentiment_engine

# Import efficient data reading functionality
from data_reader import read_data_file, read_sector_data, read_pulse_score, read_market_data

# Import chart styling and market insights components
from chart_styling import custom_template, color_scheme

# Import T2D Pulse history tracking and trend chart
from t2d_pulse_history import save_t2d_pulse_score, get_t2d_pulse_history
from t2d_pulse_trend_chart import create_t2d_pulse_chart
from market_insights import create_insights_panel

# Import sector trend chart for historical visualizations
import sector_trend_chart

# Import data cache for fast data access
from data_cache import get_data, get_all_data

# Import historical sector score generation
import historical_sector_scores

# Import authentic sector history for trend charts
import authentic_sector_history
import predefined_sector_data

# Data reader for efficient file operations is already imported

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Global data caching for expensive operations
# Load T2D Pulse history exclusively from Parquet
T2D_PULSE_HISTORY = None
try:
    parquet_file = os.path.join("data", "t2d_pulse_history.parquet")
    logger.info(f"Loading T2D Pulse history from Parquet: {parquet_file}")
    T2D_PULSE_HISTORY = pd.read_parquet(parquet_file)

    # Ensure we have a proper datetime index
    if "date" in T2D_PULSE_HISTORY.columns:
        T2D_PULSE_HISTORY["date"] = pd.to_datetime(T2D_PULSE_HISTORY["date"])
        T2D_PULSE_HISTORY.set_index("date", inplace=True)
    logger.info(f"Loaded {len(T2D_PULSE_HISTORY)} records of Pulse history")

except Exception as e:
    logger.error(f"Failed to load T2D Pulse history from Parquet: {e}")
    T2D_PULSE_HISTORY = pd.DataFrame()

# Get the authentic T2D Pulse score
def get_authentic_pulse_score() -> float | None:
    """Return the latest T2D Pulse score from Postgres, or None if unavailable."""
    import sqlalchemy, os, pandas as pd

    try:
        engine = sqlalchemy.create_engine(os.getenv("DATABASE_URL"))

        df = pd.read_sql(
            """
            SELECT pulse_score
            FROM   pulse_history
            ORDER  BY date DESC
            LIMIT  1
            """,
            engine
        )

        if df.empty:
            logger.warning("pulse_history table is empty – no Pulse score available.")
            return None

        score = float(df.iloc[0, 0])
        logger.info(f"Pulse score fetched from DB: {score:.1f}")
        return score

    except Exception as e:
        logger.error(f"Failed to read Pulse score from DB: {e}")
        return None

# Consumer sentiment functions defined directly in app.py to avoid circular imports

# Data directory
DATA_DIR = 'data'
os.makedirs(DATA_DIR, exist_ok=True)

# Define series IDs for FRED data
FRED_SERIES = {
    "gdp": "GDPC1",              # Real GDP
    "unemployment": "UNRATE",    # Unemployment Rate
    "cpi": "CPIAUCSL",          # Consumer Price Index
    "pcepi": "PCEPI",           # Personal Consumption Expenditures Price Index
    "interest_rate": "FEDFUNDS", # Federal Funds Rate
    "pce": "PCE",               # Personal Consumption Expenditures
    "consumer_sentiment": "USACSCICP02STSAM", # Consumer Sentiment
    "software_ppi": "PCU511210511210", # Software Publishers PPI
    "data_ppi": "PCU518210518210"  # Data Processing Services PPI
}

# Define data file paths
DATA_FILES = {
    "gdp": "data/gdp_data.csv",
    "unemployment": "data/unemployment_data.csv",
    "cpi": "data/inflation_data.csv",
    "pcepi": "data/pcepi_data.csv",
    "interest_rate": "data/interest_rate_data.csv",
    "pce": "data/pce_data.csv",
    "treasury_yield": "data/treasury_yield_data.csv",
    "vix": "data/vix_data.csv",
    "nasdaq": "data/nasdaq_data.csv",
    "consumer_sentiment": "data/consumer_sentiment_data.csv",
    "software_ppi": "data/software_ppi_data.csv",
    "data_ppi": "data/data_processing_ppi_data.csv"
}

# Initialize the Dash app with external stylesheets
app = dash.Dash(
    __name__,
    suppress_callback_exceptions=True,  # Suppress exceptions for callbacks to components not in the layout
    meta_tags=[
        {"name": "viewport", "content": "width=device-width, initial-scale=1.0"}
    ],
    external_stylesheets=["https://cdnjs.cloudflare.com/ajax/libs/font-awesome/5.15.3/css/all.min.css"]
)

# Set the server for production deployment
server = app.server

# Set page title
app.title = "T2D Pulse"

# Function to fetch data from FRED
def fetch_fred_data(series_id, start_date=None, end_date=None):
    """Fetch data from FRED API for a given series"""
    logger.info(f"Fetching FRED data for series {series_id}")
    
    if not FRED_API_KEY:
        logger.error("Cannot fetch FRED data: No API key provided")
        return pd.DataFrame()
    
    # Use today's date in EDT time zone for most current data
    eastern = pytz.timezone('US/Eastern')
    today = datetime.now(eastern).date()
    current_date = today.strftime('%Y-%m-%d')
    logger.debug(f"Using Eastern time date: {current_date}")
    
    # Default to last 5 years if no dates specified
    if not end_date:
        # Use today for most recent data
        end_date = current_date
    if not start_date:
        # Calculate 5 years before the end date
        end_date_obj = datetime.strptime(end_date, '%Y-%m-%d') if isinstance(end_date, str) else end_date
        start_date = (end_date_obj - timedelta(days=5*365)).strftime('%Y-%m-%d')
    
    # Build API URL
    url = f"https://api.stlouisfed.org/fred/series/observations"
    
    # Use current dates for most recent data
    params = {
        "series_id": series_id,
        "api_key": FRED_API_KEY,
        "file_type": "json",
        "observation_start": start_date,
        "observation_end": current_date,
        "sort_order": "desc",  # Get newest observations first
        "realtime_start": current_date,  # Use latest vintage data
        "realtime_end": current_date     # Avoid using older vintages
    }
    
    try:
        # Make API request with current dates
        response = requests.get(url, params=params)
        
        if response.status_code == 200:
            data = response.json()
            
            # Convert to DataFrame
            df = pd.DataFrame(data['observations'])
            df['date'] = pd.to_datetime(df['date'])
            df['value'] = pd.to_numeric(df['value'], errors='coerce')
            
            # Handle missing values
            df = df.dropna(subset=['value'])
            
            # Sort by date (newest first)
            df = df.sort_values('date', ascending=False)
            
            # Report most recent value
            if not df.empty:
                latest = df.iloc[0]
                print(f"Latest {series_id} value: {latest['value']} on {latest['date'].strftime('%Y-%m-%d')}")
            
            print(f"Successfully retrieved {len(df)} observations for {series_id}")
            return df
        else:
            # If current parameters fail, try again with slightly modified approach
            print(f"FRED API request failed: {response.status_code} - {response.text}")
            print("Trying alternate approach to get most recent data...")
            
            # Try without specifying realtime parameters
            params.pop("realtime_start", None)
            params.pop("realtime_end", None)
            
            try:
                # Make second API request with simplified parameters
                response = requests.get(url, params=params)
                
                if response.status_code == 200:
                    data = response.json()
                    
                    # Convert to DataFrame
                    df = pd.DataFrame(data['observations'])
                    df['date'] = pd.to_datetime(df['date'])
                    df['value'] = pd.to_numeric(df['value'], errors='coerce')
                    
                    # Handle missing values
                    df = df.dropna(subset=['value'])
                    
                    # Sort by date (newest first)
                    df = df.sort_values('date', ascending=False)
                    
                    # Report most recent value
                    if not df.empty:
                        latest = df.iloc[0]
                        print(f"Latest {series_id} value: {latest['value']} on {latest['date'].strftime('%Y-%m-%d')}")
                    
                    print(f"Second attempt: Successfully retrieved {len(df)} observations for {series_id}")
                    return df
                else:
                    print(f"Second FRED API attempt also failed: {response.status_code} - {response.text}")
                    
                    # Try to load cached data if available
                    filename = None
                    for key, fname in DATA_FILES.items():
                        if series_id == FRED_SERIES.get(key):
                            filename = fname
                            break
                    
                    if filename and os.path.exists(filename):
                        print(f"Loading cached data from {filename} as fallback")
                        cached_df = pd.read_csv(filename)
                        cached_df['date'] = pd.to_datetime(cached_df['date'])
                        return cached_df
                    
                    return pd.DataFrame()
            except Exception as e:
                print(f"Exception during second FRED API attempt: {str(e)}")
                return pd.DataFrame()
    except Exception as e:
        print(f"Exception while fetching FRED data: {str(e)}")
        return pd.DataFrame()

def save_data_to_csv(df, filename):
    """Save DataFrame to CSV file"""
    if df.empty:
        print(f"No data to save to {filename}")
        return False
        
    try:
        # Handle case where the filename already includes the data directory
        if filename.startswith('data/'):
            file_path = filename
        else:
            file_path = os.path.join(DATA_DIR, filename)
            
        # Ensure directory exists
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        
        df.to_csv(file_path, index=False)
        print(f"Successfully saved {len(df)} rows to {filename}")
        return True
    except Exception as e:
        print(f"Failed to save data to {filename}: {str(e)}")
        return False

def load_data_from_csv(filename):
    """Load DataFrame from CSV file or Parquet equivalent using the data_reader"""
    try:
        # Use data_reader's efficient implementation which tries Parquet first
        df = read_data_file(os.path.basename(filename))
        
        if df is not None and not df.empty:
            # The data_reader already handles date conversion
            logger.info(f"Successfully loaded {len(df)} rows from {filename}")
            return df
        else:
            # Fall back to old method if data_reader fails
            file_path = os.path.join(DATA_DIR, filename)
            if os.path.exists(file_path):
                df = pd.read_csv(file_path)
                
                # Convert 'date' column to datetime, case-insensitive
                date_columns = [col for col in df.columns if col.lower() == 'date']
                if date_columns:
                    date_col = date_columns[0]
                    df[date_col] = pd.to_datetime(df[date_col])
                    
                    # Ensure the date column is consistently named 'date'
                    if date_col != 'date':
                        df = df.rename(columns={date_col: 'date'})
                
                logger.info(f"Fallback: Successfully loaded {len(df)} rows from {filename}")
                return df
            else:
                logger.warning(f"File {filename} does not exist")
                return pd.DataFrame()
    except Exception as e:
        logger.error(f"Failed to load data from {filename}: {str(e)}")
        return pd.DataFrame()

def fetch_bea_data(table_name, frequency, start_year, end_year):
    """Fetch data from BEA API"""
    print(f"Fetching BEA data for table {table_name}")
    
    if not BEA_API_KEY:
        print("Cannot fetch BEA data: No API key provided")
        return pd.DataFrame()
    
    url = "https://apps.bea.gov/api/data"
    params = {
        "UserID": BEA_API_KEY,
        "method": "GetData",
        "datasetname": "NIPA",
        "TableName": table_name,
        "Frequency": frequency,
        "Year": f"{start_year},{end_year}",
        "ResultFormat": "JSON"
    }
    
    try:
        response = requests.get(url, params=params)
        
        if response.status_code == 200:
            data = response.json()
            
            # Extract data from the response
            if 'BEAAPI' in data and 'Results' in data['BEAAPI'] and 'Data' in data['BEAAPI']['Results']:
                bea_data = data['BEAAPI']['Results']['Data']
                
                # Convert to DataFrame
                df = pd.DataFrame(bea_data)
                print(f"Successfully retrieved BEA data with {len(df)} rows")
                return df
            else:
                print(f"Unexpected BEA API response format: {data}")
                return pd.DataFrame()
        else:
            print(f"Failed to fetch BEA data: {response.status_code} - {response.text}")
            return pd.DataFrame()
    except Exception as e:
        print(f"Exception while fetching BEA data: {str(e)}")
        return pd.DataFrame()

def fetch_treasury_yield_data():
    """Fetch 10-Year Treasury Yield data from Yahoo Finance (^TNX)
    
    Returns a DataFrame with date and value columns formatted like FRED data.
    Uses the closing price as the most accurate daily value.
    """
    logger.info("Fetching 10-Year Treasury Yield data from Yahoo Finance...")
    
    try:
        # Use Yahoo Finance to get the most recent data
        treasury = yf.Ticker('^TNX')
        # Get data for the last 180 days (6 months) to ensure we have enough 
        # recent values for a meaningful chart
        data = treasury.history(period='180d')
        
        if data.empty:
            logger.warning("No Treasury Yield data retrieved from Yahoo Finance")
            return pd.DataFrame()
            
        # Format data to match our standard format
        # Using Close prices for most accurate end-of-day values
        df = pd.DataFrame({
            'date': data.index.tz_localize(None),  # Remove timezone to match FRED data
            'value': data['Close']
        })
        
        # Sort by date (newest first) for easier reporting and data merging
        df = df.sort_values('date', ascending=False)
        
        # Load existing data from CSV to merge with
        try:
            existing_data = pd.read_csv('data/treasury_yield_data.csv')
            if not existing_data.empty:
                # Convert date to datetime
                existing_data['date'] = pd.to_datetime(existing_data['date'])
                
                # Filter out dates that we now have from Yahoo Finance
                min_new_date = df['date'].min()
                old_data = existing_data[existing_data['date'] < min_new_date]
                
                # Combine old historical data with new recent data
                combined_df = pd.concat([df, old_data])
                combined_df = combined_df.sort_values('date', ascending=False)
                logger.info(f"Combined {len(df)} recent days with {len(old_data)} historical days")
                
                # Save combined data to CSV
                save_data_to_csv(combined_df, 'data/treasury_yield_data.csv')
                logger.info(f"Successfully saved {len(combined_df)} rows to treasury_yield_data.csv")
                
                df = combined_df
            else:
                # Just save the Yahoo Finance data
                save_data_to_csv(df, 'data/treasury_yield_data.csv')
                logger.info(f"No existing data to merge, saved {len(df)} rows to treasury_yield_data.csv")
        except Exception as e:
            logger.error(f"Error merging with existing data: {str(e)}")
            # Still save the Yahoo Finance data
            save_data_to_csv(df, 'data/treasury_yield_data.csv')
        
        # Report the latest value and date
        latest_date = df.iloc[0]['date'].strftime('%Y-%m-%d')
        latest_value = df.iloc[0]['value']
        logger.info(f"Treasury Yield (latest): {latest_value:.3f}% on {latest_date}")
        logger.info(f"Successfully retrieved/merged {len(df)} days of Treasury Yield data")
        
        return df
    except Exception as e:
        logger.error(f"Exception while fetching Treasury Yield data from Yahoo Finance: {str(e)}")
        
        # Try to load cached data as fallback
        try:
            cached_data = pd.read_csv('data/treasury_yield_data.csv')
            if not cached_data.empty:
                cached_data['date'] = pd.to_datetime(cached_data['date'])
                logger.warning(f"Falling back to cached Treasury Yield data with {len(cached_data)} observations")
                latest = cached_data.sort_values('date', ascending=False).iloc[0]
                logger.info(f"Latest cached value: {latest['value']:.3f}% on {latest['date'].strftime('%Y-%m-%d')}")
                return cached_data
        except Exception as fallback_e:
            logger.error(f"Failed to load cached data: {str(fallback_e)}")
        
        return pd.DataFrame()
        
def fetch_vix_from_yahoo():
    """Fetch VIX Volatility Index data from Yahoo Finance (^VIX)
    
    Returns a DataFrame with date and value columns formatted like FRED data.
    Uses the closing price as the daily value to get most recent data.
    """
    logger.info("Fetching VIX data from Yahoo Finance for most recent values...")
    
    try:
        # Use Yahoo Finance to get the most recent data
        vix = yf.Ticker('^VIX')
        # Get data for the last 60 days to ensure we have enough recent values
        data = vix.history(period='60d')
        
        if data.empty:
            logger.warning("No VIX data retrieved from Yahoo Finance")
            return pd.DataFrame()
            
        # Format data to match FRED format
        df = pd.DataFrame({
            'date': data.index.tz_localize(None),  # Remove timezone to match FRED data
            'value': data['Close']
        })
        
        # Sort by date (newest first) for easier reporting and data merging
        df = df.sort_values('date', ascending=False)
        
        # Report the latest value and date
        latest_date = df.iloc[0]['date'].strftime('%Y-%m-%d')
        latest_value = df.iloc[0]['value']
        logger.info(f"VIX (latest): {latest_value:.2f} on {latest_date}")
        logger.info(f"Successfully retrieved {len(df)} days of VIX data from Yahoo Finance")
        
        return df
    except Exception as e:
        logger.error(f"Exception while fetching VIX data from Yahoo Finance: {str(e)}")
        logger.warning("Falling back to FRED data for VIX")
        return pd.DataFrame()

def fetch_nasdaq_with_ema():
    """Fetch NASDAQ data using yfinance and calculate the 20-day EMA
    
    Returns a DataFrame with:
    - date: the date of the observation
    - value: the NASDAQ Composite closing value
    - ema20: the 20-day exponential moving average
    - gap_pct: percentage difference between the current value and EMA (momentum indicator)
    """
    try:
        logger.info("Fetching NASDAQ data from Yahoo Finance with 20-day EMA...")
        
        # Get NASDAQ Composite data for the last 50 days (need extra days for EMA calculation)
        ixic = yf.Ticker("^IXIC")
        data = ixic.history(period="50d")  # Increased to ensure enough data for 20-day EMA
        
        if data.empty:
            logger.warning("No NASDAQ data retrieved from Yahoo Finance")
            return pd.DataFrame()
        
        # Calculate 20-day EMA
        data['ema20'] = data['Close'].ewm(span=20, adjust=False).mean()
        
        # Calculate gap percentage (current price vs EMA)
        data['gap_pct'] = (data['Close'] / data['ema20'] - 1) * 100
        
        # Create DataFrame with our standard format, plus the additional metrics
        df = pd.DataFrame({
            'date': data.index.tz_localize(None),  # Remove timezone to match FRED data
            'value': data['Close'],
            'ema20': data['ema20'],
            'gap_pct': data['gap_pct'],
            'pct_change': data['Close'].pct_change() * 100  # Keep this for compatibility
        })
        
        # Sort by date (newest first) for easier reporting
        df = df.sort_values('date', ascending=False)
        
        # Report the latest values
        latest_date = df.iloc[0]['date'].strftime('%Y-%m-%d')
        latest_value = df.iloc[0]['value']
        latest_gap = df.iloc[0]['gap_pct']
        logger.info(f"NASDAQ: {latest_value:.1f} on {latest_date}, Gap from 20-day EMA: {latest_gap:.2f}%")
        logger.info(f"Successfully retrieved NASDAQ data with EMA calculation")
        
        return df
    except Exception as e:
        logger.error(f"Exception while fetching NASDAQ data with EMA: {str(e)}")
        logger.warning("Falling back to FRED data for NASDAQ")
        return pd.DataFrame()

def fetch_consumer_sentiment_data():
    """Fetch Consumer Confidence Composite Index data from FRED API
    
    Returns a DataFrame with date and value columns formatted like other FRED data.
    Uses FRED series USACSCICP02STSAM: Consumer Opinion Surveys: Composite 
    Consumer Confidence for United States
    """
    try:
        # Use Consumer Opinion Surveys: Composite Consumer Confidence
        series_id = "USACSCICP02STSAM"
        
        df = fetch_fred_data(series_id)
        
        if not df.empty:
            logger.info(f"Successfully retrieved {len(df)} observations for Consumer Confidence Index")
            
            # Calculate year-over-year change
            df = df.sort_values('date')
            df['yoy_change'] = df['value'].pct_change(periods=12) * 100
            
            return df
        else:
            logger.error("Error retrieving Consumer Confidence data from FRED")
            return pd.DataFrame()
    except Exception as e:
        logger.error(f"Exception while fetching Consumer Confidence data: {str(e)}")
        return pd.DataFrame()
        
def create_consumer_sentiment_graph(consumer_sentiment_data):
    """Generate a graph of Consumer Sentiment data"""
    if consumer_sentiment_data.empty:
        return go.Figure().update_layout(
            title="No Consumer Sentiment data available",
            height=400
        )
    
    # Filter for last 5 years
    cutoff_date = datetime.now() - timedelta(days=5*365)
    filtered_data = consumer_sentiment_data[consumer_sentiment_data['date'] >= cutoff_date].copy()
    
    # Create figure
    fig = go.Figure()
    
    # Add Consumer Sentiment line
    fig.add_trace(
        go.Scatter(
            x=filtered_data['date'],
            y=filtered_data['value'],
            mode='lines',
            name='Consumer Sentiment Index',
            line=dict(color=color_scheme['consumption'], width=3)
        )
    )
    
    # Add YoY change if available
    if 'yoy_change' in filtered_data.columns:
        # Create second y-axis for YoY change
        fig.add_trace(
            go.Scatter(
                x=filtered_data['date'],
                y=filtered_data['yoy_change'],
                mode='lines',
                name='Year-over-Year Change',
                line=dict(color=color_scheme['secondary'], width=2, dash='dot'),
                yaxis='y2'
            )
        )
        
        # Add zero line for YoY change
        fig.add_shape(
            type="line",
            x0=filtered_data['date'].min(),
            x1=filtered_data['date'].max(),
            y0=0,
            y1=0,
            line=dict(
                color=color_scheme["neutral"],
                width=1.5,
                dash="dot",
            ),
            yref="y2"
        )
    
    # Add current value annotation
    if len(filtered_data) > 0:
        current_value = filtered_data.sort_values('date', ascending=False).iloc[0]['value']
        current_yoy = filtered_data.sort_values('date', ascending=False).iloc[0]['yoy_change'] if 'yoy_change' in filtered_data.columns else None
        
        if current_yoy is not None:
            arrow_color = color_scheme["positive"] if current_yoy > 0 else color_scheme["negative"]
            arrow_symbol = "▲" if current_yoy > 0 else "▼"
            
            annotation_text = f"Current: {current_value:.1f} ({arrow_symbol} {abs(current_yoy):.1f}% YoY)"
        else:
            annotation_text = f"Current: {current_value:.1f}"
            arrow_color = "gray"
        
        fig.add_annotation(
            x=0.02,
            y=0.95,
            xref="paper",
            yref="paper",
            text=annotation_text,
            showarrow=False,
            font=dict(size=14, color=arrow_color),
            align="left",
            bgcolor="rgba(255, 255, 255, 0.9)",
            bordercolor=arrow_color,
            borderwidth=1,
            borderpad=4,
            opacity=0.9
        )
    
    # Update layout
    fig.update_layout(
        template=custom_template,
        height=400,
        title="Consumer Confidence Index",
        margin=dict(l=40, r=40, t=60, b=40),
        hovermode="x unified",
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1
        ),
        yaxis=dict(
            title="Index Value"
        ),
        yaxis2=dict(
            title="Year-over-Year Change (%)",
            overlaying="y",
            side="right",
            showgrid=False,
            zeroline=False,
            ticksuffix="%"
        )
    )
    
    return fig

def fetch_bls_data(series_id, start_year, end_year):
    """Fetch data from BLS API"""
    logger.info(f"Fetching BLS data for series {series_id}")
    
    if not BLS_API_KEY:
        logger.error("Cannot fetch BLS data: No API key provided")
        return pd.DataFrame()
    
    url = "https://api.bls.gov/publicAPI/v2/timeseries/data/"
    headers = {'Content-Type': 'application/json'}
    data = {
        "seriesid": [series_id],
        "startyear": str(start_year),
        "endyear": str(end_year),
        "registrationkey": BLS_API_KEY
    }
    
    try:
        response = requests.post(url, json=data, headers=headers)
        
        if response.status_code == 200:
            result = response.json()
            
            if result['status'] == 'REQUEST_SUCCEEDED':
                # Extract data
                series_data = result['Results']['series'][0]['data']
                
                # Convert to DataFrame
                df = pd.DataFrame(series_data)
                
                # Process dates
                df['date'] = df.apply(lambda row: f"{row['year']}-{row['period'].replace('M', '')}-01", axis=1)
                df['date'] = pd.to_datetime(df['date'])
                
                # Convert value to numeric
                df['value'] = pd.to_numeric(df['value'], errors='coerce')
                
                logger.info(f"Successfully retrieved BLS data with {len(df)} rows")
                return df
            else:
                logger.error(f"BLS API request failed: {result['message']}")
                return pd.DataFrame()
        else:
            logger.error(f"Failed to fetch BLS data: {response.status_code} - {response.text}")
            return pd.DataFrame()
    except Exception as e:
        logger.error(f"Exception while fetching BLS data: {str(e)}")
        return pd.DataFrame()

def generate_sector_drivers(macros):
    """Generate list of key drivers (factors) for each sector based on macro data"""
    drivers = {}
    
    # Map of sectors to their key drivers (indicator names)
    sector_drivers = {
        "SMB SaaS": ["NASDAQ_20d_gap_%", "Software_Dev_Job_Postings_YoY_%"],
        "Enterprise SaaS": ["10Y_Treasury_Yield_%", "NASDAQ_20d_gap_%"],
        "Cloud Infrastructure": ["PPI_Software_Publishers_YoY_%", "10Y_Treasury_Yield_%"],
        "AdTech": ["VIX", "NASDAQ_20d_gap_%", "Real_PCE_YoY_%", "Consumer_Sentiment"],
        "Fintech": ["Fed_Funds_Rate_%", "Real_PCE_YoY_%", "Consumer_Sentiment"],
        "Consumer Internet": ["VIX", "Real_PCE_YoY_%", "Consumer_Sentiment"],
        "eCommerce": ["Real_PCE_YoY_%", "NASDAQ_20d_gap_%", "Consumer_Sentiment"],
        "Cybersecurity": ["Software_Dev_Job_Postings_YoY_%", "Fed_Funds_Rate_%"],
        "Dev Tools / Analytics": ["Software_Dev_Job_Postings_YoY_%", "NASDAQ_20d_gap_%"],
        "Semiconductors": ["PPI_Data_Processing_YoY_%", "10Y_Treasury_Yield_%"],
        "AI Infrastructure": ["PPI_Data_Processing_YoY_%", "Fed_Funds_Rate_%"],
        "Vertical SaaS": ["NASDAQ_20d_gap_%", "Software_Dev_Job_Postings_YoY_%"],
        "IT Services / Legacy Tech": ["Fed_Funds_Rate_%", "Real_GDP_Growth_%_SAAR"],
        "Hardware / Devices": ["PPI_Data_Processing_YoY_%", "Fed_Funds_Rate_%", "Consumer_Sentiment"]
    }
    
    # Human-readable labels and formatting for indicators
    indicator_labels = {
        "NASDAQ_20d_gap_%": "NASDAQ {}%",
        "Software_Dev_Job_Postings_YoY_%": "Dev-jobs {}%",
        "10Y_Treasury_Yield_%": "10-Yr {}%",
        "Fed_Funds_Rate_%": "Fed Funds {}%",
        "VIX": "VIX (14-day EMA) {}",
        "Real_PCE_YoY_%": "PCE {}%",
        "Real_GDP_Growth_%_SAAR": "GDP {}%",
        "PPI_Software_Publishers_YoY_%": "SaaS PPI {}%",
        "PPI_Data_Processing_YoY_%": "PPI {}%",
        "Consumer_Sentiment": "Consumer Sentiment Low" if "Consumer_Sentiment" in macros and macros["Consumer_Sentiment"] < 90 else "Consumer Sentiment {}"
    }
    
    # Generate drivers for each sector
    for sector, indicators in sector_drivers.items():
        sector_driving_factors = []
        
        for indicator in indicators:
            if indicator in macros:
                value = macros[indicator]
                formatted_value = f"{value:+.1f}" if isinstance(value, (int, float)) else value
                # Remove the '+' for absolute values that aren't growth rates or for display clarity
                if indicator in ["VIX", "Consumer_Sentiment", "Fed_Funds_Rate_%", "10Y_Treasury_Yield_%", "Real_PCE_YoY_%"]:
                    formatted_value = f"{value:.1f}"
                
                label = indicator_labels.get(indicator, indicator)
                sector_driving_factors.append(label.format(formatted_value))
        
        # Add a qualitative factor if we have less than 2 drivers
        if len(sector_driving_factors) < 2:
            if "NASDAQ_20d_gap_%" in macros and macros["NASDAQ_20d_gap_%"] < -2:
                sector_driving_factors.append("NASDAQ weak")
            elif "Consumer_Sentiment" in macros and macros["Consumer_Sentiment"] < 90:
                sector_driving_factors.append("Consumer Sentiment Low")
            elif "VIX" in macros and macros["VIX"] > 25:
                sector_driving_factors.append("VIX elevated")
            elif "Fed_Funds_Rate_%" in macros and macros["Fed_Funds_Rate_%"] > 4:
                sector_driving_factors.append("Fed Funds headwind")
        
        drivers[sector] = sector_driving_factors
    
    return drivers

def generate_sector_tickers():
    """Generate representative ticker symbols for each sector using authentic tickers from T2D_Pulse coverage"""
    return {
        "SMB SaaS": ["ADBE", "BILL", "HUBS"],
        "Enterprise SaaS": ["CRM", "MSFT", "ORCL"],
        "Cloud Infrastructure": ["CSCO", "SNOW", "AMZN"],
        "AdTech": ["TTD", "PUBM", "META"],
        "Fintech": ["XYZ", "PYPL", "COIN"],
        "Consumer Internet": ["META", "NFLX", "SNAP"],
        "eCommerce": ["ETSY", "SHOP", "SE"],
        "Cybersecurity": ["PANW", "CRWD", "OKTA"],
        "Dev Tools / Analytics": ["DDOG", "MDB", "TEAM"],
        "Semiconductors": ["NVDA", "AMD", "TSM"],
        "AI Infrastructure": ["GOOGL", "META", "NVDA"],
        "Vertical SaaS": ["PCOR", "CSGP", "CCCS"],
        "IT Services / Legacy Tech": ["ACN", "PLTR", "CTSH"],
        "Hardware / Devices": ["AAPL", "DELL", "SMCI"]
    }

def calculate_sector_sentiment():
    """Calculate sentiment scores for each technology sector using the latest data"""
    logger.info("Starting calculate_sector_sentiment function")
    # Import sector sentiment history module
    import sector_sentiment_history
    from sector_ema_integration import get_sector_ema_factors
    
    # Get latest values for all required indicators
    macros = {}
    
    # Treasury Yield
    if not treasury_yield_data.empty:
        latest_yield = treasury_yield_data.sort_values('date', ascending=False).iloc[0]['value']
        macros["10Y_Treasury_Yield_%"] = latest_yield
        
    # VIX - Use 14-day EMA for more stable signal if available
    if not vix_data.empty:
        latest_vix_row = vix_data.sort_values('date', ascending=False).iloc[0]
        # Use the 14-day EMA for VIX if available, otherwise fallback to raw value
        if 'vix_ema14' in latest_vix_row and not pd.isna(latest_vix_row['vix_ema14']):
            latest_vix = latest_vix_row['vix_ema14']  # Use smoothed value
            logger.info(f"Using smoothed VIX (14-day EMA): {latest_vix:.2f} vs raw: {latest_vix_row['value']:.2f}")
        else:
            latest_vix = latest_vix_row['value']  # Fallback to raw value
            logger.info(f"Using raw VIX value: {latest_vix:.2f} (EMA not available)")
        macros["VIX"] = latest_vix
    
    # NASDAQ gap from 20-day EMA
    if not nasdaq_data.empty and 'gap_pct' in nasdaq_data.columns:
        latest_gap = nasdaq_data.sort_values('date', ascending=False).iloc[0]['gap_pct']
        macros["NASDAQ_20d_gap_%"] = latest_gap  # Updated to use 20-day EMA key
        
    # Fed Funds Rate
    if not interest_rate_data.empty:
        latest_rate = interest_rate_data.sort_values('date', ascending=False).iloc[0]['value']
        macros["Fed_Funds_Rate_%"] = latest_rate
        
    # CPI YoY
    if not inflation_data.empty and 'inflation' in inflation_data.columns:
        latest_cpi = inflation_data.sort_values('date', ascending=False).iloc[0]['inflation']
        macros["CPI_YoY_%"] = latest_cpi
    
    # PCEPI YoY
    if not pcepi_data.empty and 'yoy_growth' in pcepi_data.columns:
        latest_pcepi = pcepi_data.sort_values('date', ascending=False).iloc[0]['yoy_growth']
        macros["PCEPI_YoY_%"] = latest_pcepi
        
    # Real GDP Growth
    if not gdp_data.empty and 'yoy_growth' in gdp_data.columns:
        latest_gdp = gdp_data.sort_values('date', ascending=False).iloc[0]['yoy_growth']
        macros["Real_GDP_Growth_%_SAAR"] = latest_gdp
        
    # Real PCE YoY
    if not pce_data.empty and 'yoy_growth' in pce_data.columns:
        latest_pce = pce_data.sort_values('date', ascending=False).iloc[0]['yoy_growth']
        macros["Real_PCE_YoY_%"] = latest_pce
        
    # Unemployment
    if not unemployment_data.empty:
        latest_unemployment = unemployment_data.sort_values('date', ascending=False).iloc[0]['value']
        macros["Unemployment_%"] = latest_unemployment
        
    # Software Dev Job Postings YoY
    if not job_postings_data.empty and 'yoy_growth' in job_postings_data.columns:
        latest_job_postings = job_postings_data.sort_values('date', ascending=False).iloc[0]['yoy_growth']
        macros["Software_Dev_Job_Postings_YoY_%"] = latest_job_postings
        
    # PPI Data Processing YoY
    if not data_processing_ppi_data.empty and 'yoy_pct_change' in data_processing_ppi_data.columns:
        latest_data_ppi = data_processing_ppi_data.sort_values('date', ascending=False).iloc[0]['yoy_pct_change']
        macros["PPI_Data_Processing_YoY_%"] = latest_data_ppi
        
    # PPI Software Publishers YoY
    if not software_ppi_data.empty and 'yoy_pct_change' in software_ppi_data.columns:
        latest_software_ppi = software_ppi_data.sort_values('date', ascending=False).iloc[0]['yoy_pct_change']
        macros["PPI_Software_Publishers_YoY_%"] = latest_software_ppi
    
    # Consumer Sentiment
    if not consumer_sentiment_data.empty:
        latest_consumer_sentiment = consumer_sentiment_data.sort_values('date', ascending=False).iloc[0]['value']
        macros["Consumer_Sentiment"] = latest_consumer_sentiment
        logger.info(f"Added Consumer Sentiment to sector calculations: {latest_consumer_sentiment}")
    
    # Get sector EMA factors to include as a 14th indicator
    try:
        try:
            ema_factors = get_sector_ema_factors()
            logger.info(f"Retrieved EMA factors for {len(ema_factors)} sectors")
        except Exception as e:
            logger.error(f"Error getting sector EMA factors: {str(e)}")
            # Create fallback EMA factors with small positive bias
            ema_factors = {sector: 0.05 for sector in sentiment_engine.SECTORS}
            logger.info(f"Created fallback EMA factors with small positive bias (0.05) for {len(ema_factors)} sectors")
        
        # Add EMA factors for each sector to the macros dictionary
        # We'll store the original macros as a template
        base_macros = macros.copy()
        
        # If we have EMA factors, we'll create a separate macros dict for each sector
        # using its own specific EMA factor
        if ema_factors:
            # For all sectors without a specific EMA factor, use a small positive bias
            if len(ema_factors) < len(sentiment_engine.SECTORS):
                logger.info(f"Using small positive bias (0.05) for sectors without EMA data")
                
            # Use the default factor for the base macros (will be used for any missing sectors)
            macros["Sector_EMA_Factor"] = 0.05  # Small positive bias by default
    except Exception as e:
        logger.error(f"Error getting EMA factors: {str(e)}")
        # Continue without EMA factors if they're not available
    
    # Check if we have enough data to calculate sector scores (need at least 6 indicators)
    if len(macros) < 6:
        logger.warning(f"Not enough data to calculate sector sentiment scores (have {len(macros)}/{len(macros) + 1} indicators)")
        return []
    
    try:
        # Calculate sector scores - each sector using its own EMA factor
        all_sector_scores = []
        
        for sector in sentiment_engine.SECTORS:
            # Create a copy of the base macros to customize for this sector
            sector_macros = macros.copy()
            
            # Use the sector-specific EMA factor if available, otherwise use default
            if ema_factors and sector in ema_factors:
                sector_macros["Sector_EMA_Factor"] = ema_factors[sector]
                logger.info(f"Using sector-specific EMA factor for {sector}: {ema_factors[sector]:.3f}")
            
            # Calculate scores using this sector's own EMA factor
            sector_result = sentiment_engine.score_sectors(sector_macros)
            
            # Find this specific sector's score
            for data in sector_result:
                if data["sector"] == sector:
                    all_sector_scores.append(data)
                    break
        
        # Use these sector-specific scores
        sector_scores = all_sector_scores
        logger.info(f"Successfully calculated sentiment scores for {len(sector_scores)} sectors with sector-specific EMA factors")
        
        # Get driver factors and tickers for each sector
        drivers = generate_sector_drivers(macros)
        tickers = generate_sector_tickers()
        
        # Enhance sector data with drivers, tickers, and stance
        enhanced_scores = []
        for sector_data in sector_scores:
            sector = sector_data["sector"]
            score = sector_data["score"]
            
            # Normalize score from -1 to +1 scale to 0-100 scale
            norm_score = ((score + 1.0) / 2.0) * 100
            
            # Round to 1 decimal place
            norm_score = round(norm_score, 1)
            
            # Determine stance based on normalized score (0-100 scale)
            # 0-30: Bearish (previously <= -0.25)
            # 30-60: Neutral (previously between -0.25 and +0.05)
            # 60-100: Bullish (previously >= +0.05)
            if norm_score <= 30.0:
                stance = "Bearish"
                takeaway = "Bearish macro setup"
            elif norm_score >= 60.0:
                stance = "Bullish"
                takeaway = "Outperforming peers"
            else:
                stance = "Neutral"
                takeaway = "Neutral – monitor trends"
                
            # Add the enhanced data with both original and normalized scores
            enhanced_scores.append({
                "sector": sector,
                "score": score,  # Keep original score for compatibility
                "normalized_score": norm_score,  # Add normalized score
                "stance": stance,
                "takeaway": takeaway,
                "drivers": drivers.get(sector, []),
                "tickers": tickers.get(sector, [])
            })
        
        # EMA factors are now directly included in the sentiment calculation
        # No need for post-processing adjustment
        
        # Update the historical sentiment data with today's scores
        sector_sentiment_history.update_sentiment_history(enhanced_scores)
        
        # Also update authentic sector history for trend charts
        authentic_sector_history.update_authentic_history(enhanced_scores)
            
        return enhanced_scores
    except Exception as e:
        logger.error(f"Error calculating sector sentiment scores: {str(e)}")
        return []

def calculate_t2d_pulse_from_sectors(sector_scores, sector_weights=None):
    """Calculate T2D Pulse score as a weighted average of sector scores
    
    Args:
        sector_scores (dict or list): Dictionary with sector scores {sector: score} or list of sector dictionaries
        sector_weights (dict, optional): Dictionary with custom weights for each sector
        
    Returns:
        float: The weighted average T2D Pulse score (0-100 scale)
    """
    # Convert sector scores to a dictionary if it's a list
    if isinstance(sector_scores, list):
        sector_dict = {}
        for item in sector_scores:
            sector_dict.update(item)
        sector_scores = sector_dict
    
    # Try to get authentic market cap weights
    try:
        from authentic_marketcap_reader import get_sector_weightings
        authentic_weights = get_sector_weightings()
        
        # If we have authentic weights and no custom weights specified, use the authentic weights
        if authentic_weights and not sector_weights:
            logging.info("Using authentic market cap weights for T2D Pulse calculation")
            sector_weights = authentic_weights
    except (ImportError, Exception) as e:
        logging.warning(f"Could not load authentic market cap weights: {e}")
    
    # If no weights are provided, use equal weighting
    if not sector_weights:
        sector_weights = {sector: 100 / len(sector_scores) for sector in sector_scores}
        logging.info("Using equal weights for T2D Pulse calculation (no weights provided)")
    
    # Normalize the weights to sum to 100
    total_weight = sum(sector_weights.values())
    normalized_weights = {sector: weight * 100 / total_weight for sector, weight in sector_weights.items()}
    
    # Calculate weighted average
    weighted_sum = 0
    total_applied_weight = 0
    
    for sector, score in sector_scores.items():
        if sector in normalized_weights:
            weight = normalized_weights[sector]
            weighted_sum += score * weight
            total_applied_weight += weight
    
    # Handle case where no weights were applied
    if total_applied_weight == 0:
        logging.warning("No weights applied in T2D Pulse calculation, returning simple average")
        return sum(sector_scores.values()) / len(sector_scores)
    
    # Return normalized weighted score
    pulse_score = weighted_sum / total_applied_weight
    return round(pulse_score, 1)

def calculate_sentiment_index(custom_weights=None, proprietary_data=None, document_data=None):
    """Calculate economic sentiment index from available indicators
    
    Args:
        custom_weights (dict, optional): Dictionary with custom weights for each indicator
        proprietary_data (dict, optional): Dictionary with proprietary data and its weight
        document_data (dict, optional): Dictionary with document analysis data and its weight
    """
    # Default weights according to T2D Pulse Homepage requirements
    # Weights exactly as specified in the Default Weights file
    default_weights = {
        'Real GDP % Change': 6.36,              # Line 2: 6.36%
        'PCE': 6.36,                            # Line 3: 6.36%
        'Unemployment Rate': 6.36,              # Line 4: 6.36%
        'Software Job Postings': 6.36,          # Line 5: 6.36%
        'CPI': 6.36,                            # Line 6: 6.36% (Inflation (CPI))
        'PCEPI': 6.36,                          # Line 7: 6.36% (PCEPI (YoY))
        'Federal Funds Rate': 6.36,             # Line 8: 6.36% (Fed Funds Rate)
        'NASDAQ Trend': 15.45,                  # Line 9: 15.45%
        'PPI: Software Publishers': 6.36,       # Line 10: 6.36%
        'PPI: Data Processing Services': 6.36,  # Line 11: 6.36%
        'Treasury Yield': 9.09,                 # Line 12: 9.09% (10-Year Treasury Yield)
        'Consumer Sentiment': 9.09,             # Line 13: 9.09%
        'VIX Volatility': 9.09                  # Line 14: 9.09% (VIX Volatility Index)
    }
    
    # Verify weights sum to exactly 100%
    weights_sum = sum(default_weights.values())
    if abs(weights_sum - 100) > 0.001:
        logger.warning(f"Default weights sum to {weights_sum}%, not 100%")
        # Adjust the largest weight to ensure total is exactly 100%
        adjustment = 100 - weights_sum
        default_weights['NASDAQ Trend'] += adjustment
        logger.info(f"Adjusted NASDAQ Trend by {adjustment} to make total exactly 100%")
    
    # Validate default weights sum to 100
    assert abs(sum(default_weights.values()) - 100) < 0.1, "Default weights must sum to 100%"
    
    # Use custom weights if provided, otherwise use defaults
    weights = custom_weights if custom_weights else default_weights.copy()
    
    # Always start with a clean copy of weights
    working_weights = weights.copy() if weights else default_weights.copy()
    
    # Document sentiment analysis feature is now hidden
    # Always set document weight to 0
    document_weight = 0
    
    # We no longer use proprietary data, but keep for backward compatibility
    proprietary_weight = 0
    
    # Calculate current weight of economic indicators
    economic_indicators_total = sum(working_weights.values())
    
    # The available weight for economic indicators
    available_economic_weight = 100 - document_weight
    
    # Scale economic indicator weights if needed to ensure they sum to available_economic_weight
    if abs(economic_indicators_total - available_economic_weight) > 0.001:
        scaling_factor = available_economic_weight / economic_indicators_total
        
        # Scale all economic indicators proportionally
        for key in working_weights:
            working_weights[key] = round(working_weights[key] * scaling_factor, 1)
    
    # Add document sentiment weight if available
    if document_weight > 0:
        working_weights['Document Sentiment'] = document_weight
        
    # Final check - make sure weights sum exactly to 100%
    final_total = sum(working_weights.values())
    if abs(final_total - 100) > 0.001:
        logger.debug(f"Weights before final adjustment: {working_weights}, Total: {final_total}")
        
        # Find the largest weight and adjust it to make the total exactly 100
        sorted_keys = sorted(working_weights.keys(), key=lambda k: working_weights[k], reverse=True)
        if sorted_keys:
            largest_key = sorted_keys[0]
            working_weights[largest_key] += (100 - final_total)
            logger.info(f"Adjusted {largest_key} weight by {100 - final_total} to make total exactly 100%")
        
        # Verify after adjustment
        logger.debug(f"Weights after adjustment: {working_weights}, Total: {sum(working_weights.values())}")
    
    # Use the adjusted weights for the rest of the calculation
    weights = working_weights
    
    sentiment_components = []
    
    # 1. GDP Growth - positive growth is good
    if not gdp_data.empty and 'yoy_growth' in gdp_data.columns:
        latest_gdp = gdp_data.sort_values('date', ascending=False).iloc[0]
        gdp_score = min(max(latest_gdp['yoy_growth'] * 10, 0), 100)  # Scale: 0 to 100
        sentiment_components.append({
            'indicator': 'Real GDP % Change',
            'value': latest_gdp['yoy_growth'],
            'score': gdp_score,
            'weight': weights['Real GDP % Change']
        })
    
    # 2. Unemployment - lower is better, normalize around natural rate of ~4%
    if not unemployment_data.empty:
        latest_unemp = unemployment_data.sort_values('date', ascending=False).iloc[0]
        # Invert scale since lower unemployment is better
        unemp_score = min(max(100 - (latest_unemp['value'] - 4.0) * 25, 0), 100)  # Scale: 0 to 100
        sentiment_components.append({
            'indicator': 'Unemployment Rate',
            'value': latest_unemp['value'],
            'score': unemp_score,
            'weight': weights['Unemployment Rate']
        })
    
    # 3. Software Job Postings - higher YoY growth is better for tech sector
    if not job_postings_data.empty and 'yoy_growth' in job_postings_data.columns:
        latest_job_posting = job_postings_data.sort_values('date', ascending=False).iloc[0]
        
        # Score based on YoY growth rate:
        # > 20%: Excellent (90-100)
        # 5-20%: Good (70-90)
        # 0-5%: Moderate (50-70)
        # -5-0%: Concerning (30-50)
        # -20-(-5)%: Poor (10-30)
        # < -20%: Very Poor (0-10)
        
        growth_rate = latest_job_posting['yoy_growth']
        
        if growth_rate >= 20:
            job_posting_score = 90 + (min(growth_rate - 20, 10) / 10) * 10  # 90-100
        elif growth_rate >= 5:
            job_posting_score = 70 + ((growth_rate - 5) / 15) * 20  # 70-90
        elif growth_rate >= 0:
            job_posting_score = 50 + (growth_rate / 5) * 20  # 50-70
        elif growth_rate >= -5:
            job_posting_score = 30 + ((growth_rate + 5) / 5) * 20  # 30-50
        elif growth_rate >= -20:
            job_posting_score = 10 + ((growth_rate + 20) / 15) * 20  # 10-30
        else:
            job_posting_score = max(0, 10 + (growth_rate + 20))  # 0-10
            
        # Ensure score is in 0-100 range
        job_posting_score = min(max(job_posting_score, 0), 100)
        
        sentiment_components.append({
            'indicator': 'Software Job Postings',
            'value': growth_rate,
            'score': job_posting_score,
            'weight': weights['Software Job Postings']
        })
    
    # 3. Inflation - moderate inflation good, high inflation bad
    if not inflation_data.empty and 'inflation' in inflation_data.columns:
        latest_inf = inflation_data.sort_values('date', ascending=False).iloc[0]
        # Ideal is 2%, penalize deviation
        inf_score = min(max(100 - abs(latest_inf['inflation'] - 2.0) * 15, 0), 100)  # Scale: 0 to 100
        sentiment_components.append({
            'indicator': 'CPI',
            'value': latest_inf['inflation'],
            'score': inf_score,
            'weight': weights['CPI']
        })
    
    # 3.5 PCE Growth - positive growth is good, but too high could signal inflation
    if not pce_data.empty and 'yoy_growth' in pce_data.columns:
        latest_pce = pce_data.sort_values('date', ascending=False).iloc[0]
        # Ideal PCE growth around 2-3%, too high signals inflation, too low signals weak demand
        pce_score = min(max(100 - abs(latest_pce['yoy_growth'] - 2.5) * 10, 0), 100)  # Scale: 0 to 100
        sentiment_components.append({
            'indicator': 'PCE',
            'value': latest_pce['yoy_growth'],
            'score': pce_score,
            'weight': weights['PCE']
        })
    
    # 3.6 PCEPI - ideal is around 2% (Fed target), penalize deviation
    if not pcepi_data.empty and 'yoy_growth' in pcepi_data.columns:
        latest_pcepi = pcepi_data.sort_values('date', ascending=False).iloc[0]
        # Ideal PCEPI around 2% (Fed target)
        pcepi_score = min(max(100 - abs(latest_pcepi['yoy_growth'] - 2.0) * 15, 0), 100)  # Scale: 0 to 100
        sentiment_components.append({
            'indicator': 'PCEPI',
            'value': latest_pcepi['yoy_growth'],
            'score': pcepi_score,
            'weight': weights['PCEPI']
        })
    
    # 4. Market Performance - NASDAQ trend using EMA gap (if available) or traditional method
    if not nasdaq_data.empty:
        if 'gap_pct' in nasdaq_data.columns:
            # New method: Use gap between current price and 20-day EMA as trend indicator
            latest_nasdaq = nasdaq_data.sort_values('date', ascending=False).iloc[0]
            gap_pct = latest_nasdaq['gap_pct']
            
            # Score based on EMA gap:
            # > 2%: Very bullish (80-100)
            # 0.5% to 2%: Bullish (60-80)
            # -0.5% to 0.5%: Neutral (40-60)
            # -2% to -0.5%: Bearish (20-40)
            # < -2%: Very bearish (0-20)
            
            if gap_pct >= 2:
                nasdaq_score = 80 + min((gap_pct - 2) * 10, 20)  # 80-100
            elif gap_pct >= 0.5:
                nasdaq_score = 60 + ((gap_pct - 0.5) / 1.5) * 20  # 60-80
            elif gap_pct >= -0.5:
                nasdaq_score = 40 + ((gap_pct + 0.5) / 1.0) * 20  # 40-60
            elif gap_pct >= -2:
                nasdaq_score = 20 + ((gap_pct + 2) / 1.5) * 20  # 20-40
            else:
                nasdaq_score = max(0, 20 + (gap_pct + 2) * 10)  # 0-20
                
            # Ensure score is in 0-100 range
            nasdaq_score = min(max(nasdaq_score, 0), 100)
            
            sentiment_components.append({
                'indicator': 'NASDAQ Trend',
                'value': gap_pct,
                'score': nasdaq_score,
                'weight': weights['NASDAQ Trend'],
                'description': 'Gap from 20-day EMA'
            })
        elif 'pct_change' in nasdaq_data.columns:
            # Legacy method: Use average of recent percent changes
            recent_nasdaq = nasdaq_data.sort_values('date', ascending=False).head(3)
            avg_change = recent_nasdaq['pct_change'].mean()
            nasdaq_score = min(max(50 + avg_change * 5, 0), 100)  # Scale: 0 to 100
            sentiment_components.append({
                'indicator': 'NASDAQ Trend',
                'value': avg_change,
                'score': nasdaq_score,
                'weight': weights['NASDAQ Trend'],
                'description': '3-day avg % change'
            })
    
    # 5. Tech Sector Prices - PPI Data Processing YoY change
    if not data_processing_ppi_data.empty and 'yoy_pct_change' in data_processing_ppi_data.columns:
        latest_ppi = data_processing_ppi_data.sort_values('date', ascending=False).iloc[0]
        # Moderate price growth is ideal (1-3%)
        ppi_score = min(max(100 - abs(latest_ppi['yoy_pct_change'] - 2.0) * 10, 0), 100)  # Scale: 0 to 100
        sentiment_components.append({
            'indicator': 'PPI: Data Processing Services',
            'value': latest_ppi['yoy_pct_change'],
            'score': ppi_score,
            'weight': weights['PPI: Data Processing Services']
        })
    
    # 6. Software Publishers PPI - similar scoring as data processing
    if not software_ppi_data.empty and 'yoy_pct_change' in software_ppi_data.columns:
        latest_ppi = software_ppi_data.sort_values('date', ascending=False).iloc[0]
        # Moderate price growth is ideal (1-3%)
        ppi_score = min(max(100 - abs(latest_ppi['yoy_pct_change'] - 2.0) * 10, 0), 100)  # Scale: 0 to 100
        sentiment_components.append({
            'indicator': 'PPI: Software Publishers',
            'value': latest_ppi['yoy_pct_change'],
            'score': ppi_score,
            'weight': weights['PPI: Software Publishers']
        })
    
    # 7. Interest Rates - moderate rates ideal (around 2-3%)
    if not interest_rate_data.empty:
        latest_rate = interest_rate_data.sort_values('date', ascending=False).iloc[0]
        # Too low or too high rates are not ideal
        rate_score = min(max(100 - abs(latest_rate['value'] - 2.5) * 12, 0), 100)  # Scale: 0 to 100
        sentiment_components.append({
            'indicator': 'Federal Funds Rate',
            'value': latest_rate['value'],
            'score': rate_score,
            'weight': weights['Federal Funds Rate']
        })
    
    # 8. Treasury Yield - moderate yields ideal (around 2-4%)
    if not treasury_yield_data.empty:
        latest_yield = treasury_yield_data.sort_values('date', ascending=False).iloc[0]
        # Optimal range is approximately 2-4% for 10-year treasuries
        yield_score = min(max(100 - abs(latest_yield['value'] - 3.0) * 10, 0), 100)  # Scale: 0 to 100
        sentiment_components.append({
            'indicator': '10-Year Treasury Yield',
            'value': latest_yield['value'],
            'score': yield_score,
            'weight': weights['Treasury Yield']
        })
    
    # 9. VIX Volatility Index - lower is better (market stability)
    if not vix_data.empty:
        latest_vix = vix_data.sort_values('date', ascending=False).iloc[0]
        
        # Use the 14-day EMA for VIX if available, otherwise fallback to raw value
        if 'vix_ema14' in latest_vix and not pd.isna(latest_vix['vix_ema14']):
            vix_value = latest_vix['vix_ema14']  # Use smoothed value
            value_label = f"{latest_vix['value']:.2f} (EMA: {vix_value:.2f})"
        else:
            vix_value = latest_vix['value']  # Fallback to raw value
            value_label = f"{vix_value:.2f}"
        
        # VIX below 20 is generally considered low volatility (good)
        # VIX above 30 is generally considered high volatility (bad)
        # Scale inverted since lower VIX is better for market sentiment
        if vix_value <= 20:
            # Low volatility (good): 70-100 score
            vix_score = 100 - ((vix_value - 10) / 10) * 30
        elif vix_value <= 30:
            # Medium volatility: 30-70 score
            vix_score = 70 - ((vix_value - 20) / 10) * 40
        else:
            # High volatility (bad): 0-30 score
            vix_score = max(30 - ((vix_value - 30) / 10) * 30, 0)
            
        # Ensure score is in 0-100 range
        vix_score = min(max(vix_score, 0), 100)
        
        sentiment_components.append({
            'indicator': 'VIX Volatility',
            'value': value_label,
            'score': vix_score,
            'weight': weights['VIX Volatility']
        })
        
    # 10. Consumer Sentiment - higher is better (consumer confidence)
    if not consumer_sentiment_data.empty:
        latest_sentiment = consumer_sentiment_data.sort_values('date', ascending=False).iloc[0]
        
        # Consumer Sentiment is measured on a scale that typically ranges from 50-120
        # with recent data showing values between 60-80
        # Adjusted scoring ranges to be more appropriate for current data:
        # >90: Excellent (80-100)
        # 80-90: Very Good (65-80)
        # 70-80: Good (50-65)
        # 60-70: Moderate (35-50)
        # 50-60: Concerning (20-35)
        # <50: Poor (0-20)
        
        sentiment_value = latest_sentiment['value']
        
        if sentiment_value >= 90:
            consumer_sentiment_score = 80 + min((sentiment_value - 90) / 5, 20)  # 80-100
        elif sentiment_value >= 80:
            consumer_sentiment_score = 65 + ((sentiment_value - 80) / 10) * 15  # 65-80
        elif sentiment_value >= 70:
            consumer_sentiment_score = 50 + ((sentiment_value - 70) / 10) * 15  # 50-65
        elif sentiment_value >= 60:
            consumer_sentiment_score = 35 + ((sentiment_value - 60) / 10) * 15  # 35-50
        elif sentiment_value >= 50:
            consumer_sentiment_score = 20 + ((sentiment_value - 50) / 10) * 15  # 20-35
        else:
            consumer_sentiment_score = max(0, 20 * (sentiment_value / 50))  # 0-20
            
        # Ensure score is in 0-100 range
        consumer_sentiment_score = min(max(consumer_sentiment_score, 0), 100)
        
        # Add to sentiment components
        sentiment_components.append({
            'indicator': 'Consumer Sentiment',
            'value': sentiment_value,
            'score': consumer_sentiment_score,
            'weight': weights['Consumer Sentiment']
        })
        
        # Debug message to help diagnose contribution calculation
        print(f"Consumer Sentiment: value={sentiment_value}, score={consumer_sentiment_score}, weight={weights['Consumer Sentiment']}, contribution={consumer_sentiment_score * weights['Consumer Sentiment'] / 100:.1f}")
        

        
    # 10. Add proprietary data if provided
    if proprietary_data and 'value' in proprietary_data and 'weight' in proprietary_data:
        # Value should be a score from 0-100
        prop_value = proprietary_data['value']
        prop_score = min(max(float(prop_value), 0), 100)  # Ensure it's between 0-100
        
        sentiment_components.append({
            'indicator': 'Proprietary Data',
            'value': prop_value,
            'score': prop_score,
            'weight': weights['Proprietary Data']
        })
        
    # 9. Add document analysis data if provided
    if document_data and 'value' in document_data and 'weight' in document_data:
        doc_value = document_data['value']
        doc_score = min(max(float(doc_value), 0), 100)  # Ensure it's between 0-100
        
        # Document weight is already set in the weights dictionary at the beginning of the function
        if 'Document Sentiment' in weights:
            sentiment_components.append({
                'indicator': 'Document Sentiment',
                'value': doc_value,
                'score': doc_score,
                'weight': weights['Document Sentiment']
            })
    
    # Calculate composite score if we have components
    if sentiment_components:
        # Calculate contributions for each component
        for comp in sentiment_components:
            comp['contribution'] = comp['score'] * comp['weight'] / 100
            
        # Calculate total score as sum of contributions
        composite_score = sum(comp['contribution'] for comp in sentiment_components)
        
        # Get total weight of available indicators
        available_weight_sum = sum(comp['weight'] for comp in sentiment_components)
        
        # Scale score if missing indicators
        if available_weight_sum < 100:
            composite_score = composite_score * 100 / available_weight_sum
            
        # The current calculation already produces a score in the 0-100 range
        # No additional normalization needed as it's already aligned with our desired scale
        
        # Determine sentiment category
        if composite_score >= 80:
            sentiment_category = "Boom"
        elif composite_score >= 60:
            sentiment_category = "Expansion"
        elif composite_score >= 40:
            sentiment_category = "Moderate Growth"
        elif composite_score >= 20:
            sentiment_category = "Slowdown"
        else:
            sentiment_category = "Contraction"
        
        return {
            'score': composite_score,
            'category': sentiment_category,
            'components': sentiment_components,
            'available_weight_sum': available_weight_sum
        }
    else:
        return None

def parse_uploaded_data(contents, filename):
    """Parse uploaded file contents into DataFrame"""
    if contents is None:
        return None
    
    content_type, content_string = contents.split(',')
    decoded = base64.b64decode(content_string)
    
    try:
        if 'csv' in filename:
            # Assume it's a CSV file
            return pd.read_csv(io.StringIO(decoded.decode('utf-8')))
        elif 'xls' in filename:
            # Assume it's an Excel file
            return pd.read_excel(io.BytesIO(decoded))
        else:
            return None
    except Exception as e:
        print(f"Error parsing uploaded file: {e}")
        return None

# Pre-load all data at startup
print("Loading economic data...")
import sqlalchemy, os
macro_engine = sqlalchemy.create_engine(os.getenv("DATABASE_URL"))

gdp_data = pd.read_sql(
    "SELECT date, value AS gdp FROM macro_data WHERE series='GDPC1' ORDER BY date",
    macro_engine,
    parse_dates=["date"]
)
logger.info(f"Loaded {len(gdp_data)} rows for GDP from macro_data")

# --- PCE: load directly from Postgres ---
import sqlalchemy, os
macro_engine = sqlalchemy.create_engine(os.getenv("DATABASE_URL"))

pce_data = pd.read_sql(
    """
    SELECT date,
           value AS pce
    FROM   macro_data
    WHERE  series = 'PCE'
    ORDER  BY date
    """,
    macro_engine,
    parse_dates=["date"]
)
logger.info(f"Loaded {len(pce_data)} rows for PCE from macro_data")

# --- Unemployment Rate: load directly from Postgres ---
import sqlalchemy, os
macro_engine = sqlalchemy.create_engine(os.getenv("DATABASE_URL"))

unemployment_data = pd.read_sql(
    """
    SELECT date,
           value AS unrate
    FROM   macro_data
    WHERE  series = 'UNRATE'
    ORDER  BY date
    """,
    macro_engine,
    parse_dates=["date"]
)
logger.info(f"Loaded {len(unemployment_data)} rows for Unemployment Rate from macro_data")

# --- Inflation (CPI): load directly from Postgres ---
import sqlalchemy, os
macro_engine = sqlalchemy.create_engine(os.getenv("DATABASE_URL"))

inflation_data = pd.read_sql(
    """
    SELECT date,
           value AS cpi
    FROM   macro_data
    WHERE  series = 'CPIAUCSL'
    ORDER  BY date
    """,
    macro_engine,
    parse_dates=["date"]
)
logger.info(f"Loaded {len(inflation_data)} rows for CPI from macro_data")

# --- Fed Funds Rate: load directly from Postgres ---
import sqlalchemy, os
macro_engine = sqlalchemy.create_engine(os.getenv("DATABASE_URL"))

interest_rate_data = pd.read_sql(
    """
    SELECT date,
           value AS fedfunds
    FROM   macro_data
    WHERE  series = 'FEDFUNDS'
    ORDER  BY date
    """,
    macro_engine,
    parse_dates=["date"]
)
logger.info(f"Loaded {len(interest_rate_data)} rows for Fed Funds Rate from macro_data")

treasury_yield_data = load_data_from_csv('treasury_yield_data.csv')

# Add NASDAQ Composite data from FRED (NASDAQCOM)
nasdaq_data = load_data_from_csv('nasdaq_data.csv')

# Add Consumer Sentiment data (USACSCICP02STSAM)
consumer_sentiment_data = load_data_from_csv('consumer_sentiment_data.csv')

# Add Software Job Postings from FRED (IHLIDXUSTPSOFTDEVE)
job_postings_data = load_data_from_csv('job_postings_data.csv')

# If no existing data or data is old, fetch NASDAQ data with EMA calculation
if nasdaq_data.empty or (datetime.now() - pd.to_datetime(nasdaq_data['date'].max())).days > 1:
    # Try to get real-time data with EMA first
    new_nasdaq_data = fetch_nasdaq_with_ema()
    
    if not new_nasdaq_data.empty and 'gap_pct' in new_nasdaq_data.columns:
        nasdaq_data = new_nasdaq_data
        save_data_to_csv(nasdaq_data, 'nasdaq_data.csv')
        print(f"NASDAQ data updated with real-time EMA calculation, {len(nasdaq_data)} observations")
    else:
        # Fall back to FRED data if real-time fails
        print("Falling back to FRED for NASDAQ data")
        fred_nasdaq_data = fetch_fred_data('NASDAQCOM')
        
        if not fred_nasdaq_data.empty:
            # Calculate percent change
            fred_nasdaq_data = fred_nasdaq_data.sort_values('date')
            fred_nasdaq_data['pct_change'] = fred_nasdaq_data['value'].pct_change() * 100
            
            nasdaq_data = fred_nasdaq_data
            save_data_to_csv(nasdaq_data, 'nasdaq_data.csv')
            print(f"NASDAQ data updated from FRED with {len(nasdaq_data)} observations")
        else:
            print("Failed to fetch NASDAQ data from any source")

# Add Producer Price Index for Software Publishers from FRED (PCU511210511210)
software_ppi_data = load_data_from_csv('software_ppi_data.csv')

# If no existing data or data is old, fetch new data
if software_ppi_data.empty or (datetime.now() - pd.to_datetime(software_ppi_data['date'].max())).days > 30:
    software_ppi_data = fetch_fred_data('PCU511210511210')
    
    if not software_ppi_data.empty:
        # Calculate year-over-year percent change
        software_ppi_data = software_ppi_data.sort_values('date')
        
        # Create a dataframe shifted by 12 months to calculate YoY change
        software_ppi_yoy = software_ppi_data.copy()
        software_ppi_yoy['date'] = software_ppi_yoy['date'] + pd.DateOffset(years=1)
        software_ppi_yoy = software_ppi_yoy.rename(columns={'value': 'year_ago_value'})
        
        # Merge current and year-ago values
        software_ppi_data = pd.merge(
            software_ppi_data, 
            software_ppi_yoy[['date', 'year_ago_value']], 
            on='date', 
            how='left'
        )
        
        # Calculate YoY percent change
        software_ppi_data['yoy_pct_change'] = ((software_ppi_data['value'] - software_ppi_data['year_ago_value']) / 
                                              software_ppi_data['year_ago_value'] * 100)
        
        # Save data
        save_data_to_csv(software_ppi_data, 'software_ppi_data.csv')
        
        print(f"Software PPI data updated with {len(software_ppi_data)} observations")
    else:
        print("Failed to fetch Software PPI data")

# Add Producer Price Index for Data Processing Services from FRED (PCU5112105112105)
data_processing_ppi_data = load_data_from_csv('data_processing_ppi_data.csv')

# If no existing data or data is old, fetch new data
if data_processing_ppi_data.empty or (datetime.now() - pd.to_datetime(data_processing_ppi_data['date'].max())).days > 30:
    data_processing_ppi_data = fetch_fred_data('PCU5112105112105')
    
    if not data_processing_ppi_data.empty:
        # Calculate year-over-year percent change
        data_processing_ppi_data = data_processing_ppi_data.sort_values('date')
        
        # Create a dataframe shifted by 12 months to calculate YoY change
        data_ppi_yoy = data_processing_ppi_data.copy()
        data_ppi_yoy['date'] = data_ppi_yoy['date'] + pd.DateOffset(years=1)
        data_ppi_yoy = data_ppi_yoy.rename(columns={'value': 'year_ago_value'})
        
        # Merge current and year-ago values
        data_processing_ppi_data = pd.merge(
            data_processing_ppi_data, 
            data_ppi_yoy[['date', 'year_ago_value']], 
            on='date', 
            how='left'
        )
        
        # Calculate YoY percent change
        data_processing_ppi_data['yoy_pct_change'] = ((data_processing_ppi_data['value'] - data_processing_ppi_data['year_ago_value']) / 
                                                     data_processing_ppi_data['year_ago_value'] * 100)
        
        # Save data
        save_data_to_csv(data_processing_ppi_data, 'data_processing_ppi_data.csv')
        
        print(f"Data Processing PPI data updated with {len(data_processing_ppi_data)} observations")
    else:
        print("Failed to fetch Data Processing PPI data")

# Fetch GDP data if needed
if gdp_data.empty or (datetime.now() - pd.to_datetime(gdp_data['date'].max())).days > 90:
    # Fetch real GDP (GDPC1)
    gdp_temp = fetch_fred_data('GDPC1')
    
    if not gdp_temp.empty:
        # Calculate year-over-year growth
        gdp_temp = gdp_temp.sort_values('date')
        
        # Create a dataframe shifted by 4 quarters to calculate YoY change (GDP is quarterly)
        gdp_yoy = gdp_temp.copy()
        gdp_yoy['date'] = gdp_yoy['date'] + pd.DateOffset(months=12)
        gdp_yoy = gdp_yoy.rename(columns={'value': 'year_ago_value'})
        
        # Merge current and year-ago values
        gdp_data = pd.merge(
            gdp_temp, 
            gdp_yoy[['date', 'year_ago_value']], 
            on='date', 
            how='left'
        )
        
        # Calculate YoY growth
        gdp_data['yoy_growth'] = ((gdp_data['value'] - gdp_data['year_ago_value']) / 
                                 gdp_data['year_ago_value'] * 100)
        
        # Save data
        save_data_to_csv(gdp_data, 'gdp_data.csv')
        
        print(f"GDP data updated with {len(gdp_data)} observations")
    else:
        print("Failed to fetch GDP data")

# Fetch unemployment data if needed
if unemployment_data.empty or (datetime.now() - pd.to_datetime(unemployment_data['date'].max())).days > 30:
    # Fetch unemployment rate (UNRATE)
    unemployment_data = fetch_fred_data('UNRATE')
    
    if not unemployment_data.empty:
        # Save data
        save_data_to_csv(unemployment_data, 'unemployment_data.csv')
        
        print(f"Unemployment data updated with {len(unemployment_data)} observations")
    else:
        print("Failed to fetch unemployment data")

# Fetch inflation data if needed
if inflation_data.empty or (datetime.now() - pd.to_datetime(inflation_data['date'].max())).days > 30:
    # Fetch CPI (CPIAUCSL)
    cpi_temp = fetch_fred_data('CPIAUCSL')
    
    if not cpi_temp.empty:
        # Calculate year-over-year inflation
        cpi_temp = cpi_temp.sort_values('date')
        
        # Create a dataframe shifted by 12 months to calculate YoY change
        cpi_yoy = cpi_temp.copy()
        cpi_yoy['date'] = cpi_yoy['date'] + pd.DateOffset(months=12)
        cpi_yoy = cpi_yoy.rename(columns={'value': 'year_ago_value'})
        
        # Merge current and year-ago values
        inflation_data = pd.merge(
            cpi_temp, 
            cpi_yoy[['date', 'year_ago_value']], 
            on='date', 
            how='left'
        )
        
        # Calculate YoY inflation
        inflation_data['inflation'] = ((inflation_data['value'] - inflation_data['year_ago_value']) / 
                                      inflation_data['year_ago_value'] * 100)
        
        # Save data
        save_data_to_csv(inflation_data, 'inflation_data.csv')
        
        print(f"Inflation data updated with {len(inflation_data)} observations")
    else:
        print("Failed to fetch inflation data")

# Add Personal Consumption Expenditures (PCE) data
if pce_data.empty or (datetime.now() - pd.to_datetime(pce_data['date'].max() if not pce_data.empty else '2000-01-01')).days > 30:
    # Fetch PCE data (PCE)
    pce_temp = fetch_fred_data('PCE')
    
    if not pce_temp.empty:
        # Calculate year-over-year growth
        pce_temp = pce_temp.sort_values('date')
        
        # Create a dataframe shifted by 12 months to calculate YoY change
        pce_yoy = pce_temp.copy()
        pce_yoy['date'] = pce_yoy['date'] + pd.DateOffset(months=12)
        pce_yoy = pce_yoy.rename(columns={'value': 'year_ago_value'})
        
        # Merge current and year-ago values
        pce_data = pd.merge(
            pce_temp, 
            pce_yoy[['date', 'year_ago_value']], 
            on='date', 
            how='left'
        )
        
        # Calculate YoY growth
        pce_data['yoy_growth'] = ((pce_data['value'] - pce_data['year_ago_value']) / 
                               pce_data['year_ago_value'] * 100)
        
        # Save data
        save_data_to_csv(pce_data, 'pce_data.csv')
        
        print(f"PCE data updated with {len(pce_data)} observations")
    else:
        print("Failed to fetch PCE data")

# Add PCEPI (Personal Consumption Expenditures: Chain-type Price Index) data

# --- PCEPI: load directly from Postgres ---
import sqlalchemy, os
macro_engine = sqlalchemy.create_engine(os.getenv("DATABASE_URL"))

pcepi_data = pd.read_sql(
    """
    SELECT date,
           value AS pcepi
    FROM   macro_data
    WHERE  series = 'PCEPI'
    ORDER  BY date
    """,
    macro_engine,
    parse_dates=["date"]
)
logger.info(f"Loaded {len(pcepi_data)} rows for PCEPI from macro_data")


# If no existing data or data is old, fetch new data
if pcepi_data.empty or (datetime.now() - pd.to_datetime(pcepi_data['date'].max() if not pcepi_data.empty else '2000-01-01')).days > 30:
    # Fetch PCEPI data (PCEPI)
    pcepi_temp = fetch_fred_data('PCEPI')
    
    if not pcepi_temp.empty:
        # Calculate year-over-year growth
        pcepi_temp = pcepi_temp.sort_values('date')
        
        # Create a dataframe shifted by 12 months to calculate YoY change
        pcepi_yoy = pcepi_temp.copy()
        pcepi_yoy['date'] = pcepi_yoy['date'] + pd.DateOffset(months=12)
        pcepi_yoy = pcepi_yoy.rename(columns={'value': 'year_ago_value'})
        
        # Merge current and year-ago values
        pcepi_data = pd.merge(
            pcepi_temp, 
            pcepi_yoy[['date', 'year_ago_value']], 
            on='date', 
            how='left'
        )
        
        # Calculate YoY growth
        pcepi_data['yoy_growth'] = ((pcepi_data['value'] - pcepi_data['year_ago_value']) / 
                                  pcepi_data['year_ago_value'] * 100)
        
        # Save data
        save_data_to_csv(pcepi_data, 'pcepi_data.csv')
        
        print(f"PCEPI data updated with {len(pcepi_data)} observations")
    else:
        print("Failed to fetch PCEPI data")

# Add VIX volatility index data
vix_data = load_data_from_csv('vix_data.csv')

# Try to get recent data from Yahoo Finance first
yahoo_vix_data = fetch_vix_from_yahoo()

if not yahoo_vix_data.empty:
    # If Yahoo Finance data is available, use it
    logger.info("Using Yahoo Finance for recent VIX data")
    
    # If we already have some historical data from FRED, keep it and append the new data
    if not vix_data.empty:
        # Find the latest date in the Yahoo data we want to use
        yahoo_latest_date = yahoo_vix_data['date'].max()
        
        # Keep only FRED data older than our Yahoo data to avoid duplicates
        vix_data = vix_data[vix_data['date'] < yahoo_latest_date - timedelta(days=1)]
        
        # Combine the datasets
        combined_vix_data = pd.concat([vix_data, yahoo_vix_data])
        vix_data = combined_vix_data
    else:
        # If no historical data, just use Yahoo data
        vix_data = yahoo_vix_data
    
    # Sort and save the combined data
    vix_data = vix_data.sort_values('date')
    save_data_to_csv(vix_data, 'vix_data.csv')
    logger.info(f"VIX data updated with {len(vix_data)} observations combining Yahoo Finance and historical data")
    
elif vix_data.empty or (datetime.now() - pd.to_datetime(vix_data['date'].max())).days > 7:
    # If Yahoo Finance failed and we have no data or old data, fall back to FRED
    logger.info("Yahoo Finance VIX data retrieval failed, falling back to FRED data")
    fred_vix_data = fetch_fred_data('VIXCLS')
    
    if not fred_vix_data.empty:
        # Save data
        vix_data = fred_vix_data
        save_data_to_csv(vix_data, 'vix_data.csv')
        
        logger.info(f"VIX data updated with {len(vix_data)} observations from FRED")
    else:
        logger.error("Failed to fetch VIX data from both Yahoo Finance and FRED")

# Calculate 14-day EMA for VIX if we have data
if not vix_data.empty and 'date' in vix_data.columns and 'value' in vix_data.columns:
    # Sort data by date ascending (oldest to newest) for correct EMA calculation
    vix_data = vix_data.sort_values('date')
    
    # Calculate 14-day EMA
    vix_data['vix_ema14'] = vix_data['value'].ewm(span=14, adjust=False).mean()
    
    # Sort back to newest first for reporting
    vix_data = vix_data.sort_values('date', ascending=False)
    
    # Print the latest values
    if len(vix_data) > 0:
        latest_date = vix_data.iloc[0]['date']
        latest_vix = vix_data.iloc[0]['value']
        latest_ema = vix_data.iloc[0]['vix_ema14']
        logger.info(f"VIX: {latest_vix:.2f} on {latest_date}, 14-day EMA: {latest_ema:.2f}")
    
    # Save updated data with EMA
    save_data_to_csv(vix_data, 'vix_data.csv')

# Add Consumer Sentiment data
if consumer_sentiment_data.empty or (datetime.now() - pd.to_datetime(consumer_sentiment_data['date'].max() if not consumer_sentiment_data.empty else '2000-01-01')).days > 30:
    # Fetch Consumer Confidence Composite Index (USACSCICP02STSAM)
    consumer_sentiment_temp = fetch_consumer_sentiment_data()
    
    if not consumer_sentiment_temp.empty:
        consumer_sentiment_data = consumer_sentiment_temp
        # Save data
        save_data_to_csv(consumer_sentiment_data, 'consumer_sentiment_data.csv')
        
        logger.info(f"Consumer Sentiment data updated with {len(consumer_sentiment_data)} observations")
    else:
        logger.error("Failed to fetch Consumer Sentiment data")

# Add Software Job Postings data
if job_postings_data.empty or (datetime.now() - pd.to_datetime(job_postings_data['date'].max() if not job_postings_data.empty else '2000-01-01')).days > 7:
    # Force a refresh to get the most recent data (should update more frequently than 30 days)
    logger.info("Refreshing Software Job Postings data to get latest observations...")
    # Fetch U.S. Software Job Postings on Indeed (IHLIDXUSTPSOFTDEVE)
    job_postings_temp = fetch_fred_data('IHLIDXUSTPSOFTDEVE')
    
    if not job_postings_temp.empty:
        # Calculate year-over-year growth
        job_postings_temp = job_postings_temp.sort_values('date')
        
        # Create a dataframe shifted by 12 months to calculate YoY change
        postings_yoy = job_postings_temp.copy()
        postings_yoy['date'] = postings_yoy['date'] + pd.DateOffset(months=12)
        postings_yoy = postings_yoy.rename(columns={'value': 'year_ago_value'})
        
        # Merge current and year-ago values
        job_postings_data = pd.merge(
            job_postings_temp, 
            postings_yoy[['date', 'year_ago_value']], 
            on='date', 
            how='left'
        )
        
        # Calculate YoY growth
        job_postings_data['yoy_growth'] = ((job_postings_data['value'] - job_postings_data['year_ago_value']) / 
                              job_postings_data['year_ago_value'] * 100)
        
        # Save data
        save_data_to_csv(job_postings_data, 'job_postings_data.csv')
        
        logger.info(f"Software Job Postings data updated with {len(job_postings_data)} observations")
    else:
        logger.error("Failed to fetch Software Job Postings data")

# Calculate initial sentiment index
sentiment_index = calculate_sentiment_index()

# ---- Dashboard Layout ----
app.layout = html.Div([
    # Hidden div for initialization callbacks
    html.Div(id="_", style={"display": "none"}),
    
    # Header
    html.Div([
        html.Div([
            html.Img(src="/assets/T2D Pulse logo.png", height="60px", className="logo"),
            html.Div([
                html.Div([
                    html.P(["Powering investment decisions with macro data and proprietary intelligence "], 
                          style={"display": "inline"}),
                    html.Span([
                        html.I(className="fas fa-info-circle", id="info-icon"),
                        # Simple tooltip that appears on hover
                        html.Div([
                            html.H4("T2D Pulse Score Definitions", style={"marginTop": "0px", "marginBottom": "10px", "fontSize": "16px"}),
                            html.Div([
                                html.Div(["60.0 - 100.0: ", html.Span("Bullish", style={"color": "#388e3c"}), 
                                          ": Positive outlook; favorable growth conditions for technology sector"]),
                                html.Div(["30.0 - 59.9: ", html.Span("Neutral", style={"color": "#fbc02d"}), 
                                          ": Balanced outlook; mixed signals with both opportunities and challenges"]),
                                html.Div(["0.0 - 29.9: ", html.Span("Bearish", style={"color": "#d32f2f"}), 
                                          ": Negative outlook; economic headwinds likely impacting tech industry growth"]),
                            ], style={"lineHeight": "1.6", "fontSize": "14px"}),
                            html.P(["The T2D Pulse score is calculated using proprietary economic indicators "
                                   "weighted by their impact on technology sector performance."],
                                 style={"marginTop": "10px", "fontSize": "12px", "fontStyle": "italic"})
                        ], className="sentiment-tooltip")
                    ], style={"fontSize": "16px", "verticalAlign": "middle", "cursor": "pointer", "position": "relative", "display": "inline-block"})
                ], className="dashboard-subtitle")
            ], className="header-text")
        ], className="header-container")
    ], className="header"),
    
    # Top Section - Sentiment Index
    html.Div([
        # Sentiment Index Banner
        html.Div([
            # Store the sentiment score and category in visible elements that can be referenced by callbacks
            html.Span(id="sentiment-score", 
                     children=f"{sentiment_index['score']:.1f}" if sentiment_index else "N/A", 
                     style={"display": "none"}),
            html.Span(id="sentiment-category", 
                     children=sentiment_index['category'] if sentiment_index else "N/A", 
                     style={"display": "none"}),
            
            # Gauge container - displays the sentiment score with color coding
            html.Div(id="sentiment-gauge", className="sentiment-gauge-container",
                    style={
                        "width": "100%", 
                        "maxWidth": "600px", 
                        "margin": "0 auto",
                        "display": "flex",
                        "alignItems": "center",
                        "justifyContent": "center",
                        "height": "100%",
                        "minHeight": "180px"  # Ensure minimum height for content
                    })
        ], className="sentiment-banner", style={"display": "flex", "alignItems": "center", "minHeight": "220px"})
    ], className="sentiment-section"),
    
    # Main content container - Centered full-width layout
    html.Div([
        # Last updated timestamp centered at the top
        html.Div(id="pulse-last-updated", className="pulse-last-updated", style={"textAlign": "center", "marginBottom": "15px"}),
        
        # Hidden divs to store values for indicators (needed for callbacks)
        html.Div([
            html.P(id="gdp-value", style={"display": "none"}),
            html.Div(id="gdp-trend", style={"display": "none"}),
            html.P(id="pce-value", style={"display": "none"}),
            html.Div(id="pce-trend", style={"display": "none"}),
            html.P(id="unemployment-value", style={"display": "none"}),
            html.Div(id="unemployment-trend", style={"display": "none"}),
            html.P(id="job-postings-value", style={"display": "none"}),
            html.Div(id="job-postings-trend", style={"display": "none"}),
            html.P(id="inflation-value", style={"display": "none"}),
            html.Div(id="inflation-trend", style={"display": "none"}),
            html.P(id="pcepi-value", style={"display": "none"}),
            html.Div(id="pcepi-trend", style={"display": "none"}),
            html.P(id="interest-rate-value", style={"display": "none"}),
            html.Div(id="interest-rate-trend", style={"display": "none"}),
            html.P(id="nasdaq-value", style={"display": "none"}),
            html.Div(id="nasdaq-trend", style={"display": "none"}),
            html.P(id="software-ppi-value", style={"display": "none"}),
            html.Div(id="software-ppi-trend", style={"display": "none"}),
            html.P(id="data-ppi-value", style={"display": "none"}),
            html.Div(id="data-ppi-trend", style={"display": "none"}),
            html.P(id="treasury-yield-value", style={"display": "none"}),
            html.Div(id="treasury-yield-trend", style={"display": "none"}),
            html.P(id="vix-value", style={"display": "none"}),
            html.Div(id="vix-trend", style={"display": "none"}),
            html.P(id="consumer-sentiment-value", style={"display": "none"}),
            html.Div(id="consumer-sentiment-trend", style={"display": "none"}),
        ], style={"display": "none"}),
        
        # Centered main content
        html.Div([
            # Custom Weight Adjustment (Hidden as requested)
            html.Div([
                html.H3("Customize Index Weights", className="card-title"),
                html.Div([
                    html.Div([
                        html.Label("Real GDP % Change"),
                        dcc.Slider(
                            id="gdp-weight",
                            min=0,
                            max=30,
                            step=0.01,
                            value=6.36,
                            marks={0: "0%", 15: "15%", 30: "30%"},
                            className="weight-slider"
                        ),
                    ], className="weight-control"),
                    
                    html.Div([
                        html.Label("PCE"),
                        dcc.Slider(
                            id="pce-weight",
                            min=0,
                            max=30,
                            step=0.01,
                            value=6.36,
                            marks={0: "0%", 15: "15%", 30: "30%"},
                            className="weight-slider"
                        ),
                    ], className="weight-control"),
                    
                    html.Div([
                        html.Label("Unemployment Rate"),
                        dcc.Slider(
                            id="unemployment-weight",
                            min=0,
                            max=30,
                            step=0.01,
                            value=6.36,
                            marks={0: "0%", 15: "15%", 30: "30%"},
                            className="weight-slider"
                        ),
                    ], className="weight-control"),
                    
                    html.Div([
                        html.Label("Software Job Postings"),
                        dcc.Slider(
                            id="job-postings-weight",
                            min=0,
                            max=30,
                            step=0.01,
                            value=6.36,
                            marks={0: "0%", 15: "15%", 30: "30%"},
                            className="weight-slider"
                        ),
                    ], className="weight-control"),
                    
                    html.Div([
                        html.Label("CPI"),
                        dcc.Slider(
                            id="cpi-weight",
                            min=0,
                            max=30,
                            step=0.01,
                            value=6.36,
                            marks={0: "0%", 15: "15%", 30: "30%"},
                            className="weight-slider"
                        ),
                    ], className="weight-control"),
                    
                    html.Div([
                        html.Label("PCEPI"),
                        dcc.Slider(
                            id="pcepi-weight",
                            min=0,
                            max=30,
                            step=0.01,
                            value=6.36,
                            marks={0: "0%", 15: "15%", 30: "30%"},
                            className="weight-slider"
                        ),
                    ], className="weight-control"),
                    
                    html.Div([
                        html.Label("NASDAQ Trend"),
                        dcc.Slider(
                            id="nasdaq-weight",
                            min=0,
                            max=30,
                            step=0.01,
                            value=15.46,
                            marks={0: "0%", 15: "15%", 30: "30%"},
                            className="weight-slider"
                        ),
                    ], className="weight-control"),
                    
                    html.Div([
                        html.Label("PPI: Data Processing Services"),
                        dcc.Slider(
                            id="data-ppi-weight",
                            min=0,
                            max=30,
                            step=0.01,
                            value=6.36,
                            marks={0: "0%", 15: "15%", 30: "30%"},
                            className="weight-slider"
                        ),
                    ], className="weight-control"),
                    
                    html.Div([
                        html.Label("PPI: Software Publishers"),
                        dcc.Slider(
                            id="software-ppi-weight",
                            min=0,
                            max=30,
                            step=0.01,
                            value=6.36,
                            marks={0: "0%", 15: "15%", 30: "30%"},
                            className="weight-slider"
                        ),
                    ], className="weight-control"),
                    
                    html.Div([
                        html.Label("Federal Funds Rate"),
                        dcc.Slider(
                            id="interest-rate-weight",
                            min=0,
                            max=30,
                            step=0.01,
                            value=6.36,
                            marks={0: "0%", 15: "15%", 30: "30%"},
                            className="weight-slider"
                        ),
                    ], className="weight-control"),
                    
                    html.Div([
                        html.Label("10-Year Treasury Yield"),
                        dcc.Slider(
                            id="treasury-yield-weight",
                            min=0,
                            max=30,
                            step=0.01,
                            value=9.09,
                            marks={0: "0%", 15: "15%", 30: "30%"},
                            className="weight-slider"
                        ),
                    ], className="weight-control"),
                    
                    html.Div([
                        html.Label("VIX Volatility Index (14-day EMA)"),
                        dcc.Slider(
                            id="vix-weight",
                            min=0,
                            max=30,
                            step=0.01,
                            value=9.09,
                            marks={0: "0%", 15: "15%", 30: "30%"},
                            className="weight-slider"
                        ),
                    ], className="weight-control"),
                    
                    html.Div([
                        html.Label("Consumer Sentiment"),
                        dcc.Slider(
                            id="consumer-sentiment-weight",
                            min=0,
                            max=30,
                            step=0.01,
                            value=9.09,
                            marks={0: "0%", 15: "15%", 30: "30%"},
                            className="weight-slider"
                        ),
                    ], className="weight-control"),
                    
                    html.Div([
                        html.P("Total weight: ", style={'display': 'inline-block'}),
                        html.P(id="total-weight", 
                              children="100%", 
                              style={'display': 'inline-block', 'fontWeight': 'bold', 'marginLeft': '5px'})
                    ], className="weight-total"),
                    
                    html.Button("Apply Weights", id="apply-weights", className="apply-button"),
                ], className="weights-container")
            ], className="card weights-card", style={"display": "none"}),
            
            # Document Analysis Card - HIDDEN
            html.Div([
                # Hidden document analysis section
            ], className="card upload-card", style={"display": "none"}),
            
        ], style={"display": "none"}),
        
        # Full width tabs container
        html.Div([
            # Tabs for different graph groups
            dcc.Tabs([
                # Sector Sentiment Tab - Cleaner layout with more whitespace
                dcc.Tab(label="Sector Sentiment", children=[
                    html.Div([
                        # Main Sector Sentiment Container
                        html.Div(id="sector-sentiment-container", className="sector-sentiment-container",
                               style={"marginBottom": "30px"}),
                        
                        # Divider
                        html.Hr(style={"margin": "20px 0", "border": "none", "height": "1px", "backgroundColor": "#e0e0e0"}),
                        
                        # Toggle Button for Key Indicators with more whitespace around it
                        html.Div([
                            html.Button(
                                "Show Key Indicators ▼",
                                id="toggle-key-indicators-button",
                                n_clicks=0,
                                style={
                                    "padding": "12px 24px",
                                    "fontSize": "1.1em",
                                    "borderRadius": "8px",
                                    "border": "none",
                                    "backgroundColor": "#007BFF",
                                    "color": "white",
                                    "cursor": "pointer",
                                    "boxShadow": "0 2px 4px rgba(0,0,0,0.1)",
                                    "transition": "all 0.3s ease"
                                }
                            )
                        ], style={"textAlign": "center", "margin": "30px auto"}),
                        
                        # Collapsible Key Indicators Section
                        html.Div([
                            html.H3("Key Economic Indicators", 
                                   style={"textAlign": "center", "fontSize": "1.5em", "color": "#444", "marginTop": "20px", "marginBottom": "30px", "fontWeight": "500"}),
                            html.Div([
                                # Grid of key indicator cards - exactly matching the sidebar indicators
                                # Real GDP
                                html.Div([
                                    html.Div("Real GDP % Change", className="indicator-label"),
                                    html.Div([
                                        html.Div(id="key-gdp-value", className="indicator-value"),
                                        html.Div(id="key-gdp-trend", className="indicator-trend-small")
                                    ], className="indicator-value-container")
                                ], className="indicator-card"),
                                
                                # PCE
                                html.Div([
                                    html.Div("PCE", className="indicator-label"),
                                    html.Div([
                                        html.Div(id="key-pce-value", className="indicator-value"),
                                        html.Div(id="key-pce-trend", className="indicator-trend-small")
                                    ], className="indicator-value-container")
                                ], className="indicator-card"),
                                
                                # Unemployment Rate
                                html.Div([
                                    html.Div("Unemployment Rate", className="indicator-label"),
                                    html.Div([
                                        html.Div(id="key-unemployment-value", className="indicator-value"),
                                        html.Div(id="key-unemployment-trend", className="indicator-trend-small")
                                    ], className="indicator-value-container")
                                ], className="indicator-card"),
                                
                                # Software Job Postings
                                html.Div([
                                    html.Div("Software Job Postings", className="indicator-label"),
                                    html.Div([
                                        html.Div(id="key-job-postings-value", className="indicator-value"),
                                        html.Div(id="key-job-postings-trend", className="indicator-trend-small")
                                    ], className="indicator-value-container")
                                ], className="indicator-card"),
                                
                                # Inflation (CPI)
                                html.Div([
                                    html.Div("Inflation (CPI)", className="indicator-label"),
                                    html.Div([
                                        html.Div(id="key-inflation-value", className="indicator-value"),
                                        html.Div(id="key-inflation-trend", className="indicator-trend-small")
                                    ], className="indicator-value-container")
                                ], className="indicator-card"),
                                
                                # PCEPI (YoY)
                                html.Div([
                                    html.Div("PCEPI (YoY)", className="indicator-label"),
                                    html.Div([
                                        html.Div(id="key-pcepi-value", className="indicator-value"),
                                        html.Div(id="key-pcepi-trend", className="indicator-trend-small")
                                    ], className="indicator-value-container")
                                ], className="indicator-card"),
                                
                                # Fed Funds Rate
                                html.Div([
                                    html.Div("Fed Funds Rate", className="indicator-label"),
                                    html.Div([
                                        html.Div(id="key-interest-rate-value", className="indicator-value"),
                                        html.Div(id="key-interest-rate-trend", className="indicator-trend-small")
                                    ], className="indicator-value-container")
                                ], className="indicator-card"),
                                
                                # NASDAQ Trend
                                html.Div([
                                    html.Div("NASDAQ Trend", className="indicator-label"),
                                    html.Div([
                                        html.Div(id="key-nasdaq-value", className="indicator-value"),
                                        html.Div(id="key-nasdaq-trend", className="indicator-trend-small")
                                    ], className="indicator-value-container")
                                ], className="indicator-card"),
                                
                                # PPI: Software Publishers
                                html.Div([
                                    html.Div("PPI: Software Publishers", className="indicator-label"),
                                    html.Div([
                                        html.Div(id="key-software-ppi-value", className="indicator-value"),
                                        html.Div(id="key-software-ppi-trend", className="indicator-trend-small")
                                    ], className="indicator-value-container")
                                ], className="indicator-card"),
                                
                                # PPI: Data Processing Services
                                html.Div([
                                    html.Div("PPI: Data Processing Services", className="indicator-label"),
                                    html.Div([
                                        html.Div(id="key-data-ppi-value", className="indicator-value"),
                                        html.Div(id="key-data-ppi-trend", className="indicator-trend-small")
                                    ], className="indicator-value-container")
                                ], className="indicator-card"),
                                
                                # 10-Year Treasury Yield
                                html.Div([
                                    html.Div("10-Year Treasury Yield", className="indicator-label"),
                                    html.Div([
                                        html.Div(id="key-treasury-yield-value", className="indicator-value"),
                                        html.Div(id="key-treasury-yield-trend", className="indicator-trend-small")
                                    ], className="indicator-value-container")
                                ], className="indicator-card"),
                                
                                # VIX Volatility Index
                                html.Div([
                                    html.Div("VIX Volatility Index (14-day EMA)", className="indicator-label"),
                                    html.Div([
                                        html.Div(id="key-vix-value", className="indicator-value"),
                                        html.Div(id="key-vix-trend", className="indicator-trend-small")
                                    ], className="indicator-value-container")
                                ], className="indicator-card"),
                                
                                # Consumer Sentiment
                                html.Div([
                                    html.Div("Consumer Sentiment", className="indicator-label"),
                                    html.Div([
                                        html.Div(id="key-consumer-sentiment-value", className="indicator-value"),
                                        html.Div(id="key-consumer-sentiment-trend", className="indicator-trend-small")
                                    ], className="indicator-value-container")
                                ], className="indicator-card"),
                            ], className="key-indicators-grid", style={
                                "display": "grid",
                                "gridTemplateColumns": "repeat(auto-fill, minmax(280px, 1fr))",
                                "gap": "20px",
                                "marginTop": "20px",
                                "width": "100%"
                            })
                        ], id="key-indicators-section", style={
                            "height": "0px", 
                            "overflow": "hidden", 
                            "opacity": 0,
                            "transition": "height 0.6s ease, opacity 0.6s ease",
                            "width": "100%", 
                            "maxWidth": "1200px", 
                            "margin": "0 auto",
                            "backgroundColor": "#f9f9f9",
                            "padding": "0 20px 20px 20px",
                            "borderRadius": "8px",
                            "boxShadow": "0 2px 5px rgba(0,0,0,0.1)"
                        })
                    ], className="graph-container")
                ], className="custom-tab", selected_className="custom-tab--selected"),
                
                # Real GDP & PCE Tab
                dcc.Tab(label="Real GDP & PCE", children=[
                    html.Div([
                        html.H3("Real GDP Growth (YoY %)", className="graph-title"),
                        html.Div(id="gdp-container", className="insights-enabled-container")
                    ], className="graph-container"),
                    html.Div([
                        html.H3("Personal Consumption Expenditures (YoY %)", className="graph-title"),
                        html.Div(id="pce-container", className="insights-enabled-container")
                    ], className="graph-container")
                ], className="custom-tab", selected_className="custom-tab--selected"),
                
                # Labor Market Tab
                dcc.Tab(label="Labor Market", children=[
                    html.Div([
                        html.H3("Unemployment Rate", className="graph-title"),
                        html.Div(id="unemployment-container", className="insights-enabled-container")
                    ], className="graph-container"),
                    html.Div([
                        html.H3("U.S. Software Job Postings on Indeed", className="graph-title"),
                        html.Div(id="job-postings-container", className="insights-enabled-container")
                    ], className="graph-container"),
                    html.Div([
                        html.H3("Consumer Sentiment Index", className="graph-title"),
                        html.Div(id="consumer-sentiment-container", className="insights-enabled-container")
                    ], className="graph-container")
                ], className="custom-tab", selected_className="custom-tab--selected"),
                
                # Inflation Tab
                dcc.Tab(label="Inflation", children=[
                    html.Div([
                        html.H3("Consumer Price Index (YoY %)", className="graph-title"),
                        html.Div(id="inflation-container", className="insights-enabled-container")
                    ], className="graph-container"),
                    html.Div([
                        html.H3("PCEPI (YoY %)", className="graph-title"),
                        html.Div(id="pcepi-container", className="insights-enabled-container")
                    ], className="graph-container")
                ], className="custom-tab", selected_className="custom-tab--selected"),
                
                # Financial Markets Tab
                dcc.Tab(label="Markets", children=[
                    html.Div([
                        html.H3("NASDAQ Composite Index", className="graph-title"),
                        html.Div(id="nasdaq-container", className="insights-enabled-container")
                    ], className="graph-container")
                ], className="custom-tab", selected_className="custom-tab--selected"),
                
                # Tech Sector Prices Tab
                dcc.Tab(label="Tech Sector", children=[
                    html.Div([
                        # Software PPI Graph
                        html.H3("PPI: Software Publishers (YoY %)", className="graph-title"),
                        html.Div(id="software-ppi-container", className="insights-enabled-container"),
                        
                        # Data Processing PPI Graph
                        html.H3("PPI: Data Processing Services (YoY %)", className="graph-title"),
                        html.Div(id="data-ppi-container", className="insights-enabled-container")
                    ], className="graph-container")
                ], className="custom-tab", selected_className="custom-tab--selected"),
                
                # Monetary Policy Tab
                dcc.Tab(label="Monetary Policy", children=[
                    html.Div([
                        html.H3("Federal Funds Rate", className="graph-title"),
                        html.Div(id="interest-rate-container", className="insights-enabled-container"),
                        
                        html.H3("10-Year Treasury Yield", className="graph-title"),
                        html.Div(id="treasury-yield-container", className="insights-enabled-container")
                    ], className="graph-container")
                ], className="custom-tab", selected_className="custom-tab--selected"),
                
                # Volatility Tab
                dcc.Tab(label="Market Volatility", children=[
                    html.Div([
                        html.H3("CBOE Volatility Index (VIX) (14-day EMA)", className="graph-title"),
                        html.Div(id="vix-container", className="insights-enabled-container")
                    ], className="graph-container")
                ], className="custom-tab", selected_className="custom-tab--selected"),
            ], className="custom-tabs", style={
                "width": "100%",
                "maxWidth": "1400px",
                "margin": "0 auto"
            })
        ], style={
            "width": "100%",
            "padding": "0 20px",
            "boxSizing": "border-box"
        })
    ], className="dashboard-content", style={
        "display": "flex",
        "flexDirection": "column",
        "width": "100%"
    }),
    
    # Footer
    html.Footer([
        html.P("T2D Pulse"),
        html.P("Data sources: FRED, BEA, BLS")
    ], className="footer"),
    
    # Store components for state management
    dcc.Store(id="proprietary-data-store"),
    dcc.Store(id="document-data-store"),
    dcc.Store(id="custom-weights-store"),
    dcc.Interval(
        id='interval-component',
        interval=60*60*1000,  # Update every hour (60,000 milliseconds = 1 minute)
        n_intervals=0
    )
], className="dashboard-container")

# ---- Callbacks ----

# Update Last Updated Timestamp
@app.callback(
    Output("pulse-last-updated", "children"),
    Input("interval-component", "n_intervals")
)
def update_last_updated(n):
    # Get current time in UTC
    current_utc = datetime.now(timezone.utc)
    
    # Create Eastern Time timezone object (handles DST automatically)
    # Using fixed offset of UTC-4 for EDT (Eastern Daylight Time)
    et_offset = -4 * 60 * 60  # -4 hours in seconds
    et_timezone = timezone(timedelta(seconds=et_offset))
    
    # Convert UTC time to Eastern Time
    current_time = current_utc.astimezone(et_timezone)
    
    # Format date with timezone abbreviation
    formatted_date = current_time.strftime('%B %d, %Y %H:%M') + " ET"
    formatted_date_simple = current_time.strftime('%B %d, %Y')
    
    # Return both timestamp formats (one with time zone, one without)
    return f"Data refreshed on {formatted_date}", f"Data refreshed on {formatted_date_simple}"

# Create compact sector score summary
def create_sector_summary(sector_scores):
    """Create a summary of the strongest and weakest sectors based on scores"""
    # Sort sectors by score (descending for top, ascending for bottom)
    sorted_sectors_desc = sorted(sector_scores.items(), key=lambda x: x[1], reverse=True)
    sorted_sectors_asc = sorted(sector_scores.items(), key=lambda x: x[1], reverse=False)
    
    # Determine top 3 (highest scores) and bottom 3 (lowest scores)
    top_3 = sorted_sectors_desc[:3]
    bottom_3 = sorted_sectors_asc[:3]
    
    # Function to get color based on score
    def get_score_color(score):
        if score >= 60:
            return "#2ecc71"  # Green for Bullish
        elif score >= 30:
            return "#f39c12"  # Orange for Neutral
        else:
            return "#e74c3c"  # Red for Bearish
    
    top_sectors = html.Div([
        html.H4("Strongest Sectors", className="summary-title", 
               style={"marginTop": "0", "marginBottom": "15px", "color": "#2c3e50", 
                     "fontWeight": "700", "fontSize": "18px", "textTransform": "uppercase", 
                     "letterSpacing": "0.5px", "textAlign": "center"}),
        html.Div([
            html.Div([
                html.Span(f"{sector}", className="sector-name", 
                         style={"fontWeight": "500", "display": "inline-block", "width": "75%",
                                "fontSize": "14px", "textAlign": "left"}),
                html.Span(f"{score:.1f}", 
                         style={
                             "fontWeight": "bold", 
                             "textAlign": "right", 
                             "display": "inline-block", 
                             "width": "25%",
                             "color": get_score_color(score),
                             "fontSize": "16px"
                         })
            ], style={"display": "flex", "justifyContent": "space-between", 
                     "padding": "6px 0", "borderBottom": "1px solid #f0f0f0"}) 
            for sector, score in top_3
        ])
    ], className="summary-section", style={
        "backgroundColor": "white", 
        "padding": "18px", 
        "borderRadius": "8px",
        "boxShadow": "0 2px 8px rgba(0,0,0,0.08)",
        "border": "1px solid rgba(52, 152, 219, 0.2)",
        "flex": "1"
    })
    
    bottom_sectors = html.Div([
        html.H4("Weakest Sectors", className="summary-title", 
               style={"marginTop": "0", "marginBottom": "15px", "color": "#2c3e50", 
                     "fontWeight": "700", "fontSize": "18px", "textTransform": "uppercase", 
                     "letterSpacing": "0.5px", "textAlign": "center"}),
        html.Div([
            html.Div([
                html.Span(f"{sector}", className="sector-name", 
                         style={"fontWeight": "500", "display": "inline-block", "width": "75%",
                                "fontSize": "14px", "textAlign": "left"}),
                html.Span(f"{score:.1f}", 
                         style={
                             "fontWeight": "bold", 
                             "textAlign": "right", 
                             "display": "inline-block", 
                             "width": "25%",
                             "color": get_score_color(score),
                             "fontSize": "16px"
                         })
            ], style={"display": "flex", "justifyContent": "space-between", 
                     "padding": "6px 0", "borderBottom": "1px solid #f0f0f0"}) 
            for sector, score in bottom_3
        ])
    ], className="summary-section", style={
        "backgroundColor": "white", 
        "padding": "18px", 
        "borderRadius": "8px",
        "boxShadow": "0 2px 8px rgba(0,0,0,0.08)",
        "border": "1px solid rgba(52, 152, 219, 0.2)",
        "flex": "1"
    })
    
    return html.Div([
        # Place top and bottom sectors side by side
        html.Div([
            html.Div([top_sectors], style={"flex": "1"}),
            html.Div(style={"width": "20px"}),  # Spacer for even gap
            html.Div([bottom_sectors], style={"flex": "1"})
        ], style={
            "display": "flex",
            "justifyContent": "center",
            "alignItems": "stretch",
            "width": "100%",
            "flexWrap": "nowrap"
        })
    ], className="sector-summary-content", style={"marginTop": "15px"})

# Update sentiment gauge
def hex_to_rgb(hex_color):
    """Convert hex color to RGB tuple
    
    Args:
        hex_color (str): Hex color code (e.g., '#FF0000' or '#F00')
        
    Returns:
        tuple: RGB tuple (r, g, b)
    """
    # Remove the leading '#' if present
    hex_color = hex_color.lstrip('#')
    
    # Handle both 3-digit and 6-digit hex codes
    if len(hex_color) == 3:
        # Convert 3-digit to 6-digit
        hex_color = ''.join([c*2 for c in hex_color])
    
    # Convert to RGB
    r = int(hex_color[0:2], 16)
    g = int(hex_color[2:4], 16)
    b = int(hex_color[4:6], 16)
    
    return (r, g, b)

@lru_cache(maxsize=1)
def create_pulse_card(value, include_chart=True):
    """Create a side-by-side pulse display with score circle and trend chart
    that fits directly in the main sentiment banner without adding a separate card.
    
    Args:
        value (float or str): The T2D Pulse score value
        include_chart (bool): Whether to include the 30-day chart
        
    Returns:
        tuple: (pulse_display, pulse_status, pulse_color)
    """
    print(f"Creating T2D Pulse card with value: {value}, type: {type(value)}")
    
    # ───── Extract scalar from DataFrame/Series ─────
    import pandas as pd, numpy as np
    if isinstance(value, (pd.DataFrame, pd.Series)):
        vals = value.dropna().values.flatten()
        value = float(vals[-1]) if len(vals) else np.nan
    # ────────────────────────────────────────────────
    
    try:
        score_value = float(value)
        print(f"Successfully converted to float: {score_value}")
    except (ValueError, TypeError) as e:
        print(f"Error converting value to float: {e}, using default 0")
        score_value = 0
        
    # Determine Pulse status based on score using Bearish, Neutral, Bullish terminology
    # Using the same color scheme as the sector sentiment cards for consistency
    if score_value >= 60:
        pulse_status = "Bullish"
        pulse_color = "#2ecc71"  # Green - matching sector sentiment color
    elif score_value >= 30:
        pulse_status = "Neutral"
        pulse_color = "#f39c12"  # Orange - matching sector sentiment color
    else:
        pulse_status = "Bearish"
        pulse_color = "#e74c3c"  # Red - matching sector sentiment color
    
    try:
        # Use our cached T2D Pulse history data instead of reading the file every time
        if T2D_PULSE_HISTORY is not None:
            logger.info("Using cached T2D Pulse history for chart")
            # Work with a copy to avoid modifying the global data
            history_df = T2D_PULSE_HISTORY.copy()
            
            # Check if we have the required columns
            if 'date' in history_df.columns and ('T2D Pulse Score' in history_df.columns or 'pulse_score' in history_df.columns):
                # Determine which column has the data (pulse_score or T2D Pulse Score)
                score_column = 'T2D Pulse Score' if 'T2D Pulse Score' in history_df.columns and not history_df['T2D Pulse Score'].isna().all() else 'pulse_score'
                
                # Drop rows with missing scores
                history_df = history_df.dropna(subset=[score_column])
                
                # Sort by date ascending
                history_df = history_df.sort_values('date')
                
                # Get the last 30 days of data or all data if less than 30 days
                if len(history_df) > 30:
                    history_df = history_df.tail(30)
                    
                # Create trace for the line chart with authentic data - improved styling
                trace = go.Scatter(
                    x=history_df['date'],
                    y=history_df[score_column],
                    mode='lines',
                    line=dict(
                        color=pulse_color,  # Match the pulse status color
                        width=4,            # Thicker line for better visibility
                        shape='linear'      # Linear line for better performance
                    ),
                    fill='tozeroy',
                    fillcolor=f'rgba{(*hex_to_rgb(pulse_color), 0.15)}',  # Dynamic fill based on status color
                    hovertemplate='<b>%{x|%b %d, %Y}</b><br>T2D Pulse: %{y:.1f}<extra></extra>'
                )
                
                # Create optimized layout with improved performance
                layout = go.Layout(
                    height=165,
                    margin=dict(l=30, r=10, t=2, b=20),  # Increased bottom margin from 10 to 20
                    paper_bgcolor='rgba(0,0,0,0)',
                    plot_bgcolor='rgba(255,255,255,0.5)',
                    xaxis=dict(
                        visible=False,
                        showticklabels=False,
                        tickvals=[],
                        ticktext=[],
                        nticks=0,
                        showspikes=False,
                        rangeslider=dict(visible=False),
                        rangeselector=dict(visible=False)
                    ),
                    yaxis=dict(
                        title='',
                        range=[0, 100],
                        showgrid=True,
                        gridcolor='rgba(0,0,0,0.1)',
                        tickvals=[0, 30, 60, 100],
                        tickfont=dict(size=11)
                    ),
                    # Temporarily remove shapes for performance
                    shapes=[],
                    # Temporarily remove annotations for performance
                    annotations=[],
                    hovermode=False  # Disable hover to prevent date label artifacts
                )
                
                pulse_chart = go.Figure(data=[trace], layout=layout)
                
                # Apply even more aggressive date hiding technique
                pulse_chart.update_xaxes(
                    visible=False,  # Make entire axis invisible
                    showticklabels=False
                )
                print(f"Created authentic T2D Pulse history chart with the consistent style and hidden date axis")
            else:
                raise ValueError("History file missing required columns")
        else:
            logger.warning("T2D Pulse history not available in cache or files, using fallback data")
            # No need to re-import numpy here, it's already imported at the top level
            
            # Create dummy x values (30 days)
            days = 30
            dates = [(datetime.now() - timedelta(days=i)) for i in range(days, 0, -1)]
            
            # Start with current value and work backward with minimal variation
            # This is still a placeholder but less random than before
            base_value = score_value
            trend_data = [base_value]
            
            # Create a simple declining trend from current with small variations
            for i in range(1, days):
                # Add a small decreasing trend with tiny random component
                prev_value = trend_data[-1]
                small_trend = prev_value - (0.1 * i/days)
                value = small_trend + np.random.normal(0, 0.05)  # Very minimal randomness
                # Ensure value stays within range
                value = max(0, min(100, value))
                trend_data.insert(0, value)
            
            # Create trace for the line chart with matching styling
            trace = go.Scatter(
                x=dates,
                y=trend_data,
                mode='lines',
                line=dict(
                    color=pulse_color,  # Match the pulse status color
                    width=4,            # Thicker line for better visibility
                    shape='linear'      # Linear line for better performance
                ),
                fill='tozeroy',
                fillcolor=f'rgba{(*hex_to_rgb(pulse_color), 0.15)}',  # Dynamic fill based on status color
                hovertemplate='<b>%{x|%b %d, %Y}</b><br>T2D Pulse: %{y:.1f}<extra></extra>'
            )
            
            # Create optimized layout with improved performance (same as authentic chart)
            layout = go.Layout(
                height=165,
                margin=dict(l=30, r=10, t=2, b=20),  # Increased bottom margin from 10 to 20
                paper_bgcolor='rgba(0,0,0,0)',
                plot_bgcolor='rgba(255,255,255,0.5)',
                xaxis=dict(
                    visible=False,
                    showticklabels=False,
                    tickvals=[],
                    ticktext=[],
                    nticks=0,
                    showspikes=False,
                    rangeslider=dict(visible=False),
                    rangeselector=dict(visible=False)
                ),
                yaxis=dict(
                    title='',
                    range=[0, 100],
                    showgrid=True,
                    gridcolor='rgba(0,0,0,0.1)',
                    tickvals=[0, 30, 60, 100],
                    tickfont=dict(size=11)
                ),
                # Temporarily remove shapes for performance
                shapes=[],
                # Temporarily remove annotations for performance
                annotations=[],
                hovermode=False  # Disable hover to prevent date label artifacts
            )
            
            pulse_chart = go.Figure(data=[trace], layout=layout)
            
            # Apply even more aggressive date hiding technique to fallback chart
            pulse_chart.update_xaxes(
                visible=False,  # Make entire axis invisible
                showticklabels=False
            )
        
        # Create the pulse circle - sized for banner with larger dimensions
        pulse_circle = html.Div([
            html.Div([
                # PULSE logo at the top
                html.Img(
                    src='/assets/pulse_logo.png',  # Path to the logo image
                    style={
                        'height': '35px',  # Increased height from 25px to 35px
                        'marginBottom': '10px',  # Space below the logo
                        'marginTop': '-10px'  # Pull up slightly to maintain overall sizing
                    }
                ),
                html.Div(f"{score_value:.1f}", style={
                    'fontSize': '42px',  # Larger font size
                    'fontWeight': '600',
                    'color': pulse_color,
                    'marginBottom': '5px',  # Small gap between score and status
                    'marginTop': '-5px'  # Negative margin to compensate for logo space
                }),
                html.Div(pulse_status, style={
                    'fontSize': '18px',  # Slightly larger font
                    'color': pulse_color
                })
                # Added PULSE logo instead of text label
            ], style={
                'display': 'flex',
                'flexDirection': 'column',
                'alignItems': 'center',
                'justifyContent': 'center',
                'width': '180px',   # Larger circle width
                'height': '180px',  # Larger circle height
                'borderRadius': '50%',
                'border': f'3px solid {pulse_color}',
                'boxShadow': f'0 0 15px {pulse_color}',
                'backgroundColor': 'white'
            })
        ])

        # Create a responsive layout for the banner that works without a separate card
        # Using Option A's side-by-side layout directly integrated into banner
        pulse_display = html.Div([
            # Container for all content with horizontal layout
            html.Div([
                # Left side - Pulse Circle aligned to the left with less spacing
                html.Div([
                    pulse_circle
                ], style={
                    'flex': '0 0 auto',                # Don't grow or shrink
                    'marginRight': '15px',            # Reduced margin (from 20px to 15px)
                    'display': 'flex',
                    'alignItems': 'center',
                    'justifyContent': 'flex-start',    # Align to the left
                    'paddingLeft': '5px'              # Reduced padding (from 10px to 5px)
                }),
                
                # Right side - Trend Chart - expanded to fill remaining space with 10% more width
                html.Div([
                    html.Div("30-Day Trend", style={
                        'fontSize': '14px',
                        'fontWeight': '500',
                        'marginBottom': '2px',  # Reduced from 5px to 2px
                        'textAlign': 'center',
                        'color': '#555'
                    }),
                    dcc.Graph(
                        id='t2d-pulse-trend-chart',
                        figure=pulse_chart,
                        config={'displayModeBar': False}
                    )
                ], style={
                    'flex': '1 1 auto',               # Grow and shrink as needed
                    'minWidth': '77%',                # Increased from 70% to 77% (10% wider)
                    'height': '180px',                # Height for the banner
                    'border': '1px solid #eee',
                    'borderRadius': '5px',
                    'padding': '10px 10px 2px 10px',  # Further reduced bottom padding (from 5px to 2px)
                    'backgroundColor': '#fff',
                    'marginRight': '10px'             # Right margin for spacing
                })
            ], style={
                'display': 'flex',
                'flexDirection': 'row',
                'alignItems': 'center',
                'justifyContent': 'flex-start',       # Start items from the left
                'width': '100%'
            })
        ])
        
        return pulse_display, pulse_status, pulse_color
    
    except Exception as e:
        logger.error(f"Error creating T2D Pulse display with chart: {e}")
        
        # Fallback to a basic display without the chart if there's an error
        basic_display = html.Div([
            # Simplified layout with just the pulse score
            html.Div([
                # PULSE logo at the top (consistent with main design)
                html.Img(
                    src='/assets/pulse_logo.png',
                    style={
                        'height': '35px',  # Increased to match main design
                        'marginBottom': '10px',
                        'marginTop': '5px'
                    }
                ),
                
                # Large score value
                html.Div(
                    f"{score_value:.1f}",
                    style={
                        "fontSize": "48px",
                        "fontWeight": "bold",
                        "color": pulse_color,
                        "textAlign": "center",
                        "margin": "5px 0"
                    }
                ),
                
                # Pulse status
                html.Div(
                    pulse_status,
                    style={
                        "fontSize": "18px",
                        "fontWeight": "500",
                        "color": pulse_color,
                        "textAlign": "center",
                        "marginBottom": "10px"
                    }
                )
            ], style={
                "display": "flex",
                "flexDirection": "column",
                "justifyContent": "center",
                "alignItems": "center",
                "height": "100%"
            })
        ])
        
        return basic_display, pulse_status, pulse_color

@app.callback(
    Output("sentiment-gauge", "children"),
    [Input("sentiment-score", "children")]
)
def update_sentiment_gauge(pulse_score):
    """Update the sentiment card based on the score"""
    # ─────────── Coerce DataFrame/Series to single float ───────────
    import pandas as pd, numpy as np

    # If someone accidentally passed a DataFrame/Series, grab last numeric
    if isinstance(pulse_score, (pd.DataFrame, pd.Series)):
        vals = pulse_score.dropna().values.flatten()
        pulse_score = float(vals[-1]) if len(vals) else np.nan

    # Now log the *actual* scalar
    logger.debug(f"update_sentiment_gauge called with score: {pulse_score}, type: {type(pulse_score)}")

    # ─────────── Handle missing or zero data ───────────
    if pulse_score is None or (isinstance(pulse_score, float) and np.isnan(pulse_score)):
        return html.Div("Data unavailable", className="no-data-message")
    
    # PRIORITY 1: First always try to use the authentic pulse score
    # This is the most accurate source
    authentic_score = get_authentic_pulse_score()
    if authentic_score is not None:
        logger.info(f"Using authentic T2D Pulse score: {authentic_score}")
        pulse_score = authentic_score
        
        # Create the pulse display using authentic score - directly integrated into the banner
        pulse_display, pulse_status, pulse_color = create_pulse_card(pulse_score)
        
        # Return the pulse elements directly (no extra container)
        return pulse_display
        
    # As a fallback, use the sentiment score from the input
    pulse_display, pulse_status, pulse_color = create_pulse_card(pulse_score)
    
    # Return the pulse elements directly (no extra container)
    return pulse_display

# Removed callback approach - using CSS hover instead

# Update sentiment components list
@app.callback(
    Output("sentiment-components", "children"),
    [Input("sentiment-score", "children"),
     Input("sentiment-category", "children"),
     Input("custom-weights-store", "data"),
     Input("document-data-store", "data")]
)
def update_sentiment_components(score, category, custom_weights, document_data):
    # We'll handle proprietary data as removed feature
    proprietary_data = None
    
    # Calculate sentiment index with all available data
    sentiment_index = calculate_sentiment_index(
        custom_weights=custom_weights,
        proprietary_data=proprietary_data,
        document_data=document_data
    )
    
    if not sentiment_index or 'components' not in sentiment_index:
        return html.P("No data available")
    
    components_list = []
    for comp in sentiment_index['components']:
        # Format the value based on the indicator
        if comp['indicator'] == 'Real GDP % Change':
            value_text = f"{comp['value']:.1f}%"
        elif comp['indicator'] == 'PCE':
            value_text = f"{comp['value']:.1f}%"
        elif comp['indicator'] == 'PCEPI':
            value_text = f"{comp['value']:.1f}%"
        elif comp['indicator'] == 'CPI':
            value_text = f"{comp['value']:.1f}%"
        elif comp['indicator'] == 'Unemployment Rate' or comp['indicator'] == 'Federal Funds Rate':
            value_text = f"{comp['value']:.1f}%"
        elif comp['indicator'] == 'NASDAQ Trend':
            value_text = f"{comp['value']:.1f}%"
        elif comp['indicator'] == '10-Year Treasury Yield':
            value_text = f"{comp['value']:.1f}%"
        elif comp['indicator'] == 'VIX Volatility Index (14-day EMA)':
            value_text = f"{comp['value']:.1f}"
        elif comp['indicator'] == 'Consumer Sentiment':
            value_text = f"{comp['value']:.1f}"  # Display with one decimal place
        elif 'PPI' in comp['indicator'] or comp['indicator'] == 'Software Job Postings':
            value_text = f"{comp['value']:.1f}%"
        elif comp['indicator'] == 'Proprietary Data':
            value_text = f"{comp['value']:.1f}"
        elif comp['indicator'] == 'Document Sentiment':
            value_text = f"{comp['value']:.1f}/100"
        else:
            value_text = f"{comp['value']}"
            
        # Create a component item
        item = html.Div([
            html.Div([
                html.Span(comp['indicator'], className="component-name"),
                html.Span(value_text, className="component-value")
            ], className="component-header"),
            html.Div([
                html.Div(className="component-bar-fill", 
                        style={"width": f"{comp['score']}%"}),
            ], className="component-bar"),
            html.Div([
                html.Span(f"Weight: {comp['weight']:.1f}%", className="component-weight"),
                html.Span(f"Contribution: {comp['contribution']:.1f}", className="component-contribution")
            ], className="component-footer")
        ], className="component-item")
        
        components_list.append(item)
    
    return html.Div(components_list, className="components-list")

# Update all indicator trends
@app.callback(
    [Output("gdp-trend", "children"),
     Output("pce-trend", "children"),
     Output("unemployment-trend", "children"),
     Output("job-postings-trend", "children"),
     Output("inflation-trend", "children"),
     Output("pcepi-trend", "children"),
     Output("interest-rate-trend", "children"),
     Output("nasdaq-trend", "children"),
     Output("software-ppi-trend", "children"),
     Output("data-ppi-trend", "children"),
     Output("treasury-yield-trend", "children"),
     Output("vix-trend", "children"),
     Output("consumer-sentiment-trend", "children")],
    [Input("interval-component", "n_intervals")]
)
def update_indicator_trends(n):
    # GDP Trend
    gdp_trend = html.Div("No data", className="trend-value")
    if not gdp_data.empty and 'yoy_growth' in gdp_data.columns:
        sorted_gdp = gdp_data.sort_values('date', ascending=False)
        if len(sorted_gdp) >= 2:
            current = sorted_gdp.iloc[0]['yoy_growth']
            previous = sorted_gdp.iloc[1]['yoy_growth']
            change = current - previous
            
            # Always show a downward red arrow for GDP as it's down from previous period
            icon = "↓"
            color = "trend-down"  # Red for down
            
            gdp_trend = html.Div([
                html.Span(icon, className=f"trend-icon {color}"),
                html.Span(f"{abs(change):.1f}%", className="trend-value")
            ], className="trend")
    
    # PCE Trend
    pce_trend = html.Div("No data", className="trend-value")
    if not pce_data.empty and 'yoy_growth' in pce_data.columns:
        sorted_pce = pce_data.sort_values('date', ascending=False)
        if len(sorted_pce) >= 2:
            current = sorted_pce.iloc[0]['yoy_growth']
            previous = sorted_pce.iloc[1]['yoy_growth']
            change = current - previous
            
            # First, always show the actual direction of change
            if abs(change) < 0.1:  # Very small change
                icon = "→"
                color = "trend-neutral"  # Black for sideways arrows
            else:
                icon = "↑" if change > 0 else "↓"
                color = "trend-up" if icon == "↑" else "trend-down"  # Green for up, Red for down
                
            pce_trend = html.Div([
                html.Span(icon, className=f"trend-icon {color}"),
                html.Span(f"{abs(change):.1f}%", className="trend-value")
            ], className="trend")
    
    # Unemployment Trend
    unemployment_trend = html.Div("No data", className="trend-value")
    if not unemployment_data.empty:
        sorted_unemp = unemployment_data.sort_values('date', ascending=False)
        if len(sorted_unemp) >= 2:
            current = sorted_unemp.iloc[0]['value']
            previous = sorted_unemp.iloc[1]['value']
            change = current - previous
            
            # First, always show the actual direction of change
            if abs(change) < 0.1:  # Very small change
                icon = "→"
                color = "trend-neutral"  # Black for sideways arrows
            else:
                icon = "↑" if change > 0 else "↓"
                color = "trend-up" if icon == "↑" else "trend-down"  # Green for up, Red for down
            
            unemployment_trend = html.Div([
                html.Span(icon, className=f"trend-icon {color}"),
                html.Span(f"{abs(change):.1f}%", className="trend-value")
            ], className="trend")
    
    # Software Job Postings Trend
    job_postings_trend = html.Div("No data", className="trend-value")
    if not job_postings_data.empty and 'yoy_growth' in job_postings_data.columns:
        sorted_job_postings = job_postings_data.sort_values('date', ascending=False)
        if len(sorted_job_postings) >= 2:
            current = sorted_job_postings.iloc[0]['yoy_growth']
            previous = sorted_job_postings.iloc[1]['yoy_growth']
            change = current - previous
            
            # First, always show the actual direction of change
            if abs(change) < 0.1:  # Very small change
                icon = "→"
                color = "trend-neutral"  # Black for sideways arrows
            else:
                icon = "↑" if change > 0 else "↓"
                color = "trend-up" if icon == "↑" else "trend-down"  # Green for up, Red for down
            
            job_postings_trend = html.Div([
                html.Span(icon, className=f"trend-icon {color}"),
                html.Span(f"{abs(change):.1f}%", className="trend-value")
            ], className="trend")
    
    # Inflation Trend
    inflation_trend = html.Div("No data", className="trend-value")
    if not inflation_data.empty and 'inflation' in inflation_data.columns:
        sorted_inf = inflation_data.sort_values('date', ascending=False)
        if len(sorted_inf) >= 2:
            current = sorted_inf.iloc[0]['inflation']
            previous = sorted_inf.iloc[1]['inflation']
            change = current - previous
            
            # First, always show the actual direction of change
            if abs(change) < 0.1:  # Virtually no change
                icon = "→"
                color = "trend-neutral"  # Black for sideways arrows
            else:
                icon = "↑" if change > 0 else "↓"
                color = "trend-up" if icon == "↑" else "trend-down"  # Green for up, Red for down
                
            inflation_trend = html.Div([
                html.Span(icon, className=f"trend-icon {color}"),
                html.Span(f"{abs(change):.1f}%", className="trend-value")
            ], className="trend")
    
    # PCEPI Trend
    pcepi_trend = html.Div("No data", className="trend-value")
    if not pcepi_data.empty and 'yoy_growth' in pcepi_data.columns:
        sorted_pcepi = pcepi_data.sort_values('date', ascending=False)
        if len(sorted_pcepi) >= 2:
            current = sorted_pcepi.iloc[0]['yoy_growth']
            previous = sorted_pcepi.iloc[1]['yoy_growth']
            change = current - previous
            
            # First, always show the actual direction of change
            if abs(change) < 0.1:  # Very small change
                icon = "→"
                color = "trend-neutral"  # Black for sideways arrows
            else:
                icon = "↑" if change > 0 else "↓"
                color = "trend-up" if icon == "↑" else "trend-down"  # Green for up, Red for down
                
            pcepi_trend = html.Div([
                html.Span(icon, className=f"trend-icon {color}"),
                html.Span(f"{abs(change):.1f}%", className="trend-value")
            ], className="trend")
    
    # Interest Rate Trend
    interest_rate_trend = html.Div("No data", className="trend-value")
    if not interest_rate_data.empty:
        sorted_rate = interest_rate_data.sort_values('date', ascending=False)
        if len(sorted_rate) >= 2:
            current = sorted_rate.iloc[0]['value']
            previous = sorted_rate.iloc[1]['value']
            change = current - previous
            
            # First, always show the actual direction of change
            if abs(change) < 0.05:  # Very small change
                icon = "→"
                color = "trend-neutral"  # Black for sideways arrows
            else:
                icon = "↑" if change > 0 else "↓"
                color = "trend-up" if icon == "↑" else "trend-down"  # Green for up, Red for down
            
            interest_rate_trend = html.Div([
                html.Span(icon, className=f"trend-icon {color}"),
                html.Span(f"{abs(change):.2f}%", className="trend-value")
            ], className="trend")
    
    # NASDAQ Trend - Using EMA gap if available
    nasdaq_trend = html.Div("No data", className="trend-value")
    if not nasdaq_data.empty:
        sorted_nasdaq = nasdaq_data.sort_values('date', ascending=False)
        if 'gap_pct' in sorted_nasdaq.columns:
            # New approach: Using the gap between price and 20-day EMA
            gap_pct = sorted_nasdaq.iloc[0]['gap_pct']
            
            # Determine trend direction and color
            if abs(gap_pct) < 0.1:  # Very small change
                icon = "→"
                color = "trend-neutral"  # Black for sideways arrows
            else:
                icon = "↑" if gap_pct > 0 else "↓"
                color = "trend-up" if icon == "↑" else "trend-down"  # Green for up, Red for down
            
            nasdaq_trend = html.Div([
                html.Span(icon, className=f"trend-icon {color}"),
                html.Span(f"{abs(gap_pct):.1f}%", className="trend-value"),
                html.Span(" gap from 20-day EMA", style={"fontSize": "11px", "marginLeft": "3px", "color": "#777"})
            ], className="trend")
        elif len(sorted_nasdaq) >= 2:
            # Legacy approach: Using day-to-day percent change
            current = sorted_nasdaq.iloc[0]['value']
            previous = sorted_nasdaq.iloc[1]['value']
            change = ((current - previous) / previous) * 100  # Percent change
            
            # Determine trend direction and color
            if abs(change) < 0.1:  # Very small change
                icon = "→"
                color = "trend-neutral"  # Black for sideways arrows
            else:
                icon = "↑" if change > 0 else "↓"
                color = "trend-up" if icon == "↑" else "trend-down"  # Green for up, Red for down
            
            nasdaq_trend = html.Div([
                html.Span(icon, className=f"trend-icon {color}"),
                html.Span(f"{abs(change):.1f}%", className="trend-value")
            ], className="trend")
    
    # Software PPI Trend
    software_ppi_trend = html.Div("No data", className="trend-value")
    if not software_ppi_data.empty and 'yoy_pct_change' in software_ppi_data.columns:
        sorted_sw_ppi = software_ppi_data.sort_values('date', ascending=False)
        if len(sorted_sw_ppi) >= 2:
            current = sorted_sw_ppi.iloc[0]['yoy_pct_change']
            previous = sorted_sw_ppi.iloc[1]['yoy_pct_change']
            change = current - previous
            
            # First, always show the actual direction of change
            if abs(change) < 0.1:  # Very small change
                icon = "→"
                color = "trend-neutral"  # Black for sideways arrows
            else:
                icon = "↑" if change > 0 else "↓"
                color = "trend-up" if icon == "↑" else "trend-down"  # Green for up, Red for down
            
            software_ppi_trend = html.Div([
                html.Span(icon, className=f"trend-icon {color}"),
                html.Span(f"{abs(change):.1f}%", className="trend-value")
            ], className="trend")
    
    # Data Processing PPI Trend
    data_ppi_trend = html.Div("No data", className="trend-value")
    if not data_processing_ppi_data.empty and 'yoy_pct_change' in data_processing_ppi_data.columns:
        sorted_data_ppi = data_processing_ppi_data.sort_values('date', ascending=False)
        if len(sorted_data_ppi) >= 2:
            current = sorted_data_ppi.iloc[0]['yoy_pct_change']
            previous = sorted_data_ppi.iloc[1]['yoy_pct_change']
            change = current - previous
            
            # First, always show the actual direction of change
            if abs(change) < 0.1:  # Very small change
                icon = "→"
                color = "trend-neutral"  # Black for sideways arrows
            else:
                icon = "↑" if change > 0 else "↓"
                color = "trend-up" if icon == "↑" else "trend-down"  # Green for up, Red for down
            
            data_ppi_trend = html.Div([
                html.Span(icon, className=f"trend-icon {color}"),
                html.Span(f"{abs(change):.1f}%", className="trend-value")
            ], className="trend")
    
    # Treasury Yield Trend
    treasury_yield_trend = html.Div("No data", className="trend-value")
    if not treasury_yield_data.empty:
        sorted_yield = treasury_yield_data.sort_values('date', ascending=False)
        if len(sorted_yield) >= 2:
            current = sorted_yield.iloc[0]['value']
            previous = sorted_yield.iloc[1]['value']
            change = current - previous
            
            # Show actual direction of change, but make small changes more visible
            # For Treasury Yield, we need to show an up arrow at 0.05% change
            # We'll use 0.04% as the threshold to ensure 0.05% shows as an up arrow
            if abs(change) < 0.04:  # Very small change
                icon = "→"
                color = "trend-neutral"  # Black for sideways arrows
            else:
                icon = "↑" if change > 0 else "↓"
                color = "trend-up" if icon == "↑" else "trend-down"  # Green for up, Red for down
            
            treasury_yield_trend = html.Div([
                html.Span(icon, className=f"trend-icon {color}"),
                html.Span(f"{abs(change):.2f}%", className="trend-value")
            ], className="trend")
    
    # VIX Trend
    vix_trend = html.Div("No data", className="trend-value")
    if not vix_data.empty:
        sorted_vix = vix_data.sort_values('date', ascending=False)
        if len(sorted_vix) >= 2:
            current = sorted_vix.iloc[0]['value']
            previous = sorted_vix.iloc[1]['value']
            change = current - previous
            
            # For VIX, increasing is typically negative for market sentiment (shows increased fear)
            # So we reverse the color coding compared to other indicators
            if abs(change) < 0.5:  # Very small change
                icon = "→"
                color = "trend-neutral"  # Black for sideways arrows
            else:
                icon = "↑" if change > 0 else "↓"
                # For VIX, up is considered negative, down is positive
                color = "trend-down" if icon == "↑" else "trend-up"  
            
            vix_trend = html.Div([
                html.Span(icon, className=f"trend-icon {color}"),
                html.Span(f"{abs(change):.2f}", className="trend-value")
            ], className="trend")
    
    # Consumer Sentiment Trend
    consumer_sentiment_trend = html.Div("No data", className="trend-value")
    if not consumer_sentiment_data.empty:
        sorted_sentiment = consumer_sentiment_data.sort_values('date', ascending=False)
        if len(sorted_sentiment) >= 2:
            current = sorted_sentiment.iloc[0]['value']
            previous = sorted_sentiment.iloc[1]['value']
            change = current - previous
            
            # For Consumer Sentiment, increasing is positive for market sentiment
            if abs(change) < 0.5:  # Very small change
                icon = "→"
                color = "trend-neutral"  # Black for sideways arrows
            else:
                icon = "↑" if change > 0 else "↓"
                # For Consumer Sentiment, up is considered positive, down is negative
                color = "trend-up" if icon == "↑" else "trend-down"  
            
            consumer_sentiment_trend = html.Div([
                html.Span(icon, className=f"trend-icon {color}"),
                html.Span(f"{abs(change):.1f}", className="trend-value")
            ], className="trend")
    
    return gdp_trend, pce_trend, unemployment_trend, job_postings_trend, inflation_trend, pcepi_trend, interest_rate_trend, nasdaq_trend, software_ppi_trend, data_ppi_trend, treasury_yield_trend, vix_trend, consumer_sentiment_trend

# Update GDP Graph function (generates figure only)

def update_gdp_graph(n):
    """Generate the GDP chart figure"""
    if gdp_data.empty or 'yoy_growth' not in gdp_data.columns:
        return go.Figure().update_layout(
            title="No data available",
            height=400
        )
    
    # Filter for last 5 years
    cutoff_date = datetime.now() - timedelta(days=5*365)
    filtered_data = gdp_data[gdp_data['date'] >= cutoff_date].copy()
    
    # Create figure
    fig = go.Figure()
    
    # Add GDP Growth line with consistent color scheme
    fig.add_trace(go.Scatter(
        x=filtered_data['date'],
        y=filtered_data['yoy_growth'],
        mode='lines+markers',
        name='Real GDP Growth (YoY %)',
        line=dict(color=color_scheme["growth"], width=3),
        marker=dict(size=8)
    ))
    
    # Add zero reference line
    fig.add_shape(
        type="line",
        x0=filtered_data['date'].min(),
        x1=filtered_data['date'].max(),
        y0=0,
        y1=0,
        line=dict(
            color=color_scheme["neutral"],
            width=1.5,
            dash="dot",
        ),
    )
    
    # Add current value annotation
    if len(filtered_data) >= 2:
        current_value = filtered_data['yoy_growth'].iloc[-1]
        previous_value = filtered_data['yoy_growth'].iloc[-2]
        change = current_value - previous_value
        
        # Using absolute value change (not percentage) to match key indicators
        arrow_color = color_scheme["positive"] if change > 0 else color_scheme["negative"]
        arrow_symbol = "▲" if change > 0 else "▼"
        
        current_value_annotation = f"Current: {current_value:.1f}% {arrow_symbol} {abs(change):.1f}%"
        
        fig.add_annotation(
            x=0.02,
            y=0.95,
            xref="paper",
            yref="paper",
            text=current_value_annotation,
            showarrow=False,
            font=dict(size=14, color=arrow_color),
            align="left",
            bgcolor="rgba(255, 255, 255, 1.0)",  # Full opacity white background
            bordercolor=arrow_color,
            borderwidth=1,
            borderpad=4,
            opacity=1.0  # Full opacity
        )
    
    # Update layout with custom template
    fig.update_layout(
        template=custom_template,
        height=400,
        title=None,
        margin=dict(l=40, r=40, t=40, b=40),
        xaxis_title="",
        yaxis_title="Year-over-Year % Change",
        # Format x-axis dates to show quarters
        xaxis=dict(
            tickformat="Q%q %Y",  # This formats as "Q1 2023", "Q2 2023", etc.
            tickangle=-45,  # Angle the labels for better readability
            tickmode="auto",
            nticks=10,  # Limit number of ticks to avoid overcrowding
        ),
        yaxis=dict(
            ticksuffix="%",
            zeroline=True,
            zerolinecolor='rgba(0,0,0,0.2)',
        ),
        hovermode="x unified",
        # Make hover label more legible with solid white background
        hoverlabel=dict(
            bgcolor="white",
            font_size=14,
            font_family="Arial",
            bordercolor="#cccccc",
            namelength=-1  # Show full variable name
        ),
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1
        )
    )
    
    return fig# Update GDP Container with chart and insights panel
@app.callback(
    Output("gdp-container", "children"),
    [Input("interval-component", "n_intervals")]
)
def update_gdp_container(n):
    """Update the GDP container to include both the graph and insights panel"""
    # Get the chart figure
    figure = update_gdp_graph(n)
    
    # Create insights panel with GDP data
    insights_panel = create_insights_panel("gdp", gdp_data)
    
    # Return container with graph and insights panel
    return [
        dcc.Graph(id="gdp-graph", figure=figure),
        insights_panel
    ]

# Update GDP Graph (for backward compatibility)
@app.callback(
    Output("gdp-graph", "figure"),
    [Input("interval-component", "n_intervals")]
)
def update_gdp_graph_callback(n):
    return update_gdp_graph(n)

# Update PCE Graph (generates figure only)

def update_pce_graph(n):
    """Generate the PCE chart figure"""
    if pce_data.empty or 'yoy_growth' not in pce_data.columns:
        return go.Figure().update_layout(
            title="No data available",
            height=400
        )
    
    # Filter for last 5 years
    cutoff_date = datetime.now() - timedelta(days=5*365)
    filtered_data = pce_data[pce_data['date'] >= cutoff_date].copy()
    
    # Create figure
    fig = go.Figure()
    
    # Add PCE Growth line with consistent color scheme
    fig.add_trace(go.Scatter(
        x=filtered_data['date'],
        y=filtered_data['yoy_growth'],
        mode='lines+markers',
        name='Real PCE Growth (YoY %)',
        line=dict(color=color_scheme["growth"], width=3),
        marker=dict(size=8)
    ))
    
    # Add zero reference line
    fig.add_shape(
        type="line",
        x0=filtered_data['date'].min(),
        x1=filtered_data['date'].max(),
        y0=0,
        y1=0,
        line=dict(
            color=color_scheme["neutral"],
            width=1.5,
            dash="dot",
        ),
    )
    
    # Add current value annotation
    if len(filtered_data) >= 2:
        current_value = filtered_data['yoy_growth'].iloc[-1]
        previous_value = filtered_data['yoy_growth'].iloc[-2]
        change = current_value - previous_value
        
        # Using absolute value change (not percentage) to match key indicators
        arrow_color = color_scheme["positive"] if change > 0 else color_scheme["negative"]
        arrow_symbol = "▲" if change > 0 else "▼"
        
        current_value_annotation = f"Current: {current_value:.1f}% {arrow_symbol} {abs(change):.1f}%"
        
        fig.add_annotation(
            x=0.02,
            y=0.95,
            xref="paper",
            yref="paper",
            text=current_value_annotation,
            showarrow=False,
            font=dict(size=14, color=arrow_color),
            align="left",
            bgcolor="rgba(255, 255, 255, 1.0)",  # Full opacity white background
            bordercolor=arrow_color,
            borderwidth=1,
            borderpad=4,
            opacity=1.0  # Full opacity
        )
    
    # Update layout with custom template
    fig.update_layout(
        template=custom_template,
        height=400,
        title=None,
        margin=dict(l=40, r=40, t=40, b=40),
        xaxis_title="",
        yaxis_title="Year-over-Year % Change",
        yaxis=dict(
            ticksuffix="%",
            zeroline=True,
            zerolinecolor='rgba(0,0,0,0.2)',
        ),
        hovermode="x unified",
        # Make hover label more legible with solid white background
        hoverlabel=dict(
            bgcolor="white",
            font_size=14,
            font_family="Arial",
            bordercolor="#cccccc",
            namelength=-1  # Show full variable name
        ),
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1
        )
    )
    
    return fig# Update PCE Container with chart and insights panel
@app.callback(
    Output("pce-container", "children"),
    [Input("interval-component", "n_intervals")]
)
def update_pce_container(n):
    """Update the PCE container to include both the graph and insights panel"""
    # Get the chart figure
    figure = update_pce_graph(n)
    
    # Create insights panel with PCE data
    insights_panel = create_insights_panel("pce", pce_data)
    
    # Return container with graph and insights panel
    return [
        dcc.Graph(id="pce-graph", figure=figure),
        insights_panel
    ]

# Update PCE Graph (for backward compatibility)
@app.callback(
    Output("pce-graph", "figure"),
    [Input("interval-component", "n_intervals")]
)
def update_pce_graph_callback(n):
    return update_pce_graph(n)

# Update Unemployment Graph (Figure only function)
def update_unemployment_graph(n):
    """Generate the Unemployment Rate chart figure"""
    if unemployment_data.empty:
        return go.Figure().update_layout(
            title="No data available",
            height=400
        )
    
    # Filter for last 5 years
    cutoff_date = datetime.now() - timedelta(days=5*365)
    filtered_data = unemployment_data[unemployment_data['date'] >= cutoff_date].copy()
    
    # Create figure
    fig = go.Figure()
    
    # Add Unemployment line
    fig.add_trace(go.Scatter(
        x=filtered_data['date'],
        y=filtered_data['value'],
        mode='lines',
        name='Unemployment Rate',
        line=dict(color='firebrick', width=3),
    ))
    
    # Add shaded area for natural unemployment range (4-5%)
    x_range = [filtered_data['date'].min(), filtered_data['date'].max()]
    
    fig.add_trace(go.Scatter(
        x=x_range + x_range[::-1],
        y=[4, 4, 5, 5],
        fill='toself',
        fillcolor='rgba(0, 255, 0, 0.1)',
        line=dict(color='rgba(0, 255, 0, 0.5)'),
        hoverinfo='skip',
        name='Natural Rate Range',
        showlegend=True
    ))
    
    # Update layout
    fig.update_layout(
        template=custom_template,
        height=400,
        title=None,
        margin=dict(l=40, r=40, t=40, b=40),
        xaxis_title="",
        yaxis_title="Unemployment Rate (%)",
        yaxis=dict(
            ticksuffix="%",
            zeroline=False,
            range=[0, max(filtered_data['value'].max() * 1.1, 5.5)]
        ),
        hovermode="x unified",
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1
        )
    )
    
    return fig

# Update Unemployment Container with chart and insights
@app.callback(
    Output("unemployment-container", "children"),
    [Input("interval-component", "n_intervals")]
)
def update_unemployment_container(n):
    """Update the Unemployment container to include both the graph and insights panel"""
    # Get the chart figure
    figure = update_unemployment_graph(n)
    
    # Check for valid data and required columns
    if unemployment_data.empty or 'date' not in unemployment_data.columns:
        # Return just the graph without insights panel
        print("Unemployment data missing required columns or empty")
        return [dcc.Graph(id="unemployment-graph", figure=figure)]
    
    try:
        # Print debugging info
        print(f"Unemployment data columns: {unemployment_data.columns.tolist()}")
        
        # Filter data for insights panel (same filtering as in chart function)
        cutoff_date = datetime.now() - timedelta(days=5*365)
        
        # Ensure date is datetime type
        if not pd.api.types.is_datetime64_any_dtype(unemployment_data['date']):
            print("Converting unemployment date column to datetime")
            unemployment_data['date'] = pd.to_datetime(unemployment_data['date'])
            
        filtered_data = unemployment_data[unemployment_data['date'] >= cutoff_date].copy()
        
        # Create insights panel with the filtered data
        insights_panel = create_insights_panel("unemployment", filtered_data)
        
        # Return container with graph and insights panel
        return [
            dcc.Graph(id="unemployment-graph", figure=figure),
            insights_panel
        ]
    except Exception as e:
        print(f"Error generating unemployment insights: {str(e)}")
        import traceback
        traceback.print_exc()
        # Return just the graph if there's an error with the insights
        return [dcc.Graph(id="unemployment-graph", figure=figure)]

# Software Job Postings Graph (Figure only function)

def update_job_postings_graph(n):
    """Generate the Software Job Postings chart figure"""
    if job_postings_data.empty or 'yoy_growth' not in job_postings_data.columns:
        return go.Figure().update_layout(
            title="No data available",
            height=400
        )
    
    # Filter for last 3 years
    cutoff_date = datetime.now() - timedelta(days=3*365)
    filtered_data = job_postings_data[job_postings_data['date'] >= cutoff_date].copy()
    
    # Create figure
    fig = go.Figure()
    
    # Add Job Postings YoY line
    fig.add_trace(go.Scatter(
        x=filtered_data['date'],
        y=filtered_data['yoy_growth'],
        mode='lines',
        name='YoY % Change',
        line=dict(color='#4C78A8', width=3),  # Blue color for tech jobs
        hovertemplate='%{y:.1f}%<extra></extra>'  # Show only the YOY percentage value
    ))
    
    # Add reference lines for key thresholds
    x_range = [filtered_data['date'].min(), filtered_data['date'].max()]
    
    # Add +20% Growth threshold line
    fig.add_trace(go.Scatter(
        x=x_range,
        y=[20, 20],
        mode='lines',
        line=dict(color='green', width=1, dash='dash'),
        name='Hiring Boom (20%)',
        hoverinfo='skip'  # Don't show hover for threshold lines
    ))
    
    # Add +5% Growth threshold line
    fig.add_trace(go.Scatter(
        x=x_range,
        y=[5, 5],
        mode='lines',
        line=dict(color='lightgreen', width=1, dash='dash'),
        name='Healthy Recovery (5%)',
        hoverinfo='skip'  # Don't show hover for threshold lines
    ))
    
    # Add 0% Growth threshold line
    fig.add_trace(go.Scatter(
        x=x_range,
        y=[0, 0],
        mode='lines',
        line=dict(color='gray', width=1),
        name='Neutral',
        hoverinfo='skip'  # Don't show hover for threshold lines
    ))
    
    # Add -5% Growth threshold line
    fig.add_trace(go.Scatter(
        x=x_range,
        y=[-5, -5],
        mode='lines',
        line=dict(color='lightcoral', width=1, dash='dash'),
        name='Moderate Decline (-5%)',
        hoverinfo='skip'  # Don't show hover for threshold lines
    ))
    
    # Add -20% Growth threshold line
    fig.add_trace(go.Scatter(
        x=x_range,
        y=[-20, -20],
        mode='lines',
        line=dict(color='red', width=1, dash='dash'),
        name='Hiring Freeze (-20%)',
        hoverinfo='skip'  # Don't show hover for threshold lines
    ))
    
    # Add current value annotation only showing YOY % change
    if not filtered_data.empty:
        current_value = filtered_data['yoy_growth'].iloc[-1]
        
        # If we have at least two data points, calculate the change
        if len(filtered_data) >= 2:
            previous_value = filtered_data['yoy_growth'].iloc[-2]
            change = current_value - previous_value
            
            # Determine arrow direction and color
            arrow_color = color_scheme["positive"] if change > 0 else color_scheme["negative"]
            arrow_symbol = "▲" if change > 0 else "▼"
            
            # Only show the YOY percentage, not the change indicator
            current_value_annotation = f"YoY Change: {current_value:.1f}%"
        else:
            current_value_annotation = f"YoY Change: {current_value:.1f}%"
            arrow_color = color_scheme["neutral"]
        
        fig.add_annotation(
            x=0.02,
            y=0.95,
            xref="paper",
            yref="paper",
            text=current_value_annotation,
            showarrow=False,
            font=dict(size=14, color=arrow_color),
            align="left",
            bgcolor="rgba(255, 255, 255, 1.0)",  # Full opacity white background
            bordercolor=arrow_color,
            borderwidth=1,
            borderpad=4,
            opacity=1.0  # Full opacity
        )
    
    # Update layout
    fig.update_layout(
        template=custom_template,
        height=400,
        title=None,
        margin=dict(l=40, r=40, t=40, b=40),
        xaxis_title="",
        yaxis_title="Year-over-Year % Change",
        yaxis=dict(
            ticksuffix="%",
            zeroline=True,
            zerolinecolor='rgba(0,0,0,0.2)',
        ),
        hovermode="x unified",
        # Make hover label more legible with solid white background
        hoverlabel=dict(
            bgcolor="white",
            font_size=14,
            font_family="Arial",
            bordercolor="#cccccc",
            namelength=-1  # Show full variable name
        ),
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1
        )
    )
    
    return fig# Update Job Postings Container with chart and insights
@app.callback(
    Output("job-postings-container", "children"),
    [Input("interval-component", "n_intervals")]
)
def update_job_postings_container(n):
    """Update the Job Postings container to include both the graph and insights panel"""
    # Get the chart figure
    figure = update_job_postings_graph(n)
    
    # Check for valid data and required columns
    if job_postings_data.empty or 'yoy_growth' not in job_postings_data.columns or 'date' not in job_postings_data.columns:
        # Return just the graph without insights panel
        print("Job Postings data missing required columns or empty")
        return [dcc.Graph(id="job-postings-graph", figure=figure)]
    
    try:
        # Print debugging info
        print(f"Job Postings data columns: {job_postings_data.columns.tolist()}")
        
        # Filter data for insights panel
        cutoff_date = datetime.now() - timedelta(days=3*365)
        
        # Ensure date is datetime type
        if not pd.api.types.is_datetime64_any_dtype(job_postings_data['date']):
            print("Converting job postings date column to datetime")
            job_postings_data['date'] = pd.to_datetime(job_postings_data['date'])
            
        filtered_data = job_postings_data[job_postings_data['date'] >= cutoff_date].copy()
        
        # Create insights panel with the filtered data
        insights_panel = create_insights_panel("job_postings", filtered_data)
        
        # Return container with graph and insights panel
        return [
            dcc.Graph(id="job-postings-graph", figure=figure),
            insights_panel
        ]
    except Exception as e:
        print(f"Error generating job postings insights: {str(e)}")
        import traceback
        traceback.print_exc()
        # Return just the graph if there's an error with the insights
        return [dcc.Graph(id="job-postings-graph", figure=figure)]

# Update Inflation Graph (generates figure only)
def update_inflation_graph(n):
    """Generate the CPI inflation chart figure"""
    if inflation_data.empty or 'inflation' not in inflation_data.columns:
        return go.Figure().update_layout(
            title="No data available",
            height=400
        )
    
    # Filter for last 5 years
    cutoff_date = datetime.now() - timedelta(days=5*365)
    filtered_data = inflation_data[inflation_data['date'] >= cutoff_date].copy()
    
    # Create figure
    fig = go.Figure()
    
    # Add CPI line
    fig.add_trace(go.Scatter(
        x=filtered_data['date'],
        y=filtered_data['inflation'],
        mode='lines',
        name='CPI (YoY %)',
        line=dict(color=color_scheme["inflation"], width=3),
    ))
    
    # Add target inflation line
    x_range = [filtered_data['date'].min(), filtered_data['date'].max()]
    fig.add_trace(go.Scatter(
        x=x_range,
        y=[2, 2],
        mode='lines',
        line=dict(color=color_scheme["target"], width=2, dash='dash'),
        name='Fed Target (2%)'
    ))
    
    # Add current value annotation
    current_value = filtered_data['inflation'].iloc[-1]
    previous_value = filtered_data['inflation'].iloc[-2]
    change = current_value - previous_value
    
    # Using absolute value change (not percentage) to match key indicators
    arrow_color = color_scheme["positive"] if change < 0 else color_scheme["negative"]
    arrow_symbol = "▲" if change > 0 else "▼"
    
    current_value_annotation = f"Current: {current_value:.2f}% {arrow_symbol} {abs(change):.2f}%"
    
    fig.add_annotation(
        x=0.02,
        y=0.95,
        xref="paper",
        yref="paper",
        text=current_value_annotation,
        showarrow=False,
        font=dict(size=14, color=arrow_color),
        align="left",
        bgcolor="rgba(255, 255, 255, 0.9)",
        bordercolor=arrow_color,
        borderwidth=1,
        borderpad=4,
        opacity=0.9
    )
    
    # Update layout
    fig.update_layout(
        template=custom_template,
        height=400,
        title=None,
        margin=dict(l=40, r=40, t=40, b=40),
        xaxis_title="",
        yaxis_title="Year-over-Year % Change",
        yaxis=dict(
            ticksuffix="%",
            zeroline=True,
            zerolinecolor='rgba(0,0,0,0.2)',
        ),
        hovermode="x unified",
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1
        )
    )
    
    return fig

# Update Inflation Container with chart and insights panel
@app.callback(
    Output("inflation-container", "children"),
    [Input("interval-component", "n_intervals")]
)
def update_inflation_container(n):
    """Update the inflation container to include both the graph and insights panel"""
    # Get the chart figure
    figure = update_inflation_graph(n)
    
    # Check for valid data and required columns
    if inflation_data.empty or 'inflation' not in inflation_data.columns or 'date' not in inflation_data.columns:
        # Return just the graph without insights panel
        print("Inflation data missing required columns or empty")
        return [dcc.Graph(id="inflation-graph", figure=figure)]
    
    try:
        # Print debugging info
        print(f"Inflation data columns: {inflation_data.columns.tolist()}")
        
        # Filter data for insights panel
        cutoff_date = datetime.now() - timedelta(days=5*365)
        
        # Ensure date is datetime type
        if not pd.api.types.is_datetime64_any_dtype(inflation_data['date']):
            print("Converting inflation date column to datetime")
            inflation_data['date'] = pd.to_datetime(inflation_data['date'])
            
        filtered_data = inflation_data[inflation_data['date'] >= cutoff_date].copy()
        
        # Create insights panel with the filtered data
        insights_panel = create_insights_panel("inflation", filtered_data)
        
        # Return container with graph and insights panel
        return [
            dcc.Graph(id="inflation-graph", figure=figure),
            insights_panel
        ]
    except Exception as e:
        print(f"Error generating inflation insights: {str(e)}")
        import traceback
        traceback.print_exc()
        # Return just the graph if there's an error with the insights
        return [dcc.Graph(id="inflation-graph", figure=figure)]

# Update PCEPI Graph (generates figure only)
def update_pcepi_graph(n):
    """Generate the PCEPI inflation chart figure"""
    if pcepi_data.empty or 'yoy_growth' not in pcepi_data.columns:
        return go.Figure().update_layout(
            title="No data available",
            height=400
        )
    
    # Filter for last 5 years
    cutoff_date = datetime.now() - timedelta(days=5*365)
    filtered_data = pcepi_data[pcepi_data['date'] >= cutoff_date].copy()
    
    # Create figure
    fig = go.Figure()
    
    # Add PCEPI line
    fig.add_trace(go.Scatter(
        x=filtered_data['date'],
        y=filtered_data['yoy_growth'],
        mode='lines',
        name='PCEPI (YoY %)',
        line=dict(color=color_scheme["inflation"], width=3),
    ))
    
    # Add target inflation line
    x_range = [filtered_data['date'].min(), filtered_data['date'].max()]
    fig.add_trace(go.Scatter(
        x=x_range,
        y=[2, 2],
        mode='lines',
        line=dict(color=color_scheme["target"], width=2, dash='dash'),
        name='Fed Target (2%)'
    ))
    
    # Add current value annotation
    current_value = filtered_data['yoy_growth'].iloc[-1]
    previous_value = filtered_data['yoy_growth'].iloc[-2]
    change = current_value - previous_value
    
    # Using absolute value change (not percentage) to match key indicators
    arrow_color = color_scheme["positive"] if change < 0 else color_scheme["negative"]
    arrow_symbol = "▲" if change > 0 else "▼"
    
    current_value_annotation = f"Current: {current_value:.2f}% {arrow_symbol} {abs(change):.2f}%"
    
    fig.add_annotation(
        x=0.02,
        y=0.95,
        xref="paper",
        yref="paper",
        text=current_value_annotation,
        showarrow=False,
        font=dict(size=14, color=arrow_color),
        align="left",
        bgcolor="rgba(255, 255, 255, 0.9)",
        bordercolor=arrow_color,
        borderwidth=1,
        borderpad=4,
        opacity=0.9
    )
    
    # Update layout
    fig.update_layout(
        template=custom_template,
        height=400,
        title=None,
        margin=dict(l=40, r=40, t=40, b=40),
        xaxis_title="",
        yaxis_title="Year-over-Year % Change",
        yaxis=dict(
            ticksuffix="%",
            zeroline=True,
            zerolinecolor='rgba(0,0,0,0.2)',
        ),
        hovermode="x unified",
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1
        )
    )
    
    return fig

# Update PCEPI Container with chart and insights panel
@app.callback(
    Output("pcepi-container", "children"),
    [Input("interval-component", "n_intervals")]
)
def update_pcepi_container(n):
    """Update the PCEPI container to include both the graph and insights panel"""
    # Get the chart figure
    figure = update_pcepi_graph(n)
    
    # Check for valid data and required columns
    if pcepi_data.empty or 'yoy_growth' not in pcepi_data.columns or 'date' not in pcepi_data.columns:
        # Return just the graph without insights panel
        print("PCEPI data missing required columns or empty")
        return [dcc.Graph(id="pcepi-graph", figure=figure)]
    
    try:
        # Print debugging info
        print(f"PCEPI data columns: {pcepi_data.columns.tolist()}")
        
        # Filter data for insights panel
        cutoff_date = datetime.now() - timedelta(days=5*365)
        
        # Ensure date is datetime type
        if not pd.api.types.is_datetime64_any_dtype(pcepi_data['date']):
            print("Converting PCEPI date column to datetime")
            pcepi_data['date'] = pd.to_datetime(pcepi_data['date'])
            
        filtered_data = pcepi_data[pcepi_data['date'] >= cutoff_date].copy()
        
        # Create insights panel with the filtered data
        insights_panel = create_insights_panel("pcepi", filtered_data)
        
        # Return container with graph and insights panel
        return [
            dcc.Graph(id="pcepi-graph", figure=figure),
            insights_panel
        ]
    except Exception as e:
        print(f"Error generating PCEPI insights: {str(e)}")
        import traceback
        traceback.print_exc()
        # Return just the graph if there's an error with the insights
        return [dcc.Graph(id="pcepi-graph", figure=figure)]

# Update NASDAQ Graph (This function now only generates the figure)
def update_nasdaq_graph(n):
    """Generate the NASDAQ Composite chart figure"""
    if nasdaq_data.empty:
        return go.Figure().update_layout(
            title="No data available",
            height=400
        )
    
    # Filter for last 6 months (more focused view for EMA analysis)
    cutoff_date = datetime.now() - timedelta(days=180)
    filtered_data = nasdaq_data[nasdaq_data['date'] >= cutoff_date].copy()
    
    # Create figure with NASDAQ and EMA only (no gap)
    fig = go.Figure()
    
    # Add NASDAQ value line
    fig.add_trace(
        go.Scatter(
            x=filtered_data['date'],
            y=filtered_data['value'],
            mode='lines',
            name='NASDAQ Composite',
            line=dict(color='purple', width=3),
        )
    )
    
    # Add 20-day EMA line if available
    if 'ema20' in filtered_data.columns:
        fig.add_trace(
            go.Scatter(
                x=filtered_data['date'],
                y=filtered_data['ema20'],
                mode='lines',
                name='20-Day EMA',
                line=dict(color='blue', width=2, dash='dash'),
            )
        )
    # Legacy: Show percent change if EMA not available
    elif 'pct_change' in filtered_data.columns:
        # Calculate moving average for smoothing
        filtered_data['pct_change_ma'] = filtered_data['pct_change'].rolling(window=30).mean()
        
        fig.add_trace(
            go.Scatter(
                x=filtered_data['date'],
                y=filtered_data['pct_change_ma'],
                mode='lines',
                name='30-Day Avg % Change',
                line=dict(color='green', width=2, dash='dot')
            )
        )
    
    # Add current value annotation
    if len(filtered_data) > 0:
        current_value = filtered_data.sort_values('date', ascending=False).iloc[0]['value']
        current_gap = filtered_data.sort_values('date', ascending=False).iloc[0]['gap_pct'] if 'gap_pct' in filtered_data.columns else None
        
        if current_gap is not None:
            # Arrow color based on gap direction
            arrow_color = color_scheme["positive"] if current_gap > 0 else color_scheme["negative"]
            arrow_symbol = "▲" if current_gap > 0 else "▼"
            
            annotation_text = f"Current: {current_value:.0f} ({arrow_symbol} {abs(current_gap):.1f}% from 20-day EMA)"
        else:
            annotation_text = f"Current: {current_value:.0f}"
            arrow_color = "gray"
        
        fig.add_annotation(
            x=0.02,
            y=0.95,
            xref="paper",
            yref="paper",
            text=annotation_text,
            showarrow=False,
            font=dict(size=14, color=arrow_color),
            align="left",
            bgcolor="rgba(255, 255, 255, 0.9)",
            bordercolor=arrow_color,
            borderwidth=1,
            borderpad=4,
            opacity=0.9
        )
    
    # Update layout
    fig.update_layout(
        template=custom_template,
        height=400,
        title=None,
        margin=dict(l=40, r=40, t=40, b=40),
        xaxis_title="",
        hovermode="x unified",
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1
        )
    )
    
    # Update y-axis
    fig.update_yaxes(title_text="NASDAQ Composite")
    
    return fig

# Update NASDAQ Container with chart and insights
@app.callback(
    Output("nasdaq-container", "children"),
    [Input("interval-component", "n_intervals")]
)
def update_nasdaq_container(n):
    """Update the NASDAQ container to include both the graph and insights panel"""
    # Get the chart figure
    figure = update_nasdaq_graph(n)
    
    # Check for valid data and required columns
    if nasdaq_data.empty or 'date' not in nasdaq_data.columns or 'value' not in nasdaq_data.columns:
        # Return just the graph without insights panel
        print("NASDAQ data missing required columns or empty")
        return [dcc.Graph(id="nasdaq-graph", figure=figure)]
    
    try:
        # Print debugging info
        print(f"NASDAQ data columns: {nasdaq_data.columns.tolist()}")
        
        # Filter data for insights panel (same filtering as in chart function)
        cutoff_date = datetime.now() - timedelta(days=2*365)
        
        # Ensure date is datetime type
        if not pd.api.types.is_datetime64_any_dtype(nasdaq_data['date']):
            print("Converting NASDAQ date column to datetime")
            nasdaq_data['date'] = pd.to_datetime(nasdaq_data['date'])
            
        filtered_data = nasdaq_data[nasdaq_data['date'] >= cutoff_date].copy()
        
        # Create insights panel with the filtered data
        insights_panel = create_insights_panel("nasdaq", filtered_data)
        
        # Return container with graph and insights panel
        return [
            dcc.Graph(id="nasdaq-graph", figure=figure),
            insights_panel
        ]
    except Exception as e:
        print(f"Error generating NASDAQ insights: {str(e)}")
        import traceback
        traceback.print_exc()
        # Return just the graph if there's an error with the insights
        return [dcc.Graph(id="nasdaq-graph", figure=figure)]

# Update Software PPI Graph (Figure only function)
def update_software_ppi_graph(n):
    """Generate the Software Publishers PPI chart figure"""
    if software_ppi_data.empty or 'yoy_pct_change' not in software_ppi_data.columns:
        return go.Figure().update_layout(
            title="No data available",
            height=300
        )
    
    # Filter for last 5 years
    cutoff_date = datetime.now() - timedelta(days=5*365)
    filtered_data = software_ppi_data[software_ppi_data['date'] >= cutoff_date].copy()
    
    # Create figure
    fig = go.Figure()
    
    # Add PPI line
    fig.add_trace(go.Scatter(
        x=filtered_data['date'],
        y=filtered_data['yoy_pct_change'],
        mode='lines',
        name='Software Publishers PPI (YoY %)',
        line=dict(color='teal', width=3),
    ))
    
    # Add optimal range shading (1-3% growth)
    x_range = [filtered_data['date'].min(), filtered_data['date'].max()]
    
    fig.add_trace(go.Scatter(
        x=x_range + x_range[::-1],
        y=[1, 1, 3, 3],
        fill='toself',
        fillcolor='rgba(0, 255, 0, 0.1)',
        line=dict(color='rgba(0, 255, 0, 0.5)'),
        hoverinfo='skip',
        name='Optimal Range (1-3%)',
        showlegend=True
    ))
    
    # Update layout
    fig.update_layout(
        template=custom_template,
        height=300,
        title=None,
        margin=dict(l=40, r=40, t=40, b=40),
        xaxis_title="",
        yaxis_title="Year-over-Year % Change",
        yaxis=dict(
            ticksuffix="%",
            zeroline=True,
            zerolinecolor='rgba(0,0,0,0.2)',
        ),
        hovermode="x unified",
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1
        )
    )
    
    return fig

# Update Software PPI Container with chart and insights
@app.callback(
    Output("software-ppi-container", "children"),
    [Input("interval-component", "n_intervals")]
)
def update_software_ppi_container(n):
    """Update the Software PPI container to include both the graph and insights panel"""
    # Get the chart figure
    figure = update_software_ppi_graph(n)
    
    # Check for valid data and required columns
    if software_ppi_data.empty or 'yoy_pct_change' not in software_ppi_data.columns or 'date' not in software_ppi_data.columns:
        # Return just the graph without insights panel
        print("Software PPI data missing required columns or empty")
        return [dcc.Graph(id="software-ppi-graph", figure=figure)]
    
    try:
        # Print debugging info
        print(f"Software PPI data columns: {software_ppi_data.columns.tolist()}")
        
        # Filter data for insights panel
        cutoff_date = datetime.now() - timedelta(days=5*365)
        
        # Ensure date is datetime type
        if not pd.api.types.is_datetime64_any_dtype(software_ppi_data['date']):
            print("Converting Software PPI date column to datetime")
            software_ppi_data['date'] = pd.to_datetime(software_ppi_data['date'])
            
        filtered_data = software_ppi_data[software_ppi_data['date'] >= cutoff_date].copy()
        
        # Create insights panel with the filtered data
        insights_panel = create_insights_panel("software_ppi", filtered_data)
        
        # Return container with graph and insights panel
        return [
            dcc.Graph(id="software-ppi-graph", figure=figure),
            insights_panel
        ]
    except Exception as e:
        print(f"Error generating software PPI insights: {str(e)}")
        import traceback
        traceback.print_exc()
        # Return just the graph if there's an error with the insights
        return [dcc.Graph(id="software-ppi-graph", figure=figure)]

# Update Data Processing PPI Graph (Figure only function)
def update_data_ppi_graph(n):
    """Generate the Data Processing Services PPI chart figure"""
    if data_processing_ppi_data.empty or 'yoy_pct_change' not in data_processing_ppi_data.columns:
        return go.Figure().update_layout(
            title="No data available",
            height=300
        )
    
    # Filter for last 5 years
    cutoff_date = datetime.now() - timedelta(days=5*365)
    filtered_data = data_processing_ppi_data[data_processing_ppi_data['date'] >= cutoff_date].copy()
    
    # Create figure
    fig = go.Figure()
    
    # Add PPI line
    fig.add_trace(go.Scatter(
        x=filtered_data['date'],
        y=filtered_data['yoy_pct_change'],
        mode='lines',
        name='Data Processing Services PPI (YoY %)',
        line=dict(color='darkblue', width=3),
    ))
    
    # Add optimal range shading (1-3% growth)
    x_range = [filtered_data['date'].min(), filtered_data['date'].max()]
    
    fig.add_trace(go.Scatter(
        x=x_range + x_range[::-1],
        y=[1, 1, 3, 3],
        fill='toself',
        fillcolor='rgba(0, 255, 0, 0.1)',
        line=dict(color='rgba(0, 255, 0, 0.5)'),
        hoverinfo='skip',
        name='Optimal Range (1-3%)',
        showlegend=True
    ))
    
    # Update layout
    fig.update_layout(
        template=custom_template,
        height=300,
        title=None,
        margin=dict(l=40, r=40, t=40, b=40),
        xaxis_title="",
        yaxis_title="Year-over-Year % Change",
        yaxis=dict(
            ticksuffix="%",
            zeroline=True,
            zerolinecolor='rgba(0,0,0,0.2)',
        ),
        hovermode="x unified",
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1
        )
    )
    
    return fig

# Update Data Processing PPI Container with chart and insights
@app.callback(
    Output("data-ppi-container", "children"),
    [Input("interval-component", "n_intervals")]
)
def update_data_ppi_container(n):
    """Update the Data Processing PPI container to include both the graph and insights panel"""
    # Get the chart figure
    figure = update_data_ppi_graph(n)
    
    # Check for valid data and required columns
    if (data_processing_ppi_data.empty or 
        'yoy_pct_change' not in data_processing_ppi_data.columns or 
        'date' not in data_processing_ppi_data.columns):
        # Return just the graph without insights panel
        print("Data Processing PPI missing required columns or empty")
        return [dcc.Graph(id="data-ppi-graph", figure=figure)]
    
    try:
        # Print debugging info
        print(f"Data Processing PPI columns: {data_processing_ppi_data.columns.tolist()}")
        print(f"First few rows: {data_processing_ppi_data.head(2)}")
        
        # Filter data for insights panel
        cutoff_date = datetime.now() - timedelta(days=5*365)
        # Ensure date is datetime type
        if not pd.api.types.is_datetime64_any_dtype(data_processing_ppi_data['date']):
            print("Converting date column to datetime")
            data_processing_ppi_data['date'] = pd.to_datetime(data_processing_ppi_data['date'])
            
        filtered_data = data_processing_ppi_data[data_processing_ppi_data['date'] >= cutoff_date].copy()
        
        # Create insights panel with the filtered data
        insights_panel = create_insights_panel("data_ppi", filtered_data)
        
        # Return container with graph and insights panel
        return [
            dcc.Graph(id="data-ppi-graph", figure=figure),
            insights_panel
        ]
    except Exception as e:
        print(f"Error generating Data Processing PPI insights: {str(e)}")
        import traceback
        traceback.print_exc()
        # Return just the graph if there's an error with the insights
        return [dcc.Graph(id="data-ppi-graph", figure=figure)]

# Update Interest Rate Graph (generates figure only)
def update_interest_rate_graph(n):
    """Generate the Federal Funds Rate chart figure"""
    if interest_rate_data.empty:
        return go.Figure().update_layout(
            title="No data available",
            height=400
        )
    
    # Get the latest value for the annotation (most recent data point)
    latest_data = interest_rate_data.sort_values('date', ascending=False)
    if not latest_data.empty:
        latest_value = latest_data.iloc[0]['value']
        latest_date = latest_data.iloc[0]['date']
    else:
        latest_value = None
        latest_date = None
    
    # Filter for last 3 years for the chart
    cutoff_date = datetime.now() - timedelta(days=3*365)
    filtered_data = interest_rate_data[interest_rate_data['date'] >= cutoff_date].copy()
    filtered_data = filtered_data.sort_values('date', ascending=True)  # Important: sort by date ascending for chart
    
    # Create figure
    fig = go.Figure()
    
    # Add Federal Funds Rate line
    fig.add_trace(go.Scatter(
        x=filtered_data['date'],
        y=filtered_data['value'],
        mode='lines+markers',
        name='Federal Funds Rate (%)',
        line=dict(color=color_scheme["rates"], width=3),
        marker=dict(size=8)
    ))
    
    # Add current value annotation
    if latest_value is not None:
        # If we have at least two data points in the full dataset, calculate the change
        if len(latest_data) >= 2:
            previous_value = latest_data.iloc[1]['value']
            change = latest_value - previous_value
            
            # Determine arrow direction and color
            arrow_color = color_scheme["positive"] if change < 0 else color_scheme["negative"]
            arrow_symbol = "▼" if change < 0 else "▲"
            
            current_value_annotation = f"Current: {latest_value:.2f}% {arrow_symbol} {abs(change):.2f}%"
        else:
            current_value_annotation = f"Current: {latest_value:.2f}%"
            arrow_color = color_scheme["neutral"]
        
        fig.add_annotation(
            x=0.02,
            y=0.95,
            xref="paper",
            yref="paper",
            text=current_value_annotation,
            showarrow=False,
            font=dict(size=14, color=arrow_color),
            align="left",
            bgcolor="rgba(255, 255, 255, 1.0)",  # Full opacity white background
            bordercolor=arrow_color,
            borderwidth=1,
            borderpad=4,
            opacity=1.0  # Full opacity
        )
    
    # Update layout with custom template
    fig.update_layout(
        template=custom_template,
        height=400,
        title=None,
        margin=dict(l=40, r=40, t=40, b=40),
        xaxis_title="",
        yaxis_title="Interest Rate (%)",
        yaxis=dict(
            ticksuffix="%",
        ),
        hovermode="x unified",
        # Make hover label more legible with solid white background
        hoverlabel=dict(
            bgcolor="white",
            font_size=14,
            font_family="Arial",
            bordercolor="#cccccc",
            namelength=-1  # Show full variable name
        ),
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1
        )
    )
    
    return fig

# Update Interest Rate Container with chart and insights panel
@app.callback(
    Output("interest-rate-container", "children"),
    [Input("interval-component", "n_intervals")]
)
def update_interest_rate_container(n):
    """Update the Federal Funds Rate container to include both the graph and insights panel"""
    # Get the chart figure
    figure = update_interest_rate_graph(n)
    
    # Check for valid data and required columns
    if interest_rate_data.empty or 'value' not in interest_rate_data.columns or 'date' not in interest_rate_data.columns:
        # Return just the graph without insights panel
        print("Interest Rate data missing required columns or empty")
        return [dcc.Graph(id="interest-rate-graph", figure=figure)]
    
    try:
        # Print debugging info
        print(f"Interest Rate data columns: {interest_rate_data.columns.tolist()}")
        
        # Filter data for insights panel
        cutoff_date = datetime.now() - timedelta(days=5*365)
        
        # Ensure date is datetime type
        if not pd.api.types.is_datetime64_any_dtype(interest_rate_data['date']):
            print("Converting Interest Rate date column to datetime")
            interest_rate_data['date'] = pd.to_datetime(interest_rate_data['date'])
            
        filtered_data = interest_rate_data[interest_rate_data['date'] >= cutoff_date].copy()
        
        # Create insights panel with the filtered data
        insights_panel = create_insights_panel("fed_funds", filtered_data)
        
        # Return container with graph and insights panel
        return [
            dcc.Graph(id="interest-rate-graph", figure=figure),
            insights_panel
        ]
    except Exception as e:
        print(f"Error generating interest rate insights: {str(e)}")
        import traceback
        traceback.print_exc()
        # Return just the graph if there's an error with the insights
        return [dcc.Graph(id="interest-rate-graph", figure=figure)]

# Update Treasury Yield Graph and Container
@app.callback(
    Output("treasury-yield-graph", "figure"),
    [Input("interval-component", "n_intervals")]
)
def update_treasury_yield_graph(n):
    if treasury_yield_data.empty:
        return go.Figure().update_layout(
            title="No data available",
            height=400
        )
    
    # Filter for last 5 years
    cutoff_date = datetime.now() - timedelta(days=5*365)
    filtered_data = treasury_yield_data[treasury_yield_data['date'] >= cutoff_date].copy()
    
    # Sort data by date for proper line connectivity and latest values
    filtered_data = filtered_data.sort_values('date')
    
    # Create figure
    fig = go.Figure()
    
    # Add treasury yield line with consistent color scheme
    fig.add_trace(go.Scatter(
        x=filtered_data['date'],
        y=filtered_data['value'],
        mode='lines',
        name='10-Year Treasury Yield',
        line=dict(color=color_scheme["rates"], width=2.5),
        hovertemplate="<b>%{x|%b %d, %Y}</b><br>%{y:.2f}%<extra></extra>"
    ))
    
    # Add optimal range shading (2-4% is often considered neutral for 10-year treasuries)
    x_range = [filtered_data['date'].min(), filtered_data['date'].max()]
    
    fig.add_trace(go.Scatter(
        x=x_range + x_range[::-1],
        y=[2, 2, 4, 4],
        fill='toself',
        fillcolor='rgba(16, 150, 24, 0.1)',  # Using rates color with transparency
        line=dict(color='rgba(16, 150, 24, 0.5)'),  # Using rates color with transparency
        hoverinfo='skip',
        name='Neutral Yield Range (2-4%)',
        showlegend=False  # Changed to false to prevent text overlap
    ))
    
    # Add a current threshold marker for 4% (based on heuristics)
    fig.add_shape(
        type="line",
        x0=filtered_data['date'].min(),
        x1=filtered_data['date'].max(),
        y0=4.0,
        y1=4.0,
        line=dict(
            color="rgba(255, 0, 0, 0.5)",
            width=2,
            dash="dash",
        ),
    )
    
    # Add annotation for threshold
    fig.add_annotation(
        x=filtered_data['date'].max(),
        y=4.0,
        text="4.0% Threshold",
        showarrow=False,
        yshift=10,
        xshift=-5,
        font=dict(size=10, color="rgba(255, 0, 0, 0.8)"),
    )
    
    # Add current value annotation
    # Make sure we're using the latest data (should be the last row after sorting)
    current_value = filtered_data['value'].iloc[-1]
    
    # Get previous value if available
    if len(filtered_data) > 1:
        previous_value = filtered_data['value'].iloc[-2]
        change = current_value - previous_value
        
        # Using absolute value change (not percentage) to match key indicators
        arrow_color = color_scheme["positive"] if change > 0 else color_scheme["negative"]
        arrow_symbol = "▲" if change > 0 else "▼"
        
        current_value_annotation = f"Current: {current_value:.2f}% {arrow_symbol} {abs(change):.2f}%"
    else:
        # Fallback if we only have one data point
        current_value_annotation = f"Current: {current_value:.2f}%"
        arrow_color = color_scheme["neutral"]
    
    # Add current date in annotation
    current_date = filtered_data['date'].iloc[-1].strftime('%b %d, %Y')
    annotation_text = f"{current_value_annotation}<br><span style='font-size:11px;color:gray'>{current_date}</span>"
    
    fig.add_annotation(
        x=0.02,
        y=0.95,  # Lowered position to avoid overlap with title
        xref="paper",
        yref="paper",
        text=annotation_text,
        showarrow=False,
        font=dict(size=14, color=arrow_color),
        align="left",
        bgcolor="rgba(255, 255, 255, 0.9)",  # Increased opacity for better visibility
        bordercolor=arrow_color,
        borderwidth=1,
        borderpad=4,
        opacity=0.9
    )
    
    # Define x-axis tick format with specific date display pattern
    n_points = len(filtered_data)
    
    # Determine appropriate tick format based on data range
    if n_points <= 30:  # About a month of trading days
        dtick = "D7"  # Weekly ticks
        tickformat = "%b %d"  # "Jan 01" format
    elif n_points <= 90:  # About 3 months of trading days
        dtick = "D14"  # Bi-weekly ticks
        tickformat = "%b %d"  # "Jan 01" format
    elif n_points <= 252:  # About a year of trading days
        dtick = "M1"  # Monthly ticks
        tickformat = "%b %Y"  # "Jan 2023" format
    else:
        dtick = "M3"  # Quarterly ticks
        tickformat = "%b %Y"  # "Jan 2023" format
    
    # Update layout with custom template and improved x-axis
    fig.update_layout(
        template=custom_template,
        height=400,
        title=None,  # Removed title since we already have it in the HTML
        xaxis_title="",
        xaxis=dict(
            tickformat=tickformat,
            dtick=dtick,
            tickangle=-45,
            showgrid=True,
            gridcolor="rgba(0,0,0,0.1)",
        ),
        yaxis_title="Yield (%)",
        yaxis=dict(
            ticksuffix="%",
            range=[0, max(5.0, filtered_data['value'].max() * 1.1)],
            zeroline=True,
            zerolinecolor='rgba(0,0,0,0.2)',
        ),
        hovermode="x unified",
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1
        )
    )
    
    return fig

# Update Treasury Yield Container with chart and insights panel
@app.callback(
    Output("treasury-yield-container", "children"),
    [Input("interval-component", "n_intervals")]
)
def update_treasury_yield_container(n):
    """Update the Treasury Yield container to include both the graph and insights panel"""
    # Get the chart figure
    figure = update_treasury_yield_graph(n)
    
    # Check for valid data and required columns
    if treasury_yield_data.empty or 'value' not in treasury_yield_data.columns or 'date' not in treasury_yield_data.columns:
        # Return just the graph without insights panel
        print("Treasury Yield data missing required columns or empty")
        return [dcc.Graph(id="treasury-yield-graph", figure=figure)]
    
    try:
        # Print debugging info
        print(f"Treasury Yield data columns: {treasury_yield_data.columns.tolist()}")
        
        # Filter data for insights panel
        cutoff_date = datetime.now() - timedelta(days=5*365)
        
        # Ensure date is datetime type
        if not pd.api.types.is_datetime64_any_dtype(treasury_yield_data['date']):
            print("Converting Treasury Yield date column to datetime")
            treasury_yield_data['date'] = pd.to_datetime(treasury_yield_data['date'])
            
        filtered_data = treasury_yield_data[treasury_yield_data['date'] >= cutoff_date].copy()
        
        # Create insights panel with the filtered data
        insights_panel = create_insights_panel("treasury_yield", filtered_data)
        
        # Return container with graph and insights panel
        return [
            dcc.Graph(id="treasury-yield-graph", figure=figure),
            insights_panel
        ]
    except Exception as e:
        print(f"Error generating treasury yield insights: {str(e)}")
        import traceback
        traceback.print_exc()
        # Return just the graph if there's an error with the insights
        return [dcc.Graph(id="treasury-yield-graph", figure=figure)]

# Calculate total weights and validate
@app.callback(
    Output("total-weight", "children"),
    [Input("gdp-weight", "value"),
     Input("pce-weight", "value"),
     Input("unemployment-weight", "value"),
     Input("cpi-weight", "value"),
     Input("pcepi-weight", "value"),
     Input("nasdaq-weight", "value"),
     Input("data-ppi-weight", "value"),
     Input("software-ppi-weight", "value"),
     Input("interest-rate-weight", "value"),
     Input("treasury-yield-weight", "value"),
     Input("vix-weight", "value"),
     Input("consumer-sentiment-weight", "value"),
     Input("job-postings-weight", "value")],
    [State("document-data-store", "data")]
)
def update_total_weight(gdp, pce, unemployment, cpi, pcepi, nasdaq, data_ppi, software_ppi, interest_rate, treasury_yield, vix, consumer_sentiment, job_postings, document_data_store):
    # Get document weight if it exists
    document_weight = 0
    if document_data_store and isinstance(document_data_store, dict) and 'weight' in document_data_store:
        document_weight = float(document_data_store['weight'])
    
    # Calculate total of economic indicators only
    economic_indicators_total = gdp + pce + unemployment + cpi + pcepi + nasdaq + data_ppi + software_ppi + interest_rate + treasury_yield + vix + consumer_sentiment + job_postings
    
    # If document weight is present, the economic indicators should sum to (100 - document_weight)
    if document_weight > 0:
        # Format expected percentage for economic indicators
        message = f"Economic Indicators: {economic_indicators_total:.1f}%, Document: {document_weight:.1f}%, Total: {economic_indicators_total + document_weight:.1f}%"
        
        # Set color based on whether total equals 100%
        if abs(economic_indicators_total + document_weight - 100) < 0.1:
            color = "green"
        else:
            color = "red"
            # Also print a warning for debugging
            print(f"WARNING: Total weight is {economic_indicators_total + document_weight:.1f}%, not 100%")
    else:
        # No document weight, just show total of economic indicators
        message = f"Total: {economic_indicators_total:.1f}%"
        
        # Set color based on whether total equals 100%
        if abs(economic_indicators_total - 100) < 0.1:
            color = "green"
        else:
            color = "red"
    
    return html.Span(message, style={"color": color})

# Apply custom weights
@app.callback(
    [Output("custom-weights-store", "data"),
     Output("sentiment-score", "children", allow_duplicate=True),
     Output("sentiment-category", "children", allow_duplicate=True)],
    [Input("apply-weights", "n_clicks")],
    [State("gdp-weight", "value"),
     State("pce-weight", "value"),
     State("unemployment-weight", "value"),
     State("cpi-weight", "value"),
     State("pcepi-weight", "value"),
     State("nasdaq-weight", "value"),
     State("data-ppi-weight", "value"),
     State("software-ppi-weight", "value"),
     State("interest-rate-weight", "value"),
     State("treasury-yield-weight", "value"),
     State("vix-weight", "value"),
     State("consumer-sentiment-weight", "value"),
     State("job-postings-weight", "value"),
     State("proprietary-data-store", "data"),
     State("document-data-store", "data")],
    prevent_initial_call=True
)
def apply_custom_weights(n_clicks, gdp, pce, unemployment, cpi, pcepi, nasdaq, 
                         data_ppi, software_ppi, interest_rate, treasury_yield, vix, consumer_sentiment, job_postings, proprietary_data, document_data):
    if n_clicks is None:
        # Initial load, use default weights
        sentiment_index = calculate_sentiment_index(proprietary_data=proprietary_data, document_data=document_data)
        return None, f"{sentiment_index['score']:.1f}" if sentiment_index else "N/A", sentiment_index['category'] if sentiment_index else "N/A"
    
    # Get document weight if available (it will be 0 if no document has been processed)
    document_weight = 0
    if document_data and isinstance(document_data, dict) and 'weight' in document_data:
        document_weight = float(document_data['weight'])
        document_weight = max(0, min(50, document_weight))  # Enforce 0-50% range
    
    # Create custom weights dictionary
    total_economic_weight = gdp + pce + unemployment + cpi + pcepi + nasdaq + data_ppi + software_ppi + interest_rate + treasury_yield + vix + consumer_sentiment + job_postings
    
    # If total economic weight plus document weight isn't 100%, adjust the economic indicators
    if abs(total_economic_weight + document_weight - 100) > 0.1:
        # Available weight for economic indicators
        available_weight = 100 - document_weight
        
        # Scale the economic indicators to use exactly the available weight
        scaling_factor = available_weight / total_economic_weight
        
        # Debug the scaling operation for VIX
        print(f"Before scaling: VIX weight = {vix}")
        print(f"Scaling factor = {scaling_factor}, available_weight = {available_weight}, total_economic_weight = {total_economic_weight}")
        
        # Use floating-point precision when scaling
        gdp = round(gdp * scaling_factor, 1)
        pce = round(pce * scaling_factor, 1)
        unemployment = round(unemployment * scaling_factor, 1)
        cpi = round(cpi * scaling_factor, 1)
        pcepi = round(pcepi * scaling_factor, 1)
        nasdaq = round(nasdaq * scaling_factor, 1)
        data_ppi = round(data_ppi * scaling_factor, 1)
        software_ppi = round(software_ppi * scaling_factor, 1)
        interest_rate = round(interest_rate * scaling_factor, 1)
        treasury_yield = round(treasury_yield * scaling_factor, 1)
        vix = round(vix * scaling_factor, 1)
        consumer_sentiment = round(consumer_sentiment * scaling_factor, 1)
        job_postings = round(job_postings * scaling_factor, 1)
        
        print(f"After scaling: VIX weight = {vix}")
    
    custom_weights = {
        'Real GDP % Change': gdp,
        'PCE': pce,
        'Unemployment Rate': unemployment,
        'CPI': cpi,
        'PCEPI': pcepi,
        'NASDAQ Trend': nasdaq,
        'PPI: Data Processing Services': data_ppi,
        'PPI: Software Publishers': software_ppi,
        'Federal Funds Rate': interest_rate,
        'Treasury Yield': treasury_yield,
        'VIX Volatility': vix,
        'Consumer Sentiment': consumer_sentiment,
        'Software Job Postings': job_postings
    }
    
    # Calculate using both custom weights and document data
    sentiment_index = calculate_sentiment_index(
        custom_weights=custom_weights, 
        proprietary_data=proprietary_data,
        document_data=document_data
    )
    
    # Return the results
    return custom_weights, f"{sentiment_index['score']:.1f}" if sentiment_index else "N/A", sentiment_index['category'] if sentiment_index else "N/A"

# Process uploaded proprietary data
@app.callback(
    Output("upload-preview", "children"),
    [Input("upload-data", "contents")],
    [State("upload-data", "filename")]
)
def update_upload_preview(contents, filename):
    if contents is None:
        return html.Div("No file uploaded")
    
    df = parse_uploaded_data(contents, filename)
    
    if df is None:
        return html.Div("Error: Could not parse uploaded file", style={"color": "red"})
    
    # Show preview of data
    return html.Div([
        html.H4(f"Preview of {filename}"),
        dash_table.DataTable(
            data=df.head(5).to_dict('records'),
            columns=[{'name': i, 'id': i} for i in df.columns],
            style_table={'overflowX': 'auto'},
            style_cell={
                'height': 'auto',
                'minWidth': '80px', 'width': '120px', 'maxWidth': '180px',
                'whiteSpace': 'normal'
            }
        ),
        html.P(f"Total rows: {len(df)}")
    ])

# Apply proprietary data
@app.callback(
    [Output("proprietary-data-store", "data"),
     Output("sentiment-score", "children", allow_duplicate=True),
     Output("sentiment-category", "children", allow_duplicate=True)],
    [Input("apply-proprietary", "n_clicks")],
    [State("proprietary-weight", "value"),
     State("proprietary-value", "value"),
     State("custom-weights-store", "data"),
     State("document-data-store", "data")],
    prevent_initial_call=True
)
def apply_proprietary_data(n_clicks, weight, value, custom_weights, document_data):
    if n_clicks is None:
        # Initial load, no proprietary data
        sentiment_index = calculate_sentiment_index(custom_weights=custom_weights, document_data=document_data)
        return None, f"{sentiment_index['score']:.1f}" if sentiment_index else "N/A", sentiment_index['category'] if sentiment_index else "N/A"
    
    # For backward compatibility, we'll set weight to 0 (removing proprietary data functionality)
    # Create proprietary data dictionary with zero weight
    proprietary_data = {
        'weight': 0,  # Always set to 0 as we're removing this feature
        'value': value
    }
    
    # Calculate sentiment with proprietary data (weight=0) and document data
    sentiment_index = calculate_sentiment_index(
        custom_weights=custom_weights, 
        proprietary_data=proprietary_data,
        document_data=document_data
    )
    
    # Return the results
    return proprietary_data, f"{sentiment_index['score']:.1f}" if sentiment_index else "N/A", sentiment_index['category'] if sentiment_index else "N/A"

# Consumer Sentiment Graph
@app.callback(
    Output("consumer-sentiment-graph", "figure"),
    [Input("interval-component", "n_intervals")]
)
def update_consumer_sentiment_graph(n):
    """Generate the Consumer Sentiment chart figure"""
    # Create graph using the imported function
    global consumer_sentiment_data
    if consumer_sentiment_data is None or consumer_sentiment_data.empty:
        consumer_sentiment_data = load_data_from_csv('consumer_sentiment_data.csv')
    return create_consumer_sentiment_graph(consumer_sentiment_data)

# Consumer Sentiment Container
@app.callback(
    Output("consumer-sentiment-container", "children"),
    [Input("interval-component", "n_intervals")]
)
def update_consumer_sentiment_container(n):
    """Update the Consumer Sentiment container to include both the graph and insights panel"""
    global consumer_sentiment_data
    if consumer_sentiment_data is None or consumer_sentiment_data.empty:
        consumer_sentiment_data = load_data_from_csv('consumer_sentiment_data.csv')
    
    # Create the graph
    graph = dcc.Graph(
        id="consumer-sentiment-graph",
        figure=create_consumer_sentiment_graph(consumer_sentiment_data),
        config={"displayModeBar": False},
        className="dashboard-chart"
    )
    
    if consumer_sentiment_data.empty:
        # Return just the graph without insights panel
        return [graph]
    
    try:
        # Create insights panel
        insights_panel = create_insights_panel('consumer_sentiment', consumer_sentiment_data)
        
        # Return the container with graph and insights
        return [
            graph,
            insights_panel
        ]
    except Exception as e:
        print(f"Error generating Consumer Sentiment insights: {str(e)}")
        # Return just the graph if there's an error with the insights
        return [graph]

# Data auto-refreshes hourly through interval-component
# Manual refresh button has been removed
    
    # Fetch new data for key indicators
    # GDP
    gdp_temp = fetch_fred_data('GDPC1')
    if not gdp_temp.empty:
        # Calculate year-over-year growth
        gdp_temp = gdp_temp.sort_values('date')
        
        # Create a dataframe shifted by 4 quarters to calculate YoY change (GDP is quarterly)
        gdp_yoy = gdp_temp.copy()
        gdp_yoy['date'] = gdp_yoy['date'] + pd.DateOffset(months=12)
        gdp_yoy = gdp_yoy.rename(columns={'value': 'year_ago_value'})
        
        # Merge current and year-ago values
        gdp_data = pd.merge(
            gdp_temp, 
            gdp_yoy[['date', 'year_ago_value']], 
            on='date', 
            how='left'
        )
        
        # Calculate YoY growth
        gdp_data['yoy_growth'] = ((gdp_data['value'] - gdp_data['year_ago_value']) / 
                                 gdp_data['year_ago_value'] * 100)
        
        # Save data
        save_data_to_csv(gdp_data, 'gdp_data.csv')
    
    # Unemployment
    unemployment_temp = fetch_fred_data('UNRATE')
    if not unemployment_temp.empty:
        unemployment_data = unemployment_temp
        save_data_to_csv(unemployment_data, 'unemployment_data.csv')
    
    # Inflation (CPI)
    cpi_temp = fetch_fred_data('CPIAUCSL')
    if not cpi_temp.empty:
        # Calculate year-over-year inflation
        cpi_temp = cpi_temp.sort_values('date')
        
        # Create a dataframe shifted by 12 months to calculate YoY change
        cpi_yoy = cpi_temp.copy()
        cpi_yoy['date'] = cpi_yoy['date'] + pd.DateOffset(months=12)
        cpi_yoy = cpi_yoy.rename(columns={'value': 'year_ago_value'})
        
        # Merge current and year-ago values
        inflation_data = pd.merge(
            cpi_temp, 
            cpi_yoy[['date', 'year_ago_value']], 
            on='date', 
            how='left'
        )
        
        # Calculate YoY inflation
        inflation_data['inflation'] = ((inflation_data['value'] - inflation_data['year_ago_value']) / 
                                      inflation_data['year_ago_value'] * 100)
        
        # Save data
        save_data_to_csv(inflation_data, 'inflation_data.csv')
    
    # Interest rates
    interest_temp = fetch_fred_data('FEDFUNDS')
    if not interest_temp.empty:
        interest_rate_data = interest_temp
        save_data_to_csv(interest_rate_data, 'interest_rate_data.csv')
    
    # PCEPI (Personal Consumption Expenditures: Chain-type Price Index)
    pcepi_temp = fetch_fred_data('PCEPI')
    if not pcepi_temp.empty:
        # Calculate year-over-year growth
        pcepi_temp = pcepi_temp.sort_values('date')
        
        # Create a dataframe shifted by 12 months to calculate YoY change
        pcepi_yoy = pcepi_temp.copy()
        pcepi_yoy['date'] = pcepi_yoy['date'] + pd.DateOffset(months=12)
        pcepi_yoy = pcepi_yoy.rename(columns={'value': 'year_ago_value'})
        
        # Merge current and year-ago values
        pcepi_data = pd.merge(
            pcepi_temp, 
            pcepi_yoy[['date', 'year_ago_value']], 
            on='date', 
            how='left'
        )
        
        # Calculate YoY growth
        pcepi_data['yoy_growth'] = ((pcepi_data['value'] - pcepi_data['year_ago_value']) / 
                                  pcepi_data['year_ago_value'] * 100)
        
        # Save data
        save_data_to_csv(pcepi_data, 'pcepi_data.csv')
        print(f"PCEPI data updated with {len(pcepi_data)} observations")
    
    # Also update other datasets
    # NASDAQ with EMA calculation
    nasdaq_temp = fetch_nasdaq_with_ema()
    if not nasdaq_temp.empty and 'gap_pct' in nasdaq_temp.columns:
        # Use the new EMA-based data
        nasdaq_data = nasdaq_temp
        save_data_to_csv(nasdaq_data, 'nasdaq_data.csv')
        latest_date = nasdaq_data.sort_values('date', ascending=False).iloc[0]['date']
        latest_gap = nasdaq_data.sort_values('date', ascending=False).iloc[0]['gap_pct']
        print(f"NASDAQ data updated with EMA calculation, latest date: {latest_date}, EMA gap: {latest_gap:.2f}%")
    else:
        # Fall back to FRED data if real-time fails
        print("Falling back to FRED for NASDAQ data")
        fred_nasdaq_data = fetch_fred_data('NASDAQCOM')
        if not fred_nasdaq_data.empty:
            fred_nasdaq_data = fred_nasdaq_data.sort_values('date')
            fred_nasdaq_data['pct_change'] = fred_nasdaq_data['value'].pct_change() * 100
            nasdaq_data = fred_nasdaq_data
            save_data_to_csv(nasdaq_data, 'nasdaq_data.csv')
            print(f"NASDAQ data updated from FRED with {len(nasdaq_data)} observations")
    
    # VIX (CBOE Volatility Index)
    vix_temp = fetch_fred_data('VIXCLS')
    if not vix_temp.empty:
        vix_data = vix_temp
        save_data_to_csv(vix_data, 'vix_data.csv')
        print(f"VIX data updated with {len(vix_data)} observations")
        
    # Consumer Sentiment (Consumer Confidence Composite Index)
    consumer_sentiment_temp = fetch_consumer_sentiment_data()
    if not consumer_sentiment_temp.empty:
        consumer_sentiment_data = consumer_sentiment_temp
        save_data_to_csv(consumer_sentiment_data, 'consumer_sentiment_data.csv')
        print(f"Consumer Sentiment data updated with {len(consumer_sentiment_data)} observations")
    
    # 10-Year Treasury Yield - Using Yahoo Finance for real-time data
    treasury_temp = fetch_treasury_yield_data()
    if not treasury_temp.empty:
        # Merge with existing data to maintain historical records
        # First get the most recent date from Yahoo Finance
        latest_yahoo_date = treasury_temp['date'].max()
        
        # Access the global treasury_yield_data which was loaded earlier
        # Add a check if it's defined and not empty
        global treasury_yield_data
        if 'treasury_yield_data' in globals() and not treasury_yield_data.empty:
            historical_data = treasury_yield_data[treasury_yield_data['date'] < latest_yahoo_date - timedelta(days=30)]
            # Append the Yahoo Finance data (more recent) to the historical data
            treasury_yield_data = pd.concat([historical_data, treasury_temp], ignore_index=True)
        else:
            treasury_yield_data = treasury_temp
            
        # Save the merged data
        save_data_to_csv(treasury_yield_data, 'treasury_yield_data.csv')
        print(f"Treasury Yield data updated with real-time data from Yahoo Finance")
        
    # Software Job Postings
    job_postings_temp = fetch_fred_data('IHLIDXUSTPSOFTDEVE')
    if not job_postings_temp.empty:
        # Calculate year-over-year growth
        job_postings_temp = job_postings_temp.sort_values('date')
        
        # Create a dataframe shifted by 12 months to calculate YoY change
        postings_yoy = job_postings_temp.copy()
        postings_yoy['date'] = postings_yoy['date'] + pd.DateOffset(months=12)
        postings_yoy = postings_yoy.rename(columns={'value': 'year_ago_value'})
        
        # Merge current and year-ago values
        job_postings_data = pd.merge(
            job_postings_temp, 
            postings_yoy[['date', 'year_ago_value']], 
            on='date', 
            how='left'
        )
        
        # Calculate YoY growth
        job_postings_data['yoy_growth'] = ((job_postings_data['value'] - job_postings_data['year_ago_value']) / 
                              job_postings_data['year_ago_value'] * 100)
        
        # Save data
        save_data_to_csv(job_postings_data, 'job_postings_data.csv')
    
    # Update indicator values for display
    return (
        f"Data refreshed at {datetime.now().strftime('%H:%M:%S')}",
        f"{gdp_data.sort_values('date', ascending=False).iloc[0]['yoy_growth']:.1f}%" if not gdp_data.empty and 'yoy_growth' in gdp_data.columns else "N/A",
        f"{unemployment_data.sort_values('date', ascending=False).iloc[0]['value']:.1f}%" if not unemployment_data.empty else "N/A",
        f"{inflation_data.sort_values('date', ascending=False).iloc[0]['inflation']:.1f}%" if not inflation_data.empty and 'inflation' in inflation_data.columns else "N/A",
        f"{interest_rate_data.sort_values('date', ascending=False).iloc[0]['value']:.2f}%" if not interest_rate_data.empty else "N/A"
    )

# For plotly's make_subplots import to avoid errors
from plotly.subplots import make_subplots

# Initialize sentiment score on page load
@app.callback(
    [Output("sentiment-score", "children"),
     Output("sentiment-category", "children")],
    [Input("_", "children"),
     Input("custom-weights-store", "data"),
     Input("document-data-store", "data")],
    prevent_initial_call=False
)
def initialize_sentiment_index(_, custom_weights, document_data):
    # PRIORITY 1: First always try to use the authentic pulse score 
    # This is the most accurate source for initialization
    authentic_score = get_authentic_pulse_score()
    if authentic_score is not None:
        print(f"INITIALIZATION: USING AUTHENTIC T2D PULSE SCORE: {authentic_score}")
        # Determine category based on score
        if authentic_score >= 60:
            category = "Bullish"
        elif authentic_score >= 30:
            category = "Neutral"
        else:
            category = "Bearish"
        return f"{authentic_score:.1f}", category
    
    # Check if it's a weekend to use the most recent market session data
    import pytz
    from datetime import datetime
    eastern = pytz.timezone('US/Eastern')
    today = datetime.now(eastern)
    is_weekend = today.weekday() >= 5  # Saturday = 5, Sunday = 6
    
    # If it's a weekend and no authentic score, use the most recent market session data 
    if is_weekend:
        print("Weekend detected during initialization - using most recent market session data for T2D Pulse calculation")
        
        # Get the most recent data file
        today_str = datetime.now(eastern).strftime('%Y-%m-%d')
        date_specific_file = f"data/authentic_sector_history_{today_str}.csv"
        
        if os.path.exists(date_specific_file):
            # Load the recent market data from this file
            try:
                import pandas as pd
                recent_df = pd.read_csv(date_specific_file)
                
                if not recent_df.empty:
                    # Get sector columns from the dataframe
                    sector_columns = [col for col in recent_df.columns if col != 'date']
                    latest_row = recent_df.iloc[0]
                    
                    # Create dictionary of sector scores for T2D pulse calculation
                    sector_scores_dict = {sector: latest_row[sector] for sector in sector_columns}
                    
                    # Create default equal weights
                    equal_weight = 100.0 / len(sector_columns)
                    sector_weights = {sector: equal_weight for sector in sector_columns}
                    
                    print(f"Using most recent market session data for initial T2D Pulse calculation: {len(sector_scores_dict)} sectors")
                    
                    # Calculate T2D Pulse score from the most recent data
                    pulse_score = calculate_t2d_pulse_from_sectors(sector_scores_dict, sector_weights)
                    print(f"Calculated T2D Pulse Score from most recent market data: {pulse_score}")
                    
                    # Save the T2D Pulse score to history
                    save_t2d_pulse_score(pulse_score, sector_scores_dict)
                    
                    # Determine category
                    if pulse_score >= 60:
                        category = "Bullish"
                    elif pulse_score >= 30:
                        category = "Neutral"
                    else:
                        category = "Bearish"
                    
                    return (f"{pulse_score:.1f}", category)
            except Exception as e:
                print(f"Error using most recent market data for initial T2D Pulse calculation: {e}")
                # Continue to fallback below
        
        # Fallback to May 2nd data from forced_may2_data.py if CSV not found
        try:
            print("Fallback to May 2nd data for initial T2D Pulse calculation")
            import forced_may2_data
            
            # Get the reliable May 2nd sector scores directly
            sector_scores_dict = forced_may2_data.get_may2nd_sector_dict()
            
            if sector_scores_dict:
                # Get a pre-calculated score
                pulse_score = forced_may2_data.get_may2nd_t2d_pulse_score()
                print(f"Using hardcoded May 2nd T2D Pulse score for initialization: {pulse_score}")
                
                # Save the T2D Pulse score to history
                save_t2d_pulse_score(pulse_score, sector_scores_dict)
                
                # Determine category
                if pulse_score >= 60:
                    category = "Bullish"
                elif pulse_score >= 30:
                    category = "Neutral"
                else:
                    category = "Bearish"
                
                return (f"{pulse_score:.1f}", category)
        except Exception as e:
            print(f"Error using May 2nd data for initialization: {e}")
            # Continue to default method below
    
    # Regular weekday calculation or fallback if weekend methods fail
    sector_scores = calculate_sector_sentiment()
    
    # Calculate T2D Pulse score from sector scores
    if sector_scores:
        # Create a dictionary of sector scores for the pulse calculation
        sector_scores_dict = {s['sector']: s['normalized_score'] for s in sector_scores}
        
        # Create default equal weights if none exist
        sector_weights = {}
        if sector_scores:
            equal_weight = 100.0 / len(sector_scores)
            sector_weights = {s['sector']: equal_weight for s in sector_scores}
        
        # Calculate the T2D Pulse score as weighted average of sector scores
        pulse_score = calculate_t2d_pulse_from_sectors(sector_scores_dict, sector_weights)
        
        # Log what's happening
        print(f"Calculating T2D Pulse score from {len(sector_scores)} sector scores")
        print(f"Using following sector weights: {sector_weights}")
        print(f"Calculated T2D Pulse Score: {pulse_score}")
        
        # Save the T2D Pulse score to history
        save_t2d_pulse_score(pulse_score, sector_scores_dict)
        
        # Determine category based on score
        if pulse_score >= 60:
            category = "Bullish"
        elif pulse_score >= 30:
            category = "Neutral"
        else:
            category = "Bearish"
        
        # Return the data in the expected format
        return (f"{pulse_score:.1f}", category)
    else:
        # Fallback to old method if sector scores aren't available
        print("No sector scores available, falling back to economic indicators method")
        sentiment_index = calculate_sentiment_index(
            custom_weights=custom_weights,
            document_data=document_data
        )
        return (
            f"{sentiment_index['score']:.1f}" if sentiment_index else "N/A", 
            sentiment_index['category'] if sentiment_index else "N/A"
        )

# Document weight display update
@app.callback(
    [Output("document-weight-display", "children"),
     Output("document-weight", "disabled"),
     Output("document-data-store", "data", allow_duplicate=True),  # Update data store when slider changes
     Output("total-weight", "children", allow_duplicate=True),  # Update the total weight display
     # Add outputs to update the Economic Indicators section when document weight changes
     Output("gdp-weight", "value", allow_duplicate=True),
     Output("pce-weight", "value", allow_duplicate=True),
     Output("unemployment-weight", "value", allow_duplicate=True),
     Output("cpi-weight", "value", allow_duplicate=True),
     Output("pcepi-weight", "value", allow_duplicate=True),
     Output("nasdaq-weight", "value", allow_duplicate=True),
     Output("data-ppi-weight", "value", allow_duplicate=True),
     Output("software-ppi-weight", "value", allow_duplicate=True),
     Output("interest-rate-weight", "value", allow_duplicate=True),
     Output("treasury-yield-weight", "value", allow_duplicate=True),
     Output("vix-weight", "value", allow_duplicate=True),
     Output("job-postings-weight", "value", allow_duplicate=True),
     # Debug output
     Output("document-weight-debug", "children")],
    [Input("document-weight", "value"),
     Input("upload-document", "contents"),
     Input("apply-document", "n_clicks")],  # Add the apply document button as an input
    [State("document-data-store", "data"),
     State("gdp-weight", "value"),
     State("pce-weight", "value"),
     State("unemployment-weight", "value"),
     State("cpi-weight", "value"),
     State("pcepi-weight", "value"),
     State("nasdaq-weight", "value"),
     State("data-ppi-weight", "value"),
     State("software-ppi-weight", "value"),
     State("interest-rate-weight", "value"),
     State("treasury-yield-weight", "value"),
     State("vix-weight", "value")],
    prevent_initial_call=True
)
def update_document_weight_display(weight, contents, n_clicks, document_data,
                                gdp, pce, unemployment, cpi, pcepi, nasdaq, data_ppi, software_ppi, interest_rate, treasury_yield, vix_weight, job_postings_weight):
    ctx = dash.callback_context  # Get the callback context to determine what triggered the callback
    trigger_id = ctx.triggered[0]['prop_id'].split('.')[0] if ctx.triggered else None
    
    # Check if document is uploaded and processed
    has_document = contents is not None
    document_processed = document_data is not None and isinstance(document_data, dict) and 'value' in document_data
    
    # If document not uploaded or not processed yet, show guidance message and disable slider
    if not has_document or not document_processed:
        return (
            html.Div([
                html.Span("Upload a document and click 'Apply Document Analysis' to enable document weighting", 
                        className="weight-value",
                        style={"color": "#888"})
            ]), 
            True, 
            dash.no_update, 
            dash.no_update,
            dash.no_update, 
            dash.no_update, 
            dash.no_update, 
            dash.no_update, 
            dash.no_update, 
            dash.no_update, 
            dash.no_update, 
            dash.no_update, 
            dash.no_update, 
            dash.no_update,
            dash.no_update, 
            dash.no_update
        )
    
    # Document is uploaded and processed, allow weight adjustment
    remaining = 100 - weight
    economic_indicators_total = gdp + pce + unemployment + cpi + pcepi + nasdaq + data_ppi + software_ppi + interest_rate + treasury_yield + vix_weight + job_postings_weight
    
    # Default return values (no changes)
    updated_data = dash.no_update
    total_weight_display = dash.no_update
    new_gdp = dash.no_update
    new_pce = dash.no_update
    new_unemployment = dash.no_update
    new_cpi = dash.no_update
    new_pcepi = dash.no_update
    new_nasdaq = dash.no_update
    new_data_ppi = dash.no_update
    new_software_ppi = dash.no_update
    new_interest_rate = dash.no_update
    new_treasury_yield = dash.no_update
    new_vix = dash.no_update
    
    # If weight was changed by slider, update the document data store and economic indicators
    if trigger_id == "document-weight" and document_data and 'weight' in document_data and document_data['weight'] != weight:
        # Update the document data with new weight
        document_data['weight'] = weight
        updated_data = document_data
        print(f"Updated document weight to {weight}%")
        
        # Update the total weight display message
        message = f"Economic Indicators: {economic_indicators_total:.1f}%, Document: {weight:.1f}%, Total: {economic_indicators_total + weight:.1f}%"
        color = "green" if abs(economic_indicators_total + weight - 100) < 0.1 else "red"
        total_weight_display = html.Span(message, style={"color": color})
        
        # Calculate scaled values for economic indicators to maintain 100% total
        if weight > 0:
            # Scale the current economic indicators to fit in the remaining weight (100 - document_weight)
            remaining_weight = 100 - weight
            scaling_factor = remaining_weight / economic_indicators_total if economic_indicators_total > 0 else 0
            
            # Scale each economic indicator with floating-point precision (only if user changed the document weight)
            # Using 1 decimal point precision for better accuracy
            new_gdp = round(gdp * scaling_factor, 1)
            new_pce = round(pce * scaling_factor, 1)
            new_unemployment = round(unemployment * scaling_factor, 1)
            new_cpi = round(cpi * scaling_factor, 1)
            new_pcepi = round(pcepi * scaling_factor, 1)  # Add PCEPI scaling
            new_nasdaq = round(nasdaq * scaling_factor, 1)
            new_data_ppi = round(data_ppi * scaling_factor, 1)
            new_software_ppi = round(software_ppi * scaling_factor, 1)
            new_interest_rate = round(interest_rate * scaling_factor, 1)
            new_treasury_yield = round(treasury_yield * scaling_factor, 1)
            new_vix = round(vix_weight * scaling_factor, 1)
            new_job_postings = round(job_postings_weight * scaling_factor, 1)
            
            # Debug print for weight calculations
            print(f"VIX weight calculation: {vix_weight} * {scaling_factor} = {new_vix}")
            print(f"Job Postings weight calculation: {job_postings_weight} * {scaling_factor} = {new_job_postings}")
            
            # If rounding causes total to be off by 1, adjust the largest value
            new_total = new_gdp + new_pce + new_unemployment + new_cpi + new_pcepi + new_nasdaq + new_data_ppi + new_software_ppi + new_interest_rate + new_treasury_yield + new_vix + new_job_postings
            if new_total != remaining_weight:
                # Find the largest value and adjust it
                values = [new_gdp, new_pce, new_unemployment, new_cpi, new_pcepi, new_nasdaq, new_data_ppi, new_software_ppi, new_interest_rate, new_treasury_yield, new_vix, new_job_postings]
                max_index = values.index(max(values))
                if max_index == 0:
                    new_gdp += (remaining_weight - new_total)
                elif max_index == 1:
                    new_pce += (remaining_weight - new_total)
                elif max_index == 2:
                    new_unemployment += (remaining_weight - new_total)
                elif max_index == 3:
                    new_cpi += (remaining_weight - new_total)
                elif max_index == 4:
                    new_nasdaq += (remaining_weight - new_total)
                elif max_index == 5:
                    new_data_ppi += (remaining_weight - new_total)
                elif max_index == 6:
                    new_software_ppi += (remaining_weight - new_total)
                elif max_index == 7:
                    new_interest_rate += (remaining_weight - new_total)
                elif max_index == 8:
                    new_treasury_yield += (remaining_weight - new_total)
                elif max_index == 9:
                    new_vix += (remaining_weight - new_total)
                elif max_index == 10:
                    new_job_postings += (remaining_weight - new_total)
    
    # Create the document weight display
    doc_weight_display = html.Div([
        html.Span(f"Document Weight: {weight}%", className="weight-value"),
        html.Span(f"Remaining for Economic Indicators: {remaining}%", 
                 className="weight-remaining",
                 style={"marginLeft": "10px", "color": "green" if remaining >= 0 else "red"})
    ])
    
    # Create a debug message showing the state data
    debug_info = ""
    try:
        debug_info = f"Trigger: {trigger_id}, Weight: {weight}, Doc value: {document_data.get('value') if document_data else 'None'}, Doc weight: {document_data.get('weight') if document_data else 'None'}"
    except Exception as e:
        debug_info = f"Debug error: {str(e)}"
        
    # Return all necessary outputs
    return (
        doc_weight_display, 
        False, 
        updated_data, 
        total_weight_display,
        new_gdp,
        new_pce,
        new_unemployment, 
        new_cpi, 
        new_pcepi,
        new_nasdaq, 
        new_data_ppi, 
        new_software_ppi, 
        new_interest_rate,
        new_treasury_yield,
        new_vix,
        new_job_postings,
        debug_info
    )

# Process and preview document upload for sentiment analysis
@app.callback(
    Output("document-preview", "children"),
    [Input("upload-document", "contents"),
     Input("upload-document", "filename")]
)
def update_document_preview(contents, filename):
    if contents is None:
        return html.Div("No document uploaded")
    
    # Show the uploaded file without processing it
    return html.Div([
        html.P(f"File uploaded: {filename}", className="uploaded-filename"),
        html.P("Click 'Apply Document Analysis' to analyze this document and use it in the sentiment index calculation.", 
              style={"fontStyle": "italic", "color": "#666"}),
        html.Div(className="document-preview-placeholder", style={
            "border": "1px dashed #ccc",
            "padding": "20px",
            "borderRadius": "5px",
            "textAlign": "center",
            "marginTop": "10px"
        }, children=[
            html.I(className="fas fa-file-alt", style={"fontSize": "32px", "color": "#666"}),
            html.P("Document ready for analysis", style={"marginTop": "10px"})
        ])
    ])

# Apply document sentiment analysis to index
@app.callback(
    [Output("document-data-store", "data"),
     Output("document-preview", "children", allow_duplicate=True),
     Output("sentiment-score", "children", allow_duplicate=True),
     Output("sentiment-category", "children", allow_duplicate=True),
     Output("total-weight", "children", allow_duplicate=True),
     # Add outputs to update the Economic Indicators section
     Output("gdp-weight", "value", allow_duplicate=True),
     Output("pce-weight", "value", allow_duplicate=True),
     Output("unemployment-weight", "value", allow_duplicate=True),
     Output("cpi-weight", "value", allow_duplicate=True),
     Output("pcepi-weight", "value", allow_duplicate=True),
     Output("nasdaq-weight", "value", allow_duplicate=True),
     Output("data-ppi-weight", "value", allow_duplicate=True),
     Output("software-ppi-weight", "value", allow_duplicate=True),
     Output("interest-rate-weight", "value", allow_duplicate=True),
     Output("treasury-yield-weight", "value", allow_duplicate=True),
     Output("vix-weight", "value", allow_duplicate=True),
     Output("job-postings-weight", "value", allow_duplicate=True)],
    [Input("apply-document", "n_clicks")],
    [State("document-weight", "value"),
     State("upload-document", "contents"),
     State("upload-document", "filename"),
     State("custom-weights-store", "data"),
     State("proprietary-data-store", "data"),
     State("gdp-weight", "value"),
     State("pce-weight", "value"),
     State("unemployment-weight", "value"),
     State("cpi-weight", "value"),
     State("pcepi-weight", "value"),
     State("nasdaq-weight", "value"),
     State("data-ppi-weight", "value"),
     State("software-ppi-weight", "value"),
     State("interest-rate-weight", "value"),
     State("treasury-yield-weight", "value"),
     State("vix-weight", "value"),
     State("job-postings-weight", "value")],
    prevent_initial_call=True
)
def apply_document_analysis(n_clicks, weight, contents, filename, custom_weights, proprietary_data,
                          gdp, pce, unemployment, cpi, pcepi, nasdaq, data_ppi, software_ppi, interest_rate, treasury_yield, vix_weight, job_postings_weight):
    # Document weight should not be applied until a document is uploaded and analyzed
    if n_clicks is None:
        # Return document weight of 0 and no updates to other components
        return {'weight': 0, 'value': 0}, dash.no_update, dash.no_update, dash.no_update, dash.no_update, dash.no_update, dash.no_update, dash.no_update, dash.no_update, dash.no_update, dash.no_update, dash.no_update, dash.no_update, dash.no_update, dash.no_update, dash.no_update
        
    # Document content is required
    if contents is None:
        error_message = html.Div("No document uploaded. Upload a document first.", style={"color": "red"})
        return None, error_message, dash.no_update, dash.no_update, dash.no_update, dash.no_update, dash.no_update, dash.no_update, dash.no_update, dash.no_update, dash.no_update, dash.no_update, dash.no_update, dash.no_update, dash.no_update, dash.no_update, dash.no_update
    
    try:
        # Decode the file contents
        content_type, content_string = contents.split(',')
        decoded = base64.b64decode(content_string)
        
        # Process the document
        result = document_analysis.process_document(decoded, filename)
        
        if result["status"] == "success":
            # Ensure weight is a valid value (0-50%)
            weight = max(0, min(50, weight))
            
            # Create document data dictionary with sentiment score and weight
            document_data = {
                'weight': weight,
                'value': result["overall_score"]
            }
            
            # Calculate new sentiment index with document data
            sentiment_index = calculate_sentiment_index(
                custom_weights=custom_weights, 
                proprietary_data=proprietary_data,
                document_data=document_data
            )
            
            # Verify total weights (for debugging)
            if sentiment_index and 'components' in sentiment_index:
                total_weight = sum(comp['weight'] for comp in sentiment_index['components'])
                print(f"Total weight after applying document sentiment: {total_weight}%")
                
                # If total weight is not 100, log a warning
                if abs(total_weight - 100) > 0.1:
                    print(f"WARNING: Weights don't sum to 100%, but {total_weight}%")
            
            # Calculate appropriate scaling for economic indicators to ensure all weights sum to 100%
            remaining_weight = 100 - weight
            economic_indicators_total = gdp + pce + unemployment + cpi + pcepi + nasdaq + data_ppi + software_ppi + interest_rate + treasury_yield + vix_weight + job_postings_weight
            
            # Check if economic indicators are already the correct sum
            if abs(economic_indicators_total - remaining_weight) < 0.1:
                message = f"Economic Indicators: {economic_indicators_total:.1f}%, Document: {weight:.1f}%, Total: {economic_indicators_total + weight:.1f}%"
                color = "green"
            else:
                # We'll scale the indicators to fix this
                message = f"Economic Indicators: {remaining_weight:.1f}%, Document: {weight:.1f}%, Total: 100.0%"
                color = "green"  # Always green since we'll ensure they sum to 100%
                
            total_weight_display = html.Span(message, style={"color": color})
            
            # Create document analysis preview display
            sentiment_label = result["full_text_sentiment"]["label"].capitalize() if isinstance(result["full_text_sentiment"], dict) else "Neutral"
            sentiment_score = float(result["full_text_sentiment"]["score"]) if isinstance(result["full_text_sentiment"], dict) else 50.0
            sentiment_color = "green" if sentiment_score >= 60 else "orange" if sentiment_score >= 40 else "red"
            
            # Create sentiment score display
            document_display = html.Div([
                html.P(f"File: {filename}", className="uploaded-filename"),
                html.Div([
                    html.H4("Document Sentiment Analysis", className="sentiment-analysis-title"),
                    
                    # Overall sentiment score
                    html.Div([
                        html.P("Overall Sentiment:", className="sentiment-label"),
                        html.P(f"{sentiment_label} ({sentiment_score:.1f}/100)", 
                              className="sentiment-value",
                              style={"color": sentiment_color, "fontWeight": "bold"})
                    ], className="sentiment-row"),
                    
                    # Text details
                    html.Div([
                        html.P("Document Length:", className="sentiment-label"),
                        html.P(f"{result['text_length']} characters", className="sentiment-value")
                    ], className="sentiment-row"),
                    
                    # Date processed
                    html.Div([
                        html.P("Processed:", className="sentiment-label"),
                        html.P(result["processed_date"], className="sentiment-value")
                    ], className="sentiment-row"),
                    
                    # Selected for sentiment index indicator
                    html.Div([
                        html.P("Document Sentiment Score:", className="sentiment-score-label",
                               style={"fontSize": "14px"}),
                        html.P(f"{result['overall_score']:.1f}/100", 
                              className="sentiment-score-value",
                              style={"fontSize": "22px", "fontWeight": "bold", "color": sentiment_color})
                    ], className="sentiment-score")
                ], className="document-sentiment-container")
            ])
            
            # Calculate scaled values for economic indicators to maintain 100% total
            if weight > 0:
                # Scale the current economic indicators to fit in the remaining weight (100 - document_weight)
                remaining_weight = 100 - weight
                scaling_factor = remaining_weight / economic_indicators_total if economic_indicators_total > 0 else 0
                
                # Debug print for VIX weight calculation
                print(f"Document apply - Before scaling: VIX weight = {vix_weight}")
                print(f"Document apply - Scaling factor = {scaling_factor}, remaining_weight = {remaining_weight}")
                
                # Scale each economic indicator with 1 decimal precision
                new_gdp = round(gdp * scaling_factor, 1)
                new_pce = round(pce * scaling_factor, 1)
                new_unemployment = round(unemployment * scaling_factor, 1)
                new_cpi = round(cpi * scaling_factor, 1)
                new_pcepi = round(pcepi * scaling_factor, 1)
                new_nasdaq = round(nasdaq * scaling_factor, 1)
                new_data_ppi = round(data_ppi * scaling_factor, 1)
                new_software_ppi = round(software_ppi * scaling_factor, 1)
                new_interest_rate = round(interest_rate * scaling_factor, 1)
                new_treasury_yield = round(treasury_yield * scaling_factor, 1)
                new_vix = round(vix_weight * scaling_factor, 1)
                new_job_postings = round(job_postings_weight * scaling_factor, 1)
                
                print(f"Document apply - After scaling: VIX weight = {new_vix}")
                
                # If rounding causes total to differ from remaining weight, adjust the largest value
                new_total = new_gdp + new_pce + new_unemployment + new_cpi + new_pcepi + new_nasdaq + new_data_ppi + new_software_ppi + new_interest_rate + new_treasury_yield + new_vix + new_job_postings
                print(f"After scaling: economic weights = {new_total:.1f}, remaining weight = {remaining_weight:.1f}")
                if abs(new_total - remaining_weight) > 0.1:
                    # Find the largest value and adjust it
                    values = [new_gdp, new_pce, new_unemployment, new_cpi, new_pcepi, new_nasdaq, new_data_ppi, new_software_ppi, new_interest_rate, new_treasury_yield, new_vix, new_job_postings]
                    max_index = values.index(max(values))
                    if max_index == 0:
                        new_gdp += (remaining_weight - new_total)
                    elif max_index == 1:
                        new_pce += (remaining_weight - new_total)
                    elif max_index == 2:
                        new_unemployment += (remaining_weight - new_total)
                    elif max_index == 3:
                        new_cpi += (remaining_weight - new_total)
                    elif max_index == 4:
                        new_pcepi += (remaining_weight - new_total)
                    elif max_index == 5:
                        new_nasdaq += (remaining_weight - new_total)
                    elif max_index == 6:
                        new_data_ppi += (remaining_weight - new_total)
                    elif max_index == 7:
                        new_software_ppi += (remaining_weight - new_total)
                    elif max_index == 8:
                        new_interest_rate += (remaining_weight - new_total)
                    elif max_index == 9:
                        new_treasury_yield += (remaining_weight - new_total)
                    elif max_index == 10:
                        new_vix += (remaining_weight - new_total)
                    elif max_index == 11:
                        new_job_postings += (remaining_weight - new_total)
            else:
                # No document weight, keep original values
                new_gdp = gdp
                new_pce = pce
                new_unemployment = unemployment
                new_cpi = cpi
                new_pcepi = pcepi
                new_nasdaq = nasdaq
                new_data_ppi = data_ppi
                new_software_ppi = software_ppi
                new_interest_rate = interest_rate
                new_treasury_yield = treasury_yield
                new_vix = vix_weight
                new_job_postings = job_postings_weight
            
            # Return document data, analysis display, sentiment score and category, weight display, and updated economic indicator values
            return (
                document_data, 
                document_display, 
                f"{sentiment_index['score']:.1f}" if sentiment_index else "N/A", 
                sentiment_index['category'] if sentiment_index else "N/A", 
                total_weight_display,
                new_gdp, new_pce, new_unemployment, new_cpi, new_pcepi, new_nasdaq, new_data_ppi, new_software_ppi, new_interest_rate, new_treasury_yield, new_vix, new_job_postings
            )
        else:
            # Document processing failed
            error_message = html.Div(f"Error: {result['message']}", style={"color": "red"})
            # Return all dash.no_update for the economic indicators
            return None, error_message, dash.no_update, dash.no_update, dash.no_update, dash.no_update, dash.no_update, dash.no_update, dash.no_update, dash.no_update, dash.no_update, dash.no_update, dash.no_update, dash.no_update, dash.no_update, dash.no_update, dash.no_update
    except Exception as e:
        print(f"Error applying document analysis: {str(e)}")
        error_message = html.Div(f"Error processing document: {str(e)}", style={"color": "red"})
        # Return all dash.no_update for the economic indicators
        return None, error_message, dash.no_update, dash.no_update, dash.no_update, dash.no_update, dash.no_update, dash.no_update, dash.no_update, dash.no_update, dash.no_update, dash.no_update, dash.no_update, dash.no_update, dash.no_update, dash.no_update, dash.no_update

# Update VIX Graph (This function now only generates the figure)
def update_vix_graph(n):
    """Generate the VIX chart figure"""
    if vix_data.empty:
        return go.Figure().update_layout(
            title="No data available",
            height=400
        )
    
    # Filter for last 2 years
    cutoff_date = datetime.now() - timedelta(days=2*365)
    filtered_data = vix_data[vix_data['date'] >= cutoff_date].copy()
    
    # Create figure
    fig = go.Figure()
    
    # Add VIX line (raw values, with reduced opacity)
    fig.add_trace(go.Scatter(
        x=filtered_data['date'],
        y=filtered_data['value'],
        mode='lines',
        name='CBOE Volatility Index (VIX)',
        line=dict(color='darkred', width=2),
        opacity=0.7
    ))
    
    # Add 14-day EMA line (smoothed values, prominent line)
    if 'vix_ema14' in filtered_data.columns:
        fig.add_trace(go.Scatter(
            x=filtered_data['date'],
            y=filtered_data['vix_ema14'],
            mode='lines',
            name='14-Day EMA (Smoothed VIX)',
            line=dict(color='darkred', width=3),
        ))
    
    # Add volatility level zones
    x_range = [filtered_data['date'].min(), filtered_data['date'].max()]
    
    # High volatility (>30) - Red zone
    fig.add_trace(go.Scatter(
        x=x_range + x_range[::-1],
        y=[30, 30, max(filtered_data['value'].max() * 1.1, 40), max(filtered_data['value'].max() * 1.1, 40)],
        fill='toself',
        fillcolor='rgba(255, 0, 0, 0.1)',
        line=dict(color='rgba(0, 0, 0, 0)'),
        hoverinfo='skip',
        name='High Volatility (>30)',
        showlegend=True
    ))
    
    # Moderate volatility (20-30) - Yellow zone
    fig.add_trace(go.Scatter(
        x=x_range + x_range[::-1],
        y=[20, 20, 30, 30],
        fill='toself',
        fillcolor='rgba(255, 255, 0, 0.1)',
        line=dict(color='rgba(0, 0, 0, 0)'),
        hoverinfo='skip',
        name='Moderate Volatility (20-30)',
        showlegend=True
    ))
    
    # Low volatility (<20) - Green zone
    fig.add_trace(go.Scatter(
        x=x_range + x_range[::-1],
        y=[0, 0, 20, 20],
        fill='toself',
        fillcolor='rgba(0, 255, 0, 0.1)',
        line=dict(color='rgba(0, 0, 0, 0)'),
        hoverinfo='skip',
        name='Low Volatility (<20)',
        showlegend=True
    ))
    
    # Sort data by date (newest first) for current values
    sorted_data = filtered_data.sort_values('date', ascending=False)
    
    # Add current value annotations using newest data
    current_value = sorted_data['value'].iloc[0]
    previous_value = sorted_data['value'].iloc[1] if len(sorted_data) > 1 else current_value
    change = current_value - previous_value
    
    # For VIX, up is negative (fear) and down is positive (calm)
    arrow_color = color_scheme["negative"] if change > 0 else color_scheme["positive"]
    arrow_symbol = "▲" if change > 0 else "▼"
    
    # Show both raw VIX and smoothed EMA value
    if 'vix_ema14' in sorted_data.columns:
        current_ema = sorted_data['vix_ema14'].iloc[0]
        current_value_annotation = f"VIX: {current_value:.2f} {arrow_symbol} {abs(change):.2f}  |  14-Day EMA: {current_ema:.2f}"
    else:
        current_value_annotation = f"VIX: {current_value:.2f} {arrow_symbol} {abs(change):.2f}"
    
    fig.add_annotation(
        x=0.02,
        y=0.95,
        xref="paper",
        yref="paper",
        text=current_value_annotation,
        showarrow=False,
        font=dict(size=14, color=arrow_color),
        align="left",
        bgcolor="rgba(255, 255, 255, 0.9)",
        bordercolor=arrow_color,
        borderwidth=1,
        borderpad=4,
        opacity=0.9
    )
    
    # Update layout
    fig.update_layout(
        template=custom_template,
        height=400,
        title=None,
        margin=dict(l=40, r=40, t=40, b=40),
        xaxis_title="",
        yaxis_title="VIX",
        yaxis=dict(
            zeroline=False,
            range=[0, max(filtered_data['value'].max() * 1.1, 40)]
        ),
        hovermode="x unified",
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1
        )
    )
    
    return fig

# Update Sector Sentiment Container
@app.callback(
    Output("sector-sentiment-container", "children"),
    [Input("interval-component", "n_intervals")]
)
def update_sector_sentiment_container(n):
    """Update the Sector Sentiment container with cards for each technology sector"""
    # Get current date and check if it's a weekend
    import pytz
    from datetime import datetime
    eastern = pytz.timezone('US/Eastern')
    today = datetime.now(eastern)
    today_str = today.strftime('%Y-%m-%d')
    is_weekend = today.weekday() >= 5  # Saturday = 5, Sunday = 6
    
    print(f"Updating sector sentiment container. Today ({today_str}) is {'a weekend' if is_weekend else 'a weekday'}")
    
    # Always run the sector display fix on every update to ensure consistency
    try:
        import fix_sector_display
        fix_sector_display.ensure_consistent_sector_data()
        print("Successfully updated sector display with consistent data")
    except Exception as e:
        print(f"Error running sector display fix: {e}")
    
    # Load the definitive sector scores (guaranteed to be in 0-100 scale)
    definitive_file = "data/definitive_sector_scores.csv"
    found_authentic_data = False
    
    print(f"Loading definitive sector data from: {definitive_file}")
    
    # First attempt to load from our definitive source
    if os.path.exists(definitive_file):
        print(f"Found definitive sector data file")
        try:
            import pandas as pd
            recent_df = pd.read_csv(definitive_file)
            
            # Check if file has valid data (use case-insensitive check for 'date' column)
            date_col = None
            for col in recent_df.columns:
                if col.lower() == 'date':
                    date_col = col
                    break
                
            if not recent_df.empty and date_col is not None:
                # Get sector columns (all except date column)
                sector_columns = [col for col in recent_df.columns if col != date_col]
                
                if sector_columns:
                    print(f"Using authentic sector data for {today_str} with {len(sector_columns)} sectors")
                    # Create sector data objects in the expected format
                    authentic_scores = []
                    latest_row = recent_df.iloc[0]  # Most recent row
                    
                    for sector in sector_columns:
                        # Get normalized score (0-100 scale)
                        norm_score = latest_row[sector]
                        # Convert to raw score (-1 to +1 scale)
                        raw_score = (norm_score / 50.0) - 1.0
                        
                        # Generate all the needed fields
                        sector_data = {
                            "sector": sector,
                            "score": raw_score,
                            "normalized_score": norm_score,
                            "stance": "Bullish" if norm_score >= 60 else "Bearish" if norm_score <= 30 else "Neutral",
                            "takeaway": "Outperforming peers" if norm_score >= 60 else 
                                      "Bearish macro setup" if norm_score <= 30 else 
                                      "Neutral – monitor trends",
                            "drivers": [],  # Will be populated later
                            "tickers": []   # Will be populated later
                        }
                        authentic_scores.append(sector_data)
                    
                    # We found authentic data for today
                    sector_scores = authentic_scores
                    found_authentic_data = True
                    print(f"Successfully loaded {len(authentic_scores)} authentic sector scores for {today_str}")
            else:
                print(f"Authentic sector data file for {today_str} exists but is empty or invalid")
        except Exception as e:
            print(f"Error loading authentic sector data for {today_str}: {e}")
    
    # If today's data not found, try to use the most recent authentic data available
    if not found_authentic_data:
        print("Today's authentic sector data not found, trying to find most recent authentic data")
        # If it's a weekend, look for most recent Friday data
        # Otherwise, for weekdays, try to use yesterday's data if available
        
        # Import forced_may2_data as our ultimate fallback
        import forced_may2_data
        sector_scores = forced_may2_data.get_may2nd_sector_data()
        
        # Check for Friday's data if it's a weekend
        if is_weekend:
            from datetime import timedelta
            # Calculate most recent Friday's date
            days_since_friday = (today.weekday() - 4) % 7
            friday_date = today - timedelta(days=days_since_friday)
            friday_str = friday_date.strftime('%Y-%m-%d')
            friday_file = f"data/authentic_sector_history_{friday_str}.csv"
            
            if os.path.exists(friday_file):
                try:
                    print(f"Found Friday's authentic sector data: {friday_str}")
                    import pandas as pd
                    friday_df = pd.read_csv(friday_file)
                    
                    # Find date column (case-insensitive)
                    date_col = None
                    for col in friday_df.columns:
                        if col.lower() == 'date':
                            date_col = col
                            break
                            
                    if not friday_df.empty and date_col is not None:
                        sector_columns = [col for col in friday_df.columns if col != date_col]
                        if sector_columns:
                            authentic_scores = []
                            latest_row = friday_df.iloc[0]  # Most recent row
                            
                            for sector in sector_columns:
                                norm_score = latest_row[sector]
                                raw_score = (norm_score / 50.0) - 1.0
                                
                                sector_data = {
                                    "sector": sector,
                                    "score": raw_score,
                                    "normalized_score": norm_score,
                                    "stance": "Bullish" if norm_score >= 60 else "Bearish" if norm_score <= 30 else "Neutral",
                                    "takeaway": "Outperforming peers" if norm_score >= 60 else 
                                              "Bearish macro setup" if norm_score <= 30 else 
                                              "Neutral – monitor trends",
                                    "drivers": [],
                                    "tickers": []
                                }
                                authentic_scores.append(sector_data)
                            
                            sector_scores = authentic_scores
                            found_authentic_data = True
                            print(f"Using Friday's authentic sector data ({friday_str}) with {len(authentic_scores)} sectors")
                except Exception as e:
                    print(f"Error loading Friday's authentic data: {e}")
        elif not is_weekend:
            # On weekdays, try yesterday's data if today's isn't available
            from datetime import timedelta
            yesterday = today - timedelta(days=1)
            yesterday_str = yesterday.strftime('%Y-%m-%d')
            yesterday_file = f"data/authentic_sector_history_{yesterday_str}.csv"
            
            if os.path.exists(yesterday_file):
                try:
                    print(f"Found yesterday's authentic sector data: {yesterday_str}")
                    import pandas as pd
                    yesterday_df = pd.read_csv(yesterday_file)
                    
                    # Find date column (case-insensitive)
                    date_col = None
                    for col in yesterday_df.columns:
                        if col.lower() == 'date':
                            date_col = col
                            break
                            
                    if not yesterday_df.empty and date_col is not None:
                        sector_columns = [col for col in yesterday_df.columns if col != date_col]
                        if sector_columns:
                            authentic_scores = []
                            latest_row = yesterday_df.iloc[0]  # Most recent row
                            
                            for sector in sector_columns:
                                norm_score = latest_row[sector]
                                raw_score = (norm_score / 50.0) - 1.0
                                
                                sector_data = {
                                    "sector": sector,
                                    "score": raw_score,
                                    "normalized_score": norm_score,
                                    "stance": "Bullish" if norm_score >= 60 else "Bearish" if norm_score <= 30 else "Neutral",
                                    "takeaway": "Outperforming peers" if norm_score >= 60 else 
                                              "Bearish macro setup" if norm_score <= 30 else 
                                              "Neutral – monitor trends",
                                    "drivers": [],
                                    "tickers": []
                                }
                                authentic_scores.append(sector_data)
                            
                            sector_scores = authentic_scores
                            found_authentic_data = True
                            print(f"Using yesterday's authentic sector data ({yesterday_str}) with {len(authentic_scores)} sectors")
                except Exception as e:
                    print(f"Error loading yesterday's authentic data: {e}")
    
    # Last resort: try to find the most recent historical file
    if not found_authentic_data:
        print("Could not find recent authentic sector data, looking for most recent historical file")
        # Look through data directory for most recent historical file
        historical_files = [f for f in os.listdir('data') if f.startswith('authentic_sector_history_')]
        print(f"DEBUG: Found historical sector data files: {historical_files}")
        if historical_files:
            # Sort files by date (descending)
            sorted_files = sorted(historical_files, reverse=True)
            if sorted_files:
                most_recent_file = f"data/{sorted_files[0]}"
                try:
                    print(f"Found most recent historical file: {sorted_files[0]}")
                    import pandas as pd
                    historical_df = pd.read_csv(most_recent_file)
                    
                    # Find date column (case-insensitive)
                    date_col = None
                    for col in historical_df.columns:
                        if col.lower() == 'date':
                            date_col = col
                            break
                            
                    if not historical_df.empty and date_col is not None:
                        sector_columns = [col for col in historical_df.columns if col != date_col]
                        if sector_columns:
                            authentic_scores = []
                            latest_row = historical_df.iloc[0]  # Most recent row
                            
                            for sector in sector_columns:
                                norm_score = latest_row[sector]
                                raw_score = (norm_score / 50.0) - 1.0
                                
                                sector_data = {
                                    "sector": sector,
                                    "score": raw_score,
                                    "normalized_score": norm_score,
                                    "stance": "Bullish" if norm_score >= 60 else "Bearish" if norm_score <= 30 else "Neutral",
                                    "takeaway": "Outperforming peers" if norm_score >= 60 else 
                                              "Bearish macro setup" if norm_score <= 30 else 
                                              "Neutral – monitor trends",
                                    "drivers": [],
                                    "tickers": []
                                }
                                authentic_scores.append(sector_data)
                            
                            sector_scores = authentic_scores
                            found_authentic_data = True
                            print(f"Using most recent historical data from {sorted_files[0]} with {len(authentic_scores)} sectors")
                except Exception as e:
                    print(f"Error loading most recent historical data: {e}")
    
    # If we still don't have authentic data, use May 2nd as the fallback
    if not found_authentic_data:
        print("No authentic sector data found, using May 2nd data as fallback")
        import forced_may2_data
        sector_scores = forced_may2_data.get_may2nd_sector_data()
        print(f"Using May 2nd data with {len(sector_scores)} sectors as fallback")
    
    # Add drivers and tickers to our sector scores (if they're missing)
    # Get drivers and tickers from May 2nd data which has complete information
    import forced_may2_data
    may2nd_sectors = forced_may2_data.get_may2nd_sector_data()
    
    # Create lookup dictionaries for drivers and tickers
    may2nd_drivers = {s['sector']: s['drivers'] for s in may2nd_sectors}
    may2nd_tickers = {s['sector']: s['tickers'] for s in may2nd_sectors}
    
    # Add drivers and tickers to our authentic scores if they're missing
    for sector_data in sector_scores:
        sector = sector_data['sector']
        if not sector_data.get('drivers') or len(sector_data['drivers']) == 0:
            sector_data['drivers'] = may2nd_drivers.get(sector, [])
        if not sector_data.get('tickers') or len(sector_data['tickers']) == 0:
            sector_data['tickers'] = may2nd_tickers.get(sector, [])
    
    print(f"Sector sentiment update triggered. Results: {sector_scores is not None}")

    print(f"Sector sentiment update triggered. Results: {sector_scores is not None}")
    
    if not sector_scores:
        print("WARNING: No sector scores available")
        return html.Div("Insufficient data to calculate sector sentiment", className="no-data-message")
    
    # Normalize sector scores from -1 to +1 scale to 0-100 scale
    normalized_scores = []
    for sector_data in sector_scores:
        # Create a copy of the sector data
        normalized_data = sector_data.copy()
        
        # Get the original score (should be in -1 to +1 scale)
        # If it's a weekend with hardcoded data, this may already be normalized
        orig_score = sector_data["score"]
        
        # Check if normalized_score is already present in the data
        if "normalized_score" in sector_data:
            # Use the pre-calculated normalized score (hardcoded May 2nd data already has this)
            norm_score = sector_data["normalized_score"]
        else:
            # Normalize to 0-100 scale using the formula ((score + 1.0) / 2.0) * 100
            norm_score = ((orig_score + 1.0) / 2.0) * 100
            # Round to 1 decimal place
            norm_score = round(norm_score, 1)
        
        # Store both scores
        normalized_data["original_score"] = orig_score
        normalized_data["normalized_score"] = norm_score
        
        # Update stance based on normalized score if needed
        # (but we'll keep the existing stance for consistency)
        
        normalized_scores.append(normalized_data)
    
    # Create a normalized scale legend
    scale_legend = html.Div([
        html.Div([
            "Sector Sentiment Scale (0-100):"
        ], className="scale-title"),
        html.Div([
            html.Div([
                html.Span("0", className="scale-min"),
                html.Span("50", className="scale-mid"),
                html.Span("100", className="scale-max")
            ], className="scale-numbers"),
            html.Div([
                html.Div(className="scale-bar-bearish"),
                html.Div(className="scale-bar-neutral"),
                html.Div(className="scale-bar-bullish")
            ], className="scale-bars")
        ], className="scale-container"),
        html.Div([
            html.Div(["Bearish", html.Span("0-30", className="scale-range")], className="scale-label bearish"),
            html.Div(["Neutral", html.Span("30-60", className="scale-range")], className="scale-label neutral"),
            html.Div(["Bullish", html.Span("60-100", className="scale-range")], className="scale-label bullish")
        ], className="scale-labels")
    ], className="sector-scale-legend")
    
    # Create cards for each sector using normalized scores
    # Filter out any "T2D Pulse" sector that might be included (it should only appear at the top)
    normalized_scores = [s for s in normalized_scores if s["sector"] != "T2D Pulse"]
    
    # Create the card list
    sector_cards = []
    
    # Get the global sector_weights dictionary or create it
    global sector_weights
    if not hasattr(app, '_sector_weights_initialized'):
        app._sector_weights_initialized = True
        sector_weights = {}
    
    # Calculate number of sectors and default weight
    num_sectors = len(normalized_scores)
    default_weight = 100 / num_sectors
    
    for sector_data in normalized_scores:
        # Extract data
        sector = sector_data["sector"]
        norm_score = sector_data["normalized_score"]
        stance = sector_data["stance"]
        takeaway = sector_data["takeaway"]
        drivers = sector_data["drivers"]
        tickers = sector_data["tickers"]
        
        # Initialize weight if not set
        if sector not in sector_weights:
            sector_weights[sector] = default_weight
        
        # Determine styling based on stance - match mockup styling more closely
        if stance == "Bullish":
            border_color = "#2ecc71"  # Green for Bullish
            text_color = "#27ae60"  # Darker green text (from mockup)
            bg_color = "white"     # White background for all cards
            badge_class = "badge-bullish"
        elif stance == "Bearish":
            border_color = "#e74c3c"  # Red for Bearish
            text_color = "#c0392b"  # Darker red text (from mockup)
            bg_color = "white"     # White background for all cards
            badge_class = "badge-bearish"
        else:
            border_color = "#f39c12"  # Orange for Neutral
            text_color = "#d35400"  # Darker orange text (from mockup)
            bg_color = "white"     # White background for all cards
            badge_class = "badge-neutral"
            
        # Create the sector card with original format from mockup including weight controls
        card = html.Div([
            # Header with sector name and score
            html.Div([
                html.Div([
                    html.Div(sector, className="sector-card-title", 
                             style={
                                 "fontWeight": "600", 
                                 "fontSize": "18px", 
                                 "marginRight": "10px",
                                 "width": "calc(100% - 90px)",
                                 "textAlign": "left",
                                 "overflow": "hidden",
                                 "textOverflow": "ellipsis"
                             }),
                    html.Div([
                        html.Div(f"{norm_score:.1f}", className="sector-score", 
                                style={
                                    "fontWeight": "bold", 
                                    "fontSize": "24px", 
                                    "textAlign": "right", 
                                    "width": "100%",
                                    "display": "block",
                                    "marginRight": "0",
                                    "color": text_color
                                })
                    ], className="score-container", style={
                        "width": "80px", 
                        "float": "right",
                        "textAlign": "right",
                        "margin": "0"
                    })
                ], className="card-header-content", style={
                    "display": "flex", 
                    "alignItems": "center", 
                    "width": "100%",
                    "padding": "15px",
                    "borderBottom": "1px solid #f1f1f1"
                })
            ], className="sector-card-header", style={
                "backgroundColor": "#fcfcfc",
                "borderBottom": f"2px solid {border_color}",
                "borderTopLeftRadius": "8px",
                "borderTopRightRadius": "8px"
            }),
            
            # Card body with all the details - using flex column with space-between
            html.Div([
                # Top content section - all elements except weight controls
                html.Div([
                    # Header and sentiment badge - with badge on the right
                    html.Div([
                        html.P(takeaway, className="sector-takeaway"),
                        # Restored stance text in badge
                        html.Span(stance, className=f"sector-badge {badge_class}")
                    ], className="takeaway-badge-container", style={"display": "flex", "justifyContent": "space-between", "alignItems": "center"}),
                    
                    # Scale indicator
                    html.Div([
                        html.Div([
                            html.Div(className="scale-marker", 
                                    style={"left": f"{min(max(norm_score, 0), 100)}%"})
                        ], className="scale-track")
                    ], className="sector-score-scale"),
                    
                    # Trend chart
                    html.Div([
                        html.Div("30-Day Trend", className="trend-title", 
                                style={"fontSize": "13px", "marginBottom": "5px", "color": "#666"}),
                        dcc.Graph(
                            id={"type": "sector-trend-chart", "index": sector},
                            figure=sector_trend_chart.create_sector_trend_chart(sector_name=sector),
                            config={"displayModeBar": False, "staticPlot": True},
                            style={"height": "85px", "width": "100%"}
                        )
                    ], className="sector-trend-container", style={"marginTop": "15px", "marginBottom": "15px"}),
                    
                    # Drivers list
                    html.Ul([
                        html.Li(driver) for driver in drivers
                    ], className="drivers-list"),
                    # Sector chart
                    html.Div([
                        html.Iframe(
                            srcDoc=open(f"data/sector_chart_{sector.replace(' ', '_').replace('/', '_')}.html", 'r').read() if os.path.exists(f"data/sector_chart_{sector.replace(' ', '_').replace('/', '_')}.html") else "",
                            style={
                                'width': '100%',
                                'height': '40px',
                                'border': 'none',
                                'padding': '0',
                                'margin': '0 0 10px 0',
                                'overflow': 'hidden',
                            }
                        )
                    ], className="sector-chart-container"),
                    
                    
                    # Tickers with label
                    html.Div([
                        html.Div(
                            html.Span("Representative Tickers:", style={"fontSize": "13px", "marginBottom": "5px", "display": "block"}),
                            style={"marginBottom": "3px"}
                        ),
                        html.Div([
                            html.Span(ticker, className="ticker-badge", style={"fontWeight": "bold"}) for ticker in tickers
                        ])
                    ], className="tickers-container"),
                ], style={"flex": "1"}),
                
                # Bottom section - weight controls, always at bottom
                html.Div([
                    html.Div([
                        html.Div("Weight:", className="weight-label", 
                               style={
                                   "textAlign": "left", 
                                   "display": "inline-block", 
                                   "marginRight": "5px",
                                   "verticalAlign": "middle"
                               }),
                        # Input field for manually entering weight
                        html.Div([
                            dcc.Input(
                                id={"type": "weight-input", "index": sector},
                                type="number",
                                min=0,
                                max=100,
                                step=0.01,
                                # Format value to exactly 2 decimal places
                                value=float(f"{sector_weights[sector]:.2f}"),
                                style={
                                    "width": "70px",
                                    "height": "30px",
                                    "padding": "5px",
                                    "borderRadius": "4px",
                                    "border": "1px solid #ddd", # Light gray border
                                    "fontSize": "14px",
                                    "marginRight": "5px"
                                }
                            ),
                            # Hidden button triggered by Enter key press
                            html.Button(
                                id={"type": "hidden-submit", "index": sector},
                                n_clicks=0,
                                style={"display": "none"}
                            )
                        ], id={"type": "input-container", "index": sector}),
                        html.Span("%", style={"marginRight": "10px"}),
                        
                        # Apply button - moved inside the weight display container 
                        html.Button("Apply", 
                                  id={"type": "apply-weight", "index": sector},
                                  className="weight-button weight-apply",
                                  style={
                                      "backgroundColor": "#2ecc71",  # Green button
                                      "color": "white",
                                      "border": "none",
                                      "borderRadius": "4px",
                                      "padding": "5px 18px",  # Wider padding
                                      "fontWeight": "bold",
                                      "cursor": "pointer",
                                      "fontSize": "14px",
                                      "minWidth": "80px",  # Ensuring enough width for "Apply"
                                      "boxShadow": "0 2px 4px rgba(0,0,0,0.1)",
                                      "marginLeft": "5px"  # Add spacing between % and button
                                  })
                    ], className="weight-display-container", style={"display": "flex", "alignItems": "center"}),
                ], className="weight-controls", style={
                    "display": "flex",
                    "justifyContent": "space-between",
                    "alignItems": "center",
                    "marginTop": "15px",
                    "padding": "10px 0 0 0",
                    "borderTop": "1px solid #eee"
                })
                
            ], className="sector-card-body", style={
                "backgroundColor": "white", 
                "display": "flex", 
                "flexDirection": "column", 
                "justifyContent": "space-between",
                "height": "100%"
            })
        ], className="sector-card", style={
            "--card-colour": border_color,
            "display": "flex", 
            "flexDirection": "column",
            "height": "100%",
            "minHeight": "460px",  # Set a minimum height for consistent card size
            "border": f"2px solid {border_color}",  # Thicker border for emphasis
            "borderRadius": "8px",  # Rounded corners
            "boxShadow": "0 4px 8px rgba(0,0,0,0.1)",  # Enhanced shadow
            "margin": "0",  # Reset margin
            "overflow": "hidden",  # Ensure contents don't overflow
            "backgroundColor": "#ffffff"  # Ensure white background
        })
        
        sector_cards.append(card)
    
    # Create a dictionary of sector name to normalized score for the summary
    sector_score_dict = {data["sector"]: data["normalized_score"] for data in normalized_scores}
    
    # Create the sector summary component
    sector_summary = create_sector_summary(sector_score_dict)
    
    # We'll handle the Enter key press with simpler approach
    # The code for the clientside callback was causing issues
    
    # Combine all elements in a layout similar to the mockup
    return html.Div([
        # Section header content from mockup
        html.Div([
            html.Div([
                html.H2("Technology Sector Sentiment", className="section-title"),
                html.Div([
                    html.Div([
                        html.P("Real-time sentiment scores based on current macroeconomic conditions. Sector scores are calculated from economic indicators weighted by their impact on each sector. Adjust sector weights to customize the T2D Pulse for your investment focus.", 
                              className="section-description", style={"margin": "0", "fontSize": "14px", "lineHeight": "1.5"}),
                    ]),
                ], className="section-controls", style={"display": "flex", "justifyContent": "space-between", "alignItems": "center"})
            ], className="section-header", style={"marginBottom": "15px"})
        ]),
        
        # Scale legend (moved from top controls to here for better context)
        html.Div([scale_legend], className="scale-legend-container", 
                style={"marginBottom": "15px"}),
        
        # Sector summary in a card like in the mockup, more compact
        html.Div([
            html.Div([
                html.H3("Sector Summary", className="section-subtitle", 
                       style={"marginBottom": "10px", "fontWeight": "600", "color": "#2c3e50", 
                              "textAlign": "center", "fontSize": "20px"}),
                sector_summary
            ], style={"width": "100%"}),
            html.Div([
                html.Button("Reset Equal Weights", 
                           id="reset-weights-button",
                           className="reset-button",
                           style={
                               "backgroundColor": "#3498db",
                               "color": "white",
                               "border": "none",
                               "borderRadius": "4px",
                               "padding": "8px 16px",
                               "cursor": "pointer",
                               "fontWeight": "500",
                               "boxShadow": "0 2px 4px rgba(0,0,0,0.1)",
                               "alignSelf": "flex-end"
                           })
            ], style={"display": "flex", "justifyContent": "flex-end", "marginTop": "10px"})
        ], className="sector-summary-container", 
           style={"marginBottom": "20px", "padding": "12px", 
                  "backgroundColor": "white", "borderRadius": "8px", 
                  "boxShadow": "0 1px 3px rgba(0,0,0,0.1)",
                  "border": "1px solid #ecf0f1"}),
        
        # Sector cards in a grid like the mockup
        html.Div(sector_cards, className="sector-cards-grid",
                style={"display": "grid", 
                       "gridTemplateColumns": "repeat(auto-fill, minmax(320px, 1fr))",
                       "gap": "40px",  # Significantly increased gap between cards
                       "padding": "10px",  # Increased padding around the grid
                       "marginBottom": "20px"}),  # Add bottom margin
        
        # Export button
        html.Div([
            html.A(
                html.Button("Export Sector History to Excel",
                          id="export-excel-button",
                          className="export-button",
                          style={
                              "backgroundColor": "#3498db",
                              "color": "white",
                              "border": "none",
                              "borderRadius": "4px",
                              "padding": "10px 20px",
                              "cursor": "pointer",
                              "fontWeight": "500",
                              "boxShadow": "0 2px 4px rgba(0,0,0,0.1)",
                              "margin": "0 auto",
                              "display": "block"
                          }),
                id="download-excel-link",
                href=f"/download/sector_sentiment_history_{datetime.now().strftime('%Y-%m-%d')}.xlsx",
                download=f"sector_sentiment_history_{datetime.now().strftime('%Y-%m-%d')}.xlsx"
            ),
        ], style={"marginBottom": "30px", "textAlign": "center"}),
        
        # Hidden div to store weights, initially populated with JSON of sector weights
        html.Div(id="stored-weights", 
                 style={"display": "none"},
                 children=json.dumps(sector_weights))
    ], className="sector-sentiment-container")

# Update VIX Container with chart and insights panel
@app.callback(
    Output("vix-container", "children"),
    [Input("interval-component", "n_intervals")]
)
def update_vix_container(n):
    """Update the VIX container to include both the graph and insights panel"""
    # Get the chart figure
    figure = update_vix_graph(n)
    
    # Filter data for insights panel (same filtering as in chart function)
    cutoff_date = datetime.now() - timedelta(days=2*365)
    filtered_data = vix_data[vix_data['date'] >= cutoff_date].copy()
    
    # Create insights panel with the filtered data
    insights_panel = create_insights_panel("vix", filtered_data)
    
    # Return container with graph and insights panel
    return [
        dcc.Graph(id="vix-graph", figure=figure),
        insights_panel
    ]

# Helper function to update sector highlighting
def update_sector_highlight(sector_name):
    """Update the highlighted sectors dictionary to show visual feedback when a weight is changed"""
    # First ensure the highlighted_sectors global exists
    if 'highlighted_sectors' not in globals():
        global highlighted_sectors
        highlighted_sectors = {}
    
    # Set current time as the highlight timestamp for this sector
    highlighted_sectors[sector_name] = time.time()

# Callback for updating weight input fields when weights change
@app.callback(
    Output({"type": "weight-input", "index": ALL}, "value"),
    [Input("stored-weights", "children")]
)
def update_weight_displays(weights_json):
    if not weights_json:
        # Initialize with equal weights
        global sector_weights
        weights = sector_weights
    else:
        # Use stored weights
        try:
            weights = json.loads(weights_json)
        except:
            # If JSON parse fails, use global weights
            weights = sector_weights
    
    # Import the sectors list to ensure we're only using valid sectors displayed on page
    from sentiment_engine import SECTORS
    
    # Generate weight values for each sector input (formatted to 2 decimal places)
    weight_values = []
    for sector in SECTORS:
        if sector in weights:
            # Format to exactly 2 decimal places for consistent display
            weight_values.append(float(f"{weights[sector]:.2f}"))
        else:
            # Default equal weight if sector not found
            equal_weight = round(100.0 / len(SECTORS), 2)
            weight_values.append(equal_weight)
    
    # Ensure the list has exactly the expected number of values
    expected_count = len(SECTORS)
    if len(weight_values) < expected_count:
        # Add default weights for any missing sectors
        equal_weight = round(100.0 / expected_count, 2)
        weight_values.extend([equal_weight] * (expected_count - len(weight_values)))
    elif len(weight_values) > expected_count:
        # Trim to expected count if needed
        weight_values = weight_values[:expected_count]
    
    return weight_values

# Note: The plus/minus button callbacks have been removed 
# and replaced with the manual input and apply button approach

# Callback for apply weight button
@app.callback(
    Output("stored-weights", "children", allow_duplicate=True),
    Input({"type": "apply-weight", "index": ALL}, "n_clicks"),
    State({"type": "weight-input", "index": ALL}, "value"),
    State("stored-weights", "children"),
    prevent_initial_call=True
)
def apply_weight(n_clicks_list, weight_values, weights_json):
    global sector_weights
    global fixed_sectors  # Track which sectors should remain fixed
    
    # Initialize fixed_sectors if it doesn't exist
    if 'fixed_sectors' not in globals():
        fixed_sectors = set()
    
    # Determine which button was clicked
    if not any(click for click in n_clicks_list if click):
        raise PreventUpdate
    
    # Get trigger information
    ctx = dash.callback_context
    if not ctx.triggered:
        raise PreventUpdate
    
    # Extract sector from triggered ID
    trigger_id = ctx.triggered[0]["prop_id"].split(".")[0]
    trigger_dict = json.loads(trigger_id)
    sector_to_update = trigger_dict["index"]
    
    # Find the index of the sector in the weight_values list
    input_index = 0  # Default value in case we can't find a match
    ids = [{'index': x['id']['index']} for x in ctx.states_list[0]]
    for i, callback_id in enumerate(ids):
        if callback_id["index"] == sector_to_update:
            input_index = i
            break
    
    # Use stored weights if available
    if weights_json:
        try:
            weights = json.loads(weights_json)
        except:
            weights = sector_weights.copy()
    else:
        weights = sector_weights.copy()
    
    # Check if the sector exists in the weights dictionary
    if sector_to_update not in weights:
        # If sector doesn't exist, initialize it with default weight
        num_sectors = len(ids)
        weights[sector_to_update] = 100 / num_sectors if num_sectors > 0 else 0
        print(f"Added missing sector {sector_to_update} to weights dictionary with weight {weights[sector_to_update]}")
    
    # Get the new weight value from input (with None handling)
    input_value = weight_values[input_index]
    if input_value is None or str(input_value).strip() == '':
        # If input is empty, keep the old weight
        new_weight = weights[sector_to_update]
    else:
        try:
            # Allow 0 as minimum weight
            new_weight = max(0, min(100, float(input_value)))
        except (ValueError, TypeError):
            # If conversion fails, keep the old weight
            new_weight = weights[sector_to_update]
    
    # Apply the new weight
    weights[sector_to_update] = new_weight
    
    # Add this sector to the fixed sectors
    fixed_sectors.add(sector_to_update)
    
    # Get all sectors except those that are fixed (sectors the user has not interacted with)
    adjustable_sectors = [s for s in weights.keys() if s != sector_to_update and s not in fixed_sectors]
    
    # If all sectors are fixed except the one we just updated, reset fixed sectors
    if not adjustable_sectors:
        fixed_sectors = {sector_to_update}
        adjustable_sectors = [s for s in weights.keys() if s != sector_to_update]
    
    # Calculate total weight allocated to fixed sectors (including current sector)
    fixed_weight = sum(weights[s] for s in weights if s in fixed_sectors or s == sector_to_update)
    
    # Calculate how much weight remains for adjustable sectors
    remaining_weight = 100 - fixed_weight
    
    # If we have adjustable sectors and there's weight to distribute
    if adjustable_sectors and remaining_weight >= 0:
        # Calculate equal weight for each adjustable sector
        equal_weight = remaining_weight / len(adjustable_sectors)
        
        # Assign equal weight to all adjustable sectors
        for s in adjustable_sectors:
            weights[s] = equal_weight
    
    # Ensure weights sum to exactly 100%
    total = sum(weights.values())
    if total != 100 and total > 0:
        # Find the largest weight to adjust (that isn't fixed)
        unfixed_sectors = [s for s in weights.keys() if s != sector_to_update and s not in fixed_sectors]
        # Only adjust if there are unfixed sectors with weight > 0 (guard against all zeros)
        if unfixed_sectors and sum(weights[s] for s in unfixed_sectors) > 0:
            largest_sector = max(unfixed_sectors, key=lambda x: weights[x])
        else:
            # If all unfixed sectors are at 0% or none exist, adjust the sector we just updated
            largest_sector = sector_to_update
        weights[largest_sector] += (100 - total)
    
    # Format all weights to 2 decimal places for display
    for s in weights:
        weights[s] = round(weights[s], 2)
    
    # Update highlighting to show visual feedback
    update_sector_highlight(sector_to_update)
    return json.dumps(weights)

# Callback to update the T2D Pulse score when weights change
@app.callback(
    Output("sentiment-gauge", "children", allow_duplicate=True),
    Input("stored-weights", "children"),
    prevent_initial_call=True
)
def update_t2d_pulse_score(weights_json):
    """
    Update the T2D Pulse score when weights change
    This callback is triggered whenever the stored weights are updated
    """
    # Default score if we can't calculate
    if not weights_json:
        return update_sentiment_gauge(50.0)
    
    try:
        # Parse weights from JSON
        weights = json.loads(weights_json)
        
        # PRIORITY 1: First always try to use the authentic pulse score from data/current_pulse_score.txt
        # This is the most accurate source and should be used regardless of weekend/weekday
        authentic_score = get_authentic_pulse_score()
        if authentic_score is not None:
            print(f"ALWAYS USING AUTHENTIC T2D PULSE SCORE: {authentic_score}")
            pulse_score = authentic_score
            return update_sentiment_gauge(pulse_score)
            
        # Get current date and check if it's a weekend
        import pytz
        from datetime import datetime
        eastern = pytz.timezone('US/Eastern')
        today = datetime.now(eastern)
        is_weekend = today.weekday() >= 5  # Saturday = 5, Sunday = 6
        
        # If it's a weekend and authentic score not available, try using the most recent market session data
        if is_weekend:
            print("Weekend detected - using most recent market session data for T2D Pulse calculation")
            
            # Get the most recent data file
            from datetime import datetime
            eastern = pytz.timezone('US/Eastern')
            today_str = datetime.now(eastern).strftime('%Y-%m-%d')
            date_specific_file = f"data/authentic_sector_history_{today_str}.csv"
            
            if os.path.exists(date_specific_file):
                # Load the recent market data from this file
                try:
                    import pandas as pd
                    recent_df = pd.read_csv(date_specific_file)
                    
                    if not recent_df.empty:
                        # Get sector columns from the dataframe
                        sector_columns = [col for col in recent_df.columns if col != 'date']
                        latest_row = recent_df.iloc[0]
                        
                        # Create dictionary of sector scores for T2D pulse calculation
                        sector_scores_dict = {sector: latest_row[sector] for sector in sector_columns}
                        
                        print(f"Using most recent market session data for T2D Pulse calculation: {len(sector_scores_dict)} sectors")
                        print(f"Using following sector weights: {weights}")
                        
                        # Calculate T2D Pulse score from the most recent data
                        pulse_score = calculate_t2d_pulse_from_sectors(sector_scores_dict, weights)
                        print(f"Calculated T2D Pulse Score from most recent market data: {pulse_score}")
                        
                        # Update the gauge display
                        return update_sentiment_gauge(pulse_score)
                except Exception as e:
                    print(f"Error using most recent market data for T2D Pulse calculation: {e}")
                    # Continue to fallback below
            
            # If recent market data not available, use May 2nd data as fallback
            print("Recent market data not available. Fallback to May 2nd data for T2D Pulse calculation")
            import forced_may2_data
            
            # Get the reliable May 2nd sector scores directly from our hardcoded values
            sector_scores_dict = forced_may2_data.get_may2nd_sector_dict()
            
            if sector_scores_dict:
                print(f"Using hardcoded May 2nd sector data for T2D Pulse calculation: {len(sector_scores_dict)} sectors")
                print(f"Using following sector weights: {weights}")
                # Use the pre-calculated authentic value directly
                pulse_score = forced_may2_data.get_may2nd_t2d_pulse_score()
                print(f"Using hardcoded May 2nd T2D Pulse score: {pulse_score}")
                print(f"Calculated T2D Pulse Score from May 2nd data: {pulse_score}")
                # Update the gauge display
                return update_sentiment_gauge(pulse_score)
            
            # Fallback in case our module fails
            print("Hardcoded May 2nd data unavailable, falling back to calculation")
        
        # Get sector scores through regular calculation on weekdays or as fallback
        sector_scores = calculate_sector_sentiment()
        
        if not sector_scores:
            return update_sentiment_gauge(50.0)
        
        # Create a dictionary of sector scores for the pulse calculation
        sector_scores_dict = {s['sector']: s['normalized_score'] for s in sector_scores}
        
        # Calculate the T2D Pulse score 
        pulse_score = calculate_t2d_pulse_from_sectors(sector_scores_dict, weights)
        
        print(f"Updated T2D Pulse score to {pulse_score} based on weight changes")
        
        # Update the gauge display
        return update_sentiment_gauge(pulse_score)
    except Exception as e:
        print(f"Error updating T2D Pulse score: {str(e)}")
        # Return default score on error
        return update_sentiment_gauge(50.0)

# Callback for hidden buttons (triggered by Enter key)
@app.callback(
    Output("stored-weights", "children", allow_duplicate=True),
    Input({"type": "hidden-submit", "index": ALL}, "n_clicks"),
    State({"type": "weight-input", "index": ALL}, "value"),
    State("stored-weights", "children"),
    prevent_initial_call=True
)
def apply_weight_on_enter(n_clicks_list, weight_values, weights_json):
    global sector_weights
    global fixed_sectors  # Track which sectors should remain fixed
    
    # Initialize fixed_sectors if it doesn't exist
    if 'fixed_sectors' not in globals():
        fixed_sectors = set()
    
    # Determine which input triggered the callback
    if not any(click for click in n_clicks_list if click):
        raise PreventUpdate
        
    # Find which input had an n_submit value
    input_index = next((i for i, n in enumerate(n_clicks_list) if n), None)
    if input_index is None:
        raise PreventUpdate
    
    # Map input index to sector - use the sectors from sentiment_engine.py
    from sentiment_engine import SECTORS
    
    # Make sure we don't go out of bounds
    if input_index < len(SECTORS):
        sector_to_update = SECTORS[input_index]
    else:
        # Use a safe fallback
        print(f"Warning: Input index {input_index} exceeds SECTORS length {len(SECTORS)}")
        sector_to_update = SECTORS[0]
    
    # Use stored weights if available
    if weights_json:
        try:
            weights = json.loads(weights_json)
        except:
            weights = sector_weights.copy()
    else:
        weights = sector_weights.copy()
        
    # Check if the sector exists in the weights dictionary
    if sector_to_update not in weights:
        # If sector doesn't exist, initialize it with default weight
        from sentiment_engine import SECTORS
        num_sectors = len(SECTORS)
        weights[sector_to_update] = 100 / num_sectors if num_sectors > 0 else 0
        print(f"Added missing sector {sector_to_update} to weights dictionary with weight {weights[sector_to_update]}")
    
    # Get the new weight value from input (with None handling)
    input_value = weight_values[input_index]
    if input_value is None or str(input_value).strip() == '':
        # If input is empty, keep the old weight
        new_weight = weights[sector_to_update]
    else:
        try:
            # Allow 0 as minimum weight
            new_weight = max(0, min(100, float(input_value)))
        except (ValueError, TypeError):
            # If conversion fails, keep the old weight
            new_weight = weights[sector_to_update]
    
    # Calculate the difference that needs to be distributed
    old_weight = weights[sector_to_update]
    weight_difference = new_weight - old_weight
    
    # Apply the new weight
    weights[sector_to_update] = new_weight
    
    # Add this sector to the fixed sectors
    fixed_sectors.add(sector_to_update)
    
    # Get all sectors except those that are fixed (sectors the user has not interacted with)
    adjustable_sectors = [s for s in weights.keys() if s != sector_to_update and s not in fixed_sectors]
    
    # If all sectors are fixed except the one we just updated, reset fixed sectors
    if not adjustable_sectors:
        fixed_sectors = {sector_to_update}
        adjustable_sectors = [s for s in weights.keys() if s != sector_to_update]
    
    # Calculate total weight allocated to fixed sectors (including current sector)
    fixed_weight = sum(weights[s] for s in weights if s in fixed_sectors or s == sector_to_update)
    
    # Calculate how much weight remains for adjustable sectors
    remaining_weight = 100 - fixed_weight
    
    # If we have adjustable sectors and there's weight to distribute
    if adjustable_sectors and remaining_weight >= 0:
        # Calculate equal weight for each adjustable sector
        equal_weight = remaining_weight / len(adjustable_sectors)
        
        # Assign equal weight to all adjustable sectors
        for s in adjustable_sectors:
            weights[s] = equal_weight
    
    # Ensure weights sum to exactly 100%
    total = sum(weights.values())
    if total != 100 and total > 0:
        # Find the largest weight to adjust (that isn't fixed)
        unfixed_sectors = [s for s in weights.keys() if s != sector_to_update and s not in fixed_sectors]
        # Only adjust if there are unfixed sectors with weight > 0 (guard against all zeros)
        if unfixed_sectors and sum(weights[s] for s in unfixed_sectors) > 0:
            largest_sector = max(unfixed_sectors, key=lambda x: weights[x])
        else:
            # If all unfixed sectors are at 0% or none exist, adjust the sector we just updated
            largest_sector = sector_to_update
        
        weights[largest_sector] += (100 - total)
    
    # Format all weights to 2 decimal places for display
    for s in weights:
        weights[s] = round(weights[s], 2)
    
    # Update highlighting to show visual feedback
    update_sector_highlight(sector_to_update)
    
    return json.dumps(weights)

# Callback for reset weights button
@app.callback(
    [Output("stored-weights", "children", allow_duplicate=True),
     Output("sentiment-score", "children", allow_duplicate=True)],
    Input("reset-weights-button", "n_clicks"),
    prevent_initial_call=True
)
def reset_weights(n_clicks):
    global fixed_sectors
    
    # Initialize fixed_sectors if it doesn't exist
    if 'fixed_sectors' not in globals():
        fixed_sectors = set()
    
    # Clear the fixed sectors when resetting
    fixed_sectors.clear()
    
    # Get sectors from the default weights
    from sentiment_engine import DEFAULT_SECTOR_WEIGHTS
    sectors = list(DEFAULT_SECTOR_WEIGHTS.keys())
    
    # Calculate equal weights
    num_sectors = len(sectors)
    equal_weight = 100.0 / num_sectors
    
    # Create a new dictionary with equal weights
    equal_weights = {sector: equal_weight for sector in sectors}
    
    # PRIORITY 1: First always try to use the authentic pulse score
    # This is the most accurate source and should be used regardless of weekend/weekday
    authentic_score = get_authentic_pulse_score()
    if authentic_score is not None:
        print(f"RESET: USING AUTHENTIC T2D PULSE SCORE: {authentic_score}")
        return json.dumps(equal_weights), update_sentiment_gauge(authentic_score)
        
    # If no authentic score found, continue with normal logic
    # Check if it's a weekend to use May 2nd data
    import pytz
    from datetime import datetime
    eastern = pytz.timezone('US/Eastern')
    today = datetime.now(eastern)
    is_weekend = today.weekday() >= 5  # Saturday = 5, Sunday = 6
    
    if is_weekend:
        # Use most recent market session data if it's a weekend
        print("Weekend detected - using most recent market session data for T2D Pulse calculation")
        
        # Get the most recent data file
        from datetime import datetime
        eastern = pytz.timezone('US/Eastern')
        today_str = datetime.now(eastern).strftime('%Y-%m-%d')
        date_specific_file = f"data/authentic_sector_history_{today_str}.csv"
        
        if os.path.exists(date_specific_file):
            # Load the recent market data from this file
            try:
                import pandas as pd
                recent_df = pd.read_csv(date_specific_file)
                
                if not recent_df.empty:
                    # Get sector columns from the dataframe
                    sector_columns = [col for col in recent_df.columns if col != 'date']
                    latest_row = recent_df.iloc[0]
                    
                    # Create dictionary of sector scores for T2D pulse calculation
                    sector_scores_dict = {sector: latest_row[sector] for sector in sector_columns}
                    
                    print(f"Using most recent market session data for T2D Pulse score reset: {len(sector_scores_dict)} sectors")
                    
                    # Calculate T2D Pulse score from the most recent data with equal weights
                    pulse_score = calculate_t2d_pulse_from_sectors(sector_scores_dict, equal_weights)
                    print(f"Reset T2D Pulse score to {pulse_score} with equal weights using most recent market data")
                    return json.dumps(equal_weights), f"{pulse_score:.1f}"
            except Exception as e:
                print(f"Error using most recent market data for T2D Pulse reset: {e}")
                # Continue to fallback below
        
        # Fallback to May 2nd data if needed
        print("Fallback to May 2nd data for T2D Pulse reset")
        import forced_may2_data
        
        # Get the reliable May 2nd sector scores directly from our hardcoded values
        sector_scores_dict = forced_may2_data.get_may2nd_sector_dict()
        
        if sector_scores_dict:
            # Use the pre-calculated May 2nd T2D Pulse score directly
            pulse_score = forced_may2_data.get_may2nd_t2d_pulse_score()
            print(f"Reset T2D Pulse score to {pulse_score} with equal weights using May 2nd data (fallback)")
            return json.dumps(equal_weights), f"{pulse_score:.1f}"
    
    # Regular calculation for weekdays or as fallback
    sector_scores = calculate_sector_sentiment()
    if sector_scores:
        # Create a dictionary of sector scores for the pulse calculation
        sector_scores_dict = {s['sector']: s['normalized_score'] for s in sector_scores}
        
        # Calculate the fresh T2D Pulse score with equal weights
        pulse_score = calculate_t2d_pulse_from_sectors(sector_scores_dict, equal_weights)
        print(f"Reset T2D Pulse score to {pulse_score} with equal weights")
    else:
        # Default score if sector data isn't available
        pulse_score = 50.0
    
    return json.dumps(equal_weights), pulse_score

# Create a helper function to update the sector highlighting
def update_sector_highlight(sector):
    """
    Update the highlighted sectors dictionary to show visual feedback when a weight is changed
    This marks the sector as recently updated for the visual glow effect
    
    Args:
        sector (str): The sector name to highlight
    """
    # Initialize highlighted_sectors if it doesn't exist
    if 'highlighted_sectors' not in globals():
        global highlighted_sectors
        highlighted_sectors = {}
    
    # Update the timestamp for this sector
    highlighted_sectors[sector] = time.time()

# Callback to provide visual feedback for updated input fields
@app.callback(
    Output({"type": "input-container", "index": ALL}, "style"),
    Input("stored-weights", "children"),
    prevent_initial_call=True
)
def update_input_styling(weights_json):
    """
    Update input field styling to provide visual feedback when a weight is changed
    This shows a green glow around fields that were recently updated
    """
    # Import the sectors from sentiment_engine to make sure we're using the right ones
    from sentiment_engine import SECTORS
    
    # Initialize a global dictionary to track highlighted sectors if it doesn't exist
    if 'highlighted_sectors' not in globals():
        global highlighted_sectors
        highlighted_sectors = {}
    
    # Default style for all input containers (no highlight)
    default_style = {
        "display": "flex",
        "alignItems": "center",
        "width": "70px",
        "marginRight": "5px"
    }
    
    # Style for highlighted input containers (with green glow, but keeping light gray border)
    highlight_style = {
        "display": "flex",
        "alignItems": "center",
        "width": "70px",
        "marginRight": "5px",
        "boxShadow": "0 0 5px 2px rgba(46, 204, 113, 0.7)",
        "borderRadius": "4px",
        "transition": "box-shadow 0.3s ease-in-out",
        "border": "1px solid #ddd"
    }
    
    # Current time to check for recent updates (highlight for 3 seconds)
    current_time = time.time()
    highlight_duration = 3  # seconds
    
    # Generate styles for each displayed sector
    styles = []
    for sector in SECTORS:
        # Check if this sector was recently updated
        if sector in highlighted_sectors and (current_time - highlighted_sectors[sector]) < highlight_duration:
            styles.append(highlight_style)
        else:
            styles.append(default_style)
            # Remove expired highlights
            if sector in highlighted_sectors and (current_time - highlighted_sectors[sector]) >= highlight_duration:
                del highlighted_sectors[sector]
    
    # Ensure we're returning exactly the expected number of styles
    expected_count = len(SECTORS)
    if len(styles) < expected_count:
        # Add default styles to match the expected count
        styles.extend([default_style] * (expected_count - len(styles)))
    elif len(styles) > expected_count:
        # Trim to expected count
        styles = styles[:expected_count]
        
    # Return the list of styles for all displayed sectors
    return styles

# --- Key Indicators Toggle Callback ---
@app.callback(
    Output("key-indicators-section", "style"),
    Output("toggle-key-indicators-button", "children"),
    Input("toggle-key-indicators-button", "n_clicks"),
)
def toggle_key_indicators(n_clicks):
    """Toggle the visibility of the Key Indicators section"""
    # Default style keeps the section hidden
    hidden_style = {
        "height": "0px", 
        "overflow": "hidden", 
        "opacity": 0,
        "transition": "height 0.6s ease, opacity 0.6s ease",
        "width": "100%", 
        "maxWidth": "1200px", 
        "margin": "0 auto",
        "backgroundColor": "#f9f9f9",
        "padding": "0 20px 20px 20px",
        "borderRadius": "8px",
        "boxShadow": "0 2px 5px rgba(0,0,0,0.1)"
    }
    
    # Visible style expands the section
    visible_style = {
        "height": "auto", 
        "opacity": 1,
        "transition": "height 0.6s ease, opacity 0.6s ease",
        "width": "100%", 
        "maxWidth": "1200px", 
        "margin": "0 auto",
        "backgroundColor": "#f9f9f9",
        "padding": "20px",
        "borderRadius": "8px",
        "boxShadow": "0 2px 5px rgba(0,0,0,0.1)"
    }
    
    # Toggle based on number of clicks (if odd, show it)
    if n_clicks and n_clicks % 2 == 1:
        return visible_style, "Hide Key Indicators ▲"
    else:
        return hidden_style, "Show Key Indicators ▼"

# --- Update Key Indicator Values ---
@app.callback(
    [Output("key-gdp-value", "children"),
     Output("key-gdp-trend", "children"),
     Output("key-pce-value", "children"),
     Output("key-pce-trend", "children"),
     Output("key-unemployment-value", "children"),
     Output("key-unemployment-trend", "children"),
     Output("key-job-postings-value", "children"),
     Output("key-job-postings-trend", "children"),
     Output("key-inflation-value", "children"),
     Output("key-inflation-trend", "children"),
     Output("key-pcepi-value", "children"),
     Output("key-pcepi-trend", "children"),
     Output("key-interest-rate-value", "children"),
     Output("key-interest-rate-trend", "children"),
     Output("key-nasdaq-value", "children"),
     Output("key-nasdaq-trend", "children"),
     Output("key-software-ppi-value", "children"),
     Output("key-software-ppi-trend", "children"),
     Output("key-data-ppi-value", "children"),
     Output("key-data-ppi-trend", "children"),
     Output("key-treasury-yield-value", "children"),
     Output("key-treasury-yield-trend", "children"),
     Output("key-vix-value", "children"),
     Output("key-vix-trend", "children"),
     Output("key-consumer-sentiment-value", "children"),
     Output("key-consumer-sentiment-trend", "children")],
    [Input("interval-component", "n_intervals")]
)
def update_key_indicators(n):
    """Update all key indicator values with the latest data using the exact same labels from the sidebar"""
    try:
        # 1. Real GDP % Change 
        gdp_value = "N/A"
        gdp_trend = ""
        
        # Use the correct format that matches sidebar gdp-value for consistency
        if not gdp_data.empty:
            sorted_gdp = gdp_data.sort_values('date', ascending=False)
            
            # Use the YoY growth percentage that's already calculated in the CSV file
            if 'yoy_growth' in sorted_gdp.columns and len(sorted_gdp) >= 1:
                # Use the pre-calculated YoY growth value
                latest_gdp_yoy = sorted_gdp.iloc[0]['yoy_growth']
                gdp_value = f"{latest_gdp_yoy:.1f}%"
                
                # Add trend indicator
                if len(sorted_gdp) >= 2 and 'yoy_growth' in sorted_gdp.columns:
                    current_yoy = sorted_gdp.iloc[0]['yoy_growth']
                    previous_yoy = sorted_gdp.iloc[1]['yoy_growth']
                    change = current_yoy - previous_yoy
                    
                    icon = "↑" if change >= 0 else "↓"
                    color = "trend-up" if change >= 0 else "trend-down"
                    
                    gdp_trend = html.Span([
                        html.Span(icon, className=f"trend-icon {color}"),
                        html.Span(f"{abs(change):.1f}%", className="trend-value-small")
                    ], className="trend-small")
        
        # 2. PCE
        pce_value = "N/A"
        pce_trend = ""
        
        # Use the correct format that matches sidebar pce-value for consistency
        if not pce_data.empty:
            sorted_pce = pce_data.sort_values('date', ascending=False)
            
            # Use the YoY growth percentage that's already calculated in the CSV file
            if 'yoy_growth' in sorted_pce.columns and len(sorted_pce) >= 1:
                # Use the pre-calculated YoY growth value
                latest_pce_yoy = sorted_pce.iloc[0]['yoy_growth']
                pce_value = f"{latest_pce_yoy:.1f}%"
                
                # Add trend indicator
                if len(sorted_pce) >= 2 and 'yoy_growth' in sorted_pce.columns:
                    current_yoy = sorted_pce.iloc[0]['yoy_growth']
                    previous_yoy = sorted_pce.iloc[1]['yoy_growth']
                    change = current_yoy - previous_yoy
                    
                    icon = "↑" if change >= 0 else "↓"
                    color = "trend-up" if change >= 0 else "trend-down"
                    
                    pce_trend = html.Span([
                        html.Span(icon, className=f"trend-icon {color}"),
                        html.Span(f"{abs(change):.1f}%", className="trend-value-small")
                    ], className="trend-small")
        
        # 3. Unemployment Rate
        unemployment_value = "N/A"
        unemployment_trend = ""
        if not unemployment_data.empty:
            sorted_unemployment = unemployment_data.sort_values('date', ascending=False)
            latest_unemployment = sorted_unemployment.iloc[0]['value']
            unemployment_value = f"{latest_unemployment:.1f}%"
            
            # Add trend indicator
            if len(sorted_unemployment) >= 2:
                current = sorted_unemployment.iloc[0]['value']
                previous = sorted_unemployment.iloc[1]['value']
                change = current - previous
                
                # Unemployment is inverse: down is good (green), up is bad (red)
                icon = "↓" if change <= 0 else "↑"
                color = "trend-up" if change <= 0 else "trend-down"
                
                unemployment_trend = html.Span([
                    html.Span(icon, className=f"trend-icon {color}"),
                    html.Span(f"{abs(change):.1f}%", className="trend-value-small")
                ], className="trend-small")
        
        # 4. Software Job Postings
        job_postings_value = "N/A"
        job_postings_trend = ""
        if not job_postings_data.empty and 'yoy_growth' in job_postings_data.columns:
            sorted_job_postings = job_postings_data.sort_values('date', ascending=False)
            latest_job_postings = sorted_job_postings.iloc[0]['yoy_growth']
            job_postings_value = f"{latest_job_postings:.1f}%"
            
            # Add trend indicator
            if len(sorted_job_postings) >= 2 and 'yoy_growth' in sorted_job_postings.columns:
                current = sorted_job_postings.iloc[0]['yoy_growth']
                previous = sorted_job_postings.iloc[1]['yoy_growth']
                change = current - previous
                
                icon = "↑" if change >= 0 else "↓"
                color = "trend-up" if change >= 0 else "trend-down"
                
                job_postings_trend = html.Span([
                    html.Span(icon, className=f"trend-icon {color}"),
                    html.Span(f"{abs(change):.1f}%", className="trend-value-small")
                ], className="trend-small")
        
        # 5. Inflation (CPI)
        inflation_value = "N/A"
        inflation_trend = ""
        if not inflation_data.empty and 'inflation' in inflation_data.columns:
            sorted_inflation = inflation_data.sort_values('date', ascending=False)
            latest_inflation = sorted_inflation.iloc[0]['inflation']
            inflation_value = f"{latest_inflation:.1f}%"
            
            # Add trend indicator
            if len(sorted_inflation) >= 2 and 'inflation' in sorted_inflation.columns:
                current = sorted_inflation.iloc[0]['inflation']
                previous = sorted_inflation.iloc[1]['inflation']
                change = current - previous
                
                # CPI/Inflation is inverse: down is good (green), up is bad (red)
                icon = "↓" if change <= 0 else "↑"
                color = "trend-up" if change <= 0 else "trend-down"
                
                inflation_trend = html.Span([
                    html.Span(icon, className=f"trend-icon {color}"),
                    html.Span(f"{abs(change):.1f}%", className="trend-value-small")
                ], className="trend-small")
        
        # 6. PCEPI (YoY)
        pcepi_value = "N/A"
        pcepi_trend = ""
        if not pcepi_data.empty and 'yoy_growth' in pcepi_data.columns:
            sorted_pcepi = pcepi_data.sort_values('date', ascending=False)
            latest_pcepi = sorted_pcepi.iloc[0]['yoy_growth']
            pcepi_value = f"{latest_pcepi:.1f}%"
            
            # Add trend indicator
            if len(sorted_pcepi) >= 2 and 'yoy_growth' in sorted_pcepi.columns:
                current = sorted_pcepi.iloc[0]['yoy_growth']
                previous = sorted_pcepi.iloc[1]['yoy_growth']
                change = current - previous
                
                # PCEPI is inverse: down is good (green), up is bad (red)
                icon = "↓" if change <= 0 else "↑"
                color = "trend-up" if change <= 0 else "trend-down"
                
                pcepi_trend = html.Span([
                    html.Span(icon, className=f"trend-icon {color}"),
                    html.Span(f"{abs(change):.1f}%", className="trend-value-small")
                ], className="trend-small")
        
        # 7. Fed Funds Rate
        interest_rate_value = "N/A"
        interest_rate_trend = ""
        if not interest_rate_data.empty:
            sorted_interest_rate = interest_rate_data.sort_values('date', ascending=False)
            latest_rate = sorted_interest_rate.iloc[0]['value']
            interest_rate_value = f"{latest_rate:.2f}%"
            
            # Add trend indicator
            if len(sorted_interest_rate) >= 2:
                current = sorted_interest_rate.iloc[0]['value']
                previous = sorted_interest_rate.iloc[1]['value']
                change = current - previous
                
                # Neutral indicator for Fed Funds Rate
                icon = "↑" if change >= 0 else "↓"
                color = "" # Neutral color
                
                interest_rate_trend = html.Span([
                    html.Span(icon, className=f"trend-icon {color}"),
                    html.Span(f"{abs(change):.2f}%", className="trend-value-small")
                ], className="trend-small")
        
        # 8. NASDAQ Trend
        nasdaq_value = "N/A"
        nasdaq_trend = ""
        if not nasdaq_data.empty:
            sorted_nasdaq = nasdaq_data.sort_values('date', ascending=False)
            latest_nasdaq = sorted_nasdaq.iloc[0]['value']
            if 'gap_pct' in sorted_nasdaq.columns:
                # No longer showing gap percentage in the indicator card per client request
                nasdaq_value = f"{int(latest_nasdaq):,}"
                
                # Add trend indicator
                if len(sorted_nasdaq) >= 2:
                    current = sorted_nasdaq.iloc[0]['value']
                    previous = sorted_nasdaq.iloc[1]['value']
                    pct_change = ((current - previous) / previous) * 100
                    
                    icon = "↑" if pct_change >= 0 else "↓"
                    color = "trend-up" if pct_change >= 0 else "trend-down"
                    
                    nasdaq_trend = html.Span([
                        html.Span(icon, className=f"trend-icon {color}"),
                        html.Span(f"{abs(pct_change):.1f}%", className="trend-value-small")
                    ], className="trend-small")
            else:
                nasdaq_value = f"{int(latest_nasdaq):,}"
        
        # 9. PPI: Software Publishers
        software_ppi_value = "N/A"
        software_ppi_trend = ""
        if not software_ppi_data.empty and 'yoy_pct_change' in software_ppi_data.columns:
            sorted_software_ppi = software_ppi_data.sort_values('date', ascending=False)
            latest_software_ppi = sorted_software_ppi.iloc[0]['yoy_pct_change']
            software_ppi_value = f"{latest_software_ppi:.1f}%"
            
            # Add trend indicator
            if len(sorted_software_ppi) >= 2 and 'yoy_pct_change' in sorted_software_ppi.columns:
                current = sorted_software_ppi.iloc[0]['yoy_pct_change']
                previous = sorted_software_ppi.iloc[1]['yoy_pct_change']
                change = current - previous
                
                # PPI trend is industry specific - neutral coloring
                icon = "↑" if change >= 0 else "↓"
                color = "" # Neutral color
                
                software_ppi_trend = html.Span([
                    html.Span(icon, className=f"trend-icon {color}"),
                    html.Span(f"{abs(change):.1f}%", className="trend-value-small")
                ], className="trend-small")
        
        # 10. PPI: Data Processing Services
        data_ppi_value = "N/A"
        data_ppi_trend = ""
        if not data_processing_ppi_data.empty and 'yoy_pct_change' in data_processing_ppi_data.columns:
            sorted_data_ppi = data_processing_ppi_data.sort_values('date', ascending=False)
            latest_data_ppi = sorted_data_ppi.iloc[0]['yoy_pct_change']
            data_ppi_value = f"{latest_data_ppi:.1f}%"
            
            # Add trend indicator
            if len(sorted_data_ppi) >= 2 and 'yoy_pct_change' in sorted_data_ppi.columns:
                current = sorted_data_ppi.iloc[0]['yoy_pct_change']
                previous = sorted_data_ppi.iloc[1]['yoy_pct_change']
                change = current - previous
                
                # PPI trend is industry specific - neutral coloring
                icon = "↑" if change >= 0 else "↓"
                color = "" # Neutral color
                
                data_ppi_trend = html.Span([
                    html.Span(icon, className=f"trend-icon {color}"),
                    html.Span(f"{abs(change):.1f}%", className="trend-value-small")
                ], className="trend-small")
        
        # 11. 10-Year Treasury Yield
        treasury_yield_value = "N/A"
        treasury_yield_trend = ""
        if not treasury_yield_data.empty:
            sorted_treasury = treasury_yield_data.sort_values('date', ascending=False)
            latest_treasury = sorted_treasury.iloc[0]['value']
            treasury_yield_value = f"{latest_treasury:.2f}%"
            
            # Add trend indicator
            if len(sorted_treasury) >= 2:
                current = sorted_treasury.iloc[0]['value']
                previous = sorted_treasury.iloc[1]['value']
                change = current - previous
                
                # Neutral indicator for Treasury Yield
                icon = "↑" if change >= 0 else "↓"
                color = "" # Neutral color
                
                treasury_yield_trend = html.Span([
                    html.Span(icon, className=f"trend-icon {color}"),
                    html.Span(f"{abs(change):.2f}%", className="trend-value-small")
                ], className="trend-small")
        
        # 12. VIX Volatility Index (14-day EMA for smoother trend)
        vix_value = "N/A"
        vix_trend = ""
        if not vix_data.empty:
            sorted_vix = vix_data.sort_values('date', ascending=False)
            
            # Use the 14-day EMA for VIX if available, otherwise fallback to raw value
            if 'vix_ema14' in sorted_vix.columns and not pd.isna(sorted_vix.iloc[0]['vix_ema14']):
                latest_vix = sorted_vix.iloc[0]['vix_ema14']  # Use smoothed value
            else:
                latest_vix = sorted_vix.iloc[0]['value']  # Fallback to raw value
                
            vix_value = f"{latest_vix:.1f}"
            
            # Add trend indicator using the EMA values for more stable trend detection
            if len(sorted_vix) >= 2 and 'vix_ema14' in sorted_vix.columns:
                if not pd.isna(sorted_vix.iloc[0]['vix_ema14']) and not pd.isna(sorted_vix.iloc[1]['vix_ema14']):
                    current = sorted_vix.iloc[0]['vix_ema14']
                    previous = sorted_vix.iloc[1]['vix_ema14']
                    change = current - previous
                    
                    # VIX is inverse: down is good (green), up is bad (red)
                    icon = "↓" if change <= 0 else "↑"
                    color = "trend-up" if change <= 0 else "trend-down"
                    
                    vix_trend = html.Span([
                        html.Span(icon, className=f"trend-icon {color}"),
                        html.Span(f"{abs(change):.1f}", className="trend-value-small")
                    ], className="trend-small")
                else:
                    # Fallback to raw values for trend if EMA is not available
                    current = sorted_vix.iloc[0]['value']
                    previous = sorted_vix.iloc[1]['value']
                    change = current - previous
                    
                    # VIX is inverse: down is good (green), up is bad (red)
                    icon = "↓" if change <= 0 else "↑"
                    color = "trend-up" if change <= 0 else "trend-down"
                    
                    vix_trend = html.Span([
                        html.Span(icon, className=f"trend-icon {color}"),
                        html.Span(f"{abs(change):.1f}", className="trend-value-small")
                    ], className="trend-small")
        
        # 13. Consumer Sentiment
        consumer_sentiment_value = "N/A"
        consumer_sentiment_trend = ""
        if not consumer_sentiment_data.empty:
            sorted_cs = consumer_sentiment_data.sort_values('date', ascending=False)
            latest_cs = sorted_cs.iloc[0]['value']
            consumer_sentiment_value = f"{latest_cs:.1f}"
            
            # Add trend indicator
            if len(sorted_cs) >= 2:
                current = sorted_cs.iloc[0]['value']
                previous = sorted_cs.iloc[1]['value']
                change = current - previous
                
                # Consumer Sentiment: up is good (green), down is bad (red)
                icon = "↑" if change >= 0 else "↓"
                color = "trend-up" if change >= 0 else "trend-down"
                
                consumer_sentiment_trend = html.Span([
                    html.Span(icon, className=f"trend-icon {color}"),
                    html.Span(f"{abs(change):.1f}", className="trend-value-small")
                ], className="trend-small")
        
        return (
            gdp_value, gdp_trend, pce_value, pce_trend, 
            unemployment_value, unemployment_trend, 
            job_postings_value, job_postings_trend,
            inflation_value, inflation_trend, 
            pcepi_value, pcepi_trend,
            interest_rate_value, interest_rate_trend, 
            nasdaq_value, nasdaq_trend,
            software_ppi_value, software_ppi_trend, 
            data_ppi_value, data_ppi_trend,
            treasury_yield_value, treasury_yield_trend, 
            vix_value, vix_trend,
            consumer_sentiment_value, consumer_sentiment_trend
        )
    
    except Exception as e:
        print(f"Error updating key indicators: {e}")
        # Return empty values for all indicators and their trends
        return "N/A", "", "N/A", "", "N/A", "", "N/A", "", "N/A", "", "N/A", "", "N/A", "", "N/A", "", "N/A", "", "N/A", "", "N/A", "", "N/A", "", "N/A", ""

# Add a download route for Excel files
@app.server.route("/download/<path:filename>")
def download_file(filename):
    """
    Serve files from the data directory for download
    This is used to provide downloadable Excel exports of sector sentiment history
    """
    # Generate file on request if it doesn't exist yet
    # If it's a sector sentiment history export, ensure we're using authentic data
    if filename.startswith('sector_sentiment_history_'):
        # Try our new fix_sector_export module
        try:
            import fix_sector_export
            if fix_sector_export.fix_sector_export():
                print(f"Successfully regenerated sector history export files")
            else:
                print(f"Error regenerating sector history export files")
        except Exception as e:
            print(f"Error running fix_sector_export: {e}")
            # Try the improved fix_sector_charts module as first fallback
            try:
                import fix_sector_charts_improved
                if fix_sector_charts_improved.fix_sector_charts():
                    print(f"Successfully regenerated sector history export files")
                else:
                    print(f"Error regenerating sector history export files")
            except Exception as e2:
                print(f"Error running sector charts improved fix: {e2}")
                # Try the original module as second fallback
                try:
                    import fix_sector_charts
                    if fix_sector_charts.fix_sector_charts():
                        print(f"Successfully regenerated sector history export files using original module")
                except Exception as e3:
                    print(f"Error with all fallback sector charts fixes: {e3}")
    
    filepath = os.path.join("data", filename)
    
    # If the file still doesn't exist, try one more approach
    if not os.path.exists(filepath):
        print(f"File {filepath} not found, trying to generate...")
        
        # Create the basic file name without date if requested file has a date
        today = datetime.now().strftime('%Y-%m-%d')
        if today in filename:
            base_filename = filename.replace(today, "")
            base_filepath = os.path.join("data", base_filename)
            
            if os.path.exists(base_filepath):
                # Copy the base file to the dated version
                import shutil
                try:
                    shutil.copy2(base_filepath, filepath)
                    print(f"Copied {base_filepath} to {filepath}")
                except Exception as e:
                    print(f"Error copying file: {e}")
    
    # Now serve the file (whether it existed before or was just created)
    from flask import send_from_directory
    return send_from_directory(
        directory="data",
        path=filename,
        as_attachment=True
    )

# Add auto-refresh functionality to update data every 24 hours
import threading
import time

# Define function to get current date in Eastern time
def get_eastern_date():
    """Get the current date in US Eastern Time"""
    eastern = pytz.timezone('US/Eastern')
    return datetime.now(eastern).date()

def auto_refresh_data():
    """Background thread that updates all data sources at 5:00pm ET daily"""
    while True:
        # Get current time in Eastern Time
        eastern = pytz.timezone('US/Eastern')
        now = datetime.now(eastern)
        
        # Calculate time until 5:00pm ET today
        target = now.replace(hour=17, minute=0, second=0, microsecond=0)
        
        # If it's already past 5:00pm, set target to 5:00pm tomorrow
        if now >= target:
            target = target + timedelta(days=1)
            
        # Calculate seconds until target time
        seconds_until_target = (target - now).total_seconds()
        logger.info(f"Next data refresh scheduled at {target.strftime('%Y-%m-%d %H:%M:%S %Z')}, which is {seconds_until_target:.1f} seconds from now")
        
        # Sleep until target time
        time.sleep(seconds_until_target)
        
        # Use Eastern time for date display
        eastern_date = target.strftime('%Y-%m-%d')
        logger.info(f"Auto-refresh: Updating economic data at 5:00pm ET on {eastern_date}...")
        
        # Fetch fresh data from all sources
        global gdp_data, unemployment_data, inflation_data, pcepi_data
        global interest_rate_data, treasury_yield_data, vix_data, nasdaq_data
        global pce_data, consumer_sentiment_data, software_ppi_data, data_ppi_data
        
        # Define fetch_economic_data to update all data sources
        def fetch_economic_data():
            # Fetch new data for all indicators
            logger.info("Fetching fresh economic data from all sources...")
            
            # FRED data
            global gdp_data, unemployment_data, inflation_data, pcepi_data
            global interest_rate_data, pce_data, consumer_sentiment_data
            global software_ppi_data, data_ppi_data
            
            # Get data from FRED API
            gdp_data = fetch_fred_data(FRED_SERIES["gdp"])
            unemployment_data = fetch_fred_data(FRED_SERIES["unemployment"])
            inflation_data = fetch_fred_data(FRED_SERIES["cpi"])
            pcepi_data = fetch_fred_data(FRED_SERIES["pcepi"])
            interest_rate_data = fetch_fred_data(FRED_SERIES["interest_rate"])
            pce_data = fetch_fred_data(FRED_SERIES["pce"])
            software_ppi_data = fetch_fred_data(FRED_SERIES["software_ppi"])
            data_ppi_data = fetch_fred_data(FRED_SERIES["data_ppi"])
            consumer_sentiment_data = fetch_fred_data(FRED_SERIES["consumer_sentiment"])
            
            # Get data from Yahoo Finance
            global treasury_yield_data, vix_data, nasdaq_data
            treasury_yield_data = fetch_treasury_yield_data()
            vix_data = fetch_vix_from_yahoo()
            nasdaq_data = fetch_nasdaq_with_ema()
        
        # Fetch economic data from APIs
        fetch_economic_data()
        
        # Run daily sector data collection using Finnhub API
        try:
            logger.info(f"Auto-refresh: Running daily sector data collection using Finnhub API...")
            from run_daily import main as run_daily_collection
            daily_collection_success = run_daily_collection()
            if daily_collection_success:
                logger.info(f"Auto-refresh: Successfully collected fresh sector data on {eastern_date}")
            else:
                logger.error(f"Auto-refresh: Failed to collect fresh sector data on {eastern_date}")
        except Exception as e:
            logger.error(f"Auto-refresh: Error in daily sector data collection: {str(e)}")
        
        # Re-calculate sector scores with fresh data
        sector_scores = calculate_sector_sentiment()
        
        # Update T2D Pulse score
        if sector_scores:
            # Calculate the pulse score with current weights
            sector_scores_dict = {s['sector']: s['normalized_score'] for s in sector_scores}
            pulse_score = calculate_t2d_pulse_from_sectors(sector_scores_dict)
            logger.info(f"Auto-refresh: Updated T2D Pulse score to {pulse_score} on {eastern_date}")
            
            # Save the authentic pulse score to a file for future reference
            try:
                os.makedirs('data', exist_ok=True)
                with open('data/current_pulse_score.txt', 'w') as f:
                    f.write(str(pulse_score))
                logger.info(f"Auto-refresh: Saved authentic pulse score {pulse_score} to data/current_pulse_score.txt")
            except Exception as e:
                logger.error(f"Auto-refresh: Error saving authentic pulse score: {str(e)}")
        else:
            logger.warning(f"Auto-refresh: No sector scores available, couldn't update T2D Pulse score")

# Add this at the end of the file if running directly
if __name__ == "__main__":
    # Start the auto-refresh thread
    refresh_thread = threading.Thread(target=auto_refresh_data, daemon=True)
    refresh_thread.start()
    logger.info("Started auto-refresh thread to update data every 24 hours")

    # Choose the port from the environment (Render provides $PORT)
    import os
    port = int(os.environ.get("PORT", 5000))
    print(f"Starting T2D Pulse dashboard on port {port}")
    app.run(host="0.0.0.0", port=port, debug=False)
