import os
import pandas as pd
import numpy as np
import requests
import base64
import io
import json
from datetime import datetime, timedelta
import dash
from dash import dcc, html, dash_table
from dash.dependencies import Input, Output, State
import plotly.graph_objs as go
import plotly.express as px
import yfinance as yf

# Import API keys from the separate file
from api_keys import FRED_API_KEY, BEA_API_KEY, BLS_API_KEY

# Import document analysis functionality
import document_analysis

# Import sector sentiment scoring
import sentiment_engine

# Import chart styling and market insights components
from chart_styling import custom_template, color_scheme
from market_insights import create_insights_panel

# Consumer sentiment functions defined directly in app.py to avoid circular imports

# Data directory
DATA_DIR = 'data'
os.makedirs(DATA_DIR, exist_ok=True)

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
    print(f"Fetching FRED data for series {series_id}")
    
    if not FRED_API_KEY:
        print("Cannot fetch FRED data: No API key provided")
        return pd.DataFrame()
    
    # Use today's date minus 5 days as a safety buffer to avoid potential future date errors
    # FRED API returns an error if realtime_start is after today's date (their server date)
    # The 5-day buffer helps account for any time zone differences or server clock variations
    today = datetime.now().date()
    safe_date = (today - timedelta(days=5)).strftime('%Y-%m-%d')
    
    # Default to last 5 years if no dates specified
    if not end_date:
        # Use today for most recent data
        end_date = today.strftime('%Y-%m-%d')
    if not start_date:
        # Calculate 5 years before the end date
        end_date_obj = datetime.strptime(end_date, '%Y-%m-%d') if isinstance(end_date, str) else end_date
        start_date = (end_date_obj - timedelta(days=5*365)).strftime('%Y-%m-%d')
    
    # Build API URL
    url = f"https://api.stlouisfed.org/fred/series/observations"
    
    # First try with current dates
    params = {
        "series_id": series_id,
        "api_key": FRED_API_KEY,
        "file_type": "json",
        "observation_start": start_date,
        "observation_end": end_date,
        "realtime_start": safe_date,
        "realtime_end": end_date
    }
    
    try:
        # Make first API request with current dates
        response = requests.get(url, params=params)
        
        if response.status_code == 200:
            data = response.json()
            
            # Convert to DataFrame
            df = pd.DataFrame(data['observations'])
            df['date'] = pd.to_datetime(df['date'])
            df['value'] = pd.to_numeric(df['value'], errors='coerce')
            
            # Handle missing values
            df = df.dropna(subset=['value'])
            
            print(f"Successfully retrieved {len(df)} observations for {series_id}")
            return df
        else:
            # If current dates fail, try again with a more conservative approach
            print(f"First FRED API attempt failed: {response.status_code} - {response.text}")
            print("Trying again with more conservative date parameters...")
            
            # Get date from 30 days ago to be extra safe
            safe_date = (today - timedelta(days=30)).strftime('%Y-%m-%d')
            
            # Update params with more conservative dates
            params.update({
                "observation_end": safe_date,
                "realtime_start": safe_date,
                "realtime_end": safe_date
            })
            
            try:
                # Make second API request with conservative dates
                response = requests.get(url, params=params)
                
                if response.status_code == 200:
                    data = response.json()
                    
                    # Convert to DataFrame
                    df = pd.DataFrame(data['observations'])
                    df['date'] = pd.to_datetime(df['date'])
                    df['value'] = pd.to_numeric(df['value'], errors='coerce')
                    
                    # Handle missing values
                    df = df.dropna(subset=['value'])
                    
                    print(f"Second attempt: Successfully retrieved {len(df)} observations for {series_id}")
                    return df
                else:
                    print(f"Second FRED API attempt also failed: {response.status_code} - {response.text}")
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
        file_path = os.path.join(DATA_DIR, filename)
        df.to_csv(file_path, index=False)
        print(f"Successfully saved {len(df)} rows to {filename}")
        return True
    except Exception as e:
        print(f"Failed to save data to {filename}: {str(e)}")
        return False

def load_data_from_csv(filename):
    """Load DataFrame from CSV file"""
    try:
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
            
            print(f"Successfully loaded {len(df)} rows from {filename}")
            return df
        else:
            print(f"File {filename} does not exist")
            return pd.DataFrame()
    except Exception as e:
        print(f"Failed to load data from {filename}: {str(e)}")
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
    Uses the opening price as the daily value rather than real-time intraday values.
    """
    print("Fetching Treasury Yield data from Yahoo Finance (as of market open)...")
    
    try:
        # Use Yahoo Finance to get the most recent data
        treasury = yf.Ticker('^TNX')
        # Get data for the last 60 days to ensure we have enough recent values
        # and smooth merging with historical data
        data = treasury.history(period='60d')
        
        if data.empty:
            print("No Treasury Yield data retrieved from Yahoo Finance")
            return pd.DataFrame()
            
        # Format data to match FRED format
        # Using Open prices instead of Close to get market open values
        df = pd.DataFrame({
            'date': data.index.tz_localize(None),  # Remove timezone to match FRED data
            'value': data['Open']
        })
        
        # Sort by date (newest first) for easier reporting and data merging
        df = df.sort_values('date', ascending=False)
        
        # Report the latest value and date
        latest_date = df.iloc[0]['date'].strftime('%Y-%m-%d')
        latest_value = df.iloc[0]['value']
        print(f"Treasury Yield (market open): {latest_value:.3f}% on {latest_date}")
        print(f"Successfully retrieved {len(df)} days of Treasury Yield data from Yahoo Finance")
        
        return df
    except Exception as e:
        print(f"Exception while fetching Treasury Yield data from Yahoo Finance: {str(e)}")
        print("Falling back to cached Treasury Yield data")
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
        print("Fetching NASDAQ data from Yahoo Finance with 20-day EMA...")
        
        # Get NASDAQ Composite data for the last 50 days (need extra days for EMA calculation)
        ixic = yf.Ticker("^IXIC")
        data = ixic.history(period="50d")  # Increased to ensure enough data for 20-day EMA
        
        if data.empty:
            print("No NASDAQ data retrieved from Yahoo Finance")
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
        print(f"NASDAQ: {latest_value:.1f} on {latest_date}, Gap from 20-day EMA: {latest_gap:.2f}%")
        print(f"Successfully retrieved NASDAQ data with EMA calculation")
        
        return df
    except Exception as e:
        print(f"Exception while fetching NASDAQ data with EMA: {str(e)}")
        print("Falling back to FRED data for NASDAQ")
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
            print(f"Successfully retrieved {len(df)} observations for Consumer Confidence Index")
            
            # Calculate year-over-year change
            df = df.sort_values('date')
            df['yoy_change'] = df['value'].pct_change(periods=12) * 100
            
            return df
        else:
            print("Error retrieving Consumer Confidence data from FRED")
            return pd.DataFrame()
    except Exception as e:
        print(f"Exception while fetching Consumer Confidence data: {str(e)}")
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
    print(f"Fetching BLS data for series {series_id}")
    
    if not BLS_API_KEY:
        print("Cannot fetch BLS data: No API key provided")
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
                
                print(f"Successfully retrieved BLS data with {len(df)} rows")
                return df
            else:
                print(f"BLS API request failed: {result['message']}")
                return pd.DataFrame()
        else:
            print(f"Failed to fetch BLS data: {response.status_code} - {response.text}")
            return pd.DataFrame()
    except Exception as e:
        print(f"Exception while fetching BLS data: {str(e)}")
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
        "VIX": "VIX {}",
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
    """Generate representative ticker symbols for each sector"""
    return {
        "SMB SaaS": ["BILL", "PAYC", "DDOG"],
        "Enterprise SaaS": ["CRM", "NOW", "ADBE"],
        "Cloud Infrastructure": ["AMZN", "MSFT", "GOOG"],
        "AdTech": ["TTD", "PUBM", "GOOGL"],
        "Fintech": ["SQ", "PYPL", "ADYEY"],
        "Consumer Internet": ["META", "GOOGL", "PINS"],
        "eCommerce": ["AMZN", "SHOP", "SE"],
        "Cybersecurity": ["PANW", "FTNT", "CRWD"],
        "Dev Tools / Analytics": ["SNOW", "DDOG", "ESTC"],
        "Semiconductors": ["NVDA", "AMD", "AVGO"],
        "AI Infrastructure": ["NVDA", "AMD", "SMCI"],
        "Vertical SaaS": ["VEEV", "TYL", "WDAY"],
        "IT Services / Legacy Tech": ["IBM", "ACN", "DXC"],
        "Hardware / Devices": ["AAPL", "DELL", "HPQ"]
    }

def calculate_sector_sentiment():
    """Calculate sentiment scores for each technology sector using the latest data"""
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
            print(f"Using smoothed VIX (14-day EMA): {latest_vix:.2f} vs raw: {latest_vix_row['value']:.2f}")
        else:
            latest_vix = latest_vix_row['value']  # Fallback to raw value
            print(f"Using raw VIX value: {latest_vix:.2f} (EMA not available)")
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
        print(f"Added Consumer Sentiment to sector calculations: {latest_consumer_sentiment}")
    
    # Check if we have enough data to calculate sector scores (need at least 6 indicators)
    if len(macros) < 6:
        print(f"Not enough data to calculate sector sentiment scores (have {len(macros)}/13 indicators)")
        return []
    
    try:
        # Calculate sector scores
        sector_scores = sentiment_engine.score_sectors(macros)
        print(f"Successfully calculated sentiment scores for {len(sector_scores)} sectors")
        
        # Get driver factors and tickers for each sector
        drivers = generate_sector_drivers(macros)
        tickers = generate_sector_tickers()
        
        # Enhance sector data with drivers, tickers, and stance
        enhanced_scores = []
        for sector_data in sector_scores:
            sector = sector_data["sector"]
            score = sector_data["score"]
            
            # Determine stance based on score (similar to the React component)
            if score <= -0.25:
                stance = "Bearish"
                takeaway = "Bearish macro setup"
            elif score >= 0.05:
                stance = "Bullish"
                takeaway = "Outperforming peers"
            else:
                stance = "Neutral"
                takeaway = "Neutral – monitor trends"
                
            # Add the enhanced data
            enhanced_scores.append({
                "sector": sector,
                "score": score,
                "stance": stance,
                "takeaway": takeaway,
                "drivers": drivers.get(sector, []),
                "tickers": tickers.get(sector, [])
            })
            
        return enhanced_scores
    except Exception as e:
        print(f"Error calculating sector sentiment scores: {str(e)}")
        return []

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
        print(f"WARNING: Default weights sum to {weights_sum}%, not 100%")
        # Adjust the largest weight to ensure total is exactly 100%
        adjustment = 100 - weights_sum
        default_weights['NASDAQ Trend'] += adjustment
        print(f"Adjusted NASDAQ Trend by {adjustment} to make total exactly 100%")
    
    # Validate default weights sum to 100
    assert abs(sum(default_weights.values()) - 100) < 0.1, "Default weights must sum to 100%"
    
    # Use custom weights if provided, otherwise use defaults
    weights = custom_weights if custom_weights else default_weights.copy()
    
    # Always start with a clean copy of weights
    working_weights = weights.copy() if weights else default_weights.copy()
    
    # Get document weight if available (limit to 0-50%)
    document_weight = 0
    if document_data and 'value' in document_data and 'weight' in document_data:
        document_weight = float(document_data['weight'])
        document_weight = max(0, min(50, document_weight))  # Enforce 0-50% range
    
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
        print(f"Weights before final adjustment: {working_weights}, Total: {final_total}")
        
        # Find the largest weight and adjust it to make the total exactly 100
        sorted_keys = sorted(working_weights.keys(), key=lambda k: working_weights[k], reverse=True)
        if sorted_keys:
            largest_key = sorted_keys[0]
            working_weights[largest_key] += (100 - final_total)
            print(f"Adjusted {largest_key} weight by {100 - final_total} to make total exactly 100%")
        
        # Verify after adjustment
        print(f"Weights after adjustment: {working_weights}, Total: {sum(working_weights.values())}")
    
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
gdp_data = load_data_from_csv('gdp_data.csv')
pce_data = load_data_from_csv('pce_data.csv')
unemployment_data = load_data_from_csv('unemployment_data.csv')
inflation_data = load_data_from_csv('inflation_data.csv')
interest_rate_data = load_data_from_csv('interest_rate_data.csv')
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

# Fetch interest rate data if needed
if interest_rate_data.empty or (datetime.now() - pd.to_datetime(interest_rate_data['date'].max())).days > 7:
    # Fetch Federal Funds Rate (FEDFUNDS)
    interest_rate_data = fetch_fred_data('FEDFUNDS')
    
    if not interest_rate_data.empty:
        # Save data
        save_data_to_csv(interest_rate_data, 'interest_rate_data.csv')
        
        print(f"Interest rate data updated with {len(interest_rate_data)} observations")
    else:
        print("Failed to fetch interest rate data")

# Fetch 10-Year Treasury yield data if needed
if treasury_yield_data.empty or (datetime.now() - pd.to_datetime(treasury_yield_data['date'].max())).days > 7:
    # Fetch 10-Year Treasury Constant Maturity Rate (DGS10)
    treasury_yield_data = fetch_fred_data('DGS10')
    
    if not treasury_yield_data.empty:
        # Save data
        save_data_to_csv(treasury_yield_data, 'treasury_yield_data.csv')
        
        print(f"Treasury yield data updated with {len(treasury_yield_data)} observations")
    else:
        print("Failed to fetch treasury yield data")

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
pcepi_data = load_data_from_csv('pcepi_data.csv')

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

# If no existing data or data is old, fetch new data
if vix_data.empty or (datetime.now() - pd.to_datetime(vix_data['date'].max())).days > 7:
    # Fetch CBOE Volatility Index (VIXCLS)
    vix_data = fetch_fred_data('VIXCLS')
    
    if not vix_data.empty:
        # Save data
        save_data_to_csv(vix_data, 'vix_data.csv')
        
        print(f"VIX data updated with {len(vix_data)} observations")
    else:
        print("Failed to fetch VIX data")

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
        print(f"VIX: {latest_vix:.2f} on {latest_date}, 14-day EMA: {latest_ema:.2f}")
    
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
        
        print(f"Consumer Sentiment data updated with {len(consumer_sentiment_data)} observations")
    else:
        print("Failed to fetch Consumer Sentiment data")

# Add Software Job Postings data
if job_postings_data.empty or (datetime.now() - pd.to_datetime(job_postings_data['date'].max() if not job_postings_data.empty else '2000-01-01')).days > 30:
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
        
        print(f"Software Job Postings data updated with {len(job_postings_data)} observations")
    else:
        print("Failed to fetch Software Job Postings data")

# Calculate initial sentiment index
sentiment_index = calculate_sentiment_index()

# ---- Dashboard Layout ----
app.layout = html.Div([
    # Hidden div for initialization callbacks
    html.Div(id="_", style={"display": "none"}),
    
    # Header
    html.Div([
        html.Div([
            html.Img(src="assets/images/t2d_logo.png", height="60px", className="logo"),
            html.Div([
                html.H1("T2D Pulse", className="dashboard-title"),
                html.P("Powering investment decisions with macro data and proprietary intelligence", 
                      className="dashboard-subtitle")
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
    
    # Main content container - Two column layout
    html.Div([
        # Left column - Contributing factors and key indicators
        html.Div([
            # Contributing Factors Card
            html.Div([
                html.H3("Contributing Factors", className="card-title"),
                html.Div(id="sentiment-components", className="factors-container")
            ], className="card factors-card"),
            
            # Key Indicators
            html.Div([
                html.H3("Key Indicators", className="card-title"),
                html.Div([
                    # Last updated timestamp
                    html.Div(id="last-updated", className="last-updated"),
                    
                    # Refresh button
                    html.Button("Refresh Data", id="refresh-data", className="refresh-button"),
                    
                    # Loading spinner for refresh operation
                    dcc.Loading(
                        id="loading-refresh",
                        type="circle",
                        children=html.Div(id="loading-refresh-output")
                    )
                ], className="refresh-container"),
                
                # GDP
                html.Div([
                    html.Div([
                        html.H4("Real GDP % Change"),
                        html.P(id="gdp-value", 
                              children=f"{gdp_data.sort_values('date', ascending=False).iloc[0]['yoy_growth']:.1f}%" 
                              if not gdp_data.empty and 'yoy_growth' in gdp_data.columns else "N/A",
                              className="indicator-value")
                    ], className="indicator-text"),
                    html.Div(id="gdp-trend", className="indicator-trend")
                ], className="indicator"),
                
                # PCE
                html.Div([
                    html.Div([
                        html.H4("PCE"),
                        html.P(id="pce-value", 
                              children=f"{pce_data.sort_values('date', ascending=False).iloc[0]['yoy_growth']:.1f}%" 
                              if not pce_data.empty and 'yoy_growth' in pce_data.columns else "N/A",
                              className="indicator-value")
                    ], className="indicator-text"),
                    html.Div(id="pce-trend", className="indicator-trend")
                ], className="indicator"),
                
                # Unemployment
                html.Div([
                    html.Div([
                        html.H4("Unemployment Rate"),
                        html.P(id="unemployment-value", 
                              children=f"{unemployment_data.sort_values('date', ascending=False).iloc[0]['value']:.1f}%" 
                              if not unemployment_data.empty else "N/A",
                              className="indicator-value")
                    ], className="indicator-text"),
                    html.Div(id="unemployment-trend", className="indicator-trend")
                ], className="indicator"),
                
                # Software Job Postings
                html.Div([
                    html.Div([
                        html.H4("Software Job Postings"),
                        html.P(id="job-postings-value", 
                              children=f"{job_postings_data.sort_values('date', ascending=False).iloc[0]['yoy_growth']:.1f}%" 
                              if not job_postings_data.empty and 'yoy_growth' in job_postings_data.columns else "N/A",
                              className="indicator-value")
                    ], className="indicator-text"),
                    html.Div(id="job-postings-trend", className="indicator-trend")
                ], className="indicator"),
                
                # Inflation
                html.Div([
                    html.Div([
                        html.H4("Inflation (CPI)"),
                        html.P(id="inflation-value", 
                              children=f"{inflation_data.sort_values('date', ascending=False).iloc[0]['inflation']:.1f}%" 
                              if not inflation_data.empty and 'inflation' in inflation_data.columns else "N/A",
                              className="indicator-value")
                    ], className="indicator-text"),
                    html.Div(id="inflation-trend", className="indicator-trend")
                ], className="indicator"),
                
                # PCEPI
                html.Div([
                    html.Div([
                        html.H4("PCEPI (YoY)"),
                        html.P(id="pcepi-value", 
                              children=f"{pcepi_data.sort_values('date', ascending=False).iloc[0]['yoy_growth']:.1f}%" 
                              if not pcepi_data.empty and 'yoy_growth' in pcepi_data.columns else "N/A",
                              className="indicator-value")
                    ], className="indicator-text"),
                    html.Div(id="pcepi-trend", className="indicator-trend")
                ], className="indicator"),
                
                # Interest Rate
                html.Div([
                    html.Div([
                        html.H4("Fed Funds Rate"),
                        html.P(id="interest-rate-value", 
                              children=f"{interest_rate_data.sort_values('date', ascending=False).iloc[0]['value']:.2f}%" 
                              if not interest_rate_data.empty else "N/A",
                              className="indicator-value")
                    ], className="indicator-text"),
                    html.Div(id="interest-rate-trend", className="indicator-trend")
                ], className="indicator"),
                
                # NASDAQ
                html.Div([
                    html.Div([
                        html.H4("NASDAQ Trend"),
                        html.P(id="nasdaq-value", 
                              children=html.Span([
                                  f"{nasdaq_data.sort_values('date', ascending=False).iloc[0]['value']:.0f}",
                                  html.Span(" (Gap from EMA: ", style={"fontSize": "12px", "color": "#666"}),
                                  html.Span(f"{nasdaq_data.sort_values('date', ascending=False).iloc[0]['gap_pct']:.1f}%" 
                                          if not nasdaq_data.empty and 'gap_pct' in nasdaq_data.columns else "N/A", 
                                          style={"fontSize": "12px", "fontWeight": "bold"}),
                                  html.Span(")", style={"fontSize": "12px", "color": "#666"})
                              ]) if not nasdaq_data.empty else "N/A",
                              className="indicator-value")
                    ], className="indicator-text"),
                    html.Div(id="nasdaq-trend", className="indicator-trend")
                ], className="indicator"),
                
                # PPI: Software Publishers
                html.Div([
                    html.Div([
                        html.H4("PPI: Software Publishers"),
                        html.P(id="software-ppi-value", 
                              children=f"{software_ppi_data.sort_values('date', ascending=False).iloc[0]['yoy_pct_change']:.1f}%" 
                              if not software_ppi_data.empty and 'yoy_pct_change' in software_ppi_data.columns else "N/A",
                              className="indicator-value")
                    ], className="indicator-text"),
                    html.Div(id="software-ppi-trend", className="indicator-trend")
                ], className="indicator"),
                
                # PPI: Data Processing Services
                html.Div([
                    html.Div([
                        html.H4("PPI: Data Processing Services"),
                        html.P(id="data-ppi-value", 
                              children=f"{data_processing_ppi_data.sort_values('date', ascending=False).iloc[0]['yoy_pct_change']:.1f}%" 
                              if not data_processing_ppi_data.empty and 'yoy_pct_change' in data_processing_ppi_data.columns else "N/A",
                              className="indicator-value")
                    ], className="indicator-text"),
                    html.Div(id="data-ppi-trend", className="indicator-trend")
                ], className="indicator"),
                
                # 10-Year Treasury Yield
                html.Div([
                    html.Div([
                        html.H4("10-Year Treasury Yield"),
                        html.P(id="treasury-yield-value", 
                              children=f"{treasury_yield_data.sort_values('date', ascending=False).iloc[0]['value']:.2f}%" 
                              if not treasury_yield_data.empty else "N/A",
                              className="indicator-value")
                    ], className="indicator-text"),
                    html.Div(id="treasury-yield-trend", className="indicator-trend")
                ], className="indicator"),
                
                # CBOE Volatility Index (VIX)
                html.Div([
                    html.Div([
                        html.H4("VIX Volatility Index"),
                        html.P(id="vix-value", 
                              children=f"{vix_data.sort_values('date', ascending=False).iloc[0]['value']:.2f}" 
                              if not vix_data.empty else "N/A",
                              className="indicator-value")
                    ], className="indicator-text"),
                    html.Div(id="vix-trend", className="indicator-trend")
                ], className="indicator"),
                
                # Consumer Sentiment
                html.Div([
                    html.Div([
                        html.H4("Consumer Sentiment"),
                        html.P(id="consumer-sentiment-value", 
                              children=f"{consumer_sentiment_data.sort_values('date', ascending=False).iloc[0]['value']:.1f}" 
                              if not consumer_sentiment_data.empty else "N/A",
                              className="indicator-value")
                    ], className="indicator-text"),
                    html.Div(id="consumer-sentiment-trend", className="indicator-trend")
                ], className="indicator"),
            ], className="card indicators-card"),
            
            # Custom Weight Adjustment
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
                        html.Label("VIX Volatility Index"),
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
            ], className="card weights-card"),
            
            # Document Analysis Card
            html.Div([
                html.H3("Document Sentiment Analysis", className="card-title"),
                
                # Document upload container
                html.Div([
                    html.P(
                        "Upload documents (PDF, DOCX, TXT) for sentiment analysis. "
                        "Earnings call transcripts, financial reports, and other text documents "
                        "will be analyzed for financial sentiment and incorporated into the index.",
                        className="tab-description"
                    ),
                    
                    # Document upload component
                    dcc.Upload(
                        id="upload-document",
                        children=html.Div([
                            "Drag and Drop or ",
                            html.A("Select Document", className="upload-link")
                        ]),
                        style={
                            'width': '100%',
                            'height': '60px',
                            'lineHeight': '60px',
                            'borderWidth': '1px',
                            'borderStyle': 'dashed',
                            'borderRadius': '5px',
                            'textAlign': 'center',
                            'margin': '10px 0px'
                        },
                        multiple=False
                    ),
                    
                    # Document analysis preview
                    html.Div(id="document-preview", className="document-preview"),
                    
                    # Document data weight
                    html.Div([
                        html.Label("Document Sentiment Weight", className="prop-label"),
                        dcc.Slider(
                            id="document-weight",
                            min=0,
                            max=50,
                            step=1,
                            value=0,
                            marks={
                                0: '0%',
                                10: '10%',
                                20: '20%',
                                30: '30%',
                                40: '40%',
                                50: '50%'
                            },
                            className="weight-slider"
                        ),
                        html.Div(id="document-weight-display", className="weight-display"),
                        html.Div(id="document-weight-debug", style={"fontSize": "10px", "color": "#999", "marginTop": "5px"})
                    ], className="slider-container"),
                    
                    # Apply button
                    html.Button("Apply Document Analysis", id="apply-document", className="apply-button"),
                ], className="upload-container")
            ], className="card upload-card"),
            
        ], className="column left-column"),
        
        # Right column - Graphs
        html.Div([
            # Tabs for different graph groups
            dcc.Tabs([
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
                        html.H3("CBOE Volatility Index (VIX)", className="graph-title"),
                        html.Div(id="vix-container", className="insights-enabled-container")
                    ], className="graph-container")
                ], className="custom-tab", selected_className="custom-tab--selected"),
                
                # Sector Sentiment Tab
                dcc.Tab(label="Sector Sentiment", children=[
                    html.Div([
                        html.H3("Technology Sector Sentiment", className="graph-title"),
                        html.P("Real-time sentiment scores based on current macroeconomic conditions", 
                               className="sector-subtitle"),
                        html.Div(id="sector-sentiment-container", className="sector-sentiment-container")
                    ], className="graph-container")
                ], className="custom-tab", selected_className="custom-tab--selected"),
            ], className="custom-tabs")
        ], className="column right-column")
    ], className="dashboard-content"),
    
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
    Output("last-updated", "children"),
    [Input("interval-component", "n_intervals")]
)
def update_last_updated(n):
    return f"Last updated: {datetime.now().strftime('%Y-%m-%d %H:%M')}"

# Update sentiment gauge
@app.callback(
    Output("sentiment-gauge", "children"),
    [Input("sentiment-score", "children")]
)
def update_sentiment_gauge(score):
    try:
        score_value = float(score)
    except (ValueError, TypeError):
        score_value = 0
    
    # Determine color based on score range using the exact HEX values provided
    if score_value >= 80:
        color = "#2ECC71"  # Boom (80-100) - HEX #2ECC71
        category = "Boom"
    elif score_value >= 60:
        color = "#F1C40F"  # Expansion (60-79) - HEX #F1C40F
        category = "Expansion"
    elif score_value >= 40:
        color = "#E67E22"  # Moderate Growth (40-59) - HEX #E67E22
        category = "Moderate Growth"
    elif score_value >= 20:
        color = "#E74C3C"  # Slowdown (20-39) - HEX #E74C3C
        category = "Slowdown"
    else:
        color = "#C0392B"  # Contraction (0-19) - HEX #C0392B
        category = "Contraction"
    
    # Fully centered content with fixed vertical centering
    return html.Div(
        # Container with vertical centering for all elements
        html.Div([
            # Title 
            html.H3("T2D Pulse Sentiment", 
                    style={
                        "fontSize": "22px", 
                        "fontWeight": "bold", 
                        "marginBottom": "15px", 
                        "textAlign": "center",
                        "color": "#333333"
                    }),
            # Score value
            html.Div([
                html.Span(f"{score_value:.1f}", 
                        style={
                            "fontSize": "54px", 
                            "fontWeight": "bold", 
                            "color": color
                        }),
            ], style={"textAlign": "center", "marginBottom": "10px"}),
            # Category and tooltip in a properly contained div
            html.Div([
                html.Span(category, 
                        style={
                            "fontSize": "22px", 
                            "color": color,
                            "marginRight": "5px",
                            "display": "inline-block"
                        }),
                html.Span(
                    "ⓘ", 
                    id="sentiment-info-icon",
                    className="info-icon",
                    style={
                        "cursor": "pointer", 
                        "fontSize": "16px", 
                        "display": "inline-block",
                        "color": "#2c3e50",
                        "verticalAlign": "text-top" 
                    }
                ),
                # Positioned tooltip that won't overflow the container
                html.Div(
                    id="sentiment-info-tooltip",
                    style={
                        "display": "none", 
                        "position": "absolute", 
                        "zIndex": "1000", 
                        "backgroundColor": "white", 
                        "padding": "10px", 
                        "borderRadius": "5px", 
                        "boxShadow": "0px 0px 10px rgba(0,0,0,0.1)", 
                        "maxWidth": "400px", 
                        "top": "30px",  # Positioned below the info icon
                        "left": "50%", 
                        "transform": "translateX(-50%)"
                    },
                    children=[
                        html.H5("Sentiment Index Categories", style={"marginBottom": "10px"}),
                        html.Div([
                            html.Div([
                                html.Span("Boom (80-100): ", style={"fontWeight": "bold", "color": "#2ECC71"}),
                                "Strong growth across indicators; economic expansion accelerating"
                            ], style={"marginBottom": "5px"}),
                            html.Div([
                                html.Span("Expansion (60-79): ", style={"fontWeight": "bold", "color": "#F1C40F"}),
                                "Solid growth with positive momentum; healthy economic conditions"
                            ], style={"marginBottom": "5px"}),
                            html.Div([
                                html.Span("Moderate Growth (40-59): ", style={"fontWeight": "bold", "color": "#E67E22"}),
                                "Steady but modest growth; economy performing adequately"
                            ], style={"marginBottom": "5px"}),
                            html.Div([
                                html.Span("Slowdown (20-39): ", style={"fontWeight": "bold", "color": "#E74C3C"}),
                                "Growth decelerating; potential economic challenges ahead"
                            ], style={"marginBottom": "5px"}),
                            html.Div([
                                html.Span("Contraction (0-19): ", style={"fontWeight": "bold", "color": "#C0392B"}),
                                "Economic indicators showing decline; recession risks elevated"
                            ])
                        ])
                    ]
                )
            ], style={
                "textAlign": "center", 
                "display": "flex", 
                "alignItems": "center", 
                "justifyContent": "center", 
                "position": "relative",
                "height": "30px"  # Fixed height to prevent layout shifts
            })
        ], style={
            "display": "flex",
            "flexDirection": "column",
            "justifyContent": "center",
            "alignItems": "center",
            "padding": "20px 0"
        }),
        # Add outer container styling with color-matched glow
        style={
            "display": "flex", 
            "alignItems": "center", 
            "justifyContent": "center",
            "height": "100%",
            "backgroundColor": "white",
            "borderRadius": "8px",
            "padding": "15px",
            "boxShadow": f"0 0 10px rgba({int(color[1:3], 16)}, {int(color[3:5], 16)}, {int(color[5:7], 16)}, 0.4)",  # Light color-matched glow
            "border": f"1px solid {color}",  # Color-matched border
            "transition": "all 0.3s ease"  # Smooth transition when color changes
        }
    )

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
        elif comp['indicator'] == 'VIX Volatility Index':
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
            bgcolor="rgba(255, 255, 255, 0.9)",
            bordercolor=arrow_color,
            borderwidth=1,
            borderpad=4,
            opacity=0.9
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
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1
        )
    )
    
    return fig

# Update GDP Container with chart and insights panel
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
        name='PCE Growth (YoY %)',
        line=dict(color=color_scheme["consumption"], width=3),
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
            bgcolor="rgba(255, 255, 255, 0.9)",
            bordercolor=arrow_color,
            borderwidth=1,
            borderpad=4,
            opacity=0.9
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
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1
        )
    )
    
    return fig

# Update PCE Container with chart and insights panel
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
    ))
    
    # Add reference lines for key thresholds
    x_range = [filtered_data['date'].min(), filtered_data['date'].max()]
    
    # Add +20% Growth threshold line
    fig.add_trace(go.Scatter(
        x=x_range,
        y=[20, 20],
        mode='lines',
        line=dict(color='green', width=1, dash='dash'),
        name='Hiring Boom (20%)'
    ))
    
    # Add +5% Growth threshold line
    fig.add_trace(go.Scatter(
        x=x_range,
        y=[5, 5],
        mode='lines',
        line=dict(color='lightgreen', width=1, dash='dash'),
        name='Healthy Recovery (5%)'
    ))
    
    # Add 0% Growth threshold line
    fig.add_trace(go.Scatter(
        x=x_range,
        y=[0, 0],
        mode='lines',
        line=dict(color='gray', width=1),
        name='Neutral'
    ))
    
    # Add -5% Growth threshold line
    fig.add_trace(go.Scatter(
        x=x_range,
        y=[-5, -5],
        mode='lines',
        line=dict(color='orange', width=1, dash='dash'),
        name='Slowdown (-5%)'
    ))
    
    # Add -20% Growth threshold line
    fig.add_trace(go.Scatter(
        x=x_range,
        y=[-20, -20],
        mode='lines',
        line=dict(color='red', width=1, dash='dash'),
        name='Hiring Recession (-20%)'
    ))
    
    # Add current value annotation
    current_value = filtered_data['yoy_growth'].iloc[-1]
    previous_value = filtered_data['yoy_growth'].iloc[-2]
    change = current_value - previous_value
    
    # Using absolute value change (not percentage)
    arrow_color = 'green' if change > 0 else 'red'
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

# Update Job Postings Container with chart and insights
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
    
    # Filter for last 5 years
    cutoff_date = datetime.now() - timedelta(days=5*365)
    filtered_data = interest_rate_data[interest_rate_data['date'] >= cutoff_date].copy()
    
    # Create figure
    fig = go.Figure()
    
    # Add Federal Funds Rate line with consistent color scheme
    fig.add_trace(go.Scatter(
        x=filtered_data['date'],
        y=filtered_data['value'],
        mode='lines',
        name='Federal Funds Rate',
        line=dict(color=color_scheme["rates"], width=3),
    ))
    
    # Add current value annotation
    current_value = filtered_data['value'].iloc[-1]
    previous_value = filtered_data['value'].iloc[-2]
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
    
    # Update layout with custom template
    fig.update_layout(
        template=custom_template,
        height=400,
        title=None,
        margin=dict(l=40, r=40, t=40, b=40),
        xaxis_title="",
        yaxis_title="Rate (%)",
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
    
    # Create figure
    fig = go.Figure()
    
    # Add treasury yield line with consistent color scheme
    fig.add_trace(go.Scatter(
        x=filtered_data['date'],
        y=filtered_data['value'],
        mode='lines',
        name='10-Year Treasury Yield',
        line=dict(color=color_scheme["rates"], width=2.5),
        hovertemplate="<b>%{x|%b %Y}</b><br>%{y:.2f}%<extra></extra>"
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
    current_value = filtered_data['value'].iloc[-1]
    previous_value = filtered_data['value'].iloc[-2]
    change = current_value - previous_value
    
    # Using absolute value change (not percentage) to match key indicators
    arrow_color = color_scheme["positive"] if change > 0 else color_scheme["negative"]
    arrow_symbol = "▲" if change > 0 else "▼"
    
    current_value_annotation = f"Current: {current_value:.2f}% {arrow_symbol} {abs(change):.2f}%"
    
    fig.add_annotation(
        x=0.02,
        y=0.95,  # Lowered position to avoid overlap with title
        xref="paper",
        yref="paper",
        text=current_value_annotation,
        showarrow=False,
        font=dict(size=14, color=arrow_color),
        align="left",
        bgcolor="rgba(255, 255, 255, 0.9)",  # Increased opacity for better visibility
        bordercolor=arrow_color,
        borderwidth=1,
        borderpad=4,
        opacity=0.9
    )
    
    # Update layout with custom template
    fig.update_layout(
        template=custom_template,
        height=400,
        title=None,  # Removed title since we already have it in the HTML
        xaxis_title="",
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

# Refresh data
@app.callback(
    [Output("loading-refresh-output", "children"),
     Output("gdp-value", "children"),
     Output("unemployment-value", "children"),
     Output("inflation-value", "children"),
     Output("interest-rate-value", "children")],
    [Input("refresh-data", "n_clicks")]
)
def refresh_data(n_clicks):
    # Define variables as global at the top of the function
    global gdp_data, unemployment_data, inflation_data, interest_rate_data
    global nasdaq_data, software_ppi_data, data_processing_ppi_data, pcepi_data, job_postings_data
    global consumer_sentiment_data, vix_data
    
    if n_clicks is None:
        # Initial load
        return (
            "",
            f"{gdp_data.sort_values('date', ascending=False).iloc[0]['yoy_growth']:.1f}%" if not gdp_data.empty and 'yoy_growth' in gdp_data.columns else "N/A",
            f"{unemployment_data.sort_values('date', ascending=False).iloc[0]['value']:.1f}%" if not unemployment_data.empty else "N/A",
            f"{inflation_data.sort_values('date', ascending=False).iloc[0]['inflation']:.1f}%" if not inflation_data.empty and 'inflation' in inflation_data.columns else "N/A",
            f"{interest_rate_data.sort_values('date', ascending=False).iloc[0]['value']:.2f}%" if not interest_rate_data.empty else "N/A"
        )
    
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
    # Always use the current custom weights and document data
    # This will respond to changes in the custom-weights-store
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
    
    # Add current value annotations
    current_value = filtered_data['value'].iloc[-1]
    previous_value = filtered_data['value'].iloc[-2]
    change = current_value - previous_value
    
    # For VIX, up is negative (fear) and down is positive (calm)
    arrow_color = color_scheme["negative"] if change > 0 else color_scheme["positive"]
    arrow_symbol = "▲" if change > 0 else "▼"
    
    # Show both raw VIX and smoothed EMA value
    if 'vix_ema14' in filtered_data.columns:
        current_ema = filtered_data['vix_ema14'].iloc[-1]
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
    # Calculate sector sentiment scores
    sector_scores = calculate_sector_sentiment()
    
    if not sector_scores:
        return html.Div("Insufficient data to calculate sector sentiment", className="no-data-message")
    
    # Create a scale legend
    scale_legend = html.Div([
        html.Div([
            "Sector Sentiment Scale:",
            html.Span(" (Sector-specific scores derived from macro factors, separate from the T2D Pulse Sentiment Index)", 
                      className="scale-title-note")
        ], className="scale-title"),
        html.Div([
            html.Div([
                html.Span("-0.5", className="scale-min"),
                html.Span("0", className="scale-mid"),
                html.Span("+0.5", className="scale-max")
            ], className="scale-numbers"),
            html.Div([
                html.Div(className="scale-bar-bearish"),
                html.Div(className="scale-bar-neutral"),
                html.Div(className="scale-bar-bullish")
            ], className="scale-bars")
        ], className="scale-container"),
        html.Div([
            html.Div(["Bearish", html.Span("< -0.25", className="scale-range")], className="scale-label bearish"),
            html.Div(["Neutral", html.Span("-0.25 to +0.05", className="scale-range")], className="scale-label neutral"),
            html.Div(["Bullish", html.Span("> +0.05", className="scale-range")], className="scale-label bullish")
        ], className="scale-labels")
    ], className="sector-scale-legend")
    
    # Create cards for each sector
    sector_cards = []
    
    for sector_data in sector_scores:
        # Extract data
        sector = sector_data["sector"]
        score = sector_data["score"]
        stance = sector_data["stance"]
        takeaway = sector_data["takeaway"]
        drivers = sector_data["drivers"]
        tickers = sector_data["tickers"]
        
        # Determine score and badge styling based on stance
        if stance == "Bullish":
            score_class = "score-positive"
            badge_class = "badge-bullish"
        elif stance == "Bearish":
            score_class = "score-negative"
            badge_class = "badge-bearish"
        else:
            score_class = "score-neutral"
            badge_class = "badge-neutral"
        
        # Create the sector card
        card = html.Div([
            # Header with sector name and score
            html.Div([
                html.Span(sector, className="sector-name"),
                html.Span(f"{score:+.2f}" if score > 0 else f"{score:.2f}", 
                          className=f"sector-score {score_class}")
            ], className="sector-card-header"),
            
            # Stance badge
            html.Span(stance, className=f"sector-badge {badge_class}"),
            
            # Takeaway text
            html.P(takeaway, className="sector-takeaway"),
            
            # Drivers list
            html.Ul([
                html.Li(driver) for driver in drivers
            ], className="drivers-list"),
            
            # Tickers
            html.Div([
                html.Span(ticker, className="ticker-badge") for ticker in tickers
            ], className="tickers-container")
            
        ], className="sector-card")
        
        sector_cards.append(card)
    
    # Combine the scale legend with the sector cards in a single container
    return html.Div([
        scale_legend,  # Legend at the top
        html.Div(sector_cards, className="sector-cards-container")  # Grid of cards below
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

# Add this at the end of the file if running directly
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=False)
