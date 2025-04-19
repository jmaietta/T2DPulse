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

# Import API keys from the separate file
from api_keys import FRED_API_KEY, BEA_API_KEY, BLS_API_KEY

# Import document analysis functionality
import document_analysis

# Data directory
DATA_DIR = 'data'
os.makedirs(DATA_DIR, exist_ok=True)

# Initialize the Dash app with external stylesheets
app = dash.Dash(
    __name__,
    meta_tags=[
        {"name": "viewport", "content": "width=device-width, initial-scale=1.0"}
    ]
)

# Set the server for production deployment
server = app.server

# Set page title
app.title = "Economic Dashboard: Software & Technology"

# Function to fetch data from FRED
def fetch_fred_data(series_id, start_date=None, end_date=None):
    """Fetch data from FRED API for a given series"""
    print(f"Fetching FRED data for series {series_id}")
    
    if not FRED_API_KEY:
        print("Cannot fetch FRED data: No API key provided")
        return pd.DataFrame()
    
    # Default to last 5 years if no dates specified
    if not end_date:
        end_date = datetime.now().strftime('%Y-%m-%d')
    if not start_date:
        start_date = (datetime.now() - timedelta(days=5*365)).strftime('%Y-%m-%d')
    
    # Build API URL
    url = f"https://api.stlouisfed.org/fred/series/observations"
    params = {
        "series_id": series_id,
        "api_key": FRED_API_KEY,
        "file_type": "json",
        "observation_start": start_date,
        "observation_end": end_date
    }
    
    try:
        # Make API request
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
            print(f"Failed to fetch data: {response.status_code} - {response.text}")
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
            if 'date' in df.columns:
                df['date'] = pd.to_datetime(df['date'])
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

def calculate_sentiment_index(custom_weights=None, proprietary_data=None, document_data=None):
    """Calculate economic sentiment index from available indicators
    
    Args:
        custom_weights (dict, optional): Dictionary with custom weights for each indicator
        proprietary_data (dict, optional): Dictionary with proprietary data and its weight
        document_data (dict, optional): Dictionary with document analysis data and its weight
    """
    # Default weights (equal percentages that sum to 100%)
    default_weights = {
        'GDP % Change': 14,
        'Unemployment Rate': 14,
        'CPI': 14,
        'NASDAQ Trend': 14,
        'PPI: Data Processing Services': 14,
        'PPI: Software Publishers': 15,
        'Federal Funds Rate': 15
    }
    
    # Validate default weights sum to 100
    assert abs(sum(default_weights.values()) - 100) < 0.1, "Default weights must sum to 100%"
    
    # Use custom weights if provided, otherwise use defaults
    weights = custom_weights if custom_weights else default_weights.copy()
    
    # If this is our initial calculation with no document, ensure economic indicators sum to 100
    if not document_data:
        economic_indicators_sum = sum(weights.values())
        if abs(economic_indicators_sum - 100) > 0.1:
            print(f"Adjusting economic indicators from {economic_indicators_sum}% to 100%")
            scaling_factor = 100 / economic_indicators_sum
            for key in weights:
                weights[key] = weights[key] * scaling_factor
    
    # Get document weight if available (limit to 0-50%)
    document_weight = 0
    if document_data and 'value' in document_data and 'weight' in document_data:
        document_weight = float(document_data['weight'])
        document_weight = max(0, min(50, document_weight))  # Enforce 0-50% range
        
    # Get proprietary data weight if available (for legacy support)
    proprietary_weight = 0
    if proprietary_data and 'value' in proprietary_data and 'weight' in proprietary_data:
        proprietary_weight = float(proprietary_data['weight'])
        proprietary_weight = 0  # Set to 0 since we're removing this feature
        
    # The document weight plus all economic indicators must sum to 100%
    extra_weights = document_weight + proprietary_weight
    
    # Normalize weights to ensure they sum to 100%
    if extra_weights > 0:
        # First calculate what the total weight of economic indicators should be
        economic_indicator_total_weight = 100 - extra_weights
        current_total_weight = sum(weights.values())
        
        # Only scale if the current total is different from what we need
        if abs(current_total_weight - economic_indicator_total_weight) > 0.001:
            scaling_factor = economic_indicator_total_weight / current_total_weight
            
            # Scale economic indicator weights proportionally
            for key in weights:
                weights[key] = round(weights[key] * scaling_factor, 1)
        
        # Add proprietary data weight if available
        if proprietary_weight > 0:
            weights['Proprietary Data'] = proprietary_weight
            
        # Add document sentiment weight if available
        if document_weight > 0:
            weights['Document Sentiment'] = document_weight
            
        # Final check - make sure the sum is exactly 100 after rounding
        final_total = sum(weights.values())
        if abs(final_total - 100) > 0.001:
            print(f"Weights before adjustment: {weights}, Total: {final_total}")
            
            # Find the largest weight and adjust it to make the total exactly 100
            # Sort keys by weight value to find the largest
            sorted_keys = sorted(weights.keys(), key=lambda k: weights[k], reverse=True)
            if sorted_keys:
                largest_key = sorted_keys[0]
                weights[largest_key] += (100 - final_total)
                print(f"Adjusted {largest_key} weight by {100 - final_total} to make total exactly 100%")
            
            # Verify after adjustment
            print(f"Weights after adjustment: {weights}, Total: {sum(weights.values())}")
    
    sentiment_components = []
    
    # 1. GDP Growth - positive growth is good
    if not gdp_data.empty and 'yoy_growth' in gdp_data.columns:
        latest_gdp = gdp_data.sort_values('date', ascending=False).iloc[0]
        gdp_score = min(max(latest_gdp['yoy_growth'] * 10, 0), 100)  # Scale: 0 to 100
        sentiment_components.append({
            'indicator': 'GDP % Change',
            'value': latest_gdp['yoy_growth'],
            'score': gdp_score,
            'weight': weights['GDP % Change']
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
    
    # 4. Market Performance - NASDAQ recent trend
    if not nasdaq_data.empty and 'pct_change' in nasdaq_data.columns:
        # Take average of last 3 months for trend
        recent_nasdaq = nasdaq_data.sort_values('date', ascending=False).head(3)
        avg_change = recent_nasdaq['pct_change'].mean()
        nasdaq_score = min(max(50 + avg_change * 5, 0), 100)  # Scale: 0 to 100
        sentiment_components.append({
            'indicator': 'NASDAQ Trend',
            'value': avg_change,
            'score': nasdaq_score,
            'weight': weights['NASDAQ Trend']
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
    
    # 8. Add proprietary data if provided
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
unemployment_data = load_data_from_csv('unemployment_data.csv')
inflation_data = load_data_from_csv('inflation_data.csv')
interest_rate_data = load_data_from_csv('interest_rate_data.csv')

# Add NASDAQ Composite data from FRED (NASDAQCOM)
nasdaq_data = load_data_from_csv('nasdaq_data.csv')

# If no existing data or data is old, fetch new data
if nasdaq_data.empty or (datetime.now() - pd.to_datetime(nasdaq_data['date'].max())).days > 7:
    nasdaq_data = fetch_fred_data('NASDAQCOM')
    
    if not nasdaq_data.empty:
        # Calculate percent change
        nasdaq_data = nasdaq_data.sort_values('date')
        nasdaq_data['pct_change'] = nasdaq_data['value'].pct_change() * 100
        
        # Save data
        save_data_to_csv(nasdaq_data, 'nasdaq_data.csv')
        
        print(f"NASDAQ data updated with {len(nasdaq_data)} observations")
    else:
        print("Failed to fetch NASDAQ data")

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

# Calculate initial sentiment index
sentiment_index = calculate_sentiment_index()

# ---- Dashboard Layout ----
app.layout = html.Div([
    # Hidden div for initialization callbacks
    html.Div(id="_", style={"display": "none"}),
    
    # Header
    html.Div([
        html.H1("Economic Dashboard: Software & Technology Industry", className="dashboard-title"),
        html.P("Interactive dashboard of economic indicators for software and technology sector", 
               className="dashboard-subtitle")
    ], className="header"),
    
    # Top Section - Sentiment Index
    html.Div([
        # Sentiment Index Banner
        html.Div([
            # Left side - Score and Category
            html.Div([
                html.H3("Tech Economy Sentiment Index", className="sentiment-banner-title"),
                html.Div([
                    html.H2(id="sentiment-score", 
                           children=f"{sentiment_index['score']:.1f}" if sentiment_index else "N/A", 
                           className="sentiment-score"),
                    html.Div([
                        html.H4(id="sentiment-category", 
                               children=sentiment_index['category'] if sentiment_index else "N/A", 
                               className="sentiment-category", 
                               style={"display": "inline-block", "marginRight": "5px"}),
                        html.Span(
                            "ⓘ", 
                            id="sentiment-info-icon",
                            className="info-icon",
                            style={
                                "cursor": "pointer", 
                                "fontSize": "16px", 
                                "display": "inline-block",
                                "color": "#2c3e50"
                            }
                        ),
                        html.Div(
                            id="sentiment-info-tooltip",
                            style={"display": "none", "position": "absolute", "zIndex": "1000", 
                                  "backgroundColor": "white", "padding": "10px", "borderRadius": "5px", 
                                  "boxShadow": "0px 0px 10px rgba(0,0,0,0.1)", "maxWidth": "400px", 
                                  "top": "100%", "left": "50%", "transform": "translateX(-50%)",
                                  "marginTop": "10px"},
                            children=[
                                html.H5("Sentiment Index Categories", style={"marginBottom": "10px"}),
                                html.Div([
                                    html.Div([
                                        html.Span("Boom (80-100): ", style={"fontWeight": "bold", "color": "#28a745"}),
                                        "Strong growth across indicators; economic expansion accelerating"
                                    ], style={"marginBottom": "5px"}),
                                    html.Div([
                                        html.Span("Expansion (60-79): ", style={"fontWeight": "bold", "color": "#5cb85c"}),
                                        "Solid growth with positive momentum; healthy economic conditions"
                                    ], style={"marginBottom": "5px"}),
                                    html.Div([
                                        html.Span("Moderate Growth (40-59): ", style={"fontWeight": "bold", "color": "#f0ad4e"}),
                                        "Steady but modest growth; economy performing adequately"
                                    ], style={"marginBottom": "5px"}),
                                    html.Div([
                                        html.Span("Slowdown (20-39): ", style={"fontWeight": "bold", "color": "#d9534f"}),
                                        "Growth decelerating; potential economic challenges ahead"
                                    ], style={"marginBottom": "5px"}),
                                    html.Div([
                                        html.Span("Contraction (0-19): ", style={"fontWeight": "bold", "color": "#d9534f"}),
                                        "Economic indicators showing decline; recession risks elevated"
                                    ])
                                ])
                            ]
                        )
                    ], style={"display": "flex", "alignItems": "center", "justifyContent": "center", "position": "relative"})
                ], className="sentiment-display")
            ], className="sentiment-banner-left"),
            
            # Right side - Gauge
            html.Div([
                html.Div(id="sentiment-gauge", className="sentiment-gauge-container")
            ], className="sentiment-banner-right")
        ], className="sentiment-banner")
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
                        html.H4("GDP % Change"),
                        html.P(id="gdp-value", 
                              children=f"{gdp_data.sort_values('date', ascending=False).iloc[0]['yoy_growth']:.1f}%" 
                              if not gdp_data.empty and 'yoy_growth' in gdp_data.columns else "N/A",
                              className="indicator-value")
                    ], className="indicator-text"),
                    html.Div(id="gdp-trend", className="indicator-trend")
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
                              children=f"{nasdaq_data.sort_values('date', ascending=False).iloc[0]['value']:.0f}" 
                              if not nasdaq_data.empty else "N/A",
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
            ], className="card indicators-card"),
            
            # Custom Weight Adjustment
            html.Div([
                html.H3("Customize Index Weights", className="card-title"),
                html.Div([
                    html.Div([
                        html.Label("GDP % Change"),
                        dcc.Slider(
                            id="gdp-weight",
                            min=0,
                            max=30,
                            step=1,
                            value=14,
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
                            step=1,
                            value=14,
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
                            step=1,
                            value=14,
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
                            step=1,
                            value=14,
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
                            step=1,
                            value=14,
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
                            step=1,
                            value=15,
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
                            step=1,
                            value=15,
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
                            value=20,
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
                        html.Div(id="document-weight-display", className="weight-display")
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
                # GDP & Economic Growth Tab
                dcc.Tab(label="GDP & Growth", children=[
                    html.Div([
                        html.H3("Real GDP Growth (YoY %)", className="graph-title"),
                        dcc.Graph(id="gdp-graph")
                    ], className="graph-container")
                ], className="custom-tab", selected_className="custom-tab--selected"),
                
                # Labor Market Tab
                dcc.Tab(label="Labor Market", children=[
                    html.Div([
                        html.H3("Unemployment Rate", className="graph-title"),
                        dcc.Graph(id="unemployment-graph")
                    ], className="graph-container")
                ], className="custom-tab", selected_className="custom-tab--selected"),
                
                # Inflation Tab
                dcc.Tab(label="Inflation", children=[
                    html.Div([
                        html.H3("Consumer Price Index (YoY %)", className="graph-title"),
                        dcc.Graph(id="inflation-graph")
                    ], className="graph-container")
                ], className="custom-tab", selected_className="custom-tab--selected"),
                
                # Financial Markets Tab
                dcc.Tab(label="Markets", children=[
                    html.Div([
                        html.H3("NASDAQ Composite Index", className="graph-title"),
                        dcc.Graph(id="nasdaq-graph")
                    ], className="graph-container")
                ], className="custom-tab", selected_className="custom-tab--selected"),
                
                # Tech Sector Prices Tab
                dcc.Tab(label="Tech Sector", children=[
                    html.Div([
                        # Software PPI Graph
                        html.H3("PPI: Software Publishers (YoY %)", className="graph-title"),
                        dcc.Graph(id="software-ppi-graph"),
                        
                        # Data Processing PPI Graph
                        html.H3("PPI: Data Processing Services (YoY %)", className="graph-title"),
                        dcc.Graph(id="data-ppi-graph")
                    ], className="graph-container")
                ], className="custom-tab", selected_className="custom-tab--selected"),
                
                # Monetary Policy Tab
                dcc.Tab(label="Monetary Policy", children=[
                    html.Div([
                        html.H3("Federal Funds Rate", className="graph-title"),
                        dcc.Graph(id="interest-rate-graph")
                    ], className="graph-container")
                ], className="custom-tab", selected_className="custom-tab--selected"),
            ], className="custom-tabs")
        ], className="column right-column")
    ], className="dashboard-content"),
    
    # Footer
    html.Footer([
        html.P("Economic Dashboard for Software & Technology Industry"),
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
    
    # Create gauge chart
    fig = go.Figure(go.Indicator(
        mode="gauge+number",
        value=score_value,
        domain={'x': [0, 1], 'y': [0, 1]},
        number={'font': {'size': 40}},
        gauge={
            'axis': {'range': [0, 100], 'tickwidth': 1, 'tickfont': {'size': 12}},
            'bar': {'color': "royalblue"},
            'bgcolor': "white",
            'borderwidth': 2,
            'bordercolor': "lightgray",
            'steps': [
                {'range': [0, 20], 'color': "rgba(255, 0, 0, 0.7)"},
                {'range': [20, 40], 'color': "rgba(255, 165, 0, 0.7)"},
                {'range': [40, 60], 'color': "rgba(255, 255, 0, 0.7)"},
                {'range': [60, 80], 'color': "rgba(144, 238, 144, 0.7)"},
                {'range': [80, 100], 'color': "rgba(0, 128, 0, 0.7)"}
            ],
            'threshold': {
                'line': {'color': "white", 'width': 4},
                'thickness': 0.75,
                'value': score_value
            }
        }
    ))
    
    fig.update_layout(
        height=140,
        margin=dict(l=10, r=10, t=10, b=10),
        paper_bgcolor='rgba(0,0,0,0)',
        plot_bgcolor='rgba(0,0,0,0)',
        font={'size': 12}
    )
    
    return dcc.Graph(figure=fig, config={'displayModeBar': False})

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
        if comp['indicator'] == 'GDP % Change' or comp['indicator'] == 'CPI':
            value_text = f"{comp['value']:.1f}%"
        elif comp['indicator'] == 'Unemployment Rate' or comp['indicator'] == 'Federal Funds Rate':
            value_text = f"{comp['value']:.2f}%"
        elif comp['indicator'] == 'NASDAQ Trend':
            value_text = f"{comp['value']:.2f}%"
        elif 'PPI' in comp['indicator']:
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
                html.Span(f"Weight: {comp['weight']}%", className="component-weight"),
                html.Span(f"Contribution: {comp['contribution']:.1f}", className="component-contribution")
            ], className="component-footer")
        ], className="component-item")
        
        components_list.append(item)
    
    return html.Div(components_list, className="components-list")

# Update all indicator trends
@app.callback(
    [Output("gdp-trend", "children"),
     Output("unemployment-trend", "children"),
     Output("inflation-trend", "children"),
     Output("interest-rate-trend", "children"),
     Output("nasdaq-trend", "children"),
     Output("software-ppi-trend", "children"),
     Output("data-ppi-trend", "children")],
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
            
            # First, always show the actual direction of change
            if abs(change) < 0.2:  # Very small change
                icon = "→"
                color = "trend-neutral"  # Black for sideways arrows
            else:
                icon = "↑" if change > 0 else "↓"
                color = "trend-up" if icon == "↑" else "trend-down"  # Green for up, Red for down
                
            gdp_trend = html.Div([
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
    
    # NASDAQ Trend
    nasdaq_trend = html.Div("No data", className="trend-value")
    if not nasdaq_data.empty:
        sorted_nasdaq = nasdaq_data.sort_values('date', ascending=False)
        if len(sorted_nasdaq) >= 2:
            current = sorted_nasdaq.iloc[0]['value']
            previous = sorted_nasdaq.iloc[1]['value']
            change = ((current - previous) / previous) * 100  # Percent change
            
            # First, always show the actual direction of change
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
    
    return gdp_trend, unemployment_trend, inflation_trend, interest_rate_trend, nasdaq_trend, software_ppi_trend, data_ppi_trend

# Update GDP Graph
@app.callback(
    Output("gdp-graph", "figure"),
    [Input("interval-component", "n_intervals")]
)
def update_gdp_graph(n):
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
    
    # Add GDP Growth line
    fig.add_trace(go.Scatter(
        x=filtered_data['date'],
        y=filtered_data['yoy_growth'],
        mode='lines+markers',
        name='Real GDP Growth (YoY %)',
        line=dict(color='royalblue', width=3),
        marker=dict(size=8)
    ))
    
    # Add recession shading (if data available)
    # This would require recession date data which is not included
    
    # Update layout
    fig.update_layout(
        height=400,
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

# Update Unemployment Graph
@app.callback(
    Output("unemployment-graph", "figure"),
    [Input("interval-component", "n_intervals")]
)
def update_unemployment_graph(n):
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
        height=400,
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

# Update Inflation Graph
@app.callback(
    Output("inflation-graph", "figure"),
    [Input("interval-component", "n_intervals")]
)
def update_inflation_graph(n):
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
        line=dict(color='orange', width=3),
    ))
    
    # Add target inflation line
    x_range = [filtered_data['date'].min(), filtered_data['date'].max()]
    fig.add_trace(go.Scatter(
        x=x_range,
        y=[2, 2],
        mode='lines',
        line=dict(color='green', width=2, dash='dash'),
        name='Fed Target (2%)'
    ))
    
    # Update layout
    fig.update_layout(
        height=400,
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

# Update NASDAQ Graph
@app.callback(
    Output("nasdaq-graph", "figure"),
    [Input("interval-component", "n_intervals")]
)
def update_nasdaq_graph(n):
    if nasdaq_data.empty:
        return go.Figure().update_layout(
            title="No data available",
            height=400
        )
    
    # Filter for last 2 years
    cutoff_date = datetime.now() - timedelta(days=2*365)
    filtered_data = nasdaq_data[nasdaq_data['date'] >= cutoff_date].copy()
    
    # Create figure with both value and percent change
    fig = make_subplots(specs=[[{"secondary_y": True}]])
    
    # Add NASDAQ value line
    fig.add_trace(
        go.Scatter(
            x=filtered_data['date'],
            y=filtered_data['value'],
            mode='lines',
            name='NASDAQ Composite',
            line=dict(color='purple', width=3),
        ),
        secondary_y=False
    )
    
    # Add percent change line if available
    if 'pct_change' in filtered_data.columns:
        # Calculate moving average for smoothing
        filtered_data['pct_change_ma'] = filtered_data['pct_change'].rolling(window=30).mean()
        
        fig.add_trace(
            go.Scatter(
                x=filtered_data['date'],
                y=filtered_data['pct_change_ma'],
                mode='lines',
                name='30-Day Avg % Change',
                line=dict(color='green', width=2, dash='dot'),
            ),
            secondary_y=True
        )
    
    # Update layout
    fig.update_layout(
        height=400,
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
    
    # Update y-axes
    fig.update_yaxes(title_text="NASDAQ Composite", secondary_y=False)
    fig.update_yaxes(title_text="30-Day Avg % Change", ticksuffix="%", secondary_y=True)
    
    return fig

# Update Software PPI Graph
@app.callback(
    Output("software-ppi-graph", "figure"),
    [Input("interval-component", "n_intervals")]
)
def update_software_ppi_graph(n):
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
        height=300,
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

# Update Data Processing PPI Graph
@app.callback(
    Output("data-ppi-graph", "figure"),
    [Input("interval-component", "n_intervals")]
)
def update_data_ppi_graph(n):
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
        height=300,
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

# Update Interest Rate Graph
@app.callback(
    Output("interest-rate-graph", "figure"),
    [Input("interval-component", "n_intervals")]
)
def update_interest_rate_graph(n):
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
    
    # Add interest rate line
    fig.add_trace(go.Scatter(
        x=filtered_data['date'],
        y=filtered_data['value'],
        mode='lines',
        name='Federal Funds Rate',
        line=dict(color='darkgreen', width=3),
    ))
    
    # Add optimal range shading (2-3% is often considered neutral)
    x_range = [filtered_data['date'].min(), filtered_data['date'].max()]
    
    fig.add_trace(go.Scatter(
        x=x_range + x_range[::-1],
        y=[2, 2, 3, 3],
        fill='toself',
        fillcolor='rgba(0, 255, 0, 0.1)',
        line=dict(color='rgba(0, 255, 0, 0.5)'),
        hoverinfo='skip',
        name='Neutral Rate Range',
        showlegend=True
    ))
    
    # Update layout
    fig.update_layout(
        height=400,
        margin=dict(l=40, r=40, t=40, b=40),
        xaxis_title="",
        yaxis_title="Federal Funds Rate (%)",
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

# Calculate total weights and validate
@app.callback(
    Output("total-weight", "children"),
    [Input("gdp-weight", "value"),
     Input("unemployment-weight", "value"),
     Input("cpi-weight", "value"),
     Input("nasdaq-weight", "value"),
     Input("data-ppi-weight", "value"),
     Input("software-ppi-weight", "value"),
     Input("interest-rate-weight", "value")],
    [State("document-data-store", "data")]
)
def update_total_weight(gdp, unemployment, cpi, nasdaq, data_ppi, software_ppi, interest_rate, document_data_store):
    # Get document weight if it exists
    document_weight = 0
    if document_data_store and isinstance(document_data_store, dict) and 'weight' in document_data_store:
        document_weight = float(document_data_store['weight'])
    
    # Calculate total of economic indicators only
    economic_indicators_total = gdp + unemployment + cpi + nasdaq + data_ppi + software_ppi + interest_rate
    
    # If we have document weight, we need to normalize the economic weights to (100 - document_weight)
    if document_weight > 0:
        # Calculate what the economic indicators should total
        target_economic_total = 100 - document_weight
        
        # Format message to show both economic indicator total and overall total
        message = f"Economic Indicators: {economic_indicators_total:.1f}% (target: {target_economic_total:.1f}%), Document: {document_weight:.1f}%, Total: {economic_indicators_total + document_weight:.1f}%"
        
        # Change color based on if economic indicators sum to the target
        if abs(economic_indicators_total - target_economic_total) < 0.1:
            color = "green"
        else:
            color = "red"
    else:
        # No document weight, so economic indicators should sum to 100%
        message = f"Total: {economic_indicators_total:.1f}%"
        
        # Change color based on total
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
     State("unemployment-weight", "value"),
     State("cpi-weight", "value"),
     State("nasdaq-weight", "value"),
     State("data-ppi-weight", "value"),
     State("software-ppi-weight", "value"),
     State("interest-rate-weight", "value"),
     State("proprietary-data-store", "data"),
     State("document-data-store", "data")],
    prevent_initial_call=True
)
def apply_custom_weights(n_clicks, gdp, unemployment, cpi, nasdaq, 
                         data_ppi, software_ppi, interest_rate, proprietary_data, document_data):
    if n_clicks is None:
        # Initial load, use default weights
        sentiment_index = calculate_sentiment_index(proprietary_data=proprietary_data, document_data=document_data)
        return None, f"{sentiment_index['score']:.1f}" if sentiment_index else "N/A", sentiment_index['category'] if sentiment_index else "N/A"
    
    # Get document weight if available (it will be 0 if no document has been processed)
    document_weight = 0
    if document_data and isinstance(document_data, dict) and 'weight' in document_data:
        document_weight = float(document_data['weight'])
        document_weight = max(0, min(50, document_weight))  # Enforce 0-50% range
    
    # Total of economic indicators should be (100 - document_weight)
    target_economic_total = 100 - document_weight
    
    # Create custom weights dictionary
    total_economic_weight = gdp + unemployment + cpi + nasdaq + data_ppi + software_ppi + interest_rate
    
    # Check if we need to normalize the weights to target_economic_total
    if abs(total_economic_weight - target_economic_total) > 0.1:
        # Normalize economic indicators to sum to (100 - document_weight)
        scaling_factor = target_economic_total / total_economic_weight
        gdp = gdp * scaling_factor
        unemployment = unemployment * scaling_factor
        cpi = cpi * scaling_factor
        nasdaq = nasdaq * scaling_factor
        data_ppi = data_ppi * scaling_factor
        software_ppi = software_ppi * scaling_factor
        interest_rate = interest_rate * scaling_factor
    
    custom_weights = {
        'GDP % Change': gdp,
        'Unemployment Rate': unemployment,
        'CPI': cpi,
        'NASDAQ Trend': nasdaq,
        'PPI: Data Processing Services': data_ppi,
        'PPI: Software Publishers': software_ppi,
        'Federal Funds Rate': interest_rate
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
    global nasdaq_data, software_ppi_data, data_processing_ppi_data
    
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
    
    # Also update other datasets
    # NASDAQ
    nasdaq_temp = fetch_fred_data('NASDAQCOM')
    if not nasdaq_temp.empty:
        nasdaq_temp = nasdaq_temp.sort_values('date')
        nasdaq_temp['pct_change'] = nasdaq_temp['value'].pct_change() * 100
        nasdaq_data = nasdaq_temp
        save_data_to_csv(nasdaq_data, 'nasdaq_data.csv')
    
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
    [Input("_", "children")],
    prevent_initial_call=False
)
def initialize_sentiment_index(_):
    sentiment_index = calculate_sentiment_index()
    return (
        f"{sentiment_index['score']:.1f}" if sentiment_index else "N/A", 
        sentiment_index['category'] if sentiment_index else "N/A"
    )

# Document weight display update
@app.callback(
    [Output("document-weight-display", "children"),
     Output("document-weight", "disabled")],
    [Input("document-weight", "value"),
     Input("upload-document", "contents"),
     Input("document-data-store", "data")]
)
def update_document_weight_display(weight, contents, document_data):
    # Check if document is uploaded and processed
    has_document = contents is not None
    document_processed = document_data is not None and isinstance(document_data, dict) and 'value' in document_data
    
    # If document not uploaded or not processed yet, show guidance message and disable slider
    if not has_document or not document_processed:
        return html.Div([
            html.Span("Upload a document and click 'Apply Document Analysis' to enable document weighting", 
                    className="weight-value",
                    style={"color": "#888"})
        ]), True
    
    # Document is uploaded and processed, allow weight adjustment
    remaining = 100 - weight
    return html.Div([
        html.Span(f"Document Weight: {weight}%", className="weight-value"),
        html.Span(f"Remaining for Economic Indicators: {remaining}%", 
                 className="weight-remaining",
                 style={"marginLeft": "10px", "color": "green" if remaining >= 0 else "red"})
    ]), False

# Process and preview document upload for sentiment analysis
@app.callback(
    Output("document-preview", "children"),
    [Input("upload-document", "contents"),
     Input("upload-document", "filename")]
)
def update_document_preview(contents, filename):
    if contents is None:
        return html.Div("No document uploaded")
    
    try:
        # Decode the file contents
        content_type, content_string = contents.split(',')
        decoded = base64.b64decode(content_string)
        
        # Process the document
        result = document_analysis.process_document(decoded, filename)
        
        if result["status"] == "success":
            # Format the sentiment scores for display
            sentiment_label = result["full_text_sentiment"]["label"].capitalize()
            sentiment_score = result["full_text_sentiment"]["score"]
            sentiment_color = "green" if sentiment_score >= 60 else "orange" if sentiment_score >= 40 else "red"
            
            # Create sentiment score display
            sentiment_display = html.Div([
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
                
                # Q&A section if found
                html.Div([
                    html.P("Q&A Section:", className="sentiment-label"),
                    html.P(
                        f"Found ({result['qa_section_length']} characters)" if result["qa_section_found"] 
                        else "Not found",
                        className="sentiment-value"
                    )
                ], className="sentiment-row"),
                
                # Q&A specific sentiment if available
                html.Div([
                    html.P("Q&A Sentiment:", className="sentiment-label"),
                    html.P(
                        f"{result['qa_sentiment']['label'].capitalize()} ({result['qa_sentiment']['score']:.1f}/100)" 
                        if result["qa_sentiment"] else "N/A",
                        className="sentiment-value",
                        style={"color": sentiment_color if result["qa_sentiment"] else "inherit"}
                    )
                ], className="sentiment-row") if result["qa_sentiment"] else None,
                
                # Date processed
                html.Div([
                    html.P("Processed:", className="sentiment-label"),
                    html.P(result["processed_date"], className="sentiment-value")
                ], className="sentiment-row"),
                
                # Selected for sentiment index indicator
                html.Div([
                    html.P("Document Score:", className="sentiment-score-label"),
                    html.P(f"{result['overall_score']:.1f}/100", 
                          className="sentiment-score-value",
                          style={"fontSize": "24px", "fontWeight": "bold", "color": sentiment_color})
                ], className="sentiment-score")
            ], className="document-sentiment-container")
            
            return html.Div([
                html.P(f"File: {filename}", className="uploaded-filename"),
                sentiment_display
            ])
        else:
            return html.Div(f"Error: {result['message']}")
    except Exception as e:
        return html.Div(f"Error processing document: {str(e)}")

# Apply document sentiment analysis to index
@app.callback(
    [Output("document-data-store", "data"),
     Output("sentiment-score", "children", allow_duplicate=True),
     Output("sentiment-category", "children", allow_duplicate=True),
     Output("total-weight", "children", allow_duplicate=True)],
    [Input("apply-document", "n_clicks")],
    [State("document-weight", "value"),
     State("upload-document", "contents"),
     State("upload-document", "filename"),
     State("custom-weights-store", "data"),
     State("proprietary-data-store", "data"),
     State("gdp-weight", "value"),
     State("unemployment-weight", "value"),
     State("cpi-weight", "value"),
     State("nasdaq-weight", "value"),
     State("data-ppi-weight", "value"),
     State("software-ppi-weight", "value"),
     State("interest-rate-weight", "value")],
    prevent_initial_call=True
)
def apply_document_analysis(n_clicks, weight, contents, filename, custom_weights, proprietary_data,
                          gdp, unemployment, cpi, nasdaq, data_ppi, software_ppi, interest_rate):
    # Document weight should not be applied until a document is uploaded and analyzed
    if n_clicks is None:
        # Return document weight of 0
        return {'weight': 0, 'value': 0}, dash.no_update, dash.no_update, dash.no_update
        
    # Document content is required
    if contents is None:
        return None, dash.no_update, dash.no_update, html.Span("No document uploaded. Upload a document first.", style={"color": "red"})
    
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
            
            # Calculate total weight display
            economic_indicators_total = gdp + unemployment + cpi + nasdaq + data_ppi + software_ppi + interest_rate
            
            # Format message for total weight
            message = f"Economic Indicators: {economic_indicators_total:.1f}%, Document: {weight:.1f}%, Total: {economic_indicators_total + weight:.1f}%"
            
            # Change color based on if the total is exactly 100%
            if abs((economic_indicators_total + weight) - 100) < 0.1:
                color = "green"
            else:
                color = "red"
                
            total_weight_display = html.Span(message, style={"color": color})
            
            return document_data, f"{sentiment_index['score']:.1f}" if sentiment_index else "N/A", sentiment_index['category'] if sentiment_index else "N/A", total_weight_display
        else:
            # Document processing failed
            return None, dash.no_update, dash.no_update, dash.no_update
    except Exception as e:
        print(f"Error applying document analysis: {str(e)}")
        return None, dash.no_update, dash.no_update, dash.no_update

# Add this at the end of the file if running directly
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=False)
