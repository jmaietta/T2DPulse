import os
import pandas as pd
import numpy as np
import requests
from datetime import datetime, timedelta
import dash
from dash import dcc, html
from dash.dependencies import Input, Output, State
import plotly.graph_objs as go
import plotly.express as px

# Import API keys from the separate file
from api_keys import FRED_API_KEY, BEA_API_KEY, BLS_API_KEY

# Data directory
DATA_DIR = 'data'
os.makedirs(DATA_DIR, exist_ok=True)

# Initialize the Dash app
app = dash.Dash(__name__)

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

def calculate_sentiment_index(custom_weights=None):
    """Calculate economic sentiment index from available indicators
    
    Args:
        custom_weights (dict, optional): Dictionary with custom weights for each indicator
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
    
    # Use custom weights if provided, otherwise use defaults
    weights = custom_weights if custom_weights else default_weights
    
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
    
    # 6. NEW: Software Publishers PPI - similar scoring as data processing
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
    
    # 7. NEW: Interest Rates - moderate rates ideal (around 2-3%)
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
        data_processing_ppi_yoy = data_processing_ppi_data.copy()
        data_processing_ppi_yoy['date'] = data_processing_ppi_yoy['date'] + pd.DateOffset(years=1)
        data_processing_ppi_yoy = data_processing_ppi_yoy.rename(columns={'value': 'year_ago_value'})
        
        # Merge current and year-ago values
        data_processing_ppi_data = pd.merge(
            data_processing_ppi_data, 
            data_processing_ppi_yoy[['date', 'year_ago_value']], 
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

# Calculate year-over-year GDP growth if we have GDP data
if not gdp_data.empty:
    # Sort by date to ensure proper calculation
    gdp_data = gdp_data.sort_values('date')
    
    # Calculate year-over-year growth
    gdp_data['year_ago_date'] = gdp_data['date'] - pd.DateOffset(years=1)
    
    # Create a lookup dictionary for values from a year ago
    value_lookup = dict(zip(gdp_data['date'], gdp_data['value']))
    
    # Get the value from a year ago for each date
    gdp_data['year_ago_value'] = gdp_data['year_ago_date'].map(value_lookup)
    
    # Calculate the growth rate
    gdp_data['yoy_growth'] = ((gdp_data['value'] - gdp_data['year_ago_value']) / 
                             gdp_data['year_ago_value'] * 100)
    
    # Drop intermediate columns
    gdp_data = gdp_data.drop(['year_ago_date', 'year_ago_value'], axis=1)
    
    print("Year-over-year GDP growth calculated")

# Alternatively, fetch the GDP growth rate directly from FRED
gdp_growth_data = load_data_from_csv('gdp_growth_data.csv')

# If no existing data or data is old, fetch new data
if gdp_growth_data.empty or (datetime.now() - pd.to_datetime(gdp_growth_data['date'].max())).days > 90:
    # A1Y is the GDP growth rate (1-year change)
    gdp_growth_data = fetch_fred_data('A191RL1Q225SBEA')
    
    if not gdp_growth_data.empty:
        # Save data
        save_data_to_csv(gdp_growth_data, 'gdp_growth_data.csv')

# Calculate inflation rate if we have CPI data
if not inflation_data.empty:
    # Sort by date to ensure proper calculation
    inflation_data = inflation_data.sort_values('date')
    
    # Create a dataframe shifted by 12 months to calculate YoY change
    inflation_yoy = inflation_data.copy()
    inflation_yoy['date'] = inflation_yoy['date'] + pd.DateOffset(years=1)
    inflation_yoy = inflation_yoy.rename(columns={'value': 'year_ago_value'})
    
    # Merge current and year-ago values
    inflation_data = pd.merge(
        inflation_data, 
        inflation_yoy[['date', 'year_ago_value']], 
        on='date', 
        how='left'
    )
    
    # Calculate YoY percent change (inflation rate)
    inflation_data['inflation'] = ((inflation_data['value'] - inflation_data['year_ago_value']) / 
                                  inflation_data['year_ago_value'] * 100)
    
    print(f"Inflation rate calculated with {len(inflation_data)} observations")


# Calculate sentiment index
sentiment_index = calculate_sentiment_index()

# Handle case where sentiment index couldn't be calculated
if sentiment_index is None:
    sentiment_index = {
        'score': 50,
        'category': 'Data Unavailable',
        'components': [],
        'available_weight_sum': 0
    }

# Now define the layout
app.layout = html.Div([
    html.H1("Economic Dashboard: Software & Technology", style={"textAlign": "center"}),

    # Add weight adjustment controls
    html.Div([
        html.H3("Customize Sentiment Weights", style={"textAlign": "center"}),
        html.Div([
            html.Div([
                html.Label("GDP % Change", style={"display": "block", "marginBottom": "5px"}),
                dcc.Slider(
                    id='gdp-weight-slider',
                    min=0,
                    max=50,
                    step=5,
                    value=14,
                    marks={i: f'{i}%' for i in range(0, 51, 10)},
                ),
            ], style={"marginBottom": "15px"}),
            
            html.Div([
                html.Label("Unemployment Rate", style={"display": "block", "marginBottom": "5px"}),
                dcc.Slider(
                    id='unemployment-weight-slider',
                    min=0,
                    max=50,
                    step=5,
                    value=14,
                    marks={i: f'{i}%' for i in range(0, 51, 10)},
                ),
            ], style={"marginBottom": "15px"}),
            
            html.Div([
                html.Label("CPI", style={"display": "block", "marginBottom": "5px"}),
                dcc.Slider(
                    id='inflation-weight-slider',
                    min=0,
                    max=50,
                    step=5,
                    value=14,
                    marks={i: f'{i}%' for i in range(0, 51, 10)},
                ),
            ], style={"marginBottom": "15px"}),
            
            html.Div([
                html.Label("NASDAQ Trend", style={"display": "block", "marginBottom": "5px"}),
                dcc.Slider(
                    id='nasdaq-weight-slider',
                    min=0,
                    max=50,
                    step=5,
                    value=14,
                    marks={i: f'{i}%' for i in range(0, 51, 10)},
                ),
            ], style={"marginBottom": "15px"}),
            
            html.Div([
                html.Label("PPI: Data Processing Services", style={"display": "block", "marginBottom": "5px"}),
                dcc.Slider(
                    id='tech-weight-slider',
                    min=0,
                    max=50,
                    step=5,
                    value=14,
                    marks={i: f'{i}%' for i in range(0, 51, 10)},
                ),
            ], style={"marginBottom": "15px"}),
            
            html.Div([
                html.Label("PPI: Software Publishers", style={"display": "block", "marginBottom": "5px"}),
                dcc.Slider(
                    id='software-ppi-weight-slider',
                    min=0,
                    max=50,
                    step=5,
                    value=15,
                    marks={i: f'{i}%' for i in range(0, 51, 10)},
                ),
            ], style={"marginBottom": "15px"}),
            
            html.Div([
                html.Label("Federal Funds Rate", style={"display": "block", "marginBottom": "5px"}),
                dcc.Slider(
                    id='interest-rate-weight-slider',
                    min=0,
                    max=50,
                    step=5,
                    value=15,
                    marks={i: f'{i}%' for i in range(0, 51, 10)},
                ),
            ], style={"marginBottom": "15px"}),
            
            html.Div([
                html.Button(
                    "Apply Weights", 
                    id="apply-weights-button",
                    style={
                        "backgroundColor": "#007bff",
                        "color": "white",
                        "border": "none",
                        "padding": "10px 20px",
                        "borderRadius": "5px",
                        "cursor": "pointer",
                        "marginRight": "10px"
                    }
                ),
                html.Div(id="total-weight-display", style={"marginTop": "10px", "fontWeight": "bold"})
            ], style={"textAlign": "center"})
        ], style={"backgroundColor": "white", "padding": "20px", "borderRadius": "10px", "boxShadow": "0 2px 4px rgba(0,0,0,0.1)"})
    ]),

    # Add sentiment gauge to dashboard
    html.Div([
        html.H3("Economic Sentiment Index", style={"textAlign": "center"}),
        html.Div([
            html.Div([
                html.Div(
                    sentiment_index['category'],
                    id="sentiment-category",
                    style={
                        "fontSize": "24px",
                        "fontWeight": "bold",
                        "textAlign": "center",
                        "margin": "20px"
                    }
                ),
                html.Div(
                    f"Score: {sentiment_index['score']:.1f}/100",
                    id="sentiment-score",
                    style={
                        "fontSize": "18px",
                        "textAlign": "center",
                        "margin": "10px"
                    }
                ),
                # Gauge visualization
                html.Div(
                    style={
                        "height": "30px",
                        "width": "80%",
                        "margin": "20px auto",
                        "background": "linear-gradient(to right, #d9534f, #f0ad4e, #5cb85c)",
                        "borderRadius": "15px",
                        "position": "relative"
                    },
                    children=[
                        html.Div(
                            id="sentiment-gauge-indicator",
                            style={
                                "position": "absolute",
                                "left": f"{sentiment_index['score']}%",
                                "top": "-10px",
                                "width": "20px",
                                "height": "50px",
                                "background": "black",
                                "transform": "translateX(-50%)"
                            }
                        )
                    ]
                ),
                
                # Category descriptions
                html.Div([
                    html.H4("Sentiment Categories", style={"fontSize": "16px", "fontWeight": "bold", "textAlign": "center", "marginBottom": "10px"}),
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
                        ]),
                    ], style={"backgroundColor": "#f8f9fa", "padding": "10px", "borderRadius": "5px", "fontSize": "14px"}),
                ], style={"margin": "20px 40px 30px 40px"}),
                
                # Component breakdown
                html.Div([
                   html.Table(
                        id="sentiment-components-table",
                        children=[
                            html.Thead(
                                html.Tr([
                                    html.Th("Indicator", style={"textAlign": "left", "padding": "8px"}),
                                    html.Th("Value", style={"textAlign": "right", "padding": "8px"}),
                                    html.Th("Weight", style={"textAlign": "right", "padding": "8px"}),
                                    html.Th("Contribution", style={"textAlign": "right", "padding": "8px"}),
                                    html.Th("Rating", style={"textAlign": "left", "padding": "8px"})
                                ])
                            ),
                            html.Tbody([
                                html.Tr([
                                    html.Td(comp['indicator'], style={"textAlign": "left", "padding": "8px"}),
                                    html.Td(f"{comp['value']:.2f}", style={"textAlign": "right", "padding": "8px"}),
                                    html.Td(f"{comp['weight']}%", style={"textAlign": "right", "padding": "8px"}),
                                    html.Td(f"{comp['contribution']:.1f}", style={"textAlign": "right", "padding": "8px"}),
                                    html.Td(
                                        "●" * int(comp['score'] / 20), 
                                        style={
                                            "color": "#28a745" if comp['score'] >= 60 else "#dc3545",
                                            "textAlign": "left", 
                                            "padding": "8px"
                                        }
                                    )
                                ]) for comp in sentiment_index['components']
                            ])
                        ],
                        style={
                            "width": "100%", 
                            "border": "1px solid #ddd", 
                            "borderCollapse": "collapse",
                            "fontFamily": "Arial, sans-serif"
                        }
                    )
                ], style={"margin": "20px"})
            ], style={"backgroundColor": "white", "padding": "20px", "borderRadius": "10px", "boxShadow": "0 2px 4px rgba(0,0,0,0.1)"})
        ])
    ]),

    html.Div([
        html.H3("NASDAQ Composite Index", style={"textAlign": "center"}),
        dcc.Graph(
            id='nasdaq-graph',
            figure={
                'data': [
                    go.Scatter(
                        x=nasdaq_data['date'],
                        y=nasdaq_data['value'],
                        mode='lines',
                        name='NASDAQ Composite',
                        line=dict(color='#6600cc', width=2)
                    )
                ],
                'layout': go.Layout(
                    xaxis={'title': 'Date'},
                    yaxis={'title': 'Index Value'},
                    margin={'l': 60, 'b': 50, 't': 30, 'r': 30}
                )
            }
        )
    ]),   

    html.Div([
        html.H3("GDP & Year-Over-Year % Change", style={"textAlign": "center"}),
        dcc.Graph(
            id='gdp-graph',
            figure={
                'data': [
                    go.Scatter(
                        x=gdp_data['date'],
                        y=gdp_data['value'],
                        mode='lines',
                        name='GDP (Billions of $)',
                        line=dict(color='blue', width=2)
                    ),
                    go.Scatter(
                        x=gdp_data['date'] if 'yoy_growth' in gdp_data.columns else [],
                        y=gdp_data['yoy_growth'] if 'yoy_growth' in gdp_data.columns else [],
                        mode='lines',
                        name='GDP Growth (% YoY)',
                        line=dict(color='red', width=2),
                        yaxis='y2'
                    )
                ],
                'layout': go.Layout(
                    xaxis={'title': 'Date'},
                    yaxis={'title': 'Billions of Dollars', 'side': 'left'},
                    yaxis2={
                        'title': 'Year-Over-Year % Change', 
                        'side': 'right', 
                        'overlaying': 'y',
                        'showgrid': False
                    },
                    legend={'x': 0, 'y': 1.1, 'orientation': 'h'},
                    margin={'l': 60, 'b': 50, 't': 30, 'r': 60}
                )
            }
        )
    ]),

    html.Div([
        html.H3("PPI: Software Publishers", style={"textAlign": "center"}),
        dcc.Graph(
            id='software-ppi-graph',
            figure={
                'data': [
                    go.Scatter(
                        x=software_ppi_data['date'],
                        y=software_ppi_data['value'],
                        mode='lines',
                        name='PPI Index',
                        line=dict(color='#0066cc', width=2)
                    ),
                    go.Scatter(
                        x=software_ppi_data['date'],
                        y=software_ppi_data['yoy_pct_change'] if 'yoy_pct_change' in software_ppi_data.columns else [],
                        mode='lines',
                        name='YoY % Change',
                        line=dict(color='#009933', width=2, dash='dot'),
                        yaxis='y2'
                    )
                ],
                'layout': go.Layout(
                    xaxis={'title': 'Date'},
                    yaxis={'title': 'Index Value (Dec 2009=100)'},
                    yaxis2={
                        'title': 'Year-Over-Year % Change', 
                        'side': 'right', 
                        'overlaying': 'y',
                        'showgrid': False
                    },
                    legend={'x': 0, 'y': 1.1, 'orientation': 'h'},
                    margin={'l': 60, 'b': 50, 't': 30, 'r': 60}
                )
            }
        )
    ]),

    html.Div([
        html.H3("PPI: Data Processing Services", style={"textAlign": "center"}),
        dcc.Graph(
            id='data-processing-ppi-graph',
            figure={
                'data': [
                    go.Scatter(
                        x=data_processing_ppi_data['date'],
                        y=data_processing_ppi_data['value'],
                        mode='lines',
                        name='PPI Index',
                        line=dict(color='#ff9900', width=2)
                    ),
                    go.Scatter(
                        x=data_processing_ppi_data['date'],
                        y=data_processing_ppi_data['yoy_pct_change'] if 'yoy_pct_change' in data_processing_ppi_data.columns else [],
                        mode='lines',
                        name='YoY % Change',
                        line=dict(color='#cc3300', width=2, dash='dot'),
                        yaxis='y2'
                    )
                ],
                'layout': go.Layout(
                    xaxis={'title': 'Date'},
                    yaxis={'title': 'Index Value (Dec 2009=100)'},
                    yaxis2={
                        'title': 'Year-Over-Year % Change', 
                        'side': 'right', 
                        'overlaying': 'y',
                        'showgrid': False
                    },
                    legend={'x': 0, 'y': 1.1, 'orientation': 'h'},
                    margin={'l': 60, 'b': 50, 't': 30, 'r': 60}
                )
            }
        )
    ]),

    html.Div([
        html.H3("Consumer Price Index (CPI) & Inflation Rate", style={"textAlign": "center"}),
        dcc.Graph(
            id='inflation-graph',
            figure={
                'data': [
                    go.Scatter(
                        x=inflation_data['date'],
                        y=inflation_data['value'],
                        mode='lines',
                        name='CPI Index',
                        line=dict(color='#cc6600', width=2)
                    ),
                    go.Scatter(
                        x=inflation_data['date'] if 'inflation' in inflation_data.columns else [],
                        y=inflation_data['inflation'] if 'inflation' in inflation_data.columns else [],
                        mode='lines',
                        name='Inflation Rate (%)',
                        line=dict(color='#ff3300', width=2, dash='dot'),
                        yaxis='y2'
                    )
                ],
                'layout': go.Layout(
                    xaxis={'title': 'Date'},
                    yaxis={'title': 'CPI (1982-84=100)'},
                    yaxis2={
                        'title': 'Inflation Rate (%)', 
                        'side': 'right', 
                        'overlaying': 'y',
                        'showgrid': False
                    },
                    legend={'x': 0, 'y': 1.1, 'orientation': 'h'},
                    margin={'l': 60, 'b': 50, 't': 30, 'r': 60}
                )
            }
        )
    ]),

    html.Div([
        html.H3("Unemployment Rate", style={"textAlign": "center"}),
        dcc.Graph(
            id='unemployment-graph',
            figure={
                'data': [
                    go.Scatter(
                        x=unemployment_data['date'],
                        y=unemployment_data['value'],
                        mode='lines',
                        name='Unemployment Rate (%)',
                        line=dict(color='#0099cc', width=2)
                    )
                ],
                'layout': go.Layout(
                    xaxis={'title': 'Date'},
                    yaxis={'title': 'Unemployment Rate (%)'},
                    margin={'l': 60, 'b': 50, 't': 30, 'r': 30}
                )
            }
        )
    ]),

    html.Div([
        html.H3("Federal Funds Rate", style={"textAlign": "center"}),
        dcc.Graph(
            id='interest-rate-graph',
            figure={
                'data': [
                    go.Scatter(
                        x=interest_rate_data['date'],
                        y=interest_rate_data['value'],
                        mode='lines',
                        name='Federal Funds Rate (%)',
                        line=dict(color='#009933', width=2)
                    )
                ],
                'layout': go.Layout(
                    xaxis={'title': 'Date'},
                    yaxis={'title': 'Rate (%)'},
                    margin={'l': 60, 'b': 50, 't': 30, 'r': 30}
                )
            }
        )
    ])
])

import webbrowser
import threading

def open_browser():
    webbrowser.open_new("http://127.0.0.1:8050/")

# Add these callbacks after the app layout definition

# Callback to update total weight display as sliders are adjusted
@app.callback(
    Output("total-weight-display", "children"),
    [
        Input("gdp-weight-slider", "value"),
        Input("unemployment-weight-slider", "value"),
        Input("inflation-weight-slider", "value"),
        Input("nasdaq-weight-slider", "value"),
        Input("tech-weight-slider", "value"),
        Input("software-ppi-weight-slider", "value"),
        Input("interest-rate-weight-slider", "value")
    ]
)
def update_total_weight(gdp_weight, unemployment_weight, inflation_weight, 
                        nasdaq_weight, tech_weight, software_ppi_weight,
                        interest_rate_weight):
    """Update the display of total weight as sliders change"""
    total_weight = (gdp_weight + unemployment_weight + inflation_weight + 
                   nasdaq_weight + tech_weight + software_ppi_weight + 
                   interest_rate_weight)
    
    color = "#28a745" if total_weight == 100 else "#dc3545"  # Green if 100%, red otherwise
    
    return html.Div([
        f"Total Weight: {total_weight}%",
        html.Div(
            "(Weights should sum to 100%)" if total_weight != 100 else "(✓ Weights sum to 100%)",
            style={"color": color, "fontSize": "12px", "marginTop": "5px"}
        )
    ])

# Callback to update sentiment index and components when Apply button is clicked
@app.callback(
    [
        Output("sentiment-category", "children"),
        Output("sentiment-score", "children"),
        Output("sentiment-gauge-indicator", "style"),
        Output("sentiment-components-table", "children")
    ],
    [Input("apply-weights-button", "n_clicks")],
    [
        State("gdp-weight-slider", "value"),
        State("unemployment-weight-slider", "value"),
        State("inflation-weight-slider", "value"),
        State("nasdaq-weight-slider", "value"),
        State("tech-weight-slider", "value"),
        State("software-ppi-weight-slider", "value"),
        State("interest-rate-weight-slider", "value")
    ]
)
def update_sentiment_index(n_clicks, gdp_weight, unemployment_weight, inflation_weight, 
                           nasdaq_weight, tech_weight, software_ppi_weight,
                           interest_rate_weight):
    """Calculate new sentiment index based on custom weights when Apply button is clicked"""
    if n_clicks is None:
        # Initial render, use default weights
        updated_sentiment = sentiment_index
    else:
        # Apply custom weights
        custom_weights = {
            'GDP % Change': gdp_weight,
            'Unemployment Rate': unemployment_weight,
            'CPI': inflation_weight,
            'NASDAQ Trend': nasdaq_weight,
            'PPI: Data Processing Services': tech_weight,
            'PPI: Software Publishers': software_ppi_weight,
            'Federal Funds Rate': interest_rate_weight
        }
        
        updated_sentiment = calculate_sentiment_index(custom_weights)
        
        # Handle case where sentiment index couldn't be calculated
        if updated_sentiment is None:
            updated_sentiment = {
                'score': 50,
                'category': 'Data Unavailable',
                'components': [],
                'available_weight_sum': 0
            }
    
    # Update gauge indicator position
    gauge_style = {
        "position": "absolute",
        "left": f"{updated_sentiment['score']}%",
        "top": "-10px",
        "width": "20px",
        "height": "50px",
        "background": "black",
        "transform": "translateX(-50%)"
    }
    
    # Format score display
    score_display = f"Score: {updated_sentiment['score']:.1f}/100"
    
    # Update component table
    table_header = html.Thead(
        html.Tr([
            html.Th("Indicator", style={"textAlign": "left", "padding": "8px"}),
            html.Th("Value", style={"textAlign": "right", "padding": "8px"}),
            html.Th("Weight", style={"textAlign": "right", "padding": "8px"}),
            html.Th("Contribution", style={"textAlign": "right", "padding": "8px"}),
            html.Th("Rating", style={"textAlign": "left", "padding": "8px"})
        ])
    )
    
    table_rows = []
    for comp in updated_sentiment['components']:
        table_rows.append(
            html.Tr([
                html.Td(comp['indicator'], style={"textAlign": "left", "padding": "8px"}),
                html.Td(f"{comp['value']:.2f}", style={"textAlign": "right", "padding": "8px"}),
                html.Td(f"{comp['weight']}%", style={"textAlign": "right", "padding": "8px"}),
                html.Td(f"{comp['contribution']:.1f}", style={"textAlign": "right", "padding": "8px"}),
                html.Td(
                    "●" * int(comp['score'] / 20), 
                    style={
                        "color": "#28a745" if comp['score'] >= 60 else "#dc3545",
                        "textAlign": "left", 
                        "padding": "8px"
                    }
                )
            ])
        )
    
    table_body = html.Tbody(table_rows)
    table_content = [table_header, table_body]
    
    return updated_sentiment['category'], score_display, gauge_style, table_content

if __name__ == '__main__':
    print("Starting Economic Dashboard: Software & Technology...")
    print(f"FRED API Key available: {'Yes' if FRED_API_KEY else 'No'}")
    print(f"BEA API Key available: {'Yes' if BEA_API_KEY else 'No'}")
    print(f"BLS API Key available: {'Yes' if BLS_API_KEY else 'No'}")

    threading.Timer(1.0, open_browser).start()
    app.run(debug=True, port=8050)