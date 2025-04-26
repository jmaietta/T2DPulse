"""
Fixed version of app.py with proper tooltip implementation
This addresses the '[' was never closed syntax error
"""

import pandas as pd
import numpy as np
import os
import json
import requests
from datetime import datetime, timedelta
import time
import dash
from dash import html, dcc
from dash.dependencies import Input, Output, State
from dash.exceptions import PreventUpdate
import plotly.graph_objs as go
import plotly.express as px
import yfinance as yf

# Custom helper modules
from chart_styling import color_maps, heuristics, get_threshold_colors
import market_insights
from pulse_card_implementation import create_pulse_card, update_sentiment_gauge, create_sector_summary

# Initialize the Dash app
app = dash.Dash(__name__, suppress_callback_exceptions=True)

# For Replit deployment
server = app.server

# Create app layout
app.layout = html.Div([
    # Interval component for auto-refreshing
    dcc.Interval(
        id='interval-component',
        interval=60*60*1000,  # refresh every hour
        n_intervals=0
    ),
    
    # Store components for app state
    dcc.Store(id='custom-weights-store', storage_type='session'),
    dcc.Store(id='proprietary-data-store', storage_type='session'),
    dcc.Store(id='document-data-store', storage_type='session'),
    
    # Main content
    html.Div([
        # App header with logo
        html.Div([
            html.Img(src="/assets/T2D Pulse logo.png", height="70px", 
                     style={"marginRight": "20px", "display": "inline-block", "verticalAlign": "middle"}),
            html.H1("Economic Dashboard", 
                 style={"display": "inline-block", "verticalAlign": "middle", "margin": "0", "color": "#2c3e50"})
        ], style={"textAlign": "center", "marginBottom": "20px", "marginTop": "20px"}),
        
        # Navigation tabs
        dcc.Tabs(id="tabs", value='tab-pulse', children=[
            dcc.Tab(label='T2D Pulse', value='tab-pulse', className='custom-tab', selected_className='custom-tab--selected'),
            dcc.Tab(label='Economic Indicators', value='tab-indicators', className='custom-tab', selected_className='custom-tab--selected'),
            dcc.Tab(label='Sector Analysis', value='tab-sectors', className='custom-tab', selected_className='custom-tab--selected'),
            dcc.Tab(label='Settings', value='tab-settings', className='custom-tab', selected_className='custom-tab--selected'),
        ], style={'marginBottom': '20px'}, colors={
            "border": "#ddd",
            "primary": "#3498db",
            "background": "#f9f9f9"
        }),
        
        # Content area for selected tab
        html.Div(id='tabs-content')
    ], className="container")
])

# Callback to update tabs content
@app.callback(Output('tabs-content', 'children'),
              [Input('tabs', 'value'),
               Input('custom-weights-store', 'data'),
               Input('proprietary-data-store', 'data'),
               Input('document-data-store', 'data')])
def render_content(tab, custom_weights, proprietary_data, document_data):
    if tab == 'tab-pulse':
        return html.Div([
            # Hidden divs to store scores
            html.Div(id='sentiment-score', style={'display': 'none'}),
            html.Div(id='sentiment-category', style={'display': 'none'}),
            
            # Pulse Score section
            html.Div([
                html.Div([
                    # Pulse card with sentiment gauge - this will use our tooltip implementation
                    html.Div(id='sentiment-gauge', className='pulse-card-container')
                ], className='col-md-6'),
                
                html.Div([
                    # Sector summary
                    html.Div(id='sector-summary', className='sector-summary-container')
                ], className='col-md-6')
            ], className='row'),
            
            # Methodology section
            html.Div([
                html.H3("T2D Pulse Methodology", className="section-title",
                       style={"textAlign": "center", "marginTop": "20px", "color": "#2c3e50"}),
                html.P([
                    "The T2D Pulse Score is a weighted average of multiple economic indicators ",
                    "and sector sentiment scores, calibrated specifically for the technology industry. ",
                    "The score ranges from 0-100, with higher scores indicating more favorable conditions."
                ], style={"textAlign": "center", "maxWidth": "800px", "margin": "0 auto"}),
                
                # Components display
                html.H4("Component Contribution", style={"textAlign": "center", "marginTop": "20px"}),
                html.Div(id="sentiment-components", className="components-container")
            ], className='methodology-section')
        ])
    
    elif tab == 'tab-indicators':
        return html.Div([
            # Economic indicators tab content
            html.Div([
                # Row 1 - GDP and PCE
                html.Div([
                    html.Div([
                        html.H3("Real GDP", className="chart-title"),
                        html.Div(id="gdp-trend", className="trend-container"),
                        html.Div(id="gdp-container", className="chart-container")
                    ], className="col-md-6"),
                    html.Div([
                        html.H3("Personal Consumption Expenditures", className="chart-title"),
                        html.Div(id="pce-trend", className="trend-container"),
                        html.Div(id="pce-container", className="chart-container")
                    ], className="col-md-6")
                ], className="row chart-row"),
                
                # Row 2 - Unemployment and Job Postings
                html.Div([
                    html.Div([
                        html.H3("Unemployment Rate", className="chart-title"),
                        html.Div(id="unemployment-trend", className="trend-container"),
                        html.Div(id="unemployment-container", className="chart-container")
                    ], className="col-md-6"),
                    html.Div([
                        html.H3("Software Job Postings", className="chart-title"),
                        html.Div(id="job-postings-trend", className="trend-container"),
                        html.Div(id="job-postings-container", className="chart-container")
                    ], className="col-md-6")
                ], className="row chart-row"),
                
                # Row 3 - Inflation and PCEPI
                html.Div([
                    html.Div([
                        html.H3("Consumer Price Index", className="chart-title"),
                        html.Div(id="inflation-trend", className="trend-container"),
                        html.Div(id="inflation-container", className="chart-container")
                    ], className="col-md-6"),
                    html.Div([
                        html.H3("PCE Price Index", className="chart-title"),
                        html.Div(id="pcepi-trend", className="trend-container"),
                        html.Div(id="pcepi-container", className="chart-container")
                    ], className="col-md-6")
                ], className="row chart-row"),
                
                # Row 4 - Interest Rates and NASDAQ
                html.Div([
                    html.Div([
                        html.H3("Federal Funds Rate", className="chart-title"),
                        html.Div(id="interest-rate-trend", className="trend-container"),
                        html.Div(id="interest-rate-container", className="chart-container")
                    ], className="col-md-6"),
                    html.Div([
                        html.H3("NASDAQ Composite", className="chart-title"),
                        html.Div(id="nasdaq-trend", className="trend-container"),
                        html.Div(id="nasdaq-container", className="chart-container")
                    ], className="col-md-6")
                ], className="row chart-row"),
                
                # Row 5 - PPI groups
                html.Div([
                    html.Div([
                        html.H3("Software Publishers PPI", className="chart-title"),
                        html.Div(id="software-ppi-trend", className="trend-container"),
                        html.Div(id="software-ppi-container", className="chart-container")
                    ], className="col-md-6"),
                    html.Div([
                        html.H3("Data Processing Services PPI", className="chart-title"),
                        html.Div(id="data-ppi-trend", className="trend-container"),
                        html.Div(id="data-ppi-container", className="chart-container")
                    ], className="col-md-6")
                ], className="row chart-row"),
                
                # Row 6 - Treasury Yield and VIX
                html.Div([
                    html.Div([
                        html.H3("10-Year Treasury Yield", className="chart-title"),
                        html.Div(id="treasury-yield-trend", className="trend-container"),
                        html.Div(id="treasury-yield-container", className="chart-container")
                    ], className="col-md-6"),
                    html.Div([
                        html.H3("VIX Volatility Index", className="chart-title"),
                        html.Div(id="vix-trend", className="trend-container"),
                        html.Div(id="vix-container", className="chart-container")
                    ], className="col-md-6")
                ], className="row chart-row"),
                
                # Row 7 - Consumer Sentiment
                html.Div([
                    html.Div([
                        html.H3("Consumer Sentiment", className="chart-title"),
                        html.Div(id="consumer-sentiment-trend", className="trend-container"),
                        html.Div(id="consumer-sentiment-container", className="chart-container")
                    ], className="col-md-6")
                ], className="row chart-row")
            ])
        ])
    
    elif tab == 'tab-sectors':
        return html.Div([
            # Sector sentiment tab content
            html.Div([
                html.H3("Technology Sector Sentiment", 
                        style={"textAlign": "center", "marginBottom": "20px"}),
                
                html.P([
                    "Sentiment scores for tech industry sectors, assessed on a 0-100 scale. ",
                    "Scores above 60 indicate bullish outlook, 30-60 neutral, and below 30 bearish."
                ], style={"textAlign": "center", "maxWidth": "800px", "margin": "0 auto 20px"}),
                
                # Sector cards
                html.Div(id="sector-sentiment-container", className="sector-container")
            ])
        ])
    
    elif tab == 'tab-settings':
        return html.Div([
            # Settings tab content
            html.Div([
                html.H3("Customize Indicator Weights", 
                       style={"textAlign": "center", "marginBottom": "20px", "color": "#2c3e50"}),
                
                html.P([
                    "Adjust the influence of each indicator on the overall T2D Pulse score. ",
                    "The weights will be normalized to sum to 100%."
                ], style={"textAlign": "center", "marginBottom": "20px"}),
                
                # Weight adjustment controls
                html.Div([
                    html.Div([
                        html.Div([
                            html.Div("Economic Indicators", className="weight-section-title"),
                            
                            # GDP
                            html.Div([
                                html.Div("Real GDP:", style={"width": "50%", "display": "inline-block"}),
                                html.Div([
                                    html.Button("−", id="decrease-gdp", n_clicks=0, className="weight-btn"),
                                    html.Div(id="gdp-weight-display", className="weight-display"),
                                    html.Button("+", id="increase-gdp", n_clicks=0, className="weight-btn")
                                ], style={"width": "50%", "display": "inline-block", "textAlign": "right"})
                            ], className="weight-control-row"),
                            
                            # PCE
                            html.Div([
                                html.Div("PCE:", style={"width": "50%", "display": "inline-block"}),
                                html.Div([
                                    html.Button("−", id="decrease-pce", n_clicks=0, className="weight-btn"),
                                    html.Div(id="pce-weight-display", className="weight-display"),
                                    html.Button("+", id="increase-pce", n_clicks=0, className="weight-btn")
                                ], style={"width": "50%", "display": "inline-block", "textAlign": "right"})
                            ], className="weight-control-row"),
                            
                            # Unemployment
                            html.Div([
                                html.Div("Unemployment:", style={"width": "50%", "display": "inline-block"}),
                                html.Div([
                                    html.Button("−", id="decrease-unemployment", n_clicks=0, className="weight-btn"),
                                    html.Div(id="unemployment-weight-display", className="weight-display"),
                                    html.Button("+", id="increase-unemployment", n_clicks=0, className="weight-btn")
                                ], style={"width": "50%", "display": "inline-block", "textAlign": "right"})
                            ], className="weight-control-row"),
                            
                            # Job Postings
                            html.Div([
                                html.Div("Job Postings:", style={"width": "50%", "display": "inline-block"}),
                                html.Div([
                                    html.Button("−", id="decrease-job-postings", n_clicks=0, className="weight-btn"),
                                    html.Div(id="job-postings-weight-display", className="weight-display"),
                                    html.Button("+", id="increase-job-postings", n_clicks=0, className="weight-btn")
                                ], style={"width": "50%", "display": "inline-block", "textAlign": "right"})
                            ], className="weight-control-row"),
                            
                            # CPI
                            html.Div([
                                html.Div("CPI Inflation:", style={"width": "50%", "display": "inline-block"}),
                                html.Div([
                                    html.Button("−", id="decrease-cpi", n_clicks=0, className="weight-btn"),
                                    html.Div(id="cpi-weight-display", className="weight-display"),
                                    html.Button("+", id="increase-cpi", n_clicks=0, className="weight-btn")
                                ], style={"width": "50%", "display": "inline-block", "textAlign": "right"})
                            ], className="weight-control-row"),
                            
                            # PCEPI
                            html.Div([
                                html.Div("PCE Price Index:", style={"width": "50%", "display": "inline-block"}),
                                html.Div([
                                    html.Button("−", id="decrease-pcepi", n_clicks=0, className="weight-btn"),
                                    html.Div(id="pcepi-weight-display", className="weight-display"),
                                    html.Button("+", id="increase-pcepi", n_clicks=0, className="weight-btn")
                                ], style={"width": "50%", "display": "inline-block", "textAlign": "right"})
                            ], className="weight-control-row"),
                        ], className="weight-section col-md-6")
                    ]),
                    
                    html.Div([
                        html.Div([
                            html.Div("Market Indicators", className="weight-section-title"),
                            
                            # NASDAQ
                            html.Div([
                                html.Div("NASDAQ:", style={"width": "50%", "display": "inline-block"}),
                                html.Div([
                                    html.Button("−", id="decrease-nasdaq", n_clicks=0, className="weight-btn"),
                                    html.Div(id="nasdaq-weight-display", className="weight-display"),
                                    html.Button("+", id="increase-nasdaq", n_clicks=0, className="weight-btn")
                                ], style={"width": "50%", "display": "inline-block", "textAlign": "right"})
                            ], className="weight-control-row"),
                            
                            # Treasury Yield
                            html.Div([
                                html.Div("10Y Treasury:", style={"width": "50%", "display": "inline-block"}),
                                html.Div([
                                    html.Button("−", id="decrease-treasury-yield", n_clicks=0, className="weight-btn"),
                                    html.Div(id="treasury-yield-weight-display", className="weight-display"),
                                    html.Button("+", id="increase-treasury-yield", n_clicks=0, className="weight-btn")
                                ], style={"width": "50%", "display": "inline-block", "textAlign": "right"})
                            ], className="weight-control-row"),
                            
                            # VIX
                            html.Div([
                                html.Div("VIX:", style={"width": "50%", "display": "inline-block"}),
                                html.Div([
                                    html.Button("−", id="decrease-vix", n_clicks=0, className="weight-btn"),
                                    html.Div(id="vix-weight-display", className="weight-display"),
                                    html.Button("+", id="increase-vix", n_clicks=0, className="weight-btn")
                                ], style={"width": "50%", "display": "inline-block", "textAlign": "right"})
                            ], className="weight-control-row"),
                            
                            # Software PPI
                            html.Div([
                                html.Div("Software PPI:", style={"width": "50%", "display": "inline-block"}),
                                html.Div([
                                    html.Button("−", id="decrease-software-ppi", n_clicks=0, className="weight-btn"),
                                    html.Div(id="software-ppi-weight-display", className="weight-display"),
                                    html.Button("+", id="increase-software-ppi", n_clicks=0, className="weight-btn")
                                ], style={"width": "50%", "display": "inline-block", "textAlign": "right"})
                            ], className="weight-control-row"),
                            
                            # Data PPI
                            html.Div([
                                html.Div("Data PPI:", style={"width": "50%", "display": "inline-block"}),
                                html.Div([
                                    html.Button("−", id="decrease-data-ppi", n_clicks=0, className="weight-btn"),
                                    html.Div(id="data-ppi-weight-display", className="weight-display"),
                                    html.Button("+", id="increase-data-ppi", n_clicks=0, className="weight-btn")
                                ], style={"width": "50%", "display": "inline-block", "textAlign": "right"})
                            ], className="weight-control-row"),
                            
                            # Interest Rate
                            html.Div([
                                html.Div("Fed Funds Rate:", style={"width": "50%", "display": "inline-block"}),
                                html.Div([
                                    html.Button("−", id="decrease-interest-rate", n_clicks=0, className="weight-btn"),
                                    html.Div(id="interest-rate-weight-display", className="weight-display"),
                                    html.Button("+", id="increase-interest-rate", n_clicks=0, className="weight-btn")
                                ], style={"width": "50%", "display": "inline-block", "textAlign": "right"})
                            ], className="weight-control-row"),
                            
                            # Consumer Sentiment
                            html.Div([
                                html.Div("Consumer Sentiment:", style={"width": "50%", "display": "inline-block"}),
                                html.Div([
                                    html.Button("−", id="decrease-consumer-sentiment", n_clicks=0, className="weight-btn"),
                                    html.Div(id="consumer-sentiment-weight-display", className="weight-display"),
                                    html.Button("+", id="increase-consumer-sentiment", n_clicks=0, className="weight-btn")
                                ], style={"width": "50%", "display": "inline-block", "textAlign": "right"})
                            ], className="weight-control-row"),
                        ], className="weight-section col-md-6")
                    ])
                ], className="row weight-controls-container"),
                
                # Total weight
                html.Div([
                    html.Div("Total Weight:", style={"width": "50%", "display": "inline-block", "fontWeight": "bold"}),
                    html.Div(id="total-weight-display", style={"width": "50%", "display": "inline-block", "textAlign": "right", "fontWeight": "bold"})
                ], className="total-weight-row"),
                
                # Apply and Reset buttons
                html.Div([
                    html.Button("Apply Custom Weights", id="apply-weights-button", n_clicks=0, className="action-button"),
                    html.Button("Reset to Defaults", id="reset-weights-button", n_clicks=0, className="action-button cancel-button")
                ], style={"marginTop": "20px", "textAlign": "center"}),
                
                # Document analysis section
                html.Div([
                    html.H3("Document Analysis", style={"textAlign": "center", "marginTop": "30px", "color": "#2c3e50"}),
                    html.P([
                        "Upload earnings reports, financial documents, or other text for sentiment analysis. ",
                        "Results will be incorporated into the T2D Pulse score."
                    ], style={"textAlign": "center"}),
                    
                    html.Div([
                        # Document upload control
                        html.Div([
                            dcc.Upload(
                                id='document-upload',
                                children=html.Div([
                                    'Drag and Drop or ',
                                    html.A('Select Document', style={"color": "#3498db"})
                                ]),
                                style={
                                    'width': '100%',
                                    'height': '60px',
                                    'lineHeight': '60px',
                                    'borderWidth': '1px',
                                    'borderStyle': 'dashed',
                                    'borderRadius': '5px',
                                    'textAlign': 'center',
                                    'margin': '10px 0'
                                },
                                multiple=False
                            ),
                            
                            # Document preview
                            html.Div(id='document-preview', style={"marginTop": "10px", "marginBottom": "10px"}),
                            
                            # Document analysis controls
                            html.Div([
                                html.Div([
                                    html.Div("Document Weight:", style={"width": "120px", "display": "inline-block"}),
                                    dcc.Slider(
                                        id='document-weight-slider',
                                        min=0,
                                        max=50,
                                        step=5,
                                        value=10,
                                        marks={
                                            0: '0%',
                                            10: '10%',
                                            20: '20%',
                                            30: '30%',
                                            40: '40%',
                                            50: '50%'
                                        }
                                    ),
                                ], style={"marginBottom": "15px"}),
                                
                                html.Div(id='document-weight-display', style={"margin": "10px 0", "fontWeight": "bold"}),
                                
                                html.Button("Apply Document Analysis", id="apply-document-button", n_clicks=0, 
                                          className="action-button", style={"width": "100%"})
                            ], id="document-analysis-controls", style={"marginTop": "15px"})
                        ], className="col-md-6"),
                    ], className="row")
                ], className="document-analysis-section")
            ], className="settings-container")
        ])

# Function to calculate economic sentiment index
def calculate_sentiment_index(custom_weights=None, proprietary_data=None, document_data=None):
    """Calculate economic sentiment index from available indicators
    
    Args:
        custom_weights (dict, optional): Dictionary with custom weights for each indicator
        proprietary_data (dict, optional): Dictionary with proprietary data and its weight
        document_data (dict, optional): Dictionary with document analysis data and its weight
    """
    # Default weights (totaling 100%)
    default_weights = {
        'gdp': 12.0,  # Real GDP Growth
        'pce': 8.0,   # Personal Consumption Expenditures
        'unemployment': 10.0,  # Unemployment Rate
        'cpi': 8.0,   # Consumer Price Index (inflation)
        'pcepi': 7.0, # PCE Price Index
        'nasdaq': 10.0,  # NASDAQ Composite
        'data_ppi': 6.0,  # Data Processing Services PPI
        'software_ppi': 6.0,  # Software Publishers PPI
        'interest_rate': 8.0,  # Federal Funds Rate
        'treasury_yield': 8.0,  # 10-Year Treasury Yield
        'vix': 8.0,   # VIX Volatility Index
        'consumer_sentiment': 9.0,  # Consumer Sentiment
        'job_postings': 0.0,  # Software job postings (added feature)
    }
    
    # Check if default weights sum to 100%
    default_sum = sum(default_weights.values())
    if abs(default_sum - 100.0) > 0.1:  # Allow for small rounding errors
        print(f"WARNING: Default weights sum to {default_sum}%, not 100%")
        # Adjust largest weight to make total exactly 100%
        largest_key = max(default_weights, key=default_weights.get)
        adjustment = 100.0 - default_sum
        default_weights[largest_key] += adjustment
        print(f"Adjusted {largest_key} by {adjustment} to make total exactly 100%")
    
    # Use custom weights if provided
    weights = custom_weights if custom_weights else default_weights
    
    # Normalize weights to account for proprietary data and document analysis
    total_standard_weight = 100.0
    proprietary_weight = 0
    document_weight = 0
    
    if proprietary_data and 'weight' in proprietary_data:
        proprietary_weight = proprietary_data['weight']
    
    if document_data and 'weight' in document_data:
        document_weight = document_data['weight']
    
    # Calculate the scaling factor for standard weights
    total_weight = total_standard_weight + proprietary_weight + document_weight
    scaling_factor = total_standard_weight / total_weight
    
    # Scale all standard weights
    for key in weights:
        weights[key] *= scaling_factor
        
    # Collect all component data and scores
    components = []
    
    # Calculate scores for economic indicators
    
    # 1. GDP growth
    gdp_score = 0
    if not gdp_data.empty and 'yoy_growth' in gdp_data.columns:
        latest_gdp = gdp_data.sort_values('date', ascending=False).iloc[0]
        gdp_value = latest_gdp['yoy_growth']
        
        # Score GDP growth on a 0-100 scale
        # Typical range: -2% to 4% annually
        if gdp_value >= 4.0:
            gdp_score = 100
        elif gdp_value >= 2.0:
            gdp_score = 60 + (gdp_value - 2.0) * 20  # 60-100 for 2-4%
        elif gdp_value >= 0:
            gdp_score = 30 + gdp_value * 15  # 30-60 for 0-2%
        elif gdp_value >= -4.0:
            gdp_score = max(0, 30 + gdp_value * 7.5)  # 0-30 for -4 to 0%
        else:
            gdp_score = 0
            
        components.append({
            'indicator': 'Real GDP % Change',
            'value': gdp_value,
            'score': gdp_score,
            'weight': weights['gdp'],
            'contribution': gdp_score * weights['gdp'] / 100
        })
    
    # 2. PCE growth
    pce_score = 0
    if not pce_data.empty and 'yoy_growth' in pce_data.columns:
        latest_pce = pce_data.sort_values('date', ascending=False).iloc[0]
        pce_value = latest_pce['yoy_growth']
        
        # Score PCE growth on a 0-100 scale
        # Typical range: -2% to 6% annually
        if pce_value >= 6.0:
            pce_score = 100
        elif pce_value >= 3.0:
            pce_score = 60 + (pce_value - 3.0) * 13.33  # 60-100 for 3-6%
        elif pce_value >= 1.0:
            pce_score = 30 + (pce_value - 1.0) * 15  # 30-60 for 1-3%
        elif pce_value >= -2.0:
            pce_score = max(0, 10 + (pce_value + 2.0) * 6.67)  # 10-30 for -2 to 1%
        else:
            pce_score = 0
            
        components.append({
            'indicator': 'PCE',
            'value': pce_value,
            'score': pce_score,
            'weight': weights['pce'],
            'contribution': pce_score * weights['pce'] / 100
        })
    
    # 3. Unemployment rate
    unemployment_score = 0
    if not unemployment_data.empty:
        latest_unemployment = unemployment_data.sort_values('date', ascending=False).iloc[0]
        unemployment_value = latest_unemployment['value']
        
        # Score unemployment on a 0-100 scale (inverse: lower is better)
        # Typical range: 3% to 10%
        if unemployment_value <= 3.5:
            unemployment_score = 100
        elif unemployment_value <= 5.0:
            unemployment_score = 60 + (5.0 - unemployment_value) * 26.67  # 60-100 for 5-3.5%
        elif unemployment_value <= 7.0:
            unemployment_score = 30 + (7.0 - unemployment_value) * 15  # 30-60 for 7-5%
        elif unemployment_value <= 10.0:
            unemployment_score = max(0, 30 - (unemployment_value - 7.0) * 10)  # 0-30 for 7-10%
        else:
            unemployment_score = 0
            
        components.append({
            'indicator': 'Unemployment Rate',
            'value': unemployment_value,
            'score': unemployment_score,
            'weight': weights['unemployment'],
            'contribution': unemployment_score * weights['unemployment'] / 100
        })
    
    # 4. CPI (Inflation)
    inflation_score = 0
    if not inflation_data.empty and 'inflation' in inflation_data.columns:
        latest_inflation = inflation_data.sort_values('date', ascending=False).iloc[0]
        inflation_value = latest_inflation['inflation']
        
        # Score inflation on a 0-100 scale (inverse: lower is better, but deflation is also bad)
        # Ideal range around 2%, poor above 4% or below 0%
        if 1.8 <= inflation_value <= 2.2:
            inflation_score = 100  # Ideal at ~2%
        elif 1.0 <= inflation_value < 1.8:
            inflation_score = 80 + (inflation_value - 1.0) * 25  # 80-100 for 1-1.8%
        elif 2.2 < inflation_value <= 3.0:
            inflation_score = 60 + (3.0 - inflation_value) * 50  # 60-100 for 3-2.2%
        elif 0 <= inflation_value < 1.0:
            inflation_score = 50 + inflation_value * 30  # 50-80 for 0-1%
        elif 3.0 < inflation_value <= 5.0:
            inflation_score = 20 + (5.0 - inflation_value) * 20  # 20-60 for 5-3%
        elif -1.0 <= inflation_value < 0:
            inflation_score = max(20, 50 + inflation_value * 30)  # 20-50 for -1-0%
        elif inflation_value > 5.0:
            inflation_score = max(0, 20 - (inflation_value - 5.0) * 5)  # 0-20 for >5%
        else:
            inflation_score = max(0, 20 + (inflation_value + 1.0) * 20)  # 0-20 for <-1%
            
        components.append({
            'indicator': 'CPI',
            'value': inflation_value,
            'score': inflation_score,
            'weight': weights['cpi'],
            'contribution': inflation_score * weights['cpi'] / 100
        })
    
    # 5. PCEPI (PCE Price Index)
    pcepi_score = 0
    if not pcepi_data.empty and 'yoy_growth' in pcepi_data.columns:
        latest_pcepi = pcepi_data.sort_values('date', ascending=False).iloc[0]
        pcepi_value = latest_pcepi['yoy_growth']
        
        # Score PCEPI on a 0-100 scale (inverse: lower is better, but deflation is also bad)
        # Ideal range around 2%, poor above 4% or below 0%
        if 1.8 <= pcepi_value <= 2.2:
            pcepi_score = 100  # Ideal at ~2%
        elif 1.0 <= pcepi_value < 1.8:
            pcepi_score = 80 + (pcepi_value - 1.0) * 25  # 80-100 for 1-1.8%
        elif 2.2 < pcepi_value <= 3.0:
            pcepi_score = 60 + (3.0 - pcepi_value) * 50  # 60-100 for 3-2.2%
        elif 0 <= pcepi_value < 1.0:
            pcepi_score = 50 + pcepi_value * 30  # 50-80 for 0-1%
        elif 3.0 < pcepi_value <= 5.0:
            pcepi_score = 20 + (5.0 - pcepi_value) * 20  # 20-60 for 5-3%
        elif -1.0 <= pcepi_value < 0:
            pcepi_score = max(20, 50 + pcepi_value * 30)  # 20-50 for -1-0%
        elif pcepi_value > 5.0:
            pcepi_score = max(0, 20 - (pcepi_value - 5.0) * 5)  # 0-20 for >5%
        else:
            pcepi_score = max(0, 20 + (pcepi_value + 1.0) * 20)  # 0-20 for <-1%
            
        components.append({
            'indicator': 'PCEPI',
            'value': pcepi_value,
            'score': pcepi_score,
            'weight': weights['pcepi'],
            'contribution': pcepi_score * weights['pcepi'] / 100
        })
    
    # 6. NASDAQ Composite
    nasdaq_score = 0
    if not nasdaq_data.empty:
        # If we have EMA data, use the gap as a momentum indicator
        if 'gap_pct' in nasdaq_data.columns:
            latest_nasdaq = nasdaq_data.sort_values('date', ascending=False).iloc[0]
            nasdaq_gap = latest_nasdaq['gap_pct']
            
            # Score NASDAQ momentum on a 0-100 scale
            # Typical range: -15% to +15% from EMA
            if nasdaq_gap >= 15:
                nasdaq_score = 100
            elif nasdaq_gap >= 5:
                nasdaq_score = 60 + (nasdaq_gap - 5) * 4  # 60-100 for 5-15%
            elif nasdaq_gap >= -5:
                nasdaq_score = 30 + (nasdaq_gap + 5) * 6  # 30-60 for -5-5%
            elif nasdaq_gap >= -15:
                nasdaq_score = max(0, 30 + (nasdaq_gap + 5) * 3)  # 0-30 for -15--5%
            else:
                nasdaq_score = 0
                
            components.append({
                'indicator': 'NASDAQ Trend',
                'value': nasdaq_gap,
                'score': nasdaq_score,
                'weight': weights['nasdaq'],
                'contribution': nasdaq_score * weights['nasdaq'] / 100
            })
            
        # Fallback if we don't have EMA: use year-over-year change
        elif 'year_ago_value' in nasdaq_data.columns:
            latest_nasdaq = nasdaq_data.sort_values('date', ascending=False).iloc[0]
            current_value = latest_nasdaq['value']
            year_ago_value = latest_nasdaq['year_ago_value']
            
            # Calculate year-over-year change
            if year_ago_value > 0:  # Avoid division by zero
                yoy_change = (current_value - year_ago_value) / year_ago_value * 100
            else:
                yoy_change = 0
                
            # Score NASDAQ annual change on a 0-100 scale
            # Typical range: -30% to +30% annually
            if yoy_change >= 30:
                nasdaq_score = 100
            elif yoy_change >= 10:
                nasdaq_score = 60 + (yoy_change - 10) * 2  # 60-100 for 10-30%
            elif yoy_change >= -10:
                nasdaq_score = 30 + (yoy_change + 10) * 3  # 30-60 for -10-10%
            elif yoy_change >= -30:
                nasdaq_score = max(0, 30 + (yoy_change + 10) * 1.5)  # 0-30 for -30--10%
            else:
                nasdaq_score = 0
                
            components.append({
                'indicator': 'NASDAQ Trend',
                'value': yoy_change,
                'score': nasdaq_score,
                'weight': weights['nasdaq'],
                'contribution': nasdaq_score * weights['nasdaq'] / 100
            })
    
    # 7. Data Processing Services PPI
    data_ppi_score = 0
    if not data_processing_ppi_data.empty and 'yoy_pct_change' in data_processing_ppi_data.columns:
        latest_data_ppi = data_processing_ppi_data.sort_values('date', ascending=False).iloc[0]
        data_ppi_value = latest_data_ppi['yoy_pct_change']
        
        # Score Data PPI on a 0-100 scale
        # For tech sector, moderate price growth is positive
        if 2.0 <= data_ppi_value <= 5.0:
            data_ppi_score = 80 + (data_ppi_value - 2.0) * 6.67  # 80-100 for 2-5%
        elif 0.0 <= data_ppi_value < 2.0:
            data_ppi_score = 60 + data_ppi_value * 10  # 60-80 for 0-2%
        elif 5.0 < data_ppi_value <= 8.0:
            data_ppi_score = max(50, 80 - (data_ppi_value - 5.0) * 10)  # 50-80 for 5-8%
        elif -2.0 <= data_ppi_value < 0.0:
            data_ppi_score = 40 + (data_ppi_value + 2.0) * 10  # 40-60 for -2-0%
        elif 8.0 < data_ppi_value <= 12.0:
            data_ppi_score = max(20, 50 - (data_ppi_value - 8.0) * 7.5)  # 20-50 for 8-12%
        elif -5.0 <= data_ppi_value < -2.0:
            data_ppi_score = max(10, 40 + (data_ppi_value + 5.0) * 10)  # 10-40 for -5--2%
        elif data_ppi_value > 12.0:
            data_ppi_score = max(0, 20 - (data_ppi_value - 12.0) * 5)  # 0-20 for >12%
        else:
            data_ppi_score = max(0, 10 + (data_ppi_value + 5.0) * 2)  # 0-10 for <-5%
            
        components.append({
            'indicator': 'Data Processing PPI',
            'value': data_ppi_value,
            'score': data_ppi_score,
            'weight': weights['data_ppi'],
            'contribution': data_ppi_score * weights['data_ppi'] / 100
        })
    
    # 8. Software Publishers PPI
    software_ppi_score = 0
    if not software_ppi_data.empty and 'yoy_pct_change' in software_ppi_data.columns:
        latest_software_ppi = software_ppi_data.sort_values('date', ascending=False).iloc[0]
        software_ppi_value = latest_software_ppi['yoy_pct_change']
        
        # Score Software PPI on a 0-100 scale
        # For tech sector, moderate price growth is positive
        if 2.0 <= software_ppi_value <= 5.0:
            software_ppi_score = 80 + (software_ppi_value - 2.0) * 6.67  # 80-100 for 2-5%
        elif 0.0 <= software_ppi_value < 2.0:
            software_ppi_score = 60 + software_ppi_value * 10  # 60-80 for 0-2%
        elif 5.0 < software_ppi_value <= 8.0:
            software_ppi_score = max(50, 80 - (software_ppi_value - 5.0) * 10)  # 50-80 for 5-8%
        elif -2.0 <= software_ppi_value < 0.0:
            software_ppi_score = 40 + (software_ppi_value + 2.0) * 10  # 40-60 for -2-0%
        elif 8.0 < software_ppi_value <= 12.0:
            software_ppi_score = max(20, 50 - (software_ppi_value - 8.0) * 7.5)  # 20-50 for 8-12%
        elif -5.0 <= software_ppi_value < -2.0:
            software_ppi_score = max(10, 40 + (software_ppi_value + 5.0) * 10)  # 10-40 for -5--2%
        elif software_ppi_value > 12.0:
            software_ppi_score = max(0, 20 - (software_ppi_value - 12.0) * 5)  # 0-20 for >12%
        else:
            software_ppi_score = max(0, 10 + (software_ppi_value + 5.0) * 2)  # 0-10 for <-5%
            
        components.append({
            'indicator': 'Software Publishers PPI',
            'value': software_ppi_value,
            'score': software_ppi_score,
            'weight': weights['software_ppi'],
            'contribution': software_ppi_score * weights['software_ppi'] / 100
        })
    
    # 9. Federal Funds Rate
    interest_rate_score = 0
    if not interest_rate_data.empty:
        latest_interest_rate = interest_rate_data.sort_values('date', ascending=False).iloc[0]
        interest_rate_value = latest_interest_rate['value']
        
        # Score Federal Funds Rate on a 0-100 scale
        # This needs to account for both absolute level and recent changes
        
        # For the tech sector specifically, moderate rates (2-4%) are generally good
        # Very low rates (0-1%) can indicate economic trouble
        # High rates (>6%) restrict growth and investment
        if 2.0 <= interest_rate_value <= 4.0:
            interest_rate_score = 70 + (4.0 - interest_rate_value) * 15  # 70-100 for 2-4%
        elif 1.0 <= interest_rate_value < 2.0:
            interest_rate_score = 50 + (interest_rate_value - 1.0) * 20  # 50-70 for 1-2%
        elif 4.0 < interest_rate_value <= 5.5:
            interest_rate_score = 40 + (5.5 - interest_rate_value) * 20  # 40-70 for 4-5.5%
        elif 0.0 <= interest_rate_value < 1.0:
            interest_rate_score = 30 + interest_rate_value * 20  # 30-50 for 0-1%
        elif 5.5 < interest_rate_value <= 7.0:
            interest_rate_score = max(10, 40 - (interest_rate_value - 5.5) * 20)  # 10-40 for 5.5-7%
        else:
            interest_rate_score = max(0, 10 - (interest_rate_value - 7.0) * 5)  # 0-10 for >7%
            
        components.append({
            'indicator': 'Federal Funds Rate',
            'value': interest_rate_value,
            'score': interest_rate_score,
            'weight': weights['interest_rate'],
            'contribution': interest_rate_score * weights['interest_rate'] / 100
        })
    
    # 10. 10-Year Treasury Yield
    treasury_yield_score = 0
    if not treasury_yield_data.empty:
        latest_treasury_yield = treasury_yield_data.sort_values('date', ascending=False).iloc[0]
        treasury_yield_value = latest_treasury_yield['value']
        
        # Score Treasury Yield on a 0-100 scale
        # Optimal range for tech is around 2.5-4.5%
        if 2.5 <= treasury_yield_value <= 4.5:
            treasury_yield_score = 60 + (4.5 - treasury_yield_value) * 20  # 60-100 for 2.5-4.5%
        elif 1.5 <= treasury_yield_value < 2.5:
            treasury_yield_score = 40 + (treasury_yield_value - 1.5) * 20  # 40-60 for 1.5-2.5%
        elif 4.5 < treasury_yield_value <= 6.0:
            treasury_yield_score = max(20, 60 - (treasury_yield_value - 4.5) * 26.67)  # 20-60 for 4.5-6%
        elif 0.5 <= treasury_yield_value < 1.5:
            treasury_yield_score = 20 + (treasury_yield_value - 0.5) * 20  # 20-40 for 0.5-1.5%
        elif 6.0 < treasury_yield_value <= 8.0:
            treasury_yield_score = max(0, 20 - (treasury_yield_value - 6.0) * 10)  # 0-20 for 6-8%
        elif 0.0 <= treasury_yield_value < 0.5:
            treasury_yield_score = max(0, 20 * treasury_yield_value / 0.5)  # 0-20 for 0-0.5%
        else:
            treasury_yield_score = 0  # >8%
            
        components.append({
            'indicator': '10-Year Treasury Yield',
            'value': treasury_yield_value,
            'score': treasury_yield_score,
            'weight': weights['treasury_yield'],
            'contribution': treasury_yield_score * weights['treasury_yield'] / 100
        })
    
    # 11. VIX Volatility Index
    vix_score = 0
    if not vix_data.empty:
        latest_vix = vix_data.sort_values('date', ascending=False).iloc[0]
        vix_value = latest_vix['value']
        
        # Score VIX on a 0-100 scale (inverse: lower is better)
        # Typical range: 10-40, with occasional spikes above 40
        if vix_value <= 15:
            vix_score = 100
        elif vix_value <= 20:
            vix_score = 80 + (20 - vix_value) * 4  # 80-100 for 15-20
        elif vix_value <= 25:
            vix_score = 60 + (25 - vix_value) * 4  # 60-80 for 20-25
        elif vix_value <= 30:
            vix_score = 40 + (30 - vix_value) * 4  # 40-60 for 25-30
        elif vix_value <= 40:
            vix_score = 20 + (40 - vix_value) * 2  # 20-40 for 30-40
        elif vix_value <= 60:
            vix_score = max(0, 20 - (vix_value - 40) * 1)  # 0-20 for 40-60
        else:
            vix_score = 0
            
        components.append({
            'indicator': 'VIX Volatility Index',
            'value': vix_value,
            'score': vix_score,
            'weight': weights['vix'],
            'contribution': vix_score * weights['vix'] / 100
        })
    
    # 12. Consumer Sentiment
    consumer_sentiment_score = 0
    if not consumer_sentiment_data.empty:
        latest_consumer_sentiment = consumer_sentiment_data.sort_values('date', ascending=False).iloc[0]
        consumer_sentiment_value = latest_consumer_sentiment['value']
        
        # Score Consumer Sentiment on a 0-100 scale
        # Typical range: 50-110
        if consumer_sentiment_value >= 100:
            consumer_sentiment_score = 100
        elif consumer_sentiment_value >= 90:
            consumer_sentiment_score = 80 + (consumer_sentiment_value - 90) * 2  # 80-100 for 90-100
        elif consumer_sentiment_value >= 80:
            consumer_sentiment_score = 60 + (consumer_sentiment_value - 80) * 2  # 60-80 for 80-90
        elif consumer_sentiment_value >= 70:
            consumer_sentiment_score = 40 + (consumer_sentiment_value - 70) * 2  # 40-60 for 70-80
        elif consumer_sentiment_value >= 60:
            consumer_sentiment_score = 20 + (consumer_sentiment_value - 60) * 2  # 20-40 for 60-70
        elif consumer_sentiment_value >= 50:
            consumer_sentiment_score = max(0, 20 * (consumer_sentiment_value - 50) / 10)  # 0-20 for 50-60
        else:
            consumer_sentiment_score = 0
        
        print(f"Consumer Sentiment: value={consumer_sentiment_value}, score={consumer_sentiment_score}, weight={weights['consumer_sentiment']}, contribution={consumer_sentiment_score * weights['consumer_sentiment'] / 100}")
            
        components.append({
            'indicator': 'Consumer Sentiment',
            'value': consumer_sentiment_value,
            'score': consumer_sentiment_score,
            'weight': weights['consumer_sentiment'],
            'contribution': consumer_sentiment_score * weights['consumer_sentiment'] / 100
        })
    
    # 13. Software Job Postings (if data available)
    job_postings_score = 0
    if not job_postings_data.empty and 'yoy_growth' in job_postings_data.columns:
        latest_job_postings = job_postings_data.sort_values('date', ascending=False).iloc[0]
        job_postings_value = latest_job_postings['yoy_growth']
        
        # Score job postings growth on a 0-100 scale
        # Typical range: -30% to +50% annually
        if job_postings_value >= 40:
            job_postings_score = 100
        elif job_postings_value >= 20:
            job_postings_score = 80 + (job_postings_value - 20) * 1  # 80-100 for 20-40%
        elif job_postings_value >= 5:
            job_postings_score = 60 + (job_postings_value - 5) * 1.33  # 60-80 for 5-20%
        elif job_postings_value >= -10:
            job_postings_score = 30 + (job_postings_value + 10) * 2  # 30-60 for -10-5%
        elif job_postings_value >= -30:
            job_postings_score = max(0, 30 + (job_postings_value + 10) * 1.5)  # 0-30 for -30--10%
        else:
            job_postings_score = 0
            
        components.append({
            'indicator': 'Software Job Postings',
            'value': job_postings_value,
            'score': job_postings_score,
            'weight': weights['job_postings'],
            'contribution': job_postings_score * weights['job_postings'] / 100
        })
    
    # Include proprietary data if available
    if proprietary_data and 'value' in proprietary_data and 'weight' in proprietary_data:
        proprietary_value = proprietary_data['value']
        proprietary_weight = proprietary_data['weight']
        
        components.append({
            'indicator': 'Proprietary Data',
            'value': proprietary_value,
            'score': proprietary_value,  # Assuming proprietary data is already on a 0-100 scale
            'weight': proprietary_weight,
            'contribution': proprietary_value * proprietary_weight / 100
        })
    
    # Include document analysis if available
    if document_data and 'sentiment_score' in document_data and 'weight' in document_data:
        document_value = document_data['sentiment_score']
        document_weight = document_data['weight']
        
        components.append({
            'indicator': 'Document Sentiment',
            'value': document_value,
            'score': document_value,  # Assuming document sentiment is already on a 0-100 scale
            'weight': document_weight,
            'contribution': document_value * document_weight / 100
        })
    
    # Calculate overall sentiment score (weighted average)
    total_weight = sum(component['weight'] for component in components)
    if total_weight > 0:
        overall_score = sum(component['contribution'] for component in components)
    else:
        overall_score = 50  # Default to neutral if no data
    
    # Determine the sentiment category
    if overall_score >= 80:
        sentiment_category = "Boom"
    elif overall_score >= 60:
        sentiment_category = "Expansion"
    elif overall_score >= 40:
        sentiment_category = "Moderate Growth"
    elif overall_score >= 20:
        sentiment_category = "Slowdown"
    else:
        sentiment_category = "Contraction"
    
    return {
        'score': overall_score,
        'category': sentiment_category,
        'components': components
    }
    
# Initialize the sentiment index calculation on app start
@app.callback(
    [Output('sentiment-score', 'children'),
     Output('sentiment-category', 'children')],
    [Input('interval-component', 'n_intervals')],
    [State('custom-weights-store', 'data'),
     State('document-data-store', 'data')]
)
def initialize_sentiment_index(_, custom_weights, document_data):
    sentiment_index = calculate_sentiment_index(
        custom_weights=custom_weights,
        document_data=document_data
    )
    
    return f"{sentiment_index['score']:.1f}", sentiment_index['category']

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)