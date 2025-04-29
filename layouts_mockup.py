import dash
from dash import html, dcc
import plotly.graph_objs as go
import plotly.express as px
import pandas as pd
import numpy as np

# Sample data for sector cards
sectors = [
    {"sector": "SMB SaaS", "score": 62.5, "stance": "Bullish"},
    {"sector": "Enterprise SaaS", "score": 54.2, "stance": "Neutral"},
    {"sector": "Cloud Infrastructure", "score": 71.3, "stance": "Bullish"},
    {"sector": "AdTech", "score": 48.9, "stance": "Neutral"},
    {"sector": "Fintech", "score": 38.6, "stance": "Neutral"},
    {"sector": "Consumer Internet", "score": 29.4, "stance": "Bearish"},
    {"sector": "eCommerce", "score": 45.2, "stance": "Neutral"},
    {"sector": "Cybersecurity", "score": 68.7, "stance": "Bullish"},
    {"sector": "Dev Tools / Analytics", "score": 58.3, "stance": "Neutral"},
    {"sector": "Semiconductors", "score": 73.1, "stance": "Bullish"},
    {"sector": "AI Infrastructure", "score": 78.9, "stance": "Bullish"},
]

# Sample data for key indicators
key_indicators = [
    {"name": "GDP Growth", "value": "2.5%", "change": "+0.3%", "direction": "up"},
    {"name": "CPI Inflation", "value": "3.1%", "change": "-0.2%", "direction": "down"},
    {"name": "Unemployment", "value": "3.8%", "change": "0.0%", "direction": "neutral"},
    {"name": "Fed Funds Rate", "value": "5.25%", "change": "0.0%", "direction": "neutral"},
    {"name": "10Y Treasury", "value": "4.2%", "change": "+0.1%", "direction": "up"},
    {"name": "VIX", "value": "14.2", "change": "-1.3", "direction": "down"},
    {"name": "PCE", "value": "2.7%", "change": "-0.1%", "direction": "down"},
    {"name": "Software Job Postings", "value": "-4.2%", "change": "-1.8%", "direction": "down"},
]

# Initialize the app
app = dash.Dash(__name__, suppress_callback_exceptions=True)

# Define custom CSS
css = '''
    .header {
        display: flex;
        justify-content: space-between;
        align-items: center;
        margin-bottom: 20px;
        background-color: white;
        padding: 15px;
        border-radius: 8px;
        box-shadow: 0 2px 5px rgba(0,0,0,0.1);
    }
    .header-left {
        display: flex;
        align-items: center;
    }
    .logo {
        height: 50px;
        margin-right: 20px;
    }
    .title {
        font-size: 24px;
        font-weight: bold;
        color: #2c3e50;
    }
    .subtitle {
        font-size: 16px;
        color: #7f8c8d;
    }
    .t2d-pulse-container {
        display: flex;
        align-items: center;
        margin-left: 30px;
    }
    .t2d-pulse-label {
        font-size: 18px;
        font-weight: bold;
        margin-right: 15px;
    }
    .t2d-pulse-score {
        font-size: 24px;
        font-weight: bold;
        color: #f39c12;  /* Orange for Neutral */
        background-color: white;
        padding: 10px 15px;
        border-radius: 8px;
        box-shadow: 0 0 10px rgba(243, 156, 18, 0.5);
    }
    .last-updated {
        font-size: 14px;
        color: #95a5a6;
    }
    .tabs {
        margin-bottom: 20px;
    }
    .tab-content {
        background-color: white;
        padding: 20px;
        border-radius: 8px;
        box-shadow: 0 2px 5px rgba(0,0,0,0.1);
    }
    .section-title {
        font-size: 20px;
        font-weight: bold;
        color: #2c3e50;
        margin-bottom: 20px;
    }
    .cards-container {
        display: grid;
        grid-template-columns: repeat(auto-fill, minmax(240px, 1fr));
        grid-gap: 20px;
    }
    .sector-card {
        background-color: white;
        padding: 15px;
        border-radius: 8px;
        box-shadow: 0 2px 5px rgba(0,0,0,0.1);
        position: relative;
        overflow: hidden;
        display: flex;
        flex-direction: column;
        border-left: 8px solid;
    }
    .sector-card-bullish {
        border-left-color: #2ecc71;  /* Green */
    }
    .sector-card-neutral {
        border-left-color: #f39c12;  /* Orange */
    }
    .sector-card-bearish {
        border-left-color: #e74c3c;  /* Red */
    }
    .sector-name {
        font-size: 18px;
        font-weight: bold;
        margin-bottom: 10px;
    }
    .sector-score {
        font-size: 24px;
        font-weight: bold;
        margin-bottom: 10px;
    }
    .sector-bullish {
        color: #2ecc71;  /* Green */
    }
    .sector-neutral {
        color: #f39c12;  /* Orange */
    }
    .sector-bearish {
        color: #e74c3c;  /* Red */
    }
    .sector-stance {
        font-size: 14px;
        font-weight: bold;
        padding: 5px 10px;
        border-radius: 4px;
        margin-bottom: 5px;
        display: inline-block;
    }
    .stance-bullish {
        background-color: rgba(46, 204, 113, 0.2);
        color: #2ecc71;
    }
    .stance-neutral {
        background-color: rgba(243, 156, 18, 0.2);
        color: #f39c12;
    }
    .stance-bearish {
        background-color: rgba(231, 76, 60, 0.2);
        color: #e74c3c;
    }

    /* Sidebar layout styles */
    .content-with-sidebar {
        display: flex;
        gap: 20px;
    }
    .main-content {
        flex: 3;  /* Takes 3/4 of the space */
    }
    .sidebar {
        flex: 1;  /* Takes 1/4 of the space */
        background-color: white;
        padding: 15px;
        border-radius: 8px;
        box-shadow: 0 2px 5px rgba(0,0,0,0.1);
        height: fit-content;
    }
    .sidebar-title {
        font-size: 16px;
        font-weight: bold;
        color: #7f8c8d;
        margin-bottom: 15px;
        text-transform: uppercase;
        letter-spacing: 1px;
    }
    .indicator-card {
        background-color: #f8f9fa;
        padding: 10px;
        border-radius: 6px;
        margin-bottom: 10px;
    }
    .indicator-name {
        font-size: 14px;
        color: #7f8c8d;
        margin-bottom: 5px;
    }
    .indicator-value {
        font-size: 16px;
        font-weight: bold;
        color: #2c3e50;
    }
    .indicator-change {
        font-size: 12px;
        margin-left: 5px;
    }
    .indicator-positive {
        color: #2ecc71;
    }
    .indicator-negative {
        color: #e74c3c;
    }

    /* Below layout styles */
    .below-content {
        margin-top: 30px;
    }
    .key-indicators-below {
        display: grid;
        grid-template-columns: repeat(auto-fill, minmax(250px, 1fr));
        grid-gap: 15px;
        margin-top: 15px;
    }
    
    .nav-links {
        display: flex;
        justify-content: center;
        margin: 20px 0;
        gap: 20px;
    }
    
    .nav-link {
        padding: 10px 15px;
        background-color: #2c3e50;
        color: white;
        text-decoration: none;
        border-radius: 5px;
        font-weight: bold;
    }
    
    .nav-link:hover {
        background-color: #34495e;
    }
'''

def create_sidebar_layout():
    """Create layout with Key Indicators as a sidebar"""
    return html.Div([
        # Header with logo, title and pulse score
        html.Div([
            html.Div([
                html.Img(src="/assets/T2D_Pulse_logo.png", className="logo"),
                html.Div([
                    html.Div("T2D Pulse Economic Dashboard", className="title"),
                    html.Div("Software & Technology Industry Focus", className="subtitle"),
                ]),
            ], className="header-left"),
            
            html.Div([
                html.Div("T2D Pulse:", className="t2d-pulse-label"),
                html.Div("47.3", className="t2d-pulse-score"),
            ], className="t2d-pulse-container"),
        ], className="header"),
        
        # Last updated timestamp
        html.Div("Last Updated: April 26, 2025 8:30 AM ET", className="last-updated"),
        
        # Main content with sidebar
        html.Div([
            # Left column - Main content
            html.Div([
                html.Div("Tech Sector Sentiment", className="section-title"),
                
                # Sector cards 
                html.Div([
                    html.Div([
                        html.Div(sector["sector"], className="sector-name"),
                        html.Div(f"{sector['score']:.1f}", className="sector-score " + f"sector-{sector['stance'].lower()}"),
                        html.Div(sector["stance"], className=f"sector-stance stance-{sector['stance'].lower()}"),
                    ], className=f"sector-card sector-card-{sector['stance'].lower()}")
                    for sector in sectors
                ], className="cards-container")
            ], className="main-content"),
            
            # Right column - Sidebar
            html.Div([
                html.Div("Key Indicators", className="sidebar-title"),
                
                # Indicators list
                html.Div([
                    html.Div([
                        html.Div(indicator["name"], className="indicator-name"),
                        html.Div([
                            html.Span(indicator["value"], className="indicator-value"),
                            html.Span(
                                indicator["change"], 
                                className=f"indicator-change indicator-{indicator['direction']}"
                            ),
                        ], style={"display": "flex", "alignItems": "center"})
                    ], className="indicator-card")
                    for indicator in key_indicators
                ])
            ], className="sidebar"),
        ], className="content-with-sidebar"),
    ])

def create_below_layout():
    """Create layout with Key Indicators below Sector Sentiment"""
    return html.Div([
        # Header with logo, title and pulse score
        html.Div([
            html.Div([
                html.Img(src="/assets/T2D_Pulse_logo.png", className="logo"),
                html.Div([
                    html.Div("T2D Pulse Economic Dashboard", className="title"),
                    html.Div("Software & Technology Industry Focus", className="subtitle"),
                ]),
            ], className="header-left"),
            
            html.Div([
                html.Div("T2D Pulse:", className="t2d-pulse-label"),
                html.Div("47.3", className="t2d-pulse-score"),
            ], className="t2d-pulse-container"),
        ], className="header"),
        
        # Last updated timestamp
        html.Div("Last Updated: April 26, 2025 8:30 AM ET", className="last-updated"),
        
        # Main content
        html.Div([
            html.Div("Tech Sector Sentiment", className="section-title"),
            
            # Sector cards 
            html.Div([
                html.Div([
                    html.Div(sector["sector"], className="sector-name"),
                    html.Div(f"{sector['score']:.1f}", className="sector-score " + f"sector-{sector['stance'].lower()}"),
                    html.Div(sector["stance"], className=f"sector-stance stance-{sector['stance'].lower()}"),
                ], className=f"sector-card sector-card-{sector['stance'].lower()}")
                for sector in sectors
            ], className="cards-container"),
            
            # Key Indicators below
            html.Div([
                html.Div("Key Indicators", className="section-title"),
                
                html.Div([
                    html.Div([
                        html.Div(indicator["name"], className="indicator-name"),
                        html.Div([
                            html.Span(indicator["value"], className="indicator-value"),
                            html.Span(
                                indicator["change"], 
                                className=f"indicator-change indicator-{indicator['direction']}"
                            ),
                        ], style={"display": "flex", "alignItems": "center"})
                    ], className="indicator-card")
                    for indicator in key_indicators
                ], className="key-indicators-below"),
            ], className="below-content"),
        ])
    ])

# Create a stylesheets folder and file if it doesn't exist
import os
if not os.path.exists('assets'):
    os.makedirs('assets')

# Write the CSS to a file
with open('assets/layouts_mockup.css', 'w') as f:
    f.write(css)

# App layout with navigation
app.layout = html.Div([
    
    dcc.Location(id='url', refresh=False),
    
    # Navigation links
    html.Div([
        html.A('View Sidebar Layout', href='/sidebar', className='nav-link'),
        html.A('View Below Layout', href='/below', className='nav-link'),
    ], className='nav-links'),
    
    # Content will be rendered here
    html.Div(id='page-content')
])

@app.callback(
    dash.dependencies.Output('page-content', 'children'),
    [dash.dependencies.Input('url', 'pathname')]
)
def display_page(pathname):
    if pathname == '/sidebar':
        return create_sidebar_layout()
    elif pathname == '/below':
        return create_below_layout()
    else:
        # Default to sidebar layout
        return create_sidebar_layout()

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5556, debug=True)