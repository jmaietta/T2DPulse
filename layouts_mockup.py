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
    {"sector": "Vertical SaaS", "score": 51.2, "stance": "Neutral"},
    {"sector": "IT Services / Legacy Tech", "score": 32.6, "stance": "Bearish"},
    {"sector": "Hardware / Devices", "score": 41.5, "stance": "Neutral"}
]

# Sample data for key indicators
indicators = [
    {"name": "10-Year Treasury Yield", "value": "4.42%", "change": "▲ 0.05"},
    {"name": "VIX Volatility", "value": "32.48", "change": "▲ 0.82"},
    {"name": "NASDAQ Trend", "value": "+3.25%", "change": "▲ 0.75"},
    {"name": "Consumer Sentiment", "value": "61.3", "change": "▼ 2.1"},
]

# Create app
app = dash.Dash(__name__, 
                meta_tags=[{"name": "viewport", "content": "width=device-width, initial-scale=1"}])

# Sample T2D Pulse score and date
t2d_pulse_score = 46.4
last_updated = "April 27, 2025"

# App layout options
def create_sidebar_layout():
    """Create layout with Key Indicators as a sidebar"""
    return html.Div(className="container", children=[
        # Header with logo, title, and T2D Pulse score
        html.Div(className="header", children=[
            html.Div(className="header-left", children=[
                html.Img(src="https://www.t2d.io/wp-content/uploads/2023/03/t2d-logo.png", className="logo"),
                html.Div(children=[
                    html.Div("T2D Pulse Economic Dashboard", className="title"),
                    html.Div("Powering investment decisions with macro data and proprietary intelligence", className="subtitle"),
                ]),
                html.Div(className="t2d-pulse-container", children=[
                    html.Div("T2D Pulse:", className="t2d-pulse-label"),
                    html.Div(f"{t2d_pulse_score}", className="t2d-pulse-score"),
                ]),
            ]),
            html.Div(f"Last updated: {last_updated}", className="last-updated"),
        ]),
        
        # Main content with sidebar
        html.Div(className="content-with-sidebar", children=[
            # Main content (sector sentiment)
            html.Div(className="main-content", children=[
                html.Div(className="tab-content", children=[
                    html.Div("Sector Sentiment", className="section-title"),
                    
                    # Sector cards in a grid
                    html.Div(className="cards-container", children=[
                        html.Div(
                            className=f"sector-card sector-card-{sector['stance'].lower()}", 
                            children=[
                                html.Div(sector["sector"], className="sector-name"),
                                html.Div(
                                    f"{sector['score']}",
                                    className=f"sector-score sector-{sector['stance'].lower()}"
                                ),
                                html.Div(
                                    sector["stance"],
                                    className=f"sector-stance stance-{sector['stance'].lower()}"
                                ),
                            ]
                        ) for sector in sectors
                    ]),
                ]),
            ]),
            
            # Sidebar (key indicators)
            html.Div(className="sidebar", children=[
                html.Div("Key Indicators", className="sidebar-title"),
                
                # Indicator cards
                html.Div(children=[
                    html.Div(className="indicator-card", children=[
                        html.Div(indicator["name"], className="indicator-name"),
                        html.Div(children=[
                            html.Span(indicator["value"], className="indicator-value"),
                            html.Span(
                                indicator["change"],
                                className=f"indicator-change indicator-{'positive' if '▲' in indicator['change'] else 'negative'}"
                            ),
                        ]),
                    ]) for indicator in indicators
                ]),
            ]),
        ]),
        
        # Footer note
        html.Div(
            "MOCKUP: Key Indicators as Sidebar Layout", 
            style={"textAlign": "center", "marginTop": "30px", "color": "#7f8c8d"}
        ),
    ])

def create_below_layout():
    """Create layout with Key Indicators below Sector Sentiment"""
    return html.Div(className="container", children=[
        # Header with logo, title, and T2D Pulse score
        html.Div(className="header", children=[
            html.Div(className="header-left", children=[
                html.Img(src="https://www.t2d.io/wp-content/uploads/2023/03/t2d-logo.png", className="logo"),
                html.Div(children=[
                    html.Div("T2D Pulse Economic Dashboard", className="title"),
                    html.Div("Powering investment decisions with macro data and proprietary intelligence", className="subtitle"),
                ]),
                html.Div(className="t2d-pulse-container", children=[
                    html.Div("T2D Pulse:", className="t2d-pulse-label"),
                    html.Div(f"{t2d_pulse_score}", className="t2d-pulse-score"),
                ]),
            ]),
            html.Div(f"Last updated: {last_updated}", className="last-updated"),
        ]),
        
        # Main content
        html.Div(className="tab-content", children=[
            html.Div("Sector Sentiment", className="section-title"),
            
            # Sector cards in a grid
            html.Div(className="cards-container", children=[
                html.Div(
                    className=f"sector-card sector-card-{sector['stance'].lower()}", 
                    children=[
                        html.Div(sector["sector"], className="sector-name"),
                        html.Div(
                            f"{sector['score']}",
                            className=f"sector-score sector-{sector['stance'].lower()}"
                        ),
                        html.Div(
                            sector["stance"],
                            className=f"sector-stance stance-{sector['stance'].lower()}"
                        ),
                    ]
                ) for sector in sectors
            ]),
        ]),
        
        # Key Indicators below
        html.Div(className="below-content", children=[
            html.Div(className="tab-content", children=[
                html.Div("Key Indicators", className="sidebar-title", style={"textAlign": "center"}),
                
                # Indicator cards in a grid
                html.Div(className="key-indicators-below", children=[
                    html.Div(className="indicator-card", children=[
                        html.Div(indicator["name"], className="indicator-name"),
                        html.Div(children=[
                            html.Span(indicator["value"], className="indicator-value"),
                            html.Span(
                                indicator["change"],
                                className=f"indicator-change indicator-{'positive' if '▲' in indicator['change'] else 'negative'}"
                            ),
                        ]),
                    ]) for indicator in indicators
                ]),
            ]),
        ]),
        
        # Footer note
        html.Div(
            "MOCKUP: Key Indicators Below Layout", 
            style={"textAlign": "center", "marginTop": "30px", "color": "#7f8c8d"}
        ),
    ])

# App layout with navigation
app.layout = html.Div([
    # Add CSS styles inline
    html.Style('''
        body {
            font-family: 'Arial', sans-serif;
            margin: 0;
            background-color: #f8f9fa;
        }
        .container {
            max-width: 1400px;
            margin: 0 auto;
            padding: 20px;
        }
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
    '''),
    
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
    app.run(host='0.0.0.0', port=5000, debug=True)