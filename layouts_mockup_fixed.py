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
    {"name": "Interest Rate", "value": "5.25%", "change": "▬ 0.00"},
    {"name": "Inflation (CPI)", "value": "3.4%", "change": "▼ 0.1"},
    {"name": "Unemployment", "value": "4.1%", "change": "▲ 0.3"},
    {"name": "GDP Growth", "value": "2.8%", "change": "▼ 0.1"},
    {"name": "PCE", "value": "3.2%", "change": "▼ 0.05"},
]

# Initialize Dash app
app = dash.Dash(__name__)
app.title = "T2D Pulse Layout Options"

# External CSS
external_stylesheets = [
    "https://cdnjs.cloudflare.com/ajax/libs/font-awesome/5.15.3/css/all.min.css"
]

# Custom CSS as string
css = '''
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
    color: #f39c12;
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
    border-left-color: #2ecc71;
}
.sector-card-neutral {
    border-left-color: #f39c12;
}
.sector-card-bearish {
    border-left-color: #e74c3c;
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
    color: #2ecc71;
}
.sector-neutral {
    color: #f39c12;
}
.sector-bearish {
    color: #e74c3c;
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
    flex: 3;
}
.sidebar {
    flex: 1;
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

# Create the header (same for both layouts)
def create_header():
    return html.Div([
        html.Div([
            html.Img(src="/assets/T2D Pulse logo.png", className="logo"),
            html.Div([
                html.Div("T2D Pulse", className="title"),
                html.Div("Economic Dashboard", className="subtitle"),
            ]),
            html.Div([
                html.Div("T2D Pulse:", className="t2d-pulse-label"),
                html.Div("51.2", className="t2d-pulse-score"),
            ], className="t2d-pulse-container")
        ], className="header-left"),
        html.Div("Last updated: April 28, 2023", className="last-updated")
    ], className="header")

# Create sector cards
def create_sector_cards():
    return html.Div([
        html.Div([
            create_sector_card(sector)
            for sector in sectors
        ], className="cards-container")
    ])

def create_sector_card(sector_data):
    # Determine styles based on the sentiment stance
    stance = sector_data["stance"]
    card_class = f"sector-card sector-card-{stance.lower()}"
    score_class = f"sector-score sector-{stance.lower()}"
    stance_class = f"sector-stance stance-{stance.lower()}"
    
    return html.Div([
        html.Div(sector_data["sector"], className="sector-name"),
        html.Div(f"{sector_data['score']:.1f}", className=score_class),
        html.Div(stance, className=stance_class),
    ], className=card_class)

# Create key indicators for sidebar
def create_sidebar_indicators():
    return html.Div([
        html.Div("Key Indicators", className="sidebar-title"),
        html.Div([
            create_indicator_card(indicator)
            for indicator in indicators
        ])
    ], className="sidebar")

# Create key indicators for below layout
def create_below_indicators():
    return html.Div([
        html.Div("Key Indicators", className="section-title"),
        html.Div([
            create_indicator_card(indicator)
            for indicator in indicators
        ], className="key-indicators-below")
    ], className="below-content")

def create_indicator_card(indicator):
    # Determine if change is positive or negative
    change_class = "indicator-positive" if "▲" in indicator["change"] else "indicator-negative" if "▼" in indicator["change"] else ""
    
    return html.Div([
        html.Div(indicator["name"], className="indicator-name"),
        html.Div([
            html.Span(indicator["value"], className="indicator-value"),
            html.Span(indicator["change"], className=f"indicator-change {change_class}")
        ])
    ], className="indicator-card")

# Create sidebar layout
def create_sidebar_layout():
    return html.Div([
        create_header(),
        
        html.Div([
            # Main content area (sectors)
            html.Div([
                html.Div("Sector Sentiment", className="section-title"),
                create_sector_cards()
            ], className="main-content"),
            
            # Sidebar (key indicators)
            create_sidebar_indicators()
        ], className="content-with-sidebar container")
    ])

# Create below layout
def create_below_layout():
    return html.Div([
        create_header(),
        
        html.Div([
            # Sectors section
            html.Div("Sector Sentiment", className="section-title"),
            create_sector_cards(),
            
            # Key indicators section below
            create_below_indicators()
        ], className="container")
    ])

# Create a CSS tag to include the styles
app.index_string = '''
<!DOCTYPE html>
<html>
    <head>
        {%metas%}
        <title>{%title%}</title>
        {%favicon%}
        {%css%}
        <style>
''' + css + '''
        </style>
    </head>
    <body>
        {%app_entry%}
        <footer>
            {%config%}
            {%scripts%}
            {%renderer%}
        </footer>
    </body>
</html>
'''

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
    app.run(host='0.0.0.0', port=5555, debug=True)