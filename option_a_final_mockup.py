"""
T2D Pulse Landing Page - Final Layout
- T2D Pulse score as primary focus
- Sector cards in 3-column grid with weight controls
- Sector summary below the T2D Pulse score
- Methodology at the bottom
- Sector cards styled to match existing dashboard
"""

import dash
from dash import html, dcc, callback, ctx
import plotly.graph_objs as go
import numpy as np
import pandas as pd
import json
from dash.dependencies import Input, Output, State, ALL

app = dash.Dash(__name__, suppress_callback_exceptions=True)

# Generate sample data
def generate_sample_data():
    # Sample sector scores (already normalized to 0-100 scale)
    sectors = {
        "SMB SaaS": 42.5,
        "Enterprise SaaS": 65.0,
        "Cloud Infrastructure": 78.5,
        "AdTech": 46.0,
        "Fintech": 38.5,
        "Consumer Internet": 55.0,
        "eCommerce": 62.0,
        "Cybersecurity": 71.5,
        "Dev Tools / Analytics": 68.0,
        "Semiconductors": 53.0,
        "AI Infrastructure": 82.5,
        "Vertical SaaS": 57.5,
        "IT Services / Legacy Tech": 45.0,
        "Hardware / Devices": 51.0
    }
    
    # Default equal weights for each sector
    sector_count = len(sectors)
    weights = {sector: 100 / sector_count for sector in sectors.keys()}
    
    return sectors, weights

# Get sample data
sector_scores, default_weights = generate_sample_data()

# Create pulse score card with glow effect
def create_pulse_card(value):
    # Determine Pulse status based on score
    if value >= 80:
        pulse_status = "Boom"
        pulse_color = "#2ecc71"  # Green
    elif value >= 60:
        pulse_status = "Expansion"
        pulse_color = "#f1c40f"  # Yellow
    elif value >= 40:
        pulse_status = "Neutral"
        pulse_color = "#f39c12"  # Orange
    elif value >= 20:
        pulse_status = "Caution"
        pulse_color = "#e74c3c"  # Light Red
    else:
        pulse_status = "Contraction"
        pulse_color = "#c0392b"  # Dark Red
    
    # Create the pulse card component
    pulse_card = html.Div([
        # Container with vertical centering for all elements
        html.Div([
            # Title 
            html.H3("T2D Pulse", 
                    style={
                        "fontSize": "22px", 
                        "fontWeight": "bold", 
                        "marginBottom": "15px", 
                        "textAlign": "center",
                        "color": "#333333"
                    }),
            # Score value
            html.Div([
                html.Span(f"{value}", 
                        style={
                            "fontSize": "64px", 
                            "fontWeight": "bold", 
                            "color": pulse_color,
                            "display": "block",
                            "textAlign": "center",
                            "width": "100%"
                        }),
            ], style={"textAlign": "center", "marginBottom": "10px", "display": "flex", "justifyContent": "center"}),
            # Status label
            html.Div([
                html.Span(pulse_status, 
                        style={
                            "fontSize": "24px", 
                            "color": pulse_color,
                            "marginRight": "5px",
                            "display": "inline-block",
                            "fontWeight": "500"
                        }),
                html.Span(
                    "ⓘ", 
                    className="info-icon",
                    style={
                        "cursor": "pointer", 
                        "fontSize": "16px", 
                        "display": "inline-block",
                        "color": "#2c3e50",
                        "verticalAlign": "text-top" 
                    }
                )
            ], style={
                "textAlign": "center", 
                "display": "flex", 
                "alignItems": "center", 
                "justifyContent": "center", 
                "position": "relative",
                "height": "30px"  # Fixed height to prevent layout shifts
            }),
            # Last updated text
            html.Div([
                html.Span(f"Last updated: April 25, 2025", 
                        style={
                            "fontSize": "12px", 
                            "color": "#95a5a6",
                            "marginTop": "15px"
                        })
            ], style={"textAlign": "center"})
        ], style={
            "display": "flex",
            "flexDirection": "column",
            "justifyContent": "center",
            "alignItems": "center",
            "padding": "25px 20px"
        })
    ], style={
        "backgroundColor": "white",
        "borderRadius": "8px",
        "boxShadow": f"0 0 20px {pulse_color}",  # Color-matched glow
        "border": f"1px solid {pulse_color}",     # Color-matched border
        "transition": "all 0.3s ease",
        "width": "320px",
        "height": "320px",
        "margin": "0 auto",
        "display": "flex",
        "alignItems": "center",
        "justifyContent": "center"
    })
    
    return pulse_card, pulse_status, pulse_color

# Generate sample tickers for mockup purposes
def generate_sample_tickers(sector):
    """Generate sample tickers for mockup purposes"""
    if sector == "Cloud Infrastructure":
        return ["AMZN", "MSFT", "GOOG"]
    elif sector == "AI Infrastructure":
        return ["NVDA", "AMD", "INTC"]
    elif sector == "Cybersecurity":
        return ["CRWD", "PANW", "ZS"]
    elif sector == "Fintech":
        return ["SQ", "PYPL", "COIN"]
    elif sector == "Enterprise SaaS":
        return ["CRM", "WDAY", "NOW"]
    elif sector == "SMB SaaS":
        return ["HUG", "DDOG", "ZM"]
    elif sector == "AdTech":
        return ["GOOG", "FB", "TTD"]
    elif sector == "Consumer Internet":
        return ["NFLX", "SPOT", "PINS"]
    elif sector == "eCommerce":
        return ["AMZN", "SHOP", "ETSY"]
    elif sector == "Dev Tools / Analytics":
        return ["TEAM", "TWLO", "SNOW"]
    elif sector == "Semiconductors":
        return ["NVDA", "INTC", "AMD"]
    elif sector == "Vertical SaaS":
        return ["VEEV", "TDOC", "REAL"]
    elif sector == "IT Services / Legacy Tech":
        return ["IBM", "CSCO", "ORCL"]
    elif sector == "Hardware / Devices":
        return ["AAPL", "SONO", "FIT"]
    else:
        # Generate random tickers for other sectors
        return [f"{sector[0:2].upper()}{i}" for i in range(1, 4)]

# Create sector cards with embedded weight controls
def create_sector_cards(sector_scores, weights):
    cards = []
    
    for sector, score in sector_scores.items():
        # Determine color and sentiment based on score
        if score >= 60:
            sentiment = "Bullish"
            bg_color = "#ebf7f0"  # Light green background
            border_color = "#2ecc71"  # Green border
            text_color = "#27ae60"  # Darker green text
            takeaway = "Outperforming peers"
            badge_class = "badge-bullish"
        elif score >= 40:
            sentiment = "Neutral"
            bg_color = "#fef5e7"  # Light yellow background
            border_color = "#f39c12"  # Orange border
            text_color = "#d35400"  # Darker orange text
            takeaway = "Neutral – monitor trends"
            badge_class = "badge-neutral"
        else:
            sentiment = "Bearish"
            bg_color = "#fdedec"  # Light red background
            border_color = "#e74c3c"  # Red border  
            text_color = "#c0392b"  # Darker red text
            takeaway = "Bearish macro setup"
            badge_class = "badge-bearish"
        
        # Generate sample drivers based on sentiment
        drivers = [
            f"Strong signal from {'revenue growth' if score > 50 else 'market trends'}",
            f"{'Positive' if score > 50 else 'Negative'} impact from economic indicators",
            f"Monitor {'opportunities' if score > 50 else 'headwinds'} in coming quarter"
        ]
        
        # Get sample tickers for this sector
        tickers = generate_sample_tickers(sector)
        
        # Calculate percentage weight
        weight_pct = weights[sector]
        
        # Create the sector card with original format: comments, bullets and tickers
        card = html.Div([
            # Header with sector name and score
            html.Div([
                html.Div([
                    html.H3(sector, className="sector-card-title"),
                    html.Div([
                        html.Span(f"{score:.1f}", className="sector-score"),
                        html.Div(sentiment, className="sector-sentiment", 
                                style={"color": text_color})
                    ], className="score-container")
                ], className="card-header-content")
            ], className="sector-card-header", style={"borderColor": border_color}),
            
            # Card body with all the details
            html.Div([
                # Header and sentiment badge - now with badge on the right
                html.Div([
                    html.P(takeaway, className="sector-takeaway"),
                    html.Span(sentiment, className=f"sector-badge {badge_class}")
                ], className="takeaway-badge-container", style={"display": "flex", "justifyContent": "space-between", "alignItems": "center"}),
                
                # Scale indicator
                html.Div([
                    html.Div([
                        html.Div(className="scale-marker", 
                                 style={"left": f"{min(max(score, 0), 100)}%"})
                    ], className="scale-track")
                ], className="sector-score-scale"),
                
                # Drivers section
                
                # Drivers list
                html.Ul([
                    html.Li(driver) for driver in drivers
                ], className="drivers-list"),
                
                # Tickers
                html.Div([
                    html.Span(ticker, className="ticker-badge") for ticker in tickers
                ], className="tickers-container"),
                
                # Weight controls
                html.Div([
                    html.Div([
                        html.Span("Weight:", className="weight-label"),
                        html.Span(f"{weight_pct:.1f}%", 
                                 id={"type": "weight-display", "index": sector},
                                 className="weight-value")
                    ], className="weight-display-container"),
                    
                    html.Div([
                        html.Button("-", 
                                   id={"type": "decrease-weight", "index": sector},
                                   className="weight-button weight-decrease"),
                        html.Button("+", 
                                   id={"type": "increase-weight", "index": sector},
                                   className="weight-button weight-increase")
                    ], className="weight-buttons-container")
                ], className="weight-controls")
            ], className="sector-card-body", style={"backgroundColor": bg_color})
        ], className="sector-card")
        
        cards.append(card)
    
    return cards

# Create sector score summary
def create_sector_summary():
    # Sort sectors by score (descending)
    sorted_sectors = sorted(sector_scores.items(), key=lambda x: x[1], reverse=True)
    
    # Determine top 3 and bottom 3 sectors
    top_3 = sorted_sectors[:3]
    bottom_3 = sorted_sectors[-3:]
    
    top_sectors = html.Div([
        html.H4("Strongest Sectors", className="summary-title"),
        html.Ul([
            html.Li([
                html.Span(f"{sector}: ", className="sector-name"),
                html.Span(f"{score:.1f}", 
                         className="sector-score-positive" if score >= 60 else 
                                  "sector-score-neutral" if score >= 40 else 
                                  "sector-score-negative")
            ]) for sector, score in top_3
        ], className="sector-list")
    ], className="summary-section")
    
    bottom_sectors = html.Div([
        html.H4("Weakest Sectors", className="summary-title"),
        html.Ul([
            html.Li([
                html.Span(f"{sector}: ", className="sector-name"),
                html.Span(f"{score:.1f}", 
                         className="sector-score-positive" if score >= 60 else 
                                  "sector-score-neutral" if score >= 40 else 
                                  "sector-score-negative")
            ]) for sector, score in bottom_3
        ], className="sector-list")
    ], className="summary-section")
    
    return html.Div([top_sectors, bottom_sectors], className="sector-summary-content")

# Calculate T2D Pulse score from sector scores and weights
def calculate_t2d_pulse(sector_scores, weights):
    # Normalize weights to sum to 100%
    total_weight = sum(weights.values())
    normalized_weights = {k: v / total_weight * 100 for k, v in weights.items()}
    
    # Calculate weighted average
    weighted_sum = sum(sector_scores[sector] * normalized_weights[sector] for sector in sector_scores.keys())
    weighted_average = weighted_sum / 100
    
    return round(weighted_average, 1)

# Create explanation of methodology
methodology_card = html.Div([
    html.H3("T2D Pulse Methodology", className="section-subtitle"),
    html.P([
        "The T2D Pulse score is calculated as a ", 
        html.Strong("weighted average"), 
        " of all 14 technology sector scores."
    ]),
    html.P([
        "Each sector score is derived from underlying economic indicators, weighted according to their specific impact on that sector."
    ]),
    html.P([
        "Adjust weights directly on each sector card to customize the T2D Pulse for your investment focus."
    ]),
    html.Div([
        html.Strong("Formula: "),
        "T2D Pulse = Sum(Sector Score × Weight) ÷ Total Weight"
    ], className="formula-box")
], className="methodology-section")

# Initial T2D Pulse calculation
initial_t2d_pulse = calculate_t2d_pulse(sector_scores, default_weights)
initial_pulse_card, initial_status, initial_color = create_pulse_card(initial_t2d_pulse)

# Create the layout
app.layout = html.Div([
    html.Div([
        html.Img(src="assets/images/t2d_pulse_logo.png", className="logo"),
        html.H1("Economic Dashboard", className="dashboard-title")
    ], className="header"),
    
    html.Div([
        # T2D Pulse and Summary Section
        html.Div([
            html.H2("T2D Pulse", className="section-title"),
            
            html.Div([
                # T2D Pulse Card with Glow - now centered on the page
                html.Div([
                    html.Div(id="pulse-card-container", children=initial_pulse_card)
                ], className="pulse-card-container", style={"display": "flex", "justifyContent": "center", "width": "100%"}),
                
                # Explanation Text
                html.Div([
                    html.P(id="pulse-description", children="Based on weighted sector average", 
                          className="pulse-note"),
                    html.Div([
                        html.P("Adjust sector weights to customize the T2D Pulse for your investment focus.", 
                              className="pulse-instructions")
                    ], className="pulse-description-container")
                ], className="pulse-explanation")
            ], className="pulse-content", style={"flexDirection": "column", "alignItems": "center"}),
            
            # Sector Summary
            html.Div([
                html.H3("Sector Summary", className="section-subtitle"),
                html.Div(id="sector-summary", children=create_sector_summary())
            ], className="sector-summary-container")
        ], className="pulse-summary-section"),
        
        # Sector Cards Section
        html.Div([
            html.Div([
                html.H2("Sector Sentiment", className="section-title"),
                html.Div([
                    html.P("Sector scores are calculated from economic indicators weighted by their impact on each sector.", 
                          className="section-description"),
                    html.Button("Reset to Equal Weights", id="reset-weights-button", className="reset-button")
                ], className="section-controls")
            ], className="section-header"),
            
            # Sector Cards Grid
            html.Div(create_sector_cards(sector_scores, default_weights), className="sector-cards-grid")
        ], className="sectors-section"),
        
        # Methodology Section at the bottom
        html.Div([methodology_card], className="footer-section")
    ], className="dashboard-container"),
    
    # Store current weights and pulse score in hidden divs
    html.Div(id="stored-weights", style={"display": "none"}, children=json.dumps(default_weights)),
    html.Div(id="stored-pulse", style={"display": "none"}, children=str(initial_t2d_pulse))
])

# Callback for increasing weight buttons
@app.callback(
    Output("stored-weights", "children"),
    Input({"type": "increase-weight", "index": ALL}, "n_clicks"),
    State("stored-weights", "children"),
    prevent_initial_call=True
)
def increase_weight(n_clicks_list, weights_json):
    # Determine which button was clicked
    if not any(click for click in n_clicks_list if click):
        raise dash.exceptions.PreventUpdate
    
    triggered_id = ctx.triggered_id
    if triggered_id is None:
        raise dash.exceptions.PreventUpdate
    
    sector = triggered_id['index']
    
    # Parse current weights
    weights = json.loads(weights_json)
    
    # Increase the selected sector's weight by 1
    increment_amount = 1.0
    weights[sector] += increment_amount
    
    # Calculate the proportional decrease for other sectors
    sectors_to_adjust = [s for s in weights.keys() if s != sector]
    total_other_weight = sum(weights[s] for s in sectors_to_adjust)
    
    if total_other_weight > 0:
        # Distribute the decrement proportionally
        for s in sectors_to_adjust:
            proportion = weights[s] / total_other_weight
            weights[s] -= increment_amount * proportion
            # Ensure no negative weights
            weights[s] = max(0, weights[s])
    
    # Normalize to ensure sum is 100
    total = sum(weights.values())
    weights = {k: (v/total)*100 for k, v in weights.items()}
    
    return json.dumps(weights)

# Callback for decreasing weight buttons
@app.callback(
    Output("stored-weights", "children", allow_duplicate=True),
    Input({"type": "decrease-weight", "index": ALL}, "n_clicks"),
    State("stored-weights", "children"),
    prevent_initial_call=True
)
def decrease_weight(n_clicks_list, weights_json):
    # Determine which button was clicked
    if not any(click for click in n_clicks_list if click):
        raise dash.exceptions.PreventUpdate
    
    triggered_id = ctx.triggered_id
    if triggered_id is None:
        raise dash.exceptions.PreventUpdate
    
    sector = triggered_id['index']
    
    # Parse current weights
    weights = json.loads(weights_json)
    
    # Don't allow decreasing below 1%
    if weights[sector] <= 1.0:
        raise dash.exceptions.PreventUpdate
    
    # Decrease the selected sector's weight by 1
    decrement_amount = 1.0
    weights[sector] -= decrement_amount
    
    # Calculate the proportional increase for other sectors
    sectors_to_adjust = [s for s in weights.keys() if s != sector]
    total_other_weight = sum(weights[s] for s in sectors_to_adjust)
    
    if total_other_weight > 0:
        # Distribute the increment proportionally
        for s in sectors_to_adjust:
            proportion = weights[s] / total_other_weight
            weights[s] += decrement_amount * proportion
    
    # Normalize to ensure sum is 100
    total = sum(weights.values())
    weights = {k: (v/total)*100 for k, v in weights.items()}
    
    return json.dumps(weights)

# Callback for resetting weights to equal
@app.callback(
    Output("stored-weights", "children", allow_duplicate=True),
    Input("reset-weights-button", "n_clicks"),
    prevent_initial_call=True
)
def reset_weights(n_clicks):
    if n_clicks:
        # Reset to default equal weights
        sector_count = len(sector_scores)
        equal_weights = {sector: 100 / sector_count for sector in sector_scores.keys()}
        
        return json.dumps(equal_weights)
    
    # This should never happen due to prevent_initial_call=True
    raise dash.exceptions.PreventUpdate

# Callback for updating weight displays and pulse score when weights change
@app.callback(
    [Output({"type": "weight-display", "index": sector}, "children") for sector in sector_scores.keys()] +
    [Output("pulse-card-container", "children"),
     Output("stored-pulse", "children")],
    Input("stored-weights", "children")
)
def update_displays(weights_json):
    # Parse weights from JSON
    weights = json.loads(weights_json)
    
    # Calculate new T2D Pulse score
    pulse_score = calculate_t2d_pulse(sector_scores, weights)
    
    # Create new pulse card with glow
    pulse_card, _, _ = create_pulse_card(pulse_score)
    
    # Create outputs for each weight display
    weight_displays = [f"{weights[sector]:.1f}%" for sector in sector_scores.keys()]
    
    # Return all outputs
    return weight_displays + [
        pulse_card,
        str(pulse_score)
    ]

# Custom CSS for the mockup
app.index_string = '''
<!DOCTYPE html>
<html>
    <head>
        {%metas%}
        <title>T2D Pulse - Final Layout</title>
        {%favicon%}
        {%css%}
        <style>
            body {
                font-family: Arial, sans-serif;
                margin: 0;
                padding: 0;
                background-color: #f5f7fa;
            }
            
            .header {
                display: flex;
                align-items: center;
                padding: 20px;
                background-color: white;
                box-shadow: 0 2px 5px rgba(0,0,0,0.1);
            }
            
            .logo {
                height: 60px;
                margin-right: 20px;
            }
            
            .dashboard-title {
                margin: 0;
                color: #2c3e50;
                font-weight: 500;
            }
            
            .dashboard-container {
                max-width: 1400px;
                margin: 20px auto;
                padding: 0 20px;
                display: flex;
                flex-direction: column;
                gap: 30px;
            }
            
            .section-title {
                margin-top: 0;
                margin-bottom: 20px;
                color: #2c3e50;
                font-size: 24px;
                border-bottom: 1px solid #ecf0f1;
                padding-bottom: 10px;
            }
            
            .section-subtitle {
                margin-top: 0;
                color: #2c3e50;
                font-size: 20px;
                margin-bottom: 15px;
            }
            
            .section-description {
                color: #7f8c8d;
                margin: 0 0 15px 0;
            }
            
            .section-controls {
                display: flex;
                justify-content: space-between;
                align-items: center;
                margin-bottom: 20px;
            }
            
            .section-header {
                margin-bottom: 20px;
            }
            
            /* Pulse and Summary Section */
            .pulse-summary-section {
                background-color: white;
                border-radius: 8px;
                padding: 25px;
                box-shadow: 0 2px 5px rgba(0,0,0,0.1);
            }
            
            .pulse-content {
                display: flex;
                margin-bottom: 30px;
            }
            
            .pulse-gauge-container {
                flex: 0 0 55%;
            }
            
            .pulse-details {
                flex: 1;
                display: flex;
                flex-direction: column;
                justify-content: center;
                padding-left: 30px;
            }
            
            .pulse-status {
                font-size: 40px;
                margin: 0 0 15px 0;
            }
            
            .pulse-score {
                font-size: 22px;
                margin: 0 0 10px 0;
            }
            
            .pulse-note {
                font-size: 16px;
                color: #7f8c8d;
                margin: 0 0 10px 0;
            }
            
            .update-date {
                font-size: 14px;
                color: #95a5a6;
                margin: 0;
            }
            
            .sector-summary-container {
                border-top: 1px solid #ecf0f1;
                padding-top: 20px;
            }
            
            .sector-summary-content {
                display: flex;
                gap: 30px;
            }
            
            .summary-section {
                flex: 1;
            }
            
            .summary-title {
                margin-top: 0;
                font-size: 16px;
                color: #34495e;
                margin-bottom: 10px;
            }
            
            .sector-list {
                padding-left: 25px;
                margin: 10px 0;
            }
            
            .sector-name {
                font-weight: bold;
            }
            
            .sector-score-positive {
                color: #2ecc71;
                font-weight: bold;
            }
            
            .sector-score-neutral {
                color: #f39c12;
                font-weight: bold;
            }
            
            .sector-score-negative {
                color: #e74c3c;
                font-weight: bold;
            }
            
            /* Sectors Section */
            .sectors-section {
                background-color: white;
                border-radius: 8px;
                padding: 25px;
                box-shadow: 0 2px 5px rgba(0,0,0,0.1);
            }
            
            .sector-cards-grid {
                display: grid;
                grid-template-columns: repeat(auto-fill, minmax(300px, 1fr));
                gap: 20px;
            }
            
            .sector-card {
                border-radius: 8px;
                overflow: hidden;
                box-shadow: 0 2px 5px rgba(0,0,0,0.05);
            }
            
            .sector-card-header {
                padding: 15px;
                background-color: white;
                border-bottom: 3px solid;
            }
            
            .card-header-content {
                display: flex;
                justify-content: space-between;
                align-items: center;
            }
            
            .sector-card-title {
                margin: 0;
                font-size: 16px;
                font-weight: 500;
                color: #2c3e50;
            }
            
            .score-container {
                text-align: right;
            }
            
            .sector-score {
                display: block;
                font-size: 22px;
                font-weight: bold;
                margin-bottom: 3px;
            }
            
            .sector-sentiment {
                font-size: 14px;
            }
            
            .sector-card-body {
                padding: 15px;
            }
            
            /* Sector Badge */
            .sector-badge {
                display: inline-block;
                padding: 4px 8px;
                border-radius: 4px;
                font-size: 12px;
                font-weight: bold;
                margin-bottom: 10px;
            }
            
            .badge-bullish {
                background-color: #2ecc71;
                color: white;
            }
            
            .badge-neutral {
                background-color: #f39c12;
                color: white;
            }
            
            .badge-bearish {
                background-color: #e74c3c;
                color: white;
            }
            
            /* Sector Score Scale */
            .sector-score-scale {
                height: 6px;
                margin: 10px 0 15px;
                position: relative;
            }
            
            .scale-track {
                height: 100%;
                background: linear-gradient(to right, #e74c3c 0%, #e74c3c 30%, #f39c12 30%, #f39c12 60%, #2ecc71 60%, #2ecc71 100%);
                border-radius: 3px;
                position: relative;
            }
            
            .scale-marker {
                width: 10px;
                height: 10px;
                background-color: white;
                border: 2px solid #34495e;
                border-radius: 50%;
                position: absolute;
                top: 50%;
                transform: translate(-50%, -50%);
                z-index: 1;
            }
            
            /* Takeaway Badge Container */
            .takeaway-badge-container {
                display: flex;
                justify-content: space-between;
                align-items: center;
                margin-bottom: 10px;
            }
            
            /* Sector Takeaway */
            .sector-takeaway {
                font-weight: 500;
                margin: 0;
                font-size: 14px;
                flex: 1;
            }
            
            /* Drivers List */
            .drivers-list {
                margin: 0 0 15px 20px;
                padding: 0;
                font-size: 13px;
                color: #555;
            }
            
            .drivers-list li {
                margin-bottom: 5px;
            }
            
            /* Tickers Container */
            .tickers-container {
                display: flex;
                flex-wrap: wrap;
                gap: 5px;
                margin-bottom: 15px;
            }
            
            .ticker-badge {
                background-color: #eee;
                color: #34495e;
                padding: 3px 8px;
                border-radius: 3px;
                font-size: 12px;
                font-weight: bold;
            }
            
            /* Weight Controls */
            .weight-controls {
                display: flex;
                justify-content: space-between;
                align-items: center;
                border-top: 1px solid rgba(0,0,0,0.1);
                padding-top: 10px;
                margin-top: 5px;
            }
            
            .weight-display-container {
                display: flex;
                align-items: center;
                gap: 5px;
            }
            
            .weight-label {
                font-size: 14px;
                color: #7f8c8d;
            }
            
            .weight-value {
                font-weight: bold;
                color: #3498db;
                font-size: 15px;
            }
            
            .weight-buttons-container {
                display: flex;
                gap: 5px;
            }
            
            .weight-button {
                width: 28px;
                height: 28px;
                border-radius: 4px;
                font-weight: bold;
                cursor: pointer;
                border: none;
                color: white;
                display: flex;
                align-items: center;
                justify-content: center;
                font-size: 16px;
                transition: opacity 0.2s;
            }
            
            .weight-button:hover {
                opacity: 0.85;
            }
            
            .weight-decrease {
                background-color: #e74c3c;
            }
            
            .weight-increase {
                background-color: #2ecc71;
            }
            
            .reset-button {
                background-color: #3498db;
                color: white;
                border: none;
                padding: 8px 16px;
                border-radius: 4px;
                cursor: pointer;
                font-weight: 500;
                transition: background-color 0.3s;
            }
            
            .reset-button:hover {
                background-color: #2980b9;
            }
            
            /* Footer Section */
            .footer-section {
                background-color: white;
                border-radius: 8px;
                padding: 25px;
                box-shadow: 0 2px 5px rgba(0,0,0,0.1);
            }
            
            .methodology-section {
                font-size: 13px;
            }
            
            .methodology-section .section-subtitle {
                font-size: 16px;
            }
            
            .methodology-section p {
                color: #555;
                line-height: 1.5;
                margin-bottom: 12px;
                font-size: 13px;
            }
            
            .formula-box {
                background-color: #f8f9fa;
                padding: 12px;
                border-radius: 5px;
                margin-top: 12px;
                border-left: 4px solid #3498db;
                font-size: 13px;
            }
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

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5008)