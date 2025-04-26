"""
T2D Pulse Landing Page - Revised Sector-Driven Layout
- Sector cards displayed on the left side
- Weight adjustments within each sector card
- Automatic rebalancing of other sector weights when one is adjusted
- T2D Pulse score driven by weighted sector average
- Sector Summary and Methodology displayed beside the Pulse Score
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

# Create gauge chart for T2D Pulse
def create_gauge_chart(value):
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
        
    fig = go.Figure(go.Indicator(
        mode="gauge+number",
        value=value,
        domain={'x': [0, 1], 'y': [0, 1]},
        number={"suffix": "",
                "font": {"size": 24}},
        title={'text': "T2D Pulse", "font": {"size": 20}},
        gauge={
            'axis': {'range': [0, 100], 'tickwidth': 1},
            'bar': {'color': pulse_color},
            'steps': [
                {'range': [0, 20], 'color': "#c0392b"},  # Dark Red
                {'range': [20, 40], 'color': "#e74c3c"},  # Light Red
                {'range': [40, 60], 'color': "#f39c12"},  # Orange
                {'range': [60, 80], 'color': "#f1c40f"},  # Yellow
                {'range': [80, 100], 'color': "#2ecc71"}  # Green
            ],
            'threshold': {
                'line': {'color': "white", 'width': 4},
                'thickness': 0.75,
                'value': value
            }
        }
    ))
    
    fig.update_layout(
        height=300,
        margin=dict(l=30, r=30, t=30, b=30)
    )
    
    return fig, pulse_status, pulse_color

# Create sector cards with embedded weight controls
def create_sector_cards(sector_scores, weights):
    cards = []
    
    for sector, score in sector_scores.items():
        # Determine status color based on score
        if score >= 60:
            status_class = "sector-score-positive"
            bg_class = "sector-card-positive"
        elif score >= 40:
            status_class = "sector-score-neutral"
            bg_class = "sector-card-neutral"
        else:
            status_class = "sector-score-negative"
            bg_class = "sector-card-negative"
        
        # Calculate percentage weight
        weight_pct = weights[sector]
        
        # Create the sector card
        card = html.Div([
            html.Div([
                html.H3(sector, className="sector-card-title"),
                html.Div(f"{score:.1f}", className=f"sector-card-score {status_class}")
            ], className="sector-card-header"),
            
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
                               className="weight-button"),
                    html.Button("+", 
                               id={"type": "increase-weight", "index": sector},
                               className="weight-button")
                ], className="weight-buttons-container")
            ], className="sector-card-footer")
        ], className=f"sector-card {bg_class}")
        
        cards.append(card)
    
    return html.Div(cards, className="sectors-container")

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
    
    return html.Div([top_sectors, bottom_sectors], className="sector-summary")

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
    html.H4("T2D Pulse Methodology", className="card-title"),
    html.P([
        "The T2D Pulse score is calculated as a ", 
        html.Strong("weighted average"), 
        " of all 14 technology sector scores."
    ]),
    html.P([
        "Each sector score is derived from the same underlying economic indicators, but weighted according to their specific impact on that sector."
    ]),
    html.P([
        "Adjust weights directly on each sector card to customize the T2D Pulse for your specific business or investment focus."
    ]),
    html.Div([
        html.Strong("Formula: "),
        "T2D Pulse = Sum(Sector Score × Weight) ÷ Total Weight"
    ], className="formula-box")
], className="methodology-card")

# Initial T2D Pulse calculation
initial_t2d_pulse = calculate_t2d_pulse(sector_scores, default_weights)
initial_fig, initial_status, initial_color = create_gauge_chart(initial_t2d_pulse)

# Create the layout
app.layout = html.Div([
    html.Div([
        html.Img(src="assets/images/t2d_logo.png", className="logo"),
        html.H1("T2D Pulse Economic Dashboard", className="dashboard-title")
    ], className="header"),
    
    html.Div([
        html.Div([
            html.H2("Sector Sentiment", className="section-title"),
            create_sector_cards(sector_scores, default_weights),
            html.Button("Reset to Equal Weights", id="reset-weights-button", className="reset-button")
        ], className="sectors-column"),
        
        html.Div([
            html.Div([
                html.H2("T2D Pulse Score", className="section-title"),
                html.Div([
                    html.Div([
                        dcc.Graph(
                            id="pulse-gauge",
                            figure=initial_fig,
                            config={'displayModeBar': False}
                        )
                    ], className="gauge-container"),
                    html.Div([
                        html.H3(id="pulse-status", children=initial_status, className="pulse-status", style={"color": initial_color}),
                        html.P(id="pulse-score", children=f"Score: {initial_t2d_pulse}", className="pulse-score"),
                        html.P(id="pulse-description", children="Based on weighted sector average", className="pulse-note"),
                        html.P(f"Last updated: April 25, 2025", className="update-date")
                    ], className="pulse-details")
                ], className="pulse-container")
            ], className="main-score-section"),
            
            html.Div([
                html.Div([
                    html.H2("Sector Summary", className="section-title"),
                    html.Div(id="sector-summary", children=create_sector_summary())
                ], className="sector-summary-section"),
                
                html.Div([
                    html.H2("Methodology", className="section-title"),
                    methodology_card
                ], className="methodology-section")
            ], className="info-sections-container")
        ], className="main-column")
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
    [Output("pulse-gauge", "figure"),
     Output("pulse-status", "children"),
     Output("pulse-status", "style"),
     Output("pulse-score", "children"),
     Output("stored-pulse", "children")],
    Input("stored-weights", "children")
)
def update_displays(weights_json):
    # Parse weights from JSON
    weights = json.loads(weights_json)
    
    # Calculate new T2D Pulse score
    pulse_score = calculate_t2d_pulse(sector_scores, weights)
    
    # Create new gauge chart
    fig, status, color = create_gauge_chart(pulse_score)
    
    # Create outputs for each weight display
    weight_displays = [f"{weights[sector]:.1f}%" for sector in sector_scores.keys()]
    
    # Return all outputs
    return weight_displays + [
        fig, 
        status, 
        {"color": color}, 
        f"Score: {pulse_score}",
        str(pulse_score)
    ]

# Custom CSS for the mockup
app.index_string = '''
<!DOCTYPE html>
<html>
    <head>
        {%metas%}
        <title>T2D Pulse - Revised Mockup</title>
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
                height: 40px;
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
                gap: 20px;
            }
            
            .sectors-column {
                flex: 0 0 300px;
                background-color: white;
                border-radius: 8px;
                padding: 20px;
                box-shadow: 0 2px 5px rgba(0,0,0,0.1);
                max-height: calc(100vh - 100px);
                overflow-y: auto;
            }
            
            .main-column {
                flex: 1;
                display: flex;
                flex-direction: column;
                gap: 20px;
            }
            
            .info-sections-container {
                display: grid;
                grid-template-columns: 1fr 1fr;
                gap: 20px;
            }
            
            .main-score-section {
                background-color: white;
                border-radius: 8px;
                padding: 20px;
                box-shadow: 0 2px 5px rgba(0,0,0,0.1);
            }
            
            .section-title {
                margin-top: 0;
                color: #2c3e50;
                font-size: 20px;
                border-bottom: 1px solid #ecf0f1;
                padding-bottom: 10px;
            }
            
            .pulse-container {
                display: flex;
                align-items: center;
            }
            
            .gauge-container {
                flex: 1;
            }
            
            .pulse-details {
                flex: 1;
                display: flex;
                flex-direction: column;
                justify-content: center;
                padding-left: 20px;
            }
            
            .pulse-status {
                font-size: 32px;
                margin: 0 0 10px 0;
            }
            
            .pulse-score {
                font-size: 20px;
                margin: 0 0 5px 0;
            }
            
            .pulse-note {
                font-size: 14px;
                color: #7f8c8d;
                margin: 0 0 5px 0;
            }
            
            .update-date {
                font-size: 12px;
                color: #95a5a6;
                margin: 0;
            }
            
            .sector-card {
                background-color: white;
                border-radius: 8px;
                margin-bottom: 10px;
                box-shadow: 0 2px 5px rgba(0,0,0,0.05);
                overflow: hidden;
                border-left: 4px solid #95a5a6;
            }
            
            .sector-card-positive {
                border-left-color: #2ecc71;
            }
            
            .sector-card-neutral {
                border-left-color: #f39c12;
            }
            
            .sector-card-negative {
                border-left-color: #e74c3c;
            }
            
            .sector-card-header {
                padding: 12px 15px;
                display: flex;
                justify-content: space-between;
                align-items: center;
            }
            
            .sector-card-footer {
                padding: 8px 15px;
                background-color: #f8f9fa;
                border-top: 1px solid #ecf0f1;
                display: flex;
                justify-content: space-between;
                align-items: center;
            }
            
            .sector-card-title {
                margin: 0;
                font-size: 16px;
                color: #2c3e50;
            }
            
            .sector-card-score {
                font-weight: bold;
                font-size: 18px;
            }
            
            .sector-score-positive {
                color: #2ecc71;
            }
            
            .sector-score-neutral {
                color: #f39c12;
            }
            
            .sector-score-negative {
                color: #e74c3c;
            }
            
            .weight-display-container {
                display: flex;
                align-items: center;
                gap: 5px;
            }
            
            .weight-label {
                font-size: 12px;
                color: #7f8c8d;
            }
            
            .weight-value {
                font-weight: bold;
                color: #3498db;
                font-size: 13px;
            }
            
            .weight-buttons-container {
                display: flex;
                gap: 5px;
            }
            
            .weight-button {
                background-color: #3498db;
                color: white;
                border: none;
                width: 24px;
                height: 24px;
                border-radius: 4px;
                font-weight: bold;
                cursor: pointer;
                transition: background-color 0.2s;
                display: flex;
                align-items: center;
                justify-content: center;
                font-size: 14px;
            }
            
            .weight-button:hover {
                background-color: #2980b9;
            }
            
            .sector-summary-section, .methodology-section {
                background-color: white;
                border-radius: 8px;
                padding: 20px;
                box-shadow: 0 2px 5px rgba(0,0,0,0.1);
            }
            
            .sector-list {
                padding-left: 20px;
                margin: 10px 0;
            }
            
            .sector-name {
                font-weight: bold;
            }
            
            .summary-title {
                margin-top: 0;
                font-size: 16px;
                color: #34495e;
            }
            
            .card-title {
                margin-top: 0;
                font-size: 16px;
                color: #34495e;
            }
            
            .formula-box {
                background-color: #f8f9fa;
                padding: 10px;
                border-radius: 5px;
                margin-top: 15px;
                border-left: 4px solid #3498db;
            }
            
            .reset-button {
                background-color: #7f8c8d;
                color: white;
                border: none;
                padding: 8px 12px;
                border-radius: 4px;
                cursor: pointer;
                font-size: 13px;
                margin-top: 10px;
                width: 100%;
                transition: background-color 0.3s;
            }
            
            .reset-button:hover {
                background-color: #95a5a6;
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
    app.run(debug=True, host='0.0.0.0', port=5007)