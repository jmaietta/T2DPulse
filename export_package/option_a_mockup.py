"""
T2D Pulse Landing Page Mockup - Option A (with Sector Weight Customization)
Equal-weight roll-up of sector scores as the main T2D Pulse score, with customizable weights

This approach:
1. Calculates the main T2D Pulse score as a weighted average of all sector scores
2. Creates perfect alignment between sector scores and the overall pulse
3. Simplifies the relationship between sectors and the main index
4. Adds user control over sector importance weights
"""

import dash
from dash import html, dcc, callback
import plotly.graph_objs as go
import numpy as np
import pandas as pd
import json
from dash.dependencies import Input, Output, State

app = dash.Dash(__name__)

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

# Create sector score summary
def create_sector_summary(sorted_sectors):
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

# Initial T2D Pulse calculation
initial_t2d_pulse = calculate_t2d_pulse(sector_scores, default_weights)
initial_fig, initial_status, initial_color = create_gauge_chart(initial_t2d_pulse)

# Create explanation of methodology
methodology_card = html.Div([
    html.H4("Methodology - Option A (Customizable Weights)", className="card-title"),
    html.P([
        "The T2D Pulse score is calculated as a ", 
        html.Strong("weighted average"), 
        " of all 14 technology sector scores."
    ]),
    html.P([
        "Each sector score is derived from the same underlying economic indicators, but weighted according to their specific impact on that sector."
    ]),
    html.P([
        "Sector weights in the T2D Pulse can be customized to reflect the importance of different technology sectors to your specific business or investment strategy."
    ]),
    html.Div([
        html.Strong("Formula: "),
        "T2D Pulse = Sum(Sector Score × Weight) ÷ Total Weight"
    ], className="formula-box")
], className="methodology-card")

# Create the layout
app.layout = html.Div([
    html.Div([
        html.Img(src="assets/images/t2d_logo.png", className="logo"),
        html.H1("T2D Pulse Economic Dashboard", className="dashboard-title")
    ], className="header"),
    
    html.Div([
        html.Div([
            html.H2("T2D Pulse Score", className="section-title"),
            html.Div([
                html.Div([
                    dcc.Graph(
                        id="pulse-gauge",
                        figure=initial_fig
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
            html.H2("Sector Weight Customization", className="section-title"),
            html.Div([
                html.P("Adjust the importance of each technology sector in the T2D Pulse calculation", 
                       className="customization-description"),
                html.Button("Reset to Equal Weights", id="reset-weights-button", className="reset-button"),
                html.Div(id="weights-container", children=[
                    html.Div([
                        html.Div([
                            html.Label(f"{sector} ({score:.1f})", className="slider-label"),
                            html.Span(f"{default_weights[sector]:.1f}%", id=f"weight-display-{sector.replace(' ', '-').lower()}", 
                                     className="weight-display")
                        ], className="slider-header"),
                        dcc.Slider(
                            id=f"weight-slider-{sector.replace(' ', '-').lower()}",
                            min=0,
                            max=20,
                            step=0.5,
                            value=default_weights[sector],
                            marks={i: f"{i}" for i in range(0, 21, 5)},
                            className="weight-slider"
                        )
                    ], className="slider-container") for sector, score in sector_scores.items()
                ])
            ], className="weights-section")
        ], className="weights-customization-section"),
        
        html.Div([
            html.H2("Sector Summary", className="section-title"),
            html.Div(id="sector-summary", children=create_sector_summary(
                sorted(sector_scores.items(), key=lambda x: x[1], reverse=True)
            ))
        ], className="sector-summary-section"),
        
        html.Div([
            methodology_card
        ], className="methodology-section")
    ], className="dashboard-container"),
    
    # Store current weights and pulse score in hidden divs
    html.Div(id="stored-weights", style={"display": "none"}, children=json.dumps(default_weights)),
    html.Div(id="stored-pulse", style={"display": "none"}, children=str(initial_t2d_pulse))
])

# Callback for updating stored weights when sliders change
@app.callback(
    Output("stored-weights", "children"),
    [Input(f"weight-slider-{sector.replace(' ', '-').lower()}", "value") for sector in sector_scores.keys()],
    [State("stored-weights", "children")]
)
def update_stored_weights(*args):
    slider_values = args[:-1]  # All except the last one, which is the state
    sector_keys = list(sector_scores.keys())
    
    # Create new weights dictionary
    new_weights = {}
    for i, sector in enumerate(sector_keys):
        new_weights[sector] = slider_values[i]
    
    # Convert to string for storage
    import json
    return json.dumps(new_weights)

# Callback for updating weight display values and pulse score when weights change
@app.callback(
    [Output(f"weight-display-{sector.replace(' ', '-').lower()}", "children") for sector in sector_scores.keys()] +
    [Output("pulse-gauge", "figure"),
     Output("pulse-status", "children"),
     Output("pulse-status", "style"),
     Output("pulse-score", "children"),
     Output("stored-pulse", "children")],
    [Input("stored-weights", "children")]
)
def update_pulse_from_weights(weights_json):
    import json
    
    # Parse weights from JSON
    weights = json.loads(weights_json)
    
    # Calculate new T2D Pulse score
    pulse_score = calculate_t2d_pulse(sector_scores, weights)
    
    # Create new gauge chart
    fig, status, color = create_gauge_chart(pulse_score)
    
    # Create outputs for each weight display
    weight_displays = []
    total_weight = sum(weights.values())
    
    for sector in sector_scores.keys():
        # Calculate percentage of total
        pct = (weights[sector] / total_weight) * 100
        weight_displays.append(f"{pct:.1f}%")
    
    # Return all outputs
    return weight_displays + [
        fig, 
        status, 
        {"color": color}, 
        f"Score: {pulse_score}",
        str(pulse_score)
    ]

# Callback for resetting weights to equal
@app.callback(
    Output("stored-weights", "children", allow_duplicate=True),
    [Input("reset-weights-button", "n_clicks")],
    prevent_initial_call=True
)
def reset_weights(n_clicks):
    if n_clicks:
        # Reset to default equal weights
        sector_count = len(sector_scores)
        equal_weights = {sector: 100 / sector_count for sector in sector_scores.keys()}
        
        # Convert to string for storage
        import json
        return json.dumps(equal_weights)
    
    # This should never happen due to prevent_initial_call=True
    raise dash.exceptions.PreventUpdate

# Custom CSS for the mockup
app.index_string = '''
<!DOCTYPE html>
<html>
    <head>
        {%metas%}
        <title>T2D Pulse - Option A Mockup</title>
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
                max-width: 1200px;
                margin: 20px auto;
                padding: 0 20px;
                display: grid;
                grid-template-columns: 1fr 1fr;
                grid-gap: 20px;
            }
            
            .main-score-section {
                grid-column: 1 / -1;
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
            
            .sector-summary-section {
                background-color: white;
                border-radius: 8px;
                padding: 20px;
                box-shadow: 0 2px 5px rgba(0,0,0,0.1);
            }
            
            .methodology-section {
                background-color: white;
                border-radius: 8px;
                padding: 20px;
                box-shadow: 0 2px 5px rgba(0,0,0,0.1);
            }
            
            .sector-list {
                padding-left: 20px;
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
            
            .weights-customization-section {
                grid-column: 1 / -1;
                background-color: white;
                border-radius: 8px;
                padding: 20px;
                box-shadow: 0 2px 5px rgba(0,0,0,0.1);
                margin-bottom: 20px;
            }
            
            .customization-description {
                margin-top: 0;
                margin-bottom: 20px;
                color: #7f8c8d;
            }
            
            .weights-section {
                display: grid;
                grid-template-columns: repeat(auto-fill, minmax(300px, 1fr));
                grid-gap: 20px;
            }
            
            .slider-container {
                margin-bottom: 15px;
            }
            
            .slider-header {
                display: flex;
                justify-content: space-between;
                margin-bottom: 5px;
            }
            
            .slider-label {
                font-weight: bold;
                font-size: 14px;
                color: #2c3e50;
            }
            
            .weight-display {
                font-weight: bold;
                color: #3498db;
            }
            
            .weight-slider {
                margin-top: 5px;
            }
            
            .reset-button {
                background-color: #3498db;
                color: white;
                border: none;
                padding: 8px 15px;
                border-radius: 4px;
                cursor: pointer;
                font-weight: bold;
                margin-bottom: 20px;
                transition: background-color 0.3s;
            }
            
            .reset-button:hover {
                background-color: #2980b9;
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
    app.run(debug=True, host='0.0.0.0', port=5005)