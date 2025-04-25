"""
T2D Pulse Landing Page Mockup - Option A
Equal-weight roll-up of sector scores as the main T2D Pulse score

This approach:
1. Calculates the main T2D Pulse score as the average of all sector scores
2. Creates perfect alignment between sector scores and the overall pulse
3. Simplifies the relationship between sectors and the main index
"""

import dash
from dash import html, dcc
import plotly.graph_objs as go
import numpy as np
import pandas as pd
from dash.dependencies import Input, Output

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
    
    # Calculate T2D Pulse score as average of sector scores
    t2d_pulse_score = round(sum(sectors.values()) / len(sectors), 1)
    
    return sectors, t2d_pulse_score

# Get sample data
sector_scores, t2d_pulse_score = generate_sample_data()

# Determine Pulse status based on score
if t2d_pulse_score >= 80:
    pulse_status = "Boom"
    pulse_color = "#2ecc71"  # Green
elif t2d_pulse_score >= 60:
    pulse_status = "Expansion"
    pulse_color = "#f1c40f"  # Yellow
elif t2d_pulse_score >= 40:
    pulse_status = "Neutral"
    pulse_color = "#f39c12"  # Orange
elif t2d_pulse_score >= 20:
    pulse_status = "Caution"
    pulse_color = "#e74c3c"  # Light Red
else:
    pulse_status = "Contraction"
    pulse_color = "#c0392b"  # Dark Red

# Create gauge chart for T2D Pulse
def create_gauge_chart(value, title):
    fig = go.Figure(go.Indicator(
        mode="gauge+number",
        value=value,
        domain={'x': [0, 1], 'y': [0, 1]},
        title={'text': title},
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
    
    return fig

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

# Create explanation of methodology
methodology_card = html.Div([
    html.H4("Methodology - Option A", className="card-title"),
    html.P([
        "The T2D Pulse score is calculated as an ", 
        html.Strong("equal-weighted average"), 
        " of all 14 technology sector scores."
    ]),
    html.P([
        "Each sector score is derived from the same underlying economic indicators, but weighted according to their specific impact on that sector."
    ]),
    html.P([
        "This approach ensures the overall T2D Pulse score directly reflects the health of the technology sector ecosystem as a whole."
    ]),
    html.Div([
        html.Strong("Formula: "),
        "T2D Pulse = Average(Sector Scores)"
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
                        figure=create_gauge_chart(t2d_pulse_score, "T2D Pulse")
                    )
                ], className="gauge-container"),
                html.Div([
                    html.H3(pulse_status, className="pulse-status", style={"color": pulse_color}),
                    html.P(f"Score: {t2d_pulse_score}", className="pulse-score"),
                    html.P("Based on equal-weighted sector average", className="pulse-note"),
                    html.P(f"Last updated: April 25, 2025", className="update-date")
                ], className="pulse-details")
            ], className="pulse-container")
        ], className="main-score-section"),
        
        html.Div([
            html.H2("Sector Summary", className="section-title"),
            create_sector_summary()
        ], className="sector-summary-section"),
        
        html.Div([
            methodology_card
        ], className="methodology-section")
    ], className="dashboard-container")
])

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