"""
Normalized Sector Sentiment Mockup for T2D Pulse dashboard
Converts the current -1 to +1 scale to a 0-100 scale to match the T2D Pulse Sentiment Index
"""

import dash
from dash import dcc, html
import plotly.graph_objs as go
import numpy as np
import pandas as pd
from dash.dependencies import Input, Output, State

# Import current sector scores calculation from sentiment_engine
from sentiment_engine import score_sectors, SECTORS, IMPACT, IMPORTANCE, BANDS, raw_signal

# Example values for mockup - these match the actual ranges in sentiment_engine.py
EXAMPLE_SCORES = [
    {"sector": "SMB SaaS", "score": -0.48},
    {"sector": "Enterprise SaaS", "score": -0.35},
    {"sector": "Cloud Infrastructure", "score": -0.22},
    {"sector": "AdTech", "score": -0.08},
    {"sector": "Fintech", "score": 0.02},
    {"sector": "Consumer Internet", "score": 0.15},
    {"sector": "eCommerce", "score": 0.25},
    {"sector": "Cybersecurity", "score": 0.30},
    {"sector": "Dev Tools / Analytics", "score": 0.40},
    {"sector": "Semiconductors", "score": 0.55},
    {"sector": "AI Infrastructure", "score": 0.65},
    {"sector": "Vertical SaaS", "score": 0.12},
    {"sector": "IT Services / Legacy Tech", "score": -0.15},
    {"sector": "Hardware / Devices", "score": -0.05}
]

# Function to normalize sector scores to 0-100 scale
def normalize_sector_scores(sector_scores, min_score=-1.0, max_score=1.0, 
                            target_min=0, target_max=100):
    """
    Normalize sector scores from original scale (typically -1 to +1) 
    to a 0-100 scale similar to the main T2D Pulse score.
    
    Args:
        sector_scores (list): List of sector score dictionaries
        min_score (float): Minimum value in original scale
        max_score (float): Maximum value in original scale
        target_min (int): Minimum value in target scale (default 0)
        target_max (int): Maximum value in target scale (default 100)
    
    Returns:
        list: List of dictionaries with normalized scores
    """
    normalized_scores = []
    
    for sector_data in sector_scores:
        # Get the original score
        orig_score = sector_data["score"]
        
        # Normalize to 0-100 scale
        # Formula: new_value = ((old_value - old_min) / (old_max - old_min)) * (new_max - new_min) + new_min
        norm_score = ((orig_score - min_score) / (max_score - min_score)) * (target_max - target_min) + target_min
        
        # Round to 1 decimal place
        norm_score = round(norm_score, 1)
        
        # Determine stance based on normalized score
        if norm_score <= 30:
            stance = "Bearish"
            takeaway = "Bearish macro setup"
        elif norm_score >= 60:
            stance = "Bullish"
            takeaway = "Outperforming peers"
        else:
            stance = "Neutral"
            takeaway = "Neutral – monitor trends"
        
        # Add to normalized scores list
        normalized_scores.append({
            "sector": sector_data["sector"],
            "original_score": orig_score,
            "normalized_score": norm_score,
            "stance": stance,
            "takeaway": takeaway,
            # Sample drivers for mockup
            "drivers": [
                f"Strong signal from {'revenue growth' if norm_score > 50 else 'market trends'}",
                f"{'Positive' if norm_score > 50 else 'Negative'} impact from economic indicators",
                f"Monitor {'opportunities' if norm_score > 50 else 'headwinds'} in coming quarter"
            ],
            # Sample tickers for mockup - these would be pulled from actual data
            "tickers": generate_sample_tickers(sector_data["sector"])
        })
    
    return normalized_scores

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
    else:
        # Generate random tickers for other sectors
        return [f"{sector[0:2].upper()}{i}" for i in range(1, 4)]

def create_sector_card(sector_data, use_normalized=True):
    """
    Create a sector card with either original or normalized score
    
    Args:
        sector_data (dict): Sector data dictionary
        use_normalized (bool): Whether to use normalized scores
    
    Returns:
        html.Div: Dash component for sector card
    """
    # Extract data
    sector = sector_data["sector"]
    
    if use_normalized:
        score = sector_data["normalized_score"]
        # Format score for display with no + sign for normalized scores
        score_display = f"{score:.1f}"
        # Score class based on normalized range
        if score >= 60:
            score_class = "score-positive"
        elif score <= 30:
            score_class = "score-negative"
        else:
            score_class = "score-neutral"
    else:
        # For original scores, use the "score" key directly from EXAMPLE_SCORES
        score = sector_data["score"]
        # Format score for display with + sign for positive original scores
        score_display = f"{score:+.2f}" if score > 0 else f"{score:.2f}"
        # Score class based on original range
        if score >= 0.05:
            score_class = "score-positive"
        elif score <= -0.25:
            score_class = "score-negative"
        else:
            score_class = "score-neutral"
    
    # Determine stance based on score if not provided
    if "stance" in sector_data:
        stance = sector_data["stance"]
    else:
        if score >= 0.05 or (use_normalized and score >= 60):
            stance = "Bullish"
        elif score <= -0.25 or (use_normalized and score <= 30):
            stance = "Bearish"
        else:
            stance = "Neutral"
    
    # Determine takeaway based on stance if not provided
    if "takeaway" in sector_data:
        takeaway = sector_data["takeaway"]
    else:
        if stance == "Bullish":
            takeaway = "Outperforming peers"
        elif stance == "Bearish":
            takeaway = "Bearish macro setup"
        else:
            takeaway = "Neutral – monitor trends"
    
    # Use provided drivers or generate sample ones
    if "drivers" in sector_data:
        drivers = sector_data["drivers"]
    else:
        drivers = [
            f"Strong signal from {'revenue growth' if score > 0 else 'market trends'}",
            f"{'Positive' if score > 0 else 'Negative'} impact from economic indicators",
            f"Monitor {'opportunities' if score > 0 else 'headwinds'} in coming quarter"
        ]
    
    # Use provided tickers or generate sample ones
    if "tickers" in sector_data:
        tickers = sector_data["tickers"]
    else:
        tickers = generate_sample_tickers(sector)
    
    # Determine badge styling based on stance
    if stance == "Bullish":
        badge_class = "badge-bullish"
    elif stance == "Bearish":
        badge_class = "badge-bearish"
    else:
        badge_class = "badge-neutral"
    
    # Create the sector card
    card = html.Div([
        # Header with sector name and score
        html.Div([
            html.Span(sector, className="sector-name"),
            html.Span(score_display, className=f"sector-score {score_class}")
        ], className="sector-card-header"),
        
        # Stance badge
        html.Span(stance, className=f"sector-badge {badge_class}"),
        
        # Scale indicator (only for normalized scores)
        html.Div([
            html.Div([
                html.Div(className="scale-marker", 
                         style={"left": f"{min(max(score, 0), 100)}%"})
            ], className="scale-track")
        ], className="sector-score-scale") if use_normalized else None,
        
        # Takeaway text
        html.P(takeaway, className="sector-takeaway"),
        
        # Drivers list
        html.Ul([
            html.Li(driver) for driver in drivers
        ], className="drivers-list"),
        
        # Tickers
        html.Div([
            html.Span(ticker, className="ticker-badge") for ticker in tickers
        ], className="tickers-container")
        
    ], className="sector-card")
    
    return card

# Create a scale legend for normalized scores
def create_normalized_scale_legend():
    """Create a legend for the normalized 0-100 scale"""
    return html.Div([
        html.Div([
            "Sector Sentiment Scale (0-100):",
            html.Span(" (Normalized to match T2D Pulse Sentiment Index)", 
                      className="scale-title-note")
        ], className="scale-title"),
        html.Div([
            html.Div([
                html.Span("0", className="scale-min"),
                html.Span("50", className="scale-mid"),
                html.Span("100", className="scale-max")
            ], className="scale-numbers"),
            html.Div([
                html.Div(className="scale-bar-bearish"),
                html.Div(className="scale-bar-neutral"),
                html.Div(className="scale-bar-bullish")
            ], className="scale-bars")
        ], className="scale-container"),
        html.Div([
            html.Div(["Bearish", html.Span("0-30", className="scale-range")], 
                     className="scale-label bearish"),
            html.Div(["Neutral", html.Span("30-60", className="scale-range")], 
                     className="scale-label neutral"),
            html.Div(["Bullish", html.Span("60-100", className="scale-range")], 
                     className="scale-label bullish")
        ], className="scale-labels")
    ], className="sector-scale-legend")

# Create a comparison view showing both original and normalized scores
def create_comparison_view():
    """Create a comparison view of original vs normalized sector scores"""
    # Normalize the example scores
    normalized_scores = normalize_sector_scores(EXAMPLE_SCORES)
    
    # Create visualization showing the relationship between original and normalized scores
    x_orig = [s["score"] for s in EXAMPLE_SCORES]  # Original scores from example
    y_norm = [s["normalized_score"] for s in normalized_scores]  # Normalized scores
    sector_labels = [s["sector"] for s in EXAMPLE_SCORES]
    
    mapping_fig = go.Figure()
    
    # Add scatter plot for the scores
    mapping_fig.add_trace(go.Scatter(
        x=x_orig,
        y=y_norm,
        mode='markers+text',
        text=sector_labels,
        textposition="top center",
        marker=dict(
            size=10,
            color=y_norm,
            colorscale='RdYlGn',
            colorbar=dict(title="Normalized Score"),
            cmin=0,
            cmax=100
        ),
        hovertemplate="<b>%{text}</b><br>Original: %{x:.2f}<br>Normalized: %{y:.1f}<extra></extra>"
    ))
    
    # Add reference line showing the linear mapping
    mapping_fig.add_trace(go.Scatter(
        x=[-1, 1],
        y=[0, 100],
        mode='lines',
        line=dict(color='rgba(0,0,0,0.3)', dash='dash'),
        name='Mapping Line',
        hoverinfo='skip'
    ))
    
    # Update layout
    mapping_fig.update_layout(
        title="Mapping Between Original (-1 to +1) and Normalized (0-100) Scores",
        xaxis_title="Original Score (-1 to +1)",
        yaxis_title="Normalized Score (0-100)",
        xaxis=dict(
            zeroline=True,
            zerolinecolor='rgba(0,0,0,0.2)',
            range=[-1.1, 1.1]
        ),
        yaxis=dict(
            range=[-5, 105]
        ),
        height=500,
        plot_bgcolor='rgba(240,240,240,0.6)',
        hovermode='closest'
    )
    
    # Create side-by-side comparison card examples
    # Find Cybersecurity index
    cyber_idx = next((i for i, s in enumerate(EXAMPLE_SCORES) if s["sector"] == "Cybersecurity"), 7)
    
    original_card = html.Div([
        html.H3("Original Score Format (-1 to +1 scale)"),
        create_sector_card(EXAMPLE_SCORES[cyber_idx], use_normalized=False)  # Cybersecurity example
    ], style={"flex": "1"})
    
    normalized_card = html.Div([
        html.H3("Normalized Score Format (0-100 scale)"),
        create_sector_card(normalized_scores[cyber_idx], use_normalized=True)  # Cybersecurity example
    ], style={"flex": "1"})
    
    # Full comparison view
    return html.Div([
        html.H1("T2D Pulse Sector Sentiment Score Normalization Mockup"),
        html.P("This mockup demonstrates how to normalize sector scores from the original -1 to +1 scale to a 0-100 scale that aligns with the T2D Pulse Sentiment Index."),
        
        html.Div([
            html.Div([
                html.H2("Score Distribution Visualization"),
                dcc.Graph(figure=mapping_fig)
            ]),
            
            html.Div([
                html.H2("Card Format Comparison"),
                html.Div([original_card, normalized_card], style={"display": "flex", "gap": "20px"})
            ]),
            
            html.Div([
                html.H2("Scale Legend Comparison"),
                html.Div([
                    html.Div([
                        html.H3("Original Scale"),
                        html.Div([
                            html.Div([
                                "Sector Sentiment Scale:",
                                html.Span(" (Original -1 to +1 scale)", className="scale-title-note")
                            ], className="scale-title"),
                            html.Div([
                                html.Div([
                                    html.Span("-1.0", className="scale-min"),
                                    html.Span("0", className="scale-mid"),
                                    html.Span("+1.0", className="scale-max")
                                ], className="scale-numbers"),
                                html.Div([
                                    html.Div(className="scale-bar-bearish"),
                                    html.Div(className="scale-bar-neutral"),
                                    html.Div(className="scale-bar-bullish")
                                ], className="scale-bars")
                            ], className="scale-container"),
                            html.Div([
                                html.Div(["Bearish", html.Span("< -0.25", className="scale-range")], 
                                        className="scale-label bearish"),
                                html.Div(["Neutral", html.Span("-0.25 to +0.05", className="scale-range")], 
                                        className="scale-label neutral"),
                                html.Div(["Bullish", html.Span("> +0.05", className="scale-range")], 
                                        className="scale-label bullish")
                            ], className="scale-labels")
                        ], className="sector-scale-legend")
                    ], style={"flex": "1"}),
                    html.Div([
                        html.H3("Normalized Scale"),
                        create_normalized_scale_legend()
                    ], style={"flex": "1"})
                ], style={"display": "flex", "gap": "20px"})
            ]),
            
            html.Div([
                html.H2("Full Sector Card Display (Normalized 0-100 Scale)"),
                create_normalized_scale_legend(),
                html.Div([
                    create_sector_card(s, use_normalized=True) 
                    for s in normalized_scores
                ], className="sector-cards-container", style={
                    "display": "grid",
                    "gridTemplateColumns": "repeat(auto-fill, minmax(300px, 1fr))",
                    "gap": "20px",
                    "marginTop": "20px"
                })
            ])
        ])
    ])

# Initialize the Dash app
app = dash.Dash(__name__)

# Set the app layout
app.layout = create_comparison_view()

# CSS styles for the mockup
app.index_string = """
<!DOCTYPE html>
<html>
    <head>
        <meta charset="utf-8">
        <title>T2D Pulse Normalized Sector Sentiment Mockup</title>
        <style>
            body {
                font-family: Arial, sans-serif;
                max-width: 1200px;
                margin: 0 auto;
                padding: 20px;
                background-color: #f8f9fa;
            }
            .sector-card {
                background: white;
                border-radius: 8px;
                padding: 15px;
                box-shadow: 0 2px 10px rgba(0, 0, 0, 0.08);
                position: relative;
            }
            .sector-card-header {
                display: flex;
                justify-content: space-between;
                align-items: center;
                margin-bottom: 10px;
            }
            .sector-name {
                font-weight: bold;
                font-size: 18px;
            }
            .sector-score {
                font-size: 18px;
                font-weight: bold;
            }
            .score-positive {
                color: #28a745;
            }
            .score-negative {
                color: #dc3545;
            }
            .score-neutral {
                color: #6c757d;
            }
            .sector-badge {
                position: absolute;
                top: 15px;
                right: 15px;
                padding: 4px 10px;
                border-radius: 12px;
                font-size: 12px;
                font-weight: bold;
            }
            .badge-bullish {
                background-color: rgba(40, 167, 69, 0.15);
                color: #28a745;
            }
            .badge-bearish {
                background-color: rgba(220, 53, 69, 0.15);
                color: #dc3545;
            }
            .badge-neutral {
                background-color: rgba(108, 117, 125, 0.15);
                color: #6c757d;
            }
            .sector-takeaway {
                margin: 12px 0;
                font-style: italic;
                color: #666;
            }
            .drivers-list {
                padding-left: 20px;
                margin: 10px 0;
                color: #444;
            }
            .drivers-list li {
                margin-bottom: 5px;
            }
            .tickers-container {
                display: flex;
                flex-wrap: wrap;
                gap: 8px;
                margin-top: 15px;
                justify-content: left;
                align-items: flex-end;
                position: relative;
                bottom: 0;
            }
            .ticker-badge {
                background-color: #f0f0f0;
                color: #333;
                padding: 4px 8px;
                border-radius: 4px;
                font-size: 12px;
                font-family: monospace;
            }
            .sector-scale-legend {
                background: white;
                border-radius: 8px;
                padding: 15px;
                box-shadow: 0 2px 6px rgba(0, 0, 0, 0.05);
                margin-bottom: 20px;
            }
            .scale-title {
                font-weight: bold;
                margin-bottom: 10px;
            }
            .scale-title-note {
                font-weight: normal;
                font-size: 0.9em;
                color: #666;
            }
            .scale-container {
                margin-bottom: 5px;
            }
            .scale-numbers {
                display: flex;
                justify-content: space-between;
                margin-bottom: 3px;
            }
            .scale-bars {
                display: flex;
                height: 8px;
                overflow: hidden;
                border-radius: 4px;
            }
            .scale-bar-bearish {
                flex: 1;
                background-color: rgba(220, 53, 69, 0.7);
            }
            .scale-bar-neutral {
                flex: 1;
                background-color: rgba(255, 193, 7, 0.7);
            }
            .scale-bar-bullish {
                flex: 1;
                background-color: rgba(40, 167, 69, 0.7);
            }
            .scale-labels {
                display: flex;
                justify-content: space-between;
                margin-top: 5px;
            }
            .scale-label {
                font-size: 0.9em;
                font-weight: bold;
            }
            .scale-label.bearish {
                color: #dc3545;
            }
            .scale-label.neutral {
                color: #856404;
            }
            .scale-label.bullish {
                color: #28a745;
            }
            .scale-range {
                font-weight: normal;
                margin-left: 5px;
                font-size: 0.8em;
                color: #666;
            }
            .sector-score-scale {
                height: 8px;
                background: #f0f0f0;
                border-radius: 4px;
                margin: 15px 0;
                position: relative;
                overflow: hidden;
            }
            .scale-track {
                width: 100%;
                height: 100%;
                background: linear-gradient(to right, #dc3545, #ffc107, #28a745);
                position: relative;
            }
            .scale-marker {
                position: absolute;
                top: -3px;
                width: 4px;
                height: 14px;
                background: #333;
                transform: translateX(-50%);
            }
        </style>
        {%metas%}
        {%favicon%}
        {%css%}
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
"""

# Run the app
if __name__ == '__main__':
    # Start the server on port 5002
    app.run(debug=True, host='0.0.0.0', port=5002)