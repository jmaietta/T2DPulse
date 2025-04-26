"""
Pulse Card Implementation for T2D Pulse Dashboard
"""
from datetime import datetime
import dash
from dash import html, dcc

def create_pulse_card(value):
    """Create a square pulse card with glow effect based on score"""
    try:
        score_value = float(value)
    except (ValueError, TypeError):
        score_value = 0
        
    # Determine Pulse status based on score
    if score_value >= 80:
        pulse_status = "Boom"
        pulse_color = "#2ECC71"  # Green
    elif score_value >= 60:
        pulse_status = "Expansion"
        pulse_color = "#F1C40F"  # Yellow
    elif score_value >= 40:
        pulse_status = "Moderate Growth"
        pulse_color = "#E67E22"  # Orange
    elif score_value >= 20:
        pulse_status = "Slowdown"
        pulse_color = "#E74C3C"  # Light Red
    else:
        pulse_status = "Contraction"
        pulse_color = "#C0392B"  # Dark Red
    
    # Create the pulse card component
    pulse_card = html.Div([
        # Container with vertical centering for all elements
        html.Div([
            # Logo image with responsive sizing
            html.Img(
                src="/assets/T2D Pulse logo.png",
                style={
                    "width": "100%",    # Fill container width
                    "maxWidth": "350px", # Cap maximum size
                    "height": "auto",   # Maintain aspect ratio
                    "marginBottom": "5px",
                    "objectFit": "contain",
                    "display": "block", # Ensures proper centering
                    "marginLeft": "auto",
                    "marginRight": "auto"
                }
            ),
            # Score value
            html.Div([
                html.Span(f"{score_value:.1f}", 
                        style={
                            "fontSize": "64px", 
                            "fontWeight": "bold", 
                            "color": pulse_color,
                            "display": "block",
                            "textAlign": "center",
                            "width": "100%"
                        }),
            ], style={"textAlign": "center", "marginBottom": "25px", "display": "flex", "justifyContent": "center"}),
            # Status label
            html.Div([
                html.Span(pulse_status, 
                        style={
                            "fontSize": "32px", 
                            "color": pulse_color,
                            "marginRight": "10px",
                            "display": "inline-block",
                            "fontWeight": "500"
                        }),
                html.Span(
                    "ⓘ", 
                    id="sentiment-info-icon",
                    className="info-icon",
                    style={
                        "cursor": "pointer", 
                        "fontSize": "22px", 
                        "display": "inline-block",
                        "color": "#2c3e50",
                        "verticalAlign": "text-top" 
                    }
                ),
                # Positioned tooltip that won't overflow the container
                html.Div(
                    id="sentiment-info-tooltip",
                    style={
                        "display": "none", 
                        "position": "fixed",  # Changed to fixed for better positioning
                        "zIndex": "9999", 
                        "backgroundColor": "white", 
                        "padding": "15px", 
                        "borderRadius": "5px", 
                        "boxShadow": "0px 0px 10px rgba(0,0,0,0.2)", 
                        "maxWidth": "300px", 
                        "top": "50%",  # Centered vertically
                        "left": "50%", 
                        "transform": "translate(-50%, -50%)", # Center both horizontally and vertically
                        "border": "1px solid #ddd",
                        "width": "280px",
                        "textAlign": "left"
                    },
                    children=[
                        html.H5("Sentiment Index Categories", style={"marginBottom": "10px"}),
                        html.Div([
                            html.Div([
                                html.Span("Boom (80-100): ", style={"fontWeight": "bold", "color": "#2ECC71"}),
                                "Strong growth across indicators; economic expansion accelerating"
                            ], style={"marginBottom": "5px"}),
                            html.Div([
                                html.Span("Expansion (60-79): ", style={"fontWeight": "bold", "color": "#F1C40F"}),
                                "Positive growth trend; economic indicators mostly favorable"
                            ], style={"marginBottom": "5px"}),
                            html.Div([
                                html.Span("Moderate Growth (40-59): ", style={"fontWeight": "bold", "color": "#E67E22"}),
                                "Stable growth with some mixed signals; modest economic expansion"
                            ], style={"marginBottom": "5px"}),
                            html.Div([
                                html.Span("Slowdown (20-39): ", style={"fontWeight": "bold", "color": "#E74C3C"}),
                                "Growth decelerating; more negative than positive indicators"
                            ], style={"marginBottom": "5px"}),
                            html.Div([
                                html.Span("Contraction (0-19): ", style={"fontWeight": "bold", "color": "#C0392B"}),
                                "Economic indicators signaling recession or significant downturn"
                            ])
                        ])
                    ]
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
                html.Span(f"Last updated: {datetime.now().strftime('%B %d, %Y')}", 
                        style={
                            "fontSize": "12px", 
                            "color": "#95a5a6",
                            "marginTop": "15px"
                        })
            ], style={"textAlign": "center"})
        ], className="pulse-card-inner")
    ], className="pulse-card", style={
        "boxShadow": f"0 0 20px {pulse_color}",  # Color-matched glow with original size
        "border": f"1px solid {pulse_color}"     # Color-matched border
    })
    
    return pulse_card, pulse_status, pulse_color

def update_sentiment_gauge(score):
    """Update the sentiment card based on the score"""
    if not score:
        return html.Div("Data unavailable", className="no-data-message")
    
    # Create the pulse card using the new function
    pulse_card, pulse_status, pulse_color = create_pulse_card(score)
    
    # Return the pulse card in a container
    return html.Div([
        pulse_card
    ], className="pulse-card-container")

# Functions for sector weight management
def create_sector_summary(sector_scores):
    """Create a summary of the strongest and weakest sectors based on scores"""
    # Sort sectors by score (descending)
    sorted_sectors = sorted(sector_scores.items(), key=lambda x: x[1], reverse=True)
    
    # Determine top 3 and bottom 3 sectors
    top_3 = sorted_sectors[:3]
    bottom_3 = sorted_sectors[-3:]
    
    top_sectors = html.Div([
        html.H4("Strongest Sectors", className="summary-title", 
               style={"marginTop": "0", "marginBottom": "12px", "color": "#2c3e50", 
                     "fontWeight": "600", "fontSize": "18px", "textTransform": "uppercase", 
                     "letterSpacing": "0.5px"}),
        html.Div([
            html.Div([
                html.Div(f"{sector}", className="sector-name", 
                         style={"fontWeight": "500", "display": "inline-block", "width": "75%", "textAlign": "left"}),
                html.Div(f"{score:.1f}", 
                         className="sector-score-positive" if score >= 60 else 
                                  "sector-score-neutral" if score >= 40 else 
                                  "sector-score-negative",
                         style={"fontWeight": "bold", "textAlign": "right", "display": "inline-block", "width": "25%"})
            ], style={"display": "flex", "justifyContent": "space-between", 
                     "padding": "8px 0", "borderBottom": "1px solid #f0f0f0"}) 
            for sector, score in top_3
        ])
    ], className="summary-section", style={"backgroundColor": "#f8f9fa", "padding": "15px", "borderRadius": "6px"})
    
    bottom_sectors = html.Div([
        html.H4("Weakest Sectors", className="summary-title", 
               style={"marginTop": "0", "marginBottom": "12px", "color": "#2c3e50", 
                     "fontWeight": "600", "fontSize": "18px", "textTransform": "uppercase", 
                     "letterSpacing": "0.5px"}),
        html.Div([
            html.Div([
                html.Div(f"{sector}", className="sector-name", 
                         style={"fontWeight": "500", "display": "inline-block", "width": "75%", "textAlign": "left"}),
                html.Div(f"{score:.1f}", 
                         className="sector-score-positive" if score >= 60 else 
                                  "sector-score-neutral" if score >= 40 else 
                                  "sector-score-negative",
                         style={"fontWeight": "bold", "textAlign": "right", "display": "inline-block", "width": "25%"})
            ], style={"display": "flex", "justifyContent": "space-between", 
                     "padding": "8px 0", "borderBottom": "1px solid #f0f0f0"}) 
            for sector, score in bottom_3
        ])
    ], className="summary-section", style={"backgroundColor": "#f8f9fa", "padding": "15px", "borderRadius": "6px"})
    
    return html.Div([
        top_sectors,
        bottom_sectors
    ], className="sector-summary-container")

def calculate_t2d_pulse(sector_scores, weights):
    """Calculate T2D Pulse score from sector scores and weights"""
    # Normalize weights to sum to 100%
    total_weight = sum(weights.values())
    normalized_weights = {k: v / total_weight * 100 for k, v in weights.items()}
    
    # Calculate weighted average
    weighted_sum = sum(sector_scores[sector] * normalized_weights[sector] for sector in sector_scores.keys())
    weighted_average = weighted_sum / 100
    
    return round(weighted_average, 1)