"""
T2D Pulse Card Implementation
Displays the sentiment score with a color-coded glowing card
"""

import dash.html as html
import dash
from datetime import datetime

def create_pulse_card(score):
    """
    Create a pulse card with a glow effect based on the score
    
    Args:
        score (float): T2D Pulse score on a scale of 0-100
        
    Returns:
        dash component: The pulse card component
        str: The pulse status label (Bearish, Neutral, Bullish)
        str: The pulse color code (hex)
    """
    # Determine the pulse status and color based on the score
    if score >= 60:
        pulse_status = "Bullish"
        pulse_color = "#2ecc71"  # Green
    elif score >= 30:
        pulse_status = "Neutral"
        pulse_color = "#f39c12"  # Orange
    else:
        pulse_status = "Bearish"
        pulse_color = "#e74c3c"  # Red
    
    # Create the pulse card
    pulse_card = html.Div([
        html.Div([
            # T2D Pulse logo
            html.Div([
                html.Img(
                    src="/assets/T2D_Pulse_logo.png",
                    style={
                        "maxWidth": "100%",
                        "height": "auto",
                        "maxHeight": "70px",
                        "margin": "auto",
                        "display": "block"
                    }
                )
            ], style={"textAlign": "center", "marginBottom": "20px"}),
            
            # Pulse score number
            html.Div([
                html.Div(
                    f"{score:.1f}",
                    style={
                        "fontSize": "60px",
                        "fontWeight": "600",
                        "color": pulse_color,
                        "lineHeight": "1"
                    }
                ),
                html.Div(
                    pulse_status,
                    style={
                        "fontSize": "22px",
                        "fontWeight": "500",
                        "color": pulse_color,
                        "marginTop": "5px"
                    }
                )
            ], style={"textAlign": "center", "marginBottom": "15px"}),
            
            # Sentiment index title with info tooltip
            html.Div([
                html.Div(
                    "Sentiment Index",
                    style={
                        "fontSize": "18px", 
                        "fontWeight": "500",
                        "marginRight": "10px",
                        "display": "inline-block"
                    }),
                # Tooltip container with icon and content
                html.Div([
                    html.Span("ⓘ", className="tooltip-icon"),
                    html.Div([
                        html.H5("Sentiment Index Categories", style={"marginBottom": "10px"}),
                        html.Div([
                            html.Div([
                                html.Span("Bullish (60-100): ", style={"fontWeight": "bold", "color": "#2ecc71"}),
                                "Positive outlook; favorable growth conditions for technology sector"
                            ], style={"marginBottom": "5px"}),
                            html.Div([
                                html.Span("Neutral (30-60): ", style={"fontWeight": "bold", "color": "#f39c12"}),
                                "Balanced outlook; mixed signals with both opportunities and challenges"
                            ], style={"marginBottom": "5px"}),
                            html.Div([
                                html.Span("Bearish (0-30): ", style={"fontWeight": "bold", "color": "#e74c3c"}),
                                "Negative outlook; economic headwinds likely impacting tech industry growth"
                            ])
                        ])
                    ], className="tooltip-content")
                ], className="tooltip-wrapper")
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
        "boxShadow": f"0 0 20px {pulse_color}",  # Color-matched glow
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