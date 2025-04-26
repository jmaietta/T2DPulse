"""
Improved T2D Pulse Card Implementation 
with CSS-based tooltip hover functionality
"""

import dash.html as html
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
                    src="/assets/T2D Pulse logo.png",
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
                # Tooltip container
                html.Div([
                    # The tooltip trigger icon
                    html.Span("ⓘ", className="info-icon"),
                    # Tooltip content
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
                ], className="tooltip-container")
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