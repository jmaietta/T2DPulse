#!/usr/bin/env python3
# simple_glow_mockup.py
# -----------------------------------------------------------
# Create a simple glowing circle for the T2D Pulse score

import dash
import dash_bootstrap_components as dbc
from dash import html, dcc
import os
import base64

# Define color scheme for pulse scores
PULSE_COLORS = {
    'bearish': '#e74c3c',  # Red
    'neutral': '#f39c12',  # Orange/Yellow
    'bullish': '#2ecc71',  # Green
}

# T2D logo path - ensure this exists
T2D_LOGO_PATH = "attached_assets/T2D Pulse logo.png"

def create_pulse_glow_circle(value, size=280, include_logo=True):
    """
    Create a simple glowing circle for the T2D Pulse score
    
    Args:
        value (float): The T2D Pulse score (0-100)
        size (int): Size of the circle in pixels
        include_logo (bool): Whether to include the T2D logo
        
    Returns:
        dash component: The pulse circle component
    """
    # Determine status and color based on value
    if value is None or not isinstance(value, (int, float)):
        value = 0
        status = "Bearish"
        color = PULSE_COLORS['bearish']
    else:
        value = float(value)
        if value < 40:
            status = "Bearish"
            color = PULSE_COLORS['bearish']
        elif value > 60:
            status = "Bullish"
            color = PULSE_COLORS['bullish']
        else:
            status = "Neutral"
            color = PULSE_COLORS['neutral']
            
    # Encode T2D logo in base64 if it exists
    logo_img = None
    if include_logo and os.path.exists(T2D_LOGO_PATH):
        with open(T2D_LOGO_PATH, "rb") as image_file:
            encoded_image = base64.b64encode(image_file.read()).decode('utf-8')
            logo_img = f"data:image/png;base64,{encoded_image}"
    
    # Create the glowing circle component
    pulse_circle = html.Div([
        # Container for the circle and its contents
        html.Div([
            # T2D Logo at top (if available)
            html.Img(
                src=logo_img,
                style={
                    "width": "120px",
                    "margin": "0 auto 15px auto",
                    "display": "block" if logo_img else "none"
                }
            ) if logo_img else html.Div("T2D PULSE", style={
                "fontSize": "18px",
                "fontWeight": "500",
                "color": "#e74c3c",  # T2D red branding
                "textAlign": "center",
                "marginBottom": "15px",
                "fontFamily": "'Arial', sans-serif",
                "letterSpacing": "1px"
            }),
            
            # Inner circle with pulse value
            html.Div([
                # Large score value
                html.Div(
                    f"{value:.1f}",
                    style={
                        "fontSize": "60px",
                        "fontWeight": "600",
                        "color": color,
                        "textAlign": "center",
                        "lineHeight": "1"
                    }
                ),
                
                # Status text (Bullish/Neutral/Bearish)
                html.Div(
                    status,
                    style={
                        "fontSize": "20px",
                        "fontWeight": "400",
                        "color": color,
                        "textAlign": "center",
                        "marginTop": "5px"
                    }
                ),
            ], style={
                "display": "flex",
                "flexDirection": "column",
                "justifyContent": "center",
                "alignItems": "center",
                "width": f"{size-40}px",
                "height": f"{size-40}px",
                "borderRadius": "50%",
                "backgroundColor": "white",
                "boxShadow": f"0 0 20px {color}",
                "border": f"3px solid {color}",
                "margin": "auto"
            }),
        ], style={
            "display": "flex",
            "flexDirection": "column",
            "justifyContent": "center",
            "padding": "20px",
            "width": f"{size}px",
            "height": f"{size}px",
            "margin": "0 auto"
        })
    ], className="pulse-circle-container")
    
    return pulse_circle

# Test the design if run directly
if __name__ == "__main__":
    app = dash.Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])
    
    # Create layout
    app.layout = dbc.Container([
        html.H1("T2D Pulse Circle Design", className="mt-4 mb-4"),
        
        dbc.Row([
            dbc.Col([
                html.H3("Neutral (55.7)"),
                create_pulse_glow_circle(55.7)
            ], width=4),
            dbc.Col([
                html.H3("Bearish (35.2)"),
                create_pulse_glow_circle(35.2)
            ], width=4),
            dbc.Col([
                html.H3("Bullish (72.5)"),
                create_pulse_glow_circle(72.5)
            ], width=4),
        ]),
    ], fluid=True)
    
    # Run the app
    app.run_server(host='0.0.0.0', port=5050, debug=True)
