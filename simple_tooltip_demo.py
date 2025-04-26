"""
Simple Tooltip Demonstration
A stripped-down version to show the CSS-only tooltip working
"""

import dash
from dash import html
from dash.dependencies import Input, Output
import plotly.graph_objs as go

app = dash.Dash(__name__)

app.layout = html.Div([
    html.H1("CSS Tooltip Demo"),
    html.Div([
        html.Div("Sentiment Index", style={"marginRight": "10px", "display": "inline-block"}),
        # Tooltip container with icon and content
        html.Div([
            html.Span("ⓘ", className="info-icon"),
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
    ], style={"marginBottom": "50px"}),
    
    html.Div([
        html.H2("How it works:"),
        html.Ul([
            html.Li("The tooltip uses pure CSS - no JavaScript"),
            html.Li("Hover over the 'ⓘ' icon to see the tooltip"),
            html.Li("The tooltip is positioned absolutely relative to its container"),
            html.Li("CSS transitions provide a smooth fade-in/fade-out effect")
        ])
    ])
])

if __name__ == "__main__":
    app.run(debug=True, host='0.0.0.0', port=5010)