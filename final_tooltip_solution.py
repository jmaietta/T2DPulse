"""
Final CSS-Only Tooltip Solution
A clean, minimal implementation that uses only CSS for tooltips
"""

import dash
from dash import html
import os

app = dash.Dash(__name__)

# Simple HTML layout with tooltips
app.layout = html.Div([
    html.Div([
        html.H1("Pure CSS Tooltip Solution"),
        html.P("A simplified, clean approach that works without JavaScript or callbacks."),
        
        # Example tooltip 1 - Basic info
        html.Div([
            html.H3("Basic Information Tooltip"),
            html.Div([
                html.Span("Hover me for info ", style={"marginRight": "5px"}),
                html.Div([
                    html.Span("ⓘ", className="tooltip-icon"),
                    html.Div([
                        "This is a simple tooltip with basic information.",
                        html.Br(),
                        "It appears when you hover over the icon."
                    ], className="tooltip-content")
                ], className="tooltip-wrapper")
            ], style={"marginBottom": "30px"})
        ]),
        
        # Example tooltip 2 - T2D Pulse with categories
        html.Div([
            html.H3("T2D Pulse Sentiment Categories"),
            html.Div([
                html.Span("Sentiment Index ", style={"marginRight": "5px", "fontSize": "18px", "fontWeight": "500"}),
                html.Div([
                    html.Span("ⓘ", className="tooltip-icon"),
                    html.Div([
                        html.H4("Sentiment Categories", style={"marginTop": "0"}),
                        html.Div([
                            html.Div([
                                html.Span("Bullish (60-100): ", style={"fontWeight": "bold", "color": "#2ecc71"}),
                                "Positive outlook; favorable growth conditions for technology sector"
                            ], style={"marginBottom": "8px"}),
                            html.Div([
                                html.Span("Neutral (30-60): ", style={"fontWeight": "bold", "color": "#f39c12"}),
                                "Balanced outlook; mixed signals with both opportunities and challenges"
                            ], style={"marginBottom": "8px"}),
                            html.Div([
                                html.Span("Bearish (0-30): ", style={"fontWeight": "bold", "color": "#e74c3c"}),
                                "Negative outlook; economic headwinds likely impacting tech industry growth"
                            ])
                        ])
                    ], className="tooltip-content")
                ], className="tooltip-wrapper")
            ], style={"marginBottom": "30px"})
        ]),
        
        # How it works explanation
        html.Div([
            html.H3("How It Works"),
            html.Ul([
                html.Li("Uses pure CSS with :hover pseudo-class"),
                html.Li("No JavaScript or Dash callbacks required"),
                html.Li("Simple HTML structure that's easy to implement"),
                html.Li("Tooltip positioned absolutely relative to its container"),
                html.Li("Smooth transition effects with CSS opacity")
            ]),
            html.Pre("""
# HTML Structure
html.Div([
    html.Span("ⓘ", className="tooltip-icon"),
    html.Div(
        "Tooltip content goes here", 
        className="tooltip-content"
    )
], className="tooltip-wrapper")
            """, style={"backgroundColor": "#f5f5f5", "padding": "15px", "borderRadius": "5px"})
        ])
    ], style={
        "maxWidth": "800px",
        "margin": "0 auto",
        "padding": "20px",
        "fontFamily": "Arial, sans-serif"
    })
])

if __name__ == "__main__":
    # Ensure our assets are loaded
    if not os.path.exists("assets"):
        os.makedirs("assets")
    
    app.run(debug=True, host='0.0.0.0', port=5015)