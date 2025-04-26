"""
Modified version of the app.py file  
with improved Sentiment tooltip implementation
"""

from improved_pulse_card import create_pulse_card, update_sentiment_gauge
import dash
from dash import html, dcc
from dash.dependencies import Input, Output
from datetime import datetime

app = dash.Dash(__name__)

app.layout = html.Div([
    # App header
    html.Div([
        html.H1("T2D Pulse Dashboard with Improved Tooltip", 
                style={"textAlign": "center", "marginBottom": "20px"}),
        html.P("Hover over the ⓘ icon to see the tooltip.", 
               style={"textAlign": "center"})
    ]),
    
    # Main content - Sentiment gauge
    html.Div([
        html.Div([
            # Dummy data for demonstration
            html.Div("65.8", id="sentiment-score", style={"display": "none"}),
            html.Div("Bullish", id="sentiment-category", style={"display": "none"}),
            
            # Display the pulse card with sentiment gauge
            html.Div(id="sentiment-gauge")
        ], className="demo-container", style={"width": "400px", "margin": "0 auto"})
    ]),
    
    # Note about implementation
    html.Div([
        html.H2("Implementation Notes", style={"marginTop": "30px"}),
        html.Ul([
            html.Li("The tooltip uses CSS-only hover functionality - no JavaScript or callbacks"),
            html.Li("The implementation relies on tooltip.css with proper CSS classes"),
            html.Li("The tooltip container provides positioning context for absolute positioning"),
            html.Li("Smooth transitions add a polished fade-in/fade-out effect")
        ])
    ], style={"maxWidth": "800px", "margin": "30px auto"})
])

# Callback to render the sentiment gauge
@app.callback(
    Output("sentiment-gauge", "children"),
    [Input("sentiment-score", "children")]
)
def render_sentiment_gauge(score):
    try:
        score_value = float(score)
        return update_sentiment_gauge(score_value)
    except (ValueError, TypeError):
        return html.Div("Invalid score value", className="error-message")

if __name__ == "__main__":
    app.run(debug=True, host='0.0.0.0', port=5001)