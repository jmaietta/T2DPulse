"""
Simplified T2D Pulse app with streamlined sector cards.
"""
import os
import dash
import dash_bootstrap_components as dbc
from dash import html, dcc, Input, Output, State
import plotly.express as px

# Import our clean sector implementation
from fix_sectors import update_sector_sentiment_container

# Create the Dash app
app = dash.Dash(
    __name__,
    external_stylesheets=[dbc.themes.BOOTSTRAP],
    meta_tags=[{"name": "viewport", "content": "width=device-width, initial-scale=1"}],
)

# Define the layout
app.layout = html.Div([
    dcc.Interval(
        id="interval-component",
        interval=300 * 1000,  # 5 minutes in milliseconds
        n_intervals=0
    ),
    
    # Header
    html.Div([
        html.H1("T2D Pulse Dashboard"),
        html.P("A simplified version with streamlined sector cards")
    ], style={"padding": "20px", "background": "#f8f9fa", "marginBottom": "20px"}),
    
    # Main container for sector cards
    html.Div(id="sector-sentiment-container", style={"padding": "20px"})
])

# Define the callback for updating sector cards
@app.callback(
    Output("sector-sentiment-container", "children"),
    [Input("interval-component", "n_intervals")]
)
def update_sectors(n):
    """Update the sector cards using our clean implementation"""
    return update_sector_sentiment_container(n)

# Run the app
if __name__ == "__main__":
    port = 5000
    print(f"Starting T2D Pulse dashboard on port {port}")
    app.run(host="0.0.0.0", port=port, debug=False)