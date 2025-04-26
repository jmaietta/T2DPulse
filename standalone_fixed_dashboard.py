"""
T2D Pulse Standalone Weight Fixer

This is a standalone version of the T2D Pulse dashboard focusing only on the weight update functionality.
It has no dependencies on external JavaScript or CSS assets to avoid any potential conflicts.
"""

import dash
from dash import dcc, html, Input, Output, State, ctx, ALL, no_update
import json
from datetime import datetime

# Initialize the app with no external assets
app = dash.Dash(__name__, 
                assets_external_path='',
                external_stylesheets=[],
                suppress_callback_exceptions=True)

server = app.server

# Mock sector data - matching the sectors in the main dashboard
sectors = ["AdTech", "Cloud Infra", "Fintech", "SMB SaaS", "Enterprise SaaS", 
           "Digital Media", "Consumer Tech", "IT Services", "Payments", 
           "Comm. Infra", "Legacy Software", "Gaming"]

# Initialize sector weights evenly
sector_weights = {sector: round(100 / len(sectors), 2) for sector in sectors}

# Define some basic styles without relying on external CSS
styles = {
    'container': {
        'fontFamily': 'Arial, sans-serif',
        'margin': '0 auto',
        'maxWidth': '800px',
        'padding': '20px',
        'backgroundColor': '#ffffff',
    },
    'header': {
        'textAlign': 'center',
        'color': '#2c3e50',
        'marginBottom': '30px',
    },
    'pulse_score': {
        'textAlign': 'center',
        'fontSize': '28px',
        'fontWeight': 'bold',
        'color': '#2ecc71',
        'marginBottom': '30px',
        'border': '2px solid #2ecc71',
        'borderRadius': '10px',
        'padding': '10px',
        'width': '100px',
        'margin': '0 auto',
    },
    'sector_grid': {
        'display': 'grid',
        'gridTemplateColumns': 'repeat(auto-fill, minmax(320px, 1fr))',
        'gap': '15px',
        'marginBottom': '30px',
    },
    'sector_card': {
        'border': '1px solid #ddd',
        'borderRadius': '5px',
        'padding': '15px',
        'backgroundColor': '#f8f9fa',
        'boxShadow': '0 2px 4px rgba(0,0,0,0.1)',
    },
    'sector_name': {
        'fontWeight': 'bold',
        'marginBottom': '10px',
    },
    'weight_input': {
        'width': '70px',
        'textAlign': 'center',
        'fontWeight': 'bold',
        'padding': '5px',
        'border': '1px solid #ddd',
        'borderRadius': '4px',
    },
    'apply_button': {
        'backgroundColor': '#e74c3c',
        'color': 'white',
        'border': 'none',
        'borderRadius': '4px',
        'padding': '5px 10px',
        'marginLeft': '10px',
        'cursor': 'pointer',
    },
    'notification': {
        'textAlign': 'center',
        'padding': '10px',
        'backgroundColor': '#e8f5e9',
        'borderRadius': '4px',
        'marginTop': '20px',
        'color': '#2c3e50',
        'fontWeight': 'bold',
    }
}

# App layout with fully inline styling to avoid external assets
app.layout = html.Div(style=styles['container'], children=[
    # Header
    html.Div(style=styles['header'], children=[
        html.H1("T2D Pulse Weight Fix"),
        html.P("This tool allows fixing the weight update functionality"),
    ]),
    
    # T2D Pulse Score
    html.Div(id="t2d-pulse-value", children="60.0", style=styles['pulse_score']),
    
    # Sector Weights Grid
    html.Div(style=styles['sector_grid'], children=[
        html.Div(style=styles['sector_card'], children=[
            html.Div(style=styles['sector_name'], children=sector),
            html.Div(style={'display': 'flex', 'alignItems': 'center'}, children=[
                dcc.Input(
                    id={"type": "weight-input", "index": sector},
                    type="number",
                    min=1,
                    max=100,
                    step=0.25,
                    value=round(sector_weights[sector], 2),
                    style=styles['weight_input']
                ),
                html.Span("%", style={"fontWeight": "bold", "fontSize": "14px", "marginLeft": "4px"}),
                html.Button(
                    "Apply", 
                    id={"type": "apply-weight", "index": sector},
                    style=styles['apply_button']
                )
            ])
        ]) for sector in sectors
    ]),
    
    # Hidden storage
    html.Div(id="stored-weights", style={"display": "none"}),
    
    # Notification
    html.Div(id="weight-update-notification", style=styles['notification'], children="Adjust weights and click Apply"),
    
    # Debug Output
    html.Div(id="debug-output", style={"marginTop": "20px", "padding": "10px", "border": "1px solid #ddd"})
])

# Helper functions
def calculate_sector_sentiment():
    """Generate mock sector sentiment scores (50-100 range)"""
    return {sector: 50 + (i % 5) * 10 for i, sector in enumerate(sectors)}

def calculate_t2d_pulse_from_sectors(sector_scores, weights):
    """Calculate weighted average T2D Pulse score"""
    weighted_sum = sum(sector_scores[sector] * weights[sector] for sector in sectors)
    return weighted_sum / 100.0

# Callback: Apply weight update and refresh everything
@app.callback(
    [Output({"type": "weight-input", "index": sectors[i]}, "value") for i in range(len(sectors))] +
    [Output("t2d-pulse-value", "children"),
     Output("stored-weights", "children"),
     Output("weight-update-notification", "children"),
     Output("debug-output", "children")],
    [Input({"type": "apply-weight", "index": ALL}, "n_clicks")],
    [State({"type": "weight-input", "index": ALL}, "value"),
     State({"type": "weight-input", "index": ALL}, "id"),
     State("stored-weights", "children")],
    prevent_initial_call=True
)
def update_weights(n_clicks_list, input_values, input_ids, weights_json):
    """
    Handle weight updates when Apply button is clicked.
    
    This function:
    1. Determines which sector's weight was changed
    2. Updates that sector's weight
    3. Proportionally adjusts all other sector weights to maintain 100% total
    4. Calculates the new T2D Pulse score
    5. Returns updated values for all inputs and displays
    """
    # Initialize with default weights if no stored weights exist
    weights = sector_weights.copy()
    if weights_json:
        try:
            weights = json.loads(weights_json)
        except:
            # If JSON parsing fails, use the default weights
            pass
    
    # Prevent callback if not triggered by a button click
    if not ctx.triggered:
        return [no_update] * (len(sectors) + 3) + ["No updates made"]
    
    # Determine which Apply button was clicked
    trigger = json.loads(ctx.triggered[0]["prop_id"].split(".")[0])
    changed_sector = trigger["index"]
    
    # Find the index of the changed sector in our lists
    changed_idx = next((i for i, idd in enumerate(input_ids) if idd["index"] == changed_sector), None)
    if changed_idx is None or n_clicks_list[changed_idx] is None:
        return [no_update] * (len(sectors) + 3) + ["Error: Could not identify changed sector"]
    
    # Get the new weight value
    new_value = input_values[changed_idx]
    if new_value is None:
        return [no_update] * (len(sectors) + 3) + ["Error: No weight value provided"]
    
    # Ensure weight is within valid range
    new_value = float(max(1.0, min(100.0, new_value)))
    
    # Update the changed sector weight
    weights[changed_sector] = new_value
    
    # Calculate how much to distribute among other sectors
    remaining_value = 100.0 - new_value
    sectors_to_adjust = [s for s in weights if s != changed_sector]
    total_other_weight = sum(weights[s] for s in sectors_to_adjust)
    
    # Distribute remaining weight proportionally
    if total_other_weight > 0:
        scale_factor = remaining_value / total_other_weight
        for s in sectors_to_adjust:
            weights[s] = round(weights[s] * scale_factor, 2)
    
    # Ensure total is exactly 100%
    total = sum(weights.values())
    if abs(total - 100.0) > 0.01:
        # Find a sector to adjust to make total exactly 100%
        adjust_sector = next((s for s in weights if s != changed_sector), None)
        if adjust_sector:
            weights[adjust_sector] += round(100.0 - total, 2)
    
    # Format updated values for all sector inputs
    updated_values = [round(weights[sector], 2) for sector in sectors]
    
    # Calculate the new T2D Pulse score
    sector_scores_list = calculate_sector_sentiment()
    t2d_pulse_score = calculate_t2d_pulse_from_sectors(sector_scores_list, weights)
    t2d_pulse_display = f"{t2d_pulse_score:.1f}"
    
    # Create notification message
    current_time = datetime.now().strftime("%H:%M:%S")
    notification_message = f"Weights updated at {current_time} - T2D Pulse score: {t2d_pulse_display}"
    
    # Debug output
    debug_info = f"""
    Changed sector: {changed_sector} to {new_value}%
    New weights: {str(weights)}
    Total weight: {sum(weights.values())}%
    New T2D Pulse score: {t2d_pulse_display}
    """
    
    return updated_values + [t2d_pulse_display, json.dumps(weights), notification_message, debug_info]

# Run the app
if __name__ == "__main__":
    app.run_server(host="0.0.0.0", port=5020, debug=False)