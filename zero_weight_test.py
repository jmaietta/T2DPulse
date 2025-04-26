import dash
from dash import dcc, html, Input, Output, State
import json
import numpy as np
from datetime import datetime

# Initialize the app
app = dash.Dash(__name__)

# Default sector weights - for simplicity we're only using 5 sectors
sector_weights = {
    "Cloud": 25.0,
    "AI": 25.0,
    "Cybersecurity": 25.0,
    "AdTech": 15.0,
    "FinTech": 10.0,
}

# Generate random sector scores (0-100) for demonstration
np.random.seed(42)
sector_scores = {
    "Cloud": np.random.randint(30, 90),
    "AI": np.random.randint(30, 90),
    "Cybersecurity": np.random.randint(30, 90),
    "AdTech": np.random.randint(30, 90),
    "FinTech": np.random.randint(30, 90),
}

# Color mapping for sentiment scores
def get_score_color(score):
    if score >= 60:  # Bullish
        return "#2ecc71"  # Green
    elif score >= 30:  # Neutral
        return "#f39c12"  # Orange
    else:  # Bearish
        return "#e74c3c"  # Red

def create_sector_card(sector, score, weight=None):
    # Create a card for each sector showing its sentiment score
    score_color = get_score_color(score)
    
    card = html.Div([
        html.Div([
            html.Div(sector, className="sector-name"),
            html.Div(f"{score:.1f}", 
                    className="sector-score",
                    style={"color": score_color}),
        ], className="sector-header"),
        
        html.Div([
            # Weight input field
            dcc.Input(
                id=f"weight-input-{sector}",
                type="number",
                min=0,  # Allow 0% weight
                max=100,
                step=0.1,
                value=weight,
                className="weight-input",
            ),
            html.Span("%", style={"marginRight": "8px"}),
            
            # Apply button
            html.Button(
                "Apply", 
                id=f"apply-btn-{sector}",
                className="apply-button",
                n_clicks=0,
            )
        ], className="weight-controls")
    ], className="sector-card")
    
    return card

# Create app layout with inline CSS
app.layout = html.Div([
    html.H1("Zero Weight Sector Test"),
    
    html.Div([
        # Sector cards with inputs
        html.Div(id="sector-cards", className="sector-cards"),
        
        # Total weight display
        html.Div([
            html.Span("Total Weight: "),
            html.Span(id="total-weight", children="100%"),
        ], className="total-weight"),
        
        # Reset button
        html.Button(
            "Reset Weights", 
            id="reset-weights-btn",
            n_clicks=0,
        ),
        
        # Date display
        html.Div(f"April 26, 2025", className="date-display"),
    ]),
    
    # Result display (updated when weights change)
    html.Div([
        html.H2("Weighted Score"),
        html.Div(id="weighted-score", className="weighted-score"),
        
        # Hidden div for storing weights
        html.Div(id="stored-weights", style={"display": "none"}),
    ]),
    
    # CSS via style property
    html.Div(style={
        'font-family': 'Arial, sans-serif'
    })
])

# Initialize the sector cards
@app.callback(
    Output("sector-cards", "children"),
    Input("stored-weights", "children"),
)
def initialize_sector_cards(weights_json):
    if weights_json:
        weights = json.loads(weights_json)
    else:
        weights = sector_weights.copy()
    
    cards = []
    for sector, score in sector_scores.items():
        cards.append(create_sector_card(sector, score, weights[sector]))
    
    return cards

# Initialize the stored weights (first load)
@app.callback(
    Output("stored-weights", "children"),
    Input("sector-cards", "children"),
)
def initialize_weights(children):
    return json.dumps(sector_weights)

# Reset weights
@app.callback(
    Output("stored-weights", "children", allow_duplicate=True),
    Input("reset-weights-btn", "n_clicks"),
    State("stored-weights", "children"),
    prevent_initial_call=True
)
def reset_weights(n_clicks, weights_json):
    if n_clicks > 0:
        return json.dumps(sector_weights)
    return weights_json

# Update the weighted score
@app.callback(
    [Output("weighted-score", "children"),
     Output("weighted-score", "style")],
    Input("stored-weights", "children"),
)
def update_weighted_score(weights_json):
    if weights_json:
        weights = json.loads(weights_json)
    else:
        weights = sector_weights.copy()
    
    # Calculate weighted average
    weighted_sum = 0
    weight_total = sum(weights.values())
    
    if weight_total > 0:  # Prevent division by zero
        for sector, score in sector_scores.items():
            weight_percentage = weights[sector] / weight_total
            weighted_sum += score * weight_percentage
    else:
        weighted_sum = 0
    
    # Format score and determine color
    score_color = get_score_color(weighted_sum)
    
    return f"{weighted_sum:.1f}", {
        "backgroundColor": score_color,
        "color": "white"
    }

# Update the total weight display
@app.callback(
    Output("total-weight", "children"),
    Input("stored-weights", "children"),
)
def update_total_weight(weights_json):
    if weights_json:
        weights = json.loads(weights_json)
        total = sum(weights.values())
        return f"{total:.2f}%"
    return "100.00%"

# Dynamic callbacks for each sector's Apply button
for sector in sector_weights.keys():
    @app.callback(
        Output("stored-weights", "children", allow_duplicate=True),
        Input(f"apply-btn-{sector}", "n_clicks"),
        State(f"weight-input-{sector}", "value"),
        State("stored-weights", "children"),
        prevent_initial_call=True
    )
    def apply_weight(n_clicks, weight_value, weights_json, sector=sector):
        if n_clicks == 0:
            return weights_json
        
        # Parse current weights
        if weights_json:
            weights = json.loads(weights_json)
        else:
            weights = sector_weights.copy()
        
        # Get the new weight value from input (with None handling)
        if weight_value is None or str(weight_value).strip() == '':
            # If input is empty, keep the old weight
            new_weight = weights[sector]
        else:
            try:
                # Allowing 0 as minimum weight
                new_weight = max(0, min(100, float(weight_value)))
            except (ValueError, TypeError):
                # If conversion fails, keep the old weight
                new_weight = weights[sector]
        
        # Calculate the difference that needs to be distributed
        old_weight = weights[sector]
        weight_difference = new_weight - old_weight
        
        # Apply the new weight
        weights[sector] = new_weight
        
        # If there's a difference to distribute
        if weight_difference != 0:
            # Get all sectors except the one being updated
            other_sectors = [s for s in weights.keys() if s != sector]
            
            # Calculate the sum of other sector weights
            other_weights_sum = sum(weights[s] for s in other_sectors)
            
            # Distribute the difference proportionally
            if other_weights_sum > 0:  # Avoid division by zero
                for s in other_sectors:
                    # Calculate proportional adjustment
                    proportion = weights[s] / other_weights_sum
                    adjustment = -weight_difference * proportion
                    weights[s] = max(0, weights[s] + adjustment)
        
        # Ensure weights sum to exactly 100%
        total = sum(weights.values())
        if total != 100 and total > 0:
            # Find the largest weight to adjust
            largest_sector = max(weights.items(), key=lambda x: x[1])[0]
            weights[largest_sector] += (100 - total)
        
        # Format all weights to 2 decimal places for display
        for s in weights:
            weights[s] = round(weights[s], 2)
        
        return json.dumps(weights)

# Run the app
if __name__ == "__main__":
    app.run_server(debug=True, host="0.0.0.0", port=5050)