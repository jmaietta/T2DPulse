
import dash
from dash import dcc, html, Input, Output, State, ctx
import json
from datetime import datetime

# Initialize app
app = dash.Dash(__name__)
server = app.server

# Mock sector data
sectors = ["AdTech", "Cloud Infra", "Fintech", "SMB SaaS", "Enterprise SaaS", "Digital Media", "Consumer Tech", "IT Services", "Payments", "Comm. Infra", "Legacy Software", "Gaming"]

# Initialize sector weights evenly
sector_weights = {sector: round(100 / len(sectors), 2) for sector in sectors}

# App layout
app.layout = html.Div([
    html.H1("T2D Pulse", style={"textAlign": "center", "fontWeight": "bold"}),
    html.H2(id="t2d-pulse-value", style={"textAlign": "center", "marginBottom": "40px"}),

    html.Div([
        html.Div([
            html.Div([
                dcc.Input(
                    id={"type": "weight-input", "index": sector},
                    type="number",
                    min=1,
                    max=100,
                    step=0.25,
                    value=round(sector_weights[sector], 2),
                    style={"width": "50px", "textAlign": "center", "fontWeight": "bold"}
                ),
                html.Span("%", style={"fontWeight": "bold", "fontSize": "14px", "marginLeft": "4px"}),
                html.Button(
                    "Apply", 
                    id={"type": "apply-weight", "index": sector},
                    style={"fontSize": "12px", "padding": "4px 8px", "backgroundColor": "#e74c3c", "color": "white", "border": "none", "borderRadius": "4px", "cursor": "pointer", "fontWeight": "bold", "marginLeft": "8px"}
                )
            ], style={"display": "flex", "alignItems": "center", "marginBottom": "10px"})
        ]) for sector in sectors
    ], style={"maxWidth": "600px", "margin": "auto"}),

    html.Div(id="stored-weights", style={"display": "none"}),
    html.Div(id="weight-update-notification", style={"opacity": "0", "textAlign": "center", "marginTop": "20px"}),
])

# Helper functions
def calculate_sector_sentiment():
    # Dummy sentiment score for each sector
    return {sector: 50 + (i % 5) * 10 for i, sector in enumerate(sectors)}

def calculate_t2d_pulse_from_sectors(sector_scores, weights):
    weighted_sum = sum(sector_scores[sector] * weights[sector] for sector in sectors)
    return weighted_sum / 100.0

# Callback: Apply weight update and refresh everything
@app.callback(
    [Output({"type": "weight-input", "index": sectors[i]}, "value") for i in range(len(sectors))] +
    [Output("t2d-pulse-value", "children"),
     Output("stored-weights", "children"),
     Output("weight-update-notification", "children"),
     Output("weight-update-notification", "style")],
    [Input({"type": "apply-weight", "index": ALL}, "n_clicks")],
    [State({"type": "weight-input", "index": ALL}, "value"),
     State({"type": "weight-input", "index": ALL}, "id"),
     State("stored-weights", "children")],
    prevent_initial_call=True
)
def update_weights(n_clicks_list, input_values, input_ids, weights_json):
    global sector_weights

    if not ctx.triggered:
        raise dash.exceptions.PreventUpdate

    trigger = json.loads(ctx.triggered[0]["prop_id"].split(".")[0])
    changed_sector = trigger["index"]

    if weights_json:
        try:
            weights = json.loads(weights_json)
        except:
            weights = sector_weights
    else:
        weights = sector_weights

    changed_idx = next((i for i, idd in enumerate(input_ids) if idd["index"] == changed_sector), None)

    if changed_idx is None:
        raise dash.exceptions.PreventUpdate

    new_value = input_values[changed_idx]
    if new_value is None:
        raise dash.exceptions.PreventUpdate

    new_value = float(max(1.0, min(100.0, new_value)))

    # Update weights
    weights[changed_sector] = new_value
    remaining_value = 100.0 - new_value
    sectors_to_adjust = [s for s in weights if s != changed_sector]
    total_other_weight = sum(weights[s] for s in sectors_to_adjust)

    if total_other_weight > 0:
        scale_factor = remaining_value / total_other_weight
        for s in sectors_to_adjust:
            weights[s] = round(weights[s] * scale_factor, 2)

    # Fix rounding errors
    total = sum(weights.values())
    if abs(total - 100.0) > 0.01:
        adjust_sector = next((s for s in weights if s != changed_sector), None)
        if adjust_sector:
            weights[adjust_sector] += round(100.0 - total, 2)

    # Update global
    sector_weights = weights

    # Update outputs
    updated_values = [round(sector_weights[sector], 2) for sector in sectors]
    sector_scores_list = calculate_sector_sentiment()
    t2d_pulse_score = calculate_t2d_pulse_from_sectors(sector_scores_list, sector_weights)
    t2d_pulse_display = f"{t2d_pulse_score:.1f}"

    current_time = datetime.now().strftime("%H:%M:%S")
    notification_message = f"Weights updated at {current_time} - T2D Pulse score: {t2d_pulse_display}"
    notification_style = {"color": "green", "fontWeight": "bold", "fontSize": "14px", "backgroundColor": "#e8f5e9", "borderRadius": "4px", "opacity": 1}

    return updated_values + [t2d_pulse_display, json.dumps(sector_weights), notification_message, notification_style]

if __name__ == "__main__":
    app.run_server(debug=True)
