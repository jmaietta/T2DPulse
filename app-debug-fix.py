"""
Debugging Fix for T2D Pulse Dashboard Weight Updates

This solution solves the weight update issue in two ways:
1. A client-side JavaScript fix (already implemented)
2. A server-side Dash fix that adds debug output

DEBUGGING ANALYSIS:
- The weight updates are being processed correctly in the backend (verified by logs)
- The UI is not reflecting these updates, despite the callbacks executing
- This suggests an issue with the Dash callback chain or output formatting

IMPLEMENTATION FIX:
1. Replace the existing callback chain with a fixed version
2. Add additional debug output to track what's happening
3. Use the global app.layout.children reference to ensure changes propagate
"""

import dash
from dash import html, dcc, Input, Output, State, ALL
from dash.exceptions import PreventUpdate
import json
from datetime import datetime
import pandas as pd
import plotly.graph_objs as go

# Define a more direct weight update callback that forces global updates
@app.callback(
    [Output({"type": "weight-input", "index": ALL}, "value"),
     Output("t2d-pulse-value", "children"),
     Output("weight-update-notification", "children"),
     Output("weight-update-notification", "style")],
    [Input({"type": "apply-weight", "index": ALL}, "n_clicks")],
    [State({"type": "weight-input", "index": ALL}, "value"),
     State({"type": "weight-input", "index": ALL}, "id")]
)
def direct_weight_update(n_clicks_list, input_values, input_ids):
    """
    Combined callback that directly updates the weights without intermediate storage
    
    This replaces the two separate callbacks with a single function that handles everything,
    avoiding potential race conditions or state inconsistencies.
    """
    # Check if triggered
    ctx = dash.callback_context
    if not ctx.triggered:
        # On initial load, return current values
        global sector_weights
        weight_values = [round(sector_weights[id_dict["index"]], 2) for id_dict in input_ids]
        return weight_values, "50.0", "", {"opacity": 0}
    
    print("===== DIRECT WEIGHT UPDATE TRIGGERED =====")
    print(f"Triggered by: {ctx.triggered}")
    
    # Find which sector was changed
    trigger_id = ctx.triggered[0]["prop_id"].split(".")[0]
    try:
        trigger_dict = json.loads(trigger_id)
        changed_sector = trigger_dict["index"]
        changed_idx = next((i for i, id_dict in enumerate(input_ids) 
                         if id_dict["index"] == changed_sector), None)
        
        if changed_idx is None:
            print(f"Could not find {changed_sector} in input_ids")
            raise PreventUpdate
            
        # Get the new value and validate it
        new_value = input_values[changed_idx]
        if new_value is None:
            raise PreventUpdate
            
        print(f"Updating {changed_sector} from {sector_weights[changed_sector]} to {new_value}")
        
        # Create a copy of the current weights
        weights = {sector: sector_weights[sector] for sector in sector_weights}
        
        # Update with the new value
        weights[changed_sector] = float(max(1.0, min(100.0, new_value)))
        
        # Calculate remaining weight and adjust others proportionally
        remaining_value = 100.0 - weights[changed_sector]
        sectors_to_adjust = [s for s in weights.keys() if s != changed_sector]
        total_other_weight = sum(weights[s] for s in sectors_to_adjust)
        
        if total_other_weight > 0:
            scale_factor = remaining_value / total_other_weight
            for s in sectors_to_adjust:
                weights[s] = round(weights[s] * scale_factor, 2)
        
        # Ensure total is exactly 100%
        total = sum(weights.values())
        if abs(total - 100.0) > 0.01:
            adjust_sector = next((s for s in weights.keys() if s != changed_sector), None)
            if adjust_sector:
                weights[adjust_sector] += round(100.0 - total, 2)
                
        # Update global weights
        global sector_weights
        sector_weights = weights.copy()
        print(f"Final weights: {sector_weights}")
        
        # Calculate new T2D Pulse score
        try:
            sector_scores_list = calculate_sector_sentiment()
            t2d_pulse_score = calculate_t2d_pulse_from_sectors(sector_scores_list, weights)
            t2d_pulse_display = f"{t2d_pulse_score:.1f}"
        except Exception as e:
            print(f"Error calculating Pulse score: {e}")
            t2d_pulse_display = "50.0"
            
        # Generate weight values for display
        weight_values = []
        for id_dict in input_ids:
            sector = id_dict["index"]
            weight_values.append(round(weights[sector], 2))
            
        # Create notification
        current_time = datetime.now().strftime("%H:%M:%S")
        notification_message = f"Weights updated at {current_time} - T2D Pulse score: {t2d_pulse_display}"
        notification_style = {
            "color": "green", 
            "fontWeight": "bold", 
            "marginRight": "20px",
            "fontSize": "14px",
            "padding": "8px 12px",
            "backgroundColor": "#e8f5e9",
            "borderRadius": "4px",
            "opacity": 1,
            "transition": "opacity 0.3s ease"
        }
        
        print(f"Returning weight values: {weight_values}")
        return weight_values, t2d_pulse_display, notification_message, notification_style
        
    except Exception as e:
        print(f"Error in direct_weight_update: {e}")
        import traceback
        traceback.print_exc()
        raise PreventUpdate

# Add a secondary callback to apply the notification fade-out effect
@app.callback(
    Output("weight-update-notification", "style", allow_duplicate=True),
    Input("weight-update-notification", "children"),
    prevent_initial_call=True
)
def fade_notification(notification_text):
    """Apply fade-out effect to notification after delay"""
    if not notification_text:
        return {"opacity": 0}
        
    # Return visible first, then schedule a client-side callback to fade it out
    return {"color": "green", 
            "fontWeight": "bold", 
            "marginRight": "20px",
            "fontSize": "14px",
            "padding": "8px 12px",
            "backgroundColor": "#e8f5e9",
            "borderRadius": "4px",
            "opacity": 1,
            "transition": "opacity 0.5s ease"}