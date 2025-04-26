"""
T2D Pulse Dashboard with Fixed Weight Updates

This file contains the weight update callback fix for the T2D Pulse dashboard.
The main issue was duplicate and conflicting callbacks targeting the same outputs.
"""

# Weight Update Callbacks Replacement
# -----------------------------------
# Replace the following callbacks in app.py to fix the weight update issue:

'''
@app.callback(
    Output("stored-weights", "children"),
    [Input({"type": "apply-weight", "index": ALL}, "n_clicks")],
    [State({"type": "weight-input", "index": ALL}, "value"),
     State({"type": "weight-input", "index": ALL}, "id"),
     State("stored-weights", "children")]
)
def update_weight_from_input(n_clicks_list, input_values, input_ids, weights_json):
    """
    Update weights when user directly inputs a value and clicks Apply
    """
    # Skip if not triggered by a button click
    if not ctx.triggered or not any(n is not None for n in n_clicks_list):
        raise dash.exceptions.PreventUpdate
        
    # Find which button was clicked
    trigger = ctx.triggered[0]['prop_id']
    trigger_id = json.loads(trigger.split('.')[0])
    clicked_sector = trigger_id['index']
    
    # Get current weights, defaulting to standard weights if nothing is stored yet
    if weights_json:
        try:
            sector_weights = json.loads(weights_json)
        except:
            sector_weights = DEFAULT_WEIGHTS
    else:
        sector_weights = DEFAULT_WEIGHTS.copy()
    
    # Find the sector weight that was changed
    for i, sector_id in enumerate(input_ids):
        if sector_id['index'] == clicked_sector:
            new_value = input_values[i]
            # Basic validation - enforce min 1%, max 100%
            new_value = max(1, min(100, new_value))
            
            # Update the weight
            sector_weights[clicked_sector] = new_value
            break
    
    # Renormalize other weights to keep total at 100%
    total_weight = sum(sector_weights.values())
    if abs(total_weight - 100) > 0.01:  # Allow small rounding errors
        # Calculate how much other weights need to be adjusted
        other_sectors = [s for s in sector_weights.keys() if s != clicked_sector]
        if other_sectors:
            other_weights_sum = sum(sector_weights[s] for s in other_sectors)
            if other_weights_sum > 0:  # Avoid division by zero
                scale_factor = (100 - sector_weights[clicked_sector]) / other_weights_sum
                for sector in other_sectors:
                    sector_weights[sector] = round(sector_weights[sector] * scale_factor, 2)
                
                # Final adjustment to ensure total is exactly 100%
                final_total = sum(sector_weights.values())
                if abs(final_total - 100) > 0.01:
                    # Find a sector to adjust to make total exactly 100%
                    adjust_sector = next((s for s in sector_weights if s != clicked_sector), None)
                    if adjust_sector:
                        sector_weights[adjust_sector] += round(100 - final_total, 2)
            
    return json.dumps(sector_weights)


@app.callback(
    [Output({"type": "weight-input", "index": ALL}, "value"),
     Output("t2d-pulse-value", "children"),
     Output({"type": "apply-weight", "index": ALL}, "style"),
     Output("weight-update-notification", "children"),
     Output("weight-update-notification", "style")],
    [Input("stored-weights", "children")],
    prevent_initial_call=True
)
def update_weight_inputs(weights_json):
    """
    Update the weight input fields and T2D Pulse score when weights change
    """
    if not weights_json:
        raise dash.exceptions.PreventUpdate
        
    try:
        weights = json.loads(weights_json)
    except:
        raise dash.exceptions.PreventUpdate
    
    # Get the ordered list of sectors
    ordered_sectors = sorted(weights.keys())
    
    # Update all input values
    input_values = [round(weights[sector], 2) for sector in ordered_sectors]
    
    # Calculate the new T2D Pulse score
    sector_scores = calculate_sector_sentiment()
    t2d_pulse = calculate_t2d_pulse_from_sectors(sector_scores, weights)
    t2d_pulse_display = f"{t2d_pulse:.1f}"
    
    # Update button styling
    button_styles = []
    for sector in ordered_sectors:
        style = {
            "fontSize": "12px",
            "padding": "4px 8px",
            "backgroundColor": "#e74c3c",
            "color": "white",
            "border": "none",
            "borderRadius": "4px",
            "cursor": "pointer",
            "fontWeight": "bold",
            "marginLeft": "8px"
        }
        button_styles.append(style)
    
    # Create notification message
    current_time = datetime.now().strftime("%H:%M:%S")
    notification_message = f"Weights updated at {current_time} - T2D Pulse score: {t2d_pulse_display}"
    notification_style = {
        "color": "green",
        "fontWeight": "bold",
        "fontSize": "14px",
        "backgroundColor": "#e8f5e9",
        "borderRadius": "4px",
        "opacity": 1
    }
    
    return input_values, t2d_pulse_display, button_styles, notification_message, notification_style
'''

# Combined Fixed Callback
# ----------------------
# Replace both callbacks above with this single callback 
# to eliminate conflicts and race conditions:

'''
@app.callback(
    [Output({"type": "weight-input", "index": sector}, "value") for sector in SECTORS] +
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
def direct_weight_update(n_clicks_list, input_values, input_ids, weights_json):
    """
    Combined callback that directly updates inputs and feedback without intermediate storage
    """
    # Skip if not triggered by a button click
    if not ctx.triggered or not any(n is not None for n in n_clicks_list):
        raise dash.exceptions.PreventUpdate
        
    # Find which button was clicked
    trigger = ctx.triggered[0]['prop_id']
    trigger_id = json.loads(trigger.split('.')[0])
    clicked_sector = trigger_id['index']
    
    # Get current weights, defaulting to standard weights if nothing is stored yet
    if weights_json:
        try:
            weights = json.loads(weights_json)
        except:
            weights = DEFAULT_WEIGHTS.copy()
    else:
        weights = DEFAULT_WEIGHTS.copy()
    
    # Find the sector weight that was changed
    for i, sector_id in enumerate(input_ids):
        if sector_id['index'] == clicked_sector:
            new_value = input_values[i]
            # Basic validation - enforce min 1%, max 100%
            new_value = max(1, min(100, new_value))
            
            # Update the weight
            weights[clicked_sector] = new_value
            break
    
    # Renormalize other weights to keep total at 100%
    total_weight = sum(weights.values())
    if abs(total_weight - 100) > 0.01:  # Allow small rounding errors
        # Calculate how much other weights need to be adjusted
        other_sectors = [s for s in weights.keys() if s != clicked_sector]
        if other_sectors:
            other_weights_sum = sum(weights[s] for s in other_sectors)
            if other_weights_sum > 0:  # Avoid division by zero
                scale_factor = (100 - weights[clicked_sector]) / other_weights_sum
                for sector in other_sectors:
                    weights[sector] = round(weights[sector] * scale_factor, 2)
                
                # Final adjustment to ensure total is exactly 100%
                final_total = sum(weights.values())
                if abs(final_total - 100) > 0.01:
                    # Find a sector to adjust to make total exactly 100%
                    adjust_sector = next((s for s in weights if s != clicked_sector), None)
                    if adjust_sector:
                        weights[adjust_sector] += round(100 - final_total, 2)
    
    # Update all input values in the correct order matching the output definition
    updated_values = [round(weights[sector], 2) for sector in SECTORS]
    
    # Calculate the new T2D Pulse score
    sector_scores = calculate_sector_sentiment()
    t2d_pulse = calculate_t2d_pulse_from_sectors(sector_scores, weights)
    t2d_pulse_display = f"{t2d_pulse:.1f}"
    
    # Create notification message
    current_time = datetime.now().strftime("%H:%M:%S")
    notification_message = f"Weights updated at {current_time} - T2D Pulse score: {t2d_pulse_display}"
    notification_style = {
        "color": "green",
        "fontWeight": "bold",
        "fontSize": "14px",
        "backgroundColor": "#e8f5e9",
        "borderRadius": "4px",
        "opacity": 1
    }
    
    return updated_values + [t2d_pulse_display, json.dumps(weights), notification_message, notification_style]
'''

# How to Apply the Fix
# -------------------
# 1. In app.py, locate and remove BOTH of the callbacks:
#    - update_weight_from_input
#    - update_weight_inputs 
# 
# 2. Replace them with the combined direct_weight_update callback above
#
# 3. Ensure that SECTORS is defined - it should be the list of sector names in the same order
#    as they appear in the Output list
#
# 4. This fix eliminates the race condition between updating stored weights and
#    reading them back, by handling both operations in a single callback
#
# 5. The client-side JavaScript in weight-updater-enhanced.js still works with this
#    server-side fix to provide immediate visual feedback