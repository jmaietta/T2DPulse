"""
Fix for applying zero-weight inputs in the T2D Pulse dashboard

This script updates the two weight input callbacks in app.py to handle:
1. Zero-value inputs (0%)
2. None/empty inputs
3. Non-numeric inputs

Both apply_weight and apply_weight_on_enter functions are modified.
"""
import os
import shutil
import re

# Make a backup
backup_path = 'app_backup_before_zero_weight.py'
shutil.copy('app.py', backup_path)
print(f"Created backup at {backup_path}")

# Read the entire app.py file
with open('app.py', 'r') as file:
    content = file.read()

# First function replacement - apply_weight
apply_weight_pattern = r'def apply_weight\(n_clicks_list, weight_values, weights_json\):.*?return json\.dumps\(weights\)'
apply_weight_replacement = '''def apply_weight(n_clicks_list, weight_values, weights_json):
    global sector_weights
    global fixed_sectors  # Track which sectors should remain fixed
    
    # Initialize fixed_sectors if it doesn't exist
    if 'fixed_sectors' not in globals():
        fixed_sectors = set()
    
    # Determine which button was clicked
    if not any(click for click in n_clicks_list if click):
        raise PreventUpdate
    
    # Get trigger information
    ctx = dash.callback_context
    if not ctx.triggered:
        raise PreventUpdate
    
    # Extract sector from triggered ID
    trigger_id = ctx.triggered[0]["prop_id"].split(".")[0]
    trigger_dict = json.loads(trigger_id)
    sector_to_update = trigger_dict["index"]
    
    # Find the index of the sector in the weight_values list
    input_index = 0  # Default value in case we can't find a match
    ids = [{'index': x['id']['index']} for x in ctx.states_list[0]]
    for i, callback_id in enumerate(ids):
        if callback_id["index"] == sector_to_update:
            input_index = i
            break
    
    # Use stored weights if available
    if weights_json:
        try:
            weights = json.loads(weights_json)
        except:
            weights = sector_weights.copy()
    else:
        weights = sector_weights.copy()
    
    # Get the new weight value from input (with None handling)
    input_value = weight_values[input_index]
    if input_value is None or str(input_value).strip() == '':
        # If input is empty, keep the old weight
        new_weight = weights[sector_to_update]
    else:
        try:
            # Allow 0 as minimum weight
            new_weight = max(0, min(100, float(input_value)))
        except (ValueError, TypeError):
            # If conversion fails, keep the old weight
            new_weight = weights[sector_to_update]
    
    # Calculate the difference that needs to be distributed
    old_weight = weights[sector_to_update]
    weight_difference = new_weight - old_weight
    
    # Apply the new weight
    weights[sector_to_update] = new_weight
    
    # Add this sector to the fixed sectors
    fixed_sectors.add(sector_to_update)
    
    # If there's a difference to distribute
    if weight_difference != 0:
        # Get all sectors except those that are fixed
        adjustable_sectors = [s for s in weights.keys() if s != sector_to_update and s not in fixed_sectors]
        
        # If all sectors are fixed except the one we just updated, clear the fixed sectors to allow adjustment
        if not adjustable_sectors:
            fixed_sectors = {sector_to_update}
            adjustable_sectors = [s for s in weights.keys() if s != sector_to_update]
        
        # Calculate the sum of weights from adjustable sectors
        adjustable_weight_sum = sum(weights[s] for s in adjustable_sectors)
        
        # Distribute the difference proportionally
        if adjustable_weight_sum > 0:  # Avoid division by zero
            for s in adjustable_sectors:
                # Calculate proportional adjustment
                proportion = weights[s] / adjustable_weight_sum
                adjustment = -weight_difference * proportion
                weights[s] = max(0, weights[s] + adjustment)
    
    # Ensure weights sum to exactly 100%
    total = sum(weights.values())
    if total != 100 and total > 0:
        # Find the largest weight to adjust (that isn't fixed)
        unfixed_sectors = [s for s in weights.keys() if s != sector_to_update and s not in fixed_sectors]
        if unfixed_sectors:
            largest_sector = max(unfixed_sectors, key=lambda x: weights[x])
        else:
            # If all are fixed or none left with weight > 0, adjust the sector we just updated
            largest_sector = sector_to_update
        
        weights[largest_sector] += (100 - total)
    
    # Format all weights to 2 decimal places for display
    for s in weights:
        weights[s] = round(weights[s], 2)
    
    return json.dumps(weights)'''

# Second function replacement - apply_weight_on_enter
apply_weight_on_enter_pattern = r'def apply_weight_on_enter\(n_clicks_list, weight_values, weights_json\):.*?return json\.dumps\(weights\)'
apply_weight_on_enter_replacement = '''def apply_weight_on_enter(n_clicks_list, weight_values, weights_json):
    global sector_weights
    global fixed_sectors  # Track which sectors should remain fixed
    
    # Initialize fixed_sectors if it doesn't exist
    if 'fixed_sectors' not in globals():
        fixed_sectors = set()
    
    # Determine which input triggered the callback
    if not any(click for click in n_clicks_list if click):
        raise PreventUpdate
        
    # Find which input had an n_submit value
    input_index = next((i for i, n in enumerate(n_clicks_list) if n), None)
    if input_index is None:
        raise PreventUpdate
    
    # Map input index to sector
    sectors = ["Cloud", "AI", "Cybersecurity", "AdTech", "FinTech", "EdTech", 
              "HealthTech", "AR/VR", "Robotics", "Blockchain", "IoT", 
              "CleanTech", "SmartHome", "Ecommerce"]
    sector_to_update = sectors[input_index]
    
    # Use stored weights if available
    if weights_json:
        try:
            weights = json.loads(weights_json)
        except:
            weights = sector_weights.copy()
    else:
        weights = sector_weights.copy()
    
    # Get the new weight value from input (with None handling)
    input_value = weight_values[input_index]
    if input_value is None or str(input_value).strip() == '':
        # If input is empty, keep the old weight
        new_weight = weights[sector_to_update]
    else:
        try:
            # Allow 0 as minimum weight
            new_weight = max(0, min(100, float(input_value)))
        except (ValueError, TypeError):
            # If conversion fails, keep the old weight
            new_weight = weights[sector_to_update]
    
    # Calculate the difference that needs to be distributed
    old_weight = weights[sector_to_update]
    weight_difference = new_weight - old_weight
    
    # Apply the new weight
    weights[sector_to_update] = new_weight
    
    # Add this sector to the fixed sectors
    fixed_sectors.add(sector_to_update)
    
    # If there's a difference to distribute
    if weight_difference != 0:
        # Get all sectors except those that are fixed
        adjustable_sectors = [s for s in weights.keys() if s != sector_to_update and s not in fixed_sectors]
        
        # If all sectors are fixed except the one we just updated, clear the fixed sectors to allow adjustment
        if not adjustable_sectors:
            fixed_sectors = {sector_to_update}
            adjustable_sectors = [s for s in weights.keys() if s != sector_to_update]
        
        # Calculate the sum of weights from adjustable sectors
        adjustable_weight_sum = sum(weights[s] for s in adjustable_sectors)
        
        # Distribute the difference proportionally
        if adjustable_weight_sum > 0:  # Avoid division by zero
            for s in adjustable_sectors:
                # Calculate proportional adjustment
                proportion = weights[s] / adjustable_weight_sum
                adjustment = -weight_difference * proportion
                weights[s] = max(0, weights[s] + adjustment)
    
    # Ensure weights sum to exactly 100%
    total = sum(weights.values())
    if total != 100 and total > 0:
        # Find the largest weight to adjust (that isn't fixed)
        unfixed_sectors = [s for s in weights.keys() if s != sector_to_update and s not in fixed_sectors]
        if unfixed_sectors:
            largest_sector = max(unfixed_sectors, key=lambda x: weights[x])
        else:
            # If all are fixed or none left with weight > 0, adjust the sector we just updated
            largest_sector = sector_to_update
        
        weights[largest_sector] += (100 - total)
    
    # Format all weights to 2 decimal places for display
    for s in weights:
        weights[s] = round(weights[s], 2)
    
    return json.dumps(weights)'''

# Apply the replacements using regular expressions with DOTALL flag to match across lines
updated_content = re.sub(apply_weight_pattern, apply_weight_replacement, content, flags=re.DOTALL)
updated_content = re.sub(apply_weight_on_enter_pattern, apply_weight_on_enter_replacement, updated_content, flags=re.DOTALL)

# Write the updated content back to app.py
with open('app.py', 'w') as file:
    file.write(updated_content)

print("Successfully updated app.py with zero-weight handling improvements")