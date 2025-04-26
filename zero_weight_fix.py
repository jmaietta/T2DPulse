# Create a copy of the app.py file with an improved function for zero weights
import shutil
import re

# Make a backup copy
shutil.copy('app.py', 'app_original_backup.py')

with open('app.py', 'r') as file:
    content = file.read()

# Pattern to find the apply_weight function
pattern1 = r'def apply_weight\(n_clicks_list, weight_values, weights_json\):[^}]*?new_weight = max\(0, min\(100, float\(weight_values\[input_index\]\)\)\)[^}]*?return json.dumps\(weights\)'

# Replacement with improved error handling
replacement1 = '''def apply_weight(n_clicks_list, weight_values, weights_json):
    """
    Apply a new weight when the Apply button is clicked
    This will adjust other weights proportionally to maintain a 100% total
    """
    if not any(n_clicks_list):
        # No button was clicked
        return weights_json
    
    # Find which button was clicked
    input_index = next((i for i, n in enumerate(n_clicks_list) if n), None)
    if input_index is None:
        return weights_json
    
    # Map input index to sector
    sectors = ["Cloud", "AI", "Cybersecurity", "AdTech", "FinTech", "EdTech", 
              "HealthTech", "AR/VR", "Robotics", "Blockchain", "IoT", 
              "CleanTech", "SmartHome", "Ecommerce"]
    sector_to_update = sectors[input_index]
    
    # Parse the current weights
    if weights_json:
        weights = json.loads(weights_json)
    else:
        weights = sector_weights.copy()
    
    # Get the new weight value from input (with None handling)
    input_value = weight_values[input_index]
    if input_value is None or str(input_value).strip() == '':
        # If input is empty, keep the old weight
        new_weight = weights[sector_to_update]
    else:
        try:
            new_weight = max(0, min(100, float(input_value)))
        except (ValueError, TypeError):
            # If conversion fails, keep the old weight
            new_weight = weights[sector_to_update]
    
    # Calculate the difference that needs to be distributed
    old_weight = weights[sector_to_update]
    weight_difference = new_weight - old_weight
    
    # Apply the new weight
    weights[sector_to_update] = new_weight
    
    # If there's a difference to distribute
    if weight_difference != 0:
        # Get all sectors except the one being updated
        other_sectors = [s for s in weights.keys() if s != sector_to_update]
        
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
    
    return json.dumps(weights)'''

# Pattern to find the apply_weight_on_enter function
pattern2 = r'def apply_weight_on_enter\(n_clicks_list, weight_values, weights_json\):[^}]*?new_weight = max\(0, min\(100, float\(weight_values\[input_index\]\)\)\)[^}]*?return json.dumps\(weights\)'

# Replacement with improved error handling
replacement2 = '''def apply_weight_on_enter(n_clicks_list, weight_values, weights_json):
    """
    Apply a new weight when Enter is pressed in an input field
    This callback is triggered by the n_submit property of the inputs
    """
    if not any(n_clicks_list):
        # No enter key was pressed
        return weights_json
    
    # Find which input triggered the submit
    input_index = next((i for i, n in enumerate(n_clicks_list) if n), None)
    if input_index is None:
        return weights_json
    
    # Map input index to sector
    sectors = ["Cloud", "AI", "Cybersecurity", "AdTech", "FinTech", "EdTech", 
              "HealthTech", "AR/VR", "Robotics", "Blockchain", "IoT", 
              "CleanTech", "SmartHome", "Ecommerce"]
    sector_to_update = sectors[input_index]
    
    # Parse the current weights
    if weights_json:
        weights = json.loads(weights_json)
    else:
        weights = sector_weights.copy()
    
    # Get the new weight value from input (with None handling)
    input_value = weight_values[input_index]
    if input_value is None or str(input_value).strip() == '':
        # If input is empty, keep the old weight
        new_weight = weights[sector_to_update]
    else:
        try:
            new_weight = max(0, min(100, float(input_value)))
        except (ValueError, TypeError):
            # If conversion fails, keep the old weight
            new_weight = weights[sector_to_update]
    
    # Calculate the difference that needs to be distributed
    old_weight = weights[sector_to_update]
    weight_difference = new_weight - old_weight
    
    # Apply the new weight
    weights[sector_to_update] = new_weight
    
    # If there's a difference to distribute
    if weight_difference != 0:
        # Get all sectors except the one being updated
        other_sectors = [s for s in weights.keys() if s != sector_to_update]
        
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
    
    return json.dumps(weights)'''

# Replace the functions
content = re.sub(pattern1, replacement1, content, flags=re.DOTALL)
content = re.sub(pattern2, replacement2, content, flags=re.DOTALL)

# Write the changes back to app.py
with open('app.py', 'w') as file:
    file.write(content)

print("Updated apply_weight functions with improved error handling for zero weights")
