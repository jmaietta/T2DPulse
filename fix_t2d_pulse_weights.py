#!/usr/bin/env python3
"""
T2D Pulse Weight Fix Script

This script fixes the weight update issues in the T2D Pulse dashboard by:
1. Identifying the problematic callback functions in app.py
2. Replacing them with a single combined callback that avoids race conditions
3. Creating a backup of the original file before making changes

Usage:
  python fix_t2d_pulse_weights.py

The script will create app.py.bak before making any changes.
"""

import os
import re
import sys
from datetime import datetime

def backup_file(file_path):
    """Create a backup of the specified file"""
    backup_path = file_path + ".bak"
    try:
        with open(file_path, 'r') as src:
            with open(backup_path, 'w') as dst:
                dst.write(src.read())
        print(f"✓ Created backup at {backup_path}")
        return True
    except Exception as e:
        print(f"✗ Failed to create backup: {e}")
        return False

def identify_sectors(content):
    """Try to identify the sectors list in the file"""
    # Look for sector definitions using multiple patterns
    patterns = [
        r'sectors\s*=\s*\[(.*?)\]',
        r'SECTORS\s*=\s*\[(.*?)\]', 
        r'sector_weights\s*=\s*{(.*?)}'
    ]
    
    for pattern in patterns:
        match = re.search(pattern, content, re.DOTALL)
        if match:
            # Extract sector names from the matched pattern
            sector_text = match.group(1)
            # Extract items in quotes with optional trailing commas
            sectors = re.findall(r'"([^"]+)"|\'([^\']+)\'', sector_text)
            # Flatten the tuples and remove empty strings
            sectors = [s[0] if s[0] else s[1] for s in sectors]
            if sectors:
                return sectors
    
    # Default fallback sectors if we can't find them
    print("⚠ Could not identify sectors list, using default sector names")
    return ["Cloud", "SaaS", "Fintech", "AdTech", "Digital Media", "IT Services", 
            "Consumer Tech", "Payments", "Cybersecurity", "Infrastructure"]

def find_problematic_callbacks(content):
    """Find the problematic callback functions for weight updates"""
    # Look for the update_weight_from_input function
    weight_input_pattern = r'(@app\.callback.*?def\s+update_weight_from_input.*?)((?=@app\.callback)|$)'
    input_match = re.search(weight_input_pattern, content, re.DOTALL)
    
    # Look for the update_weight_inputs function
    weight_display_pattern = r'(@app\.callback.*?def\s+update_weight_inputs.*?)((?=@app\.callback)|$)'
    display_match = re.search(weight_display_pattern, content, re.DOTALL)
    
    return input_match, display_match

def create_fixed_callback(sectors):
    """Create the fixed callback function with the given sectors"""
    sectors_str = ", ".join([f'"{s}"' for s in sectors])
    
    # Get the current timestamp
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    fixed_callback = f'''
# Fixed weight update callback (T2D Pulse fix applied {timestamp})
@app.callback(
    [Output({{"type": "weight-input", "index": sector}}, "value") for sector in [{sectors_str}]] +
    [Output("t2d-pulse-value", "children"),
     Output("stored-weights", "children"),
     Output("weight-update-notification", "children"),
     Output("weight-update-notification", "style")],
    [Input({{"type": "apply-weight", "index": ALL}}, "n_clicks")],
    [State({{"type": "weight-input", "index": ALL}}, "value"),
     State({{"type": "weight-input", "index": ALL}}, "id"),
     State("stored-weights", "children")],
    prevent_initial_call=True
)
def direct_weight_update(n_clicks_list, input_values, input_ids, weights_json):
    """
    Combined callback that directly updates inputs and feedback without intermediate storage
    This fixes the weight update issue by eliminating the callback chain.
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
    changed_idx = None
    for i, sector_id in enumerate(input_ids):
        if sector_id['index'] == clicked_sector:
            changed_idx = i
            new_value = input_values[i]
            # Basic validation - enforce min 1%, max 100%
            new_value = float(max(1, min(100, new_value)))
            
            # Update the weight
            weights[clicked_sector] = new_value
            break
    
    if changed_idx is None:
        raise dash.exceptions.PreventUpdate
    
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
    sectors_list = [{sectors_str}]
    updated_values = [round(weights.get(sector, DEFAULT_WEIGHTS.get(sector, 8.33)), 2) for sector in sectors_list]
    
    # Calculate the new T2D Pulse score
    sector_scores = calculate_sector_sentiment()
    t2d_pulse = calculate_t2d_pulse_from_sectors(sector_scores, weights)
    t2d_pulse_display = f"{{t2d_pulse:.1f}}"
    
    # Create notification message
    current_time = datetime.now().strftime("%H:%M:%S")
    notification_message = f"Weights updated at {{current_time}} - T2D Pulse score: {{t2d_pulse_display}}"
    notification_style = {{
        "color": "green",
        "fontWeight": "bold",
        "fontSize": "14px",
        "backgroundColor": "#e8f5e9",
        "borderRadius": "4px",
        "opacity": 1
    }}
    
    return updated_values + [t2d_pulse_display, json.dumps(weights), notification_message, notification_style]
'''
    return fixed_callback

def fix_app_file(file_path):
    """Fix the app.py file by replacing problematic callbacks"""
    try:
        with open(file_path, 'r') as f:
            content = f.read()
        
        # Step 1: Identify sectors
        sectors = identify_sectors(content)
        print(f"✓ Found {len(sectors)} sectors: {', '.join(sectors)}")
        
        # Step 2: Find problematic callbacks
        input_match, display_match = find_problematic_callbacks(content)
        if not input_match or not display_match:
            print("✗ Could not find problematic callback functions")
            print("  Make sure the file contains update_weight_from_input and update_weight_inputs functions")
            return False
        
        # Step 3: Create fixed callback
        fixed_callback = create_fixed_callback(sectors)
        
        # Step 4: Replace the problematic callbacks with the fixed one
        fixed_content = content
        if input_match and display_match:
            # If we found both callbacks, replace the first one with the fixed version
            # and remove the second one completely
            start1, end1 = input_match.span(1)
            start2, end2 = display_match.span(1)
            
            # If first callback comes after second, swap them
            if start1 > start2:
                start1, start2 = start2, start1
                end1, end2 = end2, end1
            
            # Replace first callback with fixed version and remove second callback
            fixed_content = content[:start1] + fixed_callback + content[end1:start2] + content[end2:]
        
        # Step 5: Write the fixed content back to the file
        with open(file_path, 'w') as f:
            f.write(fixed_content)
        
        print(f"✓ Successfully fixed {file_path}")
        print("✓ Replaced problematic callbacks with combined direct_weight_update function")
        return True
    
    except Exception as e:
        print(f"✗ Error fixing app file: {e}")
        return False

def main():
    """Main function to fix the T2D Pulse dashboard"""
    print("\n 🔧 T2D Pulse Weight Update Fix 🔧")
    print("=================================")
    
    # Check if app.py exists
    file_path = "app.py"
    if not os.path.exists(file_path):
        print(f"✗ Could not find {file_path}")
        return False
    
    # Backup the original file
    if not backup_file(file_path):
        print("✗ Aborting fix due to backup failure")
        return False
    
    # Fix the app file
    if fix_app_file(file_path):
        print("\n✅ Fix completed successfully!")
        print("\nIf you encounter any issues, you can restore the backup:")
        print(f"  cp {file_path}.bak {file_path}")
        return True
    else:
        print("\n❌ Fix failed!")
        print("\nYou can try manually applying the fix by:")
        print("1. Finding and removing the update_weight_from_input and update_weight_inputs callbacks")
        print("2. Adding the direct_weight_update callback from app_fixed_weights.py")
        print("\nOr restore the backup:")
        print(f"  cp {file_path}.bak {file_path}")
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)