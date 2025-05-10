#!/usr/bin/env python3
"""
Integrate authentic market cap values into app.py.
This script updates the app.py file to use our authentic market cap data
and ensures proper handling of sector weights throughout the dashboard.
"""

import os
import re
import json
import logging
import shutil
from datetime import datetime
from pathlib import Path

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
)

# Define paths
APP_PY = "app.py"
BACKUP_DIR = "backups"
DATA_DIR = "data"
SECTOR_WEIGHTS_FILE = os.path.join(DATA_DIR, "sector_weights_latest.json")

def backup_app_py():
    """Create a backup of the app.py file"""
    if not os.path.exists(APP_PY):
        logging.error(f"app.py file not found: {APP_PY}")
        return False
    
    # Create backup directory if it doesn't exist
    Path(BACKUP_DIR).mkdir(exist_ok=True)
    
    # Generate backup filename with timestamp
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    backup_file = os.path.join(BACKUP_DIR, f"app_py_backup_{timestamp}.py")
    
    # Copy the file
    try:
        shutil.copy2(APP_PY, backup_file)
        logging.info(f"Created backup of app.py at {backup_file}")
        return True
    except Exception as e:
        logging.error(f"Failed to create backup: {e}")
        return False

def load_sector_weights():
    """Load the sector weights from the JSON file"""
    if not os.path.exists(SECTOR_WEIGHTS_FILE):
        logging.error(f"Sector weights file not found: {SECTOR_WEIGHTS_FILE}")
        return None
    
    try:
        with open(SECTOR_WEIGHTS_FILE, 'r') as f:
            data = json.load(f)
        
        if 'weights' in data:
            return data['weights']
        else:
            logging.error(f"No 'weights' key found in {SECTOR_WEIGHTS_FILE}")
            return None
    except Exception as e:
        logging.error(f"Error loading sector weights: {e}")
        return None

def add_authentic_market_cap_function(app_py_content):
    """Add or update the function to load authentic market cap data"""
    function_code = """
def get_authentic_market_caps():
    """Get authentic market cap data from the saved JSON file"""
    try:
        with open('data/sector_market_caps.json', 'r') as f:
            market_caps_data = json.load(f)
        return market_caps_data['market_caps']
    except Exception as e:
        logging.error(f"Error loading authentic market cap data: {e}")
        return None
"""
    
    # Check if the function already exists
    if "def get_authentic_market_caps():" in app_py_content:
        logging.info("get_authentic_market_caps function already exists, skipping")
        return app_py_content
    
    # Add the function after the imports
    import_section_end = app_py_content.find("# Create the Dash app")
    if import_section_end == -1:
        import_section_end = app_py_content.find("app = dash.Dash")
    
    if import_section_end == -1:
        logging.warning("Could not find a good location to insert the function")
        # Just add it at the beginning
        return function_code + app_py_content
    
    return app_py_content[:import_section_end] + function_code + app_py_content[import_section_end:]

def update_sector_weights_code(app_py_content):
    """Update how sector weights are loaded and used"""
    # Look for the calculate_t2d_pulse_from_sectors function and update it
    t2d_pulse_function_pattern = r"def calculate_t2d_pulse_from_sectors\(sector_scores,.*?(:?\n\s*return .*?\n)"
    t2d_pulse_function_replacement = """def calculate_t2d_pulse_from_sectors(sector_scores, sector_weights=None):
    """Calculate T2D Pulse score as a weighted average of sector scores
    
    Args:
        sector_scores (dict or list): Dictionary with sector scores {sector: score} or list of sector dictionaries
        sector_weights (dict, optional): Dictionary with custom weights for each sector
        
    Returns:
        float: The weighted average T2D Pulse score (0-100 scale)
    """
    # Initialize weights if not provided
    if sector_weights is None:
        try:
            # First try to load authentic weights
            with open('data/sector_weights_latest.json', 'r') as f:
                weights_data = json.load(f)
            sector_weights = weights_data.get('weights', {})
            logging.info("Loaded latest sector weights from data/sector_weights_latest.json")
        except Exception as e:
            logging.warning(f"Could not load authentic market cap weights: {e}")
            # Fall back to default equal weights
            sector_weights = {}
    
    # Handle list format of sector_scores
    if isinstance(sector_scores, list):
        # Convert list of dicts to single dict
        scores_dict = {}
        for sector_data in sector_scores:
            scores_dict[sector_data['sector']] = sector_data['score']
        sector_scores = scores_dict
    
    # Ensure all sectors have weights
    # If no weights provided or missing weights, use equal weighting
    sectors = list(sector_scores.keys())
    total_weight = 0
    for sector in sectors:
        if sector not in sector_weights:
            sector_weights[sector] = 100 / len(sectors)
        total_weight += sector_weights[sector]
    
    # Normalize weights if they don't add up to 100
    if abs(total_weight - 100.0) > 0.01:
        logging.warning(f"Sector weights sum to {total_weight}%, normalizing to 100%")
        for sector in sector_weights:
            sector_weights[sector] = (sector_weights[sector] / total_weight) * 100
    
    # Calculate weighted average
    total_score = 0
    used_weight = 0
    
    for sector, score in sector_scores.items():
        if sector in sector_weights:
            weight = sector_weights[sector]
            total_score += score * weight
            used_weight += weight
    
    # Avoid division by zero
    if used_weight == 0:
        return 50.0  # Return neutral score if no weights used
    
    # Normalize by the actual weight used
    final_score = total_score / used_weight
    
    # Ensure score is within bounds
    final_score = max(0, min(100, final_score))
    return final_score
"""
    
    updated_content = re.sub(t2d_pulse_function_pattern, t2d_pulse_function_replacement, app_py_content, flags=re.DOTALL)
    
    # If nothing was replaced, the pattern didn't match
    if updated_content == app_py_content:
        logging.warning("Could not find and update the calculate_t2d_pulse_from_sectors function")
    
    return updated_content

def update_t2d_pulse_score_callback(app_py_content):
    """Update the update_t2d_pulse_score callback to use authentic weights"""
    callback_pattern = r"def update_t2d_pulse_score\(weights_json\):.*?(:?\n\s*return .*?\n)"
    callback_replacement = """def update_t2d_pulse_score(weights_json):
    """
    Update the T2D Pulse score when weights change
    This callback is triggered whenever the stored weights are updated
    """
    try:
        # Parse the stored weights
        current_weights = json.loads(weights_json)
        
        # Get sector scores
        sector_scores = {
            sector: calculate_sector_sentiment()[sector]['score']
            for sector in calculate_sector_sentiment()
        }
        
        # Calculate new T2D Pulse score
        t2d_pulse_score = calculate_t2d_pulse_from_sectors(sector_scores, current_weights)
        
        # Format for display
        pulse_card, pulse_status, pulse_color = create_pulse_card(t2d_pulse_score)
        
        # Store the updated score for use in other callbacks
        app.T2D_PULSE_SCORE = t2d_pulse_score
        
        # Save to authentic sector history
        try:
            # Create data directory if it doesn't exist
            os.makedirs("data", exist_ok=True)
            
            # Get current date in ET
            current_date = get_eastern_date().strftime('%Y-%m-%d')
            
            # Update the authentic history
            update_authentic_pulse_history(current_date, t2d_pulse_score)
            
            logging.info(f"Updated authentic pulse history with score {t2d_pulse_score} for {current_date}")
        except Exception as e:
            logging.error(f"Error updating authentic pulse history: {e}")
        
        return [
            pulse_card, 
            pulse_status,
            {'background': pulse_color}
        ]
    except Exception as e:
        logging.error(f"Error updating T2D Pulse score: {e}")
        return [
            dbc.Card("Error Calculating Score", className="p-4"),
            "Error", 
            {'background': '#FF0000'}
        ]
"""
    
    updated_content = re.sub(callback_pattern, callback_replacement, app_py_content, flags=re.DOTALL)
    
    # If nothing was replaced, the pattern didn't match
    if updated_content == app_py_content:
        logging.warning("Could not find and update the update_t2d_pulse_score callback")
    
    return updated_content

def add_authentic_pulse_history_function(app_py_content):
    """Add a function to update the authentic pulse history"""
    function_code = """
def update_authentic_pulse_history(date_str, pulse_score):
    """Update the authentic pulse history with the latest score"""
    history_file = "data/authentic_pulse_history.json"
    
    # Load existing history or create new
    try:
        if os.path.exists(history_file):
            with open(history_file, 'r') as f:
                history = json.load(f)
        else:
            history = {"dates": [], "scores": []}
    except Exception as e:
        logging.error(f"Error loading authentic pulse history: {e}")
        history = {"dates": [], "scores": []}
    
    # Check if we already have an entry for this date
    if date_str in history["dates"]:
        # Update existing entry
        idx = history["dates"].index(date_str)
        history["scores"][idx] = pulse_score
    else:
        # Add new entry
        history["dates"].append(date_str)
        history["scores"].append(pulse_score)
    
    # Sort by date
    sorted_pairs = sorted(zip(history["dates"], history["scores"]))
    history["dates"] = [pair[0] for pair in sorted_pairs]
    history["scores"] = [pair[1] for pair in sorted_pairs]
    
    # Save updated history
    try:
        with open(history_file, 'w') as f:
            json.dump(history, f, indent=2)
    except Exception as e:
        logging.error(f"Error saving authentic pulse history: {e}")

def get_authentic_pulse_score(filename="data/authentic_pulse_history.json"):
    """Get the most recent authentic T2D Pulse score calculated from sector data"""
    try:
        if os.path.exists(filename):
            with open(filename, 'r') as f:
                history = json.load(f)
            
            if history["dates"] and history["scores"]:
                # Return the most recent score
                return history["scores"][-1]
    except Exception as e:
        logging.error(f"Error reading authentic pulse score: {e}")
    
    # Default value if we can't get the authentic score
    return 50.0
"""
    
    # Check if the function already exists
    if "def update_authentic_pulse_history(" in app_py_content:
        logging.info("update_authentic_pulse_history function already exists, skipping")
        return app_py_content
    
    # Add the function after the imports
    import_section_end = app_py_content.find("# Create the Dash app")
    if import_section_end == -1:
        import_section_end = app_py_content.find("app = dash.Dash")
    
    if import_section_end == -1:
        logging.warning("Could not find a good location to insert the function")
        # Just add it at the beginning
        return function_code + app_py_content
    
    return app_py_content[:import_section_end] + function_code + app_py_content[import_section_end:]

def update_app_py():
    """Update the app.py file with authentic market cap integration"""
    if not os.path.exists(APP_PY):
        logging.error(f"app.py file not found: {APP_PY}")
        return False
    
    # 1. Backup the file
    if not backup_app_py():
        logging.warning("Could not create backup, continuing anyway")
    
    # 2. Read the file content
    with open(APP_PY, 'r') as f:
        content = f.read()
    
    # 3. Add/update functions for authentic market caps
    content = add_authentic_market_cap_function(content)
    
    # 4. Update sector weight calculations
    content = update_sector_weights_code(content)
    
    # 5. Update T2D Pulse score callback
    content = update_t2d_pulse_score_callback(content)
    
    # 6. Add authentic pulse history functions
    content = add_authentic_pulse_history_function(content)
    
    # 7. Write back the updated content
    with open(APP_PY, 'w') as f:
        f.write(content)
    
    logging.info("Successfully updated app.py with authentic market cap integration")
    return True

def main():
    """Main function to integrate authentic market cap values"""
    print("Integrating authentic market cap values into the dashboard...")
    
    # 1. Make sure the sector weights file exists
    sector_weights = load_sector_weights()
    if not sector_weights:
        print("Sector weights file not found or invalid. Run update_sector_market_caps.py first.")
        return False
    
    # 2. Update app.py
    if not update_app_py():
        print("Failed to update app.py. Check the logs for details.")
        return False
    
    print("\nSuccessfully integrated authentic market cap values!")
    print("Restart the Economic Dashboard Server to apply these changes.")
    
    return True

if __name__ == "__main__":
    main()