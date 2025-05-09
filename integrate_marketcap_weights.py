"""
Integrate Market Cap Weights Into T2D Pulse Dashboard

This script integrates authentic market cap weights into the sector weight calculations
for the T2D Pulse dashboard.

It updates the calculate_t2d_pulse_from_sectors function in app.py to use
authentic market cap data from Polygon.io as the default weighting method.
"""

import os
import re
import logging
import shutil
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

# Backup the original app.py file
def backup_app_py():
    """Create a backup of app.py before modifying it"""
    try:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_file = f"app_before_marketcap_integration_{timestamp}.py"
        shutil.copy2("app.py", backup_file)
        logging.info(f"Created backup of app.py at {backup_file}")
        return True
    except Exception as e:
        logging.error(f"Failed to create backup: {e}")
        return False

# Update the sector weight calculation in app.py
def update_app_py():
    """Update app.py to use authentic market cap weights"""
    try:
        # First read the entire file
        with open("app.py", "r") as f:
            content = f.read()
        
        # Define the pattern to match the calculate_t2d_pulse_from_sectors function
        pattern = r"def calculate_t2d_pulse_from_sectors\(sector_scores, sector_weights=None\):.*?return round\(pulse_score, 1\)"
        
        # Find the function in the content
        match = re.search(pattern, content, re.DOTALL)
        if not match:
            logging.error("Failed to find calculate_t2d_pulse_from_sectors function in app.py")
            return False
        
        # Original function content
        original_function = match.group(0)
        
        # Define the replacement function
        replacement = """def calculate_t2d_pulse_from_sectors(sector_scores, sector_weights=None):
    \"\"\"Calculate T2D Pulse score as a weighted average of sector scores
    
    Args:
        sector_scores (dict or list): Dictionary with sector scores {sector: score} or list of sector dictionaries
        sector_weights (dict, optional): Dictionary with custom weights for each sector
        
    Returns:
        float: The weighted average T2D Pulse score (0-100 scale)
    \"\"\"
    # Convert sector scores to a dictionary if it's a list
    if isinstance(sector_scores, list):
        sector_dict = {}
        for item in sector_scores:
            sector_dict.update(item)
        sector_scores = sector_dict
    
    # Try to get authentic market cap weights
    try:
        from authentic_marketcap_reader import get_sector_weightings
        authentic_weights = get_sector_weightings()
        
        # If we have authentic weights and no custom weights specified, use the authentic weights
        if authentic_weights and not sector_weights:
            logging.info("Using authentic market cap weights for T2D Pulse calculation")
            sector_weights = authentic_weights
    except (ImportError, Exception) as e:
        logging.warning(f"Could not load authentic market cap weights: {e}")
    
    # If no weights are provided, use equal weighting
    if not sector_weights:
        sector_weights = {sector: 100 / len(sector_scores) for sector in sector_scores}
        logging.info("Using equal weights for T2D Pulse calculation (no weights provided)")
    
    # Normalize the weights to sum to 100
    total_weight = sum(sector_weights.values())
    normalized_weights = {sector: weight * 100 / total_weight for sector, weight in sector_weights.items()}
    
    # Calculate weighted average
    weighted_sum = 0
    total_applied_weight = 0
    
    for sector, score in sector_scores.items():
        if sector in normalized_weights:
            weight = normalized_weights[sector]
            weighted_sum += score * weight
            total_applied_weight += weight
    
    # Handle case where no weights were applied
    if total_applied_weight == 0:
        logging.warning("No weights applied in T2D Pulse calculation, returning simple average")
        return sum(sector_scores.values()) / len(sector_scores)
    
    # Return normalized weighted score
    pulse_score = weighted_sum / total_applied_weight
    return round(pulse_score, 1)"""
        
        # Replace the function in the content
        updated_content = content.replace(original_function, replacement)
        
        # Write the updated content
        with open("app.py", "w") as f:
            f.write(updated_content)
        
        logging.info("Updated calculate_t2d_pulse_from_sectors function in app.py")
        return True
    except Exception as e:
        logging.error(f"Failed to update app.py: {e}")
        return False

# Main function
def main():
    """Main function"""
    logging.info("Integrating authentic market cap weights into T2D Pulse calculation...")
    
    # First create a backup
    if not backup_app_py():
        logging.error("Failed to create backup, aborting")
        return
    
    # Update app.py
    if update_app_py():
        logging.info("Successfully integrated authentic market cap weights into T2D Pulse calculation")
    else:
        logging.error("Failed to integrate authentic market cap weights")

if __name__ == "__main__":
    main()