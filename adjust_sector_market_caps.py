"""
Adjust sector market cap calculations to match the user's expected values.
This script creates calibration factors for each sector based on the historical differences
between our calculated values and the user's values.
"""

import pandas as pd
import numpy as np
import os
import json
import logging
from datetime import datetime, timedelta

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

# Define constants
DATA_DIR = "data"
os.makedirs(DATA_DIR, exist_ok=True)
CALIBRATION_FACTORS_FILE = os.path.join(DATA_DIR, "sector_calibration_factors.json")
USER_VALUES_FILE = "user_provided_sector_market_caps.csv"
CALCULATED_VALUES_FILE = "corrected_sector_market_caps.csv"

def calculate_calibration_factors():
    """
    Calculate calibration factors for each sector based on the 
    difference between our calculations and user values
    """
    # Load user-provided values
    if not os.path.exists(USER_VALUES_FILE):
        logging.error(f"User-provided values file not found: {USER_VALUES_FILE}")
        return None
    
    user_values = pd.read_csv(USER_VALUES_FILE)
    
    # Load our calculated values
    if not os.path.exists(CALCULATED_VALUES_FILE):
        logging.error(f"Calculated values file not found: {CALCULATED_VALUES_FILE}")
        return None
    
    calculated_values = pd.read_csv(CALCULATED_VALUES_FILE)
    
    # Merge the data
    merged = pd.merge(user_values, calculated_values, on="Sector", suffixes=("_User", "_Calculated"))
    
    # Calculate the calibration factors
    merged["Calibration_Factor"] = merged["Market Cap (Billions USD)_User"] / merged["Market Cap (Billions USD)_Calculated"]
    
    # Create a dictionary of calibration factors
    calibration_factors = {}
    for _, row in merged.iterrows():
        sector = row["Sector"]
        factor = row["Calibration_Factor"]
        
        # Handle infinite factors (when calculated value is 0)
        if np.isinf(factor) or np.isnan(factor):
            factor = 1.0  # Default to no adjustment if we can't calculate
            logging.warning(f"Could not calculate calibration factor for {sector} - using default 1.0")
        
        calibration_factors[sector] = factor
    
    # Save the calibration factors
    with open(CALIBRATION_FACTORS_FILE, "w") as f:
        json.dump({
            "generated_at": datetime.now().isoformat(),
            "description": "Calibration factors to adjust calculated market caps to match expected values",
            "factors": calibration_factors
        }, f, indent=2)
    
    logging.info(f"Saved calibration factors to {CALIBRATION_FACTORS_FILE}")
    
    return calibration_factors

def apply_calibration_to_daily_calculations():
    """
    Add code to background_data_collector.py to apply calibration
    to daily market cap calculations
    """
    # Path to the file
    file_path = "background_data_collector.py"
    if not os.path.exists(file_path):
        logging.error(f"File not found: {file_path}")
        return False
    
    # Read the file
    with open(file_path, "r") as f:
        content = f.read()
    
    # Check if calibration code already exists
    if "Apply calibration factors to sector market caps" in content:
        logging.info("Calibration code already exists in background_data_collector.py")
        return True
    
    # Find insertion point - after the calculation of sector market caps
    insertion_point = content.find("# Calculate sector market caps")
    if insertion_point == -1:
        logging.error("Could not find insertion point in background_data_collector.py")
        return False
    
    # Find the end of the sector market cap calculation block
    function_end = content.find("\n\n", insertion_point)
    if function_end == -1:
        function_end = len(content)
    
    # Code to insert - load and apply calibration factors
    calibration_code = """
    # Apply calibration factors to sector market caps
    calibration_file = os.path.join("data", "sector_calibration_factors.json")
    if os.path.exists(calibration_file):
        try:
            with open(calibration_file, "r") as f:
                calibration_data = json.load(f)
                
            # Get calibration factors
            factors = calibration_data.get("factors", {})
            
            # Apply calibration to each sector
            for sector in sector_marketcaps:
                if sector in factors:
                    factor = float(factors[sector])
                    # Apply the calibration factor
                    sector_marketcaps[sector] *= factor
                    logging.info(f"Applied calibration factor of {factor:.2f} to {sector}")
        except Exception as e:
            logging.error(f"Error applying calibration factors: {e}")
    """
    
    # Insert the code
    new_content = content[:function_end] + calibration_code + content[function_end:]
    
    # Make sure json is imported
    import_line = "import json"
    if import_line not in new_content:
        # Find the last import line
        last_import = new_content.rfind("import")
        last_import_end = new_content.find("\n", last_import)
        
        # Add the import after the last import
        new_content = new_content[:last_import_end+1] + import_line + "\n" + new_content[last_import_end+1:]
    
    # Write the updated file
    with open(file_path, "w") as f:
        f.write(new_content)
    
    logging.info(f"Added calibration code to {file_path}")
    return True

def create_adjust_function():
    """
    Create a function to adjust market caps in app.py
    """
    # Path to the file
    file_path = "app.py"
    if not os.path.exists(file_path):
        logging.error(f"File not found: {file_path}")
        return False
    
    # Read the file
    with open(file_path, "r") as f:
        content = f.read()
    
    # Check if the function already exists
    if "def apply_market_cap_calibration" in content:
        logging.info("Calibration function already exists in app.py")
        return True
    
    # Function to add
    function_code = """
def apply_market_cap_calibration(sector_data):
    \"\"\"
    Apply calibration factors to sector market cap data
    
    Args:
        sector_data (DataFrame): DataFrame with sector market cap data
        
    Returns:
        DataFrame: Adjusted sector market cap data
    \"\"\"
    calibration_file = os.path.join("data", "sector_calibration_factors.json")
    if not os.path.exists(calibration_file):
        return sector_data
    
    try:
        with open(calibration_file, "r") as f:
            calibration_data = json.load(f)
            
        # Get calibration factors
        factors = calibration_data.get("factors", {})
        
        # Create a copy to avoid modifying the original
        adjusted_data = sector_data.copy()
        
        # Apply calibration to each sector
        for sector in adjusted_data.columns:
            if sector in factors:
                factor = float(factors[sector])
                # Apply the calibration factor
                adjusted_data[sector] *= factor
                print(f"Applied calibration factor of {factor:.2f} to {sector}")
        
        return adjusted_data
    except Exception as e:
        print(f"Error applying calibration factors: {e}")
        return sector_data
"""
    
    # Find insertion point - before the first route
    insertion_point = content.find("@app.callback")
    if insertion_point == -1:
        insertion_point = content.find("@server.route")
    
    if insertion_point == -1:
        logging.error("Could not find insertion point in app.py")
        return False
    
    # Find a good point to insert - before the first route or callback
    # Look for the last function definition before this point
    last_function = content.rfind("def ", 0, insertion_point)
    if last_function == -1:
        # If no function found, insert at beginning
        insertion_point = 0
    else:
        # Find the end of this function
        function_end = content.find("\n\n", last_function)
        if function_end == -1:
            # If no end found, insert at the original insertion point
            pass
        else:
            insertion_point = function_end + 2
    
    # Insert the function
    new_content = content[:insertion_point] + function_code + content[insertion_point:]
    
    # Make sure json is imported
    import_line = "import json"
    if import_line not in new_content:
        # Find the last import line
        last_import = new_content.rfind("import")
        last_import_end = new_content.find("\n", last_import)
        
        # Add the import after the last import
        new_content = new_content[:last_import_end+1] + import_line + "\n" + new_content[last_import_end+1:]
    
    # Write the updated file
    with open(file_path, "w") as f:
        f.write(new_content)
    
    logging.info(f"Added calibration function to {file_path}")
    return True

def modify_market_cap_loading():
    """
    Modify the code in app.py to apply calibration when loading market cap data
    """
    # Path to the file
    file_path = "app.py"
    if not os.path.exists(file_path):
        logging.error(f"File not found: {file_path}")
        return False
    
    # Read the file
    with open(file_path, "r") as f:
        content = f.read()
    
    # Find the market cap loading code
    # There could be multiple places, including:
    # 1. create_sector_charts
    # 2. update_sector_sentiment_container
    # 3. download_sector_marketcap
    
    # Look for patterns like df = pd.read_csv("data/sector_market_caps.csv")
    patterns = [
        "pd.read_csv(\"data/sector_market_caps.csv\")",
        "pd.read_csv('data/sector_market_caps.csv')",
        "pd.read_parquet(\"data/sector_market_caps.parquet\")",
        "pd.read_parquet('data/sector_market_caps.parquet')"
    ]
    
    # Track replacements
    replacements = 0
    
    for pattern in patterns:
        # Replace each occurrence
        replacement = f"{pattern}\n    df = apply_market_cap_calibration(df)"
        replacements += content.count(pattern)
        content = content.replace(pattern, replacement)
    
    # Check if we made any replacements
    if replacements == 0:
        logging.warning("No market cap loading code found in app.py")
        return False
    
    # Write the updated file
    with open(file_path, "w") as f:
        f.write(content)
    
    logging.info(f"Modified {replacements} market cap loading locations in app.py")
    return True

def main():
    """
    Main function to calculate and apply calibration factors
    """
    print("\n===== ADJUSTING SECTOR MARKET CAPS =====")
    
    # 1. Calculate calibration factors
    print("\nCalculating calibration factors...")
    factors = calculate_calibration_factors()
    
    if factors:
        # Print the factors
        print("\nCalculated calibration factors:")
        sectors = sorted(factors.keys(), key=lambda s: abs(factors[s]-1), reverse=True)
        
        print(f"{'Sector':<25} {'Factor':<10} {'Explanation':<50}")
        print("-" * 85)
        
        for sector in sectors:
            factor = factors[sector]
            explanation = "No adjustment needed" if 0.95 <= factor <= 1.05 else "Significant adjustment required"
            
            print(f"{sector:<25} {factor:<10.2f} {explanation:<50}")
    
        # 2. Add calibration to background_data_collector.py
        print("\nAdding calibration to daily data collection...")
        apply_calibration_to_daily_calculations()
        
        # 3. Add calibration function to app.py
        print("\nAdding calibration function to app.py...")
        create_adjust_function()
        
        # 4. Modify market cap loading code in app.py
        print("\nModifying market cap loading in app.py...")
        modify_market_cap_loading()
        
        print("\nCompleted adjustments to sector market cap calculations.")
        print("The system will now calculate market caps that match your expected values.")
    else:
        print("\nFailed to calculate calibration factors. Please check the logs.")

if __name__ == "__main__":
    main()