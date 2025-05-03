import csv
import os
import pytz
from datetime import datetime

def append_sector_values(results, filepath="data/sector_values.csv"):
    """
    Append sector values to CSV file
    
    Args:
        results (dict): Dictionary with sector values {sector: value}
        filepath (str): Path to the CSV file
    """
    # Create data directory if it doesn't exist
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    
    # Use Eastern Time for consistency with the rest of the dashboard
    eastern = pytz.timezone('US/Eastern')
    today = datetime.now(eastern).strftime("%Y-%m-%d")
    
    # Check if file exists to determine if header is needed
    header_needed = not os.path.exists(filepath)
    
    # Create a list of all sectors to ensure consistent column order
    sectors = sorted(list(results.keys()))
    
    try:
        with open(filepath, "a", newline="") as f:
            writer = csv.writer(f)
            
            # Write header if needed
            if header_needed:
                writer.writerow(["Date"] + sectors)
                
            # Write values
            row = [today] + [results.get(sector, 0) for sector in sectors]
            writer.writerow(row)
            
        print(f"Sector values for {today} saved to {filepath}")
        return True
    except Exception as e:
        print(f"Error saving sector values: {str(e)}")
        return False