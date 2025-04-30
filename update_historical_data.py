#!/usr/bin/env python3
# update_historical_data.py
# -----------------------------------------------------------
# Update authentic historical data for sector sentiment

import os
import json
from datetime import datetime, timedelta
import pandas as pd

# Ensure data directory exists
DATA_DIR = "data"
os.makedirs(DATA_DIR, exist_ok=True)

def update_real_historical_data():
    """
    Update the real historical sector sentiment data from existing CSV files
    This is a legacy function that provides a fallback for the original approach
    """
    try:
        from authentic_sector_history import update_authentic_history
        return update_authentic_history()
    except Exception as e:
        print(f"Error updating real historical data: {e}")
        return False

def update_authentic_historical_data():
    """
    Update authentic historical data for sector sentiment using our authentic_sector_history approach
    """
    try:
        from authentic_sector_history import update_authentic_history
        return update_authentic_history()
    except Exception as e:
        print(f"Error updating authentic historical data: {e}")
        return False

# Run the update if this script is executed directly
if __name__ == "__main__":
    print("Updating real historical data...")
    update_real_historical_data()
    
    print("Updating authentic historical data...")
    update_authentic_historical_data()