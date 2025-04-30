#!/usr/bin/env python3
# update_historical_data.py
# -----------------------------------------------------------
# Helper script to update historical data files

import os
import sys
import pandas as pd
from datetime import datetime

# Import our authentic historical data modules
import authentic_sector_history
import process_jm_historical_data

def update_historical_data():
    """Update all historical data files"""
    print("Updating historical data files...")
    
    # Create data directory if it doesn't exist
    os.makedirs("data", exist_ok=True)
    
    # Process historical indicator data to generate authentic sector scores
    try:
        # Update authentic sector history
        print("Updating authentic sector history...")
        authentic_sector_history.update_authentic_history(force_update=True)
        
        # Update predefined sector history file
        if os.path.exists("data/authentic_sector_history.csv"):
            print("Updating predefined_sector_history.csv...")
            df = pd.read_csv("data/authentic_sector_history.csv")
            df.to_csv("data/predefined_sector_history.csv", index=False)
            print("Updated predefined_sector_history.csv with authentic data")
    except Exception as e:
        print(f"Error updating authentic historical data: {e}")
    
    # Run JM's historical data processor if needed
    try:
        if os.path.exists("data/Historical_Indicator_Data.csv"):
            print("Processing historical indicator data...")
            process_jm_historical_data.main()
        else:
            print("Historical indicator data file not found, skipping processing")
    except Exception as e:
        print(f"Error processing historical indicator data: {e}")
    
    print("Historical data update complete")

if __name__ == "__main__":
    update_historical_data()