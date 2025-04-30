#!/usr/bin/env python3
# update_historical_data.py
# -----------------------------------------------------------
# Updates real historical sector sentiment data during dashboard startup

import os
from datetime import datetime, timedelta

def update_real_historical_data():
    """
    Update real historical sector sentiment data from our APIs
    This is called during dashboard startup to ensure we always have fresh data
    """
    print("Updating real historical sector sentiment data...")
    
    # Check if we already ran today to avoid duplicate calculations
    today_str = datetime.now().strftime('%Y-%m-%d')
    real_data_file = f"data/real_sector_sentiment_history_{today_str}.csv"
    
    # Skip if we already have fresh data for today
    if os.path.exists(real_data_file):
        # Check file age to make sure it's recent enough (within last 6 hours)
        file_age = datetime.now() - datetime.fromtimestamp(os.path.getmtime(real_data_file))
        if file_age.total_seconds() < 6 * 3600:  # 6 hours in seconds
            print(f"Real historical data already up to date (last updated {file_age.total_seconds() / 3600:.1f} hours ago)")
            return True
    
    # If we need to update, import and run our historical data calculator
    try:
        # Import after runtime to avoid circular imports
        from real_historical_sector_scores import update_sector_history_with_real_data
        
        # Calculate and save real historical data
        result = update_sector_history_with_real_data()
        
        # Verify success
        if result and os.path.exists(real_data_file):
            print(f"Successfully updated real historical sector sentiment data: {real_data_file}")
            return True
        else:
            print("Failed to update real historical sector sentiment data")
            return False
    except Exception as e:
        print(f"Error updating real historical sector sentiment data: {e}")
        return False

if __name__ == "__main__":
    update_real_historical_data()