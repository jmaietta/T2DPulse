#!/usr/bin/env python3
# authentic_sector_history.py
# -----------------------------------------------------------
# Generate authentic historical sector sentiment data by replaying the
# scoring routine for past business days

import os
import json
import pandas as pd
from datetime import datetime, timedelta
import sentiment_engine

# Use the SECTORS list directly from sentiment_engine
SECTORS = sentiment_engine.SECTORS

# Number of business days to include
DAYS = 20

# Paths for storing data
DATA_DIR = "data"
HISTORY_FILE = os.path.join(DATA_DIR, "authentic_sector_history.json")
CSV_EXPORT_FILE = os.path.join(DATA_DIR, f"authentic_sector_history_{datetime.now().strftime('%Y-%m-%d')}.csv")

# Ensure data directory exists
os.makedirs(DATA_DIR, exist_ok=True)

def get_business_days(days_back=DAYS):
    """Get a list of business days (Mon-Fri) going back from today"""
    today = datetime.today().replace(hour=0, minute=0, second=0, microsecond=0)
    dates = []
    curr = today
    
    # Collect the last `days_back` weekdays
    while len(dates) < days_back:
        if curr.weekday() < 5:  # Mon-Fri only
            dates.append(curr)
        curr -= timedelta(days=1)
    
    return sorted(dates)  # Return dates in chronological order

def compute_authentic_sector_history():
    """Compute authentic sector sentiment history for all sectors"""
    business_days = get_business_days(DAYS)
    sector_history = {}
    
    print(f"Computing authentic historical data for {len(business_days)} business days...")
    
    # Process each sector
    for sector in SECTORS:
        sector_name = sector
        print(f"Computing history for {sector_name}...")
        scores = []
        
        # Calculate scores for each business day
        for day in business_days:
            try:
                # Get raw score from sentiment engine (already in [-1,1] range)
                raw_score = sentiment_engine.score_sector_on_date(sector_name, day)
                
                # Convert raw score from [-1,1] to [0,100] scale for display
                normalized_score = ((raw_score + 1.0) / 2.0) * 100
                
                # Store the sector data with both scores
                scores.append({
                    "date": day.isoformat(),
                    "raw_score": raw_score,
                    "normalized_score": normalized_score  # 0-100 scale for display
                })
                
            except Exception as e:
                print(f"Error calculating score for {sector_name} on {day.strftime('%Y-%m-%d')}: {e}")
        
        # Store history for this sector
        if scores:
            sector_history[sector_name] = scores
    
    return sector_history

def save_history(sector_history):
    """Save the computed history to a JSON file"""
    try:
        with open(HISTORY_FILE, 'w') as f:
            json.dump(sector_history, f)
        print(f"Saved authentic sector history to {HISTORY_FILE}")
        return True
    except Exception as e:
        print(f"Error saving sector history: {e}")
        return False

def export_to_csv(sector_history):
    """Export the history to a CSV file for verification"""
    try:
        # Extract all dates from all sectors
        all_dates = set()
        for sector, scores in sector_history.items():
            for item in scores:
                all_dates.add(item["date"])
        
        # Create rows for each date
        rows = []
        for date_str in sorted(all_dates):
            row = {"date": date_str}
            
            # Add normalized scores for each sector
            for sector, scores in sector_history.items():
                for item in scores:
                    if item["date"] == date_str:
                        row[sector] = item["normalized_score"]
                        break
            
            rows.append(row)
        
        # Create DataFrame and save to CSV
        if rows:
            df = pd.DataFrame(rows)
            df.to_csv(CSV_EXPORT_FILE, index=False)
            print(f"Exported authentic sector history to {CSV_EXPORT_FILE}")
            return True
        else:
            print("No data to export")
            return False
    
    except Exception as e:
        print(f"Error exporting to CSV: {e}")
        return False

def load_history():
    """Load the authentic sector history from JSON file"""
    try:
        if os.path.exists(HISTORY_FILE):
            with open(HISTORY_FILE, 'r') as f:
                return json.load(f)
        return {}
    except Exception as e:
        print(f"Error loading sector history: {e}")
        return {}

def get_sector_history_dataframe(sector_name, days=DAYS):
    """
    Get a pandas DataFrame with authentic historical sentiment scores for a sector
    
    Args:
        sector_name (str): Name of the sector
        days (int): Number of days to include
        
    Returns:
        DataFrame: DataFrame with 'date' and 'score' columns
    """
    # Load the authentic history
    sector_history = load_history()
    
    # Check if we have data for this sector
    if sector_name not in sector_history:
        print(f"No authentic history data for {sector_name}")
        return pd.DataFrame(columns=["date", "score"])
    
    # Create rows from the history data
    rows = []
    for item in sector_history[sector_name]:
        rows.append({
            "date": datetime.fromisoformat(item["date"]),
            "score": item["normalized_score"]
        })
    
    # Create DataFrame
    if not rows:
        return pd.DataFrame(columns=["date", "score"])
    
    df = pd.DataFrame(rows)
    
    # Sort by date and keep only requested number of days
    df = df.sort_values("date")
    if len(df) > days:
        df = df.tail(days)
    
    return df

def update_authentic_history():
    """Update the authentic sector history"""
    try:
        # Compute new history
        sector_history = compute_authentic_sector_history()
        
        if not sector_history:
            print("No authentic history was computed")
            return False
        
        # Save and export
        save_success = save_history(sector_history)
        export_success = export_to_csv(sector_history)
        
        return save_success and export_success
    
    except Exception as e:
        print(f"Error updating authentic history: {e}")
        return False

# Run the update if this script is executed directly
if __name__ == "__main__":
    update_authentic_history()