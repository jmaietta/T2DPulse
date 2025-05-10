"""
Update the authentic sentiment history file with accurate AdTech sentiment scores
based on the corrected market cap data.

This script updates the authentic_sector_history.json and CSV files with a more
accurate set of AdTech sentiment scores that properly reflect the corrected market
cap data using fully diluted share counts.
"""
import os
import json
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

# Define directories and files
DATA_DIR = "data"
AUTHENTIC_HISTORY_FILE = os.path.join(DATA_DIR, "authentic_sector_history.json")

def load_authentic_sector_history():
    """Load the authentic sector history JSON file"""
    if os.path.exists(AUTHENTIC_HISTORY_FILE):
        try:
            with open(AUTHENTIC_HISTORY_FILE, 'r') as f:
                return json.load(f)
        except Exception as e:
            print(f"Error loading authentic sector history: {e}")
            return {}
    else:
        print(f"Authentic sector history file not found: {AUTHENTIC_HISTORY_FILE}")
        return {}

def load_combined_data():
    """Load the combined AdTech market cap and sentiment data"""
    combined_file = os.path.join(DATA_DIR, "adtech_marketcap_sentiment.csv")
    
    if os.path.exists(combined_file):
        try:
            # The index column is the date in our file
            df = pd.read_csv(combined_file)
            
            # Create a proper date index from the unnamed first column
            df = df.reset_index()
            df.columns = ['index', 'market_cap', 'sentiment']
            df['date'] = df['index'].apply(lambda x: pd.Timestamp(f"2025-{x.split('-')[1]}-{x.split('-')[2]}"))
            
            return df
        except Exception as e:
            print(f"Error loading combined data: {e}")
            return None
    else:
        print(f"Combined data file not found: {combined_file}")
        return None

def update_authentic_history():
    """Update the authentic sector history with accurate AdTech sentiment scores"""
    # Load authentic sector history
    authentic_history = load_authentic_sector_history()
    
    if not authentic_history:
        print("Could not load authentic sector history")
        return False
    
    # Load combined data
    combined_data = load_combined_data()
    
    if combined_data is None:
        print("Could not load combined data")
        return False
    
    # Dictionary of dates to update
    updates = {}
    
    # For each date in combined data, extract sentiment score
    for _, row in combined_data.iterrows():
        date = row['date']
        date_str = date.strftime("%Y-%m-%d")
        
        if not pd.isna(row['sentiment']):
            sentiment = row['sentiment']
            
            # Skip synthetic values for dates before 2025-05-01
            is_actual = date >= pd.Timestamp('2025-05-01')
            
            if is_actual:
                updates[date_str] = sentiment
    
    # Update the authentic history
    updated_dates = []
    
    for date_str, sentiment in updates.items():
        if date_str not in authentic_history:
            authentic_history[date_str] = {}
            
        # Update AdTech sentiment
        authentic_history[date_str]['AdTech'] = sentiment
        updated_dates.append(date_str)
    
    # Save updated authentic history
    if updated_dates:
        try:
            # Sort the dictionary by date
            authentic_history = dict(sorted(authentic_history.items()))
            
            # Save JSON
            with open(AUTHENTIC_HISTORY_FILE, 'w') as f:
                json.dump(authentic_history, f, indent=2)
            
            print(f"Updated authentic sector history with {len(updated_dates)} accurate AdTech sentiment scores")
            
            # Export as CSV
            export_authentic_history_to_csv(authentic_history)
            
            return True
        except Exception as e:
            print(f"Error saving updated authentic history: {e}")
            return False
    else:
        print("No updates needed for authentic sector history")
        return True

def export_authentic_history_to_csv(authentic_history):
    """Export authentic sector history to CSV format"""
    try:
        # Convert to DataFrame
        dates = sorted(authentic_history.keys())
        sectors = set()
        
        for date_data in authentic_history.values():
            sectors.update(date_data.keys())
        
        sectors = sorted(list(sectors))
        
        # Create DataFrame
        df = pd.DataFrame(index=dates, columns=sectors)
        
        for date in dates:
            for sector in sectors:
                if sector in authentic_history[date]:
                    df.loc[date, sector] = authentic_history[date][sector]
        
        # Save CSV with today's date
        today = datetime.now().strftime("%Y-%m-%d")
        csv_file = os.path.join(DATA_DIR, f"authentic_sector_history_{today}.csv")
        latest_csv = os.path.join(DATA_DIR, "authentic_sector_history.csv")
        
        # Add date column and reset index
        df = df.reset_index().rename(columns={'index': 'date'})
        
        # Save to both files
        df.to_csv(csv_file, index=False)
        df.to_csv(latest_csv, index=False)
        
        print(f"Exported authentic sector history to {csv_file} and {latest_csv}")
        
        # Print the updated AdTech sentiment scores
        adtech_df = df[['date', 'AdTech']].dropna()
        print("\nUpdated AdTech sentiment scores:")
        print(adtech_df.to_string(index=False))
        
        return True
    except Exception as e:
        print(f"Error exporting authentic history to CSV: {e}")
        return False

def main():
    """Main function"""
    update_authentic_history()

if __name__ == "__main__":
    main()