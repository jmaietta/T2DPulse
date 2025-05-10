"""
This script integrates the corrected market cap data into the dashboard display,
including updating all sector weightings based on accurate fully diluted shares.
"""
import os
import logging
import json
import pandas as pd
import numpy as np
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

# Define directories
DATA_DIR = "data"

def load_sector_market_caps():
    """Load sector market cap data from parquet file"""
    market_cap_file = os.path.join(DATA_DIR, "sector_market_caps.parquet")
    
    if os.path.exists(market_cap_file):
        try:
            sector_caps = pd.read_parquet(market_cap_file)
            logging.info(f"Loaded sector market caps from {market_cap_file}")
            return sector_caps
        except Exception as e:
            logging.error(f"Error loading sector market caps: {e}")
            return None
    else:
        logging.error(f"Sector market cap file not found: {market_cap_file}")
        return None

def create_sector_weight_json():
    """Create a JSON file with sector weights based on accurate market caps"""
    # Load sector market caps
    sector_caps = load_sector_market_caps()
    if sector_caps is None:
        return False
    
    # Get the latest date
    latest_date = sector_caps.index.max()
    latest_data = sector_caps.loc[latest_date]
    
    # Get all sector columns (excluding weight columns and Total)
    sector_columns = [col for col in sector_caps.columns 
                     if not col.endswith('_weight_pct') and col != 'Total']
    
    # Calculate total market cap
    total_market_cap = latest_data[[col for col in sector_columns]].sum()
    
    # Calculate weights
    weights = {}
    for sector in sector_columns:
        market_cap = latest_data[sector]
        weight = (market_cap / total_market_cap) * 100
        weights[sector] = round(weight, 2)
    
    # Create formatted weights dictionary
    weights_dict = {"weights": {}}
    
    for sector, weight in weights.items():
        # Convert sector name to expected format
        formatted_sector = sector.replace(" ", "_")
        weights_dict["weights"][formatted_sector] = weight
    
    # Save to JSON file
    weights_file = os.path.join(DATA_DIR, "sector_weights_latest.json")
    
    try:
        with open(weights_file, 'w') as f:
            json.dump(weights_dict, f, indent=2)
        
        logging.info(f"Saved sector weights to {weights_file}")
        
        # Print weights for verification
        print("\nUpdated Sector Weights (Market Cap Based):")
        for sector, weight in sorted(weights.items(), key=lambda x: x[1], reverse=True):
            market_cap = latest_data[sector] / 1_000_000_000_000  # to trillions
            print(f"{sector}: {weight:.2f}% (${market_cap:.2f}T)")
        
        # Print total
        print(f"Total: 100.00% (${total_market_cap/1_000_000_000_000:.2f}T)")
        
        return True
    
    except Exception as e:
        logging.error(f"Error saving sector weights: {e}")
        return False

def fetch_actual_sentiment_scores():
    """Fetch the actual sector sentiment scores from the latest data"""
    latest_csv = os.path.join(DATA_DIR, "authentic_sector_history.csv")
    
    if os.path.exists(latest_csv):
        try:
            df = pd.read_csv(latest_csv)
            # Get the latest date row
            latest_row = df.iloc[-1]
            
            # Print sectors with their sentiment scores
            print("\nLatest Sector Sentiment Scores:")
            
            for column in df.columns:
                if column != 'date' and not pd.isna(latest_row[column]):
                    print(f"{column}: {latest_row[column]:.1f}")
            
            return True
        except Exception as e:
            logging.error(f"Error loading sector scores: {e}")
            return False
    else:
        logging.error(f"Authentic sector history file not found: {latest_csv}")
        return False

def print_adtech_market_cap_history():
    """Print AdTech sector market cap history for the last 7 days"""
    # Load sector market caps
    sector_caps = load_sector_market_caps()
    if sector_caps is None:
        return False
    
    # Filter to AdTech only and convert to billions
    if 'AdTech' in sector_caps.columns:
        adtech_caps = sector_caps[['AdTech']] / 1_000_000_000_000  # to trillions
        
        # Get the last 7 days
        last_days = adtech_caps.iloc[-7:]
        
        print("\nAdTech Market Cap - Last 7 Days:")
        for date, row in last_days.iterrows():
            print(f"{date.strftime('%Y-%m-%d')}: ${row['AdTech']:.3f}T")
        
        # Calculate change
        first_value = last_days.iloc[0]['AdTech']
        last_value = last_days.iloc[-1]['AdTech']
        change = ((last_value / first_value) - 1) * 100
        
        print(f"\n7-Day Change: {change:.1f}%")
        
        return True
    else:
        logging.error(f"AdTech column not found in sector market caps")
        return False

def main():
    """Main function to update dashboard with corrected market cap data"""
    # Create sector weight JSON based on accurate market caps
    if not create_sector_weight_json():
        logging.error("Failed to create sector weight JSON")
    
    # Fetch actual sentiment scores
    if not fetch_actual_sentiment_scores():
        logging.error("Failed to fetch actual sentiment scores")
    
    # Print AdTech market cap history
    if not print_adtech_market_cap_history():
        logging.error("Failed to print AdTech market cap history")
    
    print("\nTo see these changes reflected in the dashboard, restart the server workflow.")

if __name__ == "__main__":
    main()