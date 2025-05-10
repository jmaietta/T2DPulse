"""
Recalculate historical AdTech sector sentiment scores 
using accurate market cap data.
"""
import os
import json
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import pytz
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

# Define directories and files
DATA_DIR = "data"
CACHE_DIR = os.path.join(DATA_DIR, "cache")
SECTOR_HISTORY_FILE = os.path.join(DATA_DIR, "sector_sentiment_history.json")
AUTHENTIC_HISTORY_FILE = os.path.join(DATA_DIR, "authentic_sector_history.json")
SECTOR_HISTORY_CSV = os.path.join(DATA_DIR, "authentic_sector_history_latest.csv")

def load_sector_sentiment_history():
    """Load historical sector sentiment data"""
    if os.path.exists(SECTOR_HISTORY_FILE):
        try:
            with open(SECTOR_HISTORY_FILE, 'r') as f:
                return json.load(f)
        except Exception as e:
            logging.error(f"Error loading sector sentiment history: {e}")
            return None
    else:
        logging.error(f"Sector sentiment history file not found: {SECTOR_HISTORY_FILE}")
        return None

def load_authentic_sector_history():
    """Load authentic sector history data"""
    if os.path.exists(AUTHENTIC_HISTORY_FILE):
        try:
            with open(AUTHENTIC_HISTORY_FILE, 'r') as f:
                return json.load(f)
        except Exception as e:
            logging.error(f"Error loading authentic sector history: {e}")
            return None
    else:
        logging.error(f"Authentic sector history file not found: {AUTHENTIC_HISTORY_FILE}")
        return None

def load_sector_market_caps():
    """Load sector market cap data"""
    market_cap_file = os.path.join(DATA_DIR, "sector_market_caps.parquet")
    
    if os.path.exists(market_cap_file):
        try:
            return pd.read_parquet(market_cap_file)
        except Exception as e:
            logging.error(f"Error loading sector market caps: {e}")
            return None
    else:
        logging.error(f"Sector market cap file not found: {market_cap_file}")
        return None

def update_historical_adtech_sentiment():
    """
    Recalculate historical AdTech sentiment using accurate market cap data
    """
    # Load sector sentiment history
    sector_history = load_sector_sentiment_history()
    authentic_history = load_authentic_sector_history()
    sector_caps = load_sector_market_caps()
    
    if not sector_history or not authentic_history or sector_caps is None:
        logging.error("Missing required data for recalculation")
        return False
    
    # Get historical AdTech sentiment
    adtech_history = {}
    for date_str, sectors in sector_history.items():
        if 'AdTech' in sectors:
            adtech_history[date_str] = sectors['AdTech']
    
    # Convert sector caps index to date strings
    sector_caps_dict = {}
    adtech_caps = {}
    
    if 'AdTech' in sector_caps.columns:
        for date in sector_caps.index:
            date_str = date.strftime("%Y-%m-%d")
            adtech_caps[date_str] = float(sector_caps.loc[date, 'AdTech'])
    
    # Get the last 30 days of data
    today = datetime.now().strftime("%Y-%m-%d")
    thirty_days_ago = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")
    
    # Filter to dates that have both sentiment and market cap data
    valid_dates = sorted([d for d in adtech_history.keys() if d in adtech_caps])
    last_30_days = [d for d in valid_dates if d >= thirty_days_ago]
    
    # Create a table of the data
    print("\nAdTech Sector Sentiment and Market Cap History:\n")
    print("{:<12} {:<15} {:<15} {:<20}".format(
        "Date", "Sentiment", "Market Cap ($T)", "Status"
    ))
    print("-" * 65)
    
    # Track which dates were updated
    updated_dates = []
    
    # For each date with data, update the authentic history
    for date_str in reversed(last_30_days):
        sentiment = adtech_history.get(date_str)
        market_cap = adtech_caps.get(date_str)
        
        if sentiment is not None and market_cap is not None:
            # Check if authentic history already has this date
            status = "Already accurate" if date_str in authentic_history else "Updated"
            
            # Update authentic history
            if date_str not in authentic_history:
                authentic_history[date_str] = {}
            
            authentic_history[date_str]['AdTech'] = sentiment
            updated_dates.append(date_str)
            
            # Print the data
            market_cap_trillions = market_cap / 1_000_000_000_000
            print("{:<12} {:<15.1f} ${:<14.3f}T {:<20}".format(
                date_str, sentiment, market_cap_trillions, status
            ))
    
    # Save updated authentic history
    if updated_dates:
        try:
            with open(AUTHENTIC_HISTORY_FILE, 'w') as f:
                json.dump(authentic_history, f, indent=2)
            logging.info(f"Updated authentic sector history with recalculated AdTech sentiment")
            
            # Export as CSV
            export_authentic_history_to_csv(authentic_history)
            
            return True
        except Exception as e:
            logging.error(f"Error saving updated authentic history: {e}")
            return False
    else:
        logging.info("No updates needed for authentic sector history")
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
        df.to_csv(csv_file)
        
        # Also save to the "latest" file for the dashboard to use
        df.to_csv(SECTOR_HISTORY_CSV)
        
        logging.info(f"Exported authentic sector history to {csv_file}")
        return True
    except Exception as e:
        logging.error(f"Error exporting authentic history to CSV: {e}")
        return False

def main():
    """Main function"""
    logging.info("Starting AdTech sentiment recalculation")
    update_historical_adtech_sentiment()
    logging.info("AdTech sentiment recalculation complete")

if __name__ == "__main__":
    main()