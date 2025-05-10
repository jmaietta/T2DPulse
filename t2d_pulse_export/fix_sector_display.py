#!/usr/bin/env python3
# fix_sector_display.py
# -----------------------------------------------------------
# Ensures consistent sector data for display in the dashboard

import os
import pandas as pd
import json
from datetime import datetime
import pytz
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def get_eastern_date():
    """Get the current date in US Eastern Time"""
    eastern = pytz.timezone('US/Eastern')
    return datetime.now(eastern)

def create_directory_if_needed(directory):
    """Create directory if it doesn't exist"""
    if not os.path.exists(directory):
        os.makedirs(directory, exist_ok=True)
        logger.info(f"Created directory: {directory}")

def ensure_consistent_sector_data():
    """Ensure consistent sector data for display in the dashboard"""
    # Create data directory if needed
    create_directory_if_needed('data')
    
    # Today's date
    today = get_eastern_date().strftime('%Y-%m-%d')
    
    # First, try to run the improved fix_sector_charts module
    try:
        import fix_sector_charts_improved
        logger.info("Running sector charts fix to ensure consistent data...")
        if fix_sector_charts_improved.fix_sector_charts():
            logger.info("Successfully generated sector display data")
            return True
        else:
            logger.warning("Failed to run sector charts fix")
    except Exception as e:
        logger.error(f"Error running sector charts fix: {e}")
    
    # If the fix didn't work, try to create definitive sector scores manually
    try:
        # Look for authentic sector history files
        today_file = f"data/authentic_sector_history_{today}.csv"
        
        if os.path.exists(today_file):
            logger.info(f"Found today's authentic sector history: {today_file}")
            df = pd.read_csv(today_file)
            
            # Standardize column names
            if 'date' in df.columns and 'Date' not in df.columns:
                df.rename(columns={'date': 'Date'}, inplace=True)
            
            # Save as definitive sector scores
            df.to_csv("data/definitive_sector_scores.csv", index=False)
            logger.info("Created definitive sector scores from authentic history")
            
            # Also create a JSON version for the charts
            create_json_from_csv("data/definitive_sector_scores.csv", "data/sector_history.json")
            return True
    except Exception as e:
        logger.error(f"Error creating definitive sector scores: {e}")
    
    # If we got here, we need to create a fallback using the sector_sentiment_history module
    try:
        import sector_sentiment_history
        logger.info("Using sector sentiment history as fallback...")
        
        # Get all sector scores from the history
        sectors = sector_sentiment_history.get_all_sector_names()
        recent_scores = {}
        
        for sector in sectors:
            # Get the most recent score for this sector
            sector_history = sector_sentiment_history.get_sector_history(sector)
            if sector_history and len(sector_history) > 0:
                # Get the most recent date and score
                recent_date = sector_history[-1]['date']
                recent_score = sector_history[-1]['score']
                
                # Convert from -1/+1 to 0-100 if needed
                if abs(recent_score) <= 1.0:
                    recent_score = ((recent_score + 1) * 50)
                
                # Round to 1 decimal place
                recent_score = round(recent_score, 1)
                
                recent_scores[sector] = recent_score
        
        if recent_scores:
            # Create a DataFrame with today's date and the sector scores
            df = pd.DataFrame({'Date': [today]})
            for sector, score in recent_scores.items():
                df[sector] = score
            
            # Save as definitive sector scores
            df.to_csv("data/definitive_sector_scores.csv", index=False)
            logger.info("Created definitive sector scores from sector history")
            
            # Also create a JSON version for the charts
            create_json_from_csv("data/definitive_sector_scores.csv", "data/sector_history.json")
            return True
    except Exception as e:
        logger.error(f"Error creating fallback sector scores: {e}")
    
    # If all else fails, use default values
    logger.warning("Using default sector scores as last resort")
    create_default_sector_scores()
    return False

def create_json_from_csv(csv_file, json_file):
    """Create a JSON file for sector charts from a CSV file"""
    try:
        df = pd.read_csv(csv_file)
        
        # Create a list of dates (should be just one date if it's definitive_sector_scores.csv)
        dates = df['Date'].tolist()
        
        # Create a dict of sector data
        sectors = {}
        for column in df.columns:
            if column != 'Date':
                sectors[column] = df[column].tolist()
        
        # Create the JSON structure
        data = {
            'dates': dates,
            'sectors': sectors
        }
        
        # Save the JSON data
        with open(json_file, 'w') as f:
            json.dump(data, f)
        
        logger.info(f"Created JSON sector data: {json_file}")
        return True
    except Exception as e:
        logger.error(f"Error creating JSON from CSV: {e}")
        return False

def create_default_sector_scores():
    """Create default sector scores if all else fails"""
    try:
        # Today's date
        today = get_eastern_date().strftime('%Y-%m-%d')
        
        # Default neutral scores for all sectors
        sector_scores = {
            "SMB SaaS": 52.0,
            "Enterprise SaaS": 52.0,
            "Cloud Infrastructure": 53.0,
            "AdTech": 53.5,
            "Fintech": 52.0,
            "Consumer Internet": 51.5,
            "eCommerce": 53.5,
            "Cybersecurity": 49.0,
            "Dev Tools / Analytics": 49.5,
            "Semiconductors": 57.5,
            "AI Infrastructure": 53.0,
            "Vertical SaaS": 48.0,
            "IT Services / Legacy Tech": 57.5,
            "Hardware / Devices": 57.5
        }
        
        # Create a DataFrame with today's date and the sector scores
        df = pd.DataFrame({'Date': [today]})
        for sector, score in sector_scores.items():
            df[sector] = score
        
        # Save as definitive sector scores
        df.to_csv("data/definitive_sector_scores.csv", index=False)
        logger.info("Created default sector scores")
        
        # Also create a JSON version for the charts
        create_json_from_csv("data/definitive_sector_scores.csv", "data/sector_history.json")
        return True
    except Exception as e:
        logger.error(f"Error creating default sector scores: {e}")
        return False

if __name__ == "__main__":
    success = ensure_consistent_sector_data()
    import sys
    sys.exit(0 if success else 1)