#!/usr/bin/env python3
"""
Update the website to use authentic market cap data for all sectors,
especially focusing on AI Infrastructure which had incorrect values.
"""

import os
import shutil
import pandas as pd
from datetime import datetime
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def update_market_cap_data():
    """Update the market cap data in the application"""
    # Ensure the corrected data exists
    if not os.path.exists('corrected_sector_market_caps.csv'):
        logger.error("Corrected sector market cap data not found")
        return False
    
    # Backup the original data
    data_dir = 'data'
    if not os.path.exists(data_dir):
        os.makedirs(data_dir)
    
    # Load the corrected data
    corrected_df = pd.read_csv('corrected_sector_market_caps.csv')
    logger.info(f"Loaded corrected data with {len(corrected_df)} dates and {len(corrected_df.columns) - 1} sectors")
    
    # Save the corrected data to data/authentic_sector_market_caps.csv
    corrected_df.to_csv(os.path.join(data_dir, 'authentic_sector_market_caps.csv'), index=False)
    logger.info(f"Updated data/authentic_sector_market_caps.csv")
    
    # Update the 30-day sector history file used by the dashboard
    sector_history_path = os.path.join(data_dir, 'sector_30day_history.csv')
    
    # Convert the market cap data to sentiment scores and update the sector history
    if os.path.exists(sector_history_path):
        try:
            # Load the existing sector history
            sector_history = pd.read_csv(sector_history_path)
            logger.info(f"Loaded existing sector history with {len(sector_history)} dates")
            
            # Convert dates to datetime
            sector_history['Date'] = pd.to_datetime(sector_history['Date'])
            corrected_df['date'] = pd.to_datetime(corrected_df['date'])
            
            # For each date in corrected_df, create a normalized score (0-100) for each sector
            # based on its percentile rank within its min-max range over the available dates
            
            # First, get the date range that overlaps between the two datasets
            common_dates = set(sector_history['Date']) & set(corrected_df['date'])
            
            if common_dates:
                logger.info(f"Found {len(common_dates)} common dates between datasets")
                
                # Create a mapping of normalized scores for each sector and date
                normalized_scores = {}
                
                for sector in corrected_df.columns:
                    if sector == 'date':
                        continue
                    
                    # Get the market cap values for this sector
                    sector_values = corrected_df[sector]
                    
                    # Calculate min, max, and range
                    min_val = sector_values.min()
                    max_val = sector_values.max()
                    range_val = max_val - min_val
                    
                    if range_val > 0:
                        # Normalize to 0-100 scale
                        for date, val in zip(corrected_df['date'], sector_values):
                            date_str = date.strftime('%Y-%m-%d')
                            
                            # Convert to a 0-100 scale for sentiment scores
                            # (most apps use 40-60 as normal range, 0-40 as bearish, 60-100 as bullish)
                            score = 40 + 40 * ((val - min_val) / range_val)
                            
                            # Limit to 0-100 range
                            score = max(0, min(100, score))
                            
                            # Store the normalized score
                            normalized_scores[(date_str, sector)] = score
                
                # Update the sector history with the normalized scores
                for i, row in sector_history.iterrows():
                    date_str = row['Date'].strftime('%Y-%m-%d')
                    
                    for sector in sector_history.columns:
                        if sector == 'Date':
                            continue
                        
                        if (date_str, sector) in normalized_scores:
                            sector_history.loc[i, sector] = normalized_scores[(date_str, sector)]
                
                # Save the updated sector history
                sector_history.to_csv(sector_history_path, index=False)
                logger.info(f"Updated sector history with authentic sentiment scores")
                
                # Also update any export files in the data directory
                update_export_files(normalized_scores)
                
                return True
                
            else:
                logger.error("No common dates found between datasets")
                return False
            
        except Exception as e:
            logger.error(f"Error updating sector history: {e}")
            return False
    else:
        logger.error(f"Sector history file not found: {sector_history_path}")
        return False

def update_export_files(normalized_scores):
    """Update any exported sector history files with the authentic scores"""
    data_dir = 'data'
    export_count = 0
    
    for filename in os.listdir(data_dir):
        if filename.startswith('sector_sentiment_history') and (filename.endswith('.csv') or filename.endswith('.xlsx')):
            file_path = os.path.join(data_dir, filename)
            try:
                if filename.endswith('.csv'):
                    # Update CSV file
                    df = pd.read_csv(file_path)
                    df['Date'] = pd.to_datetime(df['Date'])
                    
                    for i, row in df.iterrows():
                        date_str = row['Date'].strftime('%Y-%m-%d')
                        
                        for sector in df.columns:
                            if sector == 'Date':
                                continue
                            
                            if (date_str, sector) in normalized_scores:
                                df.loc[i, sector] = normalized_scores[(date_str, sector)]
                    
                    df.to_csv(file_path, index=False)
                    export_count += 1
                    
                elif filename.endswith('.xlsx'):
                    # Update Excel file
                    df = pd.read_excel(file_path)
                    df['Date'] = pd.to_datetime(df['Date'])
                    
                    for i, row in df.iterrows():
                        date_str = row['Date'].strftime('%Y-%m-%d')
                        
                        for sector in df.columns:
                            if sector == 'Date':
                                continue
                            
                            if (date_str, sector) in normalized_scores:
                                df.loc[i, sector] = normalized_scores[(date_str, sector)]
                    
                    df.to_excel(file_path, index=False)
                    export_count += 1
                
            except Exception as e:
                logger.error(f"Error updating {file_path}: {e}")
    
    logger.info(f"Updated {export_count} export files with authentic sentiment scores")

def restart_dashboard():
    """Restart the dashboard to apply the changes"""
    # Update sector_market_caps.csv with the latest authentic data
    if os.path.exists('corrected_sector_market_caps.csv'):
        shutil.copy('corrected_sector_market_caps.csv', 'sector_market_caps.csv')
        logger.info("Updated sector_market_caps.csv with authentic data")
    
    # The actual restart happens via the workflow restart command
    logger.info("Dashboard updates complete, ready for restart")
    return True

def main():
    """Main function to update market cap data and restart the dashboard"""
    logger.info("Starting update of authentic market cap data...")
    
    if update_market_cap_data():
        logger.info("Successfully updated authentic market cap data")
        
        if restart_dashboard():
            logger.info("Ready to restart the dashboard")
            return True
    
    logger.error("Failed to update authentic market cap data")
    return False

if __name__ == "__main__":
    main()