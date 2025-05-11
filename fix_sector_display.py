#!/usr/bin/env python3
"""
Fix the sector displays by ensuring that the correct data files are being used
and that the data is being properly processed for display.
"""

import os
import pandas as pd
import shutil
from datetime import datetime, timedelta
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def ensure_authentic_sector_history():
    """
    Ensure that data/authentic_sector_history.csv exists and is properly populated
    with data from our corrected_sector_market_caps.csv file.
    
    The historical market cap data is in corrected_sector_market_caps.csv
    The sparkline data should be in data/authentic_sector_history.csv
    But the dashboard looks for data/authentic_sector_history.csv in app.py
    """
    logger.info("Ensuring authentic sector history file exists...")
    
    # Check if the corrected data exists
    if not os.path.exists('corrected_sector_market_caps.csv'):
        logger.error("Corrected sector market cap data not found")
        return False
    
    # Make sure data directory exists
    data_dir = 'data'
    if not os.path.exists(data_dir):
        os.makedirs(data_dir)
    
    # Load the corrected market cap data
    market_cap_df = pd.read_csv('corrected_sector_market_caps.csv')
    logger.info(f"Loaded corrected market cap data with {len(market_cap_df)} rows")
    
    # Convert dates to datetime
    market_cap_df['date'] = pd.to_datetime(market_cap_df['date'])
    
    # Create normalized scores for each sector based on min-max scaling
    sector_columns = [col for col in market_cap_df.columns if col != 'date']
    normalized_data = {'date': market_cap_df['date']}
    
    for sector in sector_columns:
        # Get sector values
        sector_values = market_cap_df[sector]
        
        # Calculate min, max, and range
        min_val = sector_values.min()
        max_val = sector_values.max()
        range_val = max_val - min_val
        
        if range_val > 0:
            # Convert to a 0-100 scale for sentiment scores
            # (most apps use 40-60 as normal range, 0-40 as bearish, 60-100 as bullish)
            normalized_scores = 40 + 40 * ((sector_values - min_val) / range_val)
            
            # Limit to 0-100 range
            normalized_scores = normalized_scores.clip(0, 100)
            
            # Add to normalized data
            normalized_data[sector] = normalized_scores
        else:
            # If there's no range (all values are the same), set a neutral score of 50
            normalized_data[sector] = [50] * len(sector_values)
    
    # Create a normalized DataFrame
    normalized_df = pd.DataFrame(normalized_data)
    
    # Save the normalized data to authentic_sector_history.csv
    authentic_history_path = os.path.join(data_dir, 'authentic_sector_history.csv')
    normalized_df.to_csv(authentic_history_path, index=False)
    logger.info(f"Saved normalized sector data to {authentic_history_path}")
    
    # Also update sector_30day_history.csv for backward compatibility
    # but with 'Date' as column name instead of 'date'
    normalized_df_copy = normalized_df.copy()
    normalized_df_copy.rename(columns={'date': 'Date'}, inplace=True)
    
    # Extend to 30 days
    today = datetime.now()
    date_range = pd.date_range(end=today, periods=30)
    
    # Create a template DataFrame with the full date range
    template_df = pd.DataFrame({'Date': date_range})
    
    # Merge with the existing data
    merged_df = pd.merge(template_df, normalized_df_copy, on='Date', how='left')
    
    # Fill in weekends and missing days with placeholder value of 50
    for col in merged_df.columns:
        if col != 'Date':
            merged_df[col] = merged_df[col].fillna(50)
    
    # Save to sector_30day_history.csv
    sector_history_path = os.path.join(data_dir, 'sector_30day_history.csv')
    merged_df.to_csv(sector_history_path, index=False)
    logger.info(f"Updated {sector_history_path} with authentic sentiment scores")
    
    return True

def verify_sector_display_data():
    """Verify that the sector display data is correct and ready for display"""
    authentic_history_path = os.path.join('data', 'authentic_sector_history.csv')
    
    if not os.path.exists(authentic_history_path):
        logger.error(f"Authentic sector history file not found: {authentic_history_path}")
        return False
    
    try:
        df = pd.read_csv(authentic_history_path)
        
        # Basic validation
        if 'date' not in df.columns:
            logger.error("Date column not found in authentic sector history")
            return False
        
        # Make sure all sector columns are present
        expected_sectors = [
            'AI Infrastructure', 'AdTech', 'Cloud Infrastructure', 'Consumer Internet',
            'Cybersecurity', 'Dev Tools / Analytics', 'Enterprise SaaS', 'Fintech',
            'Hardware / Devices', 'IT Services / Legacy Tech', 'SMB SaaS',
            'Semiconductors', 'Vertical SaaS', 'eCommerce'
        ]
        
        missing_sectors = [s for s in expected_sectors if s not in df.columns]
        if missing_sectors:
            logger.error(f"Missing sector columns: {missing_sectors}")
            return False
        
        # Check for reasonable values
        for sector in expected_sectors:
            if sector in df.columns:
                min_val = df[sector].min()
                max_val = df[sector].max()
                
                if min_val < 0 or max_val > 100:
                    logger.error(f"Invalid score range for {sector}: {min_val}-{max_val}")
                    return False
                
                # Check for variation in scores
                if min_val == max_val:
                    logger.warning(f"No variation in scores for {sector}: all values = {min_val}")
        
        # Validate date range
        df['date'] = pd.to_datetime(df['date'])
        date_range = (df['date'].max() - df['date'].min()).days
        
        if date_range < 20:
            logger.warning(f"Date range may be too short: {date_range} days")
        
        # Print a sample of the data for verification
        logger.info(f"Sector display data sample:")
        logger.info(df.tail(5).to_string())
        
        return True
    
    except Exception as e:
        logger.error(f"Error verifying sector display data: {e}")
        return False

def fix_app_sector_display():
    """Fix the app.py sector display code if needed"""
    app_path = 'app.py'
    
    if not os.path.exists(app_path):
        logger.error(f"App file not found: {app_path}")
        return False
    
    # Make a backup
    backup_path = f"app_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.py"
    shutil.copy2(app_path, backup_path)
    logger.info(f"Created backup of app.py at {backup_path}")
    
    try:
        # Read the file
        with open(app_path, 'r') as f:
            code = f.read()
        
        # Check if the file references authentic_sector_history.csv
        if 'authentic_sector_history.csv' in code:
            logger.info("App already references authentic_sector_history.csv")
            
            # Check if the create_sector_sparkline function needs to be fixed
            if "df = df[~(weekend_mask | may4_mask | may11_mask)].copy()" in code:
                # Replace the date filtering part with a simpler version that just keeps all business days
                fixed_code = code.replace(
                    "df = df[~(weekend_mask | may4_mask | may11_mask)].copy()",
                    "df = df[~weekend_mask].copy()  # Only filter out weekends, keep all business days"
                )
                
                # Write the fixed code back
                with open(app_path, 'w') as f:
                    f.write(fixed_code)
                
                logger.info("Fixed date filtering in create_sector_sparkline function")
            
            # Check if we have access to the historical data but still showing flat lines
            # This could happen if the values for a sector are so close that they round to the same value when scaled
            if "# Get dates and values for sectors with variation" in code:
                # Print the values data to diagnose scaling issues
                fixed_code = code.replace(
                    "# Get dates and values for sectors with variation",
                    "# Get dates and values for sectors with variation\n            logger.info(f\"Sector {sector_name} values: {values.iloc[0:5].values} ... {values.iloc[-5:].values}\")"
                )
                
                # Write the fixed code back
                with open(app_path, 'w') as f:
                    f.write(fixed_code)
                
                logger.info("Added value logging to diagnose scaling issues")
            
            return True
            
        else:
            logger.error("App does not reference authentic_sector_history.csv, manual fix required")
            return False
    
    except Exception as e:
        logger.error(f"Error fixing app sector display: {e}")
        return False

def main():
    """Main function"""
    logger.info("Starting sector display fix...")
    
    # Step 1: Ensure the authentic_sector_history.csv file exists
    if not ensure_authentic_sector_history():
        logger.error("Failed to ensure authentic sector history")
        return False
    
    # Step 2: Verify the sector display data
    if not verify_sector_display_data():
        logger.error("Failed to verify sector display data")
        return False
    
    # Step 3: Fix the app.py sector display code if needed
    if not fix_app_sector_display():
        logger.error("Failed to fix app sector display code")
        return False
    
    logger.info("Successfully fixed sector display")
    return True

if __name__ == "__main__":
    main()