#!/usr/bin/env python3
# recalculate_historical_scores.py
# -----------------------------------------------------------
# Recalculate historical sector scores using the updated methodology
# that includes Sector EMA as a 14th indicator with the specified weights

import os
import pandas as pd
import json
import pytz
from datetime import datetime, timedelta
import sentiment_engine
import config
import sector_ema_integration

def recalculate_historical_scores(days_back=30):
    """
    Recalculate historical sector scores for the past N days using the updated methodology
    
    Args:
        days_back (int): Number of days to recalculate back from today
        
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        # Get the current date in EDT timezone
        eastern = pytz.timezone('US/Eastern')
        today = datetime.now(eastern)
        
        # Load existing authentic history data
        csv_path = "data/authentic_sector_history.csv"
        if not os.path.exists(csv_path):
            print(f"Error: Authentic history file not found at {csv_path}")
            return False
        
        df = pd.read_csv(csv_path)
        df['date'] = pd.to_datetime(df['date'])
        
        # Calculate the start date (N days ago)
        start_date = today - timedelta(days=days_back)
        start_date_str = start_date.strftime('%Y-%m-%d')
        today_str = today.strftime('%Y-%m-%d')
        
        # Get dates to recalculate - convert to string for consistent comparison
        df['date_str'] = df['date'].dt.strftime('%Y-%m-%d')
        dates_to_recalculate = df[(df['date_str'] >= start_date_str) & (df['date_str'] <= today_str)].copy()
        
        if dates_to_recalculate.empty:
            print(f"No historical dates found in the past {days_back} days")
            return False
        
        print(f"Recalculating scores for {len(dates_to_recalculate)} dates from {start_date.strftime('%Y-%m-%d')} to {today.strftime('%Y-%m-%d')}")
        
        # For each date, recalculate all sector scores
        for _, row in dates_to_recalculate.iterrows():
            date = row['date']
            print(f"\nRecalculating sector scores for {date.strftime('%Y-%m-%d')}...")
            
            # Get all sector names from the row
            sectors = [col for col in row.index if col not in ['date', 'date_str']]
            
            # Get historical indicator values for this date
            macro_values = sentiment_engine.get_historical_indicator_values(date)
            
            if not macro_values:
                print(f"No historical indicator data available for {date.strftime('%Y-%m-%d')}")
                continue
            
            # Add Sector EMA factor for this date
            try:
                ema_factors = sector_ema_integration.get_historical_ema_factors(date)
                if ema_factors:
                    # Use the average of all sector EMAs for all sectors
                    avg_ema = sum(ema_factors.values()) / len(ema_factors)
                    macro_values["Sector_EMA_Factor"] = avg_ema
                    print(f"Using average historical EMA factor: {avg_ema:.4f}")
            except Exception as e:
                print(f"Error getting historical EMA factors: {e}")
                # Use a small positive value rather than neutral
                macro_values["Sector_EMA_Factor"] = 0.05  # Small positive bias
            
            # Calculate sector scores with the updated methodology
            sector_scores = sentiment_engine.score_sectors(macro_values)
            
            if not sector_scores:
                print(f"Failed to calculate sector scores for {date.strftime('%Y-%m-%d')}")
                continue
            
            # Update the DataFrame with new scores
            for sector_data in sector_scores:
                sector_name = sector_data['sector']
                score = sector_data['score']
                
                # Normalize to 0-100 scale for storage
                normalized_score = ((score + 1.0) / 2.0) * 100
                
                # Update DataFrame if this sector exists in the columns
                if sector_name in df.columns:
                    df.loc[df['date'] == date, sector_name] = normalized_score
                else:
                    # Add new column if needed
                    df[sector_name] = None
                    df.loc[df['date'] == date, sector_name] = normalized_score
            
            print(f"Updated scores for {len(sector_scores)} sectors on {date.strftime('%Y-%m-%d')}")
        
        # Remove the date_str column before saving
        df = df.drop(columns=['date_str'])
        
        # Save updated DataFrame
        df.to_csv(csv_path, index=False)
        print(f"\nSaved recalculated sector scores to {csv_path}")
        
        # Also update the JSON file for API access
        json_path = "data/authentic_sector_history.json"
        
        # Convert to dictionary
        history_dict = {}
        for _, row in df.iterrows():
            date_str = row['date'].strftime('%Y-%m-%d')
            history_dict[date_str] = {sector: row[sector] for sector in df.columns if sector not in ['date', 'date_str']}
        
        # Save to JSON
        with open(json_path, 'w') as f:
            json.dump(history_dict, f, indent=2)
        print(f"Saved recalculated sector scores to {json_path}")
        
        # Export today's data to date-specific CSV for direct download
        today_str = today.strftime('%Y-%m-%d')
        today_csv_path = f"data/authentic_sector_history_{today_str}.csv"
        df.to_csv(today_csv_path, index=False)
        print(f"Exported recalculated sector scores to {today_csv_path}")
        
        return True
    
    except Exception as e:
        print(f"Error recalculating historical scores: {e}")
        return False

# Run the function if executed directly
if __name__ == "__main__":
    recalculate_historical_scores(days_back=30)
