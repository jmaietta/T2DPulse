#!/usr/bin/env python3
# export_updated_sector_history.py
# ------------------------------------------------------------
# Export updated sector sentiment history for dashboard download

import os
import sys
import datetime
import pandas as pd
import pytz
import json
from pathlib import Path

def get_eastern_date():
    """Get the current date in US Eastern Time"""
    eastern = pytz.timezone('US/Eastern')
    return datetime.datetime.now(eastern)

def load_sector_scores(date_str=None):
    """Load the most recent sector scores or for a specific date"""
    data_dir = 'data'
    
    # If specific date requested, try to load that file
    if date_str:
        file_path = os.path.join(data_dir, f'authentic_sector_history_{date_str}.csv')
        if os.path.exists(file_path):
            try:
                df = pd.read_csv(file_path)
                print(f"Loaded sector scores from {file_path}")
                return df
            except Exception as e:
                print(f"Error loading {file_path}: {e}")
    
    # Otherwise load the latest from the combined history file
    history_file = os.path.join(data_dir, 'authentic_sector_history.csv')
    if os.path.exists(history_file):
        try:
            df = pd.read_csv(history_file)
            print(f"Loaded sector history from {history_file}")
            return df
        except Exception as e:
            print(f"Error loading {history_file}: {e}")
    
    print("No sector history files found")
    return None

def export_sector_history_xlsx(date_str=None):
    """Export sector sentiment history to Excel format for dashboard download"""
    if date_str is None:
        date_str = get_eastern_date().strftime('%Y-%m-%d')
    
    print(f"Exporting sector history for {date_str}...")
    
    # Make sure data directory exists
    data_dir = Path('data')
    data_dir.mkdir(exist_ok=True)
    
    # Try to load data from different sources
    sector_scores = load_sector_scores(date_str)
    if sector_scores is None:
        # Try to load from JSON format also
        try:
            json_file = data_dir / 'authentic_sector_history.json'
            if json_file.exists():
                with open(json_file, 'r') as f:
                    history_data = json.load(f)
                    
                # Convert JSON to DataFrame format expected by dashboard
                if isinstance(history_data, dict) and 'dates' in history_data and 'sectors' in history_data:
                    # Format is {dates: [...], sectors: {sector1: [...], sector2: [...]}}
                    dates = history_data['dates']
                    data = {'Date': dates}
                    
                    for sector, scores in history_data['sectors'].items():
                        # Make sure scores array matches dates array in length
                        if len(scores) == len(dates):
                            data[sector] = scores
                        else:
                            print(f"Warning: Scores for {sector} don't match dates length, padding with NaN")
                            # Pad with NaN if needed
                            data[sector] = scores + [None] * (len(dates) - len(scores))
                    
                    sector_scores = pd.DataFrame(data)
                    print(f"Loaded sector history from {json_file}")
                else:
                    print(f"Error: JSON format in {json_file} not recognized")
        except Exception as e:
            print(f"Error loading sector history from JSON: {e}")
    
    if sector_scores is None:
        print("Error: Could not load sector scores from any source")
        return False
    
    # Create a filename for the Excel export
    export_path = data_dir / f'sector_sentiment_history_{date_str}.xlsx'
    csv_export_path = data_dir / f'sector_sentiment_history_{date_str}.csv'
    
    try:
        # Save to Excel format
        sector_scores.to_excel(export_path, index=False, engine='openpyxl')
        print(f"Sector history exported to {export_path}")
        
        # Also save to CSV for easier access
        sector_scores.to_csv(csv_export_path, index=False)
        print(f"Sector history exported to {csv_export_path}")
        
        return True
    except Exception as e:
        print(f"Error exporting sector history: {e}")
        return False

if __name__ == '__main__':
    # Get optional date parameter or use today's date
    date_param = sys.argv[1] if len(sys.argv) > 1 else None
    success = export_sector_history_xlsx(date_param)
    sys.exit(0 if success else 1)