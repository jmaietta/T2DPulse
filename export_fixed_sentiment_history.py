#!/usr/bin/env python3
# export_fixed_sentiment_history.py
# -----------------------------------------------------------
# Export authentic sector sentiment history for download

import os
import pandas as pd
from datetime import datetime

def export_sentiment_history(output_format='excel'):
    """
    Export sector sentiment history to Excel or CSV
    
    Args:
        output_format (str): 'excel' or 'csv'
        
    Returns:
        str: Path to the output file
    """
    try:
        # Load authentic sector history
        csv_path = "data/authentic_sector_history.csv"
        
        if not os.path.exists(csv_path):
            print(f"Warning: Authentic sector history file not found at {csv_path}")
            return None
            
        # Read the CSV file
        df = pd.read_csv(csv_path)
        
        # Format date column (if it's not already in the right format)
        if pd.api.types.is_datetime64_dtype(df['date']):
            df['date'] = df['date'].dt.strftime('%Y-%m-%d')
            
        # Create the data directory if it doesn't exist
        os.makedirs('data', exist_ok=True)
        
        # Today's date for the filename
        today = datetime.now().strftime('%Y-%m-%d')
        
        if output_format.lower() == 'excel':
            # Export to Excel
            output_path = f"data/sector_sentiment_history_{today}.xlsx"
            
            # Create Excel file
            with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
                df.to_excel(writer, sheet_name='Sector Sentiment History', index=False)
                
            print(f"Exported sentiment history to {output_path}")
            return output_path
            
        else:
            # Export to CSV
            output_path = f"data/sector_sentiment_history_{today}.csv"
            df.to_csv(output_path, index=False)
            
            print(f"Exported sentiment history to {output_path}")
            return output_path
            
    except Exception as e:
        print(f"Error exporting sentiment history: {e}")
        return None

if __name__ == "__main__":
    # Test export
    excel_path = export_sentiment_history('excel')
    csv_path = export_sentiment_history('csv')
    
    print(f"Excel export: {excel_path}")
    print(f"CSV export: {csv_path}")