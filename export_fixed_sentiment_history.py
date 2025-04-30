#!/usr/bin/env python3
# export_fixed_sentiment_history.py
# -----------------------------------------------------------
# Export the predefined sentiment history to an Excel file

import pandas as pd
import os
from datetime import datetime

# Define paths
DATA_DIR = "data"
PREDEFINED_CSV = os.path.join(DATA_DIR, "predefined_sector_history.csv")
EXCEL_EXPORT = os.path.join(DATA_DIR, f"sector_sentiment_history_{datetime.now().strftime('%Y-%m-%d')}.xlsx")

def export_predefined_history_to_excel():
    """Export the predefined sector sentiment history to an Excel file"""
    try:
        # Check if the predefined data file exists
        if not os.path.exists(PREDEFINED_CSV):
            print(f"Predefined data file not found: {PREDEFINED_CSV}")
            return False
            
        # Read the CSV file
        df = pd.read_csv(PREDEFINED_CSV)
        
        # Create an Excel writer
        writer = pd.ExcelWriter(EXCEL_EXPORT, engine='openpyxl')
        
        # Write the data to Excel
        df.to_excel(writer, sheet_name='Sector Sentiment History', index=False)
        
        # Format the Excel file (set column widths, etc.)
        workbook = writer.book
        worksheet = writer.sheets['Sector Sentiment History']
        
        # Set column widths
        worksheet.column_dimensions['A'].width = 12  # Date column
        
        # Set column width for all sector columns
        for i in range(1, len(df.columns)):
            col_letter = chr(ord('A') + i)
            worksheet.column_dimensions[col_letter].width = 15
        
        # Save the Excel file
        writer.close()
        
        print(f"Exported predefined sector history to {EXCEL_EXPORT}")
        return True
        
    except Exception as e:
        print(f"Error exporting predefined history to Excel: {e}")
        return False

if __name__ == "__main__":
    export_predefined_history_to_excel()