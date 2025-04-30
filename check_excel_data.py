#!/usr/bin/env python3
# check_excel_data.py
# Quick script to check the Excel data structure

import pandas as pd
import os
from datetime import datetime

def main():
    """Check the structure and contents of the Excel file"""
    today = datetime.now().strftime("%Y-%m-%d")
    filename = f"data/sector_sentiment_history_{today}.xlsx"
    
    if not os.path.exists(filename):
        print(f"File does not exist: {filename}")
        return
    
    print(f"Reading Excel file: {filename}")
    
    # Read the main sheet (sector data by date)
    df = pd.read_excel(filename, sheet_name=0)
    
    # Display basic information
    print("\n--- Excel File Structure ---")
    print(f"File size: {os.path.getsize(filename):,} bytes")
    print(f"Sheet names: {pd.ExcelFile(filename).sheet_names}")
    
    # Main data sheet info
    print("\n--- Main Data Sheet ---")
    print(f"Shape: {df.shape[0]} rows x {df.shape[1]} columns")
    print("Columns:", df.columns.tolist())
    
    # Show the date range
    if 'date' in df.columns:
        print(f"Date range: {df['date'].min()} to {df['date'].max()}")
    
    # Show a few rows
    print("\n--- Sample Data (First 5 Rows) ---")
    print(df.head())
    
    # Show basic statistics for each sector
    print("\n--- Sector Statistics ---")
    # Exclude the date column for statistics
    numeric_df = df.drop(columns=['date']) if 'date' in df.columns else df
    print(numeric_df.describe().round(2).T[['count', 'mean', 'std', 'min', 'max']])
    
    # If there are other sheets, check them too
    excel_file = pd.ExcelFile(filename)
    for sheet in excel_file.sheet_names[1:]:
        print(f"\n--- Sheet: {sheet} ---")
        sheet_df = pd.read_excel(filename, sheet_name=sheet)
        print(f"Shape: {sheet_df.shape[0]} rows x {sheet_df.shape[1]} columns")
        print("Columns:", sheet_df.columns.tolist())
        print(sheet_df.head())

if __name__ == "__main__":
    main()