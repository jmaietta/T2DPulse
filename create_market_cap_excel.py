"""
Create an Excel file with market cap data for all sectors.
"""

import pandas as pd
import os

try:
    # Read the CSV data
    df = pd.read_csv('sector_marketcap_table.csv')
    
    # Convert date string to datetime for better Excel formatting
    df['Date'] = pd.to_datetime(df['Date']).dt.date
    
    # Create a new Excel file
    output_file = '30day_sector_marketcap_analysis.xlsx'
    
    # Create a Pandas Excel writer using XlsxWriter as the engine
    with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
        # Write the main data to a sheet
        df.to_excel(writer, sheet_name='Market Cap Data (Billions)', index=False)
        
        # Create a summary sheet with sector averages, min, max
        summary_df = pd.DataFrame()
        for column in df.columns:
            if column != 'Date':
                summary_df.loc['Average (B)', column] = df[column].mean() / 1e9
                summary_df.loc['Min (B)', column] = df[column].min() / 1e9
                summary_df.loc['Max (B)', column] = df[column].max() / 1e9
                summary_df.loc['Latest (B)', column] = df[column].iloc[-1] / 1e9
        
        summary_df.to_excel(writer, sheet_name='Summary')
        
        # Create a sheet with formatted data in billions
        df_billions = df.copy()
        for column in df_billions.columns:
            if column != 'Date':
                df_billions[column] = df_billions[column] / 1e9
        
        df_billions.to_excel(writer, sheet_name='Market Cap Data (Readable)', index=False)
    
    print(f"Excel file created successfully: {output_file}")
    print(f"File size: {os.path.getsize(output_file)} bytes")
    
except Exception as e:
    print(f"Error creating Excel file: {e}")