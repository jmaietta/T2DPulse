"""
Fix sector score export functionality for T2D Pulse dashboard.

This script ensures that sector sentiment history can be exported to Excel and CSV formats,
and fixes the download functionality in the dashboard.
"""

import os
import pandas as pd
import json
from datetime import datetime
import numpy as np

def normalize_score(score):
    """Convert -1 to +1 score to 0-100 scale"""
    return ((score + 1) / 2) * 100

def get_sector_history_from_files():
    """Gather sector history from all available files"""
    # Find all sector history files
    sector_files = []
    for root, _, files in os.walk('.'):
        for file in files:
            if 'sector' in file.lower() and 'history' in file.lower() and (file.endswith('.csv') or file.endswith('.json')):
                sector_files.append(os.path.join(root, file))
    
    # Load data from each file
    all_records = []
    
    # Process CSV files
    for file_path in sector_files:
        if file_path.endswith('.csv'):
            try:
                df = pd.read_csv(file_path)
                if 'date' in df.columns and 'sector' in df.columns and 'score' in df.columns:
                    print(f"Loading data from {file_path}")
                    
                    # Convert scores if needed (normalize to 0-100 scale)
                    if 'normalized_score' not in df.columns:
                        df['normalized_score'] = df['score'].apply(normalize_score)
                    
                    # Add stance if missing
                    if 'stance' not in df.columns:
                        df['stance'] = df['normalized_score'].apply(lambda x: 
                                                                  'Bullish' if x >= 60 else 
                                                                  'Bearish' if x <= 30 else 
                                                                  'Neutral')
                    
                    all_records.extend(df.to_dict('records'))
            except Exception as e:
                print(f"Error processing {file_path}: {e}")
    
    # Process JSON files
    for file_path in sector_files:
        if file_path.endswith('.json'):
            try:
                with open(file_path, 'r') as f:
                    data = json.load(f)
                
                # Check if it's our expected format
                if isinstance(data, list) and all('date' in item for item in data):
                    print(f"Loading data from {file_path}")
                    
                    # Extract records
                    for day_data in data:
                        date = day_data['date']
                        for sector_data in day_data.get('sectors', []):
                            sector_data['date'] = date
                            
                            # Normalize score if needed
                            if 'normalized_score' not in sector_data and 'score' in sector_data:
                                sector_data['normalized_score'] = normalize_score(sector_data['score'])
                            
                            # Add stance if missing
                            if 'stance' not in sector_data and 'normalized_score' in sector_data:
                                score = sector_data['normalized_score']
                                sector_data['stance'] = 'Bullish' if score >= 60 else 'Bearish' if score <= 30 else 'Neutral'
                            
                            all_records.append(sector_data)
            except Exception as e:
                print(f"Error processing {file_path}: {e}")
    
    return all_records

def create_export_files():
    """Create sector history export files in Excel and CSV formats"""
    records = get_sector_history_from_files()
    
    if not records:
        print("No sector history data found")
        return
    
    # Create DataFrame from records
    df = pd.DataFrame(records)
    
    # Ensure required columns exist
    required_columns = ['date', 'sector', 'score', 'normalized_score', 'stance']
    for col in required_columns:
        if col not in df.columns:
            print(f"Missing required column: {col}")
            if col == 'normalized_score' and 'score' in df.columns:
                df['normalized_score'] = df['score'].apply(normalize_score)
            elif col == 'stance' and 'normalized_score' in df.columns:
                df['stance'] = df['normalized_score'].apply(lambda x: 
                                                         'Bullish' if x >= 60 else 
                                                         'Bearish' if x <= 30 else 
                                                         'Neutral')
            else:
                print(f"Cannot derive {col}, using placeholder")
                if col == 'date':
                    df['date'] = datetime.now().strftime('%Y-%m-%d')
                else:
                    df[col] = 'Unknown'
    
    # Convert date to datetime
    try:
        df['date'] = pd.to_datetime(df['date'])
    except:
        print("Warning: Could not convert date column to datetime")
    
    # Round scores to 2 decimal places
    if 'normalized_score' in df.columns:
        df['normalized_score'] = df['normalized_score'].round(2)
    if 'score' in df.columns:
        df['score'] = df['score'].round(4)
    
    # Sort by date (newest first) and sector
    try:
        df = df.sort_values(['date', 'sector'], ascending=[False, True])
    except:
        print("Warning: Could not sort data")
    
    # Ensure data directory exists
    os.makedirs('data', exist_ok=True)
    
    # Current date for filenames
    today = datetime.now().strftime('%Y-%m-%d')
    
    # Save to Excel (with date in filename)
    excel_file = f'data/sector_sentiment_history_{today}.xlsx'
    df.to_excel(excel_file, index=False)
    print(f"Exported sector history to Excel: {excel_file}")
    
    # Save to Excel (standard filename)
    excel_file_std = 'data/sector_sentiment_history.xlsx'
    df.to_excel(excel_file_std, index=False)
    print(f"Exported sector history to Excel: {excel_file_std}")
    
    # Save to CSV (with date in filename)
    csv_file = f'data/sector_sentiment_history_{today}.csv'
    df.to_csv(csv_file, index=False)
    print(f"Exported sector history to CSV: {csv_file}")
    
    # Save to CSV (standard filename)
    csv_file_std = 'data/sector_sentiment_history.csv'
    df.to_csv(csv_file_std, index=False)
    print(f"Exported sector history to CSV: {csv_file_std}")
    
    # Create JSON format for consistency
    json_data = []
    for date, group in df.groupby('date'):
        day_data = {'date': str(date).split(' ')[0], 'sectors': []}
        for _, row in group.iterrows():
            sector_data = {
                'sector': row['sector'],
                'score': row['score'],
                'normalized_score': row['normalized_score'],
                'stance': row['stance']
            }
            if 'takeaway' in row:
                sector_data['takeaway'] = row['takeaway']
            day_data['sectors'].append(sector_data)
        json_data.append(day_data)
    
    # Save to JSON
    json_file = 'data/sector_sentiment_history.json'
    with open(json_file, 'w') as f:
        json.dump(json_data, f, indent=2)
    print(f"Created JSON sector history file: {json_file}")
    
    return {
        'excel': excel_file_std,
        'csv': csv_file_std,
        'json': json_file,
        'record_count': len(df)
    }

def fix_download_function_in_app():
    """Fix the download_file function in app.py"""
    try:
        with open('app.py', 'r') as f:
            content = f.read()
        
        # Find the download_file function
        download_function = """def download_file(filename):
    \"\"\"
    Serve files from the data directory for download
    This is used to provide downloadable Excel exports of sector sentiment history
    \"\"\"
"""

        # Check if it exists
        if download_function in content:
            # Improved download function
            new_download_function = """def download_file(filename):
    \"\"\"
    Serve files from the data directory for download
    This is used to provide downloadable Excel exports of sector sentiment history
    \"\"\"
    # Ensure only files from the data directory can be downloaded
    if not filename.startswith('data/'):
        filename = os.path.join('data', filename)
    
    # Verify file exists
    if not os.path.exists(filename):
        # Try to generate the export files if they don't exist
        if 'sector_sentiment_history' in filename:
            try:
                import fix_sector_export
                fix_sector_export.create_export_files()
                if not os.path.exists(filename):
                    return "File not found or could not be generated"
            except Exception as e:
                return f"Error generating export: {str(e)}"
                
    # Determine content type based on extension
    if filename.endswith('.xlsx'):
        mime_type = 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
    elif filename.endswith('.csv'):
        mime_type = 'text/csv'
    elif filename.endswith('.json'):
        mime_type = 'application/json'
    else:
        mime_type = 'application/octet-stream'
    
    return flask.send_file(filename, mimetype=mime_type, as_attachment=True)
"""
            
            # Replace the function
            updated_content = content.replace(download_function, new_download_function)
            
            # Write back to the file if it changed
            if content != updated_content:
                with open('app.py', 'w') as f:
                    f.write(updated_content)
                print("Updated download_file function in app.py")
            else:
                print("Download function already updated or not found")
                
        else:
            print("Download function not found in app.py")
            
    except Exception as e:
        print(f"Error updating app.py: {e}")

def create_30day_market_cap_table():
    """Create a 30-day market cap table for all sectors"""
    try:
        # Find market cap history files
        market_cap_files = []
        for root, _, files in os.walk('.'):
            for file in files:
                if 'market' in file.lower() and 'cap' in file.lower() and ('history' in file.lower() or '30day' in file.lower()):
                    if file.endswith('.csv') or file.endswith('.parquet'):
                        market_cap_files.append(os.path.join(root, file))
        
        print(f"Found {len(market_cap_files)} market cap history files")
        
        # Load data from each file
        sector_data = {}
        
        for file_path in market_cap_files:
            try:
                if file_path.endswith('.csv'):
                    df = pd.read_csv(file_path)
                elif file_path.endswith('.parquet'):
                    df = pd.read_parquet(file_path)
                else:
                    continue
                
                # Check if this is a sector market cap file
                if 'date' in df.columns and 'market_cap' in df.columns and 'sector' in df.columns:
                    print(f"Loading market cap data from {file_path}")
                    
                    # Process each sector
                    for sector, group in df.groupby('sector'):
                        if sector not in sector_data:
                            sector_data[sector] = {}
                        
                        # Add market cap for each date
                        for _, row in group.iterrows():
                            date_str = str(row['date']).split(' ')[0]  # Just keep YYYY-MM-DD part
                            market_cap = row['market_cap']
                            sector_data[sector][date_str] = market_cap
            except Exception as e:
                print(f"Error processing {file_path}: {e}")
        
        if not sector_data:
            print("No market cap data found")
            return
        
        # Get all unique dates
        all_dates = set()
        for sector in sector_data:
            all_dates.update(sector_data[sector].keys())
        
        # Convert to sorted list
        all_dates = sorted(list(all_dates), reverse=True)
        
        # Limit to 30 days
        if len(all_dates) > 30:
            all_dates = all_dates[:30]
        
        # Get all unique sectors
        all_sectors = sorted(list(sector_data.keys()))
        
        # Create the table
        table_rows = []
        
        # Add header row
        header_row = ['Date'] + all_sectors
        table_rows.append(header_row)
        
        # Add data rows
        for date in all_dates:
            row = [date]
            for sector in all_sectors:
                if sector in sector_data and date in sector_data[sector]:
                    # Format as trillions with 2 decimal places
                    market_cap = sector_data[sector][date]
                    market_cap_t = market_cap / 1_000_000_000_000  # Convert to trillions
                    row.append(f"{market_cap_t:.2f}T")
                else:
                    row.append("N/A")
            table_rows.append(row)
        
        # Convert to DataFrame
        df = pd.DataFrame(table_rows[1:], columns=table_rows[0])
        
        # Save to CSV
        csv_file = 'data/sector_marketcap_30day_table.csv'
        df.to_csv(csv_file, index=False)
        print(f"Created 30-day market cap table: {csv_file}")
        
        # Save to Excel
        excel_file = 'data/sector_marketcap_30day_table.xlsx'
        df.to_excel(excel_file, index=False)
        print(f"Created 30-day market cap table: {excel_file}")
        
        return {
            'csv': csv_file,
            'excel': excel_file,
            'sectors': len(all_sectors),
            'dates': len(all_dates)
        }
    except Exception as e:
        print(f"Error creating market cap table: {e}")
        return None

def main():
    """Main function to fix sector export functionality"""
    # Create export files
    export_result = create_export_files()
    
    # Fix download function
    fix_download_function_in_app()
    
    # Create market cap table
    market_cap_table = create_30day_market_cap_table()
    
    print("\nSector export fix completed successfully!")
    if export_result:
        print(f"Created exports with {export_result['record_count']} records")
    if market_cap_table:
        print(f"Created market cap table with {market_cap_table['sectors']} sectors over {market_cap_table['dates']} days")

if __name__ == "__main__":
    main()