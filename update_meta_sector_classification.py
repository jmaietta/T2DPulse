"""
Update META ticker to be included in multiple sectors as requested
"""
import os
import pandas as pd
import sys
import logging
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Sector assignments for META
META_SECTORS = ["AdTech", "Consumer Internet", "AI Infrastructure", "SMB SaaS"]

def update_ticker_classification():
    """Update the classification of tickers in the coverage report"""
    
    # Check if the coverage file exists
    coverage_file = "T2D_Pulse_93_tickers_coverage.csv"
    if not os.path.exists(coverage_file):
        logging.error(f"Coverage file not found: {coverage_file}")
        return False
    
    try:
        # Load the CSV, skipping the header rows
        df = pd.read_csv(coverage_file, skiprows=7)
        
        # Make a backup of the original file
        backup_file = f"{coverage_file}.bak"
        with open(coverage_file, 'r') as src, open(backup_file, 'w') as dst:
            dst.write(src.read())
        logging.info(f"Created backup of coverage file: {backup_file}")
        
        # Get the header rows
        with open(coverage_file, 'r') as f:
            header_rows = [f.readline() for _ in range(7)]
        
        # Create a new DataFrame for the updated entries
        new_entries = []
        
        # Check if META exists in the data
        meta_data = df[df['Ticker'] == 'META']
        googl_data = df[df['Ticker'] == 'GOOGL']
        
        if len(meta_data) == 0:
            logging.error("META ticker not found in the coverage data")
            return False
        
        # Check current sector for META
        current_sector = meta_data['Sector'].iloc[0]
        logging.info(f"Current META sector: {current_sector}")
        
        # Get the latest data for META
        meta_row = meta_data.iloc[0]
        date = meta_row['Date']
        price = meta_row['Price']
        market_cap = meta_row['Market Cap']
        data_status = meta_row['Data Status']
        market_cap_m = meta_row['Market Cap (M)']
        
        # Add entries for META in each sector
        for sector in META_SECTORS:
            if sector != current_sector:  # Skip current sector to avoid duplicates
                logging.info(f"Adding META to sector: {sector}")
                new_entry = {
                    'Date': date,
                    'Ticker': 'META',
                    'Sector': sector,
                    'Price': price,
                    'Market Cap': market_cap,
                    'Data Status': data_status,
                    'Market Cap (M)': market_cap_m
                }
                new_entries.append(new_entry)
        
        # Check if GOOGL exists and update its sectors too
        if len(googl_data) > 0:
            current_sector = googl_data['Sector'].iloc[0]
            logging.info(f"Current GOOGL sector: {current_sector}")
            
            # Get the latest data for GOOGL
            googl_row = googl_data.iloc[0]
            date = googl_row['Date']
            price = googl_row['Price']
            market_cap = googl_row['Market Cap']
            data_status = googl_row['Data Status']
            market_cap_m = googl_row['Market Cap (M)']
            
            # Add entries for GOOGL in AdTech, Consumer Internet, and AI Infrastructure
            googl_sectors = ["AdTech", "Consumer Internet", "AI Infrastructure"]
            for sector in googl_sectors:
                if sector != current_sector:  # Skip current sector to avoid duplicates
                    logging.info(f"Adding GOOGL to sector: {sector}")
                    new_entry = {
                        'Date': date,
                        'Ticker': 'GOOGL',
                        'Sector': sector,
                        'Price': price,
                        'Market Cap': market_cap,
                        'Data Status': data_status,
                        'Market Cap (M)': market_cap_m
                    }
                    new_entries.append(new_entry)
        
        # Add the new entries to the DataFrame
        if new_entries:
            new_df = pd.DataFrame(new_entries)
            updated_df = pd.concat([df, new_df], ignore_index=True)
            
            # Sort by sector and ticker
            updated_df = updated_df.sort_values(['Sector', 'Ticker'])
            
            # Update the summary header
            header_rows[4] = f"Overall Coverage: {len(updated_df)}/{len(updated_df)} tickers (100.0%)\n"
            
            # Write the updated file
            with open(coverage_file, 'w') as f:
                # Write header rows
                for row in header_rows:
                    f.write(row)
                
                # Write data rows
                updated_df.to_csv(f, index=False)
            
            logging.info(f"Updated {coverage_file} with META in multiple sectors")
            
            # Create a new full ticker history file to reflect these changes
            updated_file = "T2D_Pulse_updated_full_ticker_history.csv"
            updated_df.to_csv(updated_file, index=False)
            logging.info(f"Created updated ticker history file: {updated_file}")
            
            return True
        else:
            logging.warning("No new entries were created")
            return False
        
    except Exception as e:
        logging.error(f"Error updating ticker classification: {e}")
        return False

def update_sector_weights():
    """Update sector weight calculations to reflect updated ticker assignments"""
    # Path to the sector weights file
    weights_file = "data/sector_weights_latest.json"
    
    if not os.path.exists(weights_file):
        logging.error(f"Sector weights file not found: {weights_file}")
        return False
    
    try:
        # We'll recalculate using the market cap data in the coverage file
        coverage_file = "T2D_Pulse_93_tickers_coverage.csv"
        df = pd.read_csv(coverage_file, skiprows=7)
        
        # Calculate total market cap for each sector
        sector_mcaps = df.groupby('Sector')['Market Cap (M)'].sum().reset_index()
        total_mcap = sector_mcaps['Market Cap (M)'].sum()
        
        # Calculate sector weights as percentage of total
        sector_weights = {}
        for _, row in sector_mcaps.iterrows():
            sector = row['Sector'].replace(' ', '_')  # Match format in the weights file
            mcap = row['Market Cap (M)']
            weight = (mcap / total_mcap) * 100
            sector_weights[sector] = round(weight, 2)
        
        logging.info("Calculated updated sector weights:")
        for sector, weight in sorted(sector_weights.items(), key=lambda x: x[1], reverse=True):
            logging.info(f"{sector}: {weight:.2f}%")
        
        # Create updated weights object
        weights_obj = {"weights": sector_weights}
        
        # Save to JSON file
        import json
        with open(weights_file, 'w') as f:
            json.dump(weights_obj, f, indent=2)
        
        logging.info(f"Updated sector weights saved to {weights_file}")
        return True
    
    except Exception as e:
        logging.error(f"Error updating sector weights: {e}")
        return False

if __name__ == "__main__":
    print("Updating META sector classification...")
    
    if update_ticker_classification():
        print("Successfully updated META sector classification")
        
        # Update sector weights
        if update_sector_weights():
            print("Successfully updated sector weights")
        else:
            print("Failed to update sector weights")
    else:
        print("Failed to update META sector classification")
    
    # Print summary  
    print("\nTo finalize these changes, restart the Economic Dashboard Server workflow")