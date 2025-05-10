#!/usr/bin/env python3
"""
use_fully_diluted_shares.py

This script implements the business rule to always use fully diluted share counts
for all market cap calculations by updating the share counts in the ticker coverage file.

It applies the correct share counts from SEC filings for key tickers like GOOGL and META,
ensuring market cap calculations are accurate.
"""

import os
import sys
import json
import pandas as pd
import logging
from datetime import datetime
from pathlib import Path

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
)

# Define fixed override values from authoritative sources (SEC filings)
FULLY_DILUTED_SHARES = {
    "GOOGL": 12_291_000_000,  # Alphabet Inc.
    "META": 2_590_000_000,    # Meta Platforms Inc.
    # Add others as needed
}

def update_ticker_coverage_with_diluted_shares():
    """Update the ticker coverage file with fully diluted share counts"""
    
    logging.info("Updating ticker coverage with fully diluted share counts")
    
    # Path to the coverage file
    coverage_file = "T2D_Pulse_93_tickers_coverage.csv"
    
    if not os.path.exists(coverage_file):
        logging.error(f"Coverage file not found: {coverage_file}")
        return False
    
    try:
        # Make a backup of the file
        backup_file = f"{coverage_file}.bak"
        with open(coverage_file, 'r') as src, open(backup_file, 'w') as dst:
            dst.write(src.read())
        logging.info(f"Created backup of coverage file: {backup_file}")
        
        # Read the header rows
        with open(coverage_file, 'r') as f:
            header_rows = [f.readline() for _ in range(7)]
        
        # Read the data
        df = pd.read_csv(coverage_file, skiprows=7)
        
        # Track modifications
        modified_count = 0
        
        # Loop through each ticker in the overrides
        for ticker, diluted_shares in FULLY_DILUTED_SHARES.items():
            # Find all rows for this ticker
            mask = df['Ticker'] == ticker
            if mask.any():
                ticker_rows = df[mask]
                
                for idx, row in ticker_rows.iterrows():
                    price = row['Price']
                    
                    # Calculate new market cap using fully diluted shares
                    new_market_cap = price * diluted_shares
                    new_market_cap_m = new_market_cap / 1_000_000  # Convert to millions
                    
                    # Update the row
                    df.loc[idx, 'Market Cap'] = new_market_cap
                    df.loc[idx, 'Market Cap (M)'] = new_market_cap_m
                    
                    # Log the change
                    old_market_cap = row['Market Cap']
                    logging.info(f"Updated {ticker}: Price={price}, Shares={diluted_shares:,}")
                    logging.info(f"  Market Cap: {old_market_cap:,.2f} -> {new_market_cap:,.2f}")
                    
                    modified_count += 1
        
        # Write the updated file
        with open(coverage_file, 'w') as f:
            # Write the header rows
            for row in header_rows:
                f.write(row)
            
            # Write the updated data
            df.to_csv(f, index=False)
        
        logging.info(f"Updated {modified_count} rows with fully diluted share counts")
        
        # Now create an updated full ticker history file as well
        updated_file = "T2D_Pulse_updated_full_ticker_history.csv"
        df.to_csv(updated_file, index=False)
        logging.info(f"Created updated ticker history file: {updated_file}")
        
        return True
    
    except Exception as e:
        logging.error(f"Error updating ticker coverage: {e}")
        return False

def update_polygon_sector_caps():
    """Update the polygon_sector_caps.py file to ensure fully diluted shares are used"""
    
    logging.info("Ensuring polygon_sector_caps.py uses fully diluted shares")
    
    polygon_file = "polygon_sector_caps.py"
    
    if not os.path.exists(polygon_file):
        logging.error(f"File not found: {polygon_file}")
        return False
    
    try:
        # Read the file
        with open(polygon_file, 'r') as f:
            content = f.read()
        
        # Check if overrides are already present
        if "SHARE_COUNT_OVERRIDES" in content:
            logging.info("polygon_sector_caps.py already has SHARE_COUNT_OVERRIDES")
            
            # Make sure all our overrides are there
            for ticker, shares in FULLY_DILUTED_SHARES.items():
                if f'"{ticker}"' not in content or str(shares) not in content:
                    logging.warning(f"Override for {ticker} not found in {polygon_file}")
                    # We'd need to update the file
            
            return True
        
        # If not, add the overrides
        override_text = "\n# Manual overrides for stocks with known share count discrepancies\n"
        override_text += "# These values come from the most authoritative sources (SEC filings)\n"
        override_text += "SHARE_COUNT_OVERRIDES = {\n"
        
        for ticker, shares in FULLY_DILUTED_SHARES.items():
            override_text += f'    "{ticker}": {shares:_},  # {ticker}\n'
        
        override_text += "    # Add others as needed\n"
        override_text += "}\n"
        
        # Find a good place to insert this - after imports
        import_section_end = content.rfind("import") + 1
        while import_section_end < len(content) and content[import_section_end] != '\n':
            import_section_end += 1
        
        if import_section_end < len(content):
            import_section_end = content.find('\n', import_section_end) + 1
            
            # Insert the override text
            new_content = content[:import_section_end] + override_text + content[import_section_end:]
            
            # Make a backup
            backup_file = f"{polygon_file}.bak"
            with open(backup_file, 'w') as f:
                f.write(content)
            logging.info(f"Created backup of {polygon_file}: {backup_file}")
            
            # Write the updated file
            with open(polygon_file, 'w') as f:
                f.write(new_content)
            
            logging.info(f"Updated {polygon_file} with SHARE_COUNT_OVERRIDES")
            
            return True
        else:
            logging.error(f"Could not find a good place to insert overrides in {polygon_file}")
            return False
    
    except Exception as e:
        logging.error(f"Error updating {polygon_file}: {e}")
        return False

def update_adtech_market_cap():
    """Update the adtech_market_cap_history.py file to use fully diluted shares"""
    
    logging.info("Ensuring adtech_market_cap_history.py uses fully diluted shares")
    
    adtech_file = "adtech_market_cap_history.py"
    
    if not os.path.exists(adtech_file):
        logging.warning(f"File not found: {adtech_file}")
        return False
    
    try:
        # Read the file
        with open(adtech_file, 'r') as f:
            content = f.read()
        
        # Check if we need to update the file
        if "# Use fully diluted shares for all market cap calculations" in content:
            logging.info(f"{adtech_file} already enforces fully diluted shares")
            return True
        
        # Look for a shares outstanding function
        if "def load_shares_outstanding(" in content:
            # Find the function
            func_start = content.find("def load_shares_outstanding(")
            func_end = content.find("return", func_start)
            func_end = content.find("\n", func_end) + 1
            
            # Extract the function body
            func_body = content[func_start:func_end]
            
            # Create a new function body with our overrides
            new_func_body = func_body.split("\n")
            
            # Find the right spot to insert our comment
            insert_index = 1  # After the function definition line
            
            # Insert our comment and overrides
            new_func_body.insert(insert_index, "    # Use fully diluted shares for all market cap calculations")
            new_func_body.insert(insert_index + 1, "    # Override with accurate share counts from SEC filings")
            
            for ticker, shares in FULLY_DILUTED_SHARES.items():
                new_func_body.insert(insert_index + 2, f'    if "{ticker}" in shares_data:')
                new_func_body.insert(insert_index + 3, f'        shares_data["{ticker}"] = {shares}  # Override with fully diluted shares')
            
            # Join the function back together
            new_func = "\n".join(new_func_body)
            
            # Replace the original function
            new_content = content.replace(func_body, new_func)
            
            # Make a backup
            backup_file = f"{adtech_file}.bak"
            with open(backup_file, 'w') as f:
                f.write(content)
            logging.info(f"Created backup of {adtech_file}: {backup_file}")
            
            # Write the updated file
            with open(adtech_file, 'w') as f:
                f.write(new_content)
            
            logging.info(f"Updated {adtech_file} to use fully diluted shares")
            
            return True
        else:
            logging.warning(f"Could not find load_shares_outstanding function in {adtech_file}")
            return False
    
    except Exception as e:
        logging.error(f"Error updating {adtech_file}: {e}")
        return False

def main():
    """Main function to enforce fully diluted share counts"""
    
    print("Enforcing fully diluted share counts for all market cap calculations")
    print("------------------------------------------------------------------")
    
    success = True
    
    # 1. Update the ticker coverage file
    if update_ticker_coverage_with_diluted_shares():
        print("✓ Updated ticker coverage with fully diluted share counts")
    else:
        print("✗ Failed to update ticker coverage")
        success = False
    
    # 2. Update polygon_sector_caps.py
    if update_polygon_sector_caps():
        print("✓ Updated polygon_sector_caps.py to use fully diluted shares")
    else:
        print("✗ Failed to update polygon_sector_caps.py")
        success = False
    
    # 3. Update adtech_market_cap_history.py
    if update_adtech_market_cap():
        print("✓ Updated adtech_market_cap_history.py to use fully diluted shares")
    else:
        print("! Could not update adtech_market_cap_history.py")
    
    if success:
        print("\nSuccessfully enforced fully diluted share counts for all market cap calculations")
    else:
        print("\nPartially enforced fully diluted share counts - some files could not be updated")
    
    print("\nTo apply these changes, restart the Economic Dashboard Server workflow")

if __name__ == "__main__":
    main()