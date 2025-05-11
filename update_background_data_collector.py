"""
Update background_data_collector.py to use the new sector_market_cap.py script
This allows the background data collector to trigger market cap updates.
"""

import os
import datetime as dt
import shutil
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

# Check if necessary files exist
if not os.path.exists('sector_market_cap.py'):
    logging.error("sector_market_cap.py not found. Please create it first.")
    exit(1)

if not os.path.exists('background_data_collector.py'):
    logging.error("background_data_collector.py not found.")
    exit(1)

# Create backup of background_data_collector.py
backup_file = f"background_data_collector_backup_{dt.datetime.now().strftime('%Y%m%d_%H%M%S')}.py"
shutil.copy2('background_data_collector.py', backup_file)
logging.info(f"Created backup of background_data_collector.py at {backup_file}")

# Read the content of background_data_collector.py
with open('background_data_collector.py', 'r') as f:
    content = f.read()

# Add import for sector_market_cap if it doesn't exist
if 'import sector_market_cap' not in content:
    # Find the last import statement
    import_lines = [line for line in content.split('\n') if line.startswith('import') or line.startswith('from')]
    
    if import_lines:
        last_import = import_lines[-1]
        last_import_pos = content.find(last_import) + len(last_import)
        
        # Add our import after the last import
        new_import = "\nimport sector_market_cap  # Market cap calculation engine"
        content = content[:last_import_pos] + new_import + content[last_import_pos:]
        
        logging.info("Added sector_market_cap import to background_data_collector.py")
    else:
        logging.warning("Could not find import statements in background_data_collector.py")

# Add function to update market caps if it doesn't exist
if 'def update_sector_market_caps' not in content:
    market_cap_function = """
def update_sector_market_caps():
    \"\"\"Update sector market caps using the new calculation engine\"\"\"
    logging.info("Updating sector market caps using sector_market_cap.py")
    try:
        # Calculate today's sector market caps
        today_df = sector_market_cap.calculate_sector_caps(sector_market_cap.SECTORS)
        
        # Append to historical data
        hist_df = sector_market_cap.append_to_csv(today_df)
        
        # Generate interactive chart
        chart_path = sector_market_cap.chart_sector_caps()
        
        logging.info(f"Updated sector market caps: {len(today_df)} sectors")
        return True
    except Exception as e:
        logging.error(f"Error updating sector market caps: {e}")
        return False
"""
    
    # Find a good insertion point - before 'if __name__ == "__main__"'
    insertion_point = content.find('if __name__ == "__main__"')
    
    if insertion_point != -1:
        content = content[:insertion_point] + market_cap_function + "\n\n" + content[insertion_point:]
        logging.info("Added update_sector_market_caps function to background_data_collector.py")
    else:
        # Find a good insertion point - at the end of the file
        content += "\n\n" + market_cap_function
        logging.info("Added update_sector_market_caps function to the end of background_data_collector.py")

# Update the main execution logic to call update_sector_market_caps
if 'def main(' in content:
    # Find the main function
    main_start = content.find('def main(')
    if main_start != -1:
        # Find where we should add our code
        market_cap_call = "update_sector_market_caps()"
        if market_cap_call not in content[main_start:]:
            # Look for a good place to insert our call - before the end of the function
            main_body_start = content.find(':', main_start)
            if main_body_start != -1:
                main_body_start += 1  # Move past the colon
                
                # Find a good point to insert the call - usually right after data collection code
                # Several possible patterns to look for
                patterns = [
                    "collect_market_data",
                    "collect_ticker_data",
                    "collect_economic_data",
                    "logging.info(\"Data collection completed\")"
                ]
                
                insert_pos = -1
                for pattern in patterns:
                    pattern_pos = content.find(pattern, main_body_start)
                    if pattern_pos != -1:
                        # Find the end of that line
                        line_end = content.find('\n', pattern_pos)
                        if line_end != -1:
                            insert_pos = line_end + 1
                            break
                
                if insert_pos != -1:
                    # Insert our market cap update call
                    market_cap_code = "    # Update sector market caps\n    update_sector_market_caps()\n"
                    content = content[:insert_pos] + market_cap_code + content[insert_pos:]
                    logging.info("Added sector market cap update call to main function")
                else:
                    logging.warning("Could not find a good insertion point in main function")
            else:
                logging.warning("Could not find main function body")
        else:
            logging.info("update_sector_market_caps() call already exists in main function")
    else:
        logging.warning("Could not find main function")
else:
    logging.warning("No main function found in background_data_collector.py")

# Write the updated file
with open('background_data_collector.py', 'w') as f:
    f.write(content)

print("\n")
print("=" * 80)
print("BACKGROUND DATA COLLECTOR UPDATE COMPLETE")
print("=" * 80)
print(f"✓ Created backup of background_data_collector.py at {backup_file}")
print("✓ Added sector_market_cap import")
print("✓ Added update_sector_market_caps function")
print("✓ Updated main function to call update_sector_market_caps")
print("\nThe background data collector will now update sector market caps automatically.")
print("=" * 80)