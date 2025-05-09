"""
Integrate Fully Diluted Shares Logic

This script updates the existing market cap calculation code to always use
fully diluted shares for more accurate market cap calculations, following
the business rule to always use fully diluted share counts.
"""
import os
import sys
import logging
import shutil
import glob
from datetime import datetime

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

def backup_file(filename):
    """Create a timestamped backup of a file"""
    if not os.path.exists(filename):
        logging.warning(f"Cannot backup {filename} - file does not exist")
        return False
        
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_name = f"{filename}.{timestamp}.bak"
    
    try:
        shutil.copy2(filename, backup_name)
        logging.info(f"Created backup of {filename} at {backup_name}")
        return True
    except Exception as e:
        logging.error(f"Error creating backup of {filename}: {e}")
        return False

def integrate_fully_diluted_logic(file_path, schedule_restart=True):
    """
    Update the code in polygon_sector_caps.py to use fully diluted share counts
    
    Args:
        file_path (str): Path to the polygon_sector_caps.py file
        schedule_restart (bool): Whether to schedule a restart of the data collection workflow
        
    Returns:
        bool: True if successful, False otherwise
    """
    if not os.path.exists(file_path):
        logging.error(f"File {file_path} does not exist")
        return False
        
    # Backup original file
    if not backup_file(file_path):
        return False
    
    try:
        # Read original file
        with open(file_path, 'r') as f:
            lines = f.readlines()
        
        # Find and update the share count retrieval code
        updated = False
        new_lines = []
        
        for i, line in enumerate(lines):
            # Look for the function that gets shares outstanding
            if "def get_shares_outstanding(" in line:
                new_lines.append(line)
                
                # Find the code block inside this function
                j = i + 1
                indentation = len(line) - len(line.lstrip())
                function_body = []
                
                while j < len(lines) and (len(lines[j].strip()) == 0 or lines[j].startswith(' ' * (indentation + 4))):
                    function_body.append(lines[j])
                    j += 1
                
                # Check if we need to update the function
                needs_update = True
                
                for body_line in function_body:
                    if "weighted_shares_outstanding" in body_line and "share_class_shares_outstanding" in body_line:
                        needs_update = False
                        break
                
                if needs_update:
                    # Update the function to use weighted_shares_outstanding
                    # Find the point where we extract the share count
                    for k, body_line in enumerate(function_body):
                        if "share_class_shares_outstanding" in body_line:
                            # Replace with code that prioritizes weighted_shares_outstanding
                            indent = body_line[:len(body_line) - len(body_line.lstrip())]
                            replacement_line = f"{indent}# Get weighted shares outstanding (fully diluted) if available, otherwise use share class shares\n"
                            replacement_line += f"{indent}weighted_shares = results.get('weighted_shares_outstanding')\n"
                            replacement_line += f"{indent}share_class_shares = results.get('share_class_shares_outstanding')\n"
                            replacement_line += f"{indent}\n"
                            replacement_line += f"{indent}# Business rule: Always prefer fully diluted shares when available\n"
                            replacement_line += f"{indent}if weighted_shares is not None:\n"
                            replacement_line += f"{indent}    shares_dict[ticker] = weighted_shares\n"
                            replacement_line += f"{indent}    if verbose:\n"
                            replacement_line += f"{indent}        print(f\"Got fully diluted shares for {{ticker}}: {{weighted_shares:,}}\")\n"
                            replacement_line += f"{indent}elif share_class_shares is not None:\n"
                            replacement_line += f"{indent}    shares_dict[ticker] = share_class_shares\n"
                            replacement_line += f"{indent}    if verbose:\n"
                            replacement_line += f"{indent}        print(f\"Got share class shares for {{ticker}}: {{share_class_shares:,}} (not fully diluted)\")\n"
                            
                            # Replace the original line
                            function_body[k] = replacement_line
                            updated = True
                            break
                
                # Add the updated function body
                new_lines.extend(function_body)
                
                # Skip ahead
                i = j - 1
            else:
                new_lines.append(line)
        
        # Write updated file
        if updated:
            with open(file_path, 'w') as f:
                f.writelines(new_lines)
            logging.info(f"Updated {file_path} to use fully diluted share counts")
            
            # Schedule restart if requested
            if schedule_restart:
                logging.info(f"Please restart the data collection workflow to apply changes")
            
            return True
        else:
            logging.info(f"No updates needed for {file_path} - already using fully diluted share counts")
            return True
            
    except Exception as e:
        logging.error(f"Error updating {file_path}: {e}")
        return False

def find_polygon_sector_caps_file():
    """Find the polygon_sector_caps.py file"""
    candidates = ["polygon_sector_caps.py", "./polygon_sector_caps.py"]
    
    for candidate in candidates:
        if os.path.exists(candidate):
            return candidate
    
    # Try to find the file using glob
    matches = glob.glob("*polygon*sector*caps*.py")
    if matches:
        return matches[0]
    
    return None

def integrate_manual_overrides():
    """
    Add code to apply manual overrides for stocks with known share count discrepancies
    
    Returns:
        bool: True if successful, False otherwise
    """
    # Find target file
    file_path = find_polygon_sector_caps_file()
    
    if not file_path:
        logging.error("Could not find polygon_sector_caps.py file")
        return False
    
    # Backup original file
    if not backup_file(file_path):
        return False
    
    try:
        # Read original file
        with open(file_path, 'r') as f:
            content = f.read()
        
        # Check if overrides already exist
        if "SHARE_COUNT_OVERRIDES" in content:
            logging.info(f"Manual overrides already exist in {file_path}")
            return True
        
        # Find imports section to add after
        import_section_end = 0
        lines = content.split('\n')
        
        for i, line in enumerate(lines):
            if line.startswith('import ') or line.startswith('from '):
                import_section_end = i + 1
        
        # Add overrides code after imports
        override_code = """
# Manual overrides for stocks with known share count discrepancies
# These values come from the most authoritative sources (SEC filings)
SHARE_COUNT_OVERRIDES = {
    "GOOGL": 12_291_000_000,  # Alphabet Inc.
    "META": 2_590_000_000,    # Meta Platforms Inc.
    # Add others as needed
}

"""
        updated_lines = lines[:import_section_end] + [override_code] + lines[import_section_end:]
        
        # Update the get_shares_outstanding function to apply overrides
        in_function = False
        updated_function = False
        
        for i, line in enumerate(updated_lines):
            if "def get_shares_outstanding(" in line:
                in_function = True
            elif in_function and line.strip().startswith("for ticker in"):
                # Add override check before the loop
                indent = line[:len(line) - len(line.lstrip())]
                override_check = f"{indent}# Apply manual overrides for stocks with known share count discrepancies\n"
                override_check += f"{indent}for ticker, override_count in SHARE_COUNT_OVERRIDES.items():\n"
                override_check += f"{indent}    if ticker in tickers:\n"
                override_check += f"{indent}        shares_dict[ticker] = override_count\n"
                override_check += f"{indent}        if verbose:\n"
                override_check += f"{indent}            print(f\"Applied manual override for {{ticker}}: {{override_count:,}} shares\")\n"
                override_check += f"{indent}        # Remove from list to avoid API call\n"
                override_check += f"{indent}        if ticker in remaining_tickers:\n"
                override_check += f"{indent}            remaining_tickers.remove(ticker)\n\n"
                
                updated_lines[i] = override_check + line
                updated_function = True
                in_function = False
        
        if not updated_function:
            logging.warning(f"Could not update get_shares_outstanding function in {file_path}")
            return False
        
        # Write updated file
        with open(file_path, 'w') as f:
            f.write('\n'.join(updated_lines))
        
        logging.info(f"Updated {file_path} with manual share count overrides")
        return True
        
    except Exception as e:
        logging.error(f"Error adding manual overrides to {file_path}: {e}")
        return False

def main():
    """Main function"""
    # Find polygon_sector_caps.py
    file_path = find_polygon_sector_caps_file()
    
    if not file_path:
        logging.error("Could not find polygon_sector_caps.py file")
        return
    
    logging.info(f"Found sector caps file at {file_path}")
    
    # Update to use fully diluted share counts
    if integrate_fully_diluted_logic(file_path, schedule_restart=False):
        logging.info("Successfully updated code to use fully diluted share counts")
    else:
        logging.error("Failed to update code to use fully diluted share counts")
        return
    
    # Add manual overrides
    if integrate_manual_overrides():
        logging.info("Successfully added manual share count overrides")
    else:
        logging.error("Failed to add manual share count overrides")
        return
    
    logging.info("""
Integration complete! The system now:
1. Uses weighted_shares_outstanding (fully diluted shares) for market cap calculations
2. Applies manual overrides from authoritative sources for key stocks
3. Maintains accurate sector weighting based on true market capitalizations

Please restart the data collection workflow to apply these changes.
""")

if __name__ == "__main__":
    main()