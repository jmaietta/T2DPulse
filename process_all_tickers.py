#!/usr/bin/env python3
# process_all_tickers.py
# -----------------------------------------------------------
# Script to process all remaining tickers sector by sector

import os
import sys
import time
import subprocess
import argparse

def process_all_sectors(max_per_sector=2, break_seconds=10):
    """Process all sectors, with a specified number of tickers per sector
    
    Args:
        max_per_sector (int): Maximum number of tickers to process per sector
        break_seconds (int): Number of seconds to wait between sectors to avoid API rate limits
    """
    # First, get the current coverage to see which sectors need work
    print("Getting current sector coverage...")
    result = subprocess.run(["python", "process_sector_tickers.py", "--list"], 
                            capture_output=True, text=True)
    
    if result.returncode != 0:
        print(f"Error getting sector coverage: {result.stderr}")
        return False
    
    print("\nCurrent sector coverage:")
    print(result.stdout)
    
    # Parse the output to get sector coverage
    sectors = []
    for line in result.stdout.split('\n'):
        if ': ' in line and 'Missing: ' in line:
            # Extract sector name
            sector_name = line.split(':')[0]
            
            # Check if it's not the "Overall" line
            if sector_name != "Overall":
                # Extract coverage percentage
                coverage_str = line.split(': ')[1].split(' ')[0]
                coverage_pct = float(coverage_str.replace('%', ''))
                
                # Extract missing count
                missing_str = line.split('Missing: ')[1].split(' ')[0]
                missing_count = int(missing_str)
                
                sectors.append((sector_name, coverage_pct, missing_count))
    
    # Sort sectors by coverage percentage (lowest first)
    sectors.sort(key=lambda x: x[1])
    
    print("\nProcessing sectors in order of lowest coverage:")
    for sector_name, coverage_pct, missing_count in sectors:
        if missing_count == 0:
            print(f"Skipping {sector_name} - already at 100% coverage")
            continue
        
        # Determine how many tickers to process
        tickers_to_process = min(max_per_sector, missing_count)
        
        print(f"\nProcessing {sector_name} ({coverage_pct:.1f}% coverage, {missing_count} missing)")
        print(f"Will process {tickers_to_process} tickers...")
        
        # Run the process_sector_tickers.py script for this sector
        cmd = ["python", "process_sector_tickers.py", "--sector", sector_name, "--max", str(tickers_to_process)]
        print(f"Running command: {' '.join(cmd)}")
        
        try:
            process = subprocess.Popen(cmd)
            
            # Wait for process to complete with a timeout
            timeout_seconds = 120  # 2 minutes timeout per sector batch
            try:
                process.wait(timeout=timeout_seconds)
            except subprocess.TimeoutExpired:
                print(f"Process timed out after {timeout_seconds} seconds, but likely still running.")
                print("Continuing to next sector...")
            
            # Before moving to the next sector, check the updated coverage
            print("\nChecking updated coverage for this sector...")
            result = subprocess.run(["python", "process_sector_tickers.py", "--list"], 
                                   capture_output=True, text=True)
            if result.returncode == 0:
                print("Updated sector coverage:")
                print(result.stdout)
            
            # Sleep to avoid API rate limits
            print(f"Waiting {break_seconds} seconds before processing next sector...")
            time.sleep(break_seconds)
        
        except Exception as e:
            print(f"Error processing sector {sector_name}: {e}")
            continue
    
    print("\nAll sectors have been processed!")
    
    # Get final coverage
    print("\nGetting final sector coverage...")
    result = subprocess.run(["python", "process_sector_tickers.py", "--list"], 
                           capture_output=True, text=True)
    
    if result.returncode == 0:
        print("\nFinal sector coverage:")
        print(result.stdout)
    
    return True

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Process all remaining tickers sector by sector')
    parser.add_argument('--max', type=int, default=2, help='Maximum number of tickers to process per sector')
    parser.add_argument('--break', type=int, dest='break_seconds', default=10, 
                        help='Number of seconds to wait between sectors')
    args = parser.parse_args()
    
    print(f"Starting to process all sectors with max={args.max} tickers per sector")
    print(f"and a {args.break_seconds} second break between sectors...")
    
    success = process_all_sectors(max_per_sector=args.max, break_seconds=args.break_seconds)
    
    if success:
        print("Successfully processed all sectors!")
        sys.exit(0)
    else:
        print("Errors occurred during processing.")
        sys.exit(1)