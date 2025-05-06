import time
import pytz
import os
import threading
from datetime import datetime, timedelta

# Use Eastern time zone for all timestamps
eastern = pytz.timezone('US/Eastern')

# First, initialize data in parallel to speed up startup
print(f"Starting T2D Pulse Dashboard initialization at {datetime.now(eastern).strftime('%Y-%m-%d %H:%M:%S')} EDT")
start_time = time.time()

# Import and run the parallel data fetcher first
print("Initializing data sources in parallel...")
from parallel_data_fetcher import initialize_data

# Fetch all data in parallel before starting the app
print("Fetching economic data in parallel...")
data = initialize_data()
data_fetch_time = time.time() - start_time
print(f"Economic data fetched in {data_fetch_time:.2f} seconds")

# Store the data in the cache
print("Storing data in cache...")
from data_cache import update_cache
update_cache(data)

# Now import the app (this will use the data we've already fetched from the cache)
from app import app

# Generate historical sector sentiment data for trend charts
print("Updating historical sector sentiment data...")
import update_historical_data

# For improved startup time, we'll import the modules but NOT run the updates here
# These will be run on a background thread after the app starts
print("Preparing historical data modules...")
import authentic_sector_history
print("Imported authentic_sector_history module")

# Process data asynchronously after the app starts
import threading

def update_historical_data_async():
    """Update historical data in a background thread"""
    print("Background thread started for historical data generation")
    
    try:
        # First update the authentic historical data (highest priority)
        print("Generating authentic historical sector sentiment data...")
        authentic_sector_history.update_authentic_history()
        print("Authentic historical data generation complete")
    except Exception as e:
        print(f"Error generating authentic sector history: {e}")
        
        # As fallback, update the real historical data
        try:
            print("Falling back to original historical sector data...")
            update_historical_data.update_real_historical_data()
            print("Original historical data update complete")
        except Exception as e2:
            print(f"Error updating original historical data: {e2}")

# Function to run the daily sector data collection
def run_daily_sector_data_collection():
    """Run the daily sector data collection once a day"""
    print("Starting daily sector data collection thread")
    while True:
        try:
            # First, check if we should run today
            today = datetime.now(eastern).strftime("%Y-%m-%d")
            last_run_path = "data/last_sector_run.txt"
            
            # Check when we last ran
            last_run_date = None
            if os.path.exists(last_run_path):
                with open(last_run_path, "r") as f:
                    last_run_date = f.read().strip()
                    
            # Only run if we haven't run today
            if last_run_date != today:
                # Import and run the daily collection
                print(f"Running daily sector data collection for {today}...")
                try:
                    from run_daily import main as run_daily_main
                    success = run_daily_main()
                    if success:
                        # Record that we ran today
                        with open(last_run_path, "w") as f:
                            f.write(today)
                        print(f"Successfully ran daily sector data collection for {today}")
                    else:
                        print(f"Daily sector data collection failed for {today}")
                except Exception as e:
                    print(f"Error running daily sector data collection: {e}")
            else:
                print(f"Daily sector data already collected for {today}, skipping")
                
            # Sleep until tomorrow (check every hour just to be safe)
            time.sleep(3600)  # 1 hour
        except Exception as e:
            print(f"Error in daily sector data collection thread: {e}")
            time.sleep(3600)  # Wait an hour and try again

# Start the background thread to update historical data without blocking the app
history_thread = threading.Thread(target=update_historical_data_async)
history_thread.daemon = True  # Thread will exit when main thread exits
print("Starting background thread for historical data updates")
history_thread.start()

# Start the daily sector data collection thread
sector_thread = threading.Thread(target=run_daily_sector_data_collection)
sector_thread.daemon = True  # Thread will exit when main thread exits
print("Starting background thread for daily sector data collection")
sector_thread.start()

# Start the auto-refresh thread at 5:00pm ET
from app import auto_refresh_data
auto_refresh_thread = threading.Thread(target=auto_refresh_data, daemon=True)
auto_refresh_thread.daemon = True  # Thread will exit when main thread exits
print("Starting auto-refresh thread to update data at 5:00pm ET daily")
auto_refresh_thread.start()

# Start the server
print(f"Starting T2D Pulse server at {datetime.now(eastern).strftime('%Y-%m-%d %H:%M:%S')} EDT")
print(f"Total initialization time: {time.time() - start_time:.2f} seconds")

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=False)
