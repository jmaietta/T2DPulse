import time
from datetime import datetime

# First, initialize data in parallel to speed up startup
print(f"Starting T2D Pulse Dashboard initialization at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
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

# Start the background thread to update historical data without blocking the app
history_thread = threading.Thread(target=update_historical_data_async)
history_thread.daemon = True  # Thread will exit when main thread exits
print("Starting background thread for historical data updates")
history_thread.start()

# Start the server
print(f"Starting T2D Pulse server at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print(f"Total initialization time: {time.time() - start_time:.2f} seconds")

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=False)
