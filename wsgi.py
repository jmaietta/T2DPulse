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

# First update the real historical data (using CSV files)
print("Updating original historical sector data...")
update_historical_data.update_real_historical_data()

# Then update the authentic historical data (using direct API calls for business days)
print("Updating authentic historical data for business days only...")
update_historical_data.update_authentic_historical_data()

# Start the server
print(f"Starting T2D Pulse server at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print(f"Total initialization time: {time.time() - start_time:.2f} seconds")

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=False)
