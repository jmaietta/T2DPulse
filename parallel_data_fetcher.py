import concurrent.futures
import time
import pandas as pd
import os
import json

# Import the individual data fetching functions
from app import (
    fetch_fred_data,
    fetch_treasury_yield_data,
    fetch_vix_from_yahoo,
    fetch_nasdaq_with_ema,
    fetch_consumer_sentiment_data,
    load_data_from_csv,
    save_data_to_csv
)

# Define update frequencies for different data types
UPDATE_FREQUENCIES = {
    "gdp": "quarterly",          # GDP updates quarterly
    "unemployment": "monthly",   # Unemployment updates monthly
    "cpi": "monthly",           # CPI updates monthly
    "pcepi": "monthly",         # PCEPI updates monthly
    "interest_rate": "monthly", # Interest rate updates when Fed meets
    "pce": "monthly",           # PCE updates monthly
    "treasury_yield": "daily",  # Treasury yield updates daily
    "vix": "daily",             # VIX updates daily
    "nasdaq": "daily",          # NASDAQ updates daily
    "consumer_sentiment": "monthly"  # Consumer sentiment updates monthly
}

# Define series IDs for FRED data
FRED_SERIES = {
    "gdp": "GDPC1",              # Real GDP
    "unemployment": "UNRATE",    # Unemployment Rate
    "cpi": "CPIAUCSL",          # Consumer Price Index
    "pcepi": "PCEPI",           # Personal Consumption Expenditures Price Index
    "interest_rate": "FEDFUNDS", # Federal Funds Rate
    "pce": "PCE",               # Personal Consumption Expenditures
    "consumer_sentiment": "USACSCICP02STSAM" # Consumer Sentiment
}

# Define data file paths
DATA_FILES = {
    "gdp": "data/gdp_data.csv",
    "unemployment": "data/unemployment_data.csv",
    "cpi": "data/inflation_data.csv",
    "pcepi": "data/pcepi_data.csv",
    "interest_rate": "data/interest_rate_data.csv",
    "pce": "data/pce_data.csv",
    "treasury_yield": "data/treasury_yield_data.csv",
    "vix": "data/vix_data.csv",
    "nasdaq": "data/nasdaq_data.csv",
    "consumer_sentiment": "data/consumer_sentiment_data.csv"
}

# Function to determine if data needs to be refreshed based on frequency
def needs_refresh(data_type, df=None):
    """Determine if a dataset needs to be refreshed based on its frequency and last update"""
    # Always refresh daily data to ensure latest values
    if UPDATE_FREQUENCIES[data_type] == "daily":
        return True
    
    # If we don't have the dataframe, try to load it
    if df is None:
        try:
            df = load_data_from_csv(DATA_FILES[data_type])
        except Exception:
            # If we can't load the file, we definitely need to refresh
            return True
    
    # If dataframe is empty, refresh
    if df is None or df.empty:
        return True
    
    # Get current date and latest data date
    today = pd.Timestamp.now().normalize()
    try:
        latest_date = pd.to_datetime(df['date'].iloc[0])
    except Exception:
        return True
    
    # Calculate days since last update
    days_since_update = (today - latest_date).days
    
    # Determine refresh need based on update frequency
    if UPDATE_FREQUENCIES[data_type] == "monthly" and days_since_update < 25:
        return False
    elif UPDATE_FREQUENCIES[data_type] == "quarterly" and days_since_update < 80:
        return False
    
    return True

# Wrapper functions for each data type to handle loading and refreshing
def fetch_gdp_data():
    """Fetch GDP data or load from cache if recent"""
    print("Processing GDP data...")
    start_time = time.time()
    
    try:
        # Try to load existing data
        df = load_data_from_csv(DATA_FILES["gdp"])
        
        # Check if we need to refresh
        if needs_refresh("gdp", df):
            print("Fetching fresh GDP data...")
            df = fetch_fred_data(FRED_SERIES["gdp"])
            if not df.empty:
                save_data_to_csv(df, DATA_FILES["gdp"])
                print(f"GDP data updated with {len(df)} observations")
        else:
            print("Using cached GDP data (still current)")
    except Exception as e:
        print(f"Error fetching GDP data: {str(e)}")
        # Try to use cached data if available
        df = load_data_from_csv(DATA_FILES["gdp"])
    
    elapsed = time.time() - start_time
    print(f"GDP data processing completed in {elapsed:.2f} seconds")
    return df

def fetch_unemployment_data():
    """Fetch unemployment data or load from cache if recent"""
    print("Processing unemployment data...")
    start_time = time.time()
    
    try:
        # Try to load existing data
        df = load_data_from_csv(DATA_FILES["unemployment"])
        
        # Check if we need to refresh
        if needs_refresh("unemployment", df):
            print("Fetching fresh unemployment data...")
            df = fetch_fred_data(FRED_SERIES["unemployment"])
            if not df.empty:
                save_data_to_csv(df, DATA_FILES["unemployment"])
                print(f"Unemployment data updated with {len(df)} observations")
        else:
            print("Using cached unemployment data (still current)")
    except Exception as e:
        print(f"Error fetching unemployment data: {str(e)}")
        # Try to use cached data if available
        df = load_data_from_csv(DATA_FILES["unemployment"])
    
    elapsed = time.time() - start_time
    print(f"Unemployment data processing completed in {elapsed:.2f} seconds")
    return df

def fetch_cpi_data():
    """Fetch CPI data or load from cache if recent"""
    print("Processing CPI data...")
    start_time = time.time()
    
    try:
        # Try to load existing data
        df = load_data_from_csv(DATA_FILES["cpi"])
        
        # Check if we need to refresh
        if needs_refresh("cpi", df):
            print("Fetching fresh CPI data...")
            df = fetch_fred_data(FRED_SERIES["cpi"])
            if not df.empty:
                save_data_to_csv(df, DATA_FILES["cpi"])
                print(f"CPI data updated with {len(df)} observations")
        else:
            print("Using cached CPI data (still current)")
    except Exception as e:
        print(f"Error fetching CPI data: {str(e)}")
        # Try to use cached data if available
        df = load_data_from_csv(DATA_FILES["cpi"])
    
    elapsed = time.time() - start_time
    print(f"CPI data processing completed in {elapsed:.2f} seconds")
    return df

def fetch_pcepi_data():
    """Fetch PCEPI data or load from cache if recent"""
    print("Processing PCEPI data...")
    start_time = time.time()
    
    try:
        # Try to load existing data
        df = load_data_from_csv(DATA_FILES["pcepi"])
        
        # Check if we need to refresh
        if needs_refresh("pcepi", df):
            print("Fetching fresh PCEPI data...")
            df = fetch_fred_data(FRED_SERIES["pcepi"])
            if not df.empty:
                save_data_to_csv(df, DATA_FILES["pcepi"])
                print(f"PCEPI data updated with {len(df)} observations")
        else:
            print("Using cached PCEPI data (still current)")
    except Exception as e:
        print(f"Error fetching PCEPI data: {str(e)}")
        # Try to use cached data if available
        df = load_data_from_csv(DATA_FILES["pcepi"])
    
    elapsed = time.time() - start_time
    print(f"PCEPI data processing completed in {elapsed:.2f} seconds")
    return df

def fetch_interest_rate_data():
    """Fetch interest rate data or load from cache if recent"""
    print("Processing interest rate data...")
    start_time = time.time()
    
    try:
        # Try to load existing data
        df = load_data_from_csv(DATA_FILES["interest_rate"])
        
        # Check if we need to refresh
        if needs_refresh("interest_rate", df):
            print("Fetching fresh interest rate data...")
            df = fetch_fred_data(FRED_SERIES["interest_rate"])
            if not df.empty:
                save_data_to_csv(df, DATA_FILES["interest_rate"])
                print(f"Interest rate data updated with {len(df)} observations")
        else:
            print("Using cached interest rate data (still current)")
    except Exception as e:
        print(f"Error fetching interest rate data: {str(e)}")
        # Try to use cached data if available
        df = load_data_from_csv(DATA_FILES["interest_rate"])
    
    elapsed = time.time() - start_time
    print(f"Interest rate data processing completed in {elapsed:.2f} seconds")
    return df

def fetch_pce_data():
    """Fetch PCE data or load from cache if recent"""
    print("Processing PCE data...")
    start_time = time.time()
    
    try:
        # Try to load existing data
        df = load_data_from_csv(DATA_FILES["pce"])
        
        # Check if we need to refresh
        if needs_refresh("pce", df):
            print("Fetching fresh PCE data...")
            df = fetch_fred_data(FRED_SERIES["pce"])
            if not df.empty:
                save_data_to_csv(df, DATA_FILES["pce"])
                print(f"PCE data updated with {len(df)} observations")
        else:
            print("Using cached PCE data (still current)")
    except Exception as e:
        print(f"Error fetching PCE data: {str(e)}")
        # Try to use cached data if available
        df = load_data_from_csv(DATA_FILES["pce"])
    
    elapsed = time.time() - start_time
    print(f"PCE data processing completed in {elapsed:.2f} seconds")
    return df

def fetch_treasury_data():
    """Fetch Treasury Yield data - always refresh for latest"""
    print("Processing Treasury Yield data...")
    start_time = time.time()
    
    try:
        # Treasury yield updates daily so we always want fresh data
        df = fetch_treasury_yield_data()
        
        if not df.empty:
            # Load historical data to merge if available
            try:
                historical_df = load_data_from_csv(DATA_FILES["treasury_yield"])
                # Merge with historical data, keeping the newer values when dates overlap
                if not historical_df.empty:
                    combined_df = pd.concat([df, historical_df])
                    combined_df = combined_df.drop_duplicates(subset=['date'], keep='first')
                    df = combined_df.sort_values('date', ascending=False).reset_index(drop=True)
            except Exception as e:
                print(f"Could not merge with historical Treasury data: {str(e)}")
            
            save_data_to_csv(df, DATA_FILES["treasury_yield"])
            print(f"Treasury Yield data updated with {len(df)} observations")
    except Exception as e:
        print(f"Error fetching Treasury Yield data: {str(e)}")
        # Try to use cached data if available
        df = load_data_from_csv(DATA_FILES["treasury_yield"])
    
    elapsed = time.time() - start_time
    print(f"Treasury Yield data processing completed in {elapsed:.2f} seconds")
    return df

def fetch_vix_data():
    """Fetch VIX data - always refresh for latest"""
    print("Processing VIX data...")
    start_time = time.time()
    
    try:
        # VIX updates daily so we always want fresh data
        df = fetch_vix_from_yahoo()
        
        if not df.empty:
            # Load historical data to merge if available
            try:
                historical_df = load_data_from_csv(DATA_FILES["vix"])
                # Merge with historical data, keeping the newer values when dates overlap
                if not historical_df.empty:
                    combined_df = pd.concat([df, historical_df])
                    combined_df = combined_df.drop_duplicates(subset=['date'], keep='first')
                    df = combined_df.sort_values('date', ascending=False).reset_index(drop=True)
            except Exception as e:
                print(f"Could not merge with historical VIX data: {str(e)}")
            
            save_data_to_csv(df, DATA_FILES["vix"])
            print(f"VIX data updated with {len(df)} observations")
    except Exception as e:
        print(f"Error fetching VIX data: {str(e)}")
        # Try to use cached data if available
        df = load_data_from_csv(DATA_FILES["vix"])
    
    elapsed = time.time() - start_time
    print(f"VIX data processing completed in {elapsed:.2f} seconds")
    return df

def fetch_nasdaq_data():
    """Fetch NASDAQ data - always refresh for latest"""
    print("Processing NASDAQ data...")
    start_time = time.time()
    
    try:
        # NASDAQ updates daily so we always want fresh data
        df = fetch_nasdaq_with_ema()
        
        if not df.empty:
            # Save data to CSV
            save_data_to_csv(df, DATA_FILES["nasdaq"])
            print(f"NASDAQ data updated with {len(df)} observations")
    except Exception as e:
        print(f"Error fetching NASDAQ data: {str(e)}")
        # Try to use cached data if available
        df = load_data_from_csv(DATA_FILES["nasdaq"])
    
    elapsed = time.time() - start_time
    print(f"NASDAQ data processing completed in {elapsed:.2f} seconds")
    return df

def fetch_consumer_sentiment():
    """Fetch Consumer Sentiment data or load from cache if recent"""
    print("Processing Consumer Sentiment data...")
    start_time = time.time()
    
    try:
        # Try to load existing data
        df = load_data_from_csv(DATA_FILES["consumer_sentiment"])
        
        # Check if we need to refresh
        if needs_refresh("consumer_sentiment", df):
            print("Fetching fresh Consumer Sentiment data...")
            df = fetch_consumer_sentiment_data()
            if not df.empty:
                save_data_to_csv(df, DATA_FILES["consumer_sentiment"])
                print(f"Consumer Sentiment data updated with {len(df)} observations")
        else:
            print("Using cached Consumer Sentiment data (still current)")
    except Exception as e:
        print(f"Error fetching Consumer Sentiment data: {str(e)}")
        # Try to use cached data if available
        df = load_data_from_csv(DATA_FILES["consumer_sentiment"])
    
    elapsed = time.time() - start_time
    print(f"Consumer Sentiment data processing completed in {elapsed:.2f} seconds")
    return df

# Main function to fetch all data in parallel
def fetch_all_data_parallel():
    """Fetch all economic data in parallel using thread pool"""
    start_time = time.time()
    print("Starting parallel data fetching...")
    
    # Create a mapping of data types to their fetching functions
    fetch_functions = {
        "gdp": fetch_gdp_data,
        "unemployment": fetch_unemployment_data,
        "cpi": fetch_cpi_data,
        "pcepi": fetch_pcepi_data,
        "interest_rate": fetch_interest_rate_data,
        "pce": fetch_pce_data,
        "treasury_yield": fetch_treasury_data,
        "vix": fetch_vix_data,
        "nasdaq": fetch_nasdaq_data,
        "consumer_sentiment": fetch_consumer_sentiment
    }
    
    # Results dictionary to store all data
    results = {}
    
    # Use ThreadPoolExecutor to run fetch operations in parallel
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        # Submit all fetch tasks
        future_to_data = {executor.submit(func): data_type for data_type, func in fetch_functions.items()}
        
        # Process completed tasks as they complete
        for future in concurrent.futures.as_completed(future_to_data):
            data_type = future_to_data[future]
            try:
                data = future.result()
                results[data_type] = data
            except Exception as e:
                print(f"Error in {data_type} fetching: {str(e)}")
                # Try to load from cache as fallback
                try:
                    results[data_type] = load_data_from_csv(DATA_FILES[data_type])
                except Exception:
                    results[data_type] = pd.DataFrame()
    
    elapsed = time.time() - start_time
    print(f"All data fetching completed in {elapsed:.2f} seconds")
    
    # Save metadata about the fetch operation
    metadata = {
        "fetch_time": pd.Timestamp.now().isoformat(),
        "elapsed_seconds": elapsed,
        "data_types": list(results.keys())
    }
    
    try:
        with open("data/fetch_metadata.json", "w") as f:
            json.dump(metadata, f)
    except Exception as e:
        print(f"Error saving fetch metadata: {str(e)}")
    
    return results

# Function to check if full data refresh is needed
def check_refresh_needed():
    """Check if a complete data refresh is needed based on elapsed time"""
    try:
        with open("data/fetch_metadata.json", "r") as f:
            metadata = json.load(f)
            
        last_fetch = pd.to_datetime(metadata.get("fetch_time", "2000-01-01"))
        now = pd.Timestamp.now()
        hours_since_fetch = (now - last_fetch).total_seconds() / 3600
        
        # Refresh if more than 6 hours have passed
        return hours_since_fetch > 6
    except Exception:
        # If metadata doesn't exist or can't be read, refresh is needed
        return True

# Function to fetch only daily updated data
def fetch_daily_data_parallel():
    """Fetch only daily-updating data in parallel"""
    start_time = time.time()
    print("Starting parallel daily data fetching...")
    
    # Only fetch data that updates daily
    daily_data_types = [data_type for data_type, freq in UPDATE_FREQUENCIES.items() if freq == "daily"]
    
    # Create a mapping of data types to their fetching functions
    fetch_functions = {
        "treasury_yield": fetch_treasury_data,
        "vix": fetch_vix_data,
        "nasdaq": fetch_nasdaq_data,
    }
    
    # Results dictionary to store daily data
    results = {}
    
    # Use ThreadPoolExecutor to run fetch operations in parallel
    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
        # Submit daily fetch tasks
        future_to_data = {executor.submit(func): data_type for data_type, func in fetch_functions.items()}
        
        # Process completed tasks as they complete
        for future in concurrent.futures.as_completed(future_to_data):
            data_type = future_to_data[future]
            try:
                data = future.result()
                results[data_type] = data
            except Exception as e:
                print(f"Error in {data_type} fetching: {str(e)}")
                # Try to load from cache as fallback
                try:
                    results[data_type] = load_data_from_csv(DATA_FILES[data_type])
                except Exception:
                    results[data_type] = pd.DataFrame()
    
    elapsed = time.time() - start_time
    print(f"Daily data fetching completed in {elapsed:.2f} seconds")
    
    return results

# Main function to intelligently fetch all data
def fetch_data_smart():
    """Intelligently fetch data based on update frequency and elapsed time"""
    # Check if a full refresh is needed
    if check_refresh_needed():
        print("Full data refresh needed")
        return fetch_all_data_parallel()
    else:
        # Get daily data first
        daily_data = fetch_daily_data_parallel()
        
        # Load other data from cache
        all_data = {}
        for data_type, file_path in DATA_FILES.items():
            if data_type in daily_data:
                all_data[data_type] = daily_data[data_type]
            else:
                try:
                    all_data[data_type] = load_data_from_csv(file_path)
                except Exception as e:
                    print(f"Error loading {data_type} from cache: {str(e)}")
                    all_data[data_type] = pd.DataFrame()
        
        return all_data

# Make the directory for data files if it doesn't exist
def ensure_data_directory():
    """Ensure the data directory exists"""
    os.makedirs("data", exist_ok=True)
    
    # Check that all data files can be written
    for file_path in DATA_FILES.values():
        directory = os.path.dirname(file_path)
        if directory and not os.path.exists(directory):
            os.makedirs(directory, exist_ok=True)

# Function to be called at application startup
def initialize_data():
    """Initialize data at application startup"""
    ensure_data_directory()
    return fetch_data_smart()

# If run directly, fetch all data
if __name__ == "__main__":
    initialize_data()
