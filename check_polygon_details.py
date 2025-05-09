"""
Check Polygon Details for GOOGL

This script directly queries Polygon's API to see what market cap and share count
they report for GOOGL, to understand why there's a discrepancy.
"""
import os
import json
import requests
import logging

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

def get_polygon_ticker_details(ticker):
    """
    Query Polygon's API for detailed ticker information
    
    Args:
        ticker (str): The ticker symbol to query
        
    Returns:
        dict: The API response data
    """
    # Get API key from environment variable
    api_key = os.environ.get("POLYGON_API_KEY")
    if not api_key:
        logging.error("POLYGON_API_KEY environment variable not set")
        return None
        
    # Set up request
    url = f"https://api.polygon.io/v3/reference/tickers/{ticker}"
    headers = {"Authorization": f"Bearer {api_key}"}
    
    # Make request
    try:
        response = requests.get(url, headers=headers)
        
        # Check response
        if response.status_code == 200:
            return response.json()
        else:
            logging.error(f"Error {response.status_code}: {response.text}")
            return None
    except Exception as e:
        logging.error(f"Request error: {e}")
        return None

def query_polygon_v1_metadata(ticker):
    """
    Query Polygon's v1 metadata endpoint for detailed company info
    
    Args:
        ticker (str): The ticker symbol to query
        
    Returns:
        dict: The API response data
    """
    # Get API key from environment variable
    api_key = os.environ.get("POLYGON_API_KEY")
    if not api_key:
        logging.error("POLYGON_API_KEY environment variable not set")
        return None
        
    # Set up request - using v1 endpoint which might have different data
    url = f"https://api.polygon.io/v1/meta/symbols/{ticker}/company"
    params = {"apiKey": api_key}
    
    # Make request
    try:
        response = requests.get(url, params=params)
        
        # Check response
        if response.status_code == 200:
            return response.json()
        else:
            logging.error(f"Error {response.status_code}: {response.text}")
            return None
    except Exception as e:
        logging.error(f"Request error: {e}")
        return None

def get_polygon_snapshot(ticker):
    """
    Query Polygon's snapshot endpoint for latest market cap
    
    Args:
        ticker (str): The ticker symbol to query
        
    Returns:
        dict: The API response data
    """
    # Get API key from environment variable
    api_key = os.environ.get("POLYGON_API_KEY")
    if not api_key:
        logging.error("POLYGON_API_KEY environment variable not set")
        return None
        
    # Set up request
    url = f"https://api.polygon.io/v2/snapshot/locale/us/markets/stocks/tickers/{ticker}"
    headers = {"Authorization": f"Bearer {api_key}"}
    
    # Make request
    try:
        response = requests.get(url, headers=headers)
        
        # Check response
        if response.status_code == 200:
            return response.json()
        else:
            logging.error(f"Error {response.status_code}: {response.text}")
            return None
    except Exception as e:
        logging.error(f"Request error: {e}")
        return None

def main():
    """Main function to check Polygon's reported data for GOOGL"""
    ticker = "GOOGL"
    
    print(f"Checking Polygon's data for {ticker}...")
    
    # Get ticker details from v3 endpoint
    details = get_polygon_ticker_details(ticker)
    if details and "results" in details:
        # Print relevant information
        results = details["results"]
        print("\nPolygon V3 Ticker Details:")
        print(f"Name: {results.get('name')}")
        print(f"Market cap: {results.get('market_cap', 'Not provided')}")
        print(f"Weighted shares outstanding: {results.get('weighted_shares_outstanding', 'Not provided')}")
        print(f"Share class shares outstanding: {results.get('share_class_shares_outstanding', 'Not provided')}")
        print(f"Primary exchange: {results.get('primary_exchange')}")
        print(f"CIK: {results.get('cik')}")
        print(f"List date: {results.get('list_date')}")
        print(f"Total employees: {results.get('total_employees')}")
        print(f"Type: {results.get('type')}")
        
        # Save full response for reference
        with open(f"{ticker}_v3_details.json", "w") as f:
            json.dump(details, f, indent=2)
        print(f"Saved full V3 response to {ticker}_v3_details.json")
    else:
        print("Failed to get ticker details from V3 endpoint")
    
    # Get company metadata from v1 endpoint
    metadata = query_polygon_v1_metadata(ticker)
    if metadata:
        print("\nPolygon V1 Company Metadata:")
        print(f"Name: {metadata.get('name')}")
        print(f"Market cap: {metadata.get('marketcap', 'Not provided')}")
        print(f"Outstanding shares: {metadata.get('outstanding_shares', 'Not provided')}")
        print(f"Industry: {metadata.get('industry')}")
        print(f"Sector: {metadata.get('sector')}")
        
        # Save full response for reference
        with open(f"{ticker}_v1_metadata.json", "w") as f:
            json.dump(metadata, f, indent=2)
        print(f"Saved full V1 response to {ticker}_v1_metadata.json")
    else:
        print("Failed to get company metadata from V1 endpoint")
    
    # Get snapshot data
    snapshot = get_polygon_snapshot(ticker)
    if snapshot and "ticker" in snapshot:
        ticker_data = snapshot["ticker"]
        print("\nPolygon Snapshot Data:")
        print(f"Day's volume: {ticker_data.get('day', {}).get('v', 'Not provided')}")
        print(f"Latest price: {ticker_data.get('lastTrade', {}).get('p', 'Not provided')}")
        print(f"Outstanding shares: {ticker_data.get('prevDay', {}).get('o', 'Not provided')}")
        print(f"Market cap: {ticker_data.get('prevDay', {}).get('mc', 'Not provided')}")
        
        # Save full response for reference
        with open(f"{ticker}_snapshot.json", "w") as f:
            json.dump(snapshot, f, indent=2)
        print(f"Saved full snapshot to {ticker}_snapshot.json")
    else:
        print("Failed to get snapshot data")

if __name__ == "__main__":
    main()