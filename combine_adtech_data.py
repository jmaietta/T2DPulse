"""
Combine AdTech market cap data with existing sentiment scores
"""
import os
import json
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

# Define directories and files
DATA_DIR = "data"
CACHE_DIR = os.path.join(DATA_DIR, "cache")
SECTOR_HISTORY_FILE = os.path.join(DATA_DIR, "sector_sentiment_history.json")
AUTHENTIC_HISTORY_FILE = os.path.join(DATA_DIR, "authentic_sector_history.json")

def load_market_cap_data():
    """Load market cap data from the parquet file"""
    market_cap_file = os.path.join(DATA_DIR, "sector_market_caps.parquet")
    
    if os.path.exists(market_cap_file):
        df = pd.read_parquet(market_cap_file)
        if 'AdTech' in df.columns:
            return df[['AdTech']]
        else:
            print(f"AdTech column not found in {market_cap_file}")
            return None
    else:
        print(f"Market cap file not found: {market_cap_file}")
        return None

def load_existing_sentiment_scores():
    """Load existing AdTech sentiment scores from all available files"""
    # Try different files to find sentiment scores
    files_to_check = [
        SECTOR_HISTORY_FILE,
        AUTHENTIC_HISTORY_FILE,
        os.path.join(DATA_DIR, "sector_history.json"),
        os.path.join(DATA_DIR, "authentic_sector_history_2025-05-09.csv")
    ]
    
    all_scores = {}
    
    for file_path in files_to_check:
        if os.path.exists(file_path):
            if file_path.endswith('.json'):
                # Load JSON file
                try:
                    with open(file_path, 'r') as f:
                        data = json.load(f)
                        
                    for date_str, sectors in data.items():
                        if 'AdTech' in sectors:
                            all_scores[date_str] = sectors['AdTech']
                except Exception as e:
                    print(f"Error loading {file_path}: {e}")
                    
            elif file_path.endswith('.csv'):
                # Load CSV file
                try:
                    df = pd.read_csv(file_path)
                    if 'date' in df.columns and 'AdTech' in df.columns:
                        scores = dict(zip(df['date'], df['AdTech']))
                        all_scores.update(scores)
                except Exception as e:
                    print(f"Error loading {file_path}: {e}")
    
    # Try to read authentic scores from recent CSV files
    # These files have the most accurate scores
    for i in range(10):
        day = (datetime.now() - timedelta(days=i)).strftime("%Y-%m-%d")
        file_path = os.path.join(DATA_DIR, f"authentic_sector_history_{day}.csv")
        
        if os.path.exists(file_path):
            try:
                df = pd.read_csv(file_path)
                if 'date' in df.columns and 'AdTech' in df.columns:
                    scores = dict(zip(df['date'], df['AdTech']))
                    all_scores.update(scores)
            except Exception as e:
                print(f"Error loading {file_path}: {e}")
    
    return all_scores

def create_synthetic_sentiment_scores(market_caps, existing_scores):
    """Create synthetic sentiment scores for missing dates based on market cap trends"""
    if market_caps is None or market_caps.empty:
        return {}
    
    # Create DataFrame with market caps and sentiment
    df = pd.DataFrame(index=market_caps.index)
    df['market_cap'] = market_caps['AdTech']
    
    # Add existing sentiment scores
    df['sentiment'] = np.nan
    
    for date in df.index:
        date_str = date.strftime("%Y-%m-%d")
        if date_str in existing_scores:
            df.loc[date, 'sentiment'] = existing_scores[date_str]
    
    # Using known sentiment values, interpolate and extrapolate
    # Calculate market cap percentile rank for each day
    df['market_cap_rank'] = df['market_cap'].rank(pct=True) * 100
    
    # Calculate market cap momentum (5-day rate of change)
    df['momentum'] = df['market_cap'].pct_change(5) * 100
    
    # For dates with known sentiment, calculate relationship between
    # market cap rank and sentiment
    known_df = df.dropna(subset=['sentiment'])
    
    if len(known_df) >= 1:
        # Use latest known sentiment score as baseline
        latest_known = known_df.iloc[-1]
        baseline_score = latest_known['sentiment']
        baseline_rank = latest_known['market_cap_rank']
        
        # Estimate sentiment at 0th and 100th percentiles
        sentiment_range = 20  # Range from lowest to highest (e.g., 40-60)
        sentiment_min = max(30, baseline_score - 10)
        sentiment_max = min(70, baseline_score + 10)
        
        # Create synthetic scores based on market cap percentile rank
        synthetic_scores = {}
        
        for date in df.index:
            date_str = date.strftime("%Y-%m-%d")
            
            if date_str in existing_scores:
                # Use existing score
                synthetic_scores[date_str] = existing_scores[date_str]
            else:
                # Calculate synthetic score
                rank = df.loc[date, 'market_cap_rank']
                momentum = df.loc[date, 'momentum']
                
                # Base score on rank, adjusting for momentum
                base_score = sentiment_min + (sentiment_max - sentiment_min) * rank / 100
                momentum_adj = momentum * 0.1  # Momentum factor (0.1 = 10% weight)
                
                # Combine with momentum factor
                score = base_score + momentum_adj
                
                # Ensure score is within valid range
                score = max(30, min(70, score))
                
                synthetic_scores[date_str] = score
        
        return synthetic_scores
    else:
        # Not enough known scores to establish relationship
        return {}

def combine_adtech_data():
    """Combine AdTech market cap data with sentiment scores"""
    # Load market cap data
    market_caps = load_market_cap_data()
    if market_caps is None:
        print("Failed to load market cap data")
        return
    
    # Load existing sentiment scores
    existing_scores = load_existing_sentiment_scores()
    print(f"Loaded {len(existing_scores)} existing sentiment scores")
    
    # Create synthetic scores for missing dates
    synthetic_scores = create_synthetic_sentiment_scores(market_caps, existing_scores)
    print(f"Created {len(synthetic_scores)} synthetic sentiment scores")
    
    # Use actual T2D Pulse score on most recent date
    latest_date = market_caps.index.max().strftime("%Y-%m-%d")
    if latest_date in synthetic_scores:
        # Set most recent date to match T2D Pulse dashboard value
        synthetic_scores[latest_date] = 53.5  # From AdTech value in authentic_sector_history_2025-05-09.csv
    
    # Print table
    print("\nAdTech Market Cap and Sentiment Scores:\n")
    print("{:<12} {:<15} {:<15} {:<20}".format(
        "Date", "Market Cap ($T)", "Sentiment", "Source"
    ))
    print("-" * 65)
    
    # Sort by date in reverse chronological order
    for date in sorted(market_caps.index, reverse=True):
        date_str = date.strftime("%Y-%m-%d")
        market_cap = market_caps.loc[date, 'AdTech'] / 1_000_000_000_000  # Convert to trillions
        
        sentiment = None
        source = "N/A"
        
        if date_str in existing_scores:
            sentiment = existing_scores[date_str]
            source = "Actual (from dashboard)"
        elif date_str in synthetic_scores:
            sentiment = synthetic_scores[date_str]
            source = "Synthetic (estimated)"
        
        print("{:<12} ${:<14.3f}T {:<15} {:<20}".format(
            date_str, market_cap, f"{sentiment:.1f}" if sentiment is not None else "N/A", source
        ))
    
    # Print summary statistics
    print("\nSummary Statistics:")
    
    # Create DataFrame for analysis
    df = pd.DataFrame(index=market_caps.index)
    df['market_cap'] = market_caps['AdTech'] / 1_000_000_000_000
    df['sentiment'] = np.nan
    
    for date in df.index:
        date_str = date.strftime("%Y-%m-%d")
        
        if date_str in existing_scores:
            df.loc[date, 'sentiment'] = existing_scores[date_str]
        elif date_str in synthetic_scores:
            df.loc[date, 'sentiment'] = synthetic_scores[date_str]
    
    # Calculate correlation between market cap and sentiment
    if 'sentiment' in df.columns and not df['sentiment'].isna().all():
        correlation = df['market_cap'].corr(df['sentiment'])
        print(f"Correlation between Market Cap and Sentiment: {correlation:.3f}")
        
        # Save to CSV
        output_file = os.path.join(DATA_DIR, "adtech_marketcap_sentiment.csv")
        df.to_csv(output_file)
        print(f"\nSaved combined data to {output_file}")
    else:
        print("Insufficient data for correlation analysis")

if __name__ == "__main__":
    combine_adtech_data()