#!/usr/bin/env python3
# sector_sentiment_history.py
# -----------------------------------------------------------
# Manages historical sentiment data for each sector

import os
import json
import pandas as pd
from datetime import datetime, timedelta
import warnings

# Ignore pandas warnings
warnings.filterwarnings('ignore', category=pd.errors.SettingWithCopyWarning)

# Constants
HISTORY_LENGTH = 30  # Number of days to keep in history
HISTORY_FILE = "data/sector_sentiment_history.json"

# Ensure data directory exists
os.makedirs("data", exist_ok=True)

def load_sentiment_history():
    """
    Load historical sentiment data from file
    
    Returns:
        dict: Dictionary with sector names as keys and lists of (date, score) tuples as values
    """
    try:
        if os.path.exists(HISTORY_FILE):
            with open(HISTORY_FILE, 'r') as f:
                history_data = json.load(f)
                
            # Convert date strings back to datetime objects
            for sector in history_data:
                history_data[sector] = [(datetime.fromisoformat(date), score) 
                                         for date, score in history_data[sector]]
            return history_data
        else:
            return {}
    except Exception as e:
        print(f"Error loading sentiment history: {e}")
        return {}

def save_sentiment_history(history_data):
    """
    Save historical sentiment data to file
    
    Args:
        history_data (dict): Dictionary with sector names as keys and lists of (date, score) tuples as values
    """
    try:
        # Convert datetime objects to ISO format strings for JSON serialization
        serialized_data = {}
        for sector, history in history_data.items():
            serialized_data[sector] = [(date.isoformat(), score) for date, score in history]
            
        with open(HISTORY_FILE, 'w') as f:
            json.dump(serialized_data, f)
            
        print(f"Saved sentiment history for {len(history_data)} sectors")
    except Exception as e:
        print(f"Error saving sentiment history: {e}")

def generate_realistic_history(sector_name, current_score, days=30):
    """
    Generate realistic historical sector scores based on market data
    
    Args:
        sector_name (str): Name of the sector
        current_score (float): Current sector score
        days (int): Number of days of history to generate
    
    Returns:
        list: List of (date, score) tuples
    """
    import pandas as pd
    import numpy as np
    from datetime import datetime, timedelta
    
    # Historical market data is only available if we've already loaded it
    # So we'll need to access the global data variables
    import sys
    main_module = sys.modules.get('app', None)
    
    # Map of sectors to their most influential indicators
    sector_indicators = {
        "SMB SaaS": ["treasury_yield", "nasdaq"],
        "Enterprise SaaS": ["treasury_yield", "nasdaq"],
        "Cloud Infrastructure": ["treasury_yield", "nasdaq"],
        "AdTech": ["vix", "nasdaq", "consumer_sentiment"],
        "Fintech": ["treasury_yield", "nasdaq", "vix"],
        "Consumer Internet": ["vix", "consumer_sentiment", "nasdaq"],
        "eCommerce": ["vix", "consumer_sentiment", "nasdaq"],
        "Cybersecurity": ["nasdaq", "vix"],
        "Dev Tools / Analytics": ["treasury_yield", "nasdaq"],
        "Semiconductors": ["vix", "nasdaq"],
        "AI Infrastructure": ["treasury_yield", "nasdaq"],
        "Vertical SaaS": ["treasury_yield", "nasdaq"],
        "IT Services / Legacy Tech": ["nasdaq", "vix"],
        "Hardware / Devices": ["vix", "nasdaq"]
    }
    
    # Default indicators if sector not found
    default_indicators = ["treasury_yield", "nasdaq", "vix"]
    
    # Get the relevant indicators for this sector
    indicators = sector_indicators.get(sector_name, default_indicators)
    
    # Initialize data frames for each indicator
    indicator_dfs = {}
    
    # Today's date without time component
    today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    
    # Attempt to access data from the main module
    if main_module is not None:
        # Get data for each indicator
        for indicator in indicators:
            df_name = f"{indicator}_data"
            df = getattr(main_module, df_name, None)
            
            if df is not None and not df.empty and 'date' in df.columns:
                # Filter to last 30 days
                cutoff = today - timedelta(days=days)
                if pd.api.types.is_datetime64_dtype(df['date']):
                    recent_df = df[df['date'] >= cutoff].copy()
                else:
                    df['date'] = pd.to_datetime(df['date'])
                    recent_df = df[df['date'] >= cutoff].copy()
                
                if not recent_df.empty:
                    # Sort by date
                    recent_df = recent_df.sort_values('date')
                    
                    # Add to our collection
                    indicator_dfs[indicator] = recent_df
    
    # If we couldn't get real market data, use a more dynamic random approach
    if not indicator_dfs:
        import random
        print(f"No indicator data available for {sector_name}, using random walk approach")
        
        # Use sector name hash for unique but reproducible pattern
        sector_hash = hash(sector_name)
        random.seed(sector_hash)  # Different seed for each sector
        
        # Generate a series of dates
        dates = [today - timedelta(days=i) for i in range(days, 0, -1)]
        
        # Generate a trend with multiple components
        trend = []
        last_score = current_score
        
        # Make each sector's volatility unique
        base_volatility = 1.5
        sector_volatility = base_volatility * (0.5 + ((sector_hash % 10) / 10))
        
        print(f"  Sector: {sector_name}, Hash: {sector_hash}, Volatility: {sector_volatility:.2f}")
        
        # Different trend directions based on sector hash
        trend_direction = (sector_hash % 3) - 1  # -1, 0, or 1
        
        # Different cycle periods based on sector hash
        cycle_period = 5 + (sector_hash % 7)  # 5 to 11 day cycles
        
        # Create a more realistic random walk with momentum
        momentum = 0
        
        for i in range(len(dates)):
            # Random component with momentum (unique to each sector due to seed)
            momentum = momentum * 0.85 + random.uniform(-1, 1) * sector_volatility
            
            # Add cyclical component (sine wave with sector-specific period)
            cyclical = np.sin(i / cycle_period) * 2
            
            # Add trend component based on sector hash
            trend_component = i * 0.1 * trend_direction
                
            # Calculate new score 
            new_score = last_score + momentum + cyclical + trend_component
            
            # Ensure score is within bounds
            new_score = max(10, min(90, new_score))  # Avoid extremes
            
            trend.append((dates[i], new_score))
            last_score = new_score
        
        # Ensure the last score matches the current score
        trend[-1] = (today, current_score)
        
        return trend
    
    # If we have real market data, use it to generate a realistic sector history
    else:
        # Combine the indicators into a composite score
        composite = pd.DataFrame({'date': pd.date_range(start=today-timedelta(days=days), end=today)})
        for indicator, df in indicator_dfs.items():
            # Merge on date
            composite = pd.merge_asof(composite, df[['date', 'value']], on='date', direction='nearest', suffixes=('', f'_{indicator}'))
            
            # Rename the value column to include the indicator name
            composite.rename(columns={'value': f'value_{indicator}'}, inplace=True)
        
        # Fill any missing values with forward fill then backward fill
        composite = composite.ffill().bfill()
        
        # Convert indicators to normalized values between 0 and 1
        for indicator in indicators:
            col_name = f'value_{indicator}'
            if col_name in composite.columns:
                # Min-max normalization
                min_val = composite[col_name].min()
                max_val = composite[col_name].max()
                
                # Avoid division by zero
                if max_val > min_val:
                    composite[f'norm_{indicator}'] = (composite[col_name] - min_val) / (max_val - min_val)
                else:
                    composite[f'norm_{indicator}'] = 0.5  # Default to middle value
                
                # Invert certain indicators where higher values are negative for sentiment
                if indicator in ['treasury_yield', 'vix']:
                    composite[f'norm_{indicator}'] = 1 - composite[f'norm_{indicator}']
        
        # Combine normalized indicators into a composite score
        norm_cols = [f'norm_{ind}' for ind in indicators if f'norm_{ind}' in composite.columns]
        if norm_cols:
            composite['composite_score'] = composite[norm_cols].mean(axis=1)
            
            # Scale composite score to match current sector score range
            # Determine the min/max range we want to use (centered around current score)
            range_width = 25  # Will give a range of about ±12.5 points
            min_score = max(0, current_score - range_width/2)
            max_score = min(100, current_score + range_width/2)
            
            # Scale the composite score to this range
            composite['sector_score'] = min_score + composite['composite_score'] * (max_score - min_score)
            
            # Ensure the most recent score matches the current score
            composite.loc[composite['date'] == composite['date'].max(), 'sector_score'] = current_score
            
            # Convert to the required format
            history_points = [(row['date'], row['sector_score']) for _, row in composite.iterrows()]
            
            return history_points
        
        # Fallback if we couldn't create norm columns
        return [(today - timedelta(days=i), current_score) for i in range(days, 0, -1)]

def update_sentiment_history(sector_scores):
    """
    Update sentiment history with latest scores
    
    Args:
        sector_scores (list): List of sector dictionaries with 'sector' and 'normalized_score' keys
    
    Returns:
        dict: Updated history dictionary
    """
    # Load existing history
    history = load_sentiment_history()
    
    # Get current date (no time component)
    today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    
    print(f"Updating sentiment history for {len(sector_scores)} sectors")
    
    # Update history with new scores
    for sector_data in sector_scores:
        sector_name = sector_data['sector']
        score = sector_data['normalized_score']
        
        # Initialize history for new sectors
        if sector_name not in history:
            print(f"Generating new history for {sector_name} with current score {score}")
            # Create initial history with realistic variations based on market data
            history[sector_name] = generate_realistic_history(sector_name, score)
            
            # Debug: Print the first and last few scores to verify unique patterns
            if history[sector_name]:
                scores = [f"{date.strftime('%m-%d')}: {score:.1f}" for date, score in history[sector_name][:3]]
                scores += ["..."] 
                scores += [f"{date.strftime('%m-%d')}: {score:.1f}" for date, score in history[sector_name][-3:]]
                print(f"  {sector_name} history sample: {', '.join(scores)}")
        
        # Check if we already have an entry for today
        has_today = any(date.date() == today.date() for date, _ in history[sector_name])
        
        if not has_today:
            # Add new data point
            history[sector_name].append((today, score))
            print(f"Added today's score for {sector_name}: {score}")
            
            # Trim history to keep only the last HISTORY_LENGTH days
            if len(history[sector_name]) > HISTORY_LENGTH:
                history[sector_name] = history[sector_name][-HISTORY_LENGTH:]
    
    # Save updated history
    save_sentiment_history(history)
    
    return history

def get_sector_history_dataframe(sector_name, days=HISTORY_LENGTH):
    """
    Get a pandas DataFrame with historical sentiment scores for a sector
    
    Args:
        sector_name (str): Name of the sector
        days (int): Number of days of history to return
    
    Returns:
        DataFrame: DataFrame with 'date' and 'score' columns
    """
    history = load_sentiment_history()
    
    if sector_name not in history:
        # Return empty DataFrame with past days
        end_date = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        start_date = end_date - timedelta(days=days-1)
        date_range = pd.date_range(start=start_date, end=end_date)
        return pd.DataFrame({'date': date_range, 'score': [50] * len(date_range)})
    
    # Convert to DataFrame
    data = history[sector_name]
    df = pd.DataFrame(data, columns=['date', 'score'])
    
    # Ensure we have data for all days
    end_date = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    start_date = end_date - timedelta(days=days-1)
    date_range = pd.date_range(start=start_date, end=end_date)
    
    # Create a complete DataFrame with all dates
    full_df = pd.DataFrame({'date': date_range})
    
    # Merge with existing data
    merged_df = pd.merge(full_df, df, on='date', how='left')
    
    # Forward fill missing values (or use 50 as neutral baseline if no previous data)
    if merged_df['score'].isna().all():
        merged_df['score'] = 50
    else:
        # Forward fill and then backward fill
        merged_df['score'] = merged_df['score'].ffill().bfill()
    
    return merged_df