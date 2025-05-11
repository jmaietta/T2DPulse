#!/usr/bin/env python3
"""
Update AI Infrastructure sector scores with authentic data based on ticker market caps.
This script calculates more realistic scores for the AI Infrastructure sector
using the market cap data that has been collected for its constituent stocks.
"""

import os
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Define the tickers in the AI Infrastructure sector
AI_INFRA_TICKERS = ['NVDA', 'AMD', 'ARM', 'AVGO', 'INTC', 'TSM']  # Major AI chip and infrastructure companies

def load_ticker_data():
    """Load historical market cap data for AI Infrastructure tickers"""
    ticker_data = {}
    
    # Check all parquet and CSV files for ticker data
    for root, _, files in os.walk('.'):
        for file in files:
            if any(ticker in file for ticker in AI_INFRA_TICKERS) and \
               ('history' in file.lower() or 'market' in file.lower()):
                if file.endswith('.parquet'):
                    try:
                        df = pd.read_parquet(os.path.join(root, file))
                        if 'date' in df.columns and 'market_cap' in df.columns:
                            ticker = next((t for t in AI_INFRA_TICKERS if t in file), None)
                            if ticker:
                                logger.info(f"Loading data for {ticker} from {file}")
                                ticker_data[ticker] = df
                    except Exception as e:
                        logger.error(f"Error loading {file}: {e}")
                elif file.endswith('.csv'):
                    try:
                        df = pd.read_csv(os.path.join(root, file))
                        
                        # Look for columns that might contain market cap data
                        market_cap_col = None
                        for col in df.columns:
                            if 'market' in col.lower() and 'cap' in col.lower():
                                market_cap_col = col
                                break
                        
                        # Skip if we can't find both date and market cap columns
                        if 'date' not in df.columns or not market_cap_col:
                            continue
                            
                        ticker = next((t for t in AI_INFRA_TICKERS if t in file), None)
                        if ticker:
                            logger.info(f"Loading data for {ticker} from {file}")
                            
                            # Rename the column to standardize
                            df = df.rename(columns={market_cap_col: 'market_cap'})
                            ticker_data[ticker] = df
                    except Exception as e:
                        logger.error(f"Error loading {file}: {e}")
    
    return ticker_data

def calculate_sector_sentiment(ticker_data):
    """Calculate daily sentiment scores for the AI Infrastructure sector"""
    if not ticker_data:
        logger.error("No ticker data found for AI Infrastructure sector")
        return None
    
    # Merge all ticker data by date to get a complete view
    all_data = []
    for ticker, df in ticker_data.items():
        # Ensure date is in datetime format
        if 'date' in df.columns:
            df = df.copy()
            df['date'] = pd.to_datetime(df['date'])
            df['ticker'] = ticker
            
            # Keep only needed columns
            keep_cols = ['date', 'ticker', 'market_cap']
            df = df[keep_cols].copy()
            
            all_data.append(df)
    
    if not all_data:
        logger.error("No valid data found after processing")
        return None
        
    # Combine all ticker data
    combined_df = pd.concat(all_data)
    
    # Group by date to get daily total market cap
    daily_totals = combined_df.groupby('date')['market_cap'].sum().reset_index()
    
    # Calculate simple percentage changes as a basis for sentiment
    daily_totals['pct_change'] = daily_totals['market_cap'].pct_change()
    
    # Fill missing first day
    daily_totals.loc[0, 'pct_change'] = 0
    
    # Convert to a rolling average for smoothing
    daily_totals['rolling_change'] = daily_totals['pct_change'].rolling(3, min_periods=1).mean()
    
    # Calculate sentiment scores (0-100 scale)
    # Strategy: Convert percentage changes to a 0-100 score
    # 0% change = 50 (neutral)
    # +2% daily change = 75 (quite positive)
    # -2% daily change = 25 (quite negative)
    max_pct_change = 0.02  # 2% change considered significant
    daily_totals['sentiment_score'] = 50 + (daily_totals['rolling_change'] / max_pct_change) * 25
    
    # Clip to 0-100 range
    daily_totals['sentiment_score'] = daily_totals['sentiment_score'].clip(0, 100)
    
    return daily_totals[['date', 'sentiment_score']]

def update_sector_history(sentiment_scores):
    """Update the sector history CSV with new AI Infrastructure scores"""
    if sentiment_scores is None or sentiment_scores.empty:
        logger.error("No sentiment scores to update with")
        return False
        
    # Load the existing sector history
    sector_history_path = 'data/sector_30day_history.csv'
    if not os.path.exists(sector_history_path):
        logger.error(f"Sector history file not found: {sector_history_path}")
        return False
        
    try:
        # Load the existing data
        sector_df = pd.read_csv(sector_history_path)
        
        # Ensure date formats match
        sector_df['Date'] = pd.to_datetime(sector_df['Date'])
        sentiment_scores['date'] = pd.to_datetime(sentiment_scores['date'])
        
        # Round scores for consistent decimal places
        sentiment_scores['sentiment_score'] = sentiment_scores['sentiment_score'].round(1)
        
        # Update AI Infrastructure scores where dates match
        for _, row in sentiment_scores.iterrows():
            match_idx = sector_df[sector_df['Date'] == row['date']].index
            if not match_idx.empty:
                sector_df.loc[match_idx, 'AI Infrastructure'] = row['sentiment_score']
                logger.info(f"Updated score for {row['date']}: {row['sentiment_score']}")
        
        # Save the updated file
        sector_df.to_csv(sector_history_path, index=False)
        logger.info(f"Updated {sector_history_path} with new AI Infrastructure scores")
        
        # Also update authentic_sector_history.csv if it exists
        authentic_path = 'data/authentic_sector_history.csv'
        if os.path.exists(authentic_path):
            try:
                authentic_df = pd.read_csv(authentic_path)
                
                # Handle date column name differences
                date_col = 'date' if 'date' in authentic_df.columns else 'Date'
                
                # Make sure dates are in datetime format
                authentic_df[date_col] = pd.to_datetime(authentic_df[date_col])
                
                # Update scores for matching dates
                for _, row in sentiment_scores.iterrows():
                    match_idx = authentic_df[authentic_df[date_col] == row['date']].index
                    if not match_idx.empty:
                        authentic_df.loc[match_idx, 'AI Infrastructure'] = row['sentiment_score']
                
                # Save the updated authentic file
                authentic_df.to_csv(authentic_path, index=False)
                logger.info(f"Updated {authentic_path} with new AI Infrastructure scores")
            except Exception as e:
                logger.error(f"Error updating authentic sector history: {e}")
        
        return True
    except Exception as e:
        logger.error(f"Error updating sector history: {e}")
        return False

def main():
    """Main function to update AI Infrastructure scores"""
    logger.info("Starting AI Infrastructure sector score update...")
    
    # Load ticker data
    ticker_data = load_ticker_data()
    logger.info(f"Loaded data for {len(ticker_data)} tickers")
    
    # Calculate sentiment scores
    sentiment_scores = calculate_sector_sentiment(ticker_data)
    if sentiment_scores is not None:
        logger.info(f"Calculated {len(sentiment_scores)} daily sentiment scores")
        
        # Update sector history
        if update_sector_history(sentiment_scores):
            logger.info("Successfully updated AI Infrastructure sector scores")
        else:
            logger.error("Failed to update sector history")
    else:
        logger.error("Failed to calculate sentiment scores")

if __name__ == "__main__":
    main()