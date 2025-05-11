#!/usr/bin/env python3
"""
Database access module for T2D Pulse.
This module provides functions to access the SQLite database containing market cap data.
"""

import os
import sqlite3
import pandas as pd
from datetime import datetime, timedelta
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Database path
DB_PATH = 'market_cap_data.db'

def get_connection():
    """Get a connection to the SQLite database"""
    try:
        conn = sqlite3.connect(DB_PATH)
        return conn
    except sqlite3.Error as e:
        logger.error(f"Error connecting to the database: {e}")
        return None

def get_sectors():
    """Get a list of all sectors"""
    conn = get_connection()
    if not conn:
        return []
    
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sectors ORDER BY name")
        sectors = [row[0] for row in cursor.fetchall()]
        return sectors
    except sqlite3.Error as e:
        logger.error(f"Error getting sectors: {e}")
        return []
    finally:
        conn.close()

def get_sector_market_caps(days=30):
    """
    Get sector market caps for the past N days
    
    Args:
        days (int): Number of days to get data for
        
    Returns:
        DataFrame: DataFrame with date and sector columns
    """
    conn = get_connection()
    if not conn:
        return pd.DataFrame()
    
    try:
        # Calculate the date N days ago
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days)
        
        # Query the database
        query = """
        SELECT s.name as sector, smc.date, smc.market_cap
        FROM sector_market_caps smc
        JOIN sectors s ON smc.sector_id = s.id
        WHERE smc.date >= ?
        ORDER BY smc.date, s.name
        """
        
        # Load into a pandas DataFrame
        df = pd.read_sql_query(query, conn, params=(start_date.strftime('%Y-%m-%d'),))
        
        # Pivot the DataFrame to get sectors as columns
        pivot_df = df.pivot(index='date', columns='sector', values='market_cap')
        
        # Reset the index to make date a column
        pivot_df.reset_index(inplace=True)
        
        return pivot_df
    except Exception as e:
        logger.error(f"Error getting sector market caps: {e}")
        return pd.DataFrame()
    finally:
        conn.close()

def get_sector_sentiment_scores(days=30):
    """
    Get sector sentiment scores for the past N days
    
    Args:
        days (int): Number of days to get data for
        
    Returns:
        DataFrame: DataFrame with date and sector columns
    """
    conn = get_connection()
    if not conn:
        return pd.DataFrame()
    
    try:
        # Calculate the date N days ago
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days)
        
        # Query the database
        query = """
        SELECT s.name as sector, smc.date, smc.sentiment_score
        FROM sector_market_caps smc
        JOIN sectors s ON smc.sector_id = s.id
        WHERE smc.date >= ? AND smc.sentiment_score IS NOT NULL
        ORDER BY smc.date, s.name
        """
        
        # Load into a pandas DataFrame
        df = pd.read_sql_query(query, conn, params=(start_date.strftime('%Y-%m-%d'),))
        
        # Pivot the DataFrame to get sectors as columns
        pivot_df = df.pivot(index='date', columns='sector', values='sentiment_score')
        
        # Reset the index to make date a column
        pivot_df.reset_index(inplace=True)
        
        return pivot_df
    except Exception as e:
        logger.error(f"Error getting sector sentiment scores: {e}")
        return pd.DataFrame()
    finally:
        conn.close()

def get_ticker_market_caps(ticker_symbol, days=30):
    """
    Get market cap data for a specific ticker for the past N days
    
    Args:
        ticker_symbol (str): The ticker symbol
        days (int): Number of days to get data for
        
    Returns:
        DataFrame: DataFrame with date and market_cap columns
    """
    conn = get_connection()
    if not conn:
        return pd.DataFrame()
    
    try:
        # Calculate the date N days ago
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days)
        
        # Query the database
        query = """
        SELECT tmc.date, tmc.market_cap
        FROM ticker_market_caps tmc
        JOIN tickers t ON tmc.ticker_id = t.id
        WHERE t.symbol = ? AND tmc.date >= ?
        ORDER BY tmc.date
        """
        
        # Load into a pandas DataFrame
        df = pd.read_sql_query(query, conn, params=(ticker_symbol, start_date.strftime('%Y-%m-%d')))
        
        return df
    except Exception as e:
        logger.error(f"Error getting ticker market caps: {e}")
        return pd.DataFrame()
    finally:
        conn.close()

def get_all_ticker_data(days=30):
    """
    Get market cap data for all tickers for the past N days
    
    Args:
        days (int): Number of days to get data for
        
    Returns:
        DataFrame: DataFrame with ticker, date and market_cap columns
    """
    conn = get_connection()
    if not conn:
        return pd.DataFrame()
    
    try:
        # Calculate the date N days ago
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days)
        
        # Query the database
        query = """
        SELECT t.symbol as ticker, tmc.date, tmc.market_cap
        FROM ticker_market_caps tmc
        JOIN tickers t ON tmc.ticker_id = t.id
        WHERE tmc.date >= ?
        ORDER BY t.symbol, tmc.date
        """
        
        # Load into a pandas DataFrame
        df = pd.read_sql_query(query, conn, params=(start_date.strftime('%Y-%m-%d'),))
        
        return df
    except Exception as e:
        logger.error(f"Error getting all ticker data: {e}")
        return pd.DataFrame()
    finally:
        conn.close()

def get_sector_tickers(sector_name):
    """
    Get all tickers in a specific sector
    
    Args:
        sector_name (str): The name of the sector
        
    Returns:
        list: List of ticker symbols
    """
    conn = get_connection()
    if not conn:
        return []
    
    try:
        cursor = conn.cursor()
        query = """
        SELECT t.symbol
        FROM tickers t
        JOIN ticker_sectors ts ON t.id = ts.ticker_id
        JOIN sectors s ON ts.sector_id = s.id
        WHERE s.name = ?
        ORDER BY t.symbol
        """
        cursor.execute(query, (sector_name,))
        tickers = [row[0] for row in cursor.fetchall()]
        return tickers
    except sqlite3.Error as e:
        logger.error(f"Error getting tickers for sector {sector_name}: {e}")
        return []
    finally:
        conn.close()