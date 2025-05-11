#!/usr/bin/env python3
"""
Database access module for T2D Pulse.
This module provides a standardized interface for accessing ticker, sector, and market cap data
from the PostgreSQL database.
"""
import os
import pandas as pd
import psycopg2
from psycopg2.extras import RealDictCursor
import logging
from typing import Dict, List, Tuple, Optional, Any, Union
from datetime import datetime, date, timedelta

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Get database connection information from environment variables
DB_URL = os.environ.get('DATABASE_URL')

def get_db_connection():
    """Get a connection to the PostgreSQL database"""
    try:
        conn = psycopg2.connect(DB_URL)
        return conn
    except Exception as e:
        logger.error(f"Error connecting to database: {e}")
        raise

def get_sectors() -> List[Dict[str, Any]]:
    """Get all sectors from the database"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        cursor.execute("SELECT id, name, description FROM sectors ORDER BY name")
        sectors = cursor.fetchall()
        
        cursor.close()
        conn.close()
        
        return sectors
    except Exception as e:
        logger.error(f"Error getting sectors: {e}")
        return []

def get_tickers() -> List[Dict[str, Any]]:
    """Get all tickers from the database"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        cursor.execute("SELECT id, symbol, name FROM tickers ORDER BY symbol")
        tickers = cursor.fetchall()
        
        cursor.close()
        conn.close()
        
        return tickers
    except Exception as e:
        logger.error(f"Error getting tickers: {e}")
        return []

def get_sector_tickers(sector_id: Optional[int] = None) -> List[Dict[str, Any]]:
    """
    Get tickers for a specific sector or all sector-ticker relationships
    
    Args:
        sector_id: Optional sector ID to filter by
        
    Returns:
        List of dictionaries with sector_id, ticker_id, sector_name, ticker_symbol
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        if sector_id is not None:
            cursor.execute("""
                SELECT ts.sector_id, ts.ticker_id, s.name as sector_name, t.symbol as ticker_symbol
                FROM ticker_sectors ts
                JOIN sectors s ON ts.sector_id = s.id
                JOIN tickers t ON ts.ticker_id = t.id
                WHERE ts.sector_id = %s
                ORDER BY t.symbol
            """, (sector_id,))
        else:
            cursor.execute("""
                SELECT ts.sector_id, ts.ticker_id, s.name as sector_name, t.symbol as ticker_symbol
                FROM ticker_sectors ts
                JOIN sectors s ON ts.sector_id = s.id
                JOIN tickers t ON ts.ticker_id = t.id
                ORDER BY s.name, t.symbol
            """)
        
        sector_tickers = cursor.fetchall()
        
        cursor.close()
        conn.close()
        
        return sector_tickers
    except Exception as e:
        logger.error(f"Error getting sector tickers: {e}")
        return []

def get_ticker_market_caps(ticker_symbols: Optional[List[str]] = None, 
                          start_date: Optional[str] = None,
                          end_date: Optional[str] = None) -> pd.DataFrame:
    """
    Get market cap data for specific tickers and date range
    
    Args:
        ticker_symbols: Optional list of ticker symbols to filter by
        start_date: Optional start date in YYYY-MM-DD format
        end_date: Optional end date in YYYY-MM-DD format
        
    Returns:
        DataFrame with ticker_symbol, date, market_cap columns
    """
    try:
        conn = get_db_connection()
        
        # Build the query with optional filters
        query = """
            SELECT t.symbol as ticker_symbol, tmc.date, tmc.market_cap
            FROM ticker_market_caps tmc
            JOIN tickers t ON tmc.ticker_id = t.id
            WHERE 1=1
        """
        params = []
        
        if ticker_symbols:
            placeholders = ', '.join(['%s'] * len(ticker_symbols))
            query += f" AND t.symbol IN ({placeholders})"
            params.extend(ticker_symbols)
        
        if start_date:
            query += " AND tmc.date >= %s"
            params.append(start_date)
        
        if end_date:
            query += " AND tmc.date <= %s"
            params.append(end_date)
        
        query += " ORDER BY t.symbol, tmc.date"
        
        # Execute the query and get results as a DataFrame
        df = pd.read_sql_query(query, conn, params=params)
        conn.close()
        
        return df
    except Exception as e:
        logger.error(f"Error getting ticker market caps: {e}")
        return pd.DataFrame()

def get_sector_market_caps(sector_names: Optional[List[str]] = None,
                          start_date: Optional[str] = None,
                          end_date: Optional[str] = None) -> pd.DataFrame:
    """
    Get market cap data for specific sectors and date range
    
    Args:
        sector_names: Optional list of sector names to filter by
        start_date: Optional start date in YYYY-MM-DD format
        end_date: Optional end date in YYYY-MM-DD format
        
    Returns:
        DataFrame with sector_name, date, market_cap columns
    """
    try:
        conn = get_db_connection()
        
        # Build the query with optional filters
        query = """
            SELECT s.name as sector_name, smc.date, smc.market_cap
            FROM sector_market_caps smc
            JOIN sectors s ON smc.sector_id = s.id
            WHERE 1=1
        """
        params = []
        
        if sector_names:
            placeholders = ', '.join(['%s'] * len(sector_names))
            query += f" AND s.name IN ({placeholders})"
            params.extend(sector_names)
        
        if start_date:
            query += " AND smc.date >= %s"
            params.append(start_date)
        
        if end_date:
            query += " AND smc.date <= %s"
            params.append(end_date)
        
        query += " ORDER BY s.name, smc.date"
        
        # Execute the query and get results as a DataFrame
        df = pd.read_sql_query(query, conn, params=params)
        conn.close()
        
        return df
    except Exception as e:
        logger.error(f"Error getting sector market caps: {e}")
        return pd.DataFrame()

def get_latest_market_caps() -> Dict[str, float]:
    """
    Get the latest market cap for each sector
    
    Returns:
        Dictionary mapping sector names to market cap values
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Get the most recent date with data
        cursor.execute("SELECT MAX(date) FROM sector_market_caps")
        latest_date = cursor.fetchone()[0]
        
        if not latest_date:
            logger.warning("No market cap data found in database")
            return {}
        
        # Get all sector market caps for the latest date
        cursor.execute("""
            SELECT s.name, smc.market_cap
            FROM sector_market_caps smc
            JOIN sectors s ON smc.sector_id = s.id
            WHERE smc.date = %s
        """, (latest_date,))
        
        results = cursor.fetchall()
        
        cursor.close()
        conn.close()
        
        # Convert to dictionary
        market_caps = {sector: market_cap for sector, market_cap in results}
        
        logger.info(f"Retrieved latest market caps from {latest_date}")
        return market_caps
    except Exception as e:
        logger.error(f"Error getting latest market caps: {e}")
        return {}

def get_latest_sector_data() -> pd.DataFrame:
    """
    Get the latest data for all sectors including market cap and optional sentiment score
    
    Returns:
        DataFrame with sector_name, market_cap, sentiment_score columns
    """
    try:
        conn = get_db_connection()
        
        # Get the most recent date with data
        cursor = conn.cursor()
        cursor.execute("SELECT MAX(date) FROM sector_market_caps")
        latest_date = cursor.fetchone()[0]
        
        if not latest_date:
            logger.warning("No market cap data found in database")
            return pd.DataFrame()
        
        # Get all sector data for the latest date
        query = """
            SELECT s.name as sector_name, smc.market_cap, smc.sentiment_score
            FROM sector_market_caps smc
            JOIN sectors s ON smc.sector_id = s.id
            WHERE smc.date = %s
            ORDER BY s.name
        """
        
        df = pd.read_sql_query(query, conn, params=(latest_date,))
        conn.close()
        
        logger.info(f"Retrieved latest sector data from {latest_date}")
        return df
    except Exception as e:
        logger.error(f"Error getting latest sector data: {e}")
        return pd.DataFrame()

def get_sector_sparkline_data(days: int = 30) -> Dict[str, List[float]]:
    """
    Get data for sector sparklines showing trends over the specified number of days
    
    Args:
        days: Number of days to include in the sparkline data
        
    Returns:
        Dictionary mapping sector names to lists of market cap values
    """
    try:
        conn = get_db_connection()
        
        # Calculate the start date
        cursor = conn.cursor()
        cursor.execute("SELECT MAX(date) FROM sector_market_caps")
        latest_date = cursor.fetchone()[0]
        
        if not latest_date:
            logger.warning("No market cap data found in database")
            return {}
        
        start_date = latest_date - timedelta(days=days)
        
        # Get sector market caps for the date range
        query = """
            SELECT s.name as sector_name, smc.date, smc.market_cap
            FROM sector_market_caps smc
            JOIN sectors s ON smc.sector_id = s.id
            WHERE smc.date BETWEEN %s AND %s
            ORDER BY s.name, smc.date
        """
        
        df = pd.read_sql_query(query, conn, params=(start_date, latest_date))
        conn.close()
        
        if df.empty:
            logger.warning(f"No sector market cap data found between {start_date} and {latest_date}")
            return {}
        
        # Convert to dictionary of lists
        sparkline_data = {}
        for sector, group in df.groupby('sector_name'):
            sparkline_data[sector] = group['market_cap'].tolist()
        
        logger.info(f"Retrieved sparkline data for {len(sparkline_data)} sectors")
        return sparkline_data
    except Exception as e:
        logger.error(f"Error getting sector sparkline data: {e}")
        return {}

def insert_ticker_market_cap(ticker_symbol: str, date_str: str, market_cap: float) -> bool:
    """
    Insert or update a ticker market cap value
    
    Args:
        ticker_symbol: Ticker symbol
        date_str: Date in YYYY-MM-DD format
        market_cap: Market cap value
        
    Returns:
        True if successful, False otherwise
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Get the ticker ID
        cursor.execute("SELECT id FROM tickers WHERE symbol = %s", (ticker_symbol,))
        result = cursor.fetchone()
        
        if not result:
            logger.warning(f"Ticker {ticker_symbol} not found in database")
            return False
        
        ticker_id = result[0]
        
        # Insert or update the market cap
        cursor.execute("""
            INSERT INTO ticker_market_caps (ticker_id, date, market_cap)
            VALUES (%s, %s, %s)
            ON CONFLICT (ticker_id, date) 
            DO UPDATE SET market_cap = EXCLUDED.market_cap
        """, (ticker_id, date_str, market_cap))
        
        conn.commit()
        cursor.close()
        conn.close()
        
        logger.info(f"Updated market cap for {ticker_symbol} on {date_str}: ${market_cap:,.2f}")
        return True
    except Exception as e:
        logger.error(f"Error inserting ticker market cap: {e}")
        return False

def insert_sector_market_cap(sector_name: str, date_str: str, market_cap: float, 
                             sentiment_score: Optional[float] = None) -> bool:
    """
    Insert or update a sector market cap value
    
    Args:
        sector_name: Sector name
        date_str: Date in YYYY-MM-DD format
        market_cap: Market cap value
        sentiment_score: Optional sentiment score
        
    Returns:
        True if successful, False otherwise
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Get the sector ID
        cursor.execute("SELECT id FROM sectors WHERE name = %s", (sector_name,))
        result = cursor.fetchone()
        
        if not result:
            logger.warning(f"Sector {sector_name} not found in database")
            return False
        
        sector_id = result[0]
        
        # Insert or update the market cap
        if sentiment_score is not None:
            cursor.execute("""
                INSERT INTO sector_market_caps (sector_id, date, market_cap, sentiment_score)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (sector_id, date) 
                DO UPDATE SET market_cap = EXCLUDED.market_cap, sentiment_score = EXCLUDED.sentiment_score
            """, (sector_id, date_str, market_cap, sentiment_score))
        else:
            cursor.execute("""
                INSERT INTO sector_market_caps (sector_id, date, market_cap)
                VALUES (%s, %s, %s)
                ON CONFLICT (sector_id, date) 
                DO UPDATE SET market_cap = EXCLUDED.market_cap
            """, (sector_id, date_str, market_cap))
        
        conn.commit()
        cursor.close()
        conn.close()
        
        logger.info(f"Updated market cap for {sector_name} on {date_str}: ${market_cap:,.2f}")
        return True
    except Exception as e:
        logger.error(f"Error inserting sector market cap: {e}")
        return False

def calculate_sector_market_caps(date_str: str) -> bool:
    """
    Calculate market caps for all sectors on a specific date based on ticker market caps
    
    Args:
        date_str: Date in YYYY-MM-DD format
        
    Returns:
        True if successful, False otherwise
    """
    try:
        conn = get_db_connection()
        
        # Get all sector-ticker relationships
        cursor = conn.cursor()
        cursor.execute("""
            SELECT s.id as sector_id, s.name as sector_name, t.id as ticker_id, t.symbol as ticker_symbol
            FROM ticker_sectors ts
            JOIN sectors s ON ts.sector_id = s.id
            JOIN tickers t ON ts.ticker_id = t.id
        """)
        sector_tickers = cursor.fetchall()
        
        # Get all ticker market caps for the date
        cursor.execute("""
            SELECT ticker_id, market_cap
            FROM ticker_market_caps
            WHERE date = %s
        """, (date_str,))
        ticker_market_caps = {tid: mc for tid, mc in cursor.fetchall()}
        
        # Calculate sector market caps
        sector_data = {}
        for sector_id, sector_name, ticker_id, ticker_symbol in sector_tickers:
            if sector_id not in sector_data:
                sector_data[sector_id] = {
                    'name': sector_name,
                    'total_market_cap': 0,
                    'tickers': []
                }
            
            # Add the ticker market cap to the sector total
            if ticker_id in ticker_market_caps:
                market_cap = ticker_market_caps[ticker_id]
                sector_data[sector_id]['total_market_cap'] += market_cap
                sector_data[sector_id]['tickers'].append({
                    'symbol': ticker_symbol,
                    'market_cap': market_cap
                })
        
        # Insert the sector market caps
        success_count = 0
        for sector_id, data in sector_data.items():
            cursor.execute("""
                INSERT INTO sector_market_caps (sector_id, date, market_cap)
                VALUES (%s, %s, %s)
                ON CONFLICT (sector_id, date) 
                DO UPDATE SET market_cap = EXCLUDED.market_cap
            """, (sector_id, date_str, data['total_market_cap']))
            success_count += 1
        
        conn.commit()
        cursor.close()
        conn.close()
        
        logger.info(f"Calculated market caps for {success_count} sectors on {date_str}")
        return True
    except Exception as e:
        logger.error(f"Error calculating sector market caps: {e}")
        return False

def ensure_market_cap_consistency() -> bool:
    """
    Check and fix any inconsistencies between ticker and sector market caps
    
    Returns:
        True if successful, False otherwise
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Get all dates with ticker market caps
        cursor.execute("SELECT DISTINCT date FROM ticker_market_caps ORDER BY date")
        dates = [row[0] for row in cursor.fetchall()]
        
        if not dates:
            logger.warning("No ticker market cap data found")
            return False
        
        # For each date, recalculate sector market caps
        for date_str in dates:
            # Check if we already have sector data for this date
            cursor.execute("SELECT COUNT(*) FROM sector_market_caps WHERE date = %s", (date_str,))
            count = cursor.fetchone()[0]
            
            # If we don't have sector data or we want to force recalculation
            if count == 0:
                logger.info(f"Calculating sector market caps for {date_str}")
                calculate_sector_market_caps(date_str.strftime('%Y-%m-%d'))
        
        conn.close()
        logger.info("Ensured market cap consistency for all dates")
        return True
    except Exception as e:
        logger.error(f"Error ensuring market cap consistency: {e}")
        return False