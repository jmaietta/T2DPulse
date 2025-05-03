import json
import os

# This file contains hardcoded data from May 2nd to be used when the date is a weekend
# This ensures sector cards and T2D Pulse show the correct May 2nd data

# Direct data from the exportable file for May 2nd
# These are the authentic May 2nd values directly from the file
MAY_2ND_SECTOR_SCORES = {
    'SMB SaaS': 52.5,
    'Enterprise SaaS': 55.5,
    'Cloud Infrastructure': 56.0,
    'AdTech': 55.0,
    'Fintech': 55.5,
    'Consumer Internet': 53.5,
    'eCommerce': 55.0,
    'Cybersecurity': 53.5,
    'Dev Tools / Analytics': 53.5,
    'Semiconductors': 61.0,
    'AI Infrastructure': 56.0,
    'Vertical SaaS': 52.5,
    'IT Services / Legacy Tech': 59.5,
    'Hardware / Devices': 60.5
}

# T2D Pulse score calculated using May 2nd data with equal weights
# Authentic score using the calculation from authentic_historical_data.py
MAY_2ND_T2D_PULSE_SCORE = 55.7

# Sector drivers mapping - key macro factors influencing each sector
SECTOR_DRIVERS = {
    'SMB SaaS': ['Unemployment Rate', 'Software Job Postings', 'Consumer Sentiment'],
    'Enterprise SaaS': ['GDP', 'Software Job Postings', 'Inflation Rate'],
    'Cloud Infrastructure': ['GDP', 'Interest Rates', 'NASDAQ Trend'],
    'AdTech': ['Consumer Sentiment', 'NASDAQ Trend', 'GDP'],
    'Fintech': ['Interest Rates', 'NASDAQ Trend', 'Inflation Rate'],
    'Consumer Internet': ['Consumer Sentiment', 'Unemployment Rate', 'NASDAQ Trend'],
    'eCommerce': ['Consumer Sentiment', 'Inflation Rate', 'Unemployment Rate'],
    'Cybersecurity': ['Software Job Postings', 'NASDAQ Trend', 'Enterprise IT Spending'],
    'Dev Tools / Analytics': ['Software Job Postings', 'GDP', 'NASDAQ Trend'],
    'Semiconductors': ['Hardware Demand', 'Supply Chain Health', 'NASDAQ Trend'],
    'AI Infrastructure': ['GDP', 'NASDAQ Trend', 'Enterprise IT Spending'],
    'Vertical SaaS': ['Industry-Specific Trends', 'GDP', 'Software Job Postings'],
    'IT Services / Legacy Tech': ['GDP', 'Enterprise IT Spending', 'Software Job Postings'],
    'Hardware / Devices': ['Consumer Sentiment', 'Supply Chain Health', 'NASDAQ Trend']
}

# Representative ticker symbols for each sector
SECTOR_TICKERS = {
    'SMB SaaS': ['ZM', 'DOCU', 'WDAY', 'BILL', 'HUBS'],
    'Enterprise SaaS': ['CRM', 'NOW', 'MSFT', 'ORCL', 'TEAM'],
    'Cloud Infrastructure': ['AMZN', 'MSFT', 'GOOGL', 'NET', 'DDOG'],
    'AdTech': ['TTD', 'ROKU', 'CRTO', 'MGNI', 'APPS'],
    'Fintech': ['SQ', 'PYPL', 'AFRM', 'COIN', 'HOOD'],
    'Consumer Internet': ['META', 'SNAP', 'PINS', 'GOOGL', 'TWTR'],
    'eCommerce': ['AMZN', 'SHOP', 'ETSY', 'W', 'BABA'],
    'Cybersecurity': ['CRWD', 'PANW', 'ZS', 'OKTA', 'S'],
    'Dev Tools / Analytics': ['DDOG', 'MDB', 'PD', 'SPLK', 'ESTC'],
    'Semiconductors': ['NVDA', 'AMD', 'AVGO', 'TSM', 'QCOM'],
    'AI Infrastructure': ['NVDA', 'GOOGL', 'MSFT', 'IBM', 'AMZN'],
    'Vertical SaaS': ['VEEV', 'PCTY', 'PTON', 'REAL', 'ATVI'],
    'IT Services / Legacy Tech': ['IBM', 'ACN', 'CSCO', 'HPQ', 'DELL'],
    'Hardware / Devices': ['AAPL', 'MSFT', 'SONO', 'HEAR', 'FIT']
}

def get_may2nd_sector_data():
    """Get hardcoded May 2nd sector data in the format expected by the dashboard
    
    Returns:
        list: A list of dictionaries with sector data
    """
    sector_data = []
    for sector, score in MAY_2ND_SECTOR_SCORES.items():
        # Convert normalized score (0-100) to raw score (-1 to +1)
        raw_score = (score / 50.0) - 1.0
        
        # Determine stance based on score
        if score >= 60:
            stance = "Bullish"
            takeaway = "Outperforming peers"
        elif score <= 30:
            stance = "Bearish"
            takeaway = "Bearish macro setup"
        else:
            stance = "Neutral"
            takeaway = "Neutral – monitor trends"
        
        # Create sector data object
        sector_data.append({
            "sector": sector,
            "score": raw_score,
            "normalized_score": score,
            "stance": stance,
            "takeaway": takeaway,
            "drivers": SECTOR_DRIVERS.get(sector, []),
            "tickers": SECTOR_TICKERS.get(sector, [])
        })
    
    return sector_data
