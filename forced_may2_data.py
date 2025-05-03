import json
import os

# This file contains hardcoded data from May 2nd to be used when the date is a weekend
# This ensures sector cards and T2D Pulse show the correct May 2nd data

# Direct data from May 2nd file data/authentic_sector_history_2025-05-02.csv
# These are the authentic May 2nd values
MAY_2ND_SECTOR_SCORES = {
    'SMB SaaS': 52.0,
    'Enterprise SaaS': 53.5,
    'Cloud Infrastructure': 55.0,
    'AdTech': 52.0,
    'Fintech': 53.5,
    'Consumer Internet': 51.0,
    'eCommerce': 52.0,
    'Cybersecurity': 53.5,
    'Dev Tools / Analytics': 52.0,
    'Semiconductors': 55.5,
    'AI Infrastructure': 55.0,
    'Vertical SaaS': 51.0,
    'IT Services / Legacy Tech': 56.0,
    'Hardware / Devices': 52.5
}

# T2D Pulse score calculated using May 2nd data with equal weights
MAY_2ND_T2D_PULSE_SCORE = 53.2

def get_may2nd_sector_data():
    """
    Return a list of sector data dictionaries with May 2nd values
    in the format expected by the sector cards generation code
    """
    authentic_scores = []
    
    for sector, score_value in MAY_2ND_SECTOR_SCORES.items():
        # Convert to -1 to +1 scale for consistency with sector cards code
        raw_score = (score_value / 50.0) - 1.0
        
        # Create sector data structure in the expected format
        sector_data = {
            "sector": sector,
            "score": raw_score,
            "normalized_score": score_value,  # Already in 0-100 scale
            "stance": "Bullish" if score_value >= 60 else "Bearish" if score_value <= 30 else "Neutral",
            "takeaway": "Outperforming peers" if score_value >= 60 else 
                      "Bearish macro setup" if score_value <= 30 else 
                      "Neutral – monitor trends",
            "drivers": [],  # Empty drivers for historical data
            "tickers": []   # Empty tickers for historical data
        }
        authentic_scores.append(sector_data)
    
    return authentic_scores

def get_may2nd_t2d_pulse_score():
    """
    Return the T2D Pulse score calculated using May 2nd data
    """
    return MAY_2ND_T2D_PULSE_SCORE

def get_may2nd_sector_dict():
    """
    Return a dictionary of May 2nd sector scores
    for use in T2D Pulse score calculation
    """
    return MAY_2ND_SECTOR_SCORES