#!/usr/bin/env python3
# sector_ema_integration.py
# -----------------------------------------------------------
# Integration module for sector EMAs in the sentiment calculation

import os
import pandas as pd
import numpy as np
from ema_calculator import load_sector_emas
from config import SECTOR_NAME_MAP, EMA_WEIGHT, EMA_NORMALIZATION_FACTOR

def get_sector_ema_factors():
    """
    Get sector EMA factors for sentiment calculation
    
    Returns:
        dict: Dictionary with sector factors {sector: factor}
              where factor is a value between -1 and 1
    """
    # Load sector EMAs
    sector_emas = load_sector_emas()
    if not sector_emas:
        return {}
        
    # Calculate factors
    factors = {}
    for sector, (ema, percent_change) in sector_emas.items():
        # Map sector name if needed
        mapped_sector = SECTOR_NAME_MAP.get(sector, sector)
        
        # Normalize percent change to -1 to 1 scale
        # For example: -5% to +5% maps to -1 to +1
        factor = max(-1, min(1, percent_change / EMA_NORMALIZATION_FACTOR))
        
        factors[mapped_sector] = factor
        
    return factors

def apply_ema_factors_to_sector_scores(sector_scores, ema_factors=None):
    """
    Apply EMA factors to sector scores
    
    Args:
        sector_scores (list): List of sector score dictionaries
        ema_factors (dict, optional): Dictionary of sector EMA factors
        
    Returns:
        list: Updated sector scores
    """
    if not ema_factors:
        ema_factors = get_sector_ema_factors()
        
    if not ema_factors:
        return sector_scores
        
    updated_scores = []
    for sector_data in sector_scores:
        sector_name = sector_data['sector']
        if sector_name in ema_factors:
            # Get EMA factor for this sector
            ema_factor = ema_factors[sector_name]
            
            # Apply factor to score with specified weight
            # The raw score is in range -1 to +1
            raw_score = sector_data['score']  # Original -1 to +1 score
            
            # Add EMA influence
            adjusted_score = raw_score + (ema_factor * EMA_WEIGHT)
            
            # Ensure score stays within -1 to +1 range
            adjusted_score = max(-1, min(1, adjusted_score))
            
            # Update score
            sector_data['score'] = adjusted_score
            
            # Also update normalized score (0-100 scale)
            sector_data['normalized_score'] = ((adjusted_score + 1) / 2) * 100
            
            # Add info about EMA contribution
            sector_data['ema_factor'] = ema_factor
            sector_data['ema_contribution'] = ema_factor * EMA_WEIGHT
            
        updated_scores.append(sector_data)
        
    return updated_scores