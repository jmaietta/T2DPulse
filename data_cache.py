import pandas as pd
import threading

# Global data cache dictionary with thread lock for thread safety
_data_cache = {}
_cache_lock = threading.Lock()

# Function to get data from the cache
def get_data(data_type):
    """Get data from the cache for a specific data type"""
    with _cache_lock:
        return _data_cache.get(data_type, pd.DataFrame())

# Function to set data in the cache
def set_data(data_type, data):
    """Set data in the cache for a specific data type"""
    with _cache_lock:
        _data_cache[data_type] = data

# Function to get all data from the cache
def get_all_data():
    """Get all data from the cache"""
    with _cache_lock:
        return _data_cache.copy()

# Function to update the cache with multiple datasets
def update_cache(data_dict):
    """Update the cache with multiple datasets"""
    with _cache_lock:
        for data_type, data in data_dict.items():
            _data_cache[data_type] = data
