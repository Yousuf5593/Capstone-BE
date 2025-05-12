from flask_caching import Cache
from datetime import datetime
import json
import pandas as pd

# Cache initialization (reuse in the routes and services)
def get_cache_key(start_date, end_date):
    """
    Generate a cache key based on start_date and end_date.
    """
    return f"sentiment_data_{start_date}_{end_date}"

def store_in_cache(cache, key, data):
    """
    Store the result in cache as serialized JSON.
    """
    def convert_timestamps(obj):
        if isinstance(obj, pd.Timestamp):
            return obj.isoformat()
        elif isinstance(obj, dict):
            return {k: convert_timestamps(v) for k, v in obj.items()}
        elif isinstance(obj, (list, tuple)):
            return [convert_timestamps(item) for item in obj]
        return obj
    
    serializable_data = convert_timestamps(data)
    
    # Store the serialized data
    cache.set(key, json.dumps(serializable_data))

def retrieve_from_cache(cache, key):
    """
    Retrieve data from cache based on key and return deserialized data.
    """
    cached_data = cache.get(key)
    if cached_data:
        try:
            return json.loads(cached_data)
        except json.JSONDecodeError:
            return cached_data  # Return as-is if not JSON
    return None