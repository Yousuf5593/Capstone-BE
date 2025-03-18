from flask_caching import Cache
from datetime import datetime

# Cache initialization (reuse in the routes and services)
def get_cache_key(start_date, end_date):
    """
    Generate a cache key based on start_date and end_date.
    """
    return f"sentiment_data_{start_date}_{end_date}"

def retrieve_from_cache(cache, key):
    """
    Retrieve data from cache based on key.
    """
    cached_data = cache.get(key)
    if cached_data:
        return cached_data
    return None


def store_in_cache(cache, key, data):
    """
    Store the result in cache.
    """
    cache.set(key, data)
    # Track all keys used in a global set
    existing_keys = cache.get("cache_keys_set") or set()
    existing_keys.add(key)
    cache.set("cache_keys_set", existing_keys)