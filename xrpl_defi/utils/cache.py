from functools import lru_cache, wraps
from typing import List, Callable, Any


def selective_lru_cache(arg_names: List[str], maxsize: int = 128) -> Callable:
    """
    LRU cache decorator that caches based on specified argument names.
    
    Args:
        arg_names: List of argument names to use for cache key
        maxsize: Maximum size of the cache (default: 128)
    
    Returns:
        Decorated function with selective LRU caching
    """
    def decorator(func: Callable) -> Callable:
        @lru_cache(maxsize=maxsize)
        def cached_func(*args, **kwargs) -> Any:
            # Get function signature
            sig = func.__code__.co_varnames[:func.__code__.co_argcount]
            
            # Create cache key from specified arguments
            cache_key = []
            for name in arg_names:
                if name in kwargs:
                    cache_key.append(kwargs[name])
                else:
                    try:
                        idx = sig.index(name)
                        cache_key.append(args[idx])
                    except (IndexError, ValueError):
                        raise ValueError(f"Argument '{name}' not found in function signature")
            
            # Convert cache_key to tuple for hashability
            cache_key = tuple(cache_key)
            
            # Call the function with original arguments
            return func(*args, **kwargs)
        
        # Preserve original function metadata
        @wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            return cached_func(*args, **kwargs)
        
        # Expose cache info and clear methods
        wrapper.cache_info = cached_func.cache_info
        wrapper.cache_clear = cached_func.cache_clear
        
        return wrapper
    return decorator