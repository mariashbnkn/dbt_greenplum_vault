import time
from datetime import datetime
from functools import wraps

def measure_time(func):
    @wraps(func) 
    def wrapper(*args, **kwargs):
        start_time = time.time()
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] ⏳ Running '{func.__name__}'...")
        
        result = func(*args, **kwargs)  
        
        end_time = time.time()
        elapsed_time = end_time - start_time
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] ✅ '{func.__name__}' completed in: {elapsed_time:.2f} seconds")
        
        return result
    return wrapper