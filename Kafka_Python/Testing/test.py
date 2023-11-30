import numpy as np

def prices_udf(value):
    value_str = value.decode('utf-8').replace('"', '').strip()
    
    try:
        return value_str
    except ValueError:
        return None
    

print(prices_udf([b'56.2']))