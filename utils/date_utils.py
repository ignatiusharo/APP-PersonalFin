import pandas as pd
from datetime import datetime, timedelta

def get_accounting_month(date_input):
    """
    Calculates the accounting month based on a custom cutoff date (25th).
    If the day is >= 25, it belongs to the *next* month.
    Otherwise, it belongs to the *current* month.
    
    Args:
        date_input: datetime object, pandas Timestamp, or string.
        
    Returns:
        str: Accounting month in 'YYYY-MM' format.
    """
    if pd.isna(date_input):
        return None
        
    try:
        dt = pd.to_datetime(date_input)
    except:
        return None
        
    # If day >= 25, add roughly one month to get next month's year/month
    # We use day=28 to be safe for all months when adding days, but replace is safer for month logic
    if dt.day >= 25:
        # Move to first day of next month safely
        next_month_dt = (dt.replace(day=1) + timedelta(days=32)).replace(day=1)
        return next_month_dt.strftime('%Y-%m')
    else:
        return dt.strftime('%Y-%m')
