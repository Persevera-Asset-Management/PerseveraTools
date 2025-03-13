from datetime import datetime, timedelta
from typing import List
import pandas as pd


def get_holidays() -> List[pd.Timestamp]:
    """Read and return ANBIMA holidays from database."""
    # Use lazy import to avoid circular dependency
    from ..db.operations import read_sql
    
    query = """SELECT * FROM feriados_anbima"""
    df = read_sql(query, date_columns=['date'])
    return df['date'].tolist()

def excel_to_datetime(serial_date):
    """Convert Excel serial date to datetime."""
    try:
        base_date = datetime(1970, 1, 1)
        return base_date + timedelta(days=serial_date)
    except (TypeError, ValueError):
        return None