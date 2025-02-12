import pandas as pd
import sqlalchemy
from typing import Optional

from ..db.operations import read_sql

def get_series(code: str, 
               start_date: Optional[str] = None, 
               end_date: Optional[str] = None,
               field: str = 'close') -> pd.DataFrame:
    """Get time series data for a specific indicator from the database.
    
    Args:
        code: Indicator code to retrieve
        start_date: Optional start date filter (format: 'YYYY-MM-DD')
        end_date: Optional end date filter (format: 'YYYY-MM-DD')
        field: Field to retrieve (default: 'close')
        
    Returns:
        DataFrame with date and value columns for the indicator
    """
    query = "SELECT date, value FROM indicators WHERE code = '{code}' AND field = '{field}'".format(
        code=code, field=field
    )
    
    if start_date:
        query += f" AND date >= '{start_date}'"
    if end_date:
        query += f" AND date <= '{end_date}'"
        
    query += " ORDER BY date"
    
    return read_sql(query, date_columns=['date'])