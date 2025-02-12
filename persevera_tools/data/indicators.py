import pandas as pd
import sqlalchemy
from typing import Optional

from ..db.operations import read_table

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
    query = "SELECT date, value FROM indicators WHERE code = %(code)s AND field = %(field)s"
    
    # Add date filters if provided
    if start_date:
        query += " AND date >= %(start_date)s"
    if end_date:
        query += " AND date <= %(end_date)s"
        
    query += " ORDER BY date"
    
    params = {
        'code': code,
        'field': field,
        'start_date': start_date,
        'end_date': end_date
    }
    
    df = read_table(sqlalchemy.text(query), params=params, date_columns=['date'])
    return df