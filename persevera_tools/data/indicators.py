from datetime import datetime
from typing import Optional, Union, List
import pandas as pd

from ..db.operations import read_sql

def get_series(code: Union[str, List[str]], 
               start_date: Optional[str] = None, 
               end_date: Optional[str] = None,
               field: str = 'close') -> pd.DataFrame:
    """Get time series data for one or more indicators from the database.
    
    Args:
        code: Single indicator code or list of codes to retrieve
        start_date: Optional start date filter (format: 'YYYY-MM-DD')
        end_date: Optional end date filter (format: 'YYYY-MM-DD')
        field: Field to retrieve (default: 'close')
        
    Returns:
        DataFrame with date and value columns for the indicator(s)
        If multiple codes are provided, the DataFrame will have one column per code
        
    Raises:
        ValueError: If dates are in invalid format or if end_date is before start_date
    """
    # Validate codes
    if isinstance(code, str):
        codes = [code]
    elif isinstance(code, list):
        codes = code
    else:
        raise ValueError("code must be a string or list of strings")
    
    if not all(isinstance(c, str) and c for c in codes):
        raise ValueError("All codes must be non-empty strings")

    # Validate dates if provided
    date_format = "%Y-%m-%d"
    if start_date:
        try:
            start_dt = datetime.strptime(start_date, date_format)
        except ValueError:
            raise ValueError("start_date must be in YYYY-MM-DD format")
    
    if end_date:
        try:
            end_dt = datetime.strptime(end_date, date_format)
        except ValueError:
            raise ValueError("end_date must be in YYYY-MM-DD format")
    
    if start_date and end_date and start_dt > end_dt:
        raise ValueError("end_date cannot be before start_date")

    # Build query using validated parameters
    codes_str = "','".join(codes)
    query = f"""
        SELECT date, code, value 
        FROM indicadores 
        WHERE code IN ('{codes_str}') 
        AND field = '{field}'
    """
    
    if start_date:
        query += f" AND date >= '{start_date}'"
    if end_date:
        query += f" AND date <= '{end_date}'"
        
    query += " ORDER BY date, code"
    
    df = read_sql(query, date_columns=['date'])
    
    if df.empty:
        raise ValueError(f"No data found for code(s) {codes} with field '{field}'")
    
    # Pivot the data if multiple codes to have one column per code
    if len(codes) > 1:
        df = df.pivot(index='date', columns='code', values='value')
        df.columns.name = None  # Remove column name
    else:
        df = df.drop(columns='code').set_index('date')
    
    # Reindex columns by inputed order
    df = df.reindex(columns=codes)
    
    return df