from datetime import datetime
from typing import Optional, Union, List
import pandas as pd

from ..db.operations import read_sql

def get_series(code: Union[str, List[str]], 
               start_date: Optional[Union[str, datetime, pd.Timestamp]] = None, 
               end_date: Optional[Union[str, datetime, pd.Timestamp]] = None,
               field: str = 'close') -> pd.DataFrame:
    """Get time series data for one or more indicators from the database.
    
    Args:
        code: Single indicator code or list of codes to retrieve
        start_date: Optional start date filter as string 'YYYY-MM-DD', datetime, or pandas Timestamp
        end_date: Optional end date filter as string 'YYYY-MM-DD', datetime, or pandas Timestamp
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

    # Convert and validate dates if provided
    start_date_str = None
    end_date_str = None
    
    if start_date is not None:
        if isinstance(start_date, str):
            try:
                start_dt = datetime.strptime(start_date, "%Y-%m-%d")
                start_date_str = start_date
            except ValueError:
                raise ValueError("start_date string must be in YYYY-MM-DD format")
        elif isinstance(start_date, (datetime, pd.Timestamp)):
            start_dt = start_date
            start_date_str = start_dt.strftime("%Y-%m-%d")
        else:
            raise ValueError("start_date must be a string, datetime, or pandas Timestamp")
    
    if end_date is not None:
        if isinstance(end_date, str):
            try:
                end_dt = datetime.strptime(end_date, "%Y-%m-%d")
                end_date_str = end_date
            except ValueError:
                raise ValueError("end_date string must be in YYYY-MM-DD format")
        elif isinstance(end_date, (datetime, pd.Timestamp)):
            end_dt = end_date
            end_date_str = end_dt.strftime("%Y-%m-%d")
        else:
            raise ValueError("end_date must be a string, datetime, or pandas Timestamp")
    
    if start_date is not None and end_date is not None and start_dt > end_dt:
        raise ValueError("end_date cannot be before start_date")

    # Build query using validated parameters
    codes_str = "','".join(codes)
    query = f"""
        SELECT date, code, value 
        FROM indicadores 
        WHERE code IN ('{codes_str}') 
        AND field = '{field}'
    """
    
    if start_date_str:
        query += f" AND date >= '{start_date_str}'"
    if end_date_str:
        query += f" AND date <= '{end_date_str}'"
        
    query += " ORDER BY date, code"
    
    df = read_sql(query, date_columns=['date'])
    
    if df.empty:
        raise ValueError(f"No data found for code(s) {codes} with field '{field}'")
    
    # Pivot the data if multiple codes to have one column per code
    if len(codes) > 1:
        df = df.pivot(index='date', columns='code', values='value')
        df.columns.name = None  # Remove column name
        # Reindex columns by inputed order
        df = df.reindex(columns=codes)
    else:
        df = df.drop(columns='code').set_index('date')
        df.columns = [codes[0]]
    
    return df