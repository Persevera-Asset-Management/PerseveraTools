from datetime import datetime
from typing import Optional, Union, List
import pandas as pd
from itertools import product

from ..db.operations import read_sql

def get_series(code: Union[str, List[str]], 
               start_date: Optional[Union[str, datetime, pd.Timestamp]] = None, 
               end_date: Optional[Union[str, datetime, pd.Timestamp]] = None,
               field: Union[str, List[str]] = 'close') -> Union[pd.DataFrame, pd.Series]:
    """Get time series data for one or more indicators from the database.
    
    Args:
        code: Single indicator code or list of codes to retrieve.
        start_date: Optional start date filter as string 'YYYY-MM-DD', datetime, or pandas Timestamp.
        end_date: Optional end date filter as string 'YYYY-MM-DD', datetime, or pandas Timestamp.
        field: Field or list of fields to retrieve (default: 'close').
        
    Returns:
        pd.Series or pd.DataFrame: 
        - A Series if a single code and a single field are requested.
        - A DataFrame with fields as columns if a single code and multiple fields are requested.
        - A DataFrame with codes as columns if multiple codes and a single field are requested.
        - A DataFrame with a MultiIndex (code, field) for columns if multiple codes and fields are requested.
        The columns will be ordered according to the input lists of codes and fields.
        
    Raises:
        ValueError: If dates are in invalid format or if end_date is before start_date.
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

    # Validate fields
    if isinstance(field, str):
        fields = [field]
    elif isinstance(field, list):
        fields = field
    else:
        raise ValueError("field must be a string or list of strings")

    if not all(isinstance(f, str) and f for f in fields):
        raise ValueError("All fields must be non-empty strings")

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
    fields_str = "','".join(fields)
    query = f"""
        SELECT date, code, field, value 
        FROM indicadores 
        WHERE code IN ('{codes_str}') 
        AND field IN ('{fields_str}')
    """
    
    if start_date_str:
        query += f" AND date >= '{start_date_str}'"
    if end_date_str:
        query += f" AND date <= '{end_date_str}'"
        
    query += " ORDER BY date, code, field"
    
    df = read_sql(query, date_columns=['date'])
    
    if df.empty:
        raise ValueError(f"No data found for code(s) {codes} with field(s) {fields}")
    
    # Pivot the data to get the desired format
    df = df.pivot_table(
        index='date',
        columns=['code', 'field'],
        values='value'
    )
    
    # Simplify output and reorder columns
    if len(codes) == 1 and len(fields) == 1:
        series = df.iloc[:, 0]
        series.name = fields[0]
        return series
    elif len(codes) == 1:
        df = df.droplevel('code', axis=1)
        df = df.reindex(columns=fields)
    elif len(fields) == 1:
        df = df.droplevel('field', axis=1)
        df = df.reindex(columns=codes)
    else:
        new_columns = list(product(codes, fields))
        df = df.reindex(columns=new_columns)
    
    return df