from datetime import datetime
from typing import Optional, Union, List
import pandas as pd

from ..db.operations import read_sql

def get_index_composition(index_code: Union[str, List[str]],
                          start_date: Optional[Union[str, datetime, pd.Timestamp]] = None,
                          end_date: Optional[Union[str, datetime, pd.Timestamp]] = None) -> pd.DataFrame:
    """Get index composition from b3_index_composition table.
    
    Args:
        index_code: Single index code or list of index codes (e.g., 'IBOV', 'IBX100')
        start_date: Optional start date filter as string 'YYYY-MM-DD', datetime, or pandas Timestamp
        end_date: Optional end date filter as string 'YYYY-MM-DD', datetime, or pandas Timestamp
        
    Returns:
        DataFrame with MultiIndex columns (index_code, ticker) if multiple indices
        DataFrame with simple columns (tickers) if single index
        All DataFrames are indexed by date
    """
    # Validate index_code
    if isinstance(index_code, str):
        index_code = [index_code]
    elif isinstance(index_code, list):
        index_code = index_code
    else:
        raise ValueError("index_code must be a string or list of strings")
    
    if not all(isinstance(idx, str) and idx for idx in index_code):
        raise ValueError("All index codes must be non-empty strings")

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

    # Build query
    index_str = "','".join(index_code)
    
    query = f"""
        SELECT date, code as ticker, field as index_code, value
        FROM b3_index_composition 
        WHERE field IN ('{index_str}')
    """
    
    if start_date_str:
        query += f" AND date >= '{start_date_str}'"
    if end_date_str:
        query += f" AND date <= '{end_date_str}'"
        
    query += " ORDER BY date, field, code"
    
    df = read_sql(query, date_columns=['date'])
    
    if df.empty:
        raise ValueError(f"No data found for index code(s) {index_code}")
    
    # Pivot the data to get the desired format
    df = df.pivot_table(
        index='date',
        columns=['index_code', 'ticker'],
        values='value'
    )
    
    # Simplify output if single index
    if len(index_code) == 1:
        return df.droplevel('index_code', axis=1)
    
    return df 