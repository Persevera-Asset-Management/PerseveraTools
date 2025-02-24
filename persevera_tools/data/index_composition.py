from datetime import datetime
from typing import Optional, Union, List
import pandas as pd

from ..db.operations import read_sql

def get_index_composition(index_code: Union[str, List[str]],
                          start_date: Optional[str] = None,
                          end_date: Optional[str] = None) -> pd.DataFrame:
    """Get index composition from b3_index_composition table.
    
    Args:
        index_code: Single index code or list of index codes (e.g., 'IBOV', 'IBX100')
        start_date: Optional start date filter (format: 'YYYY-MM-DD')
        end_date: Optional end date filter (format: 'YYYY-MM-DD')
        
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

    # Build query
    index_str = "','".join(index_code)
    
    query = f"""
        SELECT date, code as ticker, field as index_code, value
        FROM b3_index_composition 
        WHERE field IN ('{index_str}')
    """
    
    if start_date:
        query += f" AND date >= '{start_date}'"
    if end_date:
        query += f" AND date <= '{end_date}'"
        
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