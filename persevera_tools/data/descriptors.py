from datetime import datetime
from typing import Optional, Union, List
import pandas as pd

from ..db.operations import read_sql

def get_descriptors(tickers: Union[str, List[str]],
                    descriptors: Union[str, List[str]],
                    start_date: Optional[str] = None, 
                    end_date: Optional[str] = None) -> pd.DataFrame:
    """Get descriptors from factor_zoo table.
    
    Args:
        tickers: Single ticker or list of tickers
        descriptor: Single descriptor or list of descriptors (e.g., 'pe', 'ev_ebitda')
        start_date: Optional start date filter (format: 'YYYY-MM-DD')
        end_date: Optional end date filter (format: 'YYYY-MM-DD')
        
    Returns:
        DataFrame with MultiIndex columns (ticker, descriptor) if multiple tickers and descriptors
        DataFrame with simple columns if single ticker or descriptor
        All DataFrames are indexed by date
    """
    # Validate tickers
    if isinstance(tickers, str):
        tickers = [tickers]
    elif isinstance(tickers, list):
        tickers = tickers
    else:
        raise ValueError("ticker must be a string or list of strings")
    
    if not all(isinstance(t, str) and t for t in tickers):
        raise ValueError("All tickers must be non-empty strings")

    # Validate descriptors
    if isinstance(descriptors, str):
        descriptors = [descriptors]
    elif isinstance(descriptors, list):
        descriptors = descriptors
    else:
        raise ValueError("descriptor must be a string or list of strings")
        
    if not all(isinstance(d, str) and d for d in descriptors):
        raise ValueError("All descriptors must be non-empty strings")

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
    tickers_str = "','".join(tickers)
    descriptors_str = "','".join(descriptors)
    
    query = f"""
        SELECT date, code as ticker, field as descriptor, value
        FROM factor_zoo 
        WHERE code IN ('{tickers_str}')
        AND field IN ('{descriptors_str}')
    """
    
    if start_date:
        query += f" AND date >= '{start_date}'"
    if end_date:
        query += f" AND date <= '{end_date}'"
        
    query += " ORDER BY date, code, field"
    
    df = read_sql(query, date_columns=['date'])
    
    if df.empty:
        raise ValueError(f"No data found for ticker(s) {tickers} with descriptor(s) {descriptors}")
    
    # Pivot the data to get the desired format
    df = df.pivot_table(
        index='date',
        columns=['ticker', 'descriptor'],
        values='value'
    )
    
    # Simplify output if single ticker or descriptor
    if len(tickers) == 1 and len(descriptors) == 1:
        return df.droplevel(['ticker', 'descriptor'], axis=1)
    elif len(tickers) == 1:
        return df.droplevel('ticker', axis=1)
    elif len(descriptors) == 1:
        return df.droplevel('descriptor', axis=1)
    
    # Sort columns by inputed order
    df = df.filter(tickers)
    
    return df