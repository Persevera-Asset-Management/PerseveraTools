from datetime import datetime
from typing import Optional, Union, List
import pandas as pd

from ..db.operations import read_sql

def get_descriptors(tickers: Union[str, List[str]],
                    descriptors: Union[str, List[str]],
                    start_date: Optional[Union[str, datetime, pd.Timestamp]] = None, 
                    end_date: Optional[Union[str, datetime, pd.Timestamp]] = None) -> pd.DataFrame:
    """Get descriptors from factor_zoo table.
    
    Args:
        tickers: Single ticker or list of tickers
        descriptors: Single descriptor or list of descriptors (e.g., 'pe', 'ev_ebitda')
        start_date: Optional start date filter as string 'YYYY-MM-DD', datetime, or pandas Timestamp
        end_date: Optional end date filter as string 'YYYY-MM-DD', datetime, or pandas Timestamp
        
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
    tickers_str = "','".join(tickers)
    descriptors_str = "','".join(descriptors)
    
    query = f"""
        SELECT date, code as ticker, field as descriptor, value
        FROM factor_zoo 
        WHERE code IN ('{tickers_str}')
        AND field IN ('{descriptors_str}')
    """
    
    if start_date_str:
        query += f" AND date >= '{start_date_str}'"
    if end_date_str:
        query += f" AND date <= '{end_date_str}'"
        
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
        series = df.iloc[:, 0]  # Get the only column as a Series
        series.name = descriptors[0]  # Name it with the descriptor
        return series
    elif len(tickers) == 1:
        return df.droplevel('ticker', axis=1).reindex(columns=descriptors)
    elif len(descriptors) == 1:
        return df.droplevel('descriptor', axis=1).reindex(columns=tickers)
    
    # For multiple tickers and descriptors, reindex to maintain input order
    multi_idx = pd.MultiIndex.from_product([tickers, descriptors], names=['ticker', 'descriptor'])
    return df.reindex(columns=multi_idx)