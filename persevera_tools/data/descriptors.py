from datetime import datetime
from typing import Optional, Union, List
import pandas as pd

from ..db.operations import read_sql

def get_descriptors(tickers: Optional[Union[str, List[str]]] = None,
                    descriptors: Optional[Union[str, List[str]]] = None,
                    start_date: Optional[Union[str, datetime, pd.Timestamp]] = None, 
                    end_date: Optional[Union[str, datetime, pd.Timestamp]] = None) -> pd.DataFrame:
    """Get descriptors from factor_zoo table.
    
    Args:
        tickers: Optional single ticker or list of tickers. If None, returns data for all tickers.
        descriptors: Optional single descriptor or list of descriptors (e.g., 'pe', 'ev_ebitda'). If None, returns all descriptors.
        start_date: Optional start date filter as string 'YYYY-MM-DD', datetime, or pandas Timestamp
        end_date: Optional end date filter as string 'YYYY-MM-DD', datetime, or pandas Timestamp
        
    Returns:
        DataFrame with MultiIndex columns (ticker, descriptor) if multiple tickers and descriptors
        DataFrame with simple columns if single ticker or descriptor
        All DataFrames are indexed by date
        
    Raises:
        ValueError: If both tickers and descriptors are None or empty
    """
    # Validate that at least one of tickers or descriptors is specified
    if (tickers is None or (isinstance(tickers, list) and len(tickers) == 0)) and \
       (descriptors is None or (isinstance(descriptors, list) and len(descriptors) == 0)):
        raise ValueError("At least one of tickers or descriptors must be specified")

    # Validate tickers if provided
    if tickers is not None:
        if isinstance(tickers, str):
            tickers = [tickers]
        elif isinstance(tickers, list):
            if not all(isinstance(t, str) and t for t in tickers):
                raise ValueError("All tickers must be non-empty strings")
        else:
            raise ValueError("ticker must be a string or list of strings")

    # Validate descriptors if provided
    if descriptors is not None:
        if isinstance(descriptors, str):
            descriptors = [descriptors]
        elif isinstance(descriptors, list):
            if not all(isinstance(d, str) and d for d in descriptors):
                raise ValueError("All descriptors must be non-empty strings")
        else:
            raise ValueError("descriptor must be a string or list of strings")

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
    query = """
        SELECT date, code as ticker, field as descriptor, value
        FROM factor_zoo 
        WHERE 1=1
    """
    
    # Add ticker filter if provided
    if tickers is not None:
        tickers_str = "','".join(tickers)
        query += f" AND code IN ('{tickers_str}')"
    
    # Add descriptor filter if provided
    if descriptors is not None:
        descriptors_str = "','".join(descriptors)
        query += f" AND field IN ('{descriptors_str}')"
    
    if start_date_str:
        query += f" AND date >= '{start_date_str}'"
    if end_date_str:
        query += f" AND date <= '{end_date_str}'"
        
    query += " ORDER BY date, code, field"
    
    df = read_sql(query, date_columns=['date'])
    
    if df.empty:
        ticker_msg = f"ticker(s) {tickers}" if tickers is not None else "all tickers"
        descriptor_msg = f"descriptor(s) {descriptors}" if descriptors is not None else "all descriptors"
        raise ValueError(f"No data found for {ticker_msg} with {descriptor_msg}")
    
    # Pivot the data to get the desired format
    df = df.pivot_table(
        index='date',
        columns=['ticker', 'descriptor'],
        values='value'
    )
    
    # Simplify output if single ticker or descriptor
    if tickers is not None and len(tickers) == 1 and descriptors is not None and len(descriptors) == 1:
        series = df.iloc[:, 0]  # Get the only column as a Series
        series.name = descriptors[0]  # Name it with the descriptor
        return series
    elif tickers is not None and len(tickers) == 1:
        return df.droplevel('ticker', axis=1)
    elif descriptors is not None and len(descriptors) == 1:
        return df.droplevel('descriptor', axis=1)
    
    # For multiple tickers and descriptors, return the MultiIndex DataFrame
    return df