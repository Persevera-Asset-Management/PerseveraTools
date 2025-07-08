from datetime import datetime
from typing import Optional, Union, List
import pandas as pd
from itertools import product

from ..db.operations import read_sql

def get_equities_portfolio(date: Optional[Union[str, datetime, pd.Timestamp]] = None) -> Union[pd.DataFrame, pd.Series]:
    """Get equities portfolio data from the database.
    
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
    date_str = None
    if date is not None:
        if isinstance(date, str):
            try:
                date_dt = datetime.strptime(date, "%Y-%m-%d")
                date_str = date
            except ValueError:
                raise ValueError("date string must be in YYYY-MM-DD format")
        elif isinstance(date, (datetime, pd.Timestamp)):
            date_dt = date
            date_str = date_dt.strftime("%Y-%m-%d")
        else:
            raise ValueError("date must be a string, datetime, or pandas Timestamp")
    
    # Build query using validated parameters
    query = "SELECT * FROM cadm_carteira_rv"
    
    if date_str:
        query += f" WHERE date = '{date_str}'"
        
    df = read_sql(query, date_columns=['date'])
    
    if df.empty:
        raise ValueError(f"No data found for date {date_str}")
    
    return df