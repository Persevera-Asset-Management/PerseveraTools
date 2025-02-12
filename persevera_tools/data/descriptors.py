from datetime import datetime
from typing import Optional, Union, List
import pandas as pd

from ..db.operations import read_sql

def get_company_data(ticker: Union[str, List[str]],
                     descriptor: str,
                     start_date: Optional[str] = None,
                     end_date: Optional[str] = None) -> pd.DataFrame:
    """Get company data from factor_zoo table.
    
    Args:
        ticker: Single company ticker or list of tickers
        descriptor: Descriptor to retrieve (e.g., 'pe', 'ev_ebitda', etc.)
        start_date: Optional start date filter (format: 'YYYY-MM-DD')
        end_date: Optional end date filter (format: 'YYYY-MM-DD')
        
    Returns:
        DataFrame with date and descriptor values for the company(ies)
        If multiple tickers are provided, the DataFrame will have one column per ticker
        
    Raises:
        ValueError: If dates are invalid or if no data is found
    """
    # Validate tickers
    if isinstance(ticker, str):
        tickers = [ticker]
    elif isinstance(ticker, list):
        tickers = ticker
    else:
        raise ValueError("ticker must be a string or list of strings")
    
    if not all(isinstance(t, str) and t for t in tickers):
        raise ValueError("All tickers must be non-empty strings")

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
    tickers_str = "','".join(tickers)
    query = f"""
        SELECT date, code, value 
        FROM descriptor_zoo 
        WHERE code IN ('{tickers_str}') 
        AND field = '{descriptor}'
    """
    
    if start_date:
        query += f" AND date >= '{start_date}'"
    if end_date:
        query += f" AND date <= '{end_date}'"
        
    query += " ORDER BY date, code"
    
    df = read_sql(query, date_columns=['date'])
    
    if df.empty:
        raise ValueError(f"No data found for ticker(s) {tickers} with descriptor '{descriptor}'")
    
    # Pivot the data if multiple tickers to have one column per ticker
    if len(tickers) > 1:
        df = df.pivot(index='date', columns='code', values='value')
        df.columns.name = None  # Remove column name
    else:
        df = df.drop(columns='code').set_index('date')
    
    return df