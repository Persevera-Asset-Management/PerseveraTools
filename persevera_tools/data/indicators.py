from datetime import datetime
from typing import Optional
import pandas as pd

from ..db.operations import read_sql

def get_series(code: str,
               start_date: Optional[str] = None,
               end_date: Optional[str] = None,
               field: str = 'close') -> pd.DataFrame:
   """Get time series data for a specific indicator from the database.
   
   Args:
       code: Indicator code to retrieve
       start_date: Optional start date filter (format: 'YYYY-MM-DD')
       end_date: Optional end date filter (format: 'YYYY-MM-DD')
       field: Field to retrieve (default: 'close')
       
   Returns:
       DataFrame with date and value columns for the indicator
       
   Raises:
       ValueError: If dates are in invalid format or if end_date is before start_date
   """
   # Validate code is not empty
   if not code or not isinstance(code, str):
       raise ValueError("Code must be a non-empty string")

   # Validate field
   valid_fields = {'open', 'high', 'low', 'close', 'volume', 'value'} 
   if field not in valid_fields:
       raise ValueError(f"Field must be one of: {', '.join(valid_fields)}")

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
   
   # Validate date range if both dates provided
   if start_date and end_date and start_dt > end_dt:
       raise ValueError("end_date cannot be before start_date")

   # Build query using validated parameters
   query = "SELECT date, value FROM indicadores WHERE code = '{code}' AND field = '{field}'".format(
       code=code, field=field
   )
   
   if start_date:
       query += f" AND date >= '{start_date}'"
   if end_date:
       query += f" AND date <= '{end_date}'"
       
   query += " ORDER BY date"
   
   df = read_sql(query, date_columns=['date'])
   
   # Validate we got some data
   if df.empty:
       raise ValueError(f"No data found for code '{code}' with field '{field}'")
       
   return df