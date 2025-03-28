import pandas as pd
import logging
from typing import List, Optional, Union, Dict
from datetime import datetime, date

from ..db.operations import read_sql, to_sql
from ..utils.logging import get_logger, timed

logger = get_logger(__name__)

@timed
def get_funds_data(
    cnpjs: Optional[Union[str, List[str]]] = None,
    start_date: Optional[Union[str, date, datetime]] = None,
    end_date: Optional[Union[str, date, datetime]] = None,
    fields: Optional[List[str]] = None,
    return_dataframe: bool = True
) -> pd.DataFrame:
    """
    Retrieve fund data from the fundos_cvm database.
    
    Args:
        cnpjs: Optional fund CNPJ(s) to filter by. Can be a single CNPJ or a list.
        start_date: Optional start date for filtering data.
        end_date: Optional end date for filtering data.
        fields: Optional list of specific fields to retrieve.
                Available fields are: fund_nav, fund_total_equity, fund_total_value,
                fund_inflows, fund_outflows, fund_holders
        return_dataframe: If True, returns a DataFrame with dates as index and CNPJs as columns.
                         If False, returns the raw query result.
    
    Returns:
        DataFrame with fund data
    """
    # Convert dates to strings if they're date objects
    if isinstance(start_date, (date, datetime)):
        start_date = start_date.strftime('%Y-%m-%d')
    if isinstance(end_date, (date, datetime)):
        end_date = end_date.strftime('%Y-%m-%d')
    
    # Define available columns and their SQL names
    all_columns = {
        'fund_nav': 'fund_nav',
        'fund_total_equity': 'fund_total_equity',
        'fund_total_value': 'fund_total_value',
        'fund_inflows': 'fund_inflows',
        'fund_outflows': 'fund_outflows',
        'fund_holders': 'fund_holders'
    }
    
    # Determine which columns to retrieve
    if fields:
        # Validate fields
        invalid_fields = set(fields) - set(all_columns.keys())
        if invalid_fields:
            raise ValueError(f"Invalid fields: {invalid_fields}. Valid fields are: {list(all_columns.keys())}")
        columns_to_select = [all_columns[field] for field in fields]
    else:
        columns_to_select = list(all_columns.values())
    
    # Build SQL query
    query = f"""
    SELECT 
        fund_cnpj,
        date,
        {', '.join(columns_to_select)}
    FROM fundos_cvm
    WHERE 1=1
    """
    
    # Add filters
    params = []
    
    if cnpjs:
        if isinstance(cnpjs, str):
            cnpjs = [cnpjs]
        
        placeholders = ', '.join(['%s'] * len(cnpjs))
        query += f" AND fund_cnpj IN ({placeholders})"
        params.extend(cnpjs)
    
    if start_date:
        query += " AND date >= %s"
        params.append(start_date)
    
    if end_date:
        query += " AND date <= %s"
        params.append(end_date)
    
    # Order the results
    query += " ORDER BY date, fund_cnpj"
    
    # Execute query with parameters
    if params:
        query = read_sql(query, date_columns=['date'])
    else:
        query = read_sql(query, date_columns=['date'])
    
    if query.empty:
        logger.warning("No fund data found with the specified filters")
        return pd.DataFrame()
    
    # Return raw query result if requested
    if not return_dataframe:
        return query
    
    # Process the result into a multi-index DataFrame if multiple fields were requested
    if (fields is None) or (len(fields) > 1):
        # Pivot for each field and then combine
        result_dfs = {}
        
        for field in (fields or all_columns.keys()):
            field_name = all_columns[field]
            if field_name in query.columns:
                pivot = query.pivot(index='date', columns='fund_cnpj', values=field_name)
                result_dfs[field] = pivot
        
        # Create a multi-index DataFrame
        if result_dfs:
            return pd.concat(result_dfs, axis=1)
        return pd.DataFrame()
    
    # If only one field was requested, return a simple pivoted DataFrame
    else:
        field_name = all_columns[fields[0]]
        return query.pivot(index='date', columns='fund_cnpj', values=field_name)

@timed
def get_persevera_peers(
    persevera_group: Optional[Union[str, List[str]]] = None,
    cnpjs: Optional[Union[str, List[str]]] = None
) -> pd.DataFrame:
    """
    Retrieve data from the fundos_persevera_peers table, which contains
    information about peer groups of funds tracked by Persevera.
    
    Args:
        persevera_group: Optional group name(s) to filter by. Can be a single name or a list.
        cnpjs: Optional fund CNPJ(s) to filter by. Can be a single CNPJ or a list.
    
    Returns:
        DataFrame with fund_cnpj, short_name and persevera_group columns
    """
    # Build SQL query
    query = """
    SELECT 
        fund_cnpj,
        short_name,
        persevera_group
    FROM fundos_persevera_peers
    WHERE 1=1
    """
    
    # Add filters
    params = []
    
    if persevera_group:
        if isinstance(persevera_group, str):
            persevera_group = [persevera_group]
        
        placeholders = ', '.join(['%s'] * len(persevera_group))
        query += f" AND persevera_group IN ({placeholders})"
        params.extend(persevera_group)
    
    if cnpjs:
        if isinstance(cnpjs, str):
            cnpjs = [cnpjs]
        
        placeholders = ', '.join(['%s'] * len(cnpjs))
        query += f" AND fund_cnpj IN ({placeholders})"
        params.extend(cnpjs)
    
    # Order the results
    query += " ORDER BY persevera_group, short_name"
    
    # Execute query with parameters
    if params:
        df = read_sql(query)
    else:
        df = read_sql(query)
    
    if df.empty:
        logger.warning("No persevera peer funds found with the specified filters")
    else:
        logger.info(f"Retrieved {len(df)} persevera peer funds")
    
    return df
