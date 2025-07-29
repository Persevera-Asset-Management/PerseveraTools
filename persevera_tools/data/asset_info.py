import pandas as pd
from typing import Union, List, Dict

from ..db.operations import read_sql

def get_equities_info(
    codes: Union[str, List[str]] = None,
    fields: Union[str, List[str]] = None
) -> pd.DataFrame:
    """
    Retrieve company information from the factor_zoo_cadastro table.
    
    Args:
        codes: Single code or list of company codes to retrieve
        fields: Single field or list of fields to retrieve
            Available fields: code, code_exchange, code_cvm, isin, type, name,
            sector_layer_0, sector_layer_1, sector_layer_2, sector_layer_3, sector_layer_4
            
    Returns:
        DataFrame with company information, with 'code' set as the index
        
    Raises:
        ValueError: If invalid fields are requested
    """
    # Build query based on parameters
    query = "SELECT "
    
    # Handle field selection
    if fields is None:
        # Default to all fields if none specified
        query += "* "
    else:
        # Convert single field to list
        if isinstance(fields, str):
            fields = [fields]
            
        # Validate fields
        valid_fields = [
            'code', 'code_exchange', 'code_cvm', 'isin', 'type', 'name',
            'sector_layer_0', 'sector_layer_1', 'sector_layer_2', 
            'sector_layer_3', 'sector_layer_4'
        ]
        
        # Always include 'code' field
        if 'code' not in fields:
            fields = ['code'] + fields
            
        invalid_fields = [f for f in fields if f not in valid_fields]
        if invalid_fields:
            raise ValueError(f"Invalid fields requested: {invalid_fields}. "
                            f"Valid fields are: {valid_fields}")
            
        # Join fields for query
        query += ", ".join(fields)
        
    query += " FROM factor_zoo_cadastro"
    
    # Handle code filtering
    if codes is not None:
        # Convert single code to list
        if isinstance(codes, str):
            codes = [codes]
            
        # Add WHERE clause for codes
        placeholders = ", ".join([f"'{code}'" for code in codes])
        query += f" WHERE code IN ({placeholders})"
        
    # Execute query
    df = read_sql(query)
    
    # Set 'code' as the index
    if not df.empty and 'code' in df.columns:
        df = df.set_index('code')
        
    return df 