import os
import pandas as pd
from typing import Dict, Optional

from ..config import settings
from ..db.operations import read_sql

def get_bloomberg_codes(sheet_name: str, category: str) -> Dict[str, str]:
    """Get Bloomberg codes and mnemonics from cadastro-base.xlsx."""
    df = pd.read_excel(os.path.join(settings.DATA_PATH, "cadastro-base.xlsx"), sheet_name=sheet_name)
    df = df.query("category == @category")
    df = df.filter(['bloomberg_code', 'mnemonic'])
    df = df.set_index('bloomberg_code')
    return df['mnemonic'].to_dict()

def get_codes(source: Optional[str] = None, category: Optional[str] = None) -> Dict[str, str]:
    """Get codes from indicadores_definicoes table."""
    query = "SELECT * FROM indicadores_definicoes"
    if source:
        query += f" WHERE source = '{source}'"
    df = read_sql(query)
    df = df[df['category'] == category] if category else df
    df = df.set_index('raw_code')
    return df['code'].to_dict()

def get_securities_by_exchange(exchange: Optional[str] = None) -> Dict[str, str]:
    """
    Get securities information from the database by exchange.

    If no exchange is provided, all active securities will be returned.

    Args:
        exchange: Exchange code (e.g., 'BZ' for B3).

    Returns:
        A dictionary mapping Bloomberg tickers to internal codes.
    """
    query = "SELECT code, code_exchange FROM b3_active_securities"
    params = {}
    
    if exchange:
        query += f" WHERE code_exchange = '{exchange}'"
        
    df = read_sql(query)

    if df.empty:
        return {}

    df = df.assign(code_bloomberg=df['code'] + ' ' + df['code_exchange'] + ' Equity')
    df = df.set_index('code_bloomberg')
    return df['code'].to_dict()