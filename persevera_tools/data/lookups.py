import os
import pandas as pd
from typing import Dict, Optional

from ..config import settings
from ..db.operations import read_sql
from ..db.fibery import read_fibery

_DENOMINATION_TO_EXCHANGE = {
    'BRL': 'BZ',
    'USD': 'US',
}


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
    Get securities information from Fibery by exchange.

    If no exchange is provided, all active securities will be returned.

    Args:
        exchange: Exchange code (e.g., 'BZ' for B3).

    Returns:
        A dictionary mapping Bloomberg tickers to internal codes.
    """
    df = read_fibery(table_name='Inv-Rsrch-Quant/Ações Ativas')
    df = df.assign(code_exchange=df['Denominação'].map(_DENOMINATION_TO_EXCHANGE))

    if exchange:
        df = df[df['code_exchange'] == exchange]

    if df.empty:
        return {}

    df = df.assign(code_bloomberg=df['Ativo'] + ' ' + df['code_exchange'] + ' Equity')
    df = df.set_index('code_bloomberg')
    return df['Name'].to_dict()