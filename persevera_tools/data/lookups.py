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

def get_url(sheet_name: str) -> Dict[str, str]:
    """Get URLs from cadastro-base.xlsx."""
    df = pd.read_excel(os.path.join(settings.DATA_PATH, "cadastro-base.xlsx"), sheet_name=sheet_name, index_col=0)
    return df['url'].to_dict()