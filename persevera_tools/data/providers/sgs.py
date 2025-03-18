from typing import Dict, Optional
import pandas as pd
import requests

from .base import DataProvider, DataRetrievalError
from ..lookups import get_raw_tickers

class SGSProvider(DataProvider):
    """Provider for Brazilian Central Bank (SGS) data."""
    
    def __init__(self, start_date: str = '1980-01-01'):
        super().__init__(start_date)
    
    def get_data(self, **kwargs) -> pd.DataFrame:
        """
        Retrieve data from SGS.
        
        Returns:
            DataFrame with columns: ['date', 'code', 'field', 'value']
        """
        self._log_processing('sgs')
        
        securities_list = get_raw_tickers(source='sgs')
        df = pd.DataFrame()
        
        for code in securities_list.keys():
            try:
                r = requests.get(f"https://api.bcb.gov.br/dados/serie/bcdata.sgs.{code}/dados?formato=json")
                temp = pd.DataFrame(r.json())
                temp.columns = ['date', 'value']
                temp['sgs_code'] = code
                df = pd.concat([df, temp], ignore_index=True)
            except ValueError as e:
                self.logger.warning(f"Failed to retrieve data for code {code}: {str(e)}")
                continue
                
        if df.empty:
            raise DataRetrievalError("No data retrieved from SGS")
            
        df['code'] = df['sgs_code'].astype(int).map(securities_list)
        df['date'] = pd.to_datetime(df['date'], format='%d/%m/%Y')
        df = df.assign(field='close')
        df = df.drop(columns=['sgs_code'])

        return self._validate_output(df) 