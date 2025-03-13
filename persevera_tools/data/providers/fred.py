from typing import Dict, Optional
import pandas as pd
from fredapi import Fred

from .base import DataProvider, DataRetrievalError
from ..lookups import get_raw_tickers
from ..config.settings import FRED_API_KEY


class FredProvider(DataProvider):
    """Provider for Federal Reserve Economic Data (FRED)."""
    
    def __init__(self, start_date: str = '1980-01-01', api_key: Optional[str] = None):
        """
        Initialize FRED provider.
        
        Args:
            start_date: The start date for data retrieval
            api_key: Optional FRED API key. If not provided, uses the one from config
        """
        super().__init__(start_date)
        self.fred = Fred(api_key=api_key or FRED_API_KEY)
    
    def get_data(self, **kwargs) -> pd.DataFrame:
        """
        Retrieve data from FRED.
        
        Returns:
            DataFrame with columns: ['date', 'code', 'field', 'value']
        """
        self._log_processing('fred')
        
        securities_list = get_raw_tickers(source='fred')
        
        try:
            df = pd.DataFrame({
                series: self.fred.get_series(series, observation_start=self.start_date)
                for series in securities_list.keys()
            })
        except Exception as e:
            raise DataRetrievalError(f"Failed to retrieve FRED data: {str(e)}")
            
        if df.empty:
            raise DataRetrievalError("No data retrieved from FRED")
            
        df = df.stack().reset_index()
        df.columns = ['date', 'fred_code', 'value']
        df['code'] = df['fred_code'].map(securities_list)
        df = df.assign(field='close')
        
        return self._validate_output(df) 