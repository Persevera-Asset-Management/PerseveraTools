from typing import Dict, Optional
import pandas as pd
import urllib.request
import warnings
from datetime import datetime, timedelta
import numpy as np

from .base import DataProvider, DataRetrievalError

# Suppress openpyxl warning about workbooks without default styles
warnings.filterwarnings("ignore", message="Workbook contains no default style", category=UserWarning)

class AnbimaProvider(DataProvider):
    """Provider for ANBIMA data."""
    
    def __init__(self, start_date: str = '1980-01-01'):
        super().__init__(start_date)
    
    def get_data(self, **kwargs) -> pd.DataFrame:
        """
        Retrieve data from ANBIMA.
        
        Args:
            **kwargs: Additional keyword arguments
            
        Returns:
            DataFrame with columns: ['date', 'code', 'field', 'value']
        """
        self._log_processing('anbima')
        
        df = self._read_anbima_files()
        
        if df.empty:
            raise DataRetrievalError("No data retrieved from ANBIMA")
            
        return self._validate_output(df)

    def get_debentures_data(self) -> pd.DataFrame:
        """
        Retrieves and parses ANBIMA debentures data for a specific date.

        Args:
            date: The date for which to retrieve the data.

        Returns:
            A DataFrame with the parsed debentures data.
        """
        self._log_processing(f'anbima debentures')
        
        df = pd.DataFrame()
        for date in pd.bdate_range(start=self.start_date, end=datetime.today(), freq='B'):
            url = f"https://www.anbima.com.br/informacoes/merc-sec-debentures/arqs/db{date.strftime('%y%m%d')}.txt"
            
            try:
                with urllib.request.urlopen(url) as response:
                    content = response.read().decode('latin-1')
            except Exception as e:
                self.logger.error(f"Failed to download debentures file from {url}: {e}")
                
            try:
                df = pd.concat([df, self._parse_debentures_file(content, date)])
            except Exception as e:
                self.logger.error(f"Failed to parse debentures file: {e}")

        if df.empty:
            raise DataRetrievalError("No data retrieved from ANBIMA debentures")
        
        df = df.melt(
            id_vars=['code', 'date', 'reference'], 
            var_name='field'
        )

        return df

    def _parse_debentures_file(self, content: str, date: datetime) -> pd.DataFrame:
        """
        Parses the content of an ANBIMA debentures TXT file.

        Args:
            content: The string content of the file.

        Returns:
            A DataFrame with the parsed data.
        """
        lines = content.splitlines()
        header = lines[2].split('@')
        lines = lines[3:]

        df = pd.DataFrame(columns=header)
        for line in lines:
            df = pd.concat([df, pd.DataFrame([line.split('@')], columns=header)], ignore_index=True)

        df = df[['Código', 'Taxa Indicativa', 'PU', 'Duration', 'Referência NTN-B']]

        def clean_value(v):
            return v.strip().replace('.', '').replace(',', '.').replace('--', '0') if v.strip() else None

        df = df.map(clean_value)

        cols_num = ['Taxa Indicativa', 'PU', 'Duration']
        df[cols_num] = df[cols_num].apply(lambda x: pd.to_numeric(x, errors='coerce'))
        df['Referência NTN-B'] = pd.to_datetime(df['Referência NTN-B'], format='%d/%m/%Y', errors='coerce')
        df.columns = ['code', 'yield_to_maturity', 'price_close', 'duration', 'reference']
        df['date'] = date
        df.replace({np.nan: None, pd.NaT: None}, inplace=True)
        return df
