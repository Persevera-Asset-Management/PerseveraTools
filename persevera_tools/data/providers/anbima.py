from typing import Dict, Optional
import pandas as pd
import urllib.request
import warnings
from datetime import datetime, timedelta
import numpy as np
from ..lookups import get_codes
from .base import DataProvider, DataRetrievalError

# Suppress openpyxl warning about workbooks without default styles
warnings.filterwarnings("ignore", message="Workbook contains no default style", category=UserWarning)

class AnbimaProvider(DataProvider):
    """Provider for ANBIMA data."""
    
    def __init__(self, start_date: str = '1980-01-01'):
        super().__init__(start_date)
    
    def get_data(self, category: str, **kwargs) -> pd.DataFrame:
        """
        Retrieve data from ANBIMA.
        
        Args:
            **kwargs: Additional keyword arguments
            
        Returns:
            DataFrame with columns: ['date', 'code', 'field', 'value']
        """
        self._log_processing(category)
        
        df = self._read_anbima_files()
        
        if df.empty:
            raise DataRetrievalError("No data retrieved from ANBIMA")
            
        return self._validate_output(df)

    def _read_anbima_files(self) -> pd.DataFrame:
        """Read ANBIMA files from URL."""
        securities_list = get_codes(source='anbima')
        data_frames = []

        for url, index_name in securities_list.items():
            try:
                self.logger.info(f"Reading {index_name} from {url}")
                try:
                    temp = pd.read_excel(url, usecols="B:C", engine="openpyxl")
                except:
                    temp = pd.read_excel(url, usecols="B:C", engine="xlrd")
                    
                temp.columns = ['date', 'value']
                temp['code'] = index_name
                data_frames.append(temp)
                
            except Exception as e:
                self.logger.warning(f"Failed to process {index_name}: {str(e)}")
        
        if not data_frames:
            return pd.DataFrame()
            
        df = pd.concat(data_frames, ignore_index=True)
        df = df.assign(field='close')
        return df

    def get_debentures_data(self, category: str, **kwargs) -> pd.DataFrame:
        """
        Retrieves and parses ANBIMA debentures data for a specific date.

        Args:
            date: The date for which to retrieve the data.

        Returns:
            A DataFrame with the parsed debentures data.
        """
        self._log_processing(category)
        
        data_frames = []
        for date in pd.bdate_range(start=self.start_date, end=datetime.today(), freq='B'):
            url = f"https://www.anbima.com.br/informacoes/merc-sec-debentures/arqs/db{date.strftime('%y%m%d')}.txt"
            
            content = None
            try:
                with urllib.request.urlopen(url) as response:
                    content = response.read().decode('latin-1')
            except Exception as e:
                self.logger.error(f"Failed to download debentures file from {url}: {e}")
                continue
                
            if content:
                try:
                    parsed_df = self._parse_debentures_file(content, date)
                    if not parsed_df.empty:
                        data_frames.append(parsed_df)
                except Exception as e:
                    self.logger.error(f"Failed to parse debentures file: {e}")

        if not data_frames:
            raise DataRetrievalError("No data retrieved from ANBIMA debentures")
        
        df = pd.concat(data_frames, ignore_index=True)
        
        df = df.melt(
            id_vars=['code', 'date', 'reference'], 
            var_name='field'
        )
        df = df.replace({np.nan: None})
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
        if len(lines) < 4:
            return pd.DataFrame()
            
        header = lines[2].split('@')
        data = [line.split('@') for line in lines[3:]]

        df = pd.DataFrame(data, columns=header)

        df = df[['Código', 'Taxa Indicativa', 'PU', 'Duration', 'Referência NTN-B']]

        def clean_value(v):
            return v.strip().replace('.', '').replace(',', '.').replace('--', '0') if v.strip() else None

        df = df.map(clean_value)

        cols_num = ['Taxa Indicativa', 'PU', 'Duration']
        df[cols_num] = df[cols_num].apply(lambda x: pd.to_numeric(x, errors='coerce'))
        df['Referência NTN-B'] = pd.to_datetime(df['Referência NTN-B'], format='%d/%m/%Y', errors='coerce')
        df.columns = ['code', 'yield_to_maturity', 'price_close', 'duration', 'reference']
        df['date'] = date
        return df
