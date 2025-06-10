from typing import Dict, Optional
import pandas as pd
import os
import glob
import urllib.request

from .base import DataProvider, DataRetrievalError
from ..lookups import get_url
from ...config import settings

DATA_PATH = settings.DATA_PATH


class AnbimaProvider(DataProvider):
    """Provider for ANBIMA data."""
    
    def __init__(self, start_date: str = '1980-01-01'):
        super().__init__(start_date)
    
    def get_data(self, download_new: bool = True, **kwargs) -> pd.DataFrame:
        """
        Retrieve data from ANBIMA.
        
        Args:
            download_new: Whether to download new data files or use existing ones
            
        Returns:
            DataFrame with columns: ['date', 'code', 'field', 'value']
        """
        self._log_processing('anbima')
        
        # Create directory if it doesn't exist
        # anbima_dir = os.path.join(DATA_PATH, 'anbima')
        # os.makedirs(anbima_dir, exist_ok=True)
        
        # if download_new:
        #     self._download_anbima_files(anbima_dir)
            
        # df = self._process_anbima_files(anbima_dir)
        df = self._read_anbima_files()
        
        if df.empty:
            raise DataRetrievalError("No data retrieved from ANBIMA")
            
        return self._validate_output(df)
    
    def _download_anbima_files(self, directory: str) -> None:
        """Download ANBIMA files to the specified directory."""
        securities_list = get_url("anbima")
        
        for index_name, url in securities_list.items():
            try:
                self.logger.info(f"Downloading {index_name} from {url}")
                urllib.request.urlretrieve(
                    url=url,
                    filename=os.path.join(directory, f'{index_name}.xls')
                )
            except Exception as e:
                self.logger.warning(f"Failed to download {index_name}: {str(e)}")
    
    def _process_anbima_files(self, directory: str) -> pd.DataFrame:
        """Process ANBIMA files from the specified directory."""
        files = glob.glob(os.path.join(directory, '*'))
        df = pd.DataFrame(columns=['code', 'date', 'value'])
        
        for file in files:
            try:
                self.logger.info(f"Processing {file}")
                try:
                    temp = pd.read_excel(file, usecols="B:C", engine="openpyxl")
                except:
                    temp = pd.read_excel(file, usecols="B:C", engine="xlrd")
                    
                temp.columns = ['date', 'value']
                temp['code'] = os.path.splitext(os.path.basename(file))[0]
                df = pd.concat([df, temp], ignore_index=True)
                
            except Exception as e:
                self.logger.warning(f"Failed to process {file}: {str(e)}")
                
        df = df.assign(field='close')
        return df

    def _read_anbima_files(self) -> pd.DataFrame:
        """Read ANBIMA files from URL."""
        securities_list = get_url("anbima")
        df = pd.DataFrame(columns=['code', 'date', 'value'])

        for index_name, url in securities_list.items():
            try:
                self.logger.info(f"Reading {index_name} from {url}")
                try:
                    temp = pd.read_excel(url, usecols="B:C", engine="openpyxl")
                except:
                    temp = pd.read_excel(url, usecols="B:C", engine="xlrd")
                    
                temp.columns = ['date', 'value']
                temp['code'] = index_name
                df = pd.concat([df, temp], ignore_index=True)
                
            except Exception as e:
                self.logger.warning(f"Failed to process {index_name}: {str(e)}")
                
        df = df.assign(field='close')
        return df 