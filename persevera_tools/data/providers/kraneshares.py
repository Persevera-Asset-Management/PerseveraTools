from typing import Optional
import pandas as pd
from io import BytesIO, StringIO
import requests
from datetime import datetime

from .base import DataProvider, DataRetrievalError
from ...db.operations import read_sql


class KraneSharesProvider(DataProvider):
    """Provider for KraneShares data."""

    def __init__(self, start_date: str = '2023-01-01'):
        super().__init__(start_date)
        self.headers = {
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
            "Accept-Encoding": "gzip, deflate, br",
            "Accept-Language": "en-US,en;q=0.9",
            "Sec-Ch-Ua": '"Not/A)Brand";v="99", "Google Chrome";v="115", "Chromium";v="115"',
            "Sec-Ch-Ua-Mobile": "?0",
            "Sec-Ch-Ua-Platform": '"Windows"',
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "none",
            "Sec-Fetch-User": "?1",
            "Upgrade-Insecure-Requests": "1",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36",
        }

    def _download_and_process_date(self, date: datetime, custom_url: Optional[str] = None) -> Optional[pd.DataFrame]:
        """
        Downloads and processes KraneShares data for a single date.
        """
        if custom_url:
            url = custom_url
            self.logger.info(f"Using custom URL: {url}")
        else:
            date_str = date.strftime('%m_%d_%Y')
            url = f"https://kraneshares.com/csv/{date_str}_kmlm_holdings.csv"
            self.logger.info(f"Downloading data for {date.strftime('%Y-%m-%d')}")

        try:
            response = requests.get(url, headers=self.headers)
            response.raise_for_status()

            self.logger.info("File downloaded successfully. Reading CSV data...")
            
            content = response.content.decode('utf-8', errors='ignore')
            lines = content.splitlines()
            header_row_index = -1
            for i, line in enumerate(lines):
                if 'Company Name' in line and '% of Net Assets' in line:
                    header_row_index = i
                    break
            
            if header_row_index == -1:
                self.logger.warning(f"Header row not found in {url}.")
                return None

            df = pd.read_csv(StringIO(content), skiprows=header_row_index)

            total_row_mask = df.apply(lambda r: r.astype(str).str.contains('Total').any(), axis=1)
            if total_row_mask.any():
                last_idx = df[total_row_mask].index[0]
                df = df.iloc[:last_idx]

            df['date'] = date
            
            df = df.rename(columns={'Company Name': 'name'})

            df['name'] = df['name'].str.replace(r'\).*$', ')', regex=True)
            df['name'] = df['name'].str.replace(r'\s+[A-Z]{3}\d{2}$', '', regex=True).str.strip()

            value_cols = ['% of Net Assets', 'Market Value($)', 'Notional Value($)']
            for col in value_cols:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col].astype(str).str.replace(',', ''), errors='coerce')

            df['weight_cta_kraneshares'] = df['Notional Value($)'] / df['Market Value($)'].sum()
            df = df[df['weight_cta_kraneshares'] != 0.0]

            id_cols = ['name', 'date']
            value_cols_renamed = ['weight_cta_kraneshares']
            
            df = df.melt(
                id_vars=id_cols, 
                value_vars=[c for c in value_cols_renamed if c in df.columns], 
                var_name='field', value_name='value'
            )
            df = df.dropna(subset=['value'])
            
            self.logger.info(f"Successfully extracted table for {date.strftime('%Y-%m-%d')}. Shape: {df.shape}")
            return df

        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 404:
                self.logger.warning(f"No data file found for date {date.strftime('%Y-%m-%d')} at {url}.")
            else:
                self.logger.error(f"HTTP error for date {date.strftime('%Y-%m-%d')} from {url}: {e}")
            return None
        except Exception as e:
            self.logger.error(f"An unexpected error occurred for date {date.strftime('%Y-%m-%d')} from {url}: {e}")
            return None

    def get_data(self, category: str, data_type: str = 'kmlm_holdings', end_date: Optional[str] = None, **kwargs) -> pd.DataFrame:
        """
        Retrieve KMLM Holdings data from KraneShares.
        
        Args:
            data_type (str): The type of data to retrieve. Defaults to 'kmlm_holdings'.
            end_date (str, optional): The end date for data retrieval in 'YYYY-MM-DD' format. Defaults to today.
            **kwargs: Can contain 'custom_url' to specify a direct file URL. If provided, date range is ignored.
            
        Returns:
            pd.DataFrame: DataFrame with columns: ['date', 'code', 'field', 'value']
        """
        self._log_processing(category)

        custom_url = kwargs.get('custom_url')
        if custom_url:
            df = self._download_and_process_date(date=datetime.now(), custom_url=custom_url)
            if df is None:
                raise DataRetrievalError(f"Failed to retrieve data from custom URL: {custom_url}")
        else:
            end_date_dt = pd.to_datetime(end_date) if end_date else datetime.now()
            
            all_data = []
            for date in pd.bdate_range(start=self.start_date, end=end_date_dt, freq='B'):
                daily_df = self._download_and_process_date(date)
                if daily_df is not None and not daily_df.empty:
                    all_data.append(daily_df)
            
            if not all_data:
                raise DataRetrievalError("No data retrieved from KraneShares for the given date range.")
                
            df = pd.concat(all_data, ignore_index=True)

        query = "SELECT * FROM indicadores_cta"
        df_cta_depara = read_sql(query)
        code_map = df_cta_depara.set_index('name')['code'].to_dict()

        all_names_in_data = set(df['name'].unique())
        all_names_in_map = set(code_map.keys())
        unmapped_names = all_names_in_data - all_names_in_map

        if unmapped_names:
            for name in sorted(list(unmapped_names)):
                self.logger.warning(f"Code not found for name: '{name}'. Corresponding entries will be dropped.")
            
            df = df[~df['name'].isin(unmapped_names)].copy()

        if df.empty:
            self.logger.warning("DataFrame is empty after filtering for mapped codes.")
            return self._validate_output(pd.DataFrame())

        df['code'] = df['name'].map(code_map)
        df = df.drop(columns=['name'])
        
        return self._validate_output(df)
