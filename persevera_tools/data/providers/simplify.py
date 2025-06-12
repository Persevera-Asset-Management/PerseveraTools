from typing import Optional
import pandas as pd
import numpy as np
from io import BytesIO
import requests
from datetime import datetime

from .base import DataProvider, DataRetrievalError
from ...db.operations import read_sql


class SimplifyProvider(DataProvider):
    """Provider for Simplify data."""

    def __init__(self, start_date: str = '2022-01-01'):
        super().__init__(start_date)
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }

    def _download_and_process_date(self, date: datetime, custom_url: Optional[str] = None) -> Optional[pd.DataFrame]:
        """
        Downloads and processes Simplify data for a single date.
        """
        if custom_url:
            url = custom_url
            self.logger.info(f"Using custom URL: {url}")
        else:
            date_str = date.strftime('%Y_%m_%d')
            url = f"https://www.simplify.us/sites/default/files/excel_holdings/{date_str}_Simplify_Portfolio_EOD_Tracker.xlsx"
            self.logger.info(f"Downloading data for {date.strftime('%Y-%m-%d')}")

        try:
            response = requests.get(url, headers=self.headers)
            response.raise_for_status()

            self.logger.info("File downloaded successfully. Reading 'CTA Est. Risk Profile' sheet...")
            excel_file = BytesIO(response.content)

            xl = pd.ExcelFile(excel_file)
            sheet_name = 'CTA Est. Risk Profile'
            if sheet_name not in xl.sheet_names:
                self.logger.warning(f"'{sheet_name}' sheet not found in {url}. Available sheets: {xl.sheet_names}")
                return None

            df = pd.read_excel(excel_file, sheet_name=sheet_name, header=1)

            if 'Category' not in df.columns or 'Total' not in df['Category'].values:
                self.logger.warning(f"Could not find 'Total' in 'Category' column to delimit data in {url}.")
                return None
            
            index_last = df[df['Category'] == 'Total'].index[0]
            df = df.iloc[:index_last]
            df['date'] = date

            df.columns = ['name', 'weight_cta_simplify', 'est_initial_margin', 'cta_vol_contribution', 'date']
            df = df.drop(columns=['est_initial_margin'])
            
            df = df.melt(id_vars=['name', 'date'], var_name='field', value_name='value')
            
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

    def get_data(self, category: str = 'cta_risk_profile', end_date: Optional[str] = None, **kwargs) -> pd.DataFrame:
        """
        Retrieve CTA Risk Profile data from Simplify.
        
        Args:
            category (str): The category of data to retrieve. Defaults to 'cta_risk_profile'.
            end_date (str, optional): The end date for data retrieval in 'YYYY-MM-DD' format. Defaults to today.
            **kwargs: Can contain 'custom_url' to specify a direct file URL. If provided, date range is ignored.
            
        Returns:
            pd.DataFrame: DataFrame with columns: ['date', 'code', 'field', 'value']
        """
        self._log_processing(category)

        custom_url = kwargs.get('custom_url')
        if custom_url:
            # When custom_url is provided, we fetch a single file.
            # The date can be today or parsed from filename if needed, but for now `now()` is ok.
            df = self._download_and_process_date(date=datetime.now(), custom_url=custom_url)
            if df is None:
                raise DataRetrievalError(f"Failed to retrieve data from custom URL: {custom_url}")
            return self._validate_output(df)

        end_date_dt = pd.to_datetime(end_date) if end_date else datetime.now()
            
        all_data = []
        for date in pd.bdate_range(start=self.start_date, end=end_date_dt, freq='B'):
            daily_df = self._download_and_process_date(date)
            if daily_df is not None and not daily_df.empty:
                all_data.append(daily_df)
        
        if not all_data:
            raise DataRetrievalError("No data retrieved from Simplify for the given date range.")
            
        final_df = pd.concat(all_data, ignore_index=True)

        # Convert name to code
        query = "SELECT * FROM indicadores_cta"
        df_cta_depara = read_sql(query)
        code_map = df_cta_depara.set_index('name')['code'].to_dict()

        # Find and warn about names that are in the data but not in our mapping table
        all_names_in_data = set(final_df['name'].unique())
        all_names_in_map = set(code_map.keys())
        unmapped_names = all_names_in_data - all_names_in_map

        if unmapped_names:
            for name in sorted(list(unmapped_names)):
                self.logger.warning(f"Code not found for name: '{name}'. Corresponding entries will be dropped.")
            
            # Filter out rows with unmapped names
            final_df = final_df[~final_df['name'].isin(unmapped_names)].copy()

        final_df['code'] = final_df['name'].map(code_map)
        final_df = final_df.drop(columns=['name'])
        
        return self._validate_output(final_df)

