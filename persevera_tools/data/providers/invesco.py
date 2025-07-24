from typing import Optional
import pandas as pd
from io import StringIO
import requests
from datetime import datetime

from .base import DataProvider, DataRetrievalError
from ...db.operations import read_sql

from persevera_tools.db.operations import read_sql

class InvescoProvider(DataProvider):
    """Provider for Invesco data."""

    def __init__(self, start_date: str = '2022-01-01'):
        super().__init__(start_date)
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }

    def _download_and_process_holdings(self, ticker: str) -> Optional[pd.DataFrame]:
        """
        Downloads and processes Invesco holdings for a single ticker.
        """
        url = f"https://www.invesco.com/us/financial-products/etfs/holdings/main/holdings/0?audienceType=Investor&action=download&ticker={ticker}"
        self.logger.info(f"Downloading Invesco holdings for {ticker}")

        try:
            response = requests.get(url, headers=self.headers)
            response.raise_for_status()

            self.logger.info("File downloaded successfully. Processing CSV data...")
            
            csv_file = StringIO(response.text)
            df = pd.read_csv(csv_file)

            # Remove tabs from all string columns
            for col in df.columns:
                if df[col].dtype == 'object':
                    df[col] = df[col].str.replace('\t', '').str.strip()

            # Remove unecessary entries
            df = df[~df['Class of Shares'].isin(['Variable Margin', 'Currency', 'Money Market Fund, Taxable', 'Cash Collateral'])]

            df = df[['Name', 'Weight', 'Date']]
            df.columns = ['name', 'weight_cta_invesco', 'date']
            df['date'] = pd.to_datetime(df['date'], format='%m/%d/%Y')

            # Remove any rows where the weight is not a number
            df['weight_cta_invesco'] = pd.to_numeric(df['weight_cta_invesco'], errors='coerce')
            df.dropna(subset=['weight_cta_invesco'], inplace=True)
            df['weight_cta_invesco'] = df['weight_cta_invesco'] / 100
            
            df = df.melt(id_vars=['name', 'date'], var_name='field', value_name='value')
            
            self.logger.info(f"Successfully extracted table for {ticker}. Shape: {df.shape}")
            return df

        except requests.exceptions.HTTPError as e:
            self.logger.error(f"HTTP error for ticker {ticker} from {url}: {e}")
            return None
        except Exception as e:
            self.logger.error(f"An unexpected error occurred for ticker {ticker} from {url}: {e}")
            return None

    def get_data(self, category: str, ticker: str = 'IMF', data_type: str = 'holdings', **kwargs) -> pd.DataFrame:
        """
        Retrieve holdings data from Invesco.
        
        Args:
            category (str): The category of data to retrieve. Defaults to 'holdings'.
            ticker (str): The ticker of the fund (e.g., 'IMF').
            data_type (str): The type of data to retrieve. Defaults to 'holdings'.
            end_date (str, optional): The date for the data in 'YYYY-MM-DD' format. Defaults to today.
            
        Returns:
            pd.DataFrame: DataFrame with columns: ['date', 'code', 'field', 'value']
        """
        self._log_processing(category)

        if data_type != 'holdings':
            raise ValueError(f"Data type '{data_type}' is not supported for InvescoProvider.")
        
        df = self._download_and_process_holdings(ticker)

        if df is None or df.empty:
            raise DataRetrievalError(f"No data retrieved for ticker {ticker}.")
            
        final_df = df

        query = "SELECT * FROM indicadores_cta"
        df_cta_depara = read_sql(query)
        code_map = df_cta_depara.set_index('name')['code'].to_dict()

        all_names_in_data = set(final_df['name'].unique())
        all_names_in_map = set(code_map.keys())
        unmapped_names = all_names_in_data - all_names_in_map

        if unmapped_names:
            for name in sorted(list(unmapped_names)):
                self.logger.warning(f"Code not found for name: '{name}'. Corresponding entries will be dropped.")
            
            final_df = final_df[~final_df['name'].isin(unmapped_names)].copy()

        final_df['code'] = final_df['name'].map(code_map)
        final_df = final_df.drop(columns=['name'])
        
        return self._validate_output(final_df)

