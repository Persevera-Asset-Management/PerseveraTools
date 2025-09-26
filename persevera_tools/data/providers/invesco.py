from typing import Optional
import pandas as pd
from io import StringIO
import requests
from datetime import datetime
import time

from .base import DataProvider, DataRetrievalError
from ...db.operations import read_sql
from ...utils.logging import get_logger, timed

logger = get_logger(__name__)

class InvescoProvider(DataProvider):
    """Provider for Invesco data."""

    def __init__(self, start_date: str = '2022-01-01'):
        super().__init__(start_date)
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
            'DNT': '1',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'none',
            'Sec-Fetch-User': '?1',
            'Cache-Control': 'max-age=0'
        }

    def _download_and_process_holdings(self, ticker: str) -> Optional[pd.DataFrame]:
        """
        Downloads and processes Invesco holdings for a single ticker.
        """
        logger.info(f"Downloading Invesco holdings for {ticker}")

        try:
            # Create a more sophisticated session that mimics a real browser
            session = requests.Session()
            
            # Complete browser headers
            browser_headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
                'Accept-Language': 'en-US,en;q=0.9',
                'Accept-Encoding': 'gzip, deflate, br',
                'DNT': '1',
                'Connection': 'keep-alive',
                'Upgrade-Insecure-Requests': '1',
                'Sec-Fetch-Dest': 'document',
                'Sec-Fetch-Mode': 'navigate',
                'Sec-Fetch-Site': 'same-origin',
                'Sec-Fetch-User': '?1',
                'Cache-Control': 'max-age=0'
            }
            
            session.headers.update(browser_headers)
            
            # Step 1: Visit the main Invesco page to establish session
            main_page = "https://www.invesco.com/us/financial-products/etfs"
            # logger.info("Establishing session with Invesco website...")
            session.get(main_page)
            time.sleep(1)
            
            # Step 2: Visit the holdings page for the specific ticker
            holdings_page = f"https://www.invesco.com/us/financial-products/etfs/holdings/main/holdings/0?audienceType=Investor&ticker={ticker.upper()}"
            # logger.info(f"Visiting holdings page for {ticker}...")
            page_response = session.get(holdings_page)
            
            if page_response.status_code != 200:
                logger.warning(f"Holdings page returned status {page_response.status_code}")
            
            time.sleep(1)
            
            # Step 3: Now try to download the CSV with proper referer
            download_url = f"https://www.invesco.com/us/financial-products/etfs/holdings/main/holdings/0?audienceType=Investor&action=download&ticker={ticker.upper()}"
            
            # Update headers for the download request
            download_headers = browser_headers.copy()
            download_headers.update({
                'Referer': holdings_page,
                'Accept': 'text/csv,application/csv,*/*;q=0.1',
                'Sec-Fetch-Dest': 'document',
                'Sec-Fetch-Mode': 'navigate',
                'Sec-Fetch-Site': 'same-origin'
            })
            
            session.headers.update(download_headers)
            
            logger.info(f"Attempting to download CSV for {ticker}...")
            response = session.get(download_url)
            
            # Check the response
            logger.info(f"Response status: {response.status_code}")
            logger.info(f"Response headers: {dict(response.headers)}")
            
            if response.status_code == 406:
                logger.error(f"Still getting 406 error. Response content preview: {response.text[:500]}...")
                
                # Last attempt: Try with absolute minimal approach
                logger.info("Trying minimal approach as last resort...")
                minimal_response = requests.get(download_url, headers={'User-Agent': 'Python-requests'})
                
                if minimal_response.status_code != 406:
                    response = minimal_response
                    logger.info("Minimal approach worked!")
                else:
                    raise requests.exceptions.HTTPError(
                        f"Unable to download holdings for {ticker}. "
                        f"Invesco's server is consistently returning 406 errors, "
                        f"which suggests they have implemented anti-automation measures. "
                        f"Consider using official Invesco APIs or alternative data sources."
                    )
            
            response.raise_for_status()

            logger.info("File downloaded successfully. Processing CSV data...")
            
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
            
            logger.info(f"Successfully extracted table for {ticker}. Shape: {df.shape}")
            return df

        except requests.exceptions.HTTPError as e:
            logger.error(f"HTTP error for ticker {ticker} from URL: {e}")
            return None
        except Exception as e:
            logger.error(f"An unexpected error occurred for ticker {ticker} from URL: {e}")
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
                logger.warning(f"Code not found for name: '{name}'. Corresponding entries will be dropped.")
            
            final_df = final_df[~final_df['name'].isin(unmapped_names)].copy()

        final_df['code'] = final_df['name'].map(code_map)
        final_df = final_df.drop(columns=['name'])
        
        return self._validate_output(final_df)

