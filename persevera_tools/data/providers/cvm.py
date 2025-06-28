from typing import Optional
import pandas as pd
from io import BytesIO
import requests
from datetime import datetime
import zipfile

from .base import DataProvider, DataRetrievalError


class CVMProvider(DataProvider):
    """Provider for CVM (Comissão de Valores Mobiliários) data."""

    def __init__(self, start_date: str = '2023-01-01'):
        super().__init__(start_date)
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        self.base_url = "https://dados.cvm.gov.br/dados/FI"

    def _download_and_process_month(self, date: datetime) -> Optional[pd.DataFrame]:
        """
        Downloads and processes CVM monthly fund data for a single month.
        """
        date_str = date.strftime('%Y%m')
        url = f"{self.base_url}/DOC/INF_DIARIO/DADOS/inf_diario_fi_{date_str}.zip"
        self.logger.info(f"Downloading CVM data for {date.strftime('%Y-%m')}")

        try:
            response = requests.get(url, headers=self.headers)
            response.raise_for_status()

            self.logger.info("File downloaded successfully. Reading zip file content...")
            zip_file = BytesIO(response.content)

            with zipfile.ZipFile(zip_file, 'r') as z:
                csv_filename = z.namelist()[0]
                with z.open(csv_filename) as csv_file:
                    try:
                        cols = {
                            'TP_FUNDO': 'fund_type',
                            'CNPJ_FUNDO': 'fund_cnpj',
                            'DT_COMPTC': 'date', 
                            'VL_QUOTA': 'fund_nav',
                            'VL_PATRIM_LIQ': 'fund_total_equity',
                            'VL_TOTAL': 'fund_total_value',
                            'CAPTC_DIA': 'fund_inflows',
                            'RESG_DIA': 'fund_outflows',
                            'NR_COTST': 'fund_holders'
                        }
                        df = pd.read_csv(csv_file, sep=';', usecols=cols.keys(), encoding='latin-1', engine="pyarrow", parse_dates=['DT_COMPTC'])
                        df = df.rename(columns=cols)
                    except (ValueError, KeyError):
                        self.logger.info(f"Failed to read with old columns, trying new column format for {date_str}")
                        # The file needs to be read again from the start
                        zip_file.seek(0)
                        with zipfile.ZipFile(zip_file, 'r') as z_retry:
                            with z_retry.open(csv_filename) as csv_file_retry:
                                cols_new = {
                                    'TP_FUNDO_CLASSE': 'fund_type',
                                    'CNPJ_FUNDO_CLASSE': 'fund_cnpj',
                                    'DT_COMPTC': 'date',
                                    'VL_TOTAL': 'fund_total_value',
                                    'VL_QUOTA': 'fund_nav',
                                    'VL_PATRIM_LIQ': 'fund_total_equity',
                                    'CAPTC_DIA': 'fund_inflows',
                                    'RESG_DIA': 'fund_outflows',
                                    'NR_COTST': 'fund_holders'
                                }
                                df = pd.read_csv(csv_file_retry, sep=';', usecols=cols_new.keys(), encoding='latin-1', engine="pyarrow", parse_dates=['DT_COMPTC'])
                                df = df.rename(columns=cols_new)
                    
                    # df = df.drop(columns=['fund_type'], errors='ignore')
                    # df = df.drop_duplicates()

                    self.logger.info(f"Successfully processed data for {date.strftime('%Y-%m')}. Shape: {df.shape}")
                    return df

        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 404:
                self.logger.warning(f"No data file found for date {date.strftime('%Y-%m')} at {url}.")
            else:
                self.logger.error(f"HTTP error for date {date.strftime('%Y-%m')} from {url}: {e}")
            return None
        except Exception as e:
            self.logger.error(f"An unexpected error occurred for date {date.strftime('%Y-%m')} from {url}: {e}", exc_info=True)
            return None

    def get_data(self, end_date: Optional[str] = None, cnpjs: Optional[list] = None, **kwargs) -> pd.DataFrame:
        """
        Retrieve daily fund data from CVM.
        
        Args:
            end_date (str, optional): The end date for data retrieval in 'YYYY-MM-DD' format. Defaults to today.
            cnpjs (list, optional): A list of CNPJs to filter the results for.
            
        Returns:
            pd.DataFrame: DataFrame with columns: ['fund_cnpj', 'date', 'fund_total_value', 'fund_nav', 'fund_total_equity', 'fund_inflows', 'fund_outflows', 'fund_holders'].
        """
        self._log_processing('cvm')

        end_date_dt = pd.to_datetime(end_date) if end_date else datetime.now()
        
        all_data = []
        date_range = pd.date_range(start=self.start_date, end=end_date_dt, freq='MS')
        for date in date_range:
            monthly_df = self._download_and_process_month(date)
            if monthly_df is not None and not monthly_df.empty:
                if cnpjs:
                    self.logger.info(f"Filtering for {len(cnpjs)} CNPJs.")
                    monthly_df = monthly_df[monthly_df['fund_cnpj'].isin(cnpjs)].copy()
                all_data.append(monthly_df)
        
        if not all_data:
            raise DataRetrievalError(f"No data retrieved from CVM for the period {self.start_date} to {end_date_dt.strftime('%Y-%m-%d')}.")
        
        final_df = pd.concat(all_data, ignore_index=True)

        if final_df.empty:
            self.logger.warning("Dataframe is empty after filtering by CNPJs or for the period.")
            return pd.DataFrame()
        
        final_df = final_df.sort_values(by=['fund_cnpj', 'date', 'fund_total_equity'], ascending=True)
        final_df = final_df.drop_duplicates(subset=['fund_cnpj', 'date'], keep='last')
        final_df = final_df.drop(columns=['fund_type'])
        final_df = final_df.reset_index(drop=True)
        return final_df

