from typing import Optional
import pandas as pd
import requests
from io import BytesIO

from .base import DataProvider, DataRetrievalError


class MDICProvider(DataProvider):
    """Provider for MDIC (Ministério da Indústria, Comércio Exterior e Serviços) data."""

    def __init__(self):
        super().__init__()
        self.url = "https://balanca.economia.gov.br/balanca/SH/TOTAL.xlsx"
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }

    def get_data(self, category: str, data_type: str = 'trade_balance', **kwargs) -> pd.DataFrame:
        """
        Retrieve Brazilian trade balance data from MDIC.
        """
        self._log_processing(category)

        try:
            response = requests.get(self.url, headers=self.headers)
            response.raise_for_status()
            self.logger.info("File downloaded successfully from MDIC.")
        except requests.exceptions.HTTPError as e:
            raise DataRetrievalError(f"HTTP error while downloading MDIC data from {self.url}: {e}")
        except Exception as e:
            raise DataRetrievalError(f"Failed to download MDIC data from {self.url}: {e}")

        try:
            excel_file = BytesIO(response.content)
            df = pd.read_excel(excel_file, sheet_name='DADOS_SH', header=0)

            self.logger.info("Successfully parsed excel file and set header.")

        except Exception as e:
            raise DataRetrievalError(f"Failed to parse the MDIC Excel file: {e}")

        COLUMNS_TO_RENAME = {
            'CO_ANO': 'year',
            'CO_MES': 'month',
            'DIAS ÚTEIS': 'business_days',
            'US$ FOB_EXP': 'br_trade_balance_fob_exports_usd',
            'KG_EXP': 'br_trade_balance_exports_kg',
            'US$ FOB_IMP': 'br_trade_balance_fob_imports_usd',
            'KG_IMP': 'br_trade_balance_imports_kg',
            'SALDO_US$ FOB': 'br_trade_balance_fob_net_usd',
            'SALDO_KG': 'br_trade_balance_net_kg',
            'CORRENTE_US$ FOB': 'br_trade_balance_current_fob_usd',
            'CORRENTE_KG': 'br_trade_balance_current_kg',
        }
        df.rename(columns=COLUMNS_TO_RENAME, inplace=True)

        df['date'] = pd.to_datetime(df['year'].astype(str) + '-' + df['month'].astype(str) + '-01', errors='coerce')
        df.dropna(subset=['date'], inplace=True)

        df = df[['date', 'br_trade_balance_fob_exports_usd', 'br_trade_balance_fob_imports_usd', 'br_trade_balance_fob_net_usd']]
        df = df.melt(id_vars=['date'], var_name='code', value_name='value')
        df['field'] = 'close'

        if df.empty:
            raise DataRetrievalError("No data parsed from MDIC file or file is empty.")
        
        self.logger.info(f"Successfully extracted table from MDIC. Shape: {df.shape}")
        
        return self._validate_output(df)
