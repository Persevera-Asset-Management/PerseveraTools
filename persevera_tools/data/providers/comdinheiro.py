import requests
import pandas as pd
from datetime import datetime
from urllib.parse import urlencode

from .base import DataProvider, DataRetrievalError


class ComdinheiroProvider(DataProvider):
    """Provider for Comdinheiro data."""

    # It is recommended to use environment variables for credentials.
    USERNAME = "persevera_asset"
    PASSWORD = "ymr0pzr_xwa5wku5NMQ"  # This should not be hardcoded.
    API_URL = "https://api.comdinheiro.com.br/v1/ep1/import-data"
    HEADERS = {'Content-Type': 'application/x-www-form-urlencoded'}

    def __init__(self, start_date: str = '1980-01-01'):
        super().__init__(start_date)

    def _fetch_positions(self, date_str: str, portfolios: list[str], variables: list[str]) -> pd.DataFrame:
        """
        Fetches portfolio positions from the Comdinheiro API.
        """
        report_url_params = {
            'data_analise': date_str,
            'nome_portfolio': '+'.join(portfolios),
            'variaveis': '+'.join(variables),
            'filtro': 'all',
            'filtro_IF': 'todos',
            'layout': '0',
            'layoutB': '0',
            'enviar_email': '0',
        }
        report_url = "RelatorioGerencialCarteiras001.php?" + urlencode(report_url_params)

        payload_params = {
            "username": self.USERNAME,
            "password": self.PASSWORD,
            "URL": report_url,
            "format": "json3",
        }
        payload = urlencode(payload_params)

        try:
            response = requests.post(self.API_URL, data=payload, headers=self.HEADERS)
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            raise DataRetrievalError(f"API request failed: {e}")

        try:
            data = response.json()
            if 'tables' in data and 'tab0' in data['tables']:
                df = pd.DataFrame(data['tables']['tab0']).set_index('lin0').T.reset_index(drop=True)
                return df
            else:
                self.logger.warning("Received unexpected data structure from API: %s", data)
                return pd.DataFrame()
        except ValueError:
            raise DataRetrievalError("Failed to decode JSON from response.")

    def get_data(self, category: str = 'portfolio_positions', **kwargs) -> pd.DataFrame:
        """
        Retrieve data from Comdinheiro.

        For 'portfolio_positions', kwargs must contain:
        - portfolios (list[str]): A list of portfolio names.
        - date_str (str): The analysis date in 'DDMMYYYY' format.

        Args:
            category (str): The category of data to retrieve. Defaults to 'portfolio_positions'.
            **kwargs: Additional arguments.

        Returns:
            pd.DataFrame: DataFrame with columns: ['date', 'code', 'field', 'value']
        """
        self._log_processing(category)

        if category == 'portfolio_positions':
            portfolios = kwargs.get('portfolios')
            date_str = kwargs.get('date_str')

            if not portfolios or not date_str:
                raise ValueError("`portfolios` and `date_str` must be provided for 'portfolio_positions'")

            variable_names = ["nome_portfolio", "ativo", "desc", "saldo_bruto"]
            
            raw_df = self._fetch_positions(date_str, portfolios, variable_names)

            if raw_df.empty:
                return pd.DataFrame()

            raw_df['date'] = pd.to_datetime(date_str, format='%d%m%Y')
            
            # We are interested in saldo_bruto, which is a numeric value.
            # Other fields like 'desc' are descriptive.
            
            # Ensure saldo_bruto is numeric, coerce errors will turn non-numerics into NaT
            raw_df['saldo_bruto'] = pd.to_numeric(raw_df['saldo_bruto'], errors='coerce')
            raw_df.dropna(subset=['saldo_bruto'], inplace=True)
            
            df = raw_df.rename(columns={
                'ativo': 'code',
                'saldo_bruto': 'value'
            })
            
            df['field'] = 'saldo_bruto'

            return self._validate_output(df[['date', 'code', 'field', 'value']])
        
        else:
            raise NotImplementedError(f"Category '{category}' not supported for Comdinheiro provider.")