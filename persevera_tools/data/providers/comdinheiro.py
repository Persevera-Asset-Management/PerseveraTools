import requests
import pandas as pd
from datetime import datetime, timedelta
from urllib.parse import urlencode

from .base import DataProvider, DataRetrievalError
from ...config import settings
from ...utils.dates import subtract_business_days

COMDINHEIRO_USERNAME = settings.COMDINHEIRO_USERNAME
COMDINHEIRO_PASSWORD = settings.COMDINHEIRO_PASSWORD

class ComdinheiroProvider(DataProvider):
    """Provider for Comdinheiro data."""

    API_URL = "https://api.comdinheiro.com.br/v1/ep1/import-data"
    HEADERS = {'Content-Type': 'application/x-www-form-urlencoded'}

    def __init__(self, username: str = None, password: str = None):
        super().__init__()
        self.username = username or COMDINHEIRO_USERNAME
        self.password = password or COMDINHEIRO_PASSWORD

    def _fetch_positions(self, date_report: str, portfolios: list[str], variables: list[str]) -> pd.DataFrame:
        """
        Fetches portfolio positions from the Comdinheiro API.
        """
        date_report_str = datetime.strptime(date_report, '%Y-%m-%d').strftime('%d%m%Y')
        report_url_params = {
            'data_analise': date_report_str,
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
            "username": self.username,
            "password": self.password,
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

    def _fetch_historical_positions(self, start_date: str, end_date: str, portfolios: list[str], variables: list[str]) -> pd.DataFrame:
        """
        Fetches historical portfolio positions from the Comdinheiro API.
        """
        start_date_str = datetime.strptime(start_date, '%Y-%m-%d').strftime('%d%m%Y')
        end_date_str = datetime.strptime(end_date, '%Y-%m-%d').strftime('%d%m%Y')
        report_url_params = {
            'data_analise': end_date_str,
            'data_ini': start_date_str,
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
            "username": self.username,
            "password": self.password,
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

    def _fetch_historical_nav(self, start_date: str, end_date: str, portfolios: list[str]) -> pd.DataFrame:
        """
        Fetches historical portfolio NAV from the Comdinheiro API.
        """
        start_date_str = datetime.strptime(start_date, '%Y-%m-%d').strftime('%d%m%Y')
        end_date_str = datetime.strptime(end_date, '%Y-%m-%d').strftime('%d%m%Y')
        report_url_params = {
            'data_analise': end_date_str,
            'data_ini': start_date_str,
            'nome_portfolio': '+'.join(portfolios),
            'portfolio_editavel': '',
            'cotizacao': '0',
            'exibir': '1',
            'colunas_adicionais': '',
        }
        report_url = "RelatorioGerencialCarteiras009.php?" + urlencode(report_url_params)

        payload_params = {
            "username": self.username,
            "password": self.password,
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

    def _fetch_statement(self, portfolio: str, date_inception: str, date_report: str) -> dict[str, pd.DataFrame]:
        """
        Fetches portfolio statement from the Comdinheiro API.
        """
        date_inception_str = datetime.strptime(date_inception, '%Y-%m-%d').strftime('%d%m%Y')
        report_dt = datetime.strptime(date_report, '%Y-%m-%d')
        date_report_str = report_dt.strftime('%d%m%Y')
        date_mtd_str = subtract_business_days(report_dt.replace(day=1), 1).strftime('%d%m%Y')
        date_ytd_str = subtract_business_days(report_dt.replace(day=1, month=1), 1).strftime('%d%m%Y')

        report_url_params = {
            'nome_portfolio': portfolio,
            'layout': 'ExtratoPerseveraOnshore',
            'data_ini': date_inception_str,
            'data_ini2': date_mtd_str,
            'data_ini3': date_ytd_str,
            'data_fim': date_report_str,
            'exibicao': 'default',
            'num_casas': '2',
            'estilo_pdf': 'Persevera',
            'num_pdf': '0',
            'ord_classe': 'pesod',
            'ord_ativo': 'alfc',
            'valores': '1',
            'tema_classe': '',
            'el1_ret': 'cot(mes_atual)+cot(3m)+cot(ano_atual)+cot(12m)',
            'el1_benchmarks': 'CDI+IBOV',
            'el1_exibir_rendimento_nominal': '1',
            'el2_colunas': 'data+saldo_bruto_ini+saldo_mov_bruto+rendimento+saldo_bruto_fim',
            'el2_multiplos_periodos': '0',
            'el2_movext': '1|1||0',
            'el2_layout': 'V',
            'el2_campo_data_ini': date_mtd_str,
            'el3_benchmarks': 'CDI+IPCAdp',
            'el3_graf_pl': '0',
            'el3_y_min': '',
            'el3_y_max': '',
            'el3_cot_tir': 'cot',
            'el3_campo_data_ini': date_inception_str,
            'el4_benchmarks': '',
            'el4_graf_pl': '2',
            'el4_y_min': '90%pl_min',
            'el4_y_max': '',
            'el4_cot_tir': 'cot',
            'el4_campo_data_ini': date_inception_str,
            'el5_aloc': 'pesod|b|pizza||2||1|0|2||rel|TIPO|',
            'el6_aloc': 'pesod|b|pizza||4||1|0|2||rel|IF|',
            'el7_mes_formato': 'V',
            'el7_mes_acumulado': '1',
            'el7_mes_max_anos': '1',
            'el7_mes_benchs': 'CDI+percent(cdi)+IBOV+IPCAdp',
            'el7_cot_tir': 'cot',
            'el7_campo_data_ini': date_inception_str,
            'el8_variavel': 'performanceAttribution',
            'el8_ordem': '1',
            'el8_unidade': 'pp',
            'el8_cores': 'd3d3d3t',
            'el8_classe': 'TIPO',
            'el8_filtro': '',
            'el8_campo_data_ini': date_mtd_str,
            'el9_variavel': 'performanceAttribution',
            'el9_ordem': '1',
            'el9_unidade': 'pp',
            'el9_cores': 'd3d3d3t',
            'el9_classe': 'TIPO',
            'el9_filtro': '',
            'el9_campo_data_ini': date_ytd_str,
            'el10_colunas': 'cor+ativo+nom(mes_atual)+nom(ano_atual)',
            'el10_ret_classe': '0',
            'el10_ret_nulos': '0',
            'el10_ret_bench_ativo': '',
            'el10_bench_classe': '',
            'el10_linha_cart': '1',
            'el10_classe': 'TIPO',
            'el10_subclasse': '',
            'el10_filtro': '',
            'el11_colunas': 'cor+ativo+saldoBrutoIni+aplicacoes+resgates+rendimento_nominal+saldoBrutoFim+percentual_SB',
            'el11_classe': 'TIPO',
            'el11_subclasse': '',
            'el11_filtro': '',
            'el11_campo_data_ini': date_mtd_str,
            'el12_liq': '0+1+3+16+91+360|corridos|ambos|0|2|bru+per_bru|tabela|1',
            'el13_colunas': 'data_liquidacao+classe+descricao+operacao+quantidade+valor_bruto',
            'el13_filtro': 'Fund+TitPub+Acoes+Deb+TitPriv+Clube+MercFut+Gen+Prev+CriCra+Poup+Opcoes+FundOff+Bond',
            'el13_filtro_cv': 'AM+C+DV+IN+SP+T+TIF+VN+V+VT',
            'el13_campo_data_ini': date_mtd_str,
        }
        report_url = "MeuExtrato/MeuExtrato001.php?" + urlencode(report_url_params)

        payload_params = {
            "username": self.username,
            "password": self.password,
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
            if 'tables' in data:
                tables = {}
                table_codes = {
                    'tab0': 'Rentabilidade Bruta',
                    'tab1': 'Resumo Financeiro - no Mês',
                    'tab2': 'Rentabilidades mês a mês',
                    'tab3': 'Rentabilidade Ativos por Classe',
                    'tab4': 'Posição Consolidada - No Mês',
                    'tab5': 'Liquidez dos Ativos',
                    'tab6': 'Movimentações - no Mês',
                }
                for table_code, table_name in table_codes.items():
                    if table_code in data['tables']:
                        df = pd.DataFrame(data['tables'][table_code]).set_index('lin0').T.reset_index(drop=True)
                        if '' in df.columns:
                            df.drop(columns=[''], inplace=True)
                        tables[table_name] = df
                return tables
            else:
                self.logger.warning("Received unexpected data structure from API: %s", data)
                return {}
        except ValueError:
            raise DataRetrievalError("Failed to decode JSON from response.")

    def get_data(self, category: str, data_type: str = 'portfolio_positions', **kwargs) -> pd.DataFrame | dict[str, pd.DataFrame]:
        """
        Retrieve data from Comdinheiro.

        For 'portfolio_positions', kwargs must contain:
        - portfolios (list[str]): A list of portfolio names.
        - date_report (str): The analysis date in 'YYYY-MM-DD' format.

        For 'portfolio_statement', kwargs must contain:
        - portfolio (str): The portfolio name.
        - date_inception (str): The inception date in 'YYYY-MM-DD' format (maps to data_ini).
        - date_report (str): The report date in 'YYYY-MM-DD' format (maps to data_fim).

        For 'portfolio_historical_positions', kwargs must contain:
        - portfolios (list[str]): A list of portfolio names.
        - start_date (str): The start date in 'YYYY-MM-DD' format.
        - end_date (str): The end date in 'YYYY-MM-DD' format.

        For 'portfolio_nav', kwargs must contain:
        - portfolios (list[str]): A list of portfolio names.
        - start_date (str): The start date in 'YYYY-MM-DD' format.
        - end_date (str): The end date in 'YYYY-MM-DD' format.

        Args:
            category (str): The category of data to retrieve. Defaults to 'portfolio_positions'.
            **kwargs: Additional arguments.

        Returns:
            pd.DataFrame or dict[str, pd.DataFrame]: A DataFrame for single-table queries 
            or a dictionary of DataFrames for multi-table queries like 'portfolio_statement'.
        """
        self._log_processing(category)

        if data_type == 'portfolio_positions':
            portfolios = kwargs.get('portfolios')
            date_report = kwargs.get('date_report')

            if not portfolios or not date_report:
                raise ValueError("`portfolios` and `date_report` must be provided for 'portfolio_positions'")

            variable_names = {
                "data_analise": "date",
                "nome_portfolio": "carteira",
                "ativo": "ativo",
                "desc": "descricao",
                "saldo_bruto": "saldo_bruto",
                "instituicao_financeira": "instituicao_financeira",
                "tipo_ativo": "tipo_ativo",
                "mv(estrategia)": "estrategia"
            }
            
            df = self._fetch_positions(date_report, portfolios, variable_names.keys())

            if df.empty:
                return pd.DataFrame()

            df.columns = variable_names.values()
            df['date'] = pd.to_datetime(df['date'])

            if 'saldo_bruto' in df.columns:
                df['saldo_bruto'] = pd.to_numeric(df['saldo_bruto'], errors='coerce')
            
            return df
        
        elif data_type == 'portfolio_statement':
            portfolio = kwargs.get('portfolio')
            date_inception = kwargs.get('date_inception')
            date_report = kwargs.get('date_report')

            if not all([portfolio, date_inception, date_report]):
                raise ValueError("`portfolio`, `date_inception`, and `date_report` must be provided for 'portfolio_statement'")

            dfs = self._fetch_statement(portfolio, date_inception, date_report)

            if not dfs:
                return {}
            
            return dfs

        elif data_type == 'portfolio_historical_positions':
            portfolios = kwargs.get('portfolios')
            start_date = kwargs.get('start_date')
            end_date = kwargs.get('end_date')

            if not all([portfolios, start_date, end_date]):
                raise ValueError("`portfolios`, `start_date`, and `end_date` must be provided for 'portfolio_historical_positions'")
            
            variable_names = {
                "data_analise": "date",
                "nome_portfolio": "carteira",
                "ativo": "ativo",
                "desc": "descricao",
                "saldo_bruto": "saldo_bruto",
            }
            
            df = self._fetch_historical_positions(start_date, end_date, portfolios, variable_names.keys())

            if df.empty:
                return pd.DataFrame()

            df.columns = variable_names.values()
            df['date'] = pd.to_datetime(df['date'], format='%d/%m/%Y')

            if 'saldo_bruto' in df.columns:
                df['saldo_bruto'] = pd.to_numeric(df['saldo_bruto'], errors='coerce')
            
            return df

        elif data_type == 'portfolio_nav':
            portfolios = kwargs.get('portfolios')
            start_date = kwargs.get('start_date')
            end_date = kwargs.get('end_date')

            if not all([portfolios, start_date, end_date]):
                raise ValueError("`portfolios`, `start_date`, and `end_date` must be provided for 'portfolio_nav'")
            
            df = self._fetch_historical_nav(start_date, end_date, portfolios)

            if df.empty:
                return pd.DataFrame()
            
            return df

        else:
            raise NotImplementedError(f"Data type '{data_type}' not supported for Comdinheiro provider.")