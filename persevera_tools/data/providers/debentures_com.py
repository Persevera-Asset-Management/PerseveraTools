
import pandas as pd
import numpy as np
import requests
import io

from .base import DataProvider, DataRetrievalError


class DebenturesComProvider(DataProvider):
    """Provider for Debentures.com.br data."""

    def __init__(self):
        super().__init__()
        self.base_url = "https://www.debentures.com.br/exploreosnd/consultaadados/emissoesdedebentures/caracteristicas_e.asp?tip_deb=publicas&"
        self.column_names = [
            'code', 'empresa', 'serie', 'emissao', 'ipo', 'situacao', 'isin',
            'registro_cvm_emissao', 'data_registro_cvm_emissao',
            'registro_cvm_programa', 'data_registro_cvm_do_programa',
            'data_emissao', 'data_vencimento', 'motivo_de_saida',
            'data_saida_novo_vencimento', 'data_inicio_rentabilidade',
            'data_inicio_distribuicao', 'data_proxima_repactuacao',
            'ato_societario_1', 'data_ato_1', 'ato_societario_2', 'data_ato_2',
            'forma', 'garantia_especie', 'classe', 'quantidade_emitida',
            'quantidade_artigo_14', 'quantidade_artigo_24',
            'quantidade_em_mercado', 'quantidade_em_tesouraria',
            'quantidade_resgatada', 'quantidade_cancelada',
            'quantidade_convertida_no_snd', 'quantidade_convertida_fora_do_snd',
            'quantidade_permutada_no_snd', 'quantidade_permutada_fora_do_snd',
            'unidade_monetaria', 'valor_nominal_na_emissao',
            'unidade_monetaria_1', 'valor_nominal_atual', 'data_ult_vna',
            'indice', 'tipo', 'criterio_de_calculo',
            'dia_de_referencia_para_indice_de_precos', 'criterio_para_indice',
            'corrige_a_cada', 'percentual_multiplicador_rentabilidade',
            'limite_da_tjlp', 'tipo_de_tratamento_do_limite_da_tjlp',
            'juros_criterio_antigo_do_snd', 'premios_criterio_antigo_do_snd',
            'amortizacao_taxa', 'amortizacao_cada', 'amortizacao_unidade',
            'amortizacao_data_carencia', 'amortizacao_criterio',
            'tipo_de_amortizacao', 'juros_criterio_novo_taxa',
            'juros_criterio_novo_prazo', 'juros_criterio_novo_cada',
            'juros_criterio_novo_unidade', 'juros_criterio_novo_data_carencia',
            'juros_criterio_novo_criterio', 'juros_criterio_novo_tipo',
            'premio_criterio_novo_taxa', 'premio_criterio_novo_prazo',
            'premio_criterio_novo_cada', 'premio_criterio_novo_unidade',
            'premio_criterio_novo_data_carencia',
            'premio_criterio_novo_criterio', 'premio_criterio_novo_tipo',
            'participacao_taxa', 'participacao_cada', 'participacao_unidade',
            'participacao_data_carencia', 'participacao_descricao',
            'banco_mandatario', 'agente_fiduciario', 'instituicao_depositaria',
            'coordenador_lider', 'cnpj', 'deb_incent_lei_12431',
            'escritura_padronizada', 'resgate_antecipado'
        ]
        self.column_dates = [col for col in self.column_names if 'data_' in col]

    def _fetch_data(self):
        try:
            response = requests.get(self.base_url)
            response.raise_for_status()
            return response.content
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Request failed: {e}")
            raise DataRetrievalError(f"Failed to retrieve data from Debentures.com.br: {e}") from e

    def _parse_data(self, content):
        df = pd.read_csv(
            io.BytesIO(content),
            sep='\t',
            names=self.column_names,
            encoding='latin-1',
            engine='python',
            skiprows=4,
            header=0,
            decimal=','
        )
        df = df.replace('', np.nan)
        df = df.apply(lambda x: x.str.strip() if x.dtype == 'O' else x)

        for col in self.column_dates:
            df[col] = pd.to_datetime(df[col], format="%d/%m/%Y", errors="coerce")

        df = df.replace({
            'S': True, 's': True, 'Sim': True,
            'N': False, 'n': False, 'Não': False,
            '-': None, '': None, np.nan: None,
        })
        
        df = df.dropna(subset=['code', 'data_emissao'])
        df = df.drop_duplicates(subset=['code', 'data_emissao'], keep='last')
        return df

    def _transform_to_long_format(self, df: pd.DataFrame) -> pd.DataFrame:
        id_vars = ['data_emissao', 'code']
        value_vars = [col for col in df.columns if col not in id_vars]
        
        long_df = df.melt(id_vars=id_vars, value_vars=value_vars, var_name='field', value_name='value')
        long_df = long_df.rename(columns={'data_emissao': 'date'})
        
        # Coerce values to numeric where possible, otherwise keep as object
        long_df['value'] = pd.to_numeric(long_df['value'], errors='ignore')
        
        # Handle boolean specifically
        for col in value_vars:
            if df[col].dtype == 'bool':
                 long_df.loc[long_df['field'] == col, 'value'] = long_df.loc[long_df['field'] == col, 'value'].astype(bool)

        long_df = long_df.dropna(subset=['value'])
        return long_df

    def get_data(self, category: str, **kwargs) -> pd.DataFrame:
        self._log_processing(category)
        if category != 'emissions':
            raise NotImplementedError(f"Category '{category}' not supported by DebenturesComProvider.")

        content = self._fetch_data()
        df = self._parse_data(content)
        
        if df.empty:
            self.logger.warning("No data found after parsing.")
            return pd.DataFrame()
        
        # The base class validator expects numeric values, let's filter for them
        # long_df = self._transform_to_long_format(df)
        # numeric_df = long_df[pd.to_numeric(long_df['value'], errors='coerce').notna()].copy()
        # numeric_df['value'] = pd.to_numeric(numeric_df['value'])
        
        return df
