from datetime import datetime
from typing import Optional, Union, List
import pandas as pd
import numpy as np
from itertools import product

# from ..db.operations import read_sql
from persevera_tools.db.operations import read_sql

def get_emissions(index_code: Optional[Union[str, List[str]]] = None,
                  start_date: Optional[Union[str, datetime, pd.Timestamp]] = None,
                  selected_fields: Optional[List[str]] = None,
                  deb_incent_lei_12431: Optional[bool] = None) -> pd.DataFrame:
    """Get emissions from credito_privado_emissoes table.
    
    Args:
        index_code: Single index code, list of index codes, or None to retrieve all codes.
        start_date: Optional start date filter as string 'YYYY-MM-DD', datetime, or pandas Timestamp
        selected_fields: Optional list of fields to retrieve
        deb_incent_lei_12431: Whether to retrieve emissions under the Debêntures Incentivadas by Lei 12431 (default: None)
    Returns:
        DataFrame with emissions data, indexed by 'data_emissao'.
    """
    # Validate index_code
    index_codes = []
    if index_code is not None:
        if isinstance(index_code, str):
            index_codes = [index_code]
        elif isinstance(index_code, list):
            index_codes = index_code
        else:
            raise ValueError("index_code must be a string or list of strings")
        
        if not all(isinstance(idx, str) and idx for idx in index_codes):
            raise ValueError("All index codes must be non-empty strings")

    if selected_fields is not None:
        if not all(isinstance(field, str) and field for field in selected_fields):
            raise ValueError("All selected fields must be non-empty strings")
    else:
        selected_fields = ['code', 'empresa', 'data_emissao','data_vencimento', 'valor_nominal_na_emissao', 'quantidade_emitida', 'indice', 'percentual_multiplicador_rentabilidade']
    
    # Convert and validate dates if provided
    start_date_str = None
    
    if start_date is not None:
        if isinstance(start_date, str):
            try:
                start_dt = datetime.strptime(start_date, "%Y-%m-%d")
                start_date_str = start_date
            except ValueError:
                raise ValueError("start_date string must be in YYYY-MM-DD format")
        elif isinstance(start_date, (datetime, pd.Timestamp)):
            start_dt = start_date
            start_date_str = start_dt.strftime("%Y-%m-%d")
        else:
            raise ValueError("start_date must be a string, datetime, or pandas Timestamp")
    
    # Build query
    field_str = ", ".join(selected_fields)
    
    query = f"""
        SELECT {field_str}
        FROM credito_privado_emissoes 
    """
    
    where_clauses = []
    if index_codes:
        index_str = "','".join(index_codes)
        where_clauses.append(f"indice IN ('{index_str}')")

    if start_date_str:
        where_clauses.append(f"data_emissao >= '{start_date_str}'")
        
    if deb_incent_lei_12431 is not None:
        if deb_incent_lei_12431:
            where_clauses.append(f"deb_incent_lei_12431 = TRUE")
        else:
            where_clauses.append(f"deb_incent_lei_12431 = FALSE")
        
    if where_clauses:
        query += " WHERE " + " AND ".join(where_clauses)
        
    query += " ORDER BY data_emissao, code"
    
    date_fields = [field for field in selected_fields if 'data' in field]
    df = read_sql(query, date_columns=date_fields)
    
    if df.empty:
        raise ValueError("No data found")
    
    if 'quantidade_emitida' in df.columns and 'valor_nominal_na_emissao' in df.columns:
        df['volume_emissao'] = df['valor_nominal_na_emissao'] * df['quantidade_emitida']

    if 'data_emissao' in df.columns:
        df = df.set_index('data_emissao')
    
    return df

def get_series(code: Optional[Union[str, List[str]]] = None,
               category: Optional[str] = None,
               start_date: Optional[Union[str, datetime, pd.Timestamp]] = None, 
               end_date: Optional[Union[str, datetime, pd.Timestamp]] = None,
               field: Union[str, List[str]] = 'yield_to_maturity') -> Union[pd.DataFrame, pd.Series]:
    """Get time series data for one or more indicators from the database.
    
    Args:
        code: Single indicator code, list of codes, or None to retrieve all codes.
        start_date: Optional start date filter as string 'YYYY-MM-DD', datetime, or pandas Timestamp.
        end_date: Optional end date filter as string 'YYYY-MM-DD', datetime, or pandas Timestamp.
        field: Field or list of fields to retrieve (default: 'close').
        
    Returns:
        pd.Series or pd.DataFrame: 
        - A Series if a single code and a single field are requested.
        - A DataFrame with fields as columns if a single code and multiple fields are requested.
        - A DataFrame with codes as columns if multiple codes and a single field are requested.
        - A DataFrame with a MultiIndex (code, field) for columns if multiple codes and fields are requested.
        The columns will be ordered according to the input lists of codes and fields if provided.
        
    Raises:
        ValueError: If dates are in invalid format or if end_date is before start_date.
    """
    # Validate codes
    codes = []
    if code is not None:
        if isinstance(code, str):
            codes = [code]
        elif isinstance(code, list):
            codes = code
        else:
            raise ValueError("code must be a string or list of strings")
    
        if not all(isinstance(c, str) and c for c in codes):
            raise ValueError("All codes must be non-empty strings")

    # Validate fields
    if isinstance(field, str):
        fields = [field]
    elif isinstance(field, list):
        fields = field
    else:
        raise ValueError("field must be a string or list of strings")

    if not all(isinstance(f, str) and f for f in fields):
        raise ValueError("All fields must be non-empty strings")

    # Convert and validate dates if provided
    start_date_str = None
    end_date_str = None
    
    if start_date is not None:
        if isinstance(start_date, str):
            try:
                start_dt = datetime.strptime(start_date, "%Y-%m-%d")
                start_date_str = start_date
            except ValueError:
                raise ValueError("start_date string must be in YYYY-MM-DD format")
        elif isinstance(start_date, (datetime, pd.Timestamp)):
            start_dt = start_date
            start_date_str = start_dt.strftime("%Y-%m-%d")
        else:
            raise ValueError("start_date must be a string, datetime, or pandas Timestamp")
    
    if end_date is not None:
        if isinstance(end_date, str):
            try:
                end_dt = datetime.strptime(end_date, "%Y-%m-%d")
                end_date_str = end_date
            except ValueError:
                raise ValueError("end_date string must be in YYYY-MM-DD format")
        elif isinstance(end_date, (datetime, pd.Timestamp)):
            end_dt = end_date
            end_date_str = end_dt.strftime("%Y-%m-%d")
        else:
            raise ValueError("end_date must be a string, datetime, or pandas Timestamp")
    
    if start_date is not None and end_date is not None and start_dt > end_dt:
        raise ValueError("end_date cannot be before start_date")

    # Build query using validated parameters
    if category == 'credito_privado_di':
        table_name = 'credito_privado_historico'
        cols = ['date', 'code', 'field', 'value']
        date_cols = ['date']
    elif category == 'credito_privado_ipca':
        table_name = 'credito_privado_historico'
        cols = ['date', 'code', 'reference', 'value']
        date_cols = ['date', 'reference']
    elif category == 'titulos_publicos':
        table_name = 'anbima_titulos_publicos_historico'
        cols = ['date', 'code', 'maturity', 'value']
        date_cols = ['date', 'maturity']
    else:
        raise ValueError("Invalid category")

    fields_str = "','".join(fields)
    cols_str = ",".join(cols)
    query = f"""
        SELECT {cols_str}
        FROM {table_name} 
        WHERE field IN ('{fields_str}')
    """

    if codes:
        codes_str = "','".join(codes)
        query += f" AND code IN ('{codes_str}')"
    
    if start_date_str:
        query += f" AND date >= '{start_date_str}'"
    if end_date_str:
        query += f" AND date <= '{end_date_str}'"
        
    query += " ORDER BY date, code, field"
    
    df = read_sql(query, date_columns=date_cols)
    if category in ['credito_privado_ipca', 'titulos_publicos']:
        return df
    
    if df.empty:
        if codes:
            raise ValueError(f"No data found for code(s) {codes} with field(s) {fields}")
        else:
            raise ValueError(f"No data found for any codes with field(s) {fields}")
    
    # Pivot the data to get the desired format
    df = df.pivot_table(
        index='date',
        columns=['code', 'field'],
        values='value'
    )
    
    # Simplify output and reorder columns
    if code is not None:
        if len(codes) == 1 and len(fields) == 1:
            series = df.iloc[:, 0]
            series.name = fields[0]
            return series
        elif len(codes) == 1:
            df = df.droplevel('code', axis=1)
            df = df.reindex(columns=fields)
        elif len(fields) == 1:
            df = df.droplevel('field', axis=1)
            df = df.reindex(columns=codes)
        else:
            new_columns = list(product(codes, fields))
            df = df.reindex(columns=new_columns)
    elif len(fields) == 1:
        df = df.droplevel('field', axis=1)
    
    # Drop columns with all NaN values
    df = df.dropna(how='all', axis=1)

    return df

def calculate_spread(index_code: str,
                     start_date: Optional[Union[str, datetime, pd.Timestamp]] = None,
                     end_date: Optional[Union[str, datetime, pd.Timestamp]] = None,
                     field: Union[str, List[str]] = 'yield_to_maturity',
                     calculate_distribution: bool = False,
                     deb_incent_lei_12431: Optional[bool] = None) -> pd.DataFrame:
    """Calculate the spread for a given code.
    
    Args:
        index_code: Single index code
        start_date: Optional start date filter as string 'YYYY-MM-DD', datetime, or pandas Timestamp.
        end_date: Optional end date filter as string 'YYYY-MM-DD', datetime, or pandas Timestamp.
        field: Field or list of fields to retrieve (default: 'yield_to_maturity')
        calculate_distribution: Whether to calculate the distribution of the spread
    Returns:
        DataFrame with spread data, indexed by 'date'.
    """
    # Get the series for the index
    emissions = get_emissions(index_code=index_code, deb_incent_lei_12431=deb_incent_lei_12431)
    
    if index_code == 'DI':
        codes = emissions[emissions['percentual_multiplicador_rentabilidade'] == 100]['code'].tolist()
        series = get_series(code=codes, category='credito_privado_di', start_date=start_date, end_date=end_date, field=field)
        series = series.interpolate(limit=5)
    elif index_code == 'IPCA':
        codes = emissions['code'].tolist()
        series_ipca = get_series(code=codes, category='credito_privado_ipca', start_date=start_date, end_date=end_date, field=field)
        series_ipca = series_ipca.replace(0., np.nan)
        series_ipca_interpolated = series_ipca.pivot_table(index='date', columns='code', values='value').interpolate(limit=5).stack().reset_index()
        series_ipca_interpolated = pd.merge(series_ipca_interpolated, series_ipca[['code', 'reference']].drop_duplicates(), on=['code'], how='left').dropna().drop_duplicates()
        series_ipca_interpolated.columns = ['date', 'code', 'value', 'reference']

        series_titulos_publicos = get_series(code='NTN-B', category='titulos_publicos', start_date=start_date, end_date=end_date, field=field)
        series_merged = pd.merge(series_ipca_interpolated, series_titulos_publicos, left_on=['date', 'reference'], right_on=['date', 'maturity'], how='inner')
        series_merged = series_merged.drop(columns=['code_y', 'maturity', 'reference'])
        series_merged.columns = ['date', 'code', 'yield_to_maturity', 'ytm_ntnb']
        series_merged['spread'] = series_merged['yield_to_maturity'] - series_merged['ytm_ntnb']
        series = series_merged.pivot_table(index='date', columns='code', values='spread')
        series = series.interpolate(limit=5)
    else:
        raise ValueError("Invalid index code")
    
    emissions = emissions[emissions['code'].isin(series.columns)]

    volume_map = emissions.set_index('code')['volume_emissao']
    volume_df = pd.DataFrame({col: volume_map[col] for col in series.columns}, index=series.index)
    
    volume_df = (volume_df * (series > 0)).replace(0., np.nan)
    weight_df = volume_df.div(volume_df.sum(axis=1), axis=0)

    # Calculate the spread
    spread = pd.DataFrame(index=series.index)
    spread['median'] = series.median(axis=1)
    spread['mean'] = series.mean(axis=1)
    spread['weighted_mean'] = (series * weight_df).sum(axis=1)

    if calculate_distribution:
        spread['count_above_mean'] = (series.T > spread['mean'].values).T.sum(axis=1)
        spread['count_under_mean'] = (series.T <= spread['mean'].values).T.sum(axis=1)
        spread['volume_above_mean'] = ((series.T > spread['mean'].values).T * volume_df).sum(axis=1)
        spread['volume_under_mean'] = ((series.T <= spread['mean'].values).T * volume_df).sum(axis=1)
        
        spread['count_yield_0_50bp'] = (series.T < 0.50).T.sum(axis=1)
        spread['count_yield_50_75bp'] = ((series.T >= 0.50) & (series.T < 0.75)).T.sum(axis=1)
        spread['count_yield_75_100bp'] = ((series.T >= 0.75) & (series.T < 1.00)).T.sum(axis=1)
        spread['count_yield_100_150bp'] = ((series.T >= 1.00) & (series.T < 1.50)).T.sum(axis=1)
        spread['count_yield_150_250bp'] = ((series.T >= 1.50) & (series.T < 2.50)).T.sum(axis=1)
        spread['count_yield_above_250bp'] = (series.T >= 2.50).T.sum(axis=1)
    return spread
    