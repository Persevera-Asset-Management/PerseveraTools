from typing import Dict, List, Optional, Union, Literal, Any
import pandas as pd
import numpy as np
from datetime import datetime
import time
import os
from xbbg import blp

from .base import DataProvider, DataRetrievalError
from ..lookups import get_raw_tickers, get_bloomberg_codes, get_securities_by_exchange
from ...config import PERSEVERA_DATA_PATH

DataCategory = Literal[
    # Market data categories
    'positions_cftc', 'macro', 'commodity', 'currency', 'equity',
    # Company data categories - these will be loaded from Excel
    'valuation', 'fundamentals', 'index_weight'
]

class BloombergProvider(DataProvider):
    """Provider for all Bloomberg data - both market and company data."""
    
    COUNTRY_CURRENCIES = {
        'BZ': 'BRL',
        'US': 'USD',
        'CN': 'CAD'
    }
    
    def __init__(
        self, 
        start_date: str = '1980-01-01',
        tickers_mapping: Optional[Dict[str, Dict[str, str]]] = None,
        fields_mapping: Optional[Dict[str, Dict[str, str]]] = None,
        company_fields_file: Optional[str] = None
    ):
        """
        Initialize the Bloomberg data provider.
        
        Args:
            start_date: The start date for data retrieval in 'YYYY-MM-DD' format
            tickers_mapping: Optional custom mapping of Bloomberg tickers to internal codes
                            Format: {'category': {'bloomberg_ticker': 'internal_code', ...}, ...}
            fields_mapping: Optional custom mapping of Bloomberg fields to internal fields
                           Format: {'category': {'bloomberg_field': 'internal_field', ...}, ...}
            company_fields_file: Optional path to Excel file with company field mappings
        """
        super().__init__(start_date)
        self.tickers_mapping = tickers_mapping or {}
        self.fields_mapping = fields_mapping or {}
        self.company_fields_file = company_fields_file
        self._load_field_mappings()
        
    def _load_field_mappings(self) -> None:
        """Load field mappings from the configuration file."""
        try:
            file_path = self.company_fields_file or os.path.join(PERSEVERA_DATA_PATH, "cadastro-base.xlsx")
            if os.path.exists(file_path):
                base = pd.read_excel(file_path, sheet_name='equity_signals')
                self.field_mappings = base.groupby('category').apply(
                    lambda x: x.set_index('bloomberg_code')['mnemonic'].to_dict()
                ).to_dict()
                self.frequencies = base.groupby('category')['frequency'].first().to_dict()
            else:
                self.logger.warning(f"Field mappings file not found: {file_path}")
                self.field_mappings = {}
                self.frequencies = {}
        except Exception as e:
            raise DataRetrievalError(f"Failed to load field mappings: {str(e)}")

    def get_data(
        self,
        category: str,
        data_type: Literal['market', 'company'] = 'market',
        additional_fields: Optional[str] = None,
        exchanges: Optional[List[str]] = None,
        best_fperiod_override: Optional[str] = None,
        use_fund_currency: bool = False,
        index_list: Optional[List[str]] = None,
        custom_tickers: Optional[Dict[str, str]] = None,
        custom_fields: Optional[Dict[str, str]] = None,
        **kwargs
    ) -> pd.DataFrame:
        """
        Retrieve data from Bloomberg.
        
        Args:
            category: The category of data to retrieve
            data_type: Whether to retrieve market or company data
            additional_fields: Optional name of additional fields to retrieve
            exchanges: List of exchanges for company data
            best_fperiod_override: Optional override for BEST_FPERIOD parameter
            use_fund_currency: Whether to use local currency for each exchange
            index_list: List of indices for index weight calculations
            custom_tickers: Optional mapping of Bloomberg tickers to internal codes for this call
            custom_fields: Optional mapping of Bloomberg fields to internal fields for this call
            **kwargs: Additional arguments passed to Bloomberg API
            
        Returns:
            DataFrame with columns: ['date', 'code', 'field', 'value']
        """
        self._log_processing(f"{data_type} data - {category}")
        
        if data_type == 'market':
            return self._get_market_data(
                category=category,
                additional_fields=additional_fields,
                best_fperiod_override=best_fperiod_override,
                custom_tickers=custom_tickers,
                custom_fields=custom_fields,
                **kwargs
            )
        else:
            return self._get_company_data(
                category=category,
                exchanges=exchanges,
                best_fperiod_override=best_fperiod_override,
                use_fund_currency=use_fund_currency,
                index_list=index_list,
                custom_tickers=custom_tickers,
                custom_fields=custom_fields,
                **kwargs
            )
    
    def _get_market_data(
        self,
        category: str,
        additional_fields: Optional[str] = None,
        best_fperiod_override: Optional[str] = None,
        custom_tickers: Optional[Dict[str, str]] = None,
        custom_fields: Optional[Dict[str, str]] = None,
        **kwargs
    ) -> pd.DataFrame:
        """Get market data from Bloomberg."""
        # Use custom tickers if provided, otherwise use the mapping from the category
        # or fall back to the lookup function
        if custom_tickers:
            securities_list = custom_tickers
        elif category in self.tickers_mapping:
            securities_list = self.tickers_mapping[category]
        else:
            securities_list = get_raw_tickers(source='bloomberg', category=category)
        
        if additional_fields:
            if custom_fields:
                field_list = custom_fields
            elif additional_fields in self.fields_mapping:
                field_list = self.fields_mapping[additional_fields]
            else:
                field_list = get_bloomberg_codes(sheet_name='index_signals', category=additional_fields)
                
            df = blp.bdh(
                tickers=securities_list.keys(),
                flds=field_list.keys(),
                start_date=self.start_date,
                BEST_FPERIOD_OVERRIDE=best_fperiod_override,
                **kwargs
            )
            df = df.stack().stack().reset_index()
            df.columns = ['date', 'field', 'code_bloomberg', 'value']
            df['code'] = df['code_bloomberg'].map(securities_list)
            df['field'] = df['field'].map(field_list)
            df = df.drop(columns='code_bloomberg')
        else:
            fields = ["PX_LAST"]
            field_mapping = {'PX_LAST': 'close'}
            
            df = blp.bdh(
                tickers=securities_list.keys(),
                flds=fields,
                start_date=self.start_date,
                **kwargs
            )
            df = df.stack().stack().reset_index()
            df.columns = ['date', 'field', 'code_bloomberg', 'value']
            df['code'] = df['code_bloomberg'].map(securities_list)
            df['field'] = df['field'].map(field_mapping)
            df = df.drop(columns='code_bloomberg')
            
        if category == "macro":
            df = self._process_breakeven_rates(df)
            
        return self._validate_output(df)
    
    def _get_company_data(
        self,
        category: str,
        exchanges: Optional[List[str]] = None,
        best_fperiod_override: Optional[str] = None,
        use_fund_currency: bool = False,
        index_list: Optional[List[str]] = None,
        custom_tickers: Optional[Dict[str, str]] = None,
        custom_fields: Optional[Dict[str, str]] = None,
        **kwargs
    ) -> pd.DataFrame:
        """Get company-specific data from Bloomberg."""
        if exchanges is None:
            exchanges = ['BZ', 'US', 'CN']
            
        # Use custom fields if provided, otherwise use the mapping from the category
        if custom_fields:
            field_list = custom_fields
        else:
            field_list = self.field_mappings.get(category)
            if not field_list:
                raise ValueError(f"Unknown category: {category}")
            
        frequency = self.frequencies.get(category)
        
        all_data = []
        for exchange in exchanges:
            self.logger.info(f"Processing exchange: {exchange}")
            
            # Use custom tickers if provided, otherwise get from exchange
            if custom_tickers:
                securities_list = custom_tickers
            else:
                securities_list = get_securities_by_exchange(exchange=exchange)
                
            self.logger.info(f"{category.upper()}: {len(securities_list)} securities found")
            
            if not securities_list:
                self.logger.warning(f"No securities found for exchange {exchange}")
                continue
                
            try:
                if category == 'index_weight' and index_list:
                    df = self._get_index_weight_data(
                        securities_list=securities_list,
                        field_list=field_list,
                        index_list=index_list
                    )
                else:
                    df = self._get_regular_company_data(
                        securities_list=securities_list,
                        field_list=field_list,
                        frequency=frequency,
                        exchange=exchange if use_fund_currency else None,
                        best_fperiod_override=best_fperiod_override,
                        **kwargs
                    )
                    
                all_data.append(df)
                
            except Exception as e:
                self.logger.error(f"Error processing {exchange}: {str(e)}")
                continue
                
        if not all_data:
            raise DataRetrievalError("No data retrieved from any exchange")
            
        final_df = pd.concat(all_data, ignore_index=True)
        return self._validate_output(final_df)
    
    def _get_index_weight_data(
        self,
        securities_list: Dict[str, str],
        field_list: Dict[str, str],
        index_list: List[str]
    ) -> pd.DataFrame:
        """Get index weight data for multiple indices."""
        all_data = []
        
        for index_rel in index_list:
            self.logger.info(f"Downloading members of {index_rel}...")
            try:
                df = blp.bdh(
                    tickers=securities_list.keys(),
                    flds=field_list.keys(),
                    start_date=self.start_date,
                    REL_INDEX=index_rel,
                )
                
                df = df.stack().stack().reset_index()
                df.columns = ['date', 'field', 'code_bloomberg', 'value']
                df['code'] = df['code_bloomberg'].map(securities_list)
                df['field'] = df['field'].map(field_list) + '_' + index_rel.lower()
                df = df.drop(columns='code_bloomberg')
                
                all_data.append(df)
                time.sleep(5)  # Rate limiting
                
            except Exception as e:
                self.logger.error(f"Error processing index {index_rel}: {str(e)}")
                continue
                
        return pd.concat(all_data, ignore_index=True) if all_data else pd.DataFrame()
    
    def _get_regular_company_data(
        self,
        securities_list: Dict[str, str],
        field_list: Dict[str, str],
        frequency: str,
        exchange: Optional[str] = None,
        best_fperiod_override: Optional[str] = None,
        **kwargs
    ) -> pd.DataFrame:
        """Get regular company data."""
        api_kwargs = {
            'tickers': securities_list.keys(),
            'flds': field_list.keys(),
            'start_date': self.start_date,
            **kwargs
        }
        
        if exchange:
            api_kwargs['EQY_FUND_CRNCY'] = self.COUNTRY_CURRENCIES[exchange]
        if best_fperiod_override:
            api_kwargs['BEST_FPERIOD_OVERRIDE'] = best_fperiod_override
        if frequency == 'quarterly':
            api_kwargs['FILING_STATUS'] = 'OR'
            
        df = blp.bdh(**api_kwargs)
        df = df.stack(0).reset_index()
        df = df.rename(columns={'level_0': 'date', 'level_1': 'bloomberg_code'})
        
        if frequency == 'quarterly':
            df = self._adjust_quarterly_dates(df)
            
        df = pd.melt(df, id_vars=['date', 'bloomberg_code'], value_vars=df.columns)
        df['code'] = df['bloomberg_code'].map(securities_list)
        df['field'] = df['variable'].replace(field_list)
        
        return df[['code', 'date', 'field', 'value']].dropna()
    
    def _process_breakeven_rates(self, df: pd.DataFrame) -> pd.DataFrame:
        """Process macro data to calculate breakeven rates."""
        for vertice in ['_1y', '_2y', '_3y', '_5y', '_10y']:
            temp = (
                df
                .pivot_table(index='date', columns='code', values='value')
                .eval(f"((1 + br_pre{vertice}/100) / (1 + br_ipca{vertice}/100) - 1) * 100")
                .dropna()
            )
            temp = temp.reset_index()
            temp['code'] = f'br_breakeven{vertice}'
            temp.columns = ['date', 'value', 'code']
            temp = temp.assign(field='close')
            df = pd.concat([df, temp], ignore_index=True)
        
        return df
    
    def _adjust_quarterly_dates(self, df: pd.DataFrame) -> pd.DataFrame:
        """Adjust dates for quarterly data using announcement dates."""
        df['ANNOUNCEMENT_DT'] = pd.to_datetime(df['ANNOUNCEMENT_DT'], format="%Y%m%d")
        df['date_adj'] = df['ANNOUNCEMENT_DT'].fillna(df['date'])
        df['date_dif'] = df.groupby('bloomberg_code')['date_adj'].diff(1)
        df['date_adj'] = np.where(df['date_dif'].dt.days < 0, df['date'], df['date_adj'])
        df['date'] = df['date_adj']
        return df.drop(columns=['ANNOUNCEMENT_DT', 'date_adj', 'date_dif']) 