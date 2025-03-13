from typing import Optional, Dict, List, Union, Literal, Any
import pandas as pd
import logging
from datetime import datetime

from .providers.bloomberg import BloombergProvider, DataCategory
from .providers.sgs import SGSProvider
from .providers.fred import FredProvider
from .providers.sidra import SidraProvider
from .providers.anbima import AnbimaProvider
from ..db.operations import to_sql

logger = logging.getLogger(__name__)


class FinancialDataService:
    """High-level interface for financial data retrieval and storage from multiple sources."""
    
    def __init__(
        self,
        start_date: str = '1980-01-01',
        fred_api_key: Optional[str] = None,
        bloomberg_tickers_mapping: Optional[Dict[str, Dict[str, str]]] = None,
        bloomberg_fields_mapping: Optional[Dict[str, Dict[str, str]]] = None,
        company_fields_file: Optional[str] = None
    ):
        """
        Initialize the financial data service.
        
        Args:
            start_date: The start date for data retrieval
            fred_api_key: Optional API key for FRED
            bloomberg_tickers_mapping: Optional custom mapping of Bloomberg tickers to internal codes
            bloomberg_fields_mapping: Optional custom mapping of Bloomberg fields to internal fields
            company_fields_file: Optional path to Excel file with company field mappings
        """
        self.bloomberg = BloombergProvider(
            start_date=start_date,
            tickers_mapping=bloomberg_tickers_mapping,
            fields_mapping=bloomberg_fields_mapping,
            company_fields_file=company_fields_file
        )
        self.sgs = SGSProvider(start_date=start_date)
        self.fred = FredProvider(start_date=start_date, api_key=fred_api_key)
        self.sidra = SidraProvider(start_date=start_date)
        self.anbima = AnbimaProvider(start_date=start_date)
        self.logger = logging.getLogger(self.__class__.__name__)
        
    def get_bloomberg_data(
        self,
        category: DataCategory,
        data_type: Literal['market', 'company'] = 'market',
        additional_fields: Optional[str] = None,
        exchanges: Optional[List[str]] = None,
        best_fperiod_override: Optional[str] = None,
        use_fund_currency: bool = False,
        index_list: Optional[List[str]] = None,
        custom_tickers: Optional[Dict[str, str]] = None,
        custom_fields: Optional[Dict[str, str]] = None,
        save_to_db: bool = True,
        retry_attempts: int = 3,
        table_name: Optional[str] = None
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
            save_to_db: Whether to save the data to the database
            retry_attempts: Number of retry attempts for Bloomberg API calls
            table_name: Optional custom table name for database storage
            
        Returns:
            DataFrame with columns: ['date', 'code', 'field', 'value']
            
        Raises:
            ValueError: If category or additional_fields are invalid
            RuntimeError: If data retrieval fails after all retry attempts
        """
        self.logger.info(f"Retrieving {category} {data_type} data from Bloomberg" + 
                        (f" with {additional_fields}" if additional_fields else ""))
        
        attempt = 0
        last_error = None
        
        while attempt < retry_attempts:
            try:
                df = self.bloomberg.get_data(
                    category=category,
                    data_type=data_type,
                    additional_fields=additional_fields,
                    exchanges=exchanges,
                    best_fperiod_override=best_fperiod_override,
                    use_fund_currency=use_fund_currency,
                    index_list=index_list,
                    custom_tickers=custom_tickers,
                    custom_fields=custom_fields
                )
                
                if df.empty:
                    self.logger.warning(f"No data retrieved for {category}")
                    return df
                
                if save_to_db:
                    self.logger.info(f"Saving {len(df)} rows to database")
                    try:
                        # Use custom table name if provided, otherwise use default based on data_type
                        db_table = table_name or ('indicadores' if data_type == 'market' else 'factor_zoo')
                        
                        to_sql(
                            data=df,
                            table_name=db_table,
                            primary_keys=['code', 'date', 'field'],
                            update=True,
                            batch_size=5000 if data_type == 'company' else None
                        )
                    except Exception as e:
                        self.logger.error(f"Failed to save data to database: {str(e)}")
                        raise
                
                return df
                
            except Exception as e:
                attempt += 1
                last_error = e
                self.logger.warning(f"Attempt {attempt} failed: {str(e)}")
                if attempt < retry_attempts:
                    self.logger.info(f"Retrying... ({attempt}/{retry_attempts})")
        
        error_msg = f"Failed to retrieve data after {retry_attempts} attempts. Last error: {str(last_error)}"
        self.logger.error(error_msg)
        raise RuntimeError(error_msg)
    
    def get_data(
        self,
        source: Literal['sgs', 'fred', 'sidra', 'anbima'],
        save_to_db: bool = True,
        retry_attempts: int = 3,
        table_name: Optional[str] = None,
        **kwargs
    ) -> pd.DataFrame:
        """
        Retrieve data from various sources.
        
        Args:
            source: The data source to use
            save_to_db: Whether to save the data to the database
            retry_attempts: Number of retry attempts
            table_name: Optional custom table name for database storage
            **kwargs: Additional arguments passed to the specific provider
            
        Returns:
            DataFrame with columns: ['date', 'code', 'field', 'value']
        """
        self.logger.info(f"Retrieving data from {source}")
        
        # Map of sources to providers and default table names
        providers = {
            'sgs': (self.sgs, 'indicadores'),
            'fred': (self.fred, 'indicadores'),
            'sidra': (self.sidra, 'indicadores'),
            'anbima': (self.anbima, 'indicadores'),
        }
        
        if source not in providers:
            raise ValueError(f"Unknown source: {source}")
            
        provider, default_table = providers[source]
        
        attempt = 0
        last_error = None
        
        while attempt < retry_attempts:
            try:
                df = provider.get_data(**kwargs)
                
                if df.empty:
                    self.logger.warning(f"No data retrieved from {source}")
                    return df
                
                if save_to_db:
                    self.logger.info(f"Saving {len(df)} rows to database")
                    try:
                        # Use custom table name if provided, otherwise use default
                        db_table = table_name or default_table
                        
                        to_sql(
                            data=df,
                            table_name=db_table,
                            primary_keys=['code', 'date', 'field'],
                            update=True,
                            batch_size=5000
                        )
                    except Exception as e:
                        self.logger.error(f"Failed to save data to database: {str(e)}")
                        raise
                
                return df
                
            except Exception as e:
                attempt += 1
                last_error = e
                self.logger.warning(f"Attempt {attempt} failed: {str(e)}")
                if attempt < retry_attempts:
                    self.logger.info(f"Retrying... ({attempt}/{retry_attempts})")
        
        error_msg = f"Failed to retrieve data from {source} after {retry_attempts} attempts. Last error: {str(last_error)}"
        self.logger.error(error_msg)
        raise RuntimeError(error_msg)
        
    @staticmethod
    def create_tickers_mapping(tickers_dict: Dict[str, str], category: str) -> Dict[str, Dict[str, str]]:
        """
        Helper method to create a properly formatted tickers mapping.
        
        Args:
            tickers_dict: Dictionary mapping Bloomberg tickers to internal codes
            category: The category to associate with these tickers
            
        Returns:
            Properly formatted tickers mapping for use with BloombergProvider
        """
        return {category: tickers_dict}

    