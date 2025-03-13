from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Any
import pandas as pd
import logging
from datetime import datetime

class DataProviderError(Exception):
    """Base exception for data provider errors."""
    pass

class ValidationError(DataProviderError):
    """Raised when data validation fails."""
    pass

class DataRetrievalError(DataProviderError):
    """Raised when data retrieval fails."""
    pass

class DataProvider(ABC):
    """Base class for all data providers."""
    
    def __init__(self, start_date: str = '1980-01-01'):
        """
        Initialize the data provider.
        
        Args:
            start_date: The start date for data retrieval in 'YYYY-MM-DD' format
            
        Raises:
            ValueError: If start_date is not in the correct format
        """
        try:
            self.start_date = pd.to_datetime(start_date)
        except ValueError as e:
            raise ValueError(f"Invalid start_date format. Expected 'YYYY-MM-DD', got {start_date}") from e
            
        self.logger = logging.getLogger(self.__class__.__name__)

    @abstractmethod
    def get_data(self, category: str, **kwargs) -> pd.DataFrame:
        """
        Retrieve data for the specified category.
        
        Args:
            category: The category of data to retrieve
            **kwargs: Additional arguments specific to the provider
            
        Returns:
            DataFrame with columns: ['date', 'code', 'field', 'value']
            
        Raises:
            DataRetrievalError: If data retrieval fails
            ValidationError: If retrieved data fails validation
        """
        pass

    def _validate_output(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Validate and clean the output DataFrame.
        
        Args:
            df: DataFrame to validate
            
        Returns:
            Cleaned and validated DataFrame
            
        Raises:
            ValidationError: If validation fails
        """
        required_cols = ['date', 'code', 'field', 'value']
        
        # Check required columns
        missing_cols = [col for col in required_cols if col not in df.columns]
        if missing_cols:
            raise ValidationError(f"Missing required columns: {missing_cols}")
        
        try:
            df = df.copy()
            
            # Validate and convert date column
            df['date'] = pd.to_datetime(df['date'])
            
            # Validate code column
            if df['code'].isna().any():
                raise ValidationError("Found null values in 'code' column")
            
            # Clean value column
            df = df.dropna(subset=['value'])
            df['value'] = df['value'].astype(float)
            
            # Check for infinite values
            if not df['value'].isfinite().all():
                raise ValidationError("Found infinite values in 'value' column")
            
            # Filter by start date and sort
            df = df[df['date'] >= self.start_date]
            if df.empty:
                self.logger.warning("No data points found after start_date filter")
                return df
            
            df = df.sort_values(['date', 'code', 'field'], ascending=[False, True, True])
            
            return df[required_cols].reset_index(drop=True)
            
        except Exception as e:
            if not isinstance(e, ValidationError):
                raise ValidationError(f"Data validation failed: {str(e)}") from e
            raise

    def _log_processing(self, category: str) -> None:
        """
        Log that we're processing a category.
        
        Args:
            category: The category being processed
        """
        self.logger.info(f"Processing '{category}'...") 