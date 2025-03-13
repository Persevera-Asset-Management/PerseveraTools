"""
Example script demonstrating the use of custom tickers and fields with the FinancialDataService class.
"""

import pandas as pd
from persevera_tools.data.financial_data_service import FinancialDataService

def main():
    # Example 1: Using the default mappings
    print("Example 1: Using default mappings")
    data_service = FinancialDataService(start_date='2023-01-01')
    
    # Get Bloomberg market data using default mappings
    df = data_service.get_bloomberg_data(
        category='equity',
        data_type='market',
        save_to_db=False  # Don't save to database for this example
    )
    print(f"Retrieved {len(df)} rows using default mappings")
    
    # Example 2: Using custom tickers for a specific call
    print("\nExample 2: Using custom tickers for a specific call")
    
    # Define custom tickers mapping for this specific call
    custom_tickers = {
        'SPX Index': 'sp500',
        'INDU Index': 'dow_jones',
        'CCMP Index': 'nasdaq',
        'RTY Index': 'russell_2000',
        'DAX Index': 'dax',
        'UKX Index': 'ftse_100',
        'NKY Index': 'nikkei_225'
    }
    
    # Get Bloomberg market data using custom tickers
    df = data_service.get_bloomberg_data(
        category='equity',
        data_type='market',
        custom_tickers=custom_tickers,
        save_to_db=False
    )
    print(f"Retrieved {len(df)} rows using custom tickers")
    print("Unique codes in result:")
    print(df['code'].unique())
    
    # Example 3: Using custom fields
    print("\nExample 3: Using custom fields")
    
    # Define custom fields mapping
    custom_fields = {
        'PX_LAST': 'price',
        'PX_VOLUME': 'volume',
        'PX_HIGH': 'high',
        'PX_LOW': 'low'
    }
    
    # Get Bloomberg market data with custom fields
    df = data_service.get_bloomberg_data(
        category='equity',
        data_type='market',
        custom_tickers=custom_tickers,
        custom_fields=custom_fields,
        save_to_db=False
    )
    print(f"Retrieved {len(df)} rows using custom fields")
    print("Unique fields in result:")
    print(df['field'].unique())
    
    # Example 4: Creating a reusable tickers mapping
    print("\nExample 4: Creating a reusable tickers mapping")
    
    # Create a properly formatted tickers mapping
    tickers_mapping = FinancialDataService.create_tickers_mapping(
        tickers_dict=custom_tickers,
        category='equity'
    )
    
    # Create a new FinancialDataService instance with the custom mapping
    custom_service = FinancialDataService(
        start_date='2023-01-01',
        bloomberg_tickers_mapping=tickers_mapping
    )
    
    # Now we can use the custom mapping without specifying it each time
    df = custom_service.get_bloomberg_data(
        category='equity',
        data_type='market',
        save_to_db=False
    )
    print(f"Retrieved {len(df)} rows using reusable tickers mapping")
    print("Unique codes in result:")
    print(df['code'].unique())
    
    # Example 5: Saving to a custom table
    print("\nExample 5: Saving to a custom table")
    
    # Get data and save to a custom table
    df = data_service.get_bloomberg_data(
        category='equity',
        data_type='market',
        custom_tickers=custom_tickers,
        save_to_db=True,
        table_name='custom_equity_data'  # Custom table name
    )
    print(f"Saved {len(df)} rows to custom table 'custom_equity_data'")

if __name__ == "__main__":
    main() 