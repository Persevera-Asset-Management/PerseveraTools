# PerseveraTools

Internal tools package for Persevera Asset Management.

## Installation

Install directly from GitHub:
```
pip install git+https://github.com/Persevera-Asset-Management/PerseveraTools.git
```

## Configuration

1. Create a .persevera directory in your home folder:

```
mkdir %USERPROFILE%\.persevera  # Windows
mkdir ~/.persevera              # Mac/Linux
```

2. Create a .env file in the .persevera directory with the following structure:

```
# Paths
PERSEVERA_DATA_PATH=
PERSEVERA_AUTOMATION_PATH=

# Database Configuration
PERSEVERA_DB_USER=
PERSEVERA_DB_PASSWORD=
PERSEVERA_DB_HOST=
PERSEVERA_DB_PORT=
PERSEVERA_DB_NAME=
```

## Usage Examples

### Database Operations

The package provides utilities for reading from and writing to databases:

```python
from persevera_tools.db import read_sql, to_sql

# Read from database
df = read_sql("SELECT * FROM your_table")

# Write to database with update capability
to_sql(df, "your_table", primary_keys=["id"], update=True)
```

### Market Data

The package offers two main functions for retrieving market data:

#### get_series()
Retrieves time series data from the `indicadores` table:

```python
from persevera_tools.data import get_series

# Get market data for a single asset
ibov = get_series('br_cdi_index')

# Get market data for multiple assets with parameters
stocks = get_series(['br_ibovespa', 'us_sp500'],
                   start_date='2024-01-01',
                   field='close')
```

Parameters:
- `code`: Single indicator code or list of codes
- `start_date`: Optional start date (YYYY-MM-DD format)
- `end_date`: Optional end date (YYYY-MM-DD format)
- `field`: Field to retrieve (defaults to 'close')

#### get_descriptors()
Retrieves factor data from the `factor_zoo` table:

```python
from persevera_tools.data import get_descriptors

# Get company descriptors
pe_ratios = get_descriptors(['PETR4', 'VALE3'], 
                          descriptors='price_to_earnings_fwd',
                          start_date='2024-01-01')

# Get multiple descriptors for multiple companies
factors = get_descriptors(['PETR4', 'VALE3'],
                        descriptors=['price_to_earnings_fwd', 'ev_ebitda'])
```

Parameters:
- `tickers`: Single ticker or list of tickers
- `descriptors`: Single descriptor or list of descriptors
- `start_date`: Optional start date (YYYY-MM-DD format)
- `end_date`: Optional end date (YYYY-MM-DD format)

Returns:
- Single ticker/descriptor: DataFrame with date index and values
- Multiple tickers/descriptors: DataFrame with MultiIndex columns (ticker, descriptor)
