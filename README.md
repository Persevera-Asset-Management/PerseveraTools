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
```python
from persevera_tools.db import read_sql, to_sql

# Read from database
df = read_sql("SELECT * FROM your_table")

# Write to database
to_sql(df, "your_table", primary_keys=["id"], update=True)
```

### Market Data
```python
from persevera_tools.data import get_series, get_descriptors

# Get market data for a single asset
ibov = get_series('br_cdi_index')

# Get market data for multiple assets
stocks = get_series(['br_ibovespa', 'us_sp500'],
                   start_date='2024-01-01',
                   field='close')

# Get company descriptors
pe_ratios = get_descriptors(tickers=['PETR4', 'VALE3'], 
                          descriptors='price_to_earnings_fwd',
                          start_date='2024-01-01')
```
