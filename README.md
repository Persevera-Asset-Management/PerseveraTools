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

# API Keys
PERSEVERA_FRED_API_KEY=

# Logging Configuration (Optional)
PERSEVERA_LOG_LEVEL=INFO
PERSEVERA_LOG_FILE=
PERSEVERA_LOG_DIR=
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

The package offers three main functions for retrieving market data:

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

#### get_index_composition()
Retrieves index composition data from the `b3_index_composition` table:

```python
from persevera_tools.data import get_index_composition

# Get composition for a single index
ibov_comp = get_index_composition('IBOV')

# Get composition for multiple indices
indices_comp = get_index_composition(['IBOV', 'IBX100'],
                                    start_date='2024-01-01')
```

Parameters:
- `index_code`: Single index code or list of index codes (e.g., 'IBOV', 'IBX100')
- `start_date`: Optional start date (YYYY-MM-DD format)
- `end_date`: Optional end date (YYYY-MM-DD format)

Returns:
- Single index: DataFrame with date index and ticker columns
- Multiple indices: DataFrame with MultiIndex columns (index_code, ticker)

## Logging

PerseveraTools includes a simple logging system:

### Basic Usage

```python
from persevera_tools.utils.logging import get_logger

# Get a logger for your module
logger = get_logger(__name__)

# Log at different levels
logger.debug("Detailed information for debugging")
logger.info("General information about program execution")
logger.warning("Warning about potential issues")
logger.error("Error that doesn't prevent execution")
logger.exception("Log an exception with traceback", exc_info=True)
```

### Configuration

You can configure the logging system programmatically:

```python
import logging
from persevera_tools.utils.logging import configure_logger, set_log_level

# Configure logging with custom settings
configure_logger(
    level=logging.DEBUG,
    log_file="/path/to/your/log/file.log",
    console=True,
    format_str='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

# Change log level later in your code
set_log_level(logging.INFO)
```

Or through environment variables:

- `PERSEVERA_LOG_LEVEL`: Set the log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
- `PERSEVERA_LOG_FILE`: Specify a custom log file path
- `PERSEVERA_LOG_DIR`: Specify a custom directory for log files

### Performance Tracking

```python
from persevera_tools.utils.logging import timed

# Use the timed decorator to log function execution time
@timed
def process_data(data):
    # Function implementation
    pass
```

See the example in `examples/logging_example.py` for more details.
