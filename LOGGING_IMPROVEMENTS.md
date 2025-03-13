# Logging System Improvements

This document outlines the improvements made to the logging system in the PerseveraTools project.

## Overview of Changes

1. **Simple Logging Module**
   - Added `persevera_tools/utils/logging.py` with straightforward logging utilities
   - Implemented clean API for getting loggers and configuring logging
   - Added support for both programmatic and environment-based configuration

2. **Performance Tracking**
   - Added `@timed` decorator for tracking function execution time
   - Implemented automatic timing of database operations

3. **Improved Logging Format**
   - Enhanced default log format with level and logger name
   - Added millisecond precision to timestamps
   - Standardized log message format

4. **File Logging**
   - Added rotating file handler with size limits
   - Implemented automatic log directory creation
   - Added daily log file rotation with timestamps

5. **Documentation**
   - Updated README.md with logging documentation
   - Added example usage in `examples/logging_example.py`

## Benefits

1. **Improved Debugging**
   - More detailed logs with consistent formatting
   - Better exception handling with stack traces
   - Performance metrics for identifying bottlenecks

2. **Better Developer Experience**
   - Simple API for logging
   - Consistent logging interface across the application
   - Reduced boilerplate code for logging

3. **Operational Insights**
   - Performance tracking for critical operations
   - Progress reporting for long-running tasks

## Usage Examples

### Basic Logging

```python
from persevera_tools.utils.logging import get_logger

logger = get_logger(__name__)
logger.info("This is an informational message")
logger.error("An error occurred", exc_info=True)
```

### Configuration

```python
import logging
from persevera_tools.utils.logging import configure_logger

configure_logger(
    level=logging.DEBUG,
    log_file="/path/to/your/log/file.log",
    console=True
)
```

### Performance Tracking

```python
from persevera_tools.utils.logging import timed

@timed
def expensive_operation():
    # Function implementation
    pass
```

## Environment Variables

Logging can be configured through environment variables:

```
PERSEVERA_LOG_LEVEL=INFO
PERSEVERA_LOG_FILE=/path/to/log/file.log
PERSEVERA_LOG_DIR=/path/to/log/directory
```

Or through the `.env` file in the `.persevera` directory. 