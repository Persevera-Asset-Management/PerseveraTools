"""
Example script demonstrating the use of the logging system in PerseveraTools.
"""

import logging
import time
from persevera_tools.utils.logging import get_logger, timed, configure_logger, set_log_level

# Configure logging for this example
configure_logger(level=logging.DEBUG)

# Get a logger for this module
logger = get_logger(__name__)

# Example function with timing decorator
@timed
def slow_operation(iterations):
    """Example function that demonstrates the timing decorator."""
    result = 0
    for i in range(iterations):
        result += i
        time.sleep(0.01)  # Simulate slow operation
    return result

def process_data(data_id):
    """Example function that demonstrates logging."""
    logger.info(f"Processing data ID: {data_id}")
    
    try:
        # Simulate processing steps
        logger.debug("Validating data")
        time.sleep(0.2)
        
        logger.debug("Transforming data")
        time.sleep(0.3)
        
        # Simulate a potential error condition
        if data_id % 3 == 0:
            raise ValueError(f"Invalid data ID: {data_id}")
        
        logger.debug("Storing data")
        time.sleep(0.2)
        
        logger.info("Data processing completed successfully")
        return True
    except Exception as e:
        logger.error(f"Error processing data: {str(e)}", exc_info=True)
        return False

def main():
    """Main function demonstrating various logging features."""
    # Basic logging
    logger.debug("This is a debug message")
    logger.info("This is an info message")
    logger.warning("This is a warning message")
    logger.error("This is an error message")
    
    # Demonstrate function with timing decorator
    logger.info("Calling function with timing decorator")
    result = slow_operation(100)
    logger.info(f"Slow operation result: {result}")
    
    # Demonstrate error logging
    logger.info("Demonstrating error logging")
    for i in range(1, 5):
        success = process_data(i)
        logger.info(f"Processing data {i}: {'Success' if success else 'Failed'}")
    
    # Demonstrate changing log level
    logger.info("Changing log level to INFO")
    set_log_level(logging.INFO)
    logger.debug("This debug message should NOT be visible")
    logger.info("This info message should be visible")

if __name__ == "__main__":
    main() 