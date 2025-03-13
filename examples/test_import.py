"""
Simple test script to verify that the circular import issue is resolved.
"""

print("Importing persevera_tools...")
import persevera_tools

print("Successfully imported persevera_tools!")

print("Testing logger...")
from persevera_tools.utils.logging import get_logger
logger = get_logger("test")
logger.info("Logger is working!")

print("Testing database functions...")
try:
    from persevera_tools.db import read_sql, to_sql
    print("Successfully imported database functions!")
except ImportError as e:
    print(f"Error importing database functions: {e}")

print("Testing utility functions...")
try:
    from persevera_tools.utils.dates import get_holidays, excel_to_datetime
    print("Successfully imported utility functions!")
except ImportError as e:
    print(f"Error importing utility functions: {e}")

print("All tests completed!") 