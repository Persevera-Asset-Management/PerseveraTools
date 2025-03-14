"""
Simple logging utilities for PerseveraTools.
"""

import logging
import logging.handlers
import os
import sys
import time
import functools
from pathlib import Path
from typing import Optional, Callable, TypeVar, cast

# Type variables for decorators
F = TypeVar('F', bound=Callable)

def get_logger(name: str, level: Optional[int] = None) -> logging.Logger:
    """
    Get a logger with the specified name and level.
    
    Args:
        name: The name of the logger, typically using dot notation (e.g., 'persevera_tools.db')
        level: The logging level (e.g., logging.INFO, logging.DEBUG)
        
    Returns:
        A configured logger
    """
    logger = logging.getLogger(name)
    
    if level is not None:
        logger.setLevel(level)
        
    return logger

def configure_logger(
    level: int = logging.INFO,
    log_file: Optional[str] = None,
    console: bool = True,
    format_str: str = '%(asctime)s.%(msecs)03d %(name)s: %(message)s',
    date_format: str = '%Y-%m-%d %H:%M:%S'
) -> None:
    """
    Configure the root logger for the package.
    
    Args:
        level: The logging level (e.g., logging.INFO, logging.DEBUG)
        log_file: Optional path to a log file
        console: Whether to log to the console
        format_str: The format string for log messages
        date_format: The date format for log messages
    """
    # Get the root logger for the package
    root_logger = logging.getLogger('persevera_tools')
    root_logger.setLevel(level)
    
    # Remove any existing handlers
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)
    
    # Create formatter
    formatter = logging.Formatter(format_str, datefmt=date_format)
    
    # Add console handler if requested
    if console:
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(formatter)
        root_logger.addHandler(console_handler)
    
    # Add file handler if a log file is specified
    if log_file:
        # Create directory if it doesn't exist
        log_path = Path(log_file)
        log_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Use rotating file handler
        file_handler = logging.handlers.RotatingFileHandler(
            log_file, maxBytes=10*1024*1024, backupCount=5
        )
        file_handler.setFormatter(formatter)
        root_logger.addHandler(file_handler)

def set_log_level(level: int) -> None:
    """
    Set the log level for the package.
    
    Args:
        level: The logging level (e.g., logging.INFO, logging.DEBUG)
    """
    logging.getLogger('persevera_tools').setLevel(level)

def timed(func: F) -> F:
    """
    Decorator to time a function and log its execution time.
    
    Args:
        func: Function to decorate
    
    Returns:
        Decorated function with timing
    """
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        logger = get_logger(func.__module__)
        start_time = time.time()
        result = func(*args, **kwargs)
        elapsed = time.time() - start_time
        logger.info(f"{func.__name__} completed in {elapsed:.3f}s")
        return result
    
    return cast(F, wrapper)

def initialize():
    """Initialize logging based on environment variables."""
    # Get log level from environment or use default
    log_level_name = os.environ.get('PERSEVERA_LOG_LEVEL', 'INFO')
    log_level = getattr(logging, log_level_name.upper(), logging.INFO)
    
    # Determine log file path
    log_file = os.environ.get('PERSEVERA_LOG_FILE')
    if not log_file and os.environ.get('PERSEVERA_LOG_FILE_ENABLED', 'true').lower() in ('true', '1', 'yes'):
        log_dir = os.environ.get('PERSEVERA_LOG_DIR')
        if not log_dir:
            # Default log directory
            log_dir = Path.home() / '.persevera' / 'logs'
        else:
            log_dir = Path(log_dir)
        
        log_dir = Path(log_dir)
        log_dir.mkdir(parents=True, exist_ok=True)
        
        # Create a log file with timestamp
        from datetime import datetime
        timestamp = datetime.now().strftime('%Y%m%d')
        log_file = log_dir / f"persevera_{timestamp}.log"
    
    # Configure logging
    configure_logger(
        level=log_level,
        log_file=str(log_file) if log_file else None,
        console=True
    ) 