DEFAULT_CONFIG = {
    "LOG_CONFIG": {
        "log_format": "%(asctime)s.%(msecs)03d [%(levelname)s] %(name)s: %(message)s",
        "log_datefmt": "%Y-%m-%d %H:%M:%S",
        "default_level": "INFO",
    },
    "PATHS": {
        "DATA_PATH": None,
        "AUTOMATION_PATH": None,  # Will be set from environment
    },
    "DB": {
        "port": "5432",
        "database": None,
    }
}