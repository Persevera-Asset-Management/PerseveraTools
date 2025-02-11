import os
from pathlib import Path
from typing import Dict, Any
from dotenv import load_dotenv
from .defaults import DEFAULT_CONFIG

class Settings:
    def __init__(self):
        # Load environment variables from .env file if it exists
        env_path = Path.home() / '.persevera' / '.env'
        load_dotenv(env_path)
        
        self._load_config()

    def _load_config(self):
        # Load paths
        self.DATA_PATH = os.getenv('PERSEVERA_DATA_PATH')
        self.AUTOMATION_PATH = os.getenv('PERSEVERA_AUTOMATION_PATH')

        # Load database configuration
        self.DB_CONFIG = {
            'username': os.getenv('PERSEVERA_DB_USER'),
            'password': os.getenv('PERSEVERA_DB_PASSWORD'),
            'host': os.getenv('PERSEVERA_DB_HOST'),
            'port': os.getenv('PERSEVERA_DB_PORT', DEFAULT_CONFIG['DB']['port']),
            'database': os.getenv('PERSEVERA_DB_NAME', DEFAULT_CONFIG['DB']['database'])
        }

        # Load logging configuration
        self.LOG_CONFIG = DEFAULT_CONFIG['LOG_CONFIG']

    def validate(self) -> bool:
        """Validate that all required configuration is present"""
        required_vars = [
            'DATA_PATH',
            'AUTOMATION_PATH',
            'DB_CONFIG.username',
            'DB_CONFIG.password',
            'DB_CONFIG.host'
        ]
        
        for var in required_vars:
            if '.' in var:
                obj, attr = var.split('.')
                if not getattr(self, obj).get(attr):
                    raise ValueError(f"Missing required configuration: {var}")
            elif not getattr(self, var):
                raise ValueError(f"Missing required configuration: {var}")
        
        return True

    def get_db_url(self) -> str:
        """Get database URL while protecting sensitive information"""
        config = self.DB_CONFIG
        return f"postgresql+psycopg2://{config['username']}:{config['password']}@{config['host']}:{config['port']}/{config['database']}"

# Create a global settings instance
settings = Settings()