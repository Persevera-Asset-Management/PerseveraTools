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

        # Load FRED API key
        self.FRED_API_KEY = os.getenv('PERSEVERA_FRED_API_KEY')

        # Load Comdinheiro credentials
        self.COMDINHEIRO_USERNAME = os.getenv('PERSEVERA_COMDINHEIRO_USERNAME')
        self.COMDINHEIRO_PASSWORD = os.getenv('PERSEVERA_COMDINHEIRO_PASSWORD')

        # Load Fibery credentials
        self.FIBERY_API_TOKEN = os.getenv('PERSEVERA_FIBERY_API_TOKEN')
        self.FIBERY_DOMAIN = os.getenv('PERSEVERA_FIBERY_DOMAIN')

        # Load Google Sheets credentials
        self.GS_CLIENT_ID = os.getenv('PERSEVERA_GOOGLESHEETS_CLIENT_ID')
        self.GS_CLIENT_SECRET = os.getenv('PERSEVERA_GOOGLESHEETS_CLIENT_SECRET')
        self.GS_PROJECT_ID = os.getenv('PERSEVERA_GOOGLESHEETS_PROJECT_ID')
        self.GS_API_KEY = os.getenv('PERSEVERA_GOOGLESHEETS_API_KEY')

        # Load logging configuration with environment variable overrides
        self.LOG_CONFIG = DEFAULT_CONFIG['LOG_CONFIG'].copy()

        # Override logging settings from environment variables if provided
        if os.getenv('PERSEVERA_LOG_FORMAT'):
            self.LOG_CONFIG['log_format'] = os.getenv('PERSEVERA_LOG_FORMAT')
        
        if os.getenv('PERSEVERA_LOG_DATE_FORMAT'):
            self.LOG_CONFIG['log_datefmt'] = os.getenv('PERSEVERA_LOG_DATE_FORMAT')
        
        if os.getenv('PERSEVERA_LOG_LEVEL'):
            self.LOG_CONFIG['default_level'] = os.getenv('PERSEVERA_LOG_LEVEL')

    def get_gs_client_secret(self):
        return {
            "installed":{
                "client_id":self.GS_CLIENT_ID,
                "project_id":self.GS_PROJECT_ID,
                "auth_uri":"https://accounts.google.com/o/oauth2/auth",
                "token_uri":"https://oauth2.googleapis.com/token",
                "auth_provider_x509_cert_url":"https://www.googleapis.com/oauth2/v1/certs",
                "client_secret":self.GS_CLIENT_SECRET,
                "redirect_uris":[
                    "http://localhost"
                ]
            }
        }

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