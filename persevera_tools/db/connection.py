import os
import sqlalchemy
from sqlalchemy.engine import Engine

def get_db_config():
    return {
        'username': os.getenv('PERSEVERA_DB_USER', 'default_user'),
        'password': os.getenv('PERSEVERA_DB_PASSWORD'),
        'host': os.getenv('PERSEVERA_DB_HOST'),
        'port': os.getenv('PERSEVERA_DB_PORT', '5432'),
        'database': os.getenv('PERSEVERA_DB_NAME')
    }

def get_db_engine() -> Engine:
    """Create and return a SQLAlchemy engine for database connection."""
    config = get_db_config()
    conn_url = f'postgresql+psycopg2://{config["username"]}:{config["password"]}@{config["host"]}:{config["port"]}/{config["database"]}'
    return sqlalchemy.create_engine(conn_url)