import sqlalchemy
from sqlalchemy.engine import Engine
from ..config import settings

def get_db_engine() -> Engine:
    """Create and return a SQLAlchemy engine for database connection."""
    return sqlalchemy.create_engine(settings.get_db_url())