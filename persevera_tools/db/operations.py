import logging
import pandas as pd
import numpy as np
import time
import psycopg2
import psycopg2.extras
import sqlalchemy
from sqlalchemy.exc import SQLAlchemyError
from typing import List, Optional, Dict, Any
from psycopg2.errors import UniqueViolation
from psycopg2 import sql

from ..config import settings
from .connection import get_db_engine
from ..utils.logging import get_logger, timed

# Get a logger for this module
logger = get_logger(__name__)

@timed
def to_sql(data: pd.DataFrame,
               table_name: str,
               primary_keys: list,
               update: bool,
               batch_size: int = 5000):
    """Upload data to SQL table with batch processing and conflict handling."""
    logger.info(f"Uploading {len(data)} rows to table '{table_name}'")
    
    if len(data) == 0:
        logger.warning("No data to upload")
        return
    
    # Get database connection and engine
    engine = get_db_engine()
    conn = engine.raw_connection()
    cursor = conn.cursor()
    
    try:
        # Create table if it doesn't exist
        logger.debug(f"Ensuring table '{table_name}' exists")
        data.head(0).to_sql(table_name, engine, if_exists='append', index=False)
        
        # Prepare data for insertion
        data_tuples = [tuple(x) for x in data.to_numpy()]
        cols = ','.join(list(data.columns))
        
        # Create SQL query
        query = f"INSERT INTO {table_name} ({cols}) VALUES %s"
        
        if update:
            # Add ON CONFLICT clause for upsert
            update_cols = [col for col in data.columns if col not in primary_keys]
            if not update_cols:
                logger.warning("No columns to update (all columns are primary keys)")
                return
                
            update_stmt = ', '.join([f"{col} = EXCLUDED.{col}" for col in update_cols])
            query += f" ON CONFLICT ({', '.join(primary_keys)}) DO UPDATE SET {update_stmt}"
        else:
            # Add ON CONFLICT DO NOTHING clause
            query += f" ON CONFLICT ({', '.join(primary_keys)}) DO NOTHING"
        
        # Process in batches
        total_batches = (len(data_tuples) + batch_size - 1) // batch_size
        logger.info(f"Processing {total_batches} batches of size {batch_size}")
        
        start_time = time.time()
        for i in range(0, len(data_tuples), batch_size):
            batch = data_tuples[i:i+batch_size]
            batch_num = i // batch_size + 1
            
            batch_start = time.time()
            psycopg2.extras.execute_values(cursor, query, batch)
            conn.commit()
            batch_time = time.time() - batch_start
            
            logger.debug(f"Batch {batch_num}/{total_batches} completed in {batch_time:.2f}s")
            
            # Estimate time remaining
            elapsed = time.time() - start_time
            avg_time_per_batch = elapsed / batch_num
            remaining_batches = total_batches - batch_num
            estimated_time_left = avg_time_per_batch * remaining_batches
            
            if estimated_time_left > 60:
                estimated_time_left_minutes = estimated_time_left / 60
                logger.info(f"Estimated time remaining: {estimated_time_left_minutes:.2f} minutes")
            else:
                logger.info(f"Estimated time remaining: {estimated_time_left:.2f} seconds")

        total_duration = time.time() - start_time
        logger.info(f"All data uploaded successfully in {total_duration:.2f} seconds")
    except (psycopg2.Error, SQLAlchemyError) as e:
        logger.error(f"Database error: {e}", exc_info=True)
        raise
    except UniqueViolation as uv:
        logger.info("UniqueViolation")
    finally:
        cursor.close()
        conn.close()
        engine.dispose()

@timed
def read_sql(sql_query: str, params: Optional[Dict[str, Any]] = None, date_columns: Optional[List[str]] = None) -> pd.DataFrame:
    """Read data from SQL table based on the provided query."""
    # Extract table name from query for logging
    table_name = "unknown"
    try:
        # Simple extraction of table name from query
        if "FROM" in sql_query.upper():
            parts = sql_query.upper().split("FROM")[1].strip().split()
            if parts:
                table_name = parts[0].strip().rstrip(';')
    except Exception:
        pass  # If we can't extract the table name, just use "unknown"
    
    logger.info(f"Reading from table '{table_name}'")
    
    engine = get_db_engine()
    try:
        with engine.connect() as connection:
            start_time = time.time()
            df = pd.read_sql_query(
                sqlalchemy.text(sql_query),
                con=connection,
                params=params,
                parse_dates=date_columns
            )
            duration = time.time() - start_time
            
            logger.info(f"Query returned {len(df)} rows in {duration:.2f} seconds")
            return df
    except Exception as e:
        logger.error(f"Error executing SQL query: {e}", exc_info=True)
        return pd.DataFrame()
    finally:
        engine.dispose()
        