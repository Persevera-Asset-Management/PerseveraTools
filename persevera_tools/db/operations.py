import logging
import pandas as pd
import numpy as np
import time
import psycopg2
import psycopg2.extras
import sqlalchemy
from sqlalchemy.exc import SQLAlchemyError
from typing import List, Optional
from psycopg2.errors import UniqueViolation
from psycopg2 import sql

from ..config import settings
from .connection import get_db_engine

logger = logging.getLogger(__name__)

def to_sql(data: pd.DataFrame,
               table_name: str,
               primary_keys: list,
               update: bool,
               batch_size: int = 5000):
    """Upload data to SQL table with batch processing and conflict handling."""
    global conn, cursor
    logger.info(f'Connecting to SQL table: {table_name}')
    engine = get_db_engine()

    # Remove duplicates based on primary keys
    data = data.drop_duplicates(subset=primary_keys, keep='last')
    data = data.replace(np.nan, None)

    non_primary_keys = list(set(data.columns) - set(primary_keys))
    primary_keys_str = str(tuple(primary_keys)).replace("'", "").replace(',)', ')')
    non_primary_keys_str = ', '.join([f"{i} = excluded.{i}" for i in non_primary_keys])

    try:
        conn = engine.raw_connection()
        cursor = conn.cursor()

        # Prepare data for insertion
        total_rows = len(data)
        num_batches = (total_rows + batch_size - 1) // batch_size
        logger.info(f'Inserting {total_rows} rows, divided into {num_batches} batches...')
        tuples = [tuple(row) for row in data.itertuples(index=False)]

        total_time = 0
        start_time = time.time()

        # Process data in smaller batches
        for i in range(0, total_rows, batch_size):
            batch_start_time = time.time()
            batch = tuples[i:i + batch_size]

            # Construct SQL query for bulk insertion
            columns = list(data.columns)

            if update:
                sql_query = sql.SQL(
                    "INSERT INTO {} ({}) VALUES %s ON CONFLICT " + primary_keys_str + " DO UPDATE SET " + non_primary_keys_str
                ).format(sql.Identifier(table_name), sql.SQL(', ').join(map(sql.Identifier, columns)))
            else:
                sql_query = sql.SQL(
                    "INSERT INTO {} ({}) VALUES %s ON CONFLICT " + primary_keys_str + " DO NOTHING"
                ).format(sql.Identifier(table_name), sql.SQL(', ').join(map(sql.Identifier, columns)))

            psycopg2.extras.execute_values(cursor, sql_query, batch)
            conn.commit()

            batch_time = time.time() - batch_start_time
            total_time += batch_time
            logger.info(f"Batch {i // batch_size + 1}/{num_batches} uploaded successfully in {batch_time:.2f} seconds")

            # Estimate remaining time
            batches_done = (i // batch_size) + 1
            avg_batch_time = total_time / batches_done
            batches_left = num_batches - batches_done
            estimated_time_left = avg_batch_time * batches_left

            if estimated_time_left > 60:
                estimated_time_left_minutes = estimated_time_left / 60
                logger.info(f"Estimated time remaining: {estimated_time_left_minutes:.2f} minutes")
            else:
                logger.info(f"Estimated time remaining: {estimated_time_left:.2f} seconds")

        total_duration = time.time() - start_time
        logger.info(f"All data uploaded successfully in {total_duration:.2f} seconds")
    except (psycopg2.Error, SQLAlchemyError) as e:
        logger.error(f"Error occurred: {e}")
    except UniqueViolation as uv:
        logger.info("UniqueViolation")
    finally:
        cursor.close()
        conn.close()
        engine.dispose()

def read_sql(sql_query: str, date_columns: Optional[List[str]] = None) -> pd.DataFrame:
    """Read data from SQL table based on the provided query."""
    engine = get_db_engine()
    try:
        logger.info(f"Reading table {sql_query.split('FROM')[1].split('WHERE')[0].strip()}...")
        with engine.connect() as connection:
            df = pd.read_sql_query(
                sqlalchemy.text(sql_query),
                con=connection,
                parse_dates=date_columns
            )
        logger.info("Data read successfully.")
        return df
    except Exception as e:
        logger.error(f"An error occurred while reading from database: {e}")
        return pd.DataFrame()
    finally:
        engine.dispose()
        