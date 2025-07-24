import requests
import logging
import pandas as pd
from typing import List, Dict, Any, Optional, Tuple, Callable

from ..config import settings
from ..utils.logging import get_logger
# from persevera_tools.config import settings
# from persevera_tools.utils.logging import get_logger

logger = get_logger(__name__)

def _get_fibery_headers() -> Dict[str, str]:
    """Returns the authorization headers for Fibery API."""
    api_token = settings.FIBERY_API_TOKEN
    if not api_token:
        raise ValueError("Fibery API token is not configured.")
    return {
        "Authorization": f"Token {api_token}",
        "Content-Type": "application/json"
    }

def _get_fibery_api_url(endpoint: str) -> str:
    """Constructs the full Fibery API URL for a given endpoint."""
    domain = settings.FIBERY_DOMAIN
    if not domain:
        raise ValueError("Fibery domain is not configured.")
    return f"https://{domain}.fibery.io/api/{endpoint}"

def _get_db_schema() -> Optional[Dict[str, Any]]:
    """
    Retrieves the entire Fibery database schema and organizes it for easy access.
    Returns a dictionary mapping display names to their canonical names and fields.
    """
    logger.info("Fetching Fibery database schema...")
    api_url = _get_fibery_api_url("commands")
    headers = _get_fibery_headers()
    payload = [{"command": "fibery.schema/query", "args": {}}]

    try:
        response = requests.post(api_url, headers=headers, json=payload)
        response.raise_for_status()
        schema_data = response.json()

        if not schema_data or not schema_data[0].get("success"):
            logger.error("Failed to retrieve Fibery schema.")
            return None

        schema = schema_data[0]["result"]
        db_schema = {}

        for T in schema.get("fibery/types", []):
            display_name = T.get("ui/name", T["fibery/name"])
            
            fields = []
            for field in T.get("fibery/fields", []):
                meta = field.get("fibery/meta", {})
                # Exclude collections and any relational fields (which have a 'fibery/relation' key in their meta).
                if not field.get("fibery/collection?") and "fibery/relation" not in meta:
                    fields.append(field["fibery/name"])

            fields.extend(["fibery/id", "fibery/public-id"])
            
            db_schema[display_name] = {
                'canonical_name': T['fibery/name'],
                'fields': fields
            }
            
        logger.info("Successfully fetched and processed database schema.")
        return db_schema

    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching Fibery schema: {e}", exc_info=True)
        return None

def read_fibery(table_name: str, include_fibery_fields: bool = False) -> pd.DataFrame:
    """
    Reads all data from a Fibery table and returns it as a pandas DataFrame.
    """
    db_schema = _get_db_schema()
    if not db_schema:
        return pd.DataFrame()

    table_meta = db_schema.get(table_name)
    if not table_meta:
        logger.error(f"Table '{table_name}' not found in the processed schema.")
        return pd.DataFrame()

    canonical_name = table_meta['canonical_name']
    fields_to_query = table_meta['fields']
    
    str_to_remove = ['_deleted', 'Collaboration', 'created-by']
    if not include_fibery_fields:
        str_to_remove.append('fibery/')

    fields_to_query = [field for field in fields_to_query if not any(s in field for s in str_to_remove)]

    logger.info(f"Reading all data from Fibery table: {canonical_name}")
    
    api_url = _get_fibery_api_url("commands")
    headers = _get_fibery_headers()
    start_token = None
    page_size = 3000

    while True:
        query = {
            "q/from": canonical_name,
            "q/select": fields_to_query,
            "q/limit": page_size
        }
        if start_token:
            query["start"] = start_token
            
        payload = [{"command": "fibery.entity/query", "args": {"query": query}}]

        try:
            response = requests.post(api_url, headers=headers, json=payload)
            response.raise_for_status()
            data = response.json()
            
            if not data or not data[0].get("success"):
                error_info = data[0].get('result', {})
                logger.error(f"Fibery API error: {error_info.get('message', 'Unknown error')}")
                logger.debug(f"Error details: {error_info}")
                break

            result = data[0]["result"]
            
            if not start_token:
                break

        except requests.exceptions.RequestException as e:
            logger.error(f"Request error while reading from Fibery: {e}", exc_info=True)
            break
            
    if not result:
        return pd.DataFrame()

    df = pd.DataFrame(result)

    # Automatic datatype inference and conversion
    for col in df.columns:
        if df[col].dtype == "object" and df[col].notnull().any():
            # Attempt to convert to datetime
            try:
                df[col] = pd.to_datetime(df[col], format="%Y-%m-%d")
                continue
            except (ValueError, TypeError):
                pass

            # Attempt to convert to numeric
            try:
                df[col] = pd.to_numeric(df[col])
                continue
            except ValueError:
                pass

            # Check for and map boolean-like strings
            unique_vals = df[col].dropna().unique()
            if all(v in ['true', 'false'] for v in unique_vals):
                df[col] = df[col].map({'true': True, 'false': False})

    # Convert columns to best possible dtypes that support pd.NA
    df = df.convert_dtypes()

    logger.info(f"Successfully read {len(df)} entities from {canonical_name}")
    return df
