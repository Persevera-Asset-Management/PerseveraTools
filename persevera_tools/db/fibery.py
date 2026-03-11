import time
import requests
import logging
import pandas as pd
from typing import List, Dict, Any, Optional, Tuple, Callable

from ..config import settings
from ..utils.logging import get_logger
from persevera_tools.config import settings
from persevera_tools.utils.logging import get_logger

import warnings
warnings.filterwarnings("ignore", message="Could not infer format, so each element will be parsed individually, falling back to `dateutil`", category=UserWarning)


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

def _get_full_schema() -> Optional[Dict[str, Any]]:
    """
    Retrieves the full Fibery database schema with field type information.
    Returns the raw schema data.
    """
    logger.info("Fetching full Fibery database schema...")
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

        return schema_data[0]["result"]

    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching Fibery schema: {e}", exc_info=True)
        return None

def _get_db_schema() -> Optional[Dict[str, Any]]:
    """
    Retrieves the entire Fibery database schema and organizes it for easy access.
    Returns a dictionary mapping display names to their canonical names, fields, and field metadata.
    """
    full_schema = _get_full_schema()
    if not full_schema:
        return None
        
    db_schema = {}

    for T in full_schema.get("fibery/types", []):
        display_name = T.get("ui/name", T["fibery/name"])
        canonical_name = T['fibery/name']

        fields = {}
        for field in T.get("fibery/fields", []):
            field_name = field["fibery/name"]
            
            # Skip collections and deleted fields
            if field.get("fibery/collection?") or "_deleted" in field_name:
                continue
            
            # Determine field type
            field_meta = field.get("fibery/meta", {})
            field_type = field.get("fibery/type")
            
            is_relation = "fibery/relation" in field_meta
            # Consider Fibery workflow state fields as enums as well
            field_type_str = (field_type or "")
            is_workflow_state = "workflow/state" in field_type_str.lower()
            is_type_component = bool(field_meta.get("fibery/type-component?", False))
            is_enum = bool(field_type) and (
                "enum" in field_type_str.lower() or is_workflow_state or is_type_component
            )
            
            fields[field_name] = {
                'is_relation': is_relation,
                'is_enum': is_enum,
                'type': field_type,
                'meta': field_meta
            }
        
        # Add system fields
        fields["fibery/id"] = {'is_relation': False, 'is_enum': False, 'type': 'uuid', 'meta': {}}
        fields["fibery/public-id"] = {'is_relation': False, 'is_enum': False, 'type': 'text', 'meta': {}}
        
        db_schema[display_name] = {
            'canonical_name': canonical_name,
            'fields': fields
        }
        
    logger.info("Successfully fetched and processed database schema with field metadata.")
    return db_schema

def _build_field_selection(fields_dict: Dict[str, Dict], table_name: str) -> Dict[str, Any]:
    """
    Builds the q/select dictionary for Fibery query.
    Handles primitive fields and relational fields appropriately.
    
    Args:
        fields_dict: Dictionary with field names as keys and metadata as values
        
    Returns:
        Dictionary suitable for q/select in Fibery query
    """
    selection = {}
    
    for field_name, field_info in fields_dict.items():
        # Create a clean alias (remove space prefix)
        alias = field_name.split('/')[-1]
        field_type_str = (field_info.get('type') or '')

        # Treat On-Off component types as enums, regardless of relation detection
        if 'on-off' in field_type_str.lower():
            selection[alias] = [field_name, 'enum/name']
            continue

        primitive_types = [
            'uuid',
            'text',
            'fibery/text',
            'fibery/decimal',
            'fibery/date-time',
            'fibery/date',
            'fibery/bool',
            'fibery/int',
        ]
        unsupported_types = [
            'Collaboration~Documents/Document'
        ]
        
        if field_info['is_enum']:
            # For enum fields, get the enum/name
            selection[alias] = [field_name, 'enum/name']
        elif field_info['is_relation']:
            # For relation fields, try to get the Name field of the related entity
            # Get the related type from meta
            related_table_name = field_info['type']

            if not field_info.get('meta', {}).get('fibery/collection?', {}):
                space_name = field_info.get('type')
                # selection[alias] = [field_name, f'{space_name}/Name']

                # Workflow state fields behave like enums
                if isinstance(related_table_name, str) and 'workflow/state' in related_table_name.lower():
                    selection[alias] = [field_name, 'enum/name']
                # "On-Off" component relations also behave like enums
                elif isinstance(related_table_name, str) and 'on-off' in related_table_name.lower():
                    selection[alias] = [field_name, 'enum/name']
                elif field_info.get('meta', {}).get('fibery/type-component?', {}):
                    selection[alias] = [field_name, 'enum/name']
                else:
                    selection[alias] = [field_name, f'{related_table_name.split("/")[0]}/Name']

        else:
            field_type_value = field_info.get('type')
            # Skip unsupported heavy/non-primitive types (e.g. Documents)
            if field_type_value in unsupported_types:
                continue
            # Directly fetch primitive types
            if field_type_value in primitive_types or not field_type_value:
                selection[alias] = [field_name]
            else:
                selection[alias] = [field_name, f'{field_type_value.split("/")[0]}/Name']
    
    return selection

def _execute_fibery_page(
    api_url: str,
    headers: Dict[str, str],
    canonical_name: str,
    field_selection: Dict[str, Any],
    where_filter: Optional[List[Any]],
    params: Optional[Dict[str, Any]],
    offset: int,
    page_size: int,
    max_field_retries: int = 3,
    max_timeout_retries: int = 3,
) -> Tuple[Optional[List[Any]], Dict[str, Any]]:
    """
    Executes a single paginated Fibery query with retry logic for both field errors
    and API timeouts.

    On field errors, attempts to correct the field selection in up to max_field_retries
    attempts (enum/name → Space/Name → remove field).

    On timeout errors, retries with exponential backoff (2s, 4s, 8s, ...).

    Returns:
        A tuple of (entities, corrected_field_selection), where entities is None on
        unrecoverable error.
    """
    field_retries = 0
    timeout_retries = 0

    while field_retries < max_field_retries:
        query: Dict[str, Any] = {
            "q/from": canonical_name,
            "q/select": field_selection,
            "q/limit": page_size,
            "q/offset": offset,
        }

        if where_filter is not None:
            query["q/where"] = where_filter

        args: Dict[str, Any] = {"query": query}
        if params is not None:
            args["params"] = params

        payload = [{"command": "fibery.entity/query", "args": args}]

        try:
            response = requests.post(api_url, headers=headers, json=payload)
            response.raise_for_status()
            data = response.json()

            if not data or not data[0].get("success"):
                error_info = data[0].get("result", {})
                error_name = error_info.get("name", "")
                error_message = error_info.get("message", "Unknown error")

                # --- Timeout: retry with exponential backoff ---
                if "timeout" in error_message.lower():
                    if timeout_retries < max_timeout_retries:
                        wait = 2 ** timeout_retries
                        logger.warning(
                            f"Timeout at offset {offset}. Retrying in {wait}s "
                            f"({timeout_retries + 1}/{max_timeout_retries})..."
                        )
                        time.sleep(wait)
                        timeout_retries += 1
                        continue
                    else:
                        logger.error(f"Timeout persisted after {max_timeout_retries} retries at offset {offset}.")
                        return None, field_selection

                # --- Field type error: fix and retry ---
                if error_name == "entity.error/query-primitive-field-expr-invalid":
                    error_data = error_info.get("data", {})
                    problematic_field = error_data.get("field", [None])[0]

                    if problematic_field:
                        logger.warning(f"Field '{problematic_field}' is not primitive. Attempting to fix...")

                        alias_to_fix = next(
                            (alias for alias, spec in field_selection.items()
                             if isinstance(spec, list) and spec[0] == problematic_field),
                            None,
                        )

                        if alias_to_fix:
                            current_spec = field_selection[alias_to_fix]

                            if len(current_spec) == 1:
                                field_selection[alias_to_fix] = [problematic_field, "enum/name"]
                                logger.info(f"Trying enum/name for '{problematic_field}'")
                            elif len(current_spec) == 2 and current_spec[1] == "enum/name":
                                space_name = problematic_field.split("/")[0]
                                field_selection[alias_to_fix] = [problematic_field, f"{space_name}/Name"]
                                logger.info(f"Trying Space/Name for '{problematic_field}'")
                            else:
                                logger.warning(f"Could not fix '{problematic_field}'. Removing from query.")
                                del field_selection[alias_to_fix]

                            field_retries += 1
                            timeout_retries = 0  # reset timeout counter after a field fix
                            continue
                        else:
                            logger.error(f"Could not find alias for problematic field: {problematic_field}")
                            return None, field_selection

                logger.error(f"Fibery API error: {error_message}")
                logger.debug(f"Error details: {error_info}")
                return None, field_selection

            # Success
            return data[0]["result"], field_selection

        except requests.exceptions.RequestException as e:
            logger.error(f"Request error at offset {offset}: {e}", exc_info=True)
            return None, field_selection

    logger.error(f"Failed to fix field selection after {max_field_retries} retries.")
    return None, field_selection


def read_fibery(
    table_name: str,
    include_fibery_fields: bool = False,
    where_filter: Optional[List[Any]] = None,
    params: Optional[Dict[str, Any]] = None,
    page_size: int = 1000,
) -> pd.DataFrame:
    """
    Reads all data from a Fibery table and returns it as a pandas DataFrame.
    Automatically handles relational fields, enums, pagination, and timeouts.

    Args:
        table_name: The display name of the Fibery table to read.
        include_fibery_fields: Whether to include Fibery system fields (created-by, rank, etc.).
        where_filter: Optional filter condition using Fibery's q/where syntax.
            Example: [">=", ["fibery/creation-date"], "$cutoffDate"]
        params: Optional dictionary of parameter values for the where_filter.
            Example: {"$cutoffDate": "2026-01-23T00:00:00Z"}
        page_size: Number of records per page. Reduce for heavy tables, increase for light ones.
            Default is 1000.

    Returns:
        A pandas DataFrame with the table data.

    Example:
        df = read_fibery(
            "Inv-Asset Allocation/Posição",
            where_filter=[">=", ["Inv-Asset Allocation/Data Posição"], "$dataRecente"],
            params={"$dataRecente": "2026-01-01T00:00:00Z"},
            page_size=500,
        )
    """
    db_schema = _get_db_schema()
    if not db_schema:
        return pd.DataFrame()

    table_meta = db_schema.get(table_name)
    if not table_meta:
        logger.error(f"Table '{table_name}' not found in the processed schema.")
        return pd.DataFrame()

    canonical_name = table_meta["canonical_name"]
    all_fields = table_meta["fields"]

    str_to_remove = ["_deleted", "Collaboration", "Description", "created-by", "comments/comments"]
    if not include_fibery_fields:
        str_to_remove.extend(["fibery/created-by", "fibery/rank"])

    fields_to_query = {
        field_name: field_info
        for field_name, field_info in all_fields.items()
        if not any(s in field_name for s in str_to_remove)
    }

    logger.info(f"Reading data from Fibery table: {canonical_name} (page_size={page_size})")

    api_url = _get_fibery_api_url("commands")
    headers = _get_fibery_headers()
    field_selection = _build_field_selection(fields_to_query, canonical_name)

    all_entities: List[Any] = []
    offset = 0

    while True:
        logger.debug(f"Fetching page at offset {offset}...")
        page_entities, field_selection = _execute_fibery_page(
            api_url=api_url,
            headers=headers,
            canonical_name=canonical_name,
            field_selection=field_selection,
            where_filter=where_filter,
            params=params,
            offset=offset,
            page_size=page_size,
        )

        if page_entities is None:
            if not all_entities:
                return pd.DataFrame()
            logger.warning("Stopping pagination due to an error. Returning partial data.")
            break

        all_entities.extend(page_entities)
        logger.info(f"Fetched {len(all_entities)} records so far...")

        if len(page_entities) < page_size:
            break

        offset += page_size

    if not all_entities:
        logger.warning(f"No entities found in {canonical_name}")
        return pd.DataFrame()

    df = pd.DataFrame(all_entities)

    for col in df.columns:
        if df[col].dtype in ["object", "string"] and df[col].notnull().any():
            try:
                df[col] = pd.to_datetime(df[col]).dt.tz_convert("America/Sao_Paulo")
                continue
            except (ValueError, TypeError):
                pass

            try:
                df[col] = pd.to_numeric(df[col])
                continue
            except ValueError:
                pass

            unique_vals = df[col].dropna().unique()
            if len(unique_vals) > 0 and all(v in ["true", "false"] for v in unique_vals):
                df[col] = df[col].map({"true": True, "false": False})

    df = df.convert_dtypes()

    logger.info(f"Successfully read {len(df)} entities from {canonical_name}")
    return df

def find_database_by_id(table_id: str) -> Optional[str]:
    """
    Retrieves the display name of a Fibery table given its ID.

    Args:
        table_id: The UUID of the Fibery table.

    Returns:
        The display name of the table, or None if not found.
    """
    full_schema = _get_full_schema()
    if not full_schema:
        return None

    for T in full_schema.get("fibery/types", []):
        if T.get("fibery/id") == table_id:
            return T.get("ui/name", T.get("fibery/name"))

    logger.warning(f"Table with ID '{table_id}' not found in the schema.")
    return None

def get_fibery_table_id_by_name(table_name: str) -> Optional[str]:
    """
    Retrieves the UUID of a Fibery table given its display name.

    Args:
        table_name: The display name of the Fibery table.

    Returns:
        The UUID of the table, or None if not found.
    """
    full_schema = _get_full_schema()
    if not full_schema:
        return None

    for T in full_schema.get("fibery/types", []):
        display_name = T.get("ui/name", T.get("fibery/name"))
        if display_name == table_name:
            return T.get("fibery/id")

    logger.warning(f"Table with name '{table_name}' not found in the schema.")
    return None