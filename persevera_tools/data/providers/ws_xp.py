from __future__ import annotations

import time
from datetime import datetime
from typing import Any, Dict, List, Optional, Union

import requests
import pandas as pd
from dateutil.relativedelta import relativedelta

from .base import DataProvider, DataRetrievalError
from ...config import settings
from ...utils.logging import get_logger

logger = get_logger(__name__)

# Scope oficial na documentação XP (Assets Query external). Deve terminar em /.default
_XPWS_DEFAULT_SCOPE = "api://xpcorretora.onmicrosoft.com/api-ws-assets-query-external-prd/.default"
# Typo frequente em .env: "apis-assets-query" em vez de "api-ws-assets-query" → AADSTS500011 invalid_resource
_KNOWN_SCOPE_TYPO = "apis-assets-query-external-prd"
_KNOWN_SCOPE_FIX = "api-ws-assets-query-external-prd"


def _normalize_xpws_scope(scope: str) -> str:
    if _KNOWN_SCOPE_TYPO in scope and _KNOWN_SCOPE_FIX not in scope:
        fixed = scope.replace(_KNOWN_SCOPE_TYPO, _KNOWN_SCOPE_FIX)
        logger.warning(
            "XPWS scope looked like a typo (%s → %s). Fix PERSEVERA_XPWS_SCOPE in your .env.",
            _KNOWN_SCOPE_TYPO,
            _KNOWN_SCOPE_FIX,
        )
        return fixed
    return scope


class XPWSProvider(DataProvider):
    """
    Provider for XP Wealth Services (XPWS) APIs.

    Authentication uses OAuth2 client-credentials on Azure AD.
    You must configure the following environment variables (usually in ~/.persevera/.env):
      - PERSEVERA_XPWS_TENANT_ID
      - PERSEVERA_XPWS_CLIENT_ID
      - PERSEVERA_XPWS_CLIENT_SECRET
      - PERSEVERA_XPWS_SCOPE (optional, has default)
      - PERSEVERA_XPWS_OPERATIONS_BASE_URL (optional, defaults to https://openapi.xpi.com.br/ws-operations-query/external/api)
      - PERSEVERA_XPWS_ASSETS_BASE_URL (optional, for Assets API if needed)
      - PERSEVERA_XPWS_USER_AGENT (optional; unset uses ParceirosXP/PerseveraTools; empty string disables the header)
      - PERSEVERA_XPWS_POSITIONS_V2_USE_LEGACY_PATH (optional; true makes category positions_v2 use the legacy /assets/positions/v2 path)

    Supported categories:
      - raw: Direct endpoint calls
      - positions_v2: Posição V2 (doc) GET /v2/positions/customers/{customerCode} unless legacy path is enabled
      - positions_assets_v2: Legacy GET /assets/positions/v2 (same host as assets base URL)
      - consolidated_position_d0: Posição consolidada D0 GET /v1/consolidated-positions/customer/{customerCode}
      - consolidated_position_history: Posição consolidada histórica GET /v1/consolidated-positions/customer/{customerCode}?monthYear=
      - movements_v2: Operations/movements (Operations API) - GET /v2/operations/customers/{customerId}
      - account_statement: Extrato de Conta Investimento (Operations API) - GET /v1/statement/customers/{customerCode}?monthYear={monthYear}

    Notes:
      - SCOPE defaults to: api://xpcorretora.onmicrosoft.com/api-ws-assets-query-external-prd/.default
        (must be api-ws-assets-query…, not apis-assets-query…)
      - Operations base URL defaults to: https://openapi.xpi.com.br/ws-operations-query/external/api
    """

    def __init__(
        self,
        start_date: str = "1980-01-01",
        *,
        tenant_id: Optional[str] = None,
        client_id: Optional[str] = None,
        client_secret: Optional[str] = None,
        scope: Optional[str] = None,
        base_url: Optional[str] = None,
        timeout_seconds: int = 30,
        token_url_override: Optional[str] = None,
        verify_ssl: Optional[bool] = None,
        user_agent: Optional[str] = None,
        positions_v2_use_legacy_path: Optional[bool] = None,
    ):
        super().__init__(start_date=start_date)
        self.tenant_id = tenant_id or getattr(settings, "XPWS_TENANT_ID", None)
        self.client_id = client_id or getattr(settings, "XPWS_CLIENT_ID", None)
        self.client_secret = client_secret or getattr(settings, "XPWS_CLIENT_SECRET", None)
        raw_scope = scope or getattr(settings, "XPWS_SCOPE", None) or _XPWS_DEFAULT_SCOPE
        self.scope = _normalize_xpws_scope(raw_scope)
        # Base URLs (with sensible defaults for XP APIs)
        self.base_url = base_url or getattr(settings, "XPWS_BASE_URL", None)
        self.assets_base_url = getattr(settings, "XPWS_ASSETS_BASE_URL", None) or self.base_url
        self.operations_base_url = (
            getattr(settings, "XPWS_OPERATIONS_BASE_URL", None)
            or self.base_url
            or "https://openapi.xpi.com.br/ws-operations-query/external/api"
        )
        self.timeout_seconds = timeout_seconds
        self.token_url_override = token_url_override or getattr(settings, "XPWS_TOKEN_URL_OVERRIDE", None)
        # Parse verify_ssl from either argument or settings (string env to bool)
        if verify_ssl is None:
            verify_env = getattr(settings, "XPWS_VERIFY_SSL", "true")
            self.verify_ssl = str(verify_env).lower() in ("1", "true", "yes", "y")
        else:
            self.verify_ssl = verify_ssl

        if user_agent is not None:
            self.user_agent = user_agent.strip() or None
        else:
            env_ua = getattr(settings, "XPWS_USER_AGENT", None)
            if env_ua is None:
                self.user_agent = "ParceirosXP/PerseveraTools"
            else:
                self.user_agent = env_ua.strip() or None

        if positions_v2_use_legacy_path is not None:
            self.positions_v2_use_legacy_path = positions_v2_use_legacy_path
        else:
            legacy_flag = getattr(settings, "XPWS_POSITIONS_V2_USE_LEGACY_PATH", "false")
            self.positions_v2_use_legacy_path = str(legacy_flag).lower() in ("1", "true", "yes", "y")

        self._access_token: Optional[str] = None
        self._access_token_expiry_epoch: float = 0.0

        self._validate_credentials()

    # -------------------------
    # Public API
    # -------------------------
    def get_data(self, category: str, **kwargs) -> pd.DataFrame:
        """
        Retrieve data from XPWS.
        This is a thin routing layer around generic request helpers. Given the breadth of XPWS,
        we first expose a generic mechanism while keeping the standard DataProvider interface.

        Usage patterns:
          - Direct endpoint usage:
              provider.get_data(
                  category=\"raw\",
                  method=\"GET\",
                  path=\"/assets/positions/v2\",
                  params={...}
              )

          - Named shortcuts (when available):
              provider.get_data(
                  category=\"positions_v2\",
                  customer_code=1234,
                  start_date=\"2023-04-11\",
                  end_date=\"2023-04-11\",
                  product_types=[\"Treasury\", \"Fund\"],
              )
              # Legacy path: category=\"positions_assets_v2\" or PERSEVERA_XPWS_POSITIONS_V2_USE_LEGACY_PATH=true

        Returns a DataFrame constructed from the JSON payload when possible; otherwise,
        wraps raw lists/objects into a DataFrame.
        """
        self._log_processing(category)

        if category == "raw":
            method = kwargs.get("method", "GET").upper()
            path = kwargs["path"]
            params = kwargs.get("params")
            json_body = kwargs.get("json")
            data = self._request_json(method=method, path=path, params=params, json=json_body)
            return self._to_dataframe(data)

        if category == "positions_assets_v2":
            params = kwargs.get("params")
            data = self._request_json(
                method="GET",
                path="/assets/positions/v2",
                params=params,
                base_url=self.assets_base_url,
            )
            return self._to_dataframe(data)

        if category == "positions_v2":
            if self.positions_v2_use_legacy_path:
                params = kwargs.get("params")
                data = self._request_json(
                    method="GET",
                    path="/assets/positions/v2",
                    params=params,
                    base_url=self.assets_base_url,
                )
                return self._to_dataframe(data)
            return self._positions_v2_doc_path(**kwargs)

        if category == "consolidated_position_d0":
            cust = (
                kwargs.get("customer_code")
                or kwargs.get("customerId")
                or kwargs.get("customerCode")
            )
            if not cust:
                raise DataRetrievalError(
                    "consolidated_position_d0 requires 'customer_code' (or 'customerId' / 'customerCode')."
                )
            path = f"/v1/consolidated-positions/customer/{cust}"
            data = self._request_json(
                method="GET",
                path=path,
                params=kwargs.get("params"),
                base_url=self.assets_base_url,
            )
            return self._to_dataframe(data)

        if category == "consolidated_position_history":
            cust = (
                kwargs.get("customer_code")
                or kwargs.get("customerId")
                or kwargs.get("customerCode")
            )
            if not cust:
                raise DataRetrievalError(
                    "consolidated_position_history requires 'customer_code' (or 'customerId' / 'customerCode')."
                )
            my = (
                kwargs.get("month_year")
                or kwargs.get("monthYear")
                or (kwargs.get("params") or {}).get("monthYear")
            )
            query_params: Dict[str, Any] = dict(kwargs.get("params") or {})
            if my and "monthYear" not in query_params:
                query_params["monthYear"] = my
            if "monthYear" not in query_params:
                raise DataRetrievalError(
                    "consolidated_position_history requires 'month_year' (format 'MM-YYYY', e.g. '01-2023') "
                    "or params['monthYear']."
                )
            path = f"/v1/consolidated-positions/customer/{cust}"
            data = self._request_json(
                method="GET",
                path=path,
                params=query_params,
                base_url=self.assets_base_url,
            )
            return self._to_dataframe(data)

        if category == "movements_v2":
            # XP Operations Query API v2: /v2/operations/customers/{customerId}
            customer_id = kwargs.get("customer_id") or kwargs.get("customerId")
            if not customer_id:
                raise DataRetrievalError("movements_v2 requires 'customer_id' (or 'customerId').")
            params = kwargs.get("params", {})
            # Optional convenience: accept startReferenceDate/endReferenceDate/productTypes directly
            for key in ("startReferenceDate", "endReferenceDate", "productTypes"):
                if key in kwargs and key not in params:
                    params[key] = kwargs[key]
            path = f"/v2/operations/customers/{customer_id}"
            data = self._request_json(
                method="GET",
                path=path,
                params=params,
                base_url=self.operations_base_url,
            )
            return self._to_dataframe(data)

        if category == "account_statement":
            # Extrato de Conta Investimento
            # Endpoint: GET /v1/statement/customers/{customerCode}?monthYear={monthYear}
            # Params: customerCode (int), monthYear (string, format "MM-YYYY")
            return self._get_account_statement(**kwargs)

        raise DataRetrievalError(f"Unsupported category '{category}' for XPWSProvider.")

    # -------------------------
    # High-level API methods
    # -------------------------
    def _get_account_statement(
        self,
        customer_code: Optional[Union[str, int]] = None,
        customerCode: Optional[Union[str, int]] = None,
        customer_id: Optional[Union[str, int]] = None,
        month_year: Optional[str] = None,
        monthYear: Optional[str] = None,
        params: Optional[Dict[str, Any]] = None,
        **extra_kwargs,
    ) -> pd.DataFrame:
        """
        Extrato de Conta Investimento (Investment Account Statement).

        Obtém movimentações financeiras da conta investimento do cliente em D0.

        Endpoint: GET /v1/statement/customers/{customerCode}?monthYear={monthYear}

        Args:
            customer_code: Customer code (XP account number). Aliases: customerCode, customer_id.
            month_year: Month/year in format "MM-YYYY" (e.g., "05-2023"). Alias: monthYear.
            params: Additional query parameters (overrides individual args).

        Returns:
            DataFrame with account statement data.
        """
        # Resolve customer code from multiple possible argument names
        cust_code = (
            customer_code
            or customerCode
            or customer_id
            or extra_kwargs.get("customer_code")
            or extra_kwargs.get("customerCode")
            or extra_kwargs.get("customer_id")
        )
        if not cust_code:
            raise DataRetrievalError(
                "account_statement requires 'customer_code' (or 'customerCode' / 'customer_id')."
            )

        # Resolve month_year
        my = month_year or monthYear or extra_kwargs.get("month_year") or extra_kwargs.get("monthYear")
        if not my:
            raise DataRetrievalError(
                "account_statement requires 'month_year' (format 'MM-YYYY', e.g., '05-2023')."
            )

        # Build params
        query_params: Dict[str, Any] = dict(params) if params else {}
        if "monthYear" not in query_params:
            query_params["monthYear"] = my

        path = f"/v1/statement/customers/{cust_code}"
        data = self._request_json(
            method="GET",
            path=path,
            params=query_params,
            base_url=self.operations_base_url,
        )
        return self._to_dataframe(data)

    def get_account_statement(
        self,
        customer_code: Union[str, int],
        month_year: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> pd.DataFrame:
        """
        Extrato de Conta Investimento - movimentações financeiras da conta em D0.

        Endpoint: GET /v1/statement/customers/{customerCode}?monthYear={monthYear}

        Pode ser chamado de duas formas:
        1. Com month_year específico: retorna dados de um único mês
        2. Com start_date e end_date: itera por todos os meses no range e concatena

        Args:
            customer_code: Customer code (XP account number), e.g., 1234.
            month_year: Month/year in format "MM-YYYY", e.g., "05-2023".
                        Usado para consulta de mês único.
            start_date: Start date in format "YYYY-MM-DD", e.g., "2024-01-01".
                        Usado junto com end_date para range de meses.
            end_date: End date in format "YYYY-MM-DD", e.g., "2024-12-31".
                      Usado junto com start_date para range de meses.

        Returns:
            DataFrame with account statement data.

        Examples:
            # Mês único
            >>> xp.get_account_statement(
            ...     customer_code=2199693,
            ...     month_year="01-2025"
            ... )

            # Range de datas (itera por todos os meses)
            >>> xp.get_account_statement(
            ...     customer_code=2199693,
            ...     start_date="2024-01-01",
            ...     end_date="2024-12-31"
            ... )
        """
        # Modo 1: month_year específico
        if month_year:
            return self._get_account_statement(
                customer_code=customer_code,
                month_year=month_year,
            )

        # Modo 2: range de datas
        if start_date and end_date:
            return self._get_account_statement_range(
                customer_code=customer_code,
                start_date=start_date,
                end_date=end_date,
            )

        raise DataRetrievalError(
            "get_account_statement requires either 'month_year' (e.g., '01-2025') "
            "or both 'start_date' and 'end_date' (e.g., '2024-01-01', '2024-12-31')."
        )

    def _get_account_statement_range(
        self,
        customer_code: Union[str, int],
        start_date: str,
        end_date: str,
    ) -> pd.DataFrame:
        """
        Fetch account statements for a range of months.

        Iterates through each month from start_date to end_date, calls the API
        for each month, and concatenates all results.

        Args:
            customer_code: Customer code (XP account number).
            start_date: Start date in format "YYYY-MM-DD".
            end_date: End date in format "YYYY-MM-DD".

        Returns:
            Concatenated DataFrame with all months' data.
        """
        # Parse dates
        try:
            start_dt = datetime.strptime(start_date, "%Y-%m-%d")
            end_dt = datetime.strptime(end_date, "%Y-%m-%d")
        except ValueError as e:
            raise DataRetrievalError(
                f"Invalid date format. Expected 'YYYY-MM-DD'. Error: {e}"
            )

        if start_dt > end_dt:
            raise DataRetrievalError(
                f"start_date ({start_date}) must be before or equal to end_date ({end_date})."
            )

        # Generate list of months in range
        months: List[str] = []
        current = start_dt.replace(day=1)  # Start from first day of month
        end_month = end_dt.replace(day=1)

        while current <= end_month:
            month_year = current.strftime("%m-%Y")  # Format: MM-YYYY
            months.append(month_year)
            current += relativedelta(months=1)

        logger.info(f"Fetching account statements for {len(months)} months: {months[0]} to {months[-1]}")

        # Fetch data for each month
        dfs: List[pd.DataFrame] = []
        for month_year in months:
            try:
                logger.info(f"Fetching account statement for {month_year}...")
                df = self._get_account_statement(
                    customer_code=customer_code,
                    month_year=month_year,
                )
                if not df.empty:
                    df["_month_year"] = month_year  # Add reference column
                    dfs.append(df)
                    logger.info(f"  -> {len(df)} records for {month_year}")
                else:
                    logger.info(f"  -> No data for {month_year}")
            except DataRetrievalError as e:
                logger.warning(f"  -> Error fetching {month_year}: {e}")
                # Continue with other months even if one fails

        # Concatenate all DataFrames
        if not dfs:
            logger.warning("No data found for any month in the range.")
            return pd.DataFrame()

        result = pd.concat(dfs, ignore_index=True)
        logger.info(f"Total: {len(result)} records from {len(dfs)} months.")
        return result

    def get_operations(
        self,
        customer_id: str,
        start_date: str,
        end_date: str,
        product_types: Optional[List[str]] = None,
    ) -> pd.DataFrame:
        """
        Convenience method for Operations/Movements (movimentações).

        Args:
            customer_id: Customer ID (XP account number).
            start_date: Start date (YYYY-MM-DD).
            end_date: End date (YYYY-MM-DD).
            product_types: Optional list of product types to filter.
                Options: Coe, Treasury, Cash, Stock, TradedFunds, Repo,
                         FixedIncome, PensionFunds, Fund, etc.

        Returns:
            DataFrame with operations data.

        Example:
            >>> xp.get_operations(
            ...     customer_id="3352704",
            ...     start_date="2025-01-01",
            ...     end_date="2025-01-31",
            ...     product_types=["Treasury", "FixedIncome", "Fund"]
            ... )
        """
        params: Dict[str, Any] = {
            "startReferenceDate": start_date,
            "endReferenceDate": end_date,
        }
        if product_types:
            params["productTypes"] = product_types

        path = f"/v2/operations/customers/{customer_id}"
        data = self._request_json(
            method="GET",
            path=path,
            params=params,
            base_url=self.operations_base_url,
        )
        return self._to_dataframe(data)

    def get_positions_v2(
        self,
        customer_code: Union[str, int],
        start_date: str,
        end_date: str,
        product_types: List[str],
        params: Optional[Dict[str, Any]] = None,
    ) -> pd.DataFrame:
        """
        Posição V2 (documentação XP): posições por cliente, intervalo e tipos de produto.

        GET /v2/positions/customers/{customerCode}?startReferenceDate=...&endReferenceDate=...&productTypes=...
        """
        merged: Dict[str, Any] = dict(params) if params else {}
        merged.setdefault("startReferenceDate", start_date)
        merged.setdefault("endReferenceDate", end_date)
        merged.setdefault("productTypes", product_types)
        return self._positions_v2_doc_path(
            customer_code=customer_code,
            params=merged,
        )

    def get_consolidated_position_d0(
        self,
        customer_code: Union[str, int],
        *,
        params: Optional[Dict[str, Any]] = None,
    ) -> pd.DataFrame:
        """Posição consolidada em D0: GET /v1/consolidated-positions/customer/{customerCode}."""
        return self.get_data(
            "consolidated_position_d0",
            customer_code=customer_code,
            params=params,
        )

    def get_consolidated_position_history(
        self,
        customer_code: Union[str, int],
        month_year: str,
        *,
        params: Optional[Dict[str, Any]] = None,
    ) -> pd.DataFrame:
        """Posição consolidada histórica (fechamento mensal): monthYear no formato MM-YYYY."""
        return self.get_data(
            "consolidated_position_history",
            customer_code=customer_code,
            month_year=month_year,
            params=params,
        )

    # -------------------------
    # Internals
    # -------------------------
    def _positions_v2_doc_path(self, **kwargs) -> pd.DataFrame:
        cust = (
            kwargs.get("customer_code")
            or kwargs.get("customerId")
            or kwargs.get("customerCode")
        )
        if not cust:
            raise DataRetrievalError(
                "positions_v2 requires 'customer_code' (or 'customerId' / 'customerCode')."
            )
        params: Dict[str, Any] = dict(kwargs.get("params") or {})
        alias_map = (
            ("startReferenceDate", "startReferenceDate"),
            ("endReferenceDate", "endReferenceDate"),
            ("start_date", "startReferenceDate"),
            ("end_date", "endReferenceDate"),
            ("productTypes", "productTypes"),
            ("product_types", "productTypes"),
        )
        for src, dest in alias_map:
            if src in kwargs and dest not in params:
                params[dest] = kwargs[src]
        for key in ("startReferenceDate", "endReferenceDate", "productTypes"):
            if key not in params:
                raise DataRetrievalError(
                    f"positions_v2 requires '{key}' in params or kwargs "
                    "(aliases: start_date, end_date, product_types)."
                )
        pt = params.get("productTypes")
        if pt is None or (isinstance(pt, (list, tuple)) and len(pt) == 0) or pt == "":
            raise DataRetrievalError("positions_v2 requires non-empty 'productTypes' (per API contract).")
        path = f"/v2/positions/customers/{cust}"
        data = self._request_json(
            method="GET",
            path=path,
            params=params,
            base_url=self.assets_base_url,
        )
        return self._to_dataframe(data)

    def _validate_credentials(self) -> None:
        missing = []
        if not self.tenant_id:
            missing.append("PERSEVERA_XPWS_TENANT_ID")
        if not self.client_id:
            missing.append("PERSEVERA_XPWS_CLIENT_ID")
        if not self.client_secret:
            missing.append("PERSEVERA_XPWS_CLIENT_SECRET")
        if not self.scope:
            missing.append("PERSEVERA_XPWS_SCOPE")
        # At least one base URL must be provided (generic, assets or operations)
        if not (self.base_url or self.assets_base_url or self.operations_base_url):
            missing.append("PERSEVERA_XPWS_BASE_URL or PERSEVERA_XPWS_ASSETS_BASE_URL or PERSEVERA_XPWS_OPERATIONS_BASE_URL")
        if missing:
            raise ValueError(
                "Missing XPWS configuration variables: " + ", ".join(missing)
            )

    def _ensure_token(self) -> None:
        if self._access_token and time.time() < self._access_token_expiry_epoch - 30:
            return
        self._refresh_token()

    def _refresh_token(self) -> None:
        token_url = self.token_url_override or f"https://login.microsoftonline.com/{self.tenant_id}/oauth2/v2.0/token"
        payload = {
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "grant_type": "client_credentials",
            "scope": self.scope,
        }
        headers = {"Content-Type": "application/x-www-form-urlencoded"}
        if self.user_agent:
            headers["User-Agent"] = self.user_agent
        try:
            logger.info(f"Requesting XPWS access token at {token_url} with scope '{self.scope}'")
            resp = requests.post(
                token_url,
                data=payload,
                headers=headers,
                timeout=self.timeout_seconds,
                verify=self.verify_ssl,
            )
            resp.raise_for_status()
        except requests.RequestException as e:
            detail = ""
            try:
                if hasattr(e, "response") and e.response is not None:
                    detail = f" | body: {e.response.text}"
            except Exception:
                pass
            raise DataRetrievalError(f"XPWS auth failed: {e}{detail}")

        data = resp.json()
        access_token = data.get("access_token")
        expires_in = data.get("expires_in", 3600)
        if not access_token:
            raise DataRetrievalError(f"XPWS auth response missing access_token: {data}")

        self._access_token = access_token
        self._access_token_expiry_epoch = time.time() + int(expires_in)
        logger.info("XPWS access token acquired.")

    def _headers(self) -> Dict[str, str]:
        self._ensure_token()
        h: Dict[str, str] = {
            "Authorization": f"Bearer {self._access_token}",
            "Accept": "application/json",
            "Content-Type": "application/json",
        }
        if self.user_agent:
            h["User-Agent"] = self.user_agent
        return h

    def _request_json(
        self,
        *,
        method: str,
        path: str,
        params: Optional[Dict[str, Any]] = None,
        json: Optional[Dict[str, Any]] = None,
        base_url: Optional[str] = None,
    ) -> Any:
        url = self._build_url(path, base_url=base_url)
        headers = self._headers()
        
        # Debug logging
        token_preview = self._access_token[:20] + "..." if self._access_token else "None"
        logger.info(f"XPWS Request: {method} {url}")
        logger.info(f"XPWS Headers: Authorization=Bearer {token_preview}, Accept={headers.get('Accept')}")
        if params:
            logger.info(f"XPWS Params: {params}")
        
        try:
            resp = requests.request(
                method=method,
                url=url,
                headers=headers,
                params=params,
                json=json,
                timeout=self.timeout_seconds,
                verify=self.verify_ssl,
            )
            resp.raise_for_status()
            if resp.content and "application/json" in resp.headers.get("Content-Type", ""):
                return resp.json()
            return {}
        except requests.HTTPError as e:
            # Surface API errors with details when possible
            detail = ""
            try:
                detail = f" | body: {resp.text}"
            except Exception:
                pass
            raise DataRetrievalError(f"XPWS request failed [{method} {url}] {e}{detail}")
        except requests.RequestException as e:
            raise DataRetrievalError(f"XPWS request error [{method} {url}]: {e}")

    def _build_url(self, path: str, *, base_url: Optional[str] = None) -> str:
        # Allow absolute URLs
        if path.startswith("http://") or path.startswith("https://"):
            return path
        selected_base = (base_url or self.base_url or "").rstrip("/") if (base_url or self.base_url) else ""
        if not path.startswith("/"):
            path = f"/{path}"
        if not selected_base:
            raise DataRetrievalError("No base URL configured for XPWS request.")
        return f"{selected_base}{path}"

    @staticmethod
    def _to_dataframe(payload: Any) -> pd.DataFrame:
        """
        Convert a typical XPWS JSON response into a DataFrame.
        This function attempts to be permissive: it handles list payloads,
        wrapped 'data' fields, or single objects.
        """
        if payload is None:
            return pd.DataFrame()
        if isinstance(payload, list):
            return pd.json_normalize(payload)
        if isinstance(payload, dict):
            # Common patterns: {'data': [...]}, {'items': [...]}, or the dict itself
            for key in ("data", "items", "results", "content"):
                if key in payload and isinstance(payload[key], list):
                    return pd.json_normalize(payload[key])
            return pd.json_normalize(payload)
        return pd.DataFrame()


