from __future__ import annotations

import math
from typing import Any, Dict, List, Optional

import requests
import pandas as pd

from .base import DataProvider, DataRetrievalError
from ...config import settings
from ...utils.logging import get_logger

logger = get_logger(__name__)

_DEFAULT_BASE_URL = "https://api-advisor.xpi.com.br/rf-fixedincome-hub-apim/v2"
_DEFAULT_BRAND = "XP"
_DEFAULT_SEGMENT_CHANNEL = "pf"
_DEFAULT_SEGMENT_CODE = "300"
_DEFAULT_PAGE_SIZE = 50
# Chave de subscrição APIM do portal hub.xpi.com.br — embutida no front-end da XP,
# estática até que a XP faça uma rotação deliberada (quebraria todos os usuários).
# Pode ser sobrescrita via PERSEVERA_XPHUB_SUBSCRIPTION_KEY no .env.
_DEFAULT_SUBSCRIPTION_KEY = "3923e12297e7448398ba9a9046c4fced"


class XPHubProvider(DataProvider):
    """
    Provider for the XP Advisor Hub platform (rf-fixedincome-hub-apim).

    Authentication relies on the session Bearer JWT obtained from the advisor
    portal (https://hub.xpi.com.br). The token is short-lived (~10 h) and must
    be passed directly to the constructor — it should NOT be stored in .env
    since it expires daily and would become stale.

    Recommended usage:
        TOKEN = "eyJhbGci..."   # copiado do DevTools a cada sessão
        hub = XPHubProvider(bearer_token=TOKEN)

    Optional environment variables (stable values only, in ~/.persevera/.env):
      - PERSEVERA_XPHUB_SUBSCRIPTION_KEY: ocp-apim-subscription-key (has default)
      - PERSEVERA_XPHUB_CUSTOMER_CODE  : default customer code (can be passed per call)
      - PERSEVERA_XPHUB_BASE_URL       : API base URL (has default)
      - PERSEVERA_XPHUB_BRAND          : brand string, default "XP"
      - PERSEVERA_XPHUB_SEGMENT_CHANNEL: segmentChannel, default "pf"
      - PERSEVERA_XPHUB_SEGMENT_CODE   : segmentCode, default "300"

    Supported categories
    --------------------
    available_assets
        Lists fixed-income assets available for a customer.
        Extra kwargs accepted:
          - asset_category (str): e.g. "BANCARIO", "CRI", "CRA", "DEBENTURE"
          - customer_code (int | str): overrides the default customer code
          - page_size (int): items per page, default 50
          - max_pages (int | None): stop after N pages; None = fetch all
    """

    def __init__(
        self,
        start_date: str = "1980-01-01",
        *,
        bearer_token: Optional[str] = None,
        subscription_key: Optional[str] = None,
        customer_code: Optional[str] = None,
        base_url: Optional[str] = None,
        brand: Optional[str] = None,
        segment_channel: Optional[str] = None,
        segment_code: Optional[str] = None,
        timeout_seconds: int = 30,
        verify_ssl: bool = True,
    ):
        super().__init__(start_date=start_date)

        # Bearer token deve ser passado diretamente — não lemos do .env pois expira diariamente
        self.bearer_token = bearer_token
        self.subscription_key = (
            subscription_key
            or getattr(settings, "XPHUB_SUBSCRIPTION_KEY", None)
            or _DEFAULT_SUBSCRIPTION_KEY
        )
        self.customer_code = str(customer_code or getattr(settings, "XPHUB_CUSTOMER_CODE", "") or "")
        self.base_url = (base_url or getattr(settings, "XPHUB_BASE_URL", None) or _DEFAULT_BASE_URL).rstrip("/")
        self.brand = brand or getattr(settings, "XPHUB_BRAND", None) or _DEFAULT_BRAND
        self.segment_channel = segment_channel or getattr(settings, "XPHUB_SEGMENT_CHANNEL", None) or _DEFAULT_SEGMENT_CHANNEL
        self.segment_code = segment_code or getattr(settings, "XPHUB_SEGMENT_CODE", None) or _DEFAULT_SEGMENT_CODE
        self.timeout_seconds = timeout_seconds
        self.verify_ssl = verify_ssl

        self._validate_credentials()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def get_data(self, category: str, **kwargs) -> pd.DataFrame:
        """
        Retrieve data from the XP Hub platform.

        Parameters
        ----------
        category : str
            One of: "available_assets"
        **kwargs
            Category-specific arguments (see class docstring).

        Returns
        -------
        pd.DataFrame
            Raw DataFrame with the API response fields as columns.
        """
        self._log_processing(category)

        if category == "available_assets":
            return self._get_available_assets(**kwargs)

        raise DataRetrievalError(
            f"Unknown category '{category}'. Supported: available_assets"
        )

    # ------------------------------------------------------------------
    # Category handlers
    # ------------------------------------------------------------------

    def _get_available_assets(
        self,
        asset_category: Optional[str] = None,
        customer_code: Optional[str] = None,
        page_size: int = _DEFAULT_PAGE_SIZE,
        max_pages: Optional[int] = None,
        **_extra,
    ) -> pd.DataFrame:
        """
        Fetch all pages of /available-assets and return a consolidated DataFrame.

        Parameters
        ----------
        asset_category : str, optional
            Fixed-income category filter, e.g. "BANCARIO", "CRI", "CRA",
            "DEBENTURE". Omit to retrieve all categories (may be slow).
        customer_code : str | int, optional
            Customer code; falls back to the provider default.
        page_size : int
            Number of items per page request (max typically 50).
        max_pages : int | None
            Safety cap on the number of pages fetched. None means no limit.
        """
        code = str(customer_code or self.customer_code)
        if not code:
            raise DataRetrievalError(
                "customer_code is required. Pass it as a kwarg or set "
                "PERSEVERA_XPHUB_CUSTOMER_CODE in your .env."
            )

        params: Dict[str, Any] = {
            "customerCode": code,
            "brand": self.brand,
            "pageSize": page_size,
            "segmentChannel": self.segment_channel,
            "segmentCode": self.segment_code,
        }
        if asset_category:
            params["category"] = asset_category

        records: List[Dict[str, Any]] = []
        page = 1

        while True:
            if max_pages is not None and page > max_pages:
                break

            params["page"] = page
            data = self._request_json("/available-assets", params=params)

            # Log envelope on first page to aid debugging of API structure
            if page == 1 and isinstance(data, dict):
                envelope = {k: v for k, v in data.items() if not isinstance(v, list)}
                logger.info(
                    "available_assets envelope (page 1, customer=%s, category=%s): %s",
                    code, asset_category or "ALL", envelope,
                )

            page_items = self._extract_items(data)
            records.extend(page_items)

            logger.info(
                "available_assets: page=%d items=%d total_so_far=%d",
                page, len(page_items), len(records),
            )

            # Primary stop: page returned fewer items than requested → last page
            # Secondary stop: explicit total_pages hint from envelope (belt-and-suspenders)
            total_pages = self._extract_total_pages(data, page_size)
            last_page = (
                len(page_items) < page_size
                or not page_items
                or (total_pages is not None and page >= total_pages)
            )
            if last_page:
                break

            page += 1

        if not records:
            logger.warning("available_assets returned no records.")
            return pd.DataFrame()

        return pd.DataFrame(records)

    # ------------------------------------------------------------------
    # HTTP helpers
    # ------------------------------------------------------------------

    def _request_json(self, path: str, params: Optional[Dict] = None) -> Any:
        url = f"{self.base_url}{path}"
        headers = self._build_headers()

        logger.debug("GET %s params=%s", url, params)
        try:
            response = requests.get(
                url,
                headers=headers,
                params=params,
                timeout=self.timeout_seconds,
                verify=self.verify_ssl,
            )
            response.raise_for_status()
            return response.json()
        except requests.HTTPError as exc:
            body = ""
            try:
                body = exc.response.text[:500]
            except Exception:
                pass
            raise DataRetrievalError(
                f"HTTP {exc.response.status_code} from {url}: {body}"
            ) from exc
        except requests.RequestException as exc:
            raise DataRetrievalError(f"Request failed for {url}: {exc}") from exc

    def _build_headers(self) -> Dict[str, str]:
        # The XP WAF validates that requests look like they come from a real
        # browser session. All sec-* and user-agent headers are required.
        headers = {
            "accept": "application/json",
            "accept-language": "pt-BR,pt;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6,pt-PT;q=0.5",
            "access-control-allow-origin": "*",
            "authorization": f"Bearer {self.bearer_token}",
            "origin": "https://hub.xpi.com.br",
            "referer": "https://hub.xpi.com.br/",
            "sec-ch-ua": '"Microsoft Edge";v="147", "Not.A/Brand";v="8", "Chromium";v="147"',
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": '"Windows"',
            "sec-fetch-dest": "empty",
            "sec-fetch-mode": "cors",
            "sec-fetch-site": "same-site",
            "user-agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/147.0.0.0 Safari/537.36 Edg/147.0.0.0"
            ),
        }
        if self.subscription_key:
            headers["ocp-apim-subscription-key"] = self.subscription_key
        return headers

    # ------------------------------------------------------------------
    # Response parsing
    # ------------------------------------------------------------------

    @staticmethod
    def _extract_items(data: Any) -> List[Dict[str, Any]]:
        """Pull the list of asset records out of whatever envelope the API returns."""
        if isinstance(data, list):
            return data
        if isinstance(data, dict):
            for key in ("data", "items", "assets", "result", "content"):
                if isinstance(data.get(key), list):
                    return data[key]
        return []

    @staticmethod
    def _extract_total_pages(data: Any, page_size: int) -> Optional[int]:
        """Best-effort extraction of total page count from the response envelope."""
        if not isinstance(data, dict):
            return None

        # 1. Explicit page-count fields (highest confidence)
        for key in ("totalPages", "total_pages", "pageCount", "totalPage"):
            val = data.get(key)
            if val is not None:
                try:
                    return int(val)
                except (TypeError, ValueError):
                    pass

        # 2. Nested pagination object
        pagination = data.get("pagination") or data.get("paging") or {}
        if isinstance(pagination, dict):
            for key in ("totalPages", "total_pages", "pageCount", "totalPage"):
                val = pagination.get(key)
                if val is not None:
                    try:
                        return int(val)
                    except (TypeError, ValueError):
                        pass
            # Derive from nested totalCount
            for key in ("totalCount", "totalItems", "total"):
                val = pagination.get(key)
                if val is not None:
                    try:
                        return math.ceil(int(val) / page_size)
                    except (TypeError, ValueError):
                        pass

        # 3. Derive from root-level total item count.
        # Prefer unambiguous names; avoid "count" and "size" which often mean
        # the number of items on the *current* page, not the grand total.
        for key in ("totalCount", "totalItems", "totalRecords", "recordCount", "total"):
            val = data.get(key)
            if val is not None:
                try:
                    total = math.ceil(int(val) / page_size)
                    # Sanity check: if it computes to ≤ 1 it's probably the page size itself
                    if total > 1:
                        return total
                except (TypeError, ValueError):
                    pass

        return None

    # ------------------------------------------------------------------
    # Validation
    # ------------------------------------------------------------------

    def _validate_credentials(self) -> None:
        if not self.bearer_token:
            raise ValueError(
                "XPHubProvider requer bearer_token=. "
                "Copie o JWT do DevTools (F12 → Network → Authorization) "
                "e passe diretamente: XPHubProvider(bearer_token='eyJhbGci...')."
            )
