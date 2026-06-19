"""
BTGWSProvider — provider for BTG Pactual MFO (Multi-Family Office) APIs.

Authentication uses OAuth2 client-credentials with HTTP Basic auth (client_id:client_secret),
unlike the Azure AD flow used by XP.

Required environment variables (in ~/.persevera/.env):
  - PERSEVERA_BTGWS_CLIENT_ID
  - PERSEVERA_BTGWS_CLIENT_SECRET

Optional environment variables:
  - PERSEVERA_BTGWS_AUTH_BASE_URL      (default: https://api.btgpactual.com/iaas-auth)
  - PERSEVERA_BTGWS_ACCOUNTS_BASE_URL  (default: https://api.btgpactual.com/api-account-base)
  - PERSEVERA_BTGWS_POSITIONS_BASE_URL (default: https://api.btgpactual.com/iaas-api-position)
  - PERSEVERA_BTGWS_TIMEOUT            (default: 30)
  - PERSEVERA_BTGWS_VERIFY_SSL         (default: true)
  - PERSEVERA_BTGWS_REQUEST_DELAY      (default: 1.0 — pausa entre contas em bulk)
  - PERSEVERA_BTGWS_MAX_RETRIES        (default: 5)
  - PERSEVERA_BTGWS_RETRY_BACKOFF      (default: 2.0 — base do backoff exponencial)
"""

from __future__ import annotations

import base64
import time
import uuid
import warnings
from typing import Any, Dict, Iterator, List, Optional, Union

import pandas as pd
import requests

from .base import DataProvider, DataRetrievalError
from ...config import settings
from ...utils.logging import get_logger

logger = get_logger(__name__)

# ---------------------------------------------------------------------------
# Defaults
# ---------------------------------------------------------------------------
_DEFAULT_AUTH_BASE_URL = "https://api.btgpactual.com/iaas-auth"
_DEFAULT_ACCOUNTS_BASE_URL = "https://api.btgpactual.com/api-account-base"
_DEFAULT_POSITIONS_BASE_URL = "https://api.btgpactual.com/iaas-api-position"

_AUTH_TOKEN_PATH = "/api/v1/authorization/oauth2/accesstoken"
_ACCOUNTS_PATH = "/api/v1/account-base/accounts"
_POSITION_PATH = "/api/v1/position/{account_number}"  # {account_number} substituted at runtime
_TOKEN_DEFAULT_EXPIRES_SECONDS = 15 * 60  # BTG MFO tokens are valid ~15 minutes

_RETRY_STATUS_CODES = frozenset({429, 502, 503, 504})
_DEFAULT_REQUEST_DELAY = 0.5
_DEFAULT_MAX_RETRIES = 5
_DEFAULT_RETRY_BACKOFF = 2.0
_MIN_429_BACKOFF_SECONDS = 5.0

# Asset classes returned inside each position response.
# Used by _extract_asset_class() and get_position_by_asset_class().
ASSET_CLASSES = (
    "Equities",
    "FixedIncome",
    "InvestmentFund",
    "SummaryAccounts",
    "PensionInformations",
    "Precatories",
    "Derivative",
    "FixedIncomeStructuredNote",
    "CryptoCoins",
    "Commodity",
)


class BTGWSProvider(DataProvider):
    """
    Provider for BTG Pactual MFO APIs.

    Supported categories in get_data():
      - raw                        : Direct endpoint call (method, path, params, base_url)
      - accounts                   : List all accounts for the partner
      - position                   : Posição completa de uma conta → requires account_number
      - all_positions              : Posição de todas as contas (itera sobre accounts; filtra typeFund)
      - position_by_asset_class      : Posição normalizada de uma classe de ativo → account_number, asset_class
      - all_positions_by_asset_class : all_positions filtrado por asset_class

    Deprecated category aliases (still accepted):
      positions_all, position_flat, positions_all_flat

    Convenience methods (public API):
      - get_accounts()
      - get_position(account_number)
      - get_all_positions(skip_funds)
      - get_position_by_asset_class(account_number, asset_class)
      - get_all_positions_by_asset_class(asset_class, skip_funds)

    Asset classes (asset_class param):
      Equities, FixedIncome, InvestmentFund, SummaryAccounts, PensionInformations,
      Precatories, Derivative, FixedIncomeStructuredNote, CryptoCoins, Commodity

    Examples:
        >>> btg = BTGWSProvider()

        # Listar todas as contas
        >>> accounts = btg.get_accounts()

        # Posição completa de uma conta (dict bruto → DataFrame de 1 linha com a estrutura aninhada)
        >>> pos = btg.get_position("000360788")

        # Renda Fixa de todas as contas num único DataFrame
        >>> rf = btg.get_all_positions_by_asset_class("FixedIncome")

        # Fundos de uma conta específica
        >>> fundos = btg.get_position_by_asset_class("000360788", "InvestmentFund")
    """

    def __init__(
        self,
        start_date: str = "1980-01-01",
        *,
        client_id: Optional[str] = None,
        client_secret: Optional[str] = None,
        auth_base_url: Optional[str] = None,
        accounts_base_url: Optional[str] = None,
        positions_base_url: Optional[str] = None,
        timeout_seconds: int = 30,
        verify_ssl: Optional[bool] = None,
        request_delay: Optional[float] = None,
        max_retries: Optional[int] = None,
        retry_backoff: Optional[float] = None,
    ):
        super().__init__(start_date=start_date)

        self.client_id = client_id or getattr(settings, "BTGWS_CLIENT_ID", None)
        self.client_secret = client_secret or getattr(settings, "BTGWS_CLIENT_SECRET", None)

        self.auth_base_url = (
            auth_base_url
            or getattr(settings, "BTGWS_AUTH_BASE_URL", None)
            or _DEFAULT_AUTH_BASE_URL
        ).rstrip("/")

        self.accounts_base_url = (
            accounts_base_url
            or getattr(settings, "BTGWS_ACCOUNTS_BASE_URL", None)
            or _DEFAULT_ACCOUNTS_BASE_URL
        ).rstrip("/")

        self.positions_base_url = (
            positions_base_url
            or getattr(settings, "BTGWS_POSITIONS_BASE_URL", None)
            or _DEFAULT_POSITIONS_BASE_URL
        ).rstrip("/")

        timeout_env = getattr(settings, "BTGWS_TIMEOUT", None)
        self.timeout_seconds = int(timeout_env) if timeout_env else timeout_seconds

        if verify_ssl is None:
            verify_env = getattr(settings, "BTGWS_VERIFY_SSL", "true")
            self.verify_ssl = str(verify_env).lower() in ("1", "true", "yes", "y")
        else:
            self.verify_ssl = verify_ssl

        delay_env = getattr(settings, "BTGWS_REQUEST_DELAY", None)
        self.request_delay = (
            float(request_delay)
            if request_delay is not None
            else float(delay_env) if delay_env else _DEFAULT_REQUEST_DELAY
        )

        retries_env = getattr(settings, "BTGWS_MAX_RETRIES", None)
        self.max_retries = (
            int(max_retries)
            if max_retries is not None
            else int(retries_env) if retries_env else _DEFAULT_MAX_RETRIES
        )

        backoff_env = getattr(settings, "BTGWS_RETRY_BACKOFF", None)
        self.retry_backoff = (
            float(retry_backoff)
            if retry_backoff is not None
            else float(backoff_env) if backoff_env else _DEFAULT_RETRY_BACKOFF
        )

        self._access_token: Optional[str] = None
        self._access_token_expiry_epoch: float = 0.0

        self._validate_credentials()
        logger.info(
            "BTG rate limiting: %.1fs delay between bulk account requests, "
            "%d max retries, %.1fs retry backoff",
            self.request_delay,
            self.max_retries,
            self.retry_backoff,
        )

    # ------------------------------------------------------------------
    # Public DataProvider interface
    # ------------------------------------------------------------------

    def get_data(self, category: str, **kwargs) -> pd.DataFrame:
        """
        Route to the appropriate handler based on category.

        Supported categories and their required kwargs:
          raw                        → method, path, [params], [base_url]
          accounts                   → (none)
          position                   → account_number
          all_positions              → [skip_funds=True]
          position_by_asset_class    → account_number, asset_class
          all_positions_by_asset_class → asset_class, [skip_funds=True]

        Deprecated aliases: positions_all, position_flat, positions_all_flat
        """
        self._log_processing(category)
        category = self._normalize_category(category)

        if category == "raw":
            return self._handle_raw(**kwargs)

        if category == "accounts":
            return self.get_accounts()

        if category == "position":
            account_number = self._require_kwarg(kwargs, "account_number", category)
            return self.get_position(account_number)

        if category == "all_positions":
            skip_funds: bool = kwargs.get("skip_funds", True)
            return self.get_all_positions(skip_funds=skip_funds)

        if category == "position_by_asset_class":
            account_number = self._require_kwarg(kwargs, "account_number", category)
            asset_class = self._require_kwarg(kwargs, "asset_class", category)
            return self.get_position_by_asset_class(account_number, asset_class)

        if category == "all_positions_by_asset_class":
            asset_class = self._require_kwarg(kwargs, "asset_class", category)
            skip_funds = kwargs.get("skip_funds", True)
            return self.get_all_positions_by_asset_class(asset_class, skip_funds=skip_funds)

        raise DataRetrievalError(f"Unsupported category '{category}' for BTGWSProvider.")

    # ------------------------------------------------------------------
    # Convenience public methods
    # ------------------------------------------------------------------

    def get_accounts(self) -> pd.DataFrame:
        """
        Retorna a lista de contas vinculadas ao parceiro.

        GET /api/v1/account-base/accounts
        Campos relevantes: accountNumber, typeFund (bool string), …

        Returns:
            DataFrame com uma linha por conta.
        """
        data = self._request_json(
            method="GET",
            path=_ACCOUNTS_PATH,
            base_url=self.accounts_base_url,
        )
        # API retorna {"accounts": [...]}
        accounts = data.get("accounts", data) if isinstance(data, dict) else data
        return self._to_dataframe(accounts)

    def get_position(self, account_number: Union[str, int]) -> pd.DataFrame:
        """
        Posição completa de uma única conta.

        GET /api/v1/position/{account_number}

        Retorna um DataFrame de uma linha com a estrutura JSON bruta achatada
        (pd.json_normalize). Para extrair classes de ativos normalizadas, use
        get_position_by_asset_class().

        Args:
            account_number: Número da conta BTG (ex: "000360788").

        Returns:
            DataFrame com a posição consolidada da conta (1 linha, muitas colunas).
        """
        path = _POSITION_PATH.format(account_number=account_number)
        data = self._request_json(
            method="GET",
            path=path,
            base_url=self.positions_base_url,
        )
        return self._to_dataframe(data)

    def get_all_positions(self, *, skip_funds: bool = True) -> pd.DataFrame:
        """
        Posição consolidada de todas as contas.

        Itera sobre get_accounts() e chama get_position() para cada conta.
        Por padrão filtra contas onde typeFund=true (contas de fundo de investimento
        gerenciadas pelo BTG), replicando o filtro do cenário Make.

        Args:
            skip_funds: Se True (padrão), ignora contas com typeFund=true.

        Returns:
            DataFrame concatenado com a posição de todas as contas qualificadas.
            Coluna extra "_accountNumber" adicionada para rastreamento.
        """
        accounts_df = self.get_accounts()
        dfs: List[pd.DataFrame] = []

        for idx, account in enumerate(self._iter_accounts(accounts_df, skip_funds=skip_funds)):
            acc_number = account.get("accountNumber") or account.get("account_number")
            if not acc_number:
                continue
            if idx > 0 and self.request_delay > 0:
                time.sleep(self.request_delay)
            try:
                logger.info("Fetching position for account %s …", acc_number)
                df = self.get_position(acc_number)
                if not df.empty:
                    df["_accountNumber"] = acc_number
                    dfs.append(df)
            except DataRetrievalError as exc:
                logger.warning("Could not fetch position for %s: %s", acc_number, exc)

        if not dfs:
            return pd.DataFrame()

        return pd.concat(dfs, ignore_index=True)

    def get_position_by_asset_class(
        self,
        account_number: Union[str, int],
        asset_class: str,
    ) -> pd.DataFrame:
        """
        Posição de uma conta para uma classe de ativo específica, normalizada.

        Obtém a posição bruta da conta e extrai apenas os itens da
        classe de ativo solicitada, retornando um DataFrame achatado.

        Args:
            account_number: Número da conta BTG.
            asset_class: Nome da classe de ativo. Opções:
                Equities, FixedIncome, InvestmentFund, SummaryAccounts,
                PensionInformations, Precatories, Derivative,
                FixedIncomeStructuredNote, CryptoCoins, Commodity.

        Returns:
            DataFrame com uma linha por ativo na classe solicitada.
            Coluna extra "_accountNumber" adicionada.
            Retorna DataFrame vazio se a conta não tiver posição nessa classe.

        Example:
            >>> rf = btg.get_position_by_asset_class("000360788", "FixedIncome")
            >>> fundos = btg.get_position_by_asset_class("000360788", "InvestmentFund")
        """
        self._validate_asset_class(asset_class)
        path = _POSITION_PATH.format(account_number=account_number)
        data = self._request_json(
            method="GET",
            path=path,
            base_url=self.positions_base_url,
        )

        position_data = data.get("data", data) if isinstance(data, dict) else data
        df = self._extract_asset_class(position_data, asset_class, account_number)
        return df

    def get_all_positions_by_asset_class(
        self,
        asset_class: str,
        *,
        skip_funds: bool = True,
    ) -> pd.DataFrame:
        """
        Posição de todas as contas para uma classe de ativo, normalizada.

        Combina get_all_positions() e get_position_by_asset_class(): itera sobre todas as
        contas qualificadas e extrai a classe de ativo solicitada de cada uma,
        retornando um único DataFrame concatenado.

        Args:
            asset_class: Nome da classe de ativo (ver get_position_by_asset_class).
            skip_funds: Se True (padrão), ignora contas com typeFund=true.

        Returns:
            DataFrame concatenado com todas as posições da classe especificada,
            com coluna "_accountNumber" para rastreamento.

        Example:
            >>> rv = btg.get_all_positions_by_asset_class("Equities")
            >>> fi = btg.get_all_positions_by_asset_class("FixedIncome")
        """
        self._validate_asset_class(asset_class)
        accounts_df = self.get_accounts()
        dfs: List[pd.DataFrame] = []

        for idx, account in enumerate(self._iter_accounts(accounts_df, skip_funds=skip_funds)):
            acc_number = account.get("accountNumber") or account.get("account_number")
            if not acc_number:
                continue
            if idx > 0 and self.request_delay > 0:
                time.sleep(self.request_delay)
            try:
                logger.info("Fetching %s for account %s …", asset_class, acc_number)
                df = self.get_position_by_asset_class(acc_number, asset_class)
                if not df.empty:
                    dfs.append(df)
            except DataRetrievalError as exc:
                logger.warning("Could not fetch %s for %s: %s", asset_class, acc_number, exc)

        if not dfs:
            return pd.DataFrame()

        return pd.concat(dfs, ignore_index=True)

    # ------------------------------------------------------------------
    # Deprecated aliases (backward compatibility)
    # ------------------------------------------------------------------

    def get_positions_all(self, *, skip_funds: bool = True) -> pd.DataFrame:
        warnings.warn(
            "get_positions_all() is deprecated; use get_all_positions() instead.",
            DeprecationWarning,
            stacklevel=2,
        )
        return self.get_all_positions(skip_funds=skip_funds)

    def get_position_flat(
        self,
        account_number: Union[str, int],
        asset_class: str,
    ) -> pd.DataFrame:
        warnings.warn(
            "get_position_flat() is deprecated; use get_position_by_asset_class() instead.",
            DeprecationWarning,
            stacklevel=2,
        )
        return self.get_position_by_asset_class(account_number, asset_class)

    def get_positions_all_flat(
        self,
        asset_class: str,
        *,
        skip_funds: bool = True,
    ) -> pd.DataFrame:
        warnings.warn(
            "get_positions_all_flat() is deprecated; "
            "use get_all_positions_by_asset_class() instead.",
            DeprecationWarning,
            stacklevel=2,
        )
        return self.get_all_positions_by_asset_class(asset_class, skip_funds=skip_funds)

    # ------------------------------------------------------------------
    # Internals: routing helpers
    # ------------------------------------------------------------------

    _DEPRECATED_CATEGORIES = {
        "positions_all": "all_positions",
        "position_flat": "position_by_asset_class",
        "positions_all_flat": "all_positions_by_asset_class",
    }

    @classmethod
    def _normalize_category(cls, category: str) -> str:
        alias = cls._DEPRECATED_CATEGORIES.get(category)
        if alias is None:
            return category
        warnings.warn(
            f"category='{category}' is deprecated; use '{alias}' instead.",
            DeprecationWarning,
            stacklevel=3,
        )
        return alias

    def _handle_raw(self, **kwargs) -> pd.DataFrame:
        method = kwargs.get("method", "GET").upper()
        path = kwargs.get("path")
        if not path:
            raise DataRetrievalError("category='raw' requires a 'path' kwarg.")
        params = kwargs.get("params")
        json_body = kwargs.get("json")
        base_url = kwargs.get("base_url")
        data = self._request_json(
            method=method,
            path=path,
            params=params,
            json=json_body,
            base_url=base_url,
        )
        return self._to_dataframe(data)

    # ------------------------------------------------------------------
    # Internals: asset class extraction
    # ------------------------------------------------------------------

    def _extract_asset_class(
        self,
        position_data: Any,
        asset_class: str,
        account_number: Union[str, int],
    ) -> pd.DataFrame:
        """
        Extrai e normaliza uma classe de ativo do payload de posição do BTG.

        Cada classe tem uma estrutura levemente diferente no JSON do BTG:
          - Equities        → lista de grupos, cada um com StockPositions (lista)
          - InvestmentFund  → lista direta de fundos (cada item tem Fund + Acquisition)
          - Derivative      → lista de grupos, cada um com subchaves (BMFFuturePosition, etc.)
          - Demais          → lista direta de itens
        """
        raw = position_data.get(asset_class) if isinstance(position_data, dict) else None
        if not raw or not isinstance(raw, list):
            return pd.DataFrame()

        rows: List[Dict[str, Any]] = []

        if asset_class == "Equities":
            for group in raw:
                stock_positions = group.get("StockPositions") or []
                for stock in stock_positions:
                    rows.append(stock)

        elif asset_class == "InvestmentFund":
            # Cada item tem {Fund: {...}, Acquisition: [...], ShareValue, PositionDate, …}
            for item in raw:
                fund_info = item.get("Fund", {})
                row: Dict[str, Any] = {**fund_info}
                row["ShareValue"] = item.get("ShareValue")
                row["PositionDate"] = item.get("PositionDate")
                # Achata a primeira aquisição (ou agrega se houver mais de uma)
                acquisitions = item.get("Acquisition") or []
                if acquisitions:
                    first_acq = acquisitions[0]
                    for k, v in first_acq.items():
                        row[f"Acquisition_{k}"] = v
                rows.append(row)

        elif asset_class == "Derivative":
            sub_keys = (
                "BMFFuturePosition",
                "BMFOptionPosition",
                "SwapPosition",
                "NDFPosition",
                "CetipOptionPosition",
            )
            for group in raw:
                for sub_key in sub_keys:
                    items = group.get(sub_key) or []
                    if not isinstance(items, list):
                        continue
                    for item in items:
                        row = dict(item)
                        row["_DerivativeType"] = sub_key
                        rows.append(row)

        else:
            # Estrutura direta: SummaryAccounts, FixedIncome, PensionInformations,
            # Precatories, FixedIncomeStructuredNote, CryptoCoins, Commodity
            rows = [item for item in raw if isinstance(item, dict)]

        if not rows:
            return pd.DataFrame()

        df = pd.json_normalize(rows)
        df["_accountNumber"] = str(account_number)
        return df

    # ------------------------------------------------------------------
    # Internals: auth
    # ------------------------------------------------------------------

    def _ensure_token(self) -> None:
        """Renova o token se expirado ou ausente (margem de 30s)."""
        if self._access_token and time.time() < self._access_token_expiry_epoch - 30:
            return
        self._refresh_token()

    def _refresh_token(self) -> None:
        """
        Obtém novo access_token via OAuth2 client_credentials com HTTP Basic auth.

        O BTG usa Basic auth no header (base64(client_id:client_secret)) em vez
        do Azure AD client_credentials do XP.
        """
        token_url = f"{self.auth_base_url}{_AUTH_TOKEN_PATH}"
        credentials = f"{self.client_id}:{self.client_secret}"
        encoded = base64.b64encode(credentials.encode()).decode()

        headers = {
            "Content-Type": "application/x-www-form-urlencoded",
            "Authorization": f"Basic {encoded}",
            "x-id-partner-request": self._partner_request_id(),
        }
        payload = {"grant_type": "client_credentials"}

        try:
            logger.info("Requesting BTG access token at %s", token_url)
            resp = requests.post(
                token_url,
                data=payload,
                headers=headers,
                timeout=self.timeout_seconds,
                verify=self.verify_ssl,
            )
            resp.raise_for_status()
        except requests.RequestException as exc:
            detail = ""
            try:
                if hasattr(exc, "response") and exc.response is not None:
                    detail = f" | body: {exc.response.text}"
            except Exception:
                pass
            raise DataRetrievalError(f"BTG auth failed: {exc}{detail}")

        access_token, expires_in = self._parse_auth_token_response(resp)

        if not access_token:
            body_preview = (resp.text or "")[:500]
            raise DataRetrievalError(
                "BTG auth response missing access_token "
                f"(status={resp.status_code}, body={body_preview!r})"
            )

        self._access_token = access_token
        self._access_token_expiry_epoch = time.time() + int(expires_in)
        logger.info("BTG access token acquired (expires in %ds).", expires_in)

    @staticmethod
    def _parse_auth_token_response(resp: requests.Response) -> tuple[Optional[str], int]:
        """
        Extrai access_token da resposta de auth do BTG MFO.

        A API pode devolver o token no header ``access_token`` (corpo vazio) ou
        no JSON ``{"access_token": ..., "expires_in": ...}``.
        """
        header_token = resp.headers.get("access_token")
        if header_token:
            return header_token, _TOKEN_DEFAULT_EXPIRES_SECONDS

        if not resp.content:
            return None, _TOKEN_DEFAULT_EXPIRES_SECONDS

        try:
            data = resp.json()
        except ValueError:
            return None, _TOKEN_DEFAULT_EXPIRES_SECONDS

        if not isinstance(data, dict):
            return None, _TOKEN_DEFAULT_EXPIRES_SECONDS

        access_token = data.get("access_token")
        expires_in = data.get("expires_in", _TOKEN_DEFAULT_EXPIRES_SECONDS)
        return access_token, int(expires_in)

    def _build_headers(self) -> Dict[str, str]:
        """Monta headers para chamadas à API (inclui request-id único)."""
        self._ensure_token()
        return {
            "access_token": self._access_token,  # BTG usa access_token no header, não Bearer
            "x-id-partner-request": self._partner_request_id(),
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

    # ------------------------------------------------------------------
    # Internals: HTTP
    # ------------------------------------------------------------------

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

        token_preview = (self._access_token or "")[:20] + "..."
        logger.info("BTG Request: %s %s", method, url)
        logger.debug("BTG access_token: %s", token_preview)
        if params:
            logger.debug("BTG Params: %s", params)

        last_exc: Optional[Exception] = None
        resp: Optional[requests.Response] = None

        for attempt in range(self.max_retries):
            headers = self._build_headers()
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
            except requests.HTTPError as exc:
                last_exc = exc
                status = resp.status_code if resp is not None else None
                if (
                    status in _RETRY_STATUS_CODES
                    and attempt < self.max_retries - 1
                ):
                    wait = self._retry_wait_seconds(attempt, status, resp)
                    logger.warning(
                        "BTG HTTP %s em %s (tentativa %d/%d); "
                        "aguardando %.0fs antes de tentar novamente.",
                        status,
                        url,
                        attempt + 1,
                        self.max_retries,
                        wait,
                    )
                    time.sleep(wait)
                    continue
                detail = ""
                try:
                    if resp is not None:
                        detail = f" | body: {resp.text}"
                except Exception:
                    pass
                raise DataRetrievalError(
                    f"BTG request failed [{method} {url}] {exc}{detail}"
                ) from exc
            except requests.RequestException as exc:
                raise DataRetrievalError(f"BTG request error [{method} {url}]: {exc}") from exc

        raise DataRetrievalError(
            f"BTG request failed após {self.max_retries} tentativas "
            f"[{method} {url}]: {last_exc}"
        ) from last_exc

    def _retry_wait_seconds(
        self,
        attempt: int,
        status: Optional[int],
        resp: Optional[requests.Response],
    ) -> float:
        if resp is not None:
            retry_after = resp.headers.get("Retry-After")
            if retry_after:
                try:
                    return max(float(retry_after), self.retry_backoff)
                except ValueError:
                    pass

        wait = self.retry_backoff * (2 ** attempt)
        if status == 429:
            return max(wait, _MIN_429_BACKOFF_SECONDS)
        return wait

    def _build_url(self, path: str, *, base_url: Optional[str] = None) -> str:
        if path.startswith("http://") or path.startswith("https://"):
            return path
        selected_base = (base_url or self.positions_base_url or "").rstrip("/")
        if not selected_base:
            raise DataRetrievalError("No base URL configured for BTG request.")
        if not path.startswith("/"):
            path = f"/{path}"
        return f"{selected_base}{path}"

    # ------------------------------------------------------------------
    # Internals: utilities
    # ------------------------------------------------------------------

    @staticmethod
    def _partner_request_id() -> str:
        return str(uuid.uuid4())

    def _validate_credentials(self) -> None:
        missing = []
        if not self.client_id:
            missing.append("PERSEVERA_BTGWS_CLIENT_ID")
        if not self.client_secret:
            missing.append("PERSEVERA_BTGWS_CLIENT_SECRET")
        if missing:
            raise ValueError("Missing BTG configuration variables: " + ", ".join(missing))

    @staticmethod
    def _validate_asset_class(asset_class: str) -> None:
        if asset_class not in ASSET_CLASSES:
            raise DataRetrievalError(
                f"Unknown asset_class '{asset_class}'. "
                f"Valid options: {', '.join(ASSET_CLASSES)}."
            )

    @staticmethod
    def _require_kwarg(kwargs: Dict[str, Any], key: str, category: str) -> Any:
        value = kwargs.get(key)
        if value is None:
            raise DataRetrievalError(f"category='{category}' requires kwarg '{key}'.")
        return value

    @staticmethod
    def _iter_accounts(
        accounts_df: pd.DataFrame,
        *,
        skip_funds: bool,
    ) -> Iterator[Dict[str, Any]]:
        """
        Itera sobre as contas do DataFrame, opcionalmente filtrando contas de fundo.

        typeFund vem como string "true"/"false" na API do BTG.
        """
        for row in accounts_df.to_dict(orient="records"):
            type_fund_raw = row.get("typeFund", "false")
            is_fund = str(type_fund_raw).lower() in ("true", "1", "yes")
            if skip_funds and is_fund:
                logger.debug("Skipping fund account %s", row.get("accountNumber"))
                continue
            yield row

    @staticmethod
    def _to_dataframe(payload: Any) -> pd.DataFrame:
        """
        Converte um payload JSON do BTG em DataFrame.

        Trata os padrões mais comuns:
          - Lista direta de objetos
          - {'data': [...]} / {'accounts': [...]} / {'items': [...]}
          - Objeto único (1 linha)
        """
        if payload is None:
            return pd.DataFrame()
        if isinstance(payload, list):
            return pd.json_normalize(payload) if payload else pd.DataFrame()
        if isinstance(payload, dict):
            for key in ("data", "accounts", "items", "results", "content"):
                if key in payload and isinstance(payload[key], list):
                    items = payload[key]
                    return pd.json_normalize(items) if items else pd.DataFrame()
            return pd.json_normalize([payload])
        return pd.DataFrame()