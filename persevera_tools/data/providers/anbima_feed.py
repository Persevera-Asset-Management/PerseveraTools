"""
ANBIMA Feed API provider with OAuth2 authentication.

Uses client_credentials flow per ANBIMA documentation:
https://developers.anbima.com.br/pt/documentacao/visao-geral/autenticacao/#oauth2

Production: https://api.anbima.com.br
Sandbox: https://api-sandbox.anbima.com.br (hyphen; cert is for this hostname)
"""

import base64
import time
from typing import Any, Dict, List, Optional

import pandas as pd
import requests

from .base import DataProvider, DataRetrievalError
from ...config import settings
from ...utils.logging import get_logger

logger = get_logger(__name__)

# Default base URLs (documentation)
ANBIMA_FEED_PRODUCTION = "https://api.anbima.com.br"
ANBIMA_FEED_SANDBOX = "https://api-sandbox.anbima.com.br"
OAUTH_PATH = "/oauth/access-token"
FEED_BASE_PATH = "/feed/precos-indices"

# Map category -> (path_suffix, date_param_name)
# path_suffix includes version (v1/ or v2/) and is appended to FEED_BASE_PATH
FEED_ENDPOINTS: Dict[str, tuple] = {
    # --- Títulos Públicos (v1) ---
    "anbima_feed_titulos_publicos_mercado_secundario": (
        "v1/titulos-publicos/mercado-secundario-TPF",
        "data",
    ),
    "anbima_feed_titulos_publicos_vna": ("v1/titulos-publicos/vna", "data"),
    "anbima_feed_titulos_publicos_curvas_juros": (
        "v1/titulos-publicos/curvas-juros",
        "data",
    ),
    "anbima_feed_titulos_publicos_curva_intradiaria": (
        "v1/titulos-publicos/curva-intradiaria",
        "data",
    ),
    "anbima_feed_titulos_publicos_pu_intradiario": (
        "v1/titulos-publicos/pu-intradiario",
        "data",
    ),
    "anbima_feed_titulos_publicos_difusao_taxas": (
        "v1/titulos-publicos/difusao-taxas",
        "data",
    ),
    "anbima_feed_titulos_publicos_estimativa_selic": (
        "v1/titulos-publicos/estimativa-selic",
        "data",
    ),
    "anbima_feed_titulos_publicos_projecoes": (
        "v1/titulos-publicos/projecoes",
        None,
    ),
    # --- Debêntures (v1) ---
    "anbima_feed_debentures_mercado_secundario": (
        "v1/debentures/mercado-secundario",
        "data",
    ),
    "anbima_feed_debentures_curvas_credito": (
        "v1/debentures/curvas-credito",
        "data",
    ),
    "anbima_feed_debentures_projecoes": ("v1/debentures/projecoes", None),
    # --- Debêntures+ (v1) ---
    "anbima_feed_debentures_mais_mercado_secundario": (
        "v1/debentures-mais/mercado-secundario",
        "data",
    ),
    # --- CRI/CRA (v1) ---
    "anbima_feed_cri_cra_mercado_secundario": (
        "v1/cri-cra/mercado-secundario",
        "data",
    ),
    "anbima_feed_cri_cra_projecoes": ("v1/cri-cra/projecoes", None),
    # --- FIDC (v1) ---
    "anbima_feed_fidc_mercado_secundario": (
        "v1/fidc/mercado-secundario",
        "data",
    ),
    # --- Letras Financeiras (v1) ---
    "anbima_feed_letras_financeiras_matrizes_vertices": (
        "v1/letras-financeiras/matrizes-vertices-emissor",
        "data",
    ),
    # --- REUNE (v1) - requer instrumento= (debenture, cra, cri, cff) ---
    "anbima_feed_reune_previas": ("v1/reune/previas-do-reune", "data"),
    # --- Índices (v1) ---
    "anbima_feed_indices_carteira_teorica_ida": (
        "v1/indices/carteira-teorica-ida",
        None,
    ),
    "anbima_feed_indices_resultados_ida_fechado": (
        "v1/indices/resultados-ida-fechado",
        "data",
    ),
    "anbima_feed_indices_carteira_teorica_ihfa": (
        "v1/indices/carteira-teorica-ihfa",
        None,
    ),
    "anbima_feed_indices_resultados_ihfa_fechado": (
        "v1/indices/resultados-ihfa-fechado",
        "data",
    ),
    "anbima_feed_indices_resultados_idka": (
        "v1/indices/resultados-idka",
        "data",
    ),
    "anbima_feed_indices_carteira_teorica_ima": (
        "v1/indices/carteira-teorica-ima",
        None,
    ),
    "anbima_feed_indices_resultados_ima": (
        "v1/indices/resultados-ima",
        "data",
    ),
    "anbima_feed_indices_resultados_intradiarios_ima": (
        "v1/indices/resultados-intradiarios-ima",
        "data",
    ),
    # --- Índices v2 (IHFA RCVM 175) ---
    "anbima_feed_indices_carteira_teorica_ihfa_v2": (
        "v2/indices/carteira-teorica-ihfa",
        None,
    ),
    # --- Índices+ (v1) ---
    "anbima_feed_indices_mais_previa_carteira_ida": (
        "v1/indices-mais/previa-carteira-teorica-ida",
        None,
    ),
    "anbima_feed_indices_mais_carteira_teorica_ida": (
        "v1/indices-mais/carteira-teorica-ida",
        None,
    ),
    "anbima_feed_indices_mais_resultados_ida": (
        "v1/indices-mais/resultados-ida",
        "data",
    ),
    "anbima_feed_indices_mais_carteira_teorica_ihfa": (
        "v1/indices-mais/carteira-teorica-ihfa",
        None,
    ),
    "anbima_feed_indices_mais_resultados_ihfa": (
        "v1/indices-mais/resultados-ihfa",
        "data",
    ),
    "anbima_feed_indices_mais_resultados_idka": (
        "v1/indices-mais/resultados-idka",
        "data",
    ),
    "anbima_feed_indices_mais_previa_carteira_ima": (
        "v1/indices-mais/previa-carteira-teorica-ima",
        None,
    ),
    "anbima_feed_indices_mais_carteira_teorica_ima": (
        "v1/indices-mais/carteira-teorica-ima",
        None,
    ),
    "anbima_feed_indices_mais_resultados_ima": (
        "v1/indices-mais/resultados-ima",
        "data",
    ),
    "anbima_feed_indices_mais_resultados_intradiarios_ima": (
        "v1/indices-mais/resultados-intradiarios-ima",
        "data",
    ),
    # --- Índices+ v2 (IHFA/resultados RCVM 175) ---
    "anbima_feed_indices_mais_carteira_teorica_ihfa_v2": (
        "v2/indices-mais/carteira-teorica-ihfa",
        None,
    ),
    "anbima_feed_indices_mais_resultados_ihfa_v2": (
        "v2/indices-mais/resultados-ihfa",
        "data",
    ),
    # --- IDA LIQ (v1) ---
    "anbima_feed_ida_liq_previa_carteira": (
        "v1/ida-liq/previa-carteira-teorica-ida-liq",
        None,
    ),
    "anbima_feed_ida_liq_carteira_teorica": (
        "v1/ida-liq/carteira-teorica-ida",
        None,
    ),
    "anbima_feed_ida_liq_resultados": (
        "v1/ida-liq/resultados-ida",
        "data",
    ),
    # --- IMA para ETFs (v2) ---
    "anbima_feed_ima_etf_previa_carteira": (
        "v2/ima-etf/previa-carteira-teorica",
        None,
    ),
    "anbima_feed_ima_etf_carteira_teorica": (
        "v2/ima-etf/carteira-teorica",
        None,
    ),
    "anbima_feed_ima_etf_resultado_diario": (
        "v2/ima-etf/resultado-diario",
        "data",
    ),
    "anbima_feed_ima_etf_composicao_diaria": (
        "v2/ima-etf/composicao-diaria",
        "data",
    ),
    "anbima_feed_ima_etf_resultado_intradiario": (
        "v2/ima-etf/resultado-intradiario",
        "data",
    ),
    "anbima_feed_ima_etf_pu_intradiario": (
        "v2/ima-etf/pu-intradiario",
        "data",
    ),
    "anbima_feed_ima_etf_negocios_extra": (
        "v2/ima-etf/negocios-extra",
        "data",
    ),
}


class AnbimaFeedProvider(DataProvider):
    """
    Provider for ANBIMA Feed API (OAuth2).

    Requires PERSEVERA_ANBIMA_FEED_CLIENT_ID and PERSEVERA_ANBIMA_FEED_CLIENT_SECRET.
    Padrão é sandbox (true). Use PERSEVERA_ANBIMA_FEED_SANDBOX=false para produção (requer acesso liberado pela ANBIMA).

    Categorias disponíveis: Títulos Públicos, Debêntures, Debêntures+, CRI/CRA, FIDC,
    Letras Financeiras, REUNE, Índices, Índices+, IDA LIQ, IMA para ETFs.
    Para anbima_feed_reune_previas é obrigatório passar instrumento= ('debenture', 'cra', 'cri' ou 'cff').
    Endpoints com mes/ano: passe mes= e ano= em kwargs. IMA ETF aceita etf= (ex: 'IMA_B5_MAIS,IRF_M_P2').
    """

    def __init__(self, start_date: str = "1980-01-01", sandbox: Optional[bool] = None):
        super().__init__(start_date)
        self._client_id = getattr(settings, "ANBIMA_FEED_CLIENT_ID", None)
        self._client_secret = getattr(settings, "ANBIMA_FEED_CLIENT_SECRET", None)
        if sandbox is not None:
            self._sandbox = sandbox
        else:
            sandbox_env = getattr(settings, "ANBIMA_FEED_SANDBOX", None)
            self._sandbox = str(sandbox_env).lower() in ("1", "true", "yes")
        base_url = getattr(settings, "ANBIMA_FEED_BASE_URL", None)
        self._base_url = (base_url or (ANBIMA_FEED_SANDBOX if self._sandbox else ANBIMA_FEED_PRODUCTION)).rstrip("/")
        # OAuth endpoint exists only on production; token is valid for both prod and sandbox
        self._oauth_base_url = ANBIMA_FEED_PRODUCTION.rstrip("/")
        self._access_token: Optional[str] = None
        self._token_expires_at: float = 0.0
        self._buffer_seconds = 60

    def _ensure_credentials(self) -> None:
        if not self._client_id or not self._client_secret:
            raise DataRetrievalError(
                "ANBIMA Feed requires PERSEVERA_ANBIMA_FEED_CLIENT_ID and "
                "PERSEVERA_ANBIMA_FEED_CLIENT_SECRET in settings/env."
            )

    def _get_access_token(self) -> str:
        self._ensure_credentials()
        now = time.time()
        if self._access_token and now < self._token_expires_at:
            return self._access_token
        url = f"{self._oauth_base_url}{OAUTH_PATH}"
        credentials = f"{self._client_id}:{self._client_secret}"
        b64 = base64.b64encode(credentials.encode("utf-8")).decode("ascii")
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Basic {b64}",
        }
        payload = {"grant_type": "client_credentials"}
        try:
            resp = requests.post(url, headers=headers, json=payload, timeout=30)
            resp.raise_for_status()
            data = resp.json()
        except requests.exceptions.RequestException as e:
            logger.error("ANBIMA Feed OAuth request failed: %s", e)
            raise DataRetrievalError(f"ANBIMA Feed OAuth failed: {e}") from e
        token = data.get("access_token")
        expires_in = int(data.get("expires_in", 3600))
        if not token:
            raise DataRetrievalError(
                "ANBIMA Feed OAuth response did not contain access_token"
            )
        self._access_token = token
        self._token_expires_at = now + expires_in - self._buffer_seconds
        logger.debug("ANBIMA Feed access token obtained, expires in %ss", expires_in)
        return token

    def _auth_headers(self) -> Dict[str, str]:
        token = self._get_access_token()
        return {
            "Content-Type": "application/json",
            "client_id": self._client_id,
            "access_token": token,
            "Authorization": f"Bearer {token}",
        }

    def _get(self, path: str, params: Optional[Dict[str, Any]] = None) -> Any:
        url = f"{self._base_url}{path}"
        try:
            resp = requests.get(
                url,
                headers=self._auth_headers(),
                params=params,
                timeout=60,
            )
            resp.raise_for_status()
            return resp.json()
        except requests.exceptions.HTTPError as e:
            if e.response is not None:
                if e.response.status_code == 401:
                    self._access_token = None
                    self._token_expires_at = 0.0
                    msg = (
                        e.response.text[:500]
                        + " | Confira: use credenciais de produção (sem SANDBOX) para "
                        "api.anbima.com.br; credenciais de sandbox para api-sandbox.anbima.com.br."
                    )
                else:
                    msg = e.response.text[:500]
                logger.error("ANBIMA Feed API error %s: %s", e.response.status_code, msg)
            raise DataRetrievalError(f"ANBIMA Feed API request failed: {e}") from e
        except requests.exceptions.RequestException as e:
            logger.error("ANBIMA Feed request failed: %s", e)
            raise DataRetrievalError(f"ANBIMA Feed request failed: {e}") from e

    def _json_to_long(
        self,
        data: Any,
        date_col: str = "data_referencia",
        code_col: Optional[str] = "codigo_selic",
        value_columns: Optional[List[str]] = None,
    ) -> pd.DataFrame:
        """
        Convert API JSON (list of dicts or nested structure) to long format
        [date, code, field, value].
        """
        if isinstance(data, dict) and "data" in data:
            rows = data["data"]
        elif isinstance(data, list):
            rows = data
        else:
            rows = [data] if isinstance(data, dict) else []

        if not rows:
            return pd.DataFrame(columns=["date", "code", "field", "value"])

        df = pd.DataFrame(rows)
        # Normalize column names (API may use snake_case)
        df.columns = [str(c).strip().lower().replace(" ", "_") for c in df.columns]

        date_col_lower = date_col.lower().replace(" ", "_")
        code_col_lower = (code_col or "code").lower().replace(" ", "_")

        id_cols = []
        if date_col_lower in df.columns:
            id_cols.append(date_col_lower)
        if code_col_lower in df.columns:
            id_cols.append(code_col_lower)

        # Use first column as code if no code column
        if not id_cols and len(df.columns) > 0:
            id_cols = [df.columns[0]]

        value_cols = value_columns
        if value_cols is None:
            value_cols = [c for c in df.columns if c not in id_cols]
        else:
            value_cols = [c.lower().replace(" ", "_") for c in value_cols]
            value_cols = [c for c in value_cols if c in df.columns]

        out = df.melt(
            id_vars=[c for c in id_cols if c in df.columns],
            value_vars=value_cols,
            var_name="field",
            value_name="value",
        )
        out = out.dropna(subset=["value"])
        out["value"] = pd.to_numeric(out["value"], errors="coerce")
        out = out.dropna(subset=["value"])

        rename = {}
        if date_col_lower in out.columns:
            rename[date_col_lower] = "date"
        if code_col_lower in out.columns:
            rename[code_col_lower] = "code"
        if rename:
            out = out.rename(columns=rename)

        if "date" not in out.columns and date_col_lower in df.columns:
            out["date"] = df[date_col_lower].iloc[0]
        if "code" not in out.columns:
            out["code"] = "series"

        out["date"] = pd.to_datetime(out["date"], errors="coerce")
        out = out.dropna(subset=["date"])
        return out[["date", "code", "field", "value"]]

    def get_data(self, category: str, **kwargs) -> pd.DataFrame:
        self._ensure_credentials()
        self._log_processing(category)

        if category not in FEED_ENDPOINTS:
            raise ValueError(
                f"Unknown category: {category}. "
                f"Supported: {list(FEED_ENDPOINTS.keys())}"
            )

        path_suffix, date_param = FEED_ENDPOINTS[category]
        path = f"{FEED_BASE_PATH}/{path_suffix}"

        params: Dict[str, Any] = {}
        if date_param:
            if kwargs.get("data"):
                params[date_param] = kwargs["data"]
            else:
                params[date_param] = self.start_date.strftime("%Y-%m-%d")
        if kwargs.get("mes") is not None:
            params["mes"] = kwargs["mes"]
        if kwargs.get("ano") is not None:
            params["ano"] = kwargs["ano"]
        # REUNE: instrumento obrigatório (debenture, cra, cri, cff); faixa opcional (11:00, 13:00, 16:00, 18:00)
        if kwargs.get("instrumento") is not None:
            params["instrumento"] = kwargs["instrumento"]
        if kwargs.get("faixa") is not None:
            params["faixa"] = kwargs["faixa"]
        # IMA ETF: etf opcional (ex: IMA_B5_MAIS,IRF_M_P2)
        if kwargs.get("etf") is not None:
            params["etf"] = kwargs["etf"]

        raw = self._get(path, params=params or None)
        df = pd.DataFrame(raw)
        # df = self._json_to_long(raw)
        if df.empty:
            return self._validate_output(
                pd.DataFrame(columns=["date", "code", "field", "value"])
            )
        # return self._validate_output(df)
        return df
