import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
import json

from .base import DataProvider, DataRetrievalError


class B3Provider(DataProvider):
    """
    Provider for B3 public data.
    This provider fetches data from the B3 website, such as investor participation.
    """

    def __init__(self):
        super().__init__()
        # TODO: The cookie is hardcoded. This is not secure and might expire.
        # It should be loaded from a configuration file or environment variables.
        self.headers = {
            "cookie": "OptanonAlertBoxClosed=2023-07-07T15:16:22.847Z; _tt_enable_cookie=1; _ttp=cVRpV3NarQvTUmAaOwtcxMifqbw; _ga_X5KRPBP7ZE=GS1.3.1691027075.10.1.1691027077.58.0.0; _gcl_au=1.1.1979806928.1697752562; _gcl_aw=GCL.1699284414.Cj0KCQiAuqKqBhDxARIsAFZELmJXzW-qaqV1A2YbcSfw7OGGeQItLjuhf6ymDDbIP1m3TWM3TNkVolEaArccEALw_wcB; visid_incap_2246223=mOEsoG/mReOektPLF7Flg9LXS2UAAAAAQUIPAAAAAABFNjwU7AmF+m2SYYSlTWeR; _ga_5E2DT9ZLVR=GS1.1.1701447660.18.0.1701447660.60.0.0; _ga_FTT9L7SR7B=GS1.1.1703077550.5.1.1703077571.0.0.0; _ga_0W7NXV5699=GS1.1.1703077587.1.1.1703078621.0.0.0; _ga_CNJN5WQC5G=GS1.1.1703095648.78.0.1703095648.0.0.0; nlbi_2246223=X5+1DkgzpBk2tnz89OkOmwAAAAB2P/CZgiSEWqgElG2ZWJTp; incap_ses_1453_2246223=RhrkXXFANUN2rulBcBgqFBGHjGUAAAAAjYYT3UxwAqerr7ZndpK8OA==; auth0=; _gid=GA1.3.114354419.1703708463; dtCookie=v_4_srv_27_sn_6D3A238BEFE62944CB96090E567E47B2_perc_100000_ol_0_mul_1_app-3Afd69ce40c52bd20e_1_rcs-3Acss_0; TS0171d45d=011d592ce117d5abfcf465c12d7cdc5eaeedecd840ad6981a6d0d894ea2e58902d7b5ebcee5fc244857cafa365806d01c3f0a40cb7; rxVisitor=1703708483603TOSTBHPLPCJKG580SVT3VPE6BNT46JIT; _clck=1nu3mku%7C2%7Cfhw%7C0%7C1283; dtSa=-; _ga=GA1.3.172599087.1659060995; OptanonConsent=isGpcEnabled=0&datestamp=Wed+Dec+27+2023+17%3A24%3A25+GMT-0300+(Hor%C3%A1rio+Padr%C3%A3o+de+Bras%C3%ADlia)&version=6.21.0&isIABGlobal=false&hosts=&landingPath=NotLandingPage&groups=C0003%3A1%2CC0001%3A1%2CC0004%3A1%2CC0002%3A1&geolocation=%3B&AwaitingReconsent=false; _clsk=mp7te1%7C1703708666564%7C5%7C1%7Cz.clarity.ms%2Fcollect; _ga_SS7FXRTPP3=GS1.1.1703708496.249.1.1703708666.37.0.0; rxvt=1703710472976|1703708483611; dtPC=27$308664692_982h-vFTTUTBHBVHQBKRHMNOUFOCKULEPFRFWR-0e0",
            "authority": "arquivos.b3.com.br",
            "accept": "*/*",
            "accept-language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
            "content-type": "application/json",
            "origin": "https://arquivos.b3.com.br",
            "sec-ch-ua-mobile": "?0",
            "sec-fetch-dest": "empty",
            "sec-fetch-mode": "cors",
            "sec-fetch-site": "same-origin",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        }
        self.investor_type_map = {
            "Institucionais": "institutional_investors",
            "Instituições Financeiras": "financial_institutions",
            "Investidor Estrangeiro": "foreign_investors",
            "Investidores Individuais": "individual_investors",
            "Outros": "others",
        }
        self.api_map = {"investors_participation": "SharesInvesVolum"}

    def _fetch_day(self, dt: datetime, category: str) -> pd.DataFrame:
        """Fetch data for a single day from the B3 API."""
        dt_str = format(dt, "%Y-%m-%d")
        api_path = self.api_map.get(category)
        if not api_path:
            self.logger.warning(f"Category '{category}' not supported by B3Provider.")
            return pd.DataFrame()

        url = f"https://arquivos.b3.com.br/bdi/table/{api_path}/{dt_str}/{dt_str}/1/100"

        try:
            r = requests.post(url, json={}, headers=self.headers, timeout=30)
            r.raise_for_status()
            j = r.json()

            if not j["table"]["values"]:
                return pd.DataFrame()

            date_of_data = pd.to_datetime(
                j["table"]["texts"][1].get("textPt")[-10:], format="%d/%m/%Y"
            )
            date_of_data_str = date_of_data.strftime("%Y-%m-%d")

            self.logger.info(f"Successfully fetched B3 data for {date_of_data_str}")

            headers_en = [
                "investorTypes",
                "purchases",
                "purchases_part",
                "sales",
                "sales_part",
            ]
            temp = pd.DataFrame(j["table"]["values"]).dropna(axis="columns")
            temp.columns = headers_en
            temp["investorTypes"] = temp["investorTypes"].replace(self.investor_type_map)

            long_df = temp.set_index("investorTypes").unstack().reset_index()
            long_df.columns = ["field", "code", "value"]
            long_df["date"] = date_of_data

            is_part = long_df["field"].str.contains("_part")
            long_df.loc[is_part, "value"] /= 100

            return long_df

        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 500:
                self.logger.info(f"No data for {dt_str} (server returned 500).")
            else:
                self.logger.warning(f"HTTP error for {dt_str}: {e}")
            return pd.DataFrame()
        except json.JSONDecodeError as e:
            self.logger.error(f"Failed to decode JSON for {dt_str}: {e}")
            return pd.DataFrame()
        except Exception as e:
            self.logger.error(f"Failed to fetch data for {dt_str}: {e}")
            return pd.DataFrame()

    def _fetch_data_in_range(
        self, category: str, date_range: pd.DatetimeIndex
    ) -> pd.DataFrame:
        """Fetch data concurrently over a date range."""
        all_data = []
        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = [
                executor.submit(self._fetch_day, dt, category) for dt in date_range
            ]
            for future in as_completed(futures):
                result = future.result()
                if not result.empty:
                    all_data.append(result)

        if not all_data:
            raise DataRetrievalError(
                f"No data retrieved from B3 for category '{category}'"
            )

        return pd.concat(all_data, ignore_index=True)

    def _calculate_investor_flow(self, df: pd.DataFrame) -> pd.DataFrame:
        """Calculate daily and net changes for investor flows."""
        df_flows = df[~df["field"].str.contains("_part")].copy()
        df_flows.replace(0, np.nan, inplace=True)
        df_flows.set_index("date", inplace=True)
        df_flows.sort_index(inplace=True)

        # Calculate daily change, resetting at the start of each month.
        # If change cannot be computed (e.g., first day), use the absolute value.
        df_flows["value"] = df_flows.groupby(["code", "field"])["value"].transform(
            lambda s: s.groupby(s.index.to_period("M")).diff().fillna(s)
        )
        df_flows = df_flows.loc[df_flows.index.min() + timedelta(days=1):]
        df_flows.dropna(subset=['value'], inplace=True)
        
        # Calculate net flow
        df_pivot = df_flows.pivot_table(
            index=["date", "code"], columns="field", values="value"
        ).reset_index()

        df_pivot["net"] = df_pivot["purchases"] - df_pivot["sales"]
        
        # Melt pivot table to long format
        result_df = df_pivot.melt(
            id_vars=["date", "code"],
            value_vars=["purchases", "sales", "net"],
            var_name="field_type",
            value_name="value",
        )

        return result_df

    def get_data(self, category: str, **kwargs) -> pd.DataFrame:
        """
        Retrieve data from B3. The category should be 'investors_participation'.
        """
        self._log_processing(f"B3 - {category}")

        if category == "b3":
            category = "investors_participation"
        
        periods = kwargs.get("periods", 30)
        date_range = pd.bdate_range(end=datetime.now(), periods=periods)

        raw_df = self._fetch_data_in_range(category, date_range)
        
        processed_df = self._calculate_investor_flow(raw_df)

        processed_df["code"] = (
            "br_b3_" + processed_df["code"] + "_" + processed_df["field_type"]
        )
        processed_df["field"] = "close"
        
        final_df = processed_df[["date", "code", "value", "field"]]
        
        return self._validate_output(final_df)