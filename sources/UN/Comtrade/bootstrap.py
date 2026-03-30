"""
UN Comtrade — global trade flows.
Source: https://comtradeapi.un.org/
"""

import os
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parents[4]))

from common.base_source import BaseSource, TradeRecord
from common.http_client import HttpClient

BASE_URL = "https://comtradeapi.un.org/data/v1/get"

# Comtrade numeric code → ISO3
REPORTER_MAP = {
    "842": "USA", "156": "CHN", "276": "DEU", "826": "GBR", "392": "JPN",
    "356": "IND", "251": "FRA",  "76": "BRA", "124": "CAN", "410": "KOR",
    "36":  "AUS", "484": "MEX", "528": "NLD", "702": "SGP", "381": "ITA",
    "724": "ESP", "756": "CHE", "752": "SWE", "578": "NOR", "208": "DNK",
    "246": "FIN", "616": "POL", "203": "CZE", "40":  "AUT", "348": "HUN",
    "642": "ROU", "56":  "BEL", "300": "GRC", "191": "HRV", "620": "PRT",
    "792": "TUR", "643": "RUS", "764": "THA", "458": "MYS", "360": "IDN",
    "704": "VNM", "682": "SAU", "784": "ARE", "818": "EGY", "710": "ZAF",
    "566": "NGA", "404": "KEN", "12":  "DZA", "504": "MAR",
    "0":   "WLD",   # world total
}

ISO3_TO_COMTRADE = {v: k for k, v in REPORTER_MAP.items()}


class Source(BaseSource):

    def __init__(self, config: dict, http_client=None):
        super().__init__(config)
        api_key = os.getenv("COMTRADE_API_KEY", "")
        headers = {}
        if api_key:
            headers["Ocp-Apim-Subscription-Key"] = api_key
        self.http = http_client or HttpClient(
            rate_limit_rps=float(config.get("rate_limit_rps", 0.8)),
            headers=headers,
        )

    def fetch_all(self):
        """Fetch trade flows for all priority countries, all recent years."""
        for reporter in list(ISO3_TO_COMTRADE.keys())[:5]:  # start with top 5
            for year in range(2020, 2024):
                for flow in ["X", "M"]:
                    yield from self._fetch_page(reporter, year, flow)

    def fetch_updates(self, since_year: int):
        for reporter in list(ISO3_TO_COMTRADE.keys())[:5]:
            for flow in ["X", "M"]:
                yield from self._fetch_page(reporter, since_year, flow)

    def normalize(self, raw: dict) -> TradeRecord | None:
        try:
            return TradeRecord(
                reporter      = raw.get("_reporter_iso3", ""),
                partner       = REPORTER_MAP.get(str(raw.get("partnerCode", "")), str(raw.get("partnerCode", ""))),
                hs_code       = str(raw.get("cmdCode", "")).strip(),
                year          = int(raw.get("period", 0)),
                flow          = "export" if raw.get("_flow") == "X" else "import",
                value_usd     = int(raw.get("primaryValue") or 0),
                quantity      = raw.get("qty"),
                quantity_unit = raw.get("qtyUnitAbbr"),
                source        = "UN/Comtrade",
                source_id     = str(raw.get("refYear", "")) + str(raw.get("refMonth", "")),
            )
        except Exception:
            return None

    def _fetch_page(self, reporter_iso3: str, year: int, flow: str):
        reporter_code = ISO3_TO_COMTRADE.get(reporter_iso3, reporter_iso3)
        params = {
            "typeCode": "C", "freqCode": "A", "clCode": "HS",
            "period": str(year),
            "reporterCode": reporter_code,
            "partnerCode": "0",  # world total
            "cmdCode": "TOTAL",
            "flowCode": flow,
            "maxRecords": 500,
            "format": "JSON",
            "breakdownMode": "classic",
        }
        try:
            r = self.http.get(BASE_URL, params=params)
            for row in r.json().get("data", []):
                row["_reporter_iso3"] = reporter_iso3
                row["_flow"] = flow
                yield row
        except Exception as e:
            self.logger.error(f"Comtrade fetch failed {reporter_iso3}/{year}/{flow}: {e}")
