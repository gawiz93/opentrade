"""
Eurostat — EU external trade statistics.
Source: https://ec.europa.eu/eurostat/web/international-trade-in-goods/data/database
SDMX API: https://wikis.ec.europa.eu/display/EUROSTATHELP/API+-+Getting+started+with+statistics+API
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parents[4]))

from common.base_source import BaseSource, TradeRecord
from common.http_client import HttpClient

BASE_URL = "https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/DS-018995"

EU_COUNTRIES = [
    "DE", "FR", "IT", "ES", "NL", "BE", "PL", "SE", "AT", "CZ",
    "RO", "PT", "HU", "GR", "DK", "FI", "IE", "SK", "HR", "BG",
]

FLOW_MAP = {"1": "import", "2": "export"}


class Source(BaseSource):

    def __init__(self, config: dict, http_client=None):
        super().__init__(config)
        self.http = http_client or HttpClient(rate_limit_rps=1.0)

    def fetch_all(self):
        for country in EU_COUNTRIES[:5]:  # start with top 5
            for year in range(2020, 2024):
                yield from self._fetch(country, year)

    def fetch_updates(self, since_year: int):
        for country in EU_COUNTRIES[:5]:
            yield from self._fetch(country, since_year)

    def normalize(self, raw: dict) -> TradeRecord | None:
        try:
            return TradeRecord(
                reporter      = raw["reporter"],
                partner       = raw["partner"],
                hs_code       = raw["product"],
                year          = int(raw["year"]),
                flow          = FLOW_MAP.get(raw.get("flow", ""), "export"),
                value_usd     = int(raw.get("value", 0) or 0),
                source        = "EU/Eurostat",
            )
        except Exception:
            return None

    def _fetch(self, country: str, year: int):
        params = {
            "format": "JSON",
            "geo": country,
            "TIME_PERIOD": str(year),
            "lang": "EN",
        }
        try:
            r = self.http.get(BASE_URL, params=params)
            data = r.json()
            dims = data.get("dimension", {})
            values = data.get("value", {})

            for idx, val in values.items():
                if val is None:
                    continue
                row = {
                    "reporter": country,
                    "year": year,
                    "value": val,
                    "partner": "WLD",
                    "product": "TOTAL",
                    "flow": "1",
                }
                yield row
        except Exception as e:
            self.logger.error(f"Eurostat fetch failed {country}/{year}: {e}")
