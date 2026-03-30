"""
US Census Bureau — US import/export trade statistics.
Source: https://api.census.gov/data/timeseries/intltrade
Docs: https://www.census.gov/data/developers/data-sets/international-trade.html
"""

import os
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parents[4]))

from common.base_source import BaseSource, TradeRecord
from common.http_client import HttpClient

BASE_URL = "https://api.census.gov/data/timeseries/intltrade/exports/hs"


class Source(BaseSource):

    def __init__(self, config: dict, http_client=None):
        super().__init__(config)
        self.api_key = os.getenv("US_CENSUS_API_KEY", "")
        self.http = http_client or HttpClient(rate_limit_rps=2.0)

    def fetch_all(self):
        for year in range(2018, 2024):
            for flow, endpoint in [("X", "exports"), ("M", "imports")]:
                yield from self._fetch(year, flow, endpoint)

    def fetch_updates(self, since_year: int):
        for flow, endpoint in [("X", "exports"), ("M", "imports")]:
            yield from self._fetch(since_year, flow, endpoint)

    def normalize(self, raw: dict) -> TradeRecord | None:
        try:
            return TradeRecord(
                reporter      = "USA",
                partner       = raw.get("CTY_CODE", ""),
                hs_code       = str(raw.get("E_COMMODITY", raw.get("I_COMMODITY", ""))),
                year          = int(raw.get("YEAR", 0)),
                flow          = raw.get("_flow", "export"),
                value_usd     = int(raw.get("ALL_VAL_YR", 0) or 0),
                quantity      = raw.get("QTY_1_YR"),
                quantity_unit = raw.get("UNIT_QY1"),
                source        = "US/Census",
            )
        except Exception:
            return None

    def _fetch(self, year: int, flow: str, endpoint: str):
        url = f"https://api.census.gov/data/timeseries/intltrade/{endpoint}/hs"
        params = {
            "get": "E_COMMODITY,CTY_CODE,ALL_VAL_YR,QTY_1_YR,UNIT_QY1" if flow == "X"
                   else "I_COMMODITY,CTY_CODE,ALL_VAL_YR,QTY_1_YR,UNIT_QY1",
            "YEAR": year,
            "MONTH": "12",
            "key": self.api_key,
        }
        try:
            r = self.http.get(url, params=params)
            rows = r.json()
            headers = rows[0]
            for row in rows[1:]:
                record = dict(zip(headers, row))
                record["_flow"] = "export" if flow == "X" else "import"
                yield record
        except Exception as e:
            self.logger.error(f"US Census fetch failed {year}/{flow}: {e}")
