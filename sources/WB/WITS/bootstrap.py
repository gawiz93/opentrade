"""
World Bank WITS — trade flows + tariff rates.
Source: https://wits.worldbank.org/
No API key required.
"""

import sys
import xml.etree.ElementTree as ET
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parents[4]))

from common.base_source import BaseSource, TradeRecord
from common.http_client import HttpClient

BASE = "https://wits.worldbank.org/API/V1/SDMX/V21/datasource/tradestats-trade"

PRIORITY = ["USA", "CHN", "DEU", "IND", "GBR", "FRA", "JPN", "BRA", "CAN", "KOR"]

INDICATORS = {
    "XPRT-TRD-VL": "export",
    "MPRT-TRD-VL": "import",
}


class Source(BaseSource):

    def __init__(self, config: dict, http_client=None):
        super().__init__(config)
        self.http = http_client or HttpClient(rate_limit_rps=0.5)

    def fetch_all(self):
        for country in PRIORITY:
            for year in [2022, 2021, 2020]:
                for indicator, flow in INDICATORS.items():
                    yield from self._fetch(country, year, indicator, flow)

    def fetch_updates(self, since_year: int):
        for country in PRIORITY:
            for indicator, flow in INDICATORS.items():
                yield from self._fetch(country, since_year, indicator, flow)

    def normalize(self, raw: dict) -> TradeRecord | None:
        try:
            val = raw.get("value")
            if val is None:
                return None
            return TradeRecord(
                reporter      = raw["reporter"],
                partner       = raw.get("partner", "WLD"),
                hs_code       = raw.get("product", "TOTAL"),
                year          = int(raw["year"]),
                flow          = raw["flow"],
                value_usd     = int(float(val) * 1000),  # WITS reports in thousands USD
                source        = "WB/WITS",
            )
        except Exception:
            return None

    def _fetch(self, country: str, year: int, indicator: str, flow: str):
        url = f"{BASE}/reporter/{country}/year/{year}/partner/WLD/product/TOTAL/indicator/{indicator}"
        try:
            r = self.http.get(url)
            ns = {
                "ss": "http://www.sdmx.org/resources/sdmxml/schemas/v2_1/data/structurespecific",
                "message": "http://www.sdmx.org/resources/sdmxml/schemas/v2_1/message",
            }
            root = ET.fromstring(r.text)
            for obs in root.iter():
                if "Obs" in obs.tag:
                    val = obs.get("OBS_VALUE")
                    if val:
                        yield {
                            "reporter": country,
                            "partner":  "WLD",
                            "product":  "TOTAL",
                            "year":     year,
                            "flow":     flow,
                            "value":    val,
                        }
        except Exception as e:
            self.logger.error(f"WITS fetch failed {country}/{year}/{indicator}: {e}")
