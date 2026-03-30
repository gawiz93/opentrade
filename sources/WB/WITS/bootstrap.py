"""
World Bank WITS — tariff rates.
Source: https://wits.worldbank.org/
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parents[4]))

from common.base_source import BaseSource, TariffRecord
from common.http_client import HttpClient

BASE_URL = "http://wits.worldbank.org/API/V1/SDMX/V21/datasource/tradestats-tariff"


class Source(BaseSource):

    def __init__(self, config: dict, http_client=None):
        super().__init__(config)
        self.http = http_client or HttpClient(
            rate_limit_rps=float(config.get("rate_limit_rps", 0.5)),
        )

    def fetch_all(self):
        priority = ["USA", "CHN", "DEU", "IND", "GBR", "FRA", "JPN", "BRA"]
        for country in priority:
            for year in range(2018, 2024):
                yield from self._fetch_tariffs(country, year)

    def fetch_updates(self, since_year: int):
        priority = ["USA", "CHN", "DEU", "IND", "GBR", "FRA", "JPN", "BRA"]
        for country in priority:
            yield from self._fetch_tariffs(country, since_year)

    def normalize(self, raw: dict) -> TariffRecord | None:
        try:
            return TariffRecord(
                importer    = raw["importer"],
                hs_code     = raw["hs_code"],
                year        = int(raw["year"]),
                rate_pct    = float(raw["rate"]),
                tariff_type = "MFN",
                exporter    = None,
                source      = "WB/WITS",
            )
        except Exception:
            return None

    def _fetch_tariffs(self, country: str, year: int):
        url = f"{BASE_URL}/reporter/{country}/year/{year}/partner/WLD/product/ALL/indicator/AHS-WGHTD-AVRG"
        try:
            r = self.http.get(url)
            # Parse JSON response
            data = r.json()
            for obs in data.get("dataSets", [{}])[0].get("observations", {}).values():
                hs = obs.get("dimensions", {}).get("PRODUCTGROUP", "")
                rate = obs.get("value")
                if hs and rate is not None:
                    raw = {"importer": country, "hs_code": hs, "year": year, "rate": rate}
                    yield raw
        except Exception as e:
            self.logger.error(f"WITS fetch failed {country}/{year}: {e}")
