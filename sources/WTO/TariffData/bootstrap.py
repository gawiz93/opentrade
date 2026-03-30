"""
WTO Tariff Download Facility — MFN and bound tariff rates.
Source: https://tariffdata.wto.org/
Docs: https://tariffdata.wto.org/TariffList.aspx
"""

import io
import csv
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parents[4]))

from common.base_source import BaseSource, TariffRecord
from common.http_client import HttpClient

BASE_URL = "https://tariffdata.wto.org/ReportersList.aspx"
DOWNLOAD_URL = "https://tariffdata.wto.org/DownloadListHandler.ashx"


class Source(BaseSource):

    def __init__(self, config: dict, http_client=None):
        super().__init__(config)
        self.http = http_client or HttpClient(rate_limit_rps=0.5)

    def fetch_all(self):
        # WTO provides bulk CSV downloads per country
        priority = ["840", "156", "276", "356", "826"]  # USA, CHN, DEU, IND, GBR
        for reporter_code in priority:
            yield from self._fetch_country(reporter_code, 2023)

    def fetch_updates(self, since_year: int):
        priority = ["840", "156", "276", "356", "826"]
        for reporter_code in priority:
            yield from self._fetch_country(reporter_code, since_year)

    def normalize(self, raw: dict) -> TariffRecord | None:
        try:
            return TariffRecord(
                importer    = raw.get("reporter_iso3", ""),
                hs_code     = str(raw.get("hs6", "")).zfill(6),
                year        = int(raw.get("year", 0)),
                rate_pct    = float(raw.get("duty_avg", 0) or 0),
                tariff_type = raw.get("tariff_type", "MFN"),
                exporter    = None,
                source      = "WTO/TariffData",
            )
        except Exception:
            return None

    def _fetch_country(self, reporter_code: str, year: int):
        params = {
            "reporter": reporter_code,
            "year": year,
            "format": "csv",
        }
        try:
            r = self.http.get(DOWNLOAD_URL, params=params)
            reader = csv.DictReader(io.StringIO(r.text))
            for row in reader:
                row["year"] = year
                yield row
        except Exception as e:
            self.logger.error(f"WTO tariff fetch failed {reporter_code}/{year}: {e}")
