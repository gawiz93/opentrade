"""
Bootstrap script for CC/SourceName.

Implements the BaseSource interface:
  - fetch_all()     → initial full load
  - fetch_updates() → incremental updates
  - normalize()     → raw dict → TradeRecord or TariffRecord
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parents[3]))

from common.base_source import BaseSource, TradeRecord
from common.http_client import HttpClient

# TODO: Replace with your source's base URL
BASE_URL = "https://your-source-api.example.com"


class Source(BaseSource):

    def __init__(self, config: dict, http_client=None):
        super().__init__(config)
        self.http = http_client or HttpClient(
            rate_limit_rps=float(config.get("rate_limit_rps", 1.0))
        )

    def fetch_all(self):
        """
        Fetch all available records.
        Yield raw dicts — normalize() will convert them.
        """
        # TODO: implement pagination / bulk download
        # Example:
        # r = self.http.get(BASE_URL + "/data", params={"year": 2023})
        # for row in r.json().get("data", []):
        #     yield row
        raise NotImplementedError

    def fetch_updates(self, since_year: int):
        """
        Fetch records updated since a given year.
        For most sources, this just fetches the latest year.
        """
        # TODO: implement incremental fetch
        raise NotImplementedError

    def normalize(self, raw: dict) -> TradeRecord | None:
        """
        Transform a raw API response row into a TradeRecord.
        Return None to skip invalid / irrelevant rows.
        """
        # TODO: map source fields to TradeRecord fields
        # return TradeRecord(
        #     reporter      = raw["reporterISO3"],
        #     partner       = raw["partnerISO3"],
        #     hs_code       = str(raw["cmdCode"]),
        #     year          = int(raw["period"]),
        #     flow          = "export" if raw["flowCode"] == "X" else "import",
        #     value_usd     = int(raw.get("primaryValue") or 0),
        #     quantity      = raw.get("qty"),
        #     quantity_unit = raw.get("qtyUnitAbbr"),
        #     source        = self.source_id,
        # )
        raise NotImplementedError
