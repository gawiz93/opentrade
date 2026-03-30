"""
Base class for all OpenTrade data sources.
Every source in sources/{CC}/{Name}/bootstrap.py must implement this interface.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Generator
import logging

logger = logging.getLogger(__name__)


@dataclass
class TradeRecord:
    """Standard trade flow record — every source normalises to this."""
    reporter:      str          # ISO3 exporting/reporting country (e.g. "USA")
    partner:       str          # ISO3 partner country (e.g. "CHN"), "WLD" for world total
    hs_code:       str          # HS product code (e.g. "854140")
    year:          int          # Reference year
    flow:          str          # "export" or "import"
    value_usd:     int | None   # Trade value in USD
    quantity:      float | None = None
    quantity_unit: str | None   = None
    source:        str = ""     # set automatically from config.yaml
    source_id:     str | None   = None  # original record ID in source


@dataclass
class TariffRecord:
    """Standard tariff record."""
    importer:    str           # ISO3 importing country
    hs_code:     str           # HS product code
    year:        int
    rate_pct:    float         # tariff rate as %
    tariff_type: str           # "MFN", "preferential", "applied"
    exporter:    str | None = None   # None = MFN (applies to all)
    source:      str = ""


class BaseSource(ABC):
    """
    All collection scripts inherit from this class.

    Required:
        fetch_all()     — initial full load
        fetch_updates() — incremental updates (latest year only)
        normalize()     — transform raw row → TradeRecord or TariffRecord

    Optional:
        retrieve()      — resolve a reference (e.g. HS code + country → record)
    """

    def __init__(self, config: dict, http_client=None):
        self.config = config
        self.http = http_client
        self.source_id = config.get("source_id", "")
        self.logger = logging.getLogger(self.__class__.__name__)

    @abstractmethod
    def fetch_all(self) -> Generator[dict, None, None]:
        """Fetch all available records. Used for initial ingestion."""
        ...

    @abstractmethod
    def fetch_updates(self, since_year: int) -> Generator[dict, None, None]:
        """Fetch records updated since a given year. Used for daily updates."""
        ...

    @abstractmethod
    def normalize(self, raw: dict) -> TradeRecord | TariffRecord | None:
        """Transform a raw API/scrape response into a standard record."""
        ...

    def retrieve(self, **kwargs) -> dict | None:
        """Resolve a reference to a specific record. Optional."""
        return None

    def validate(self, record) -> bool:
        """Basic validation — override for source-specific rules."""
        if isinstance(record, TradeRecord):
            return bool(record.reporter and record.partner and record.hs_code and record.year)
        if isinstance(record, TariffRecord):
            return bool(record.importer and record.hs_code and record.year)
        return False
