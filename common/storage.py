"""
JSONL storage with deduplication. Used during ingestion before DB insert.
"""

import json
import hashlib
from pathlib import Path
from dataclasses import asdict
from common.base_source import TradeRecord, TariffRecord


class Storage:
    def __init__(self, output_dir: str = "/tmp/opentrade_output"):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self._seen: set[str] = set()

    def write(self, record: TradeRecord | TariffRecord, source_id: str) -> bool:
        """Write a record to JSONL. Returns False if duplicate."""
        key = self._dedup_key(record)
        if key in self._seen:
            return False
        self._seen.add(key)

        out_path = self.output_dir / f"{source_id}.jsonl"
        row = asdict(record)
        with open(out_path, "a") as f:
            f.write(json.dumps(row) + "\n")
        return True

    def read(self, source_id: str):
        """Read all records for a source."""
        out_path = self.output_dir / f"{source_id}.jsonl"
        if not out_path.exists():
            return
        with open(out_path) as f:
            for line in f:
                yield json.loads(line.strip())

    def _dedup_key(self, record) -> str:
        if isinstance(record, TradeRecord):
            key = f"{record.reporter}:{record.partner}:{record.hs_code}:{record.year}:{record.flow}"
        else:
            key = f"{record.importer}:{record.exporter}:{record.hs_code}:{record.year}:{record.tariff_type}"
        return hashlib.md5(key.encode()).hexdigest()
