# Adding a New Data Source

Adding your country or region's trade data to OpenTrade takes about 30–60 minutes.

## Step 1: Scaffold

```bash
python runner.py add CC/SourceName
# e.g.
python runner.py add IN/DGFT
python runner.py add JP/Customs
python runner.py add BR/MDIC
```

This creates:
```
sources/IN/DGFT/
├── bootstrap.py    ← implement fetch_all(), fetch_updates(), normalize()
├── config.yaml     ← fill in metadata
├── sample/         ← add 10+ sample records
└── README.md       ← document the source
```

## Step 2: Fill in config.yaml

```yaml
source_id: IN/DGFT
description: "India DGFT — Indian export/import statistics"
country: IN
data_type: trade_flows
status: planned       # change to "live" when working
access: api
url: "https://..."
auth: none            # or api_key
rate_limit_rps: 1.0
years_available: "2010-present"
hs_levels: [8]
```

## Step 3: Implement bootstrap.py

Your `Source` class must implement 3 methods:

```python
class Source(BaseSource):

    def fetch_all(self):
        # Yield raw dicts from the API
        for page in paginate(BASE_URL):
            for row in page["data"]:
                yield row

    def fetch_updates(self, since_year: int):
        # Fetch just the latest year
        yield from self._fetch(since_year)

    def normalize(self, raw: dict) -> TradeRecord:
        # Map raw fields → TradeRecord
        return TradeRecord(
            reporter  = "IND",
            partner   = raw["country_code"],
            hs_code   = raw["hs8"],
            year      = raw["year"],
            flow      = "export",
            value_usd = int(raw["value_usd"]),
            source    = "IN/DGFT",
        )
```

## Step 4: Add sample records

Put 10+ sample JSON records in `sources/CC/SourceName/sample/`. These are used to validate your normalizer without hitting the real API.

```bash
python runner.py validate CC/SourceName
```

## Step 5: Test

```bash
# Dry run (no DB write)
python runner.py run CC/SourceName

# With ingestion
python runner.py run CC/SourceName --ingest
```

## Step 6: Submit a PR

Change `status: planned` → `status: live` in config.yaml, then open a PR. We'll review and merge.

---

**Questions?** Open an issue or start a Discussion on GitHub.
