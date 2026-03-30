# 🌍 OpenTrade

**The open-source global trade intelligence API.**

Search, explore, and analyze international trade flows across 200+ countries and 5,000+ product categories — free, open-source, and built for scale.

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
[![Sources](https://img.shields.io/badge/sources-5-blue)](#coverage)
[![PRs Welcome](https://img.shields.io/badge/PRs-welcome-brightgreen.svg)](CONTRIBUTING.md)

---

## What is OpenTrade?

OpenTrade aggregates public trade data from the world's leading sources into a single, fast, developer-friendly API with hybrid semantic + keyword search.

**For developers & SMBs:**
- Find suppliers and buyers globally
- Discover import/export volumes by product and country
- Monitor tariff rates across markets

**For economists & researchers:**
- Time-series trade flows by country and HS code
- Trade balance trends and gravity model datasets
- Tariff policy analysis across 170+ countries

---

## Quick Start

```bash
# Search trade flows
curl -X POST https://api.opentrade.dev/v1/search \
  -H "Content-Type: application/json" \
  -d '{"q": "solar panels from China", "top_k": 5}'

# Time-series data
curl "https://api.opentrade.dev/v1/timeseries?reporter=USA&partner=CHN&year_from=2015&year_to=2023"

# Tariff rates
curl "https://api.opentrade.dev/v1/tariffs?importer=IND&hs_code=8471"
```

---

## Self-Hosting

```bash
git clone https://github.com/gawiz93/opentrade
cd opentrade
cp .env.example .env
docker compose up -d

# Run your first ingestion
python runner.py run UN/Comtrade --ingest
```

---

## Data Sources

| Source | Org | Coverage | Status |
|--------|-----|----------|--------|
| [UN/Comtrade](sources/UN/Comtrade/) | United Nations | 200+ countries, 1962–present | ✅ Live |
| [WB/WITS](sources/WB/WITS/) | World Bank | Tariffs, 170+ countries | ✅ Live |
| [US/Census](sources/US/Census/) | US Census Bureau | US trade, 1992–present | 🔄 Planned |
| [EU/Eurostat](sources/EU/Eurostat/) | European Commission | EU member states | 🔄 Planned |
| [WTO/TariffData](sources/WTO/TariffData/) | World Trade Organization | Bound tariffs, 170+ countries | 🔄 Planned |

**👉 [Add your country's data →](docs/adding-a-source.md)**

---

## Runner CLI

```bash
python runner.py status                  # Show all sources + status
python runner.py sample UN/Comtrade      # Print sample records
python runner.py run UN/Comtrade         # Dry-run a source
python runner.py run UN/Comtrade --ingest  # Run + write to DB
python runner.py validate UN/Comtrade    # Validate samples
python runner.py next                    # Show what needs work
python runner.py add IN/DGFT             # Scaffold a new source
```

---

## API Endpoints

| Endpoint | Description |
|----------|-------------|
| `POST /v1/search` | Semantic + keyword search across trade flows |
| `GET /v1/timeseries` | Time-series data by country/product/year |
| `GET /v1/tariffs` | Tariff rates by importer and HS code |
| `GET /v1/countries` | Countries with data coverage |
| `GET /v1/products` | Browse HS product codes |
| `GET /v1/partners` | Top trading partners for a country |

Full docs: [docs/api.md](docs/api.md)

---

## Contributing

The easiest way to contribute is to **add your country's trade data source**.

```bash
python runner.py add CC/SourceName
# Edit bootstrap.py and config.yaml
# Open a PR
```

See [docs/adding-a-source.md](docs/adding-a-source.md) for a full guide.

We especially welcome sources from:
- 🌏 Asia-Pacific (India, Indonesia, Vietnam, Thailand, Malaysia...)
- 🌍 Africa (Nigeria, Kenya, South Africa, Egypt...)
- 🌎 Latin America (Brazil, Mexico, Colombia, Chile...)
- 🏛️ Regional bodies (ASEAN, MERCOSUR, African Union...)

---

## License

MIT — free to use, self-host, and build on.
