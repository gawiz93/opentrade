CREATE EXTENSION IF NOT EXISTS vector;

CREATE TABLE IF NOT EXISTS countries (
    code TEXT PRIMARY KEY, code2 TEXT, name TEXT NOT NULL,
    region TEXT, income_group TEXT
);

CREATE TABLE IF NOT EXISTS products (
    hs_code TEXT PRIMARY KEY, hs_level INTEGER, description TEXT NOT NULL,
    section TEXT, chapter TEXT
);

CREATE TABLE IF NOT EXISTS trade_flows (
    id BIGSERIAL PRIMARY KEY,
    reporter TEXT NOT NULL, partner TEXT NOT NULL,
    hs_code TEXT NOT NULL, year INTEGER NOT NULL,
    flow TEXT NOT NULL, value_usd BIGINT,
    quantity NUMERIC, quantity_unit TEXT,
    source TEXT NOT NULL, source_id TEXT,
    created_at TIMESTAMP DEFAULT NOW(),
    UNIQUE(reporter, partner, hs_code, year, flow, source)
);

CREATE TABLE IF NOT EXISTS tariffs (
    id BIGSERIAL PRIMARY KEY,
    importer TEXT NOT NULL, exporter TEXT,
    hs_code TEXT NOT NULL, year INTEGER NOT NULL,
    tariff_type TEXT, rate_pct NUMERIC, source TEXT NOT NULL,
    UNIQUE(importer, exporter, hs_code, year, tariff_type)
);

CREATE INDEX IF NOT EXISTS idx_flows_reporter ON trade_flows(reporter);
CREATE INDEX IF NOT EXISTS idx_flows_partner  ON trade_flows(partner);
CREATE INDEX IF NOT EXISTS idx_flows_hs       ON trade_flows(hs_code);
CREATE INDEX IF NOT EXISTS idx_flows_year     ON trade_flows(year);
CREATE INDEX IF NOT EXISTS idx_tariffs_importer ON tariffs(importer);
CREATE INDEX IF NOT EXISTS idx_tariffs_hs       ON tariffs(hs_code);
