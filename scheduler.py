#!/usr/bin/env python3
"""
OpenTrade Scheduler — runs collection scripts automatically on a schedule.

Each source declares its update_frequency in config.yaml:
  annual   → runs every Sunday at 03:00 UTC
  monthly  → runs on the 1st of each month at 03:00 UTC
  weekly   → runs every Sunday at 03:00 UTC
  daily    → runs every day at 03:00 UTC

Usage:
  python scheduler.py               # Start the daemon (runs forever)
  python scheduler.py --once        # Run all live sources once, then exit
  python scheduler.py --source UN/Comtrade --once  # Run a single source and exit
  python scheduler.py --dry-run     # Schedule everything but don't actually ingest
  python scheduler.py --list        # Show what's scheduled and when

Environment variables:
  DATABASE_URL       PostgreSQL connection string (required for --ingest)
  COMTRADE_API_KEY   UN Comtrade API key (optional, free tier works without)
  US_CENSUS_API_KEY  US Census Bureau API key
  OPENTRADE_LOG_DIR  Log directory (default: ./logs)
"""

import argparse
import importlib
import logging
import os
import sys
import yaml
from datetime import datetime, timezone
from pathlib import Path

ROOT     = Path(__file__).parent
SOURCES  = ROOT / "sources"
MANIFEST = ROOT / "manifest.yaml"
LOG_DIR  = Path(os.getenv("OPENTRADE_LOG_DIR", ROOT / "logs"))

# Ensure log directory exists
LOG_DIR.mkdir(exist_ok=True)

# ── Logging ──────────────────────────────────────────────────────────────────

def setup_logging():
    fmt = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
    # Use stdout only — systemd/caller can redirect to file.
    # If OPENTRADE_LOG_DIR is set AND we're not a TTY, also write to file.
    handlers: list[logging.Handler] = [logging.StreamHandler(sys.stdout)]
    if not sys.stdout.isatty():
        handlers.append(logging.FileHandler(LOG_DIR / "scheduler.log"))
    logging.basicConfig(level=logging.INFO, format=fmt, handlers=handlers)

log = logging.getLogger("opentrade.scheduler")


# ── Manifest helpers ──────────────────────────────────────────────────────────

def load_manifest() -> dict:
    if MANIFEST.exists():
        return yaml.safe_load(MANIFEST.read_text()) or {}
    return {"sources": {}}


def save_manifest(data: dict):
    MANIFEST.write_text(yaml.dump(data, default_flow_style=False, sort_keys=True))


def update_manifest_run(src: str, records: int, errors: int, status: str = "success"):
    manifest = load_manifest()
    if "sources" not in manifest:
        manifest["sources"] = {}
    if src not in manifest["sources"]:
        manifest["sources"][src] = {}
    manifest["sources"][src]["last_run"] = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    manifest["sources"][src]["last_status"] = status
    if records:
        manifest["sources"][src]["records"] = records
    if errors:
        manifest["sources"][src]["last_errors"] = errors
    save_manifest(manifest)


# ── Source discovery ──────────────────────────────────────────────────────────

def load_config(source_path: str) -> dict:
    cfg_file = SOURCES / source_path / "config.yaml"
    if not cfg_file.exists():
        return {}
    return yaml.safe_load(cfg_file.read_text()) or {}


def load_source_class(source_path: str):
    module_path = f"sources.{source_path.replace('/', '.')}.bootstrap"
    try:
        mod = importlib.import_module(module_path)
        return getattr(mod, "Source", None)
    except (ModuleNotFoundError, AttributeError):
        return None


def live_sources() -> list[tuple[str, dict]]:
    """Return [(source_path, config), ...] for all live sources."""
    result = []
    manifest = load_manifest()
    for cc_dir in sorted(SOURCES.iterdir()):
        if not cc_dir.is_dir():
            continue
        for src_dir in sorted(cc_dir.iterdir()):
            if not src_dir.is_dir():
                continue
            src = f"{cc_dir.name}/{src_dir.name}"
            cfg = load_config(src)
            if not cfg:
                continue
            # Use manifest status if set, else config status
            m_status = manifest.get("sources", {}).get(src, {}).get("status")
            status = m_status or cfg.get("status", "planned")
            if status == "live":
                result.append((src, cfg))
    return result


# ── Run a single source ───────────────────────────────────────────────────────

def run_source(src: str, cfg: dict, ingest: bool = True, dry_run: bool = False) -> tuple[int, int]:
    """
    Run ingestion for a single source.
    Returns (records_count, error_count).
    """
    cls = load_source_class(src)
    if not cls:
        log.error(f"[{src}] No bootstrap.py / Source class found")
        return 0, 1

    from common.storage import Storage
    from common.validators import validate_trade_record, validate_tariff_record
    from common.base_source import TradeRecord, TariffRecord

    storage = Storage()
    count = 0
    errors = 0

    log.info(f"[{src}] Starting {'dry-run' if dry_run else 'ingestion'}")

    try:
        instance = cls(config=cfg)
        for raw in instance.fetch_all():
            record = instance.normalize(raw)
            if not record:
                continue

            if isinstance(record, TradeRecord):
                errs = validate_trade_record(record.__dict__)
            else:
                errs = validate_tariff_record(record.__dict__)

            if errs:
                log.debug(f"[{src}] Validation error: {errs}")
                errors += 1
                continue

            if not dry_run:
                storage.write(record, cfg.get("source_id", src.replace("/", "_")))

            count += 1
            if count % 5000 == 0:
                log.info(f"[{src}] {count} records processed...")

    except Exception as e:
        log.exception(f"[{src}] Fatal error during ingestion: {e}")
        update_manifest_run(src, count, errors + 1, status="error")
        return count, errors + 1

    if not dry_run and ingest:
        _write_to_db(src, cfg, storage)

    update_manifest_run(src, count, errors)
    log.info(f"[{src}] Done — {count} records, {errors} errors")
    return count, errors


def _write_to_db(src: str, cfg: dict, storage):
    """Flush records from JSONL storage to PostgreSQL."""
    try:
        import psycopg2
        from psycopg2.extras import execute_batch
    except ImportError:
        log.warning("psycopg2 not available — skipping DB write")
        return

    db_url = os.getenv("DATABASE_URL", "postgresql://opentrade:opentrade@localhost:5432/opentrade")
    try:
        conn = psycopg2.connect(db_url)
    except Exception as e:
        log.error(f"[{src}] DB connection failed: {e}")
        return

    source_id = cfg.get("source_id", src.replace("/", "_"))
    records = list(storage.read(source_id))
    if not records:
        return

    with conn.cursor() as cur:
        execute_batch(
            cur,
            """
            INSERT INTO trade_flows
                (reporter, partner, hs_code, year, flow, value_usd, quantity, quantity_unit, source, source_id)
            VALUES
                (%(reporter)s, %(partner)s, %(hs_code)s, %(year)s, %(flow)s,
                 %(value_usd)s, %(quantity)s, %(quantity_unit)s, %(source)s, %(source_id)s)
            ON CONFLICT (reporter, partner, hs_code, year, flow, source) DO UPDATE
                SET value_usd = EXCLUDED.value_usd,
                    updated_at = NOW()
            """,
            [r for r in records if "flow" in r],
            page_size=500,
        )
        conn.commit()
    conn.close()
    log.info(f"[{src}] Written {len(records)} records to DB")


# ── Frequency → APScheduler trigger ──────────────────────────────────────────

FREQ_MAP = {
    # update_frequency in config.yaml → (trigger, kwargs)
    "daily":   ("cron", {"hour": 3, "minute": 0}),
    "weekly":  ("cron", {"day_of_week": "sun", "hour": 3, "minute": 0}),
    "monthly": ("cron", {"day": 1, "hour": 3, "minute": 0}),
    "annual":  ("cron", {"month": 1, "day": 1, "hour": 3, "minute": 0}),
    "yearly":  ("cron", {"month": 1, "day": 1, "hour": 3, "minute": 0}),
}

FREQ_HUMAN = {
    "daily":   "every day at 03:00 UTC",
    "weekly":  "every Sunday at 03:00 UTC",
    "monthly": "1st of each month at 03:00 UTC",
    "annual":  "January 1st at 03:00 UTC",
    "yearly":  "January 1st at 03:00 UTC",
}


# ── Commands ──────────────────────────────────────────────────────────────────

def cmd_once(args):
    """Run all live sources (or a specific source) once and exit."""
    sources = live_sources()
    if args.source:
        sources = [(s, c) for s, c in sources if s == args.source]
        if not sources:
            log.error(f"Source not found or not live: {args.source}")
            sys.exit(1)

    log.info(f"Running {len(sources)} source(s) once")
    for src, cfg in sources:
        run_source(src, cfg, ingest=True, dry_run=args.dry_run)


def cmd_list(args):
    """Show what's scheduled and when."""
    sources = live_sources()
    print(f"\n{'Source':<30} {'Frequency':<12} {'Schedule'}")
    print("─" * 70)
    for src, cfg in sources:
        freq = cfg.get("update_frequency", "weekly")
        human = FREQ_HUMAN.get(freq, freq)
        print(f"{src:<30} {freq:<12} {human}")
    print()


def cmd_daemon(args):
    """Start the scheduler daemon (runs forever)."""
    try:
        from apscheduler.schedulers.blocking import BlockingScheduler
        from apscheduler.triggers.cron import CronTrigger
    except ImportError:
        log.error("APScheduler not installed. Run: pip install apscheduler")
        sys.exit(1)

    scheduler = BlockingScheduler(timezone="UTC")
    sources = live_sources()

    if not sources:
        log.warning("No live sources found — nothing to schedule")
        sys.exit(0)

    manifest = load_manifest()

    for src, cfg in sources:
        # manifest.yaml schedule overrides config.yaml update_frequency
        freq = (
            manifest.get("sources", {}).get(src, {}).get("schedule")
            or cfg.get("update_frequency", "weekly")
        )
        trigger_type, trigger_kwargs = FREQ_MAP.get(freq, FREQ_MAP["weekly"])

        def make_job(s=src, c=cfg):
            def job():
                log.info(f"Scheduled run starting: {s}")
                run_source(s, c, ingest=not args.dry_run)
            return job

        scheduler.add_job(
            make_job(),
            CronTrigger(**trigger_kwargs, timezone="UTC"),
            id=src.replace("/", "_"),
            name=f"ingest_{src}",
            max_instances=1,
            coalesce=True,
            misfire_grace_time=3600,
        )

        human = FREQ_HUMAN.get(freq, freq)
        log.info(f"Scheduled [{src}] — {human}")

    log.info(f"Scheduler started with {len(sources)} jobs. Waiting for next run...")
    log.info(f"Logs: {LOG_DIR / 'scheduler.log'}")

    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        log.info("Scheduler stopped")


# ── Entry point ───────────────────────────────────────────────────────────────

def main():
    setup_logging()

    parser = argparse.ArgumentParser(
        description="OpenTrade Scheduler",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument("--once",    action="store_true", help="Run all live sources once and exit")
    parser.add_argument("--source",  help="Only run this source (e.g. UN/Comtrade)")
    parser.add_argument("--dry-run", action="store_true", help="Fetch and validate but don't write to DB")
    parser.add_argument("--list",    action="store_true", help="Show scheduled sources and exit")

    args = parser.parse_args()

    if args.list:
        cmd_list(args)
    elif args.once or args.source:
        cmd_once(args)
    else:
        cmd_daemon(args)


if __name__ == "__main__":
    main()
