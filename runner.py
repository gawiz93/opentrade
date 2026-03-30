#!/usr/bin/env python3
"""
OpenTrade Runner — CLI to manage, test, and run collection scripts.

Usage:
  python runner.py status                     # Show all sources + status
  python runner.py list                       # List all sources
  python runner.py sample UN/Comtrade         # Print 5 sample records
  python runner.py run UN/Comtrade            # Dry-run a source
  python runner.py run UN/Comtrade --ingest   # Run + write to DB
  python runner.py run-all                    # Dry-run all live sources
  python runner.py run-all --ingest           # Run all live sources + write to DB
  python runner.py validate UN/Comtrade       # Validate sample records
  python runner.py next                       # Show what needs work
  python runner.py add CC/SourceName          # Scaffold a new source
"""

import argparse
import importlib
import json
import os
import shutil
import sys
import yaml
from pathlib import Path
from textwrap import indent

ROOT     = Path(__file__).parent
SOURCES  = ROOT / "sources"
MANIFEST = ROOT / "manifest.yaml"
TEMPLATES = ROOT / "templates"

# ANSI colours
GREEN  = "\033[92m"
YELLOW = "\033[93m"
RED    = "\033[91m"
CYAN   = "\033[96m"
RESET  = "\033[0m"
BOLD   = "\033[1m"


def load_manifest() -> dict:
    if MANIFEST.exists():
        return yaml.safe_load(MANIFEST.read_text()) or {}
    return {"sources": {}}


def save_manifest(data: dict):
    MANIFEST.write_text(yaml.dump(data, default_flow_style=False, sort_keys=True))


def load_config(source_path: str) -> dict:
    cfg_file = SOURCES / source_path / "config.yaml"
    if not cfg_file.exists():
        return {}
    return yaml.safe_load(cfg_file.read_text()) or {}


def load_source_class(source_path: str):
    """Dynamically import the bootstrap.py for a source."""
    module_path = f"sources.{source_path.replace('/', '.')}.bootstrap"
    try:
        mod = importlib.import_module(module_path)
        return getattr(mod, "Source", None)
    except ModuleNotFoundError:
        return None


def all_sources() -> list[str]:
    """Return all source paths like ['UN/Comtrade', 'WB/WITS', ...]"""
    paths = []
    for cc_dir in sorted(SOURCES.iterdir()):
        if not cc_dir.is_dir():
            continue
        for src_dir in sorted(cc_dir.iterdir()):
            if src_dir.is_dir() and (src_dir / "config.yaml").exists():
                paths.append(f"{cc_dir.name}/{src_dir.name}")
    return paths


# ── Commands ─────────────────────────────────────────────────────────────────

def cmd_status(args):
    manifest = load_manifest()
    sources = all_sources()
    print(f"\n{BOLD}OpenTrade — {len(sources)} sources{RESET}\n")
    print(f"{'Source':<30} {'Status':<12} {'Records':<12} {'Last run'}")
    print("─" * 70)
    for src in sources:
        cfg = load_config(src)
        meta = manifest.get("sources", {}).get(src, {})
        status = meta.get("status", cfg.get("status", "planned"))
        records = meta.get("records", "—")
        last_run = meta.get("last_run", "never")
        colour = GREEN if status == "live" else YELLOW if status == "planned" else RED
        print(f"{src:<30} {colour}{status:<12}{RESET} {str(records):<12} {last_run}")
    print()


def cmd_list(args):
    for src in all_sources():
        cfg = load_config(src)
        print(f"{CYAN}{src}{RESET} — {cfg.get('description', '')}")


def cmd_sample(args):
    src = args.source
    cls = load_source_class(src)
    cfg = load_config(src)

    if not cls:
        print(f"{RED}No bootstrap.py found for {src}{RESET}")
        sys.exit(1)

    # Try loading from sample/ directory first
    sample_dir = SOURCES / src / "sample"
    if sample_dir.exists():
        files = sorted(sample_dir.glob("*.json"))[:5]
        if files:
            print(f"\n{BOLD}Sample records from {src}{RESET}\n")
            for f in files:
                print(json.dumps(json.loads(f.read_text()), indent=2))
            return

    # Otherwise run the source and grab 5 records
    instance = cls(config=cfg)
    print(f"\n{BOLD}Live sample from {src}{RESET}\n")
    count = 0
    for raw in instance.fetch_all():
        record = instance.normalize(raw)
        if record:
            print(json.dumps(record.__dict__ if hasattr(record, '__dict__') else record, indent=2, default=str))
            count += 1
            if count >= 5:
                break


def _update_manifest(src: str, records: int, errors: int, status: str = "success"):
    """Update manifest.yaml with run results."""
    from datetime import datetime, timezone
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


def _run_one_source(src: str, ingest: bool = False, verbose: bool = True) -> tuple[int, int]:
    """Run a single source. Returns (records, errors)."""
    cls = load_source_class(src)
    cfg = load_config(src)

    if not cls:
        if verbose:
            print(f"{RED}No bootstrap.py found for {src}{RESET}")
        return 0, 1

    instance = cls(config=cfg)
    count = 0
    errors = 0

    from common.storage import Storage
    from common.validators import validate_trade_record, validate_tariff_record
    from common.base_source import TradeRecord, TariffRecord
    storage = Storage()

    for raw in instance.fetch_all():
        record = instance.normalize(raw)
        if not record:
            continue

        if isinstance(record, TradeRecord):
            errs = validate_trade_record(record.__dict__)
        else:
            errs = validate_tariff_record(record.__dict__)

        if errs:
            if verbose:
                print(f"{RED}  Validation error: {errs}{RESET}")
            errors += 1
            continue

        if ingest:
            storage.write(record, cfg.get("source_id", src.replace("/", "_")))

        count += 1
        if count % 1000 == 0 and verbose:
            print(f"  {count} records processed...")

    if ingest:
        _write_to_db(src, cfg, storage)

    _update_manifest(src, count, errors)
    return count, errors


def cmd_run(args):
    src = args.source
    cfg = load_config(src)

    if not load_source_class(src):
        print(f"{RED}No bootstrap.py found for {src}{RESET}")
        sys.exit(1)

    print(f"\n{BOLD}Running {src}{RESET} {'(dry-run)' if not args.ingest else '(ingesting)'}\n")
    count, errors = _run_one_source(src, ingest=args.ingest, verbose=True)
    print(f"\n{GREEN}✓ {count} records{RESET} | {RED}{errors} errors{RESET}")
    if args.ingest:
        print(f"{CYAN}manifest.yaml updated{RESET}")


def cmd_run_all(args):
    """Run all live sources."""
    sources = []
    for src in all_sources():
        cfg = load_config(src)
        status = load_manifest().get("sources", {}).get(src, {}).get("status") or cfg.get("status", "planned")
        if status == "live":
            sources.append(src)

    if not sources:
        print(f"{YELLOW}No live sources found.{RESET}")
        return

    print(f"\n{BOLD}Running all {len(sources)} live sources{RESET} {'(dry-run)' if not args.ingest else '(ingesting)'}\n")
    total_records = 0
    total_errors = 0
    results = []

    for src in sources:
        print(f"{CYAN}▶ {src}{RESET}")
        count, errors = _run_one_source(src, ingest=args.ingest, verbose=False)
        colour = GREEN if errors == 0 else YELLOW
        print(f"  {colour}✓ {count} records, {errors} errors{RESET}")
        total_records += count
        total_errors += errors
        results.append((src, count, errors))

    print(f"\n{BOLD}{'─'*50}{RESET}")
    print(f"{GREEN}Total: {total_records} records{RESET} | {RED}{total_errors} errors{RESET} across {len(sources)} sources")
    if args.ingest:
        print(f"{CYAN}manifest.yaml updated for all sources{RESET}")


def cmd_validate(args):
    src = args.source
    sample_dir = SOURCES / src / "sample"
    if not sample_dir.exists() or not list(sample_dir.glob("*.json")):
        print(f"{YELLOW}No sample data found in {sample_dir}{RESET}")
        return

    from common.validators import validate_trade_record
    errors = 0
    for f in sample_dir.glob("*.json"):
        data = json.loads(f.read_text())
        errs = validate_trade_record(data)
        if errs:
            print(f"{RED}✗ {f.name}: {errs}{RESET}")
            errors += 1
        else:
            print(f"{GREEN}✓ {f.name}{RESET}")

    if errors == 0:
        print(f"\n{GREEN}All samples valid!{RESET}")


def cmd_next(args):
    """Show sources that need work."""
    manifest = load_manifest()
    print(f"\n{BOLD}Sources needing work:{RESET}\n")
    for src in all_sources():
        cfg = load_config(src)
        meta = manifest.get("sources", {}).get(src, {})
        status = meta.get("status", cfg.get("status", "planned"))
        if status in ("planned", "blocked", "partial"):
            colour = YELLOW if status == "planned" else RED
            print(f"  {colour}{status:<10}{RESET} {src} — {cfg.get('description', '')}")


def cmd_add(args):
    """Scaffold a new source from templates."""
    src = args.source   # e.g. "IN/DGFT"
    parts = src.split("/")
    if len(parts) != 2:
        print(f"{RED}Source must be CC/SourceName e.g. IN/DGFT{RESET}")
        sys.exit(1)

    dest = SOURCES / src
    if dest.exists():
        print(f"{YELLOW}Source already exists: {dest}{RESET}")
        sys.exit(1)

    dest.mkdir(parents=True)
    (dest / "sample").mkdir()

    shutil.copy(TEMPLATES / "bootstrap_template.py", dest / "bootstrap.py")
    shutil.copy(TEMPLATES / "config_template.yaml",  dest / "config.yaml")

    # Patch config.yaml with source_id
    cfg_path = dest / "config.yaml"
    content = cfg_path.read_text().replace("CC/SourceName", src)
    cfg_path.write_text(content)

    readme = f"""# {src}

**Country/Org:** {parts[0]}
**Source:** {parts[1]}

## Description

_TODO: describe what this source contains_

## Access

- URL: _TODO_
- Auth: None / API key required
- Format: JSON / XML / CSV

## Notes

_TODO: rate limits, quirks, blocked status_
"""
    (dest / "README.md").write_text(readme)

    print(f"{GREEN}✓ Scaffolded {src}{RESET}")
    print(f"  Edit: {dest}/bootstrap.py")
    print(f"  Edit: {dest}/config.yaml")
    print(f"  Add samples to: {dest}/sample/")
    print(f"\nThen test with: python runner.py sample {src}")


def _write_to_db(src: str, cfg: dict, storage):
    """Write records from storage to PostgreSQL."""
    import psycopg2
    from psycopg2.extras import execute_batch

    db_url = os.getenv("DATABASE_URL", "postgresql://opentrade:opentrade@localhost:5432/opentrade")
    conn = psycopg2.connect(db_url)
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
                SET value_usd = EXCLUDED.value_usd
            """,
            [r for r in records if "flow" in r],
            page_size=500,
        )
        conn.commit()
    conn.close()
    print(f"{GREEN}✓ Written {len(records)} records to DB{RESET}")


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="OpenTrade Runner",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    sub = parser.add_subparsers(dest="command")

    sub.add_parser("status",   help="Show all sources and status")
    sub.add_parser("list",     help="List all sources")
    sub.add_parser("next",     help="Show sources needing work")

    p = sub.add_parser("sample",   help="Print sample records from a source")
    p.add_argument("source", help="e.g. UN/Comtrade")

    p = sub.add_parser("run",      help="Run a source")
    p.add_argument("source", help="e.g. UN/Comtrade")
    p.add_argument("--ingest", action="store_true", help="Write to DB (default: dry-run)")

    p = sub.add_parser("run-all",  help="Run all live sources")
    p.add_argument("--ingest", action="store_true", help="Write to DB (default: dry-run)")

    p = sub.add_parser("validate", help="Validate sample records")
    p.add_argument("source", help="e.g. UN/Comtrade")

    p = sub.add_parser("add",      help="Scaffold a new source")
    p.add_argument("source", help="e.g. IN/DGFT")

    args = parser.parse_args()

    commands = {
        "status":   cmd_status,
        "list":     cmd_list,
        "sample":   cmd_sample,
        "run":      cmd_run,
        "run-all":  cmd_run_all,
        "validate": cmd_validate,
        "next":     cmd_next,
        "add":      cmd_add,
    }

    if not args.command:
        parser.print_help()
        sys.exit(0)

    commands[args.command](args)


if __name__ == "__main__":
    main()
