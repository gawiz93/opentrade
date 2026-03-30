# Scheduling & Automated Ingestion

OpenTrade supports two modes of automated ingestion:
1. **GitHub Actions** — for open-source contributors and cloud deployments
2. **`scheduler.py` daemon** — for self-hosted deployments (VPS, on-prem)

---

## GitHub Actions (Recommended for Cloud)

### How it works

`.github/workflows/ingest.yml` runs daily at **03:00 UTC**. It:
1. Discovers all `live` sources from `manifest.yaml`
2. Runs each source **in parallel** via a matrix job
3. Writes results to your PostgreSQL instance (via `DATABASE_URL` secret)
4. Uploads run logs as artifacts (retained 7 days)

### Required GitHub Secrets

Go to **Settings → Secrets and variables → Actions** and add:

| Secret | Description |
|--------|-------------|
| `DATABASE_URL` | PostgreSQL connection string |
| `COMTRADE_API_KEY` | UN Comtrade API key (optional — free tier works) |
| `US_CENSUS_API_KEY` | US Census API key (optional) |

### Manual run

Trigger a run manually from **Actions → Scheduled Ingestion → Run workflow**. You can optionally target a single source:

```
Source: UN/Comtrade       # leave blank for all
Dry run: false
```

---

## scheduler.py (Self-Hosted)

For VPS/on-prem deployments, run the scheduler as a daemon.

### Quick start

```bash
# Install dependencies
pip install -r requirements.txt

# Copy and edit .env
cp .env.example .env
# Set DATABASE_URL, COMTRADE_API_KEY, etc.

# Test: list what's scheduled
python scheduler.py --list

# Test: dry-run all live sources
python scheduler.py --once --dry-run

# Start the daemon
python scheduler.py
```

### Schedule map

Each source's `update_frequency` in `config.yaml` maps to a cron trigger:

| update_frequency | When it runs |
|-----------------|--------------|
| `daily`   | Every day at 03:00 UTC |
| `weekly`  | Every Sunday at 03:00 UTC |
| `monthly` | 1st of each month at 03:00 UTC |
| `annual`  | January 1st at 03:00 UTC |

You can override the schedule per-source in `manifest.yaml`:
```yaml
sources:
  UN/Comtrade:
    status: live
    schedule: weekly   # overrides config.yaml update_frequency
```

### Install as systemd service

```bash
# Copy the service file
sudo cp systemd/opentrade-scheduler.service /etc/systemd/system/

# Edit credentials
sudo nano /etc/systemd/system/opentrade-scheduler.service
# → Set DATABASE_URL, COMTRADE_API_KEY, etc.

# Enable and start
sudo systemctl daemon-reload
sudo systemctl enable opentrade-scheduler
sudo systemctl start opentrade-scheduler

# Check status
sudo systemctl status opentrade-scheduler
journalctl -u opentrade-scheduler -f
```

### Logs

```bash
tail -f /opt/opentrade/logs/scheduler.log
```

---

## runner.py CLI Reference

```bash
python runner.py run-all              # Dry-run all live sources
python runner.py run-all --ingest     # Run all live sources + write to DB
python runner.py run UN/Comtrade --ingest  # Run a single source
python runner.py status               # Show status + last run time for all sources
```

After each run, `manifest.yaml` is updated with:
- `last_run` — timestamp (UTC)
- `last_status` — `success` or `error`
- `records` — total records ingested

---

## Adding a New Source

New sources are automatically picked up by the scheduler once they're marked `live` in `manifest.yaml`.

```bash
python runner.py add IN/DGFT       # Scaffold the source
# → implement bootstrap.py
# → set status: live in config.yaml
# → update manifest.yaml: status: live
```

See [adding-a-source.md](adding-a-source.md) for the full guide.
