# tc26_pdapi

Sample scripts demonstrating capabilities and use cases for the **Tableau Cloud Platform Data API (PD API)**. Initially demo'd at Tableau Conference 2026 by Jay Young.

## What's included

| Script | What it does |
| ------ | ------------ |
| `download_activity_logs.py` | Connects to Tableau Cloud Manager (TCM), browses available activity log files, and downloads them locally as JSON-lines `.txt` files |
| `publish_activity_logs.py` | Parses the downloaded log files, builds a multi-table Hyper extract (one table per event type), and publishes it to Tableau Cloud or Tableau Server as a live data source |

Run them in order: download first, then publish.

## Prerequisites

- Python 3.9+
- A Tableau Cloud Manager (TCM) account with a Personal Access Token (PAT)
- For publishing: a Tableau Cloud or Tableau Server site

## Installation

```bash
# 1. Clone the repo
git clone https://github.com/YOUR_USERNAME/tc26_pdapi.git
cd tc26_pdapi

# 2. Install dependencies
pip install -r requirements.txt
```

## Configuration

Copy `.env.example` to `.env` and fill in your values:

```bash
cp .env.example .env
```

The file is split into two sections — one for each script. Any values left blank will be prompted for interactively at runtime.

## Usage

### Step 1 — Download activity logs

```bash
python download_activity_logs.py [options]
```

The script will list available log files from TCM and download them to `./activity_logs/` (configurable via `PDC_OUTPUT_DIR`).

By default the script runs interactively, prompting you to choose the log scope and which event types to download. Any of these inputs can be passed as arguments to skip the corresponding prompt — useful for automated or scheduled runs.

| Argument | Description |
| -------- | ----------- |
| `--scope {tenant,site}` | Log scope. Prompts interactively if omitted. |
| `--site-id <uuid>` | Site UUID; only used when `--scope site`. If omitted, shows an interactive site picker. Also reads from `PDC_SITE_ID` env var. |
| `--event-types <types>` | Comma-separated event types to download, or `all`. Prompts interactively if omitted. |
| `--start-time <datetime>` | Start of date range (`YYYY-MM-DDTHH:MM:SSZ`). Defaults to yesterday at midnight UTC. |
| `--end-time <datetime>` | End of date range (`YYYY-MM-DDTHH:MM:SSZ`). Defaults to today at midnight UTC. |
| `--output-dir <path>` | Download destination. Overrides `PDC_OUTPUT_DIR` env var. |
| `--log-dir <path>` | Write a timestamped log file for this run to the given directory (e.g. `--log-dir logs`). Each run creates a new file named `tcm_download_YYYY-MM-DD_HH-MM-SS.log`. |

#### Examples

```bash
# Fully interactive (original behavior)
python download_activity_logs.py

# Skip scope and file-type prompts — download all tenant logs for yesterday
python download_activity_logs.py --scope tenant --event-types all

# Download specific event types for a site
python download_activity_logs.py --scope site --site-id <uuid> --event-types background_job,flow

# Fully automated — no prompts, with a timestamped log file per run
python download_activity_logs.py --scope tenant --event-types all --output-dir ./logs --log-dir ./run-logs

# Override the date range
python download_activity_logs.py --scope tenant --event-types all \
  --start-time 2026-05-01T00:00:00Z --end-time 2026-05-02T00:00:00Z
```

When `--scope` or `--event-types` are omitted the script falls back to its interactive menus, so the two modes can be mixed freely.

### Step 2 — Publish to Tableau

> **Note:** `publish_activity_logs.py` is a proof-of-concept intended for demonstration purposes only. Each run creates new extract files rather than appending to existing ones. It will require modification before use in a production environment.

```bash
python publish_activity_logs.py
```

The script will:

1. Parse the JSON-lines log files in `./activity_logs/`
2. Flatten each event type into its own CSV
3. Build a `.hyper` extract with one table per event type
4. Package it as a `.tdsx` and publish it to your Tableau site as **TCM Activity Logs**

The published data source will be available immediately in Tableau Cloud/Server for analysis.

## License

MIT — see [LICENSE](LICENSE) for details.
