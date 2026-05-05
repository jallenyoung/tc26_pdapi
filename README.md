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
pip install requests python-dotenv              # for download_activity_logs.py
pip install tableauhyperapi tableauserverclient  # for publish_activity_logs.py
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
python download_activity_logs.py
```

The script will list available log files from TCM and download them to `./activity_logs/` (configurable via `PDC_OUTPUT_DIR`).

### Step 2 — Publish to Tableau

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
