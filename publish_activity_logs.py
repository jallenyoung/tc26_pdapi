"""
Converts TCM activity log .txt files into a published Tableau data source.

Usage:
    python samples/activity_logs_to_tableau.py

Steps performed:
    1. Parse JSON-lines log files from ACTIVITY_LOGS_DIR → flatten →
        one CSV per event type
    2. Build a .hyper extract with one table per event type in an Extract schema
    3. Wrap the .hyper in a .tdsx archive (TDS + Hyper)
    4. Publish the .tdsx to Tableau Cloud / Tableau Server

Any required values not found in the environment or .env file will be
prompted for interactively at startup.

Optional env vars (all values can also be entered interactively):
    TABLEAU_SERVER_URL   e.g. https://10ax.online.tableau.com
    TABLEAU_SITE_ID      Site content URL ("" = default site)
    TABLEAU_TOKEN_NAME   Personal Access Token name
    TABLEAU_TOKEN_VALUE  Personal Access Token secret
    ACTIVITY_LOGS_DIR    Source folder          (default: activity_logs)
    OUTPUT_DIR           Working/output folder  (default: .tableau_output)
    DS_NAME              Published name         (default: TCM Activity Logs)

Dependencies (beyond the base library):
    pip install tableauhyperapi tableauserverclient
"""

import csv
import gzip
import json
import os
import re
import sys
import zipfile
from collections import defaultdict
from pathlib import Path
from textwrap import dedent

from dotenv import load_dotenv

load_dotenv()

# ---------------------------------------------------------------------------
# Dependency checks — give a helpful message before anything else fails
# ---------------------------------------------------------------------------

_missing = []
try:
    import tableauserverclient as TSC
except ImportError:
    _missing.append("tableauserverclient")

try:
    from tableauhyperapi import (
        Connection,
        CreateMode,
        HyperProcess,
        Inserter,
        SchemaName,
        SqlType,
        TableDefinition,
        TableName,
        Telemetry,
        NULLABLE,
    )
except ImportError:
    _missing.append("tableauhyperapi")

if _missing:
    print("Missing dependencies. Install with:")
    for pkg in _missing:
        print(f"  pip install {pkg}")
    sys.exit(1)


OUTPUT_DIR = Path(os.environ.get("OUTPUT_DIR", ".tableau_output"))

_T_INT = "int"
_T_FLOAT = "float"
_T_TEXT = "text"


# ---------------------------------------------------------------------------
# Interactive helpers
# ---------------------------------------------------------------------------


def _prompt(text: str, default: str = "") -> str:
    suffix = f" [{default}]" if default else ""
    value = input(f"{text}{suffix}: ").strip()
    return value or default


def gather_config() -> dict:
    """Read config from env vars, prompting interactively for anything missing."""
    print("\n--- Tableau connection ---")
    server_url = os.environ.get("TABLEAU_SERVER_URL") or _prompt("Server URL")
    site_id = os.environ.get(
        "TABLEAU_SITE_ID",
        _prompt("Site content URL (leave blank for default site)"),
    )
    token_name = os.environ.get("TABLEAU_TOKEN_NAME") or _prompt(
        "Personal Access Token name"
    )
    token_value = os.environ.get("TABLEAU_TOKEN_VALUE") or _prompt(
        "Personal Access Token secret"
    )

    print("\n--- Data source ---")
    logs_dir = Path(
        os.environ.get("ACTIVITY_LOGS_DIR")
        or _prompt("Activity logs directory", "activity_logs")
    )
    ds_name = os.environ.get("DS_NAME") or _prompt(
        "Data source name", "TCM Activity Logs"
    )

    return {
        "server_url": server_url,
        "site_id": site_id,
        "token_name": token_name,
        "token_value": token_value,
        "logs_dir": logs_dir,
        "ds_name": ds_name,
    }


def choose_project(server: "TSC.Server") -> str:
    """
    Browse the project hierarchy interactively and return the chosen project ID.

    Shows top-level projects first. Selecting a project with sub-projects lets
    the user drill down or confirm the current level. Typing 'back' navigates
    up one level.
    """
    print("\nFetching projects...", end=" ", flush=True)
    opts = TSC.RequestOptions(pagesize=1000)
    all_projects, _ = server.projects.get(opts)
    print(f"{len(all_projects)} found")

    # Build parent_id → sorted children map
    by_parent: dict = defaultdict(list)
    for p in all_projects:
        by_parent[p.parent_id].append(p)
    for children in by_parent.values():
        children.sort(key=lambda p: p.name.lower())

    breadcrumb: list = []  # path of ProjectItems navigated so far

    while True:
        parent_id = breadcrumb[-1].id if breadcrumb else None
        children = by_parent.get(parent_id, [])

        print()
        if breadcrumb:
            path_str = " > ".join(p.name for p in breadcrumb)
            print(f"  Location: {path_str}")
        else:
            print("--- Destination project ---")

        if children:
            for i, p in enumerate(children, 1):
                marker = "+" if by_parent.get(p.id) else " "
                print(f"  {i:>3})  [{marker}]  {p.name}")
            print()
            print("  [+] = has sub-projects")

            parts = ["number to select"]
            if breadcrumb:
                parts += ["Enter to use current location", "'back' to go up"]
            raw = _prompt(" / ".join(parts))
        else:
            # Leaf project — only option is confirm or back
            raw = _prompt("Use this project? Enter to confirm, or 'back' to go up", "")

        if raw == "" and breadcrumb:
            selected = breadcrumb[-1]
            path_str = " > ".join(p.name for p in breadcrumb)
            print(f"\n  Using project: {path_str}  (id: {selected.id})")
            return selected.id

        if raw.lower() == "back":
            if breadcrumb:
                breadcrumb.pop()
            else:
                print("  Already at the top level.")
            continue

        try:
            idx = int(raw) - 1
            if 0 <= idx < len(children):
                breadcrumb.append(children[idx])
            else:
                print(f"  Please enter a number between 1 and {len(children)}.")
        except ValueError:
            print("  Invalid input.")


# ---------------------------------------------------------------------------
# Step 1 — Parse log files → CSVs
# ---------------------------------------------------------------------------


def _extract_event_type(path: Path) -> str:
    """Derive event type from path. Handles directory structure and renamed files."""
    # Directory structure:  eventType=background_job/ActivityLog-…txt
    m = re.search(r"eventType=([^/\\]+)", str(path))
    if m:
        return m.group(1)
    # Renamed format: YYYY-MM-DD-HH[-pod-site]-eventType[-N].txt
    parts = path.stem.split("-")
    while parts and parts[-1].isdigit():
        parts.pop()
    if len(parts) > 4:
        return parts[-1]
    return path.stem


def _flatten(obj: dict, prefix: str = "") -> dict:
    """Recursively flatten a nested dict; lists become JSON strings."""
    out: dict = {}
    for k, v in obj.items():
        key = f"{prefix}_{k}" if prefix else k
        if isinstance(v, dict):
            out.update(_flatten(v, key))
        elif isinstance(v, list):
            out[key] = json.dumps(v) if v else ""
        else:
            out[key] = "" if v is None else v
    return out


def _read_jsonl(path: Path) -> list[dict]:
    """Read JSON-lines from a plain-text or gzip file, skipping bad lines."""
    rows: list[dict] = []
    opener = gzip.open if path.suffix in (".gz", ".gzip") else open
    try:
        with opener(path, "rt", encoding="utf-8", errors="replace") as fh:
            for line in fh:
                line = line.strip()
                if line:
                    try:
                        rows.append(json.loads(line))
                    except json.JSONDecodeError:
                        pass
    except Exception as exc:
        print(f"  warning: skipped {path.name}: {exc}")
    return rows


def parse_logs_to_csvs(logs_dir: Path, csv_dir: Path) -> list[Path]:
    """Group log files by event type, flatten JSON, and write one CSV each."""
    csv_dir.mkdir(parents=True, exist_ok=True)

    print(f"\nStep 1 — Parsing log files in '{logs_dir}/'")
    if not logs_dir.exists():
        print(f"  Error: '{logs_dir}' does not exist.")
        sys.exit(1)

    all_files = sorted(logs_dir.rglob("*.txt")) + sorted(logs_dir.rglob("*.gz"))
    by_type: dict[str, list[Path]] = defaultdict(list)
    for f in all_files:
        by_type[_extract_event_type(f)].append(f)

    if not by_type:
        print("  No log files found.")
        sys.exit(1)

    csv_paths: list[Path] = []
    for et, files in sorted(by_type.items()):
        print(f"  {et}: {len(files)} file(s)...", end=" ", flush=True)

        records: list[dict] = [_flatten(r) for f in files for r in _read_jsonl(f)]

        if not records:
            print("empty, skipping.")
            continue

        # Union of all keys in first-seen order
        seen_keys: set[str] = set()
        fieldnames: list[str] = []
        for r in records:
            for k in r:
                if k not in seen_keys:
                    fieldnames.append(k)
                    seen_keys.add(k)

        out_path = csv_dir / f"{et}.csv"
        with open(out_path, "w", newline="", encoding="utf-8") as fh:
            writer = csv.DictWriter(fh, fieldnames=fieldnames, extrasaction="ignore")
            writer.writeheader()
            writer.writerows(records)

        print(f"{len(records):,} rows → {out_path.name}")
        csv_paths.append(out_path)

    return csv_paths


# ---------------------------------------------------------------------------
# Step 2 — Build Hyper file
# ---------------------------------------------------------------------------


def _infer_col_type(values: list[str]) -> str:
    """Return _T_INT, _T_FLOAT, or _T_TEXT based on a sample of string values."""
    non_empty = [v for v in values if v]
    if not non_empty:
        return _T_TEXT
    try:
        for v in non_empty:
            int(v)
        return _T_INT
    except ValueError:
        pass
    try:
        for v in non_empty:
            float(v)
        return _T_FLOAT
    except ValueError:
        pass
    return _T_TEXT


def _to_hyper_value(val: str, type_tag: str):
    """Coerce a CSV string to the Python type expected by the Hyper inserter."""
    if not val:
        return None
    if type_tag == _T_INT:
        try:
            return int(val)
        except ValueError:
            return None
    if type_tag == _T_FLOAT:
        try:
            return float(val)
        except ValueError:
            return None
    return val


def _build_one_hyper(csv_path: Path, hyper_path: Path) -> int:
    """Write a single-table Hyper file from a CSV. Returns the row count."""
    table_name = csv_path.stem

    with open(csv_path, newline="", encoding="utf-8") as fh:
        reader = csv.reader(fh)
        headers = next(reader)
        sample: list[list[str]] = []
        for row in reader:
            sample.append(row)
            if len(sample) >= 500:
                break

    type_tags: list[str] = []
    cols: list[TableDefinition.Column] = []
    for i, h in enumerate(headers):
        col_vals = [r[i] for r in sample if i < len(r)]
        tag = _infer_col_type(col_vals)
        sql_type = {
            _T_INT: SqlType.big_int(),
            _T_FLOAT: SqlType.double(),
        }.get(tag, SqlType.text())
        cols.append(TableDefinition.Column(h, sql_type, NULLABLE))
        type_tags.append(tag)

    tbl_def = TableDefinition(TableName("Extract", table_name), cols)

    with HyperProcess(telemetry=Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU) as hyper:
        with Connection(
            hyper.endpoint, hyper_path, CreateMode.CREATE_AND_REPLACE
        ) as conn:
            conn.catalog.create_schema_if_not_exists(SchemaName("Extract"))
            conn.catalog.create_table(tbl_def)

            with open(csv_path, newline="", encoding="utf-8") as fh:
                reader = csv.reader(fh)
                next(reader)
                count = 0
                with Inserter(conn, tbl_def) as inserter:
                    for row in reader:
                        padded = (row + [""] * len(headers))[: len(headers)]
                        inserter.add_row(
                            [_to_hyper_value(v, t) for v, t in zip(padded, type_tags)]
                        )
                        count += 1
                    inserter.execute()

    return count


def build_hypers(csv_paths: list[Path], hyper_dir: Path) -> dict[str, Path]:
    """Create one Hyper file per CSV. Returns {event_type: hyper_path}."""
    print("\nStep 2 — Building Hyper extracts")
    hyper_dir.mkdir(parents=True, exist_ok=True)

    hypers: dict[str, Path] = {}
    for csv_path in csv_paths:
        table_name = csv_path.stem
        hyper_path = hyper_dir / f"{table_name}.hyper"
        print(f"  {table_name}...", end=" ", flush=True)
        count = _build_one_hyper(csv_path, hyper_path)
        print(f"{count:,} rows")
        hypers[table_name] = hyper_path

    return hypers


# ---------------------------------------------------------------------------
# Step 3 — Build TDSXs
# ---------------------------------------------------------------------------


def _tds_xml(ds_name: str, hyper_filename: str, t_name: str) -> str:
    """Generate a minimal TDS XML for a single-table Hyper extract."""
    return dedent(
        f"""\
        <?xml version='1.0' encoding='utf-8' ?>
        <datasource inline='true' name='{ds_name}' source-platform='win' version='18.1'>
          <connection authentication='auth-none' class='hyper'
              dbname='Data/Extracts/{hyper_filename}'
              server='localhost' username=''>
            <relation name='{t_name}' table='[Extract].[{t_name}]' type='table' />
          </connection>
        </datasource>
    """
    )


def _title(event_type: str) -> str:
    """Convert snake_case event type to Title Case:
    'background_job' → 'Background Job'."""
    return event_type.replace("_", " ").title()


def build_tdsxs(
    hypers: dict[str, Path],
    ds_name: str,
    output_dir: Path,
) -> list[Path]:
    """Create one TDSX per event type. Returns list of TDSX paths."""
    print("\nStep 3 — Building TDSX files")
    output_dir.mkdir(parents=True, exist_ok=True)

    tdsx_paths: list[Path] = []
    for table_name, hyper_path in hypers.items():
        full_name = f"{ds_name} - {_title(table_name)}"
        safe_name = re.sub(r"[^\w\- ]", "", full_name).strip()
        tdsx_path = output_dir / f"{safe_name}.tdsx"
        tds_content = _tds_xml(full_name, hyper_path.name, table_name)

        with zipfile.ZipFile(tdsx_path, "w", zipfile.ZIP_DEFLATED) as zf:
            zf.writestr(f"{safe_name}.tds", tds_content)
            zf.write(hyper_path, f"Data/Extracts/{hyper_path.name}")

        print(f"  Created: {tdsx_path.name}")
        tdsx_paths.append(tdsx_path)

    return tdsx_paths


# ---------------------------------------------------------------------------
# Step 4 — Publish
# ---------------------------------------------------------------------------


def publish_datasources(cfg: dict, project_id: str, tdsx_paths: list[Path]) -> None:
    """Publish all TDSXs to Tableau Cloud / Tableau Server in one session."""
    print("\nStep 4 — Publishing to Tableau")

    auth = TSC.PersonalAccessTokenAuth(
        cfg["token_name"], cfg["token_value"], cfg["site_id"]
    )
    server = TSC.Server(cfg["server_url"], use_server_version=True)

    print(f"  Connecting to {cfg['server_url']}...", end=" ", flush=True)
    with server.auth.sign_in(auth):
        print("OK\n")
        success, failed = 0, 0
        for tdsx_path in tdsx_paths:
            ds_name = tdsx_path.stem
            print(f"  Publishing '{ds_name}'...", end=" ", flush=True)
            try:
                ds_item = TSC.DatasourceItem(project_id, name=ds_name)
                server.datasources.publish(ds_item, str(tdsx_path), "Overwrite")
                print("OK")
                success += 1
            except Exception as exc:
                print(f"FAILED  ({exc})")
                failed += 1

    print(f"\n  Complete — {success} published, {failed} failed.")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    print("=" * 60)
    print("  TCM Activity Logs → Tableau Data Sources")
    print("=" * 60)

    cfg = gather_config()

    # Connect early to validate credentials and let the user pick a project
    # before spending time on log processing.
    print("\nStep 4 (prep) — Connecting to Tableau")
    auth = TSC.PersonalAccessTokenAuth(
        cfg["token_name"], cfg["token_value"], cfg["site_id"]
    )
    ts_server = TSC.Server(cfg["server_url"], use_server_version=True)
    print(f"  Connecting to {cfg['server_url']}...", end=" ", flush=True)
    with ts_server.auth.sign_in(auth):
        print("OK")
        project_id = choose_project(ts_server)

    csv_dir = OUTPUT_DIR / "csv"
    hyper_dir = OUTPUT_DIR / "extract"

    csv_paths = parse_logs_to_csvs(cfg["logs_dir"], csv_dir)
    hypers = build_hypers(csv_paths, hyper_dir)
    tdsx_paths = build_tdsxs(hypers, cfg["ds_name"], OUTPUT_DIR)
    publish_datasources(cfg, project_id, tdsx_paths)


if __name__ == "__main__":
    main()
