"""
Standalone script for browsing and downloading Tableau Cloud Manager activity logs.

This file has no dependency on the platform_data_client library.

Installation (one-time):
    pip install requests python-dotenv

Usage:
    python download_activity_logs_standalone.py

Credentials are loaded from environment variables (or a .env file):
    PDC_PAT_NAME      Personal Access Token name
    PDC_PAT_SECRET    Personal Access Token secret
    PDC_TENANT_ID     Tableau Cloud Manager tenant ID

    Note: PAT must be generated from the Tableau Cloud Manager user settings.

Optional:
    PDC_OUTPUT_DIR      Download destination directory (default: ./activity_logs)
    PDC_MAX_RETRIES     Max HTTP retry attempts (default: 5)
    PDC_BACKOFF_FACTOR  Retry backoff multiplier (default: 1.0)
"""

import os
import re
import sys
import getpass
import logging
import requests

from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

try:
    from dotenv import load_dotenv

    load_dotenv()
except ImportError:
    pass  # python-dotenv is optional

logger = logging.getLogger("tcm_activity_logs")

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_TCM_URL = "https://cloudmanager.tableau.com"
_DEFAULT_MAX_RETRIES = 5
_DEFAULT_BACKOFF_FACTOR = 1.0
_RETRY_STATUS_CODES = [429, 500, 502, 503, 504]
# ---------------------------------------------------------------------------
# Exceptions
# ---------------------------------------------------------------------------


class TCMError(Exception):
    pass


class TCMRequestError(TCMError):
    def __init__(self, code: int, detail: str) -> None:
        self.code = code
        self.detail = detail
        super().__init__(code, detail)

    def __str__(self) -> str:
        return f"HTTP {self.code}: {self.detail}"


class TCMAuthError(TCMError):
    pass


class TCMNotSignedInError(TCMAuthError):
    def __init__(self) -> None:
        super().__init__("Not signed in. Call server.auth.sign_in() first.")


# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------


@dataclass
class SessionItem:
    session_token: str
    user_id: str
    tenant_id: str
    session_expiration: Optional[str] = None

    @classmethod
    def from_response(cls, data: dict) -> "SessionItem":
        return cls(
            session_token=data["sessionToken"],
            user_id=data["userId"],
            tenant_id=data["tenantId"],
            session_expiration=data.get("sessionExpiration"),
        )


@dataclass
class SiteItem:
    site_id: str
    name: str
    content_url: str
    tenant_id: Optional[str] = None
    pod_location: Optional[str] = None
    status: Optional[str] = None

    @classmethod
    def from_response(cls, data: dict) -> "SiteItem":
        return cls(
            site_id=data.get("siteUUID") or data.get("siteId") or data.get("id", ""),
            name=data.get("name", ""),
            content_url=data.get("contentUrl", ""),
            tenant_id=data.get("tenantId"),
            pod_location=data.get("podLocation"),
            status=data.get("status"),
        )


@dataclass
class ActivityLogFile:
    file_path: str
    size: Optional[int] = None

    @classmethod
    def from_response(cls, data: dict) -> "ActivityLogFile":
        return cls(
            file_path=data.get("path") or data.get("filePath", ""),
            size=data.get("size"),
        )


@dataclass
class ActivityLogDownload:
    url: str
    expiration: Optional[str] = None

    @classmethod
    def from_response(cls, data: dict) -> "ActivityLogDownload":
        return cls(
            url=data.get("url", ""),
            expiration=data.get("expiration") or data.get("expiresAt"),
        )


# ---------------------------------------------------------------------------
# Auth credential types
# ---------------------------------------------------------------------------


class PersonalAccessTokenAuth:
    def __init__(self, token_name: str, personal_access_token: str) -> None:
        if not personal_access_token or not token_name:
            raise ValueError(
                "Must provide a token and token name for PAT authentication"
            )
        self.token_name = token_name
        self.personal_access_token = personal_access_token


# ---------------------------------------------------------------------------
# Request options
# ---------------------------------------------------------------------------


@dataclass
class RequestOptions:
    page_number: int = 0
    page_size: int = 100

    def to_params(self) -> dict:
        return {"pageNumber": self.page_number, "pageSize": self.page_size}


@dataclass
class ActivityLogOptions:
    start_time: Optional[str] = None
    end_time: Optional[str] = None
    event_type: Optional[str] = None
    page_size: int = 100
    page_token: Optional[str] = None

    def to_params(self) -> dict:
        params: dict = {"pageSize": self.page_size}
        if self.start_time:
            params["startTime"] = self.start_time
        if self.end_time:
            params["endTime"] = self.end_time
        if self.event_type:
            params["eventType"] = self.event_type
        if self.page_token:
            params["pageToken"] = self.page_token
        return params


# ---------------------------------------------------------------------------
# Server
# ---------------------------------------------------------------------------


def _build_headers(session_token: str = None) -> dict:
    headers = {"Content-Type": "application/json", "Accept": "application/json"}
    if session_token:
        headers["x-tableau-session-token"] = session_token
    return headers


class Server:
    """Minimal TCM API client — auth, sites listing, and activity log downloads."""

    def __init__(self, server_address: str = _TCM_URL) -> None:
        self._server_address = server_address.rstrip("/")

        max_retries = int(os.environ.get("PDC_MAX_RETRIES", _DEFAULT_MAX_RETRIES))
        backoff_factor = float(
            os.environ.get("PDC_BACKOFF_FACTOR", _DEFAULT_BACKOFF_FACTOR)
        )

        retry = Retry(
            total=max_retries,
            backoff_factor=backoff_factor,
            status_forcelist=_RETRY_STATUS_CODES,
            respect_retry_after_header=True,
        )
        adapter = HTTPAdapter(max_retries=retry)
        self._session = requests.Session()
        self._session.mount("https://", adapter)
        self._session.mount("http://", adapter)

        self._auth_token: str | None = None
        self._tenant_id: str | None = None
        self._user_id: str | None = None

        self.auth = self._Auth(self)
        self.sites = self._Sites(self)
        self.platform_data = self._PlatformData(self)

    def _build_url(self, *parts: str) -> str:
        return "/".join([self._server_address] + [p.strip("/") for p in parts])

    def _ensure_signed_in(self) -> None:
        if not self._auth_token:
            raise TCMNotSignedInError()

    def _check_response(self, response: requests.Response) -> None:
        if not response.ok:
            raise TCMRequestError(response.status_code, response.text)

    def __enter__(self) -> "Server":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        if self._auth_token:
            try:
                self.auth.sign_out()
            except Exception:
                pass

    # ------------------------------------------------------------------
    # Nested endpoint classes
    # ------------------------------------------------------------------

    class _Auth:
        def __init__(self, server: "Server") -> None:
            self._s = server

        def sign_in(self, auth_item: PersonalAccessTokenAuth) -> SessionItem:
            url = self._s._build_url("api/v1/pat/login")
            body = {"token": auth_item.personal_access_token}
            response = self._s._session.post(url, json=body, headers=_build_headers())
            self._s._check_response(response)
            session = SessionItem.from_response(response.json())
            self._s._auth_token = session.session_token
            self._s._tenant_id = session.tenant_id
            self._s._user_id = session.user_id
            return session

        def sign_out(self) -> None:
            self._s._ensure_signed_in()
            url = self._s._build_url("api/v2/sessions")
            response = self._s._session.delete(
                url, headers=_build_headers(self._s._auth_token)
            )
            self._s._check_response(response)
            self._s._auth_token = None
            self._s._tenant_id = None
            self._s._user_id = None

    class _Sites:
        def __init__(self, server: "Server") -> None:
            self._s = server

        def list(
            self, tenant_id: str, request_opts: RequestOptions = None
        ) -> tuple[list[SiteItem], None]:
            self._s._ensure_signed_in()
            url = self._s._build_url("api/v1/tenants", tenant_id, "sites")
            params = (request_opts or RequestOptions()).to_params()
            response = self._s._session.get(
                url, params=params, headers=_build_headers(self._s._auth_token)
            )
            self._s._check_response(response)
            data = response.json()
            sites = [SiteItem.from_response(s) for s in data.get("sites", [])]
            return sites, None

    class _PlatformData:
        def __init__(self, server: "Server") -> None:
            self._s = server

        def list_activity_logs(
            self,
            tenant_id: str,
            options: ActivityLogOptions = None,
        ) -> tuple[list[ActivityLogFile], str | None]:
            self._s._ensure_signed_in()
            url = self._s._build_url("api/v1/tenants", tenant_id, "activitylog")
            params = (options or ActivityLogOptions()).to_params()
            response = self._s._session.get(
                url, params=params, headers=_build_headers(self._s._auth_token)
            )
            self._s._check_response(response)
            data = response.json()
            files = [ActivityLogFile.from_response(f) for f in data.get("files", [])]
            return files, data.get("pageToken")

        def get_activity_log_urls(
            self, tenant_id: str, file_paths: list[str], batch_size: int = 20
        ) -> list[ActivityLogDownload]:
            self._s._ensure_signed_in()
            url = self._s._build_url("api/v1/tenants", tenant_id, "activitylog")
            downloads: list[ActivityLogDownload] = []
            for i in range(0, len(file_paths), batch_size):
                batch = file_paths[i : i + batch_size]
                response = self._s._session.post(
                    url,
                    json={"files": batch},
                    headers=_build_headers(self._s._auth_token),
                )
                self._s._check_response(response)
                data = response.json()
                downloads.extend(
                    ActivityLogDownload.from_response(d) for d in data.get("files", [])
                )
            return downloads

        def list_site_activity_logs(
            self,
            tenant_id: str,
            site_id: str,
            options: ActivityLogOptions = None,
        ) -> tuple[list[ActivityLogFile], str | None]:
            self._s._ensure_signed_in()
            url = self._s._build_url(
                "api/v1/tenants", tenant_id, "sites", site_id, "activitylog"
            )
            params = (options or ActivityLogOptions()).to_params()
            response = self._s._session.get(
                url, params=params, headers=_build_headers(self._s._auth_token)
            )
            self._s._check_response(response)
            data = response.json()
            files = [ActivityLogFile.from_response(f) for f in data.get("files", [])]
            return files, data.get("pageToken")

        def get_site_activity_log_urls(
            self,
            tenant_id: str,
            site_id: str,
            file_paths: list[str],
            batch_size: int = 20,
        ) -> list[ActivityLogDownload]:
            self._s._ensure_signed_in()
            url = self._s._build_url(
                "api/v1/tenants", tenant_id, "sites", site_id, "activitylog"
            )
            downloads: list[ActivityLogDownload] = []
            for i in range(0, len(file_paths), batch_size):
                batch = file_paths[i : i + batch_size]
                response = self._s._session.post(
                    url,
                    json={"files": batch},
                    headers=_build_headers(self._s._auth_token),
                )
                self._s._check_response(response)
                data = response.json()
                downloads.extend(
                    ActivityLogDownload.from_response(d) for d in data.get("files", [])
                )
            return downloads


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _fmt_size(size_bytes: int | None) -> str:
    if size_bytes is None:
        return "unknown size"
    for unit in ("B", "KB", "MB", "GB"):
        if size_bytes < 1024:
            return f"{size_bytes:.1f} {unit}"
        size_bytes /= 1024
    return f"{size_bytes:.1f} TB"


def _prompt(text: str, default: str = "") -> str:
    suffix = f" [{default}]" if default else ""
    value = input(f"{text}{suffix}: ").strip()
    return value or default


def _parse_selection(raw: str, total: int) -> list[int] | None:
    """
    Parse a selection string into a list of 0-based indices.

    Accepts:
        all        — select everything
        1          — single item
        1,3,5      — comma-separated items
        2-5        — inclusive range
        1,3-5,7    — mixed
    Returns None if input is invalid.
    """
    if raw.lower() == "all":
        return list(range(total))

    indices = set()
    for part in raw.split(","):
        part = part.strip()
        if "-" in part:
            try:
                lo, hi = part.split("-", 1)
                lo, hi = int(lo), int(hi)
                if lo < 1 or hi > total or lo > hi:
                    return None
                indices.update(range(lo - 1, hi))
            except ValueError:
                return None
        else:
            try:
                n = int(part)
                if n < 1 or n > total:
                    return None
                indices.add(n - 1)
            except ValueError:
                return None

    return sorted(indices)


# ---------------------------------------------------------------------------
# Auth
# ---------------------------------------------------------------------------


def sign_in(server: Server) -> None:
    print("\n--- Authentication ---")
    token_name = os.environ.get("PDC_PAT_NAME") or _prompt("PAT name")
    token_secret = os.environ.get("PDC_PAT_SECRET") or getpass.getpass("PAT secret: ")
    auth = PersonalAccessTokenAuth(token_name, token_secret)
    print("Signing in...", end=" ", flush=True)
    session = server.auth.sign_in(auth)
    print(f"OK  (tenant: {session.tenant_id}, user: {session.user_id})")


# ---------------------------------------------------------------------------
# Scope — tenant or site
# ---------------------------------------------------------------------------


def choose_scope(server: Server, tenant_id: str):
    """
    Ask the user whether they want tenant-level or site-level logs.

    Returns (scope, site) where scope is 'tenant' or 'site' and site is a
    SiteItem (for site scope) or None (for tenant scope).
    """
    print("\n--- Log scope ---")
    print("  1) Tenant-level logs")
    print("  2) Site-level logs")
    choice = _prompt("Select scope", "1")

    if choice != "2":
        return "tenant", None

    print("\nFetching sites...", end=" ", flush=True)
    sites, _ = server.sites.list(tenant_id, request_opts=RequestOptions(page_size=100))
    print(f"{len(sites)} site(s) found")

    if not sites:
        print("No sites available. Falling back to tenant-level logs.")
        return "tenant", None

    print()
    for i, site in enumerate(sites, 1):
        print(f"  {i:>3})  {site.name!r:<30}  {site.content_url}  ({site.status})")

    raw = _prompt("\nSelect site number")
    try:
        idx = int(raw) - 1
        if idx < 0 or idx >= len(sites):
            raise ValueError
    except ValueError:
        print("Invalid selection. Falling back to tenant-level logs.")
        return "tenant", None

    selected = sites[idx]
    print(f"Using site: {selected.name!r}  ({selected.site_id})")
    return "site", selected


# ---------------------------------------------------------------------------
# Date range
# ---------------------------------------------------------------------------


def choose_date_range() -> tuple[str, str]:
    now = datetime.now(timezone.utc)
    default_start = (now - timedelta(days=1)).strftime("%Y-%m-%dT%H:%M:%SZ")
    default_end = now.strftime("%Y-%m-%dT%H:%M:%SZ")

    print("\n--- Date range (UTC, format: YYYY-MM-DDTHH:MM:SSZ) ---")
    start = _prompt("Start time", default_start)
    end = _prompt("End time", default_end)
    return start, end


# ---------------------------------------------------------------------------
# List files
# ---------------------------------------------------------------------------


def collect_all_files(
    server: Server,
    tenant_id: str,
    site_id: str | None,
    start_time: str,
    end_time: str,
) -> list[ActivityLogFile]:
    """Page through all available log files for the chosen scope and time range."""
    opts = ActivityLogOptions(start_time=start_time, end_time=end_time, page_size=100)
    all_files: list[ActivityLogFile] = []

    print("\nFetching available log files", end="", flush=True)
    while True:
        if site_id:
            files, next_token = server.platform_data.list_site_activity_logs(
                tenant_id, site_id, options=opts
            )
        else:
            files, next_token = server.platform_data.list_activity_logs(
                tenant_id, options=opts
            )
        all_files.extend(files)
        print(".", end="", flush=True)

        if not next_token:
            break
        opts.page_token = next_token

    print(f"  {len(all_files)} file(s) found")
    return all_files


def _event_type(file: ActivityLogFile) -> str:
    m = re.search(r"eventType=([^/]+)", file.file_path)
    return m.group(1) if m else "unknown"


def _group_by_event_type(
    files: list[ActivityLogFile],
) -> dict[str, list[ActivityLogFile]]:
    groups: dict[str, list[ActivityLogFile]] = defaultdict(list)
    for f in files:
        groups[_event_type(f)].append(f)
    return dict(sorted(groups.items()))


def display_files_grouped(
    groups: dict[str, list[ActivityLogFile]],
) -> list[str]:
    """Print the grouped summary and return the ordered list of event type keys."""
    event_types = list(groups.keys())
    type_w = max(len(t) for t in event_types) + 2
    print()
    print(f"  {'#':>4}  {'Event Type':<{type_w}}  {'Files':>5}  {'Total Size':>10}")
    print(f"  {'-' * 4}  {'-' * type_w}  {'-' * 5}  {'-' * 10}")
    for i, et in enumerate(event_types, 1):
        group = groups[et]
        total = sum(f.size for f in group if f.size)
        print(f"  {i:>4})  {et:<{type_w}}  {len(group):>5}  {_fmt_size(total):>10}")
    return event_types


def display_files_flat(files: list[ActivityLogFile]) -> None:
    """Print a flat numbered list of individual files."""
    type_w = max(len(_event_type(f)) for f in files) + 2
    print()
    print(f"  {'#':>4}  {'Event Type':<{type_w}}  Size")
    print(f"  {'-' * 4}  {'-' * type_w}  --------")
    for i, f in enumerate(files, 1):
        print(f"  {i:>4})  {_event_type(f):<{type_w}}  {_fmt_size(f.size)}")


# ---------------------------------------------------------------------------
# Selection
# ---------------------------------------------------------------------------


def choose_files(files: list[ActivityLogFile]) -> list[ActivityLogFile]:
    groups = _group_by_event_type(files)
    event_types = display_files_grouped(groups)

    print()
    print("Select event types to download (selects all files of those types).")
    print("  Examples:  all   |   1   |   1,3,5   |   2-5   |   1,3-5,7")
    print("  Type 'files' to switch to individual file selection.")

    while True:
        raw = _prompt("Selection (or 'q' to quit)", "all")

        if raw.lower() == "q":
            return []

        if raw.lower() == "files":
            display_files_flat(files)
            print()
            print("Enter file numbers to download.")
            print("  Examples:  all   |   1   |   1,3,5   |   2-5   |   1,3-5,7")
            while True:
                raw2 = _prompt("Selection (or 'back' to return, 'q' to quit)", "all")
                if raw2.lower() == "q":
                    return []
                if raw2.lower() == "back":
                    event_types = display_files_grouped(groups)
                    print()
                    print("Select event types to download.")
                    print(
                        "  Examples:  all   |   1   |   1,3,5   |   2-5   |   1,3-5,7"
                    )
                    print("  Type 'files' to switch to individual file selection.")
                    break
                indices = _parse_selection(raw2, len(files))
                if indices is None:
                    print(f"  Invalid — enter numbers between 1 and {len(files)}.")
                    continue
                selected = [files[i] for i in indices]
                total_size = sum(f.size for f in selected if f.size)
                size_str = _fmt_size(total_size)
                print(f"\n  {len(selected)} file(s) selected  ({size_str} total)")
                confirm = _prompt("Proceed with download? (y/n)", "y")
                if confirm.lower() == "y":
                    return selected
                print("Selection cleared — choose again.")
            continue

        indices = _parse_selection(raw, len(event_types))
        if indices is None:
            print(f"  Invalid — enter numbers between 1 and {len(event_types)}.")
            continue

        selected = [f for i in indices for f in groups[event_types[i]]]
        total_size = sum(f.size for f in selected if f.size)
        print(
            f"\n  {len(selected)} file(s) selected across "
            f"{len(indices)} event type(s)  ({_fmt_size(total_size)} total)"
        )
        confirm = _prompt("Proceed with download? (y/n)", "y")
        if confirm.lower() == "y":
            return selected
        print("Selection cleared — choose again.")


# ---------------------------------------------------------------------------
# Download
# ---------------------------------------------------------------------------


def _make_filenames(
    file_paths: list[str],
    pod: str | None = None,
    site_name: str | None = None,
) -> list[str]:
    """
    Build descriptive filenames from log file paths.

    Tenant-level: YYYY-MM-DD-HH-eventType.txt
    Site-level:   YYYY-MM-DD-HH-pod-siteName-eventType.txt
    Duplicates:   … -2.txt, -3.txt, etc.
    """

    def _slugify(s: str) -> str:
        return re.sub(r"[^A-Za-z0-9]", "_", s).strip("_")

    def _base(fp: str) -> str:
        parts: dict[str, str] = {}
        for seg in fp.split("/"):
            if "=" in seg:
                k, v = seg.split("=", 1)
                parts[k] = v
        y = parts.get("y", "0000")
        m = parts.get("m", "00").zfill(2)
        d = parts.get("d", "00").zfill(2)
        h = parts.get("h", "00").zfill(2)
        et = parts.get("eventType", "unknown")
        if pod and site_name:
            return f"{y}-{m}-{d}-{h}-{_slugify(pod)}-{_slugify(site_name)}-{et}"
        return f"{y}-{m}-{d}-{h}-{et}"

    bases = [_base(fp) for fp in file_paths]
    counts = defaultdict(int)
    for b in bases:
        counts[b] += 1

    seen: dict[str, int] = defaultdict(int)
    names = []
    for b in bases:
        seen[b] += 1
        if counts[b] > 1:
            names.append(f"{b}-{seen[b]}.txt")
        else:
            names.append(f"{b}.txt")
    return names


def download_files(
    server: Server,
    tenant_id: str,
    site_id: str | None,
    selected: list[ActivityLogFile],
    output_dir: Path,
    pod: str | None = None,
    site_name: str | None = None,
) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    print(f"\nDownloading to: {output_dir.resolve()}\n")

    file_paths = [f.file_path for f in selected]
    filenames = _make_filenames(file_paths, pod=pod, site_name=site_name)

    print("Fetching signed download URLs...", end=" ", flush=True)
    if site_id:
        downloads = server.platform_data.get_site_activity_log_urls(
            tenant_id, site_id, file_paths
        )
    else:
        downloads = server.platform_data.get_activity_log_urls(tenant_id, file_paths)
    print("OK")

    success, failed = 0, 0
    for dl, filename in zip(downloads, filenames):
        dest = output_dir / filename

        try:
            print(f"  Downloading {filename}...", end=" ", flush=True)
            response = requests.get(dl.url, timeout=120)
            response.raise_for_status()
            dest.write_bytes(response.content)
            print(f"OK  ({_fmt_size(len(response.content))})")
            success += 1
        except Exception as exc:
            print(f"FAILED  ({exc})")
            failed += 1

    print(f"\nComplete — {success} downloaded, {failed} failed.")
    if success:
        print(f"Files saved to: {output_dir.resolve()}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    print("=" * 60)
    print("  Tableau Cloud Manager — Activity Log Downloader")
    print("=" * 60)

    tenant_id = os.environ.get("PDC_TENANT_ID") or _prompt("Tenant ID")
    output_dir = Path(
        os.environ.get("PDC_OUTPUT_DIR") or _prompt("Output directory", "activity_logs")
    )

    with Server() as server:
        try:
            sign_in(server)
        except TCMRequestError as exc:
            print(f"\nAuthentication failed: {exc}")
            sys.exit(1)

        scope, site = choose_scope(server, tenant_id)
        site_id = site.site_id if site else None
        pod = site.pod_location if site else None
        site_name = site.name if site else None

        start_time, end_time = choose_date_range()

        try:
            all_files = collect_all_files(
                server, tenant_id, site_id, start_time, end_time
            )
        except TCMRequestError as exc:
            print(f"\nFailed to list activity logs: {exc}")
            sys.exit(1)

        if not all_files:
            print("\nNo log files found for the specified time range.")
            sys.exit(0)

        while True:
            selected = choose_files(all_files)

            if not selected:
                print("\nNo files selected. Exiting.")
                sys.exit(0)

            try:
                download_files(
                    server,
                    tenant_id,
                    site_id,
                    selected,
                    output_dir,
                    pod=pod,
                    site_name=site_name,
                )
            except TCMRequestError as exc:
                print(f"\nDownload failed: {exc}")
                sys.exit(1)

            again = _prompt("\nDownload more files? (y/n)", "n")
            if again.lower() != "y":
                break


if __name__ == "__main__":
    main()
