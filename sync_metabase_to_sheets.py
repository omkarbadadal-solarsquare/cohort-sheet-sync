#!/usr/bin/env python3
"""Chunked Metabase SQL -> Google Sheets synchronisation.

Scheduled GitHub Actions runs refresh only 2026-10-01 through today (IST).
Manual GitHub Actions runs rebuild the full 2026-03-01 through today (IST).
"""

from __future__ import annotations

import json
import logging
import os
import sqlite3
import sys
import tempfile
import time
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Iterable
from zoneinfo import ZoneInfo

import gspread
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

IST = ZoneInfo("Asia/Kolkata")
ROOT = Path(__file__).resolve().parent
SQL_FILE = ROOT / "sql" / "cohort_query.sql"

START_DATE_DEFAULT = date(2026, 3, 1)
RECENT_START_DEFAULT = date(2026, 10, 1)

SHEET_HEADERS = [
    "Lead ID",
    "Created Date IST",
    "MS Date IST",
    "Channel",
    "LQI",
    "Slot",
    "Cluster",
    "MD Date IST",
    "Meeting By",
    "MS Day of the Week",
    "Meeting Confirmed Date Time IST",
    "RTM or Prescheduled",
]
SQL_HEADERS = SHEET_HEADERS + ["_dedupe_internal_id"]


def env_date(name: str, default: date) -> date:
    raw = os.getenv(name, "").strip()
    if not raw:
        return default
    try:
        return date.fromisoformat(raw)
    except ValueError as exc:
        raise ValueError(f"{name} must be YYYY-MM-DD; received {raw!r}") from exc


def env_int(name: str, default: int, minimum: int = 1) -> int:
    raw = os.getenv(name, "").strip()
    value = default if not raw else int(raw)
    if value < minimum:
        raise ValueError(f"{name} must be at least {minimum}")
    return value


def iter_windows(start: date, end_exclusive: date, chunk_days: int) -> Iterable[tuple[date, date]]:
    cursor = start
    while cursor < end_exclusive:
        nxt = min(cursor + timedelta(days=chunk_days), end_exclusive)
        yield cursor, nxt
        cursor = nxt


def normalise_timestamp(value: Any) -> str:
    """Return a sortable timestamp string; empty means SQL NULL."""
    if value is None or str(value).strip() == "":
        return ""
    raw = str(value).strip()
    try:
        dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        if dt.tzinfo is not None:
            dt = dt.astimezone(IST).replace(tzinfo=None)
        return dt.strftime("%Y-%m-%d %H:%M:%S.%f")
    except ValueError:
        # Preserve lexical ordering for any other consistent server timestamp format.
        return raw.replace("T", " ")


def parse_sheet_date(value: Any) -> date | None:
    """Parse Created Date IST as written by this script or as a Sheets date value."""
    if value is None or str(value).strip() == "":
        return None
    raw = str(value).strip()
    try:
        dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        if dt.tzinfo is not None:
            dt = dt.astimezone(IST)
        return dt.date()
    except ValueError:
        pass

    for fmt in (
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d",
        "%m/%d/%Y %H:%M:%S",
        "%m/%d/%Y",
        "%d/%m/%Y %H:%M:%S",
        "%d/%m/%Y",
    ):
        try:
            return datetime.strptime(raw, fmt).date()
        except ValueError:
            continue
    # Google Sheets serial date fallback.
    try:
        serial = float(raw)
        return (datetime(1899, 12, 30) + timedelta(days=serial)).date()
    except (ValueError, OverflowError):
        return None


def make_metabase_session(api_key: str) -> requests.Session:
    session = requests.Session()
    retry = Retry(
        total=4,
        connect=4,
        read=2,
        backoff_factor=2,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(["POST"]),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=4, pool_maxsize=4)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    session.headers.update(
        {
            "X-API-Key": api_key,
            "Content-Type": "application/json",
            "Accept": "application/json",
        }
    )
    return session


def build_sql(template: str, start: date, end_exclusive: date) -> str:
    # Dates are generated from Python date objects, not user-supplied SQL fragments.
    sql = template.replace("__START_DATE_IST__", start.isoformat())
    sql = sql.replace("__END_DATE_IST__", end_exclusive.isoformat())
    if "__START_DATE_IST__" in sql or "__END_DATE_IST__" in sql:
        raise RuntimeError("SQL date placeholders were not replaced correctly.")
    return sql


def fetch_chunk(
    session: requests.Session,
    metabase_url: str,
    database_id: int,
    sql: str,
    timeout_seconds: int,
    max_results: int,
) -> tuple[list[str], list[list[Any]]]:
    url = f"{metabase_url.rstrip('/')}/api/dataset"
    payload = {
        "database": database_id,
        "type": "native",
        "native": {"query": sql},
        "constraints": {
            "max-results": max_results,
            "max-results-bare-rows": max_results,
        },
    }
    response = session.post(url, json=payload, timeout=(30, timeout_seconds))

    if response.status_code == 401:
        raise RuntimeError(
            "Metabase returned HTTP 401. Check the METABASE_API_KEY secret and METABASE_URL."
        )
    if response.status_code == 403:
        raise RuntimeError(
            "Metabase returned HTTP 403. The API key's user needs permission to run native SQL "
            "against the configured database."
        )
    if not response.ok:
        raise RuntimeError(
            f"Metabase HTTP {response.status_code}: {response.text[:3000]}"
        )

    try:
        body = response.json()
    except ValueError as exc:
        raise RuntimeError(
            f"Metabase returned a non-JSON response: {response.text[:1500]}"
        ) from exc

    if body.get("status") == "failed" or body.get("error"):
        raise RuntimeError(f"Metabase query failed: {body.get('error', body)}")

    data = body.get("data") or {}
    cols = data.get("cols") or []
    rows = data.get("rows") or []
    if not cols:
        raise RuntimeError(f"Metabase response did not include column metadata: {body}")

    headers = [str(col.get("name") or "") for col in cols]
    if headers != SQL_HEADERS:
        display_headers = [str(col.get("display_name") or "") for col in cols]
        if display_headers != SQL_HEADERS:
            raise RuntimeError(
                "Metabase returned a different column list/order than expected.\n"
                f"Expected: {SQL_HEADERS}\nNames: {headers}\nDisplay names: {display_headers}"
            )

    if len(rows) >= max_results:
        raise RuntimeError(
            f"This chunk returned {len(rows):,} rows, equal to DATASET_MAX_RESULTS. "
            "Lower CHUNK_DAYS so Metabase cannot silently truncate the chunk."
        )

    return headers, rows


def create_staging_db(path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(path)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute(
        """
        CREATE TABLE chosen_rows (
            lead_id TEXT PRIMARY KEY,
            ms_present INTEGER NOT NULL,
            ms_key TEXT NOT NULL,
            created_key TEXT NOT NULL,
            internal_key TEXT NOT NULL,
            row_json TEXT NOT NULL
        )
        """
    )
    return conn


def add_rows_to_staging(conn: sqlite3.Connection, rows: list[list[Any]]) -> int:
    """Merge chunk candidates using the SQL DISTINCT ON ordering across all chunks."""
    payloads = []
    for row in rows:
        if len(row) != len(SQL_HEADERS):
            raise RuntimeError(f"Unexpected row width {len(row)}; expected {len(SQL_HEADERS)}")

        lead_id = str(row[0] or "").strip()
        if not lead_id:
            raise RuntimeError("Metabase returned a blank Lead ID; refusing to import it.")

        ms_key = normalise_timestamp(row[2])
        created_key = normalise_timestamp(row[1])
        internal_key = str(row[12] or "")
        ms_present = 1 if ms_key else 0
        payloads.append(
            (
                lead_id,
                ms_present,
                ms_key,
                created_key,
                internal_key,
                json.dumps(row, ensure_ascii=False, default=str),
            )
        )

    if payloads:
        conn.executemany(
            """
            INSERT INTO chosen_rows
                (lead_id, ms_present, ms_key, created_key, internal_key, row_json)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(lead_id) DO UPDATE SET
                ms_present=excluded.ms_present,
                ms_key=excluded.ms_key,
                created_key=excluded.created_key,
                internal_key=excluded.internal_key,
                row_json=excluded.row_json
            WHERE
                (excluded.ms_present, excluded.ms_key, excluded.created_key, excluded.internal_key)
                >
                (chosen_rows.ms_present, chosen_rows.ms_key, chosen_rows.created_key, chosen_rows.internal_key)
            """,
            payloads,
        )
        conn.commit()
    return len(payloads)


def get_static_lead_ids(ws: gspread.Worksheet, cutoff: date) -> set[str]:
    """Get pre-October IDs only; these records must remain untouched on scheduled runs."""
    lead_ids = ws.col_values(1)
    if len(lead_ids) <= 1:
        return set()
    last_row = len(lead_ids)
    two_cols = ws.get(f"A2:B{last_row}")
    static_ids: set[str] = set()
    for values in two_cols:
        if len(values) < 2 or not str(values[0]).strip():
            continue
        created_date = parse_sheet_date(values[1])
        if created_date is None:
            raise RuntimeError(
                f"Cannot parse Created Date IST for Lead ID {values[0]!r}. "
                "Fix the date cell before scheduled refresh so pre-October rows are not deleted."
            )
        if created_date < cutoff:
            static_ids.add(str(values[0]).strip())
    return static_ids


def group_contiguous_rows(row_numbers: list[int]) -> list[tuple[int, int]]:
    if not row_numbers:
        return []
    numbers = sorted(set(row_numbers))
    groups: list[tuple[int, int]] = []
    start = previous = numbers[0]
    for number in numbers[1:]:
        if number == previous + 1:
            previous = number
            continue
        groups.append((start, previous))
        start = previous = number
    groups.append((start, previous))
    return groups


def delete_sheet_row_range(ws: gspread.Worksheet, start_row: int, end_row: int) -> int:
    """Delete one inclusive 1-based row range."""
    if end_row < start_row:
        return 0
    ws.spreadsheet.batch_update(
        {
            "requests": [
                {
                    "deleteDimension": {
                        "range": {
                            "sheetId": ws.id,
                            "dimension": "ROWS",
                            "startIndex": start_row - 1,
                            "endIndex": end_row,
                        }
                    }
                }
            ]
        }
    )
    return end_row - start_row + 1


def delete_sheet_rows(ws: gspread.Worksheet, row_numbers: list[int]) -> int:
    """Delete exact 1-based row numbers, issuing bottom-up Sheets API requests."""
    groups = sorted(group_contiguous_rows(row_numbers), key=lambda item: item[0], reverse=True)
    if not groups:
        return 0

    # Limit requests per batch; descending order means earlier row indexes remain valid.
    for offset in range(0, len(groups), 200):
        batch = groups[offset : offset + 200]
        requests_body = [
            {
                "deleteDimension": {
                    "range": {
                        "sheetId": ws.id,
                        "dimension": "ROWS",
                        "startIndex": start_row - 1,
                        "endIndex": end_row,
                    }
                }
            }
            for start_row, end_row in batch
        ]
        ws.spreadsheet.batch_update({"requests": requests_body})
    return sum(end - start + 1 for start, end in groups)


def get_rows_for_recent_refresh(ws: gspread.Worksheet, cutoff: date) -> list[int]:
    lead_ids = ws.col_values(1)
    created_values = ws.col_values(2)
    rows_to_delete: list[int] = []
    for sheet_row, lead_id in enumerate(lead_ids[1:], start=2):
        if not str(lead_id).strip():
            continue
        value = created_values[sheet_row - 1] if len(created_values) >= sheet_row else ""
        created_date = parse_sheet_date(value)
        if created_date is None:
            raise RuntimeError(
                f"Cannot parse Created Date IST at Sheet1 row {sheet_row}; "
                "stopping so a scheduled run cannot leave stale/duplicate rows."
            )
        if created_date >= cutoff:
            rows_to_delete.append(sheet_row)
    return rows_to_delete


def insert_rows_at_top(ws: gspread.Worksheet, row_count: int) -> None:
    if row_count <= 0:
        return
    ws.spreadsheet.batch_update(
        {
            "requests": [
                {
                    "insertDimension": {
                        "range": {
                            "sheetId": ws.id,
                            "dimension": "ROWS",
                            "startIndex": 1,
                            "endIndex": 1 + row_count,
                        },
                        "inheritFromBefore": False,
                    }
                }
            ]
        }
    )


def write_staged_rows(
    ws: gspread.Worksheet,
    conn: sqlite3.Connection,
    write_batch_rows: int,
) -> int:
    cursor = conn.execute(
        "SELECT row_json FROM chosen_rows ORDER BY created_key DESC, lead_id ASC"
    )
    batch: list[list[Any]] = []
    total_written = 0
    next_row = 2

    for (row_json,) in cursor:
        full_row = json.loads(row_json)
        sheet_row = ["" if value is None else value for value in full_row[: len(SHEET_HEADERS)]]
        batch.append(sheet_row)

        if len(batch) >= write_batch_rows:
            end_row = next_row + len(batch) - 1
            ws.update(
                values=batch,
                range_name=f"A{next_row}:L{end_row}",
                value_input_option="RAW",
            )
            total_written += len(batch)
            next_row = end_row + 1
            batch = []
            logging.info("Wrote %s rows to Google Sheets", f"{total_written:,}")

    if batch:
        end_row = next_row + len(batch) - 1
        ws.update(
            values=batch,
            range_name=f"A{next_row}:L{end_row}",
            value_input_option="RAW",
        )
        total_written += len(batch)

    return total_written


def main() -> None:
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s | %(levelname)s | %(message)s",
    )

    run_mode = os.getenv("RUN_MODE", "full").strip().lower()
    if run_mode not in {"full", "recent"}:
        raise ValueError("RUN_MODE must be either 'full' or 'recent'.")

    metabase_url = os.environ["METABASE_URL"].strip().rstrip("/")
    metabase_api_key = os.environ["METABASE_API_KEY"].strip()
    database_id = int(os.environ["METABASE_DATABASE_ID"])
    spreadsheet_id = os.environ["GOOGLE_SPREADSHEET_ID"].strip()
    service_account_json = os.environ["GOOGLE_SERVICE_ACCOUNT_JSON"]
    if not metabase_url.startswith(("https://", "http://")):
        raise ValueError("METABASE_URL must be a full http:// or https:// base URL.")
    if not metabase_api_key or not spreadsheet_id or not service_account_json.strip():
        raise ValueError("One or more required secrets/variables are empty.")
    sheet_name = os.getenv("SHEET_NAME", "Sheet1").strip()

    start_date = env_date("START_DATE_IST", START_DATE_DEFAULT)
    recent_start = env_date("RECENT_START_DATE_IST", RECENT_START_DEFAULT)
    chunk_days = env_int("CHUNK_DAYS", 7)
    write_batch_rows = env_int("WRITE_BATCH_ROWS", 1500)
    max_results = env_int("DATASET_MAX_RESULTS", 500000)
    timeout_seconds = env_int("METABASE_TIMEOUT_SECONDS", 600)

    today_ist = datetime.now(IST).date()
    configured_end = os.getenv("END_DATE_IST", "").strip()
    end_exclusive = date.fromisoformat(configured_end) if configured_end else today_ist + timedelta(days=1)

    if start_date >= end_exclusive:
        raise ValueError("START_DATE_IST must be earlier than the exclusive end date.")
    if recent_start < start_date:
        raise ValueError("RECENT_START_DATE_IST cannot be earlier than START_DATE_IST.")

    if run_mode == "full":
        extraction_start = start_date
        logging.info("Mode: FULL refresh from %s through %s IST", extraction_start, end_exclusive - timedelta(days=1))
    else:
        extraction_start = recent_start
        if extraction_start >= end_exclusive:
            logging.info("There is no October-to-current date range to refresh yet. Exiting.")
            return
        logging.info(
            "Mode: RECENT refresh from %s through %s IST; pre-October sheet rows are preserved",
            extraction_start,
            end_exclusive - timedelta(days=1),
        )

    # Open Google Sheet using the service account key stored in GitHub Secrets.
    credentials_dict = json.loads(service_account_json)
    gc = gspread.service_account_from_dict(credentials_dict)
    spreadsheet = gc.open_by_key(spreadsheet_id)
    ws = spreadsheet.worksheet(sheet_name)

    # The target tab is reserved for this 12-column output. Trim unused columns.
    first_row = ws.get("A1:L1")
    existing_header = (first_row[0] if first_row else [])
    existing_header = existing_header + [""] * (len(SHEET_HEADERS) - len(existing_header))
    if all(not str(value).strip() for value in existing_header):
        ws.update(values=[SHEET_HEADERS], range_name="A1:L1", value_input_option="RAW")
    elif existing_header[: len(SHEET_HEADERS)] != SHEET_HEADERS:
        raise RuntimeError(
            f"The tab {sheet_name!r} does not have the expected 12-column header. "
            f"Expected: {SHEET_HEADERS}; found: {existing_header[:len(SHEET_HEADERS)]}"
        )
    ws.resize(cols=len(SHEET_HEADERS))

    # Keep IDs from March-September so a new October+ row cannot silently duplicate one.
    static_ids: set[str] = set()
    if run_mode == "recent":
        static_ids = get_static_lead_ids(ws, recent_start)
        logging.info("Found %s existing pre-October Lead IDs to protect", f"{len(static_ids):,}")

    sql_template = SQL_FILE.read_text(encoding="utf-8")
    session = make_metabase_session(metabase_api_key)

    tmp = tempfile.NamedTemporaryFile(prefix="cohort_sync_", suffix=".sqlite", delete=False)
    db_path = tmp.name
    tmp.close()
    conn: sqlite3.Connection | None = None

    try:
        conn = create_staging_db(db_path)
        windows = list(iter_windows(extraction_start, end_exclusive, chunk_days))
        logging.info("Running %s weekly chunks (%s-day windows)", f"{len(windows):,}", chunk_days)

        total_candidate_rows = 0
        for index, (chunk_start, chunk_end) in enumerate(windows, start=1):
            logging.info(
                "Chunk %s/%s: %s <= Created Date IST < %s",
                index,
                len(windows),
                chunk_start,
                chunk_end,
            )
            sql = build_sql(sql_template, chunk_start, chunk_end)
            _, rows = fetch_chunk(
                session,
                metabase_url,
                database_id,
                sql,
                timeout_seconds,
                max_results,
            )
            count = add_rows_to_staging(conn, rows)
            total_candidate_rows += count
            unique_count = conn.execute("SELECT COUNT(*) FROM chosen_rows").fetchone()[0]
            logging.info(
                "Chunk returned %s rows; total candidates %s; unique Lead IDs staged %s",
                f"{len(rows):,}",
                f"{total_candidate_rows:,}",
                f"{unique_count:,}",
            )
            time.sleep(0.25)

        unique_rows = conn.execute("SELECT COUNT(*) FROM chosen_rows").fetchone()[0]
        logging.info(
            "All SQL chunks completed. %s candidate rows reduced to %s unique Lead IDs.",
            f"{total_candidate_rows:,}",
            f"{unique_rows:,}",
        )

        if run_mode == "recent" and static_ids:
            collisions = []
            for (row_json,) in conn.execute("SELECT row_json FROM chosen_rows"):
                row = json.loads(row_json)
                if str(row[0]).strip() in static_ids:
                    collisions.append(str(row[0]).strip())
                    if len(collisions) >= 20:
                        break
            if collisions:
                raise RuntimeError(
                    "Recent query returned Lead IDs already present in the protected March-September "
                    f"rows (sample: {collisions}). Sheet1 was not modified. This indicates a Lead ID "
                    "exists in more than one created-date period; run a FULL refresh or review the source."
                )

        # Only mutate the live Sheet after every Metabase chunk completed successfully.
        if run_mode == "full":
            lead_ids = ws.col_values(1)
            last_data_row = len(lead_ids)
            deleted = delete_sheet_row_range(ws, 2, last_data_row) if last_data_row >= 2 else 0
            logging.info("Full mode removed %s previous data rows", f"{deleted:,}")
            insert_rows_at_top(ws, unique_rows)
        else:
            recent_rows = get_rows_for_recent_refresh(ws, recent_start)
            deleted = delete_sheet_rows(ws, recent_rows)
            logging.info("Recent mode removed %s October+ rows; pre-October rows were not deleted", f"{deleted:,}")
            insert_rows_at_top(ws, unique_rows)

        written = write_staged_rows(ws, conn, write_batch_rows)
        logging.info(
            "SUCCESS: wrote %s unique rows to %s / %s. Visible columns: %s",
            f"{written:,}",
            spreadsheet.title,
            sheet_name,
            len(SHEET_HEADERS),
        )
    finally:
        if conn is not None:
            conn.close()
        session.close()
        for suffix in ("", "-wal", "-shm"):
            try:
                os.remove(db_path + suffix)
            except FileNotFoundError:
                pass


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        logging.exception("SYNC FAILED: %s", exc)
        sys.exit(1)
