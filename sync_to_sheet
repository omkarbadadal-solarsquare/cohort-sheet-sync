#!/usr/bin/env python3
"""
Metabase -> Google Sheets sync, fetched in date chunks.

MODE=full    : fetch FULL_START -> today, rewrite the whole tab.
MODE=recent  : fetch REFRESH_FROM -> today, rewrite only the rows from
               REFRESH_FROM onwards. Older rows in the sheet stay untouched.

The tab must be sorted ascending by DATE_COLUMN (this script keeps it that way).
The same script works for the effort query: set SQL_FILE, SHEET_TAB and
DATE_COLUMN="Event Date IST".
"""

import json
import os
import re
import sys
import time
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

import gspread
import requests
from google.oauth2.service_account import Credentials
from gspread.utils import ValueRenderOption, rowcol_to_a1

IST = ZoneInfo("Asia/Kolkata")

# ---------------------------------------------------------------- config
METABASE_URL = os.environ["METABASE_URL"].rstrip("/")
METABASE_API_KEY = os.environ["METABASE_API_KEY"]
METABASE_QUESTION_ID = int(os.environ["METABASE_QUESTION_ID"])
GOOGLE_CREDENTIALS = os.environ["GOOGLE_CREDENTIALS"]

SPREADSHEET_ID = os.environ.get(
    "SPREADSHEET_ID", "1RivYArY_zCFoHw5WhfD75HsxwYTo7hUvsMI-NknARbI"
)
SHEET_TAB = os.environ.get("SHEET_TAB", "Sheet1")
DATE_COLUMN = os.environ.get("DATE_COLUMN", "Created Date IST")
LEAD_ID_COLUMN = os.environ.get("LEAD_ID_COLUMN", "Lead ID")

MODE = os.environ.get("MODE", "full").strip().lower()  # full | recent
FULL_START = date.fromisoformat(os.environ.get("FULL_START", "2026-03-01"))
REFRESH_FROM = date.fromisoformat(os.environ.get("REFRESH_FROM", "2026-10-01"))

# 0 = one query per month. Set e.g. 15 to split each month into 15-day chunks.
CHUNK_DAYS = int(os.environ.get("CHUNK_DAYS", "0"))

WRITE_BATCH_ROWS = 5000
METABASE_TIMEOUT_SEC = 1800
METABASE_RETRIES = 3

DT_RE = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}")


def log(msg):
    print(f"[{datetime.now(IST):%H:%M:%S}] {msg}", flush=True)


# ---------------------------------------------------------------- windows
def next_month_start(d):
    return date(d.year + (d.month == 12), d.month % 12 + 1, 1)


def build_windows(range_start, end_exclusive):
    """Month windows [start, end); optionally split into CHUNK_DAYS pieces."""
    windows = []
    cur = range_start
    while cur < end_exclusive:
        nxt = next_month_start(cur)
        if CHUNK_DAYS > 0:
            s = cur
            while s < nxt:
                e = min(s + timedelta(days=CHUNK_DAYS), nxt)
                windows.append((s, e))
                s = e
        else:
            windows.append((cur, nxt))
        cur = nxt
    return windows


def set_window(sql, start, end):
    """Replace the dates inside the Params block of the SQL."""
    sql, n1 = re.subn(
        r"TIMESTAMP\s+'[^']*'(\s+AS\s+start_date)",
        lambda m: f"TIMESTAMP '{start} 00:00:00'{m.group(1)}",
        sql,
    )
    sql, n2 = re.subn(
        r"TIMESTAMP\s+'[^']*'(\s+AS\s+end_date)",
        lambda m: f"TIMESTAMP '{end} 00:00:00'{m.group(1)}",
        sql,
    )
    if n1 != 1 or n2 != 1:
        raise RuntimeError(
            "Params block not found exactly once in the SQL "
            f"(start_date matches={n1}, end_date matches={n2})"
        )
    return sql


# ---------------------------------------------------------------- metabase
def load_card():
    """Read database id + SQL from the saved Metabase question."""
    r = requests.get(
        f"{METABASE_URL}/api/card/{METABASE_QUESTION_ID}",
        headers={"x-api-key": METABASE_API_KEY},
        timeout=120,
    )
    r.raise_for_status()
    dq = r.json().get("dataset_query") or {}
    db = dq.get("database")
    sql = (dq.get("native") or {}).get("query")
    if not sql and dq.get("stages"):  # newer Metabase query format
        sql = dq["stages"][0].get("native")
    if not db or not isinstance(sql, str):
        raise RuntimeError(
            f"Question {METABASE_QUESTION_ID} is not a native SQL question "
            "or the API key cannot read it."
        )
    return db, sql


def run_query(database_id, sql):
    payload = {
        "database": database_id,
        "type": "native",
        "native": {"query": sql},
    }
    last_err = None
    for attempt in range(1, METABASE_RETRIES + 1):
        try:
            r = requests.post(
                f"{METABASE_URL}/api/dataset/json",
                headers={"x-api-key": METABASE_API_KEY},
                data={"query": json.dumps(payload)},
                timeout=METABASE_TIMEOUT_SEC,
            )
            r.raise_for_status()
            data = r.json()
            if isinstance(data, dict):  # Metabase returns an object on query error
                raise RuntimeError(f"Metabase query error: {str(data)[:500]}")
            return data
        except Exception as e:  # noqa: BLE001
            last_err = e
            log(f"  attempt {attempt}/{METABASE_RETRIES} failed: {str(e)[:300]}")
            if attempt < METABASE_RETRIES:
                time.sleep(30 * attempt)
    raise RuntimeError(f"Metabase fetch failed after retries: {last_err}")


# ---------------------------------------------------------------- cleaning
def clean(col, v):
    if v is None:
        return ""
    if isinstance(v, bool):
        return "TRUE" if v else "FALSE"
    if isinstance(v, str):
        if DT_RE.match(v):
            return v[:19].replace("T", " ")  # 'YYYY-MM-DD HH:MM:SS'
        if col == LEAD_ID_COLUMN and v.isdigit():
            return "'" + v  # keep IDs as text
        if v.startswith("="):
            return "'" + v
        return v
    return v


def to_rows(records, header, date_idx, lead_idx):
    rows = [[clean(c, rec.get(c)) for c in header] for rec in records]
    rows.sort(key=lambda r: (str(r[date_idx]), str(r[lead_idx])))
    return rows


# ---------------------------------------------------------------- sheets
def open_sheet():
    creds = Credentials.from_service_account_info(
        json.loads(GOOGLE_CREDENTIALS),
        scopes=["https://www.googleapis.com/auth/spreadsheets"],
    )
    sh = gspread.authorize(creds).open_by_key(SPREADSHEET_ID)
    try:
        return sh.worksheet(SHEET_TAB)
    except gspread.WorksheetNotFound:
        return sh.add_worksheet(SHEET_TAB, rows=1000, cols=20)


def write_block(ws, start_row, rows, ncols):
    for i in range(0, len(rows), WRITE_BATCH_ROWS):
        part = rows[i : i + WRITE_BATCH_ROWS]
        r0 = start_row + i
        rng = f"{rowcol_to_a1(r0, 1)}:{rowcol_to_a1(r0 + len(part) - 1, ncols)}"
        for attempt in range(1, 6):
            try:
                ws.update(
                    values=part, range_name=rng, value_input_option="USER_ENTERED"
                )
                break
            except gspread.exceptions.APIError as e:
                if attempt == 5:
                    raise
                log(f"  sheet write retry {attempt}: {str(e)[:150]}")
                time.sleep(20 * attempt)
        log(f"  wrote rows {r0}-{r0 + len(part) - 1}")


def serial_of(v):
    """Sheet value -> Google serial number (days since 1899-12-30)."""
    if isinstance(v, (int, float)):
        return float(v)
    if isinstance(v, str):
        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
            try:
                d = datetime.strptime(v.strip(), fmt)
                return (d - datetime(1899, 12, 30)).total_seconds() / 86400
            except ValueError:
                pass
    return None


def full_write(ws, header, rows):
    ncols = len(header)
    ws.clear()
    ws.resize(rows=len(rows) + 1, cols=ncols)
    write_block(ws, 1, [header], ncols)
    write_block(ws, 2, rows, ncols)
    ws.freeze(rows=1)
    log(f"FULL refresh done: {len(rows)} data rows")


def recent_write(ws, header, rows, date_idx):
    ncols = len(header)
    existing_header = ws.row_values(1)
    if existing_header != header:
        raise RuntimeError(
            "Sheet header is empty or different from the query output. "
            "Run the workflow manually with mode=full first."
        )

    date_col_a1 = re.sub(r"\d", "", rowcol_to_a1(1, date_idx + 1))
    vals = ws.get(
        f"{date_col_a1}2:{date_col_a1}",
        value_render_option=ValueRenderOption.unformatted,
    )
    old_last_row = len(vals) + 1

    cutoff = (
        datetime.combine(REFRESH_FROM, datetime.min.time()) - datetime(1899, 12, 30)
    ).total_seconds() / 86400

    first_row = old_last_row + 1  # default: append after last row
    for i, row in enumerate(vals):
        s = serial_of(row[0]) if row else None
        if s is not None and s >= cutoff:
            first_row = i + 2
            break

    new_last_row = first_row + len(rows) - 1
    if new_last_row + 1 > ws.row_count:
        ws.resize(rows=new_last_row + 1, cols=ncols)

    log(f"recent refresh: replacing sheet rows {first_row}..{old_last_row}")
    write_block(ws, first_row, rows, ncols)

    if old_last_row > new_last_row:  # fresh data is shorter -> clear leftovers
        ws.batch_clear(
            [f"{rowcol_to_a1(new_last_row + 1, 1)}:{rowcol_to_a1(old_last_row, ncols)}"]
        )
    log(f"RECENT refresh done: {len(rows)} rows from {REFRESH_FROM}")


# ---------------------------------------------------------------- main
def main():
    if MODE not in ("full", "recent"):
        sys.exit(f"MODE must be 'full' or 'recent', got '{MODE}'")

    today = datetime.now(IST).date()
    database_id, base_sql = load_card()
    log(f"Loaded question {METABASE_QUESTION_ID} (database {database_id})")

    range_start = FULL_START if MODE == "full" else REFRESH_FROM
    windows = build_windows(range_start, today + timedelta(days=1))
    log(f"MODE={MODE} | tab={SHEET_TAB} | {len(windows)} chunk(s) from {range_start}")

    header, all_rows, date_idx = None, [], None
    for start, end in windows:
        t0 = time.time()
        records = run_query(database_id, set_window(base_sql, start, end))
        log(f"chunk {start} -> {end}: {len(records)} rows in {time.time() - t0:.0f}s")
        if not records:
            continue
        if header is None:
            header = list(records[0].keys())
            if DATE_COLUMN not in header or LEAD_ID_COLUMN not in header:
                raise RuntimeError(
                    f"'{DATE_COLUMN}' / '{LEAD_ID_COLUMN}' not in query columns: {header}"
                )
            date_idx = header.index(DATE_COLUMN)
        lead_idx = header.index(LEAD_ID_COLUMN)
        all_rows.extend(to_rows(records, header, date_idx, lead_idx))

    if not all_rows:
        sys.exit("No rows fetched - sheet left untouched.")

    ws = open_sheet()
    if MODE == "full":
        full_write(ws, header, all_rows)
    else:
        recent_write(ws, header, all_rows, date_idx)


if __name__ == "__main__":
    main()
