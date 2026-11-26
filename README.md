# Cohort data: Metabase -> Google Sheets using GitHub Actions

## What the workflow does

- Uses your SQL in `sql/cohort_query.sql` and runs it in 7-day windows using `Created Date IST`.
- Writes the 12 visible output columns to `Sheet1` in the Google spreadsheet configured in the workflow.
- The Python script uses a temporary SQLite database to apply the same global `DISTINCT ON (lead_id)` ranking across all weekly chunks (`MS Date IST DESC NULLS LAST`, then `Created Date IST DESC`, then the internal lead row ID DESC). The internal row ID is a temporary SQL helper and is not written to the sheet.
- Scheduled run: refreshes only `2026-10-01` through the current India date. Existing rows before October are preserved.
- Manual GitHub Actions run: refreshes the full `2026-03-01` through current India date range.
- The live sheet is not changed until all Metabase chunks have completed successfully.

## Required GitHub repository configuration

Create these **Actions variables**:

| Variable | Value |
|---|---|
| `METABASE_URL` | Your Metabase base URL, e.g. `https://your-metabase-host` |
| `METABASE_DATABASE_ID` | Numeric database ID for the database containing the tables in the SQL |

Create these **Actions secrets**:

| Secret | Value |
|---|---|
| `METABASE_API_KEY` | Metabase API key with native-query permission on that database |
| `GOOGLE_SERVICE_ACCOUNT_JSON` | Entire JSON key downloaded for the Google service account |

The spreadsheet ID and tab name are already set in `.github/workflows/cohort-sheet-sync.yml`:

- Spreadsheet ID: `1RivYArY_zCFoHw5WhfD75HsxwYTo7hUvsMI-NknARbI`
- Tab: `Sheet1`

The Google service account must have **Editor** access to that spreadsheet. Enable both Google Sheets API and Google Drive API in the Google Cloud project that owns the service account.

## First-time setup

1. Create a private GitHub repository, for example `cohort-sheet-sync`.
2. Upload this folder's files while preserving the folder structure. In particular, the workflow must be at `.github/workflows/cohort-sheet-sync.yml`.
3. Add the two Actions variables and two Actions secrets listed above.
4. Open **Actions -> Cohort data to Google Sheet -> Run workflow**. Manual runs always use `full` mode.
5. After the first full run succeeds, leave the workflow enabled. The daily scheduled run will update only October 1 onward.

## How to find the Metabase database ID

Open your Metabase API docs at `https://YOUR-METABASE-HOST/api/docs`, authenticate with your API key, and inspect `GET /api/database`; locate the database that contains `public.lead`, `public.meeting_metrics_history`, `public.city_state_cluster`, `public.anubis_scored_leads`, and `public.siq_webhook_logs`. Use its numeric `id` as `METABASE_DATABASE_ID`.

## Adjusting chunk size

The default is 7 days. If a chunk still times out or returns the configured maximum row count, lower `CHUNK_DAYS` to `3` or `1` in the workflow. If the query is consistently fast, it can be increased to `14`.

## Local run (optional)

Install packages with `pip install -r requirements.txt`, then set the same environment variables from the workflow. Set `RUN_MODE=full` and run `python sync_metabase_to_sheets.py`.

## Important notes

- The `Sheet1` tab is treated as a dedicated output tab with these 12 headers. The script trims unused columns to 12.
- Dates from the query are preserved as raw values/text so the SQL's IST values are not silently reinterpreted by the runner's UTC timezone.
- Scheduled updates deliberately do not re-run the source query for March-September. Those rows remain as previously imported.
- If the recent refresh discovers a Lead ID already in the protected March-September rows, it stops before changing the sheet rather than creating a duplicate. This guards against a Lead ID occurring in multiple created-date periods.
- A Google Sheet has a total cell limit across its tabs. Twelve columns allow roughly 1.66 million rows at 12 columns before considering other tabs and unused grid cells. Google announced a 20-million-cell total limit in September 2026.
