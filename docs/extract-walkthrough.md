# extract.py — Step-by-Step Walkthrough

How `src/extract.py` processes timesheet PDFs from start to finish.

---

## Overview

```
Run script
    │
    ▼
1. Interactive file selection  (no API calls yet)
    │
    ▼
2. Connect to Supabase  →  create table if missing
    │
    ▼
3. For each selected PDF:
    │
    ├─ 3a. Split PDF into single-page PDFs  (pypdf, local only)
    │
    ├─ 3b. For each page:
    │       ├─ Send page to Claude as a PDF document
    │       ├─ Receive structured JSON back
    │       └─ Retry up to 3× on failure
    │
    ├─ 3c. Save page results to Supabase
    ├─ 3d. Write per-source Excel file
    └─ 3e. Move PDF to input/processed/
    │
    ▼
4. Write master Excel (all files merged)
    │
    ▼
5. Write JSON + CSV backups
    │
    ▼
6. Log total token usage and cost
```

---

## Step 1 — Interactive File Selection

**Function:** `_select_files()`

- Scans `input/` for all `.pdf` files
- Opens each file with `pypdf` just to count pages (no content read yet)
- Prints a numbered menu showing filename, page count, and file size
- You type `1,2` or `all` to select files, or `q` to quit
- Before any API calls, calculates and displays an **estimated cost in USD** based on average token counts per page
- Asks `y/n` to confirm — nothing happens until you say yes

---

## Step 2 — Database Setup

**Function:** `_get_db_conn()`, `_ensure_table()`

- Reads `DATABASE_URL` from `.env` and opens a connection to Supabase PostgreSQL
- Runs `CREATE TABLE IF NOT EXISTS timesheet_entries (...)` — safe to run every time, does nothing if the table already exists
- Connection stays open for the entire batch run

---

## Step 3a — PDF Splitting

**Function:** `_pdf_to_pages()`

**Library:** `pypdf`

Each multi-page PDF is split into individual single-page PDFs in memory:

1. `PdfReader` opens the full PDF
2. For each page, a new `PdfWriter` is created with just that one page
3. The single-page PDF is written to an in-memory `BytesIO` buffer (never saved to disk)
4. The buffer is base64-encoded into a string
5. Returns a list of dicts: `{ page_number, image_b64, media_type: "application/pdf" }`

**Why single pages?** Claude's API processes one document at a time. Sending one page at a time gives us granular results — one JSON object per page — which maps cleanly to the DB schema and allows page-level error handling.

---

## Step 3b — Claude API Call (Text Extraction)

**Function:** `_call_claude()`

**This is where text is actually extracted.**

### File type sent to Claude

```
type: "document"
media_type: "application/pdf"
data: <base64-encoded single-page PDF>
```

This is **not** an image. The raw PDF bytes are sent directly. Claude receives it as a native PDF document and uses its built-in document understanding — it can read the PDF's text layer, understand layout, and handle mixed content (text, tables, barcodes, graphics) without any rendering step.

> **Why not image?** Earlier versions rendered pages to PNG using PyMuPDF and sent them as `type: "image"`. This failed because the timesheets contain barcodes — Claude was transcribing barcode content as binary strings instead of reading the timesheet data. Sending as `type: "document"` fixed this.

### The prompt

Claude receives a text prompt alongside the document:

```
You are extracting data from a staff timesheet image (one page only).
Return ONLY a valid JSON object - no markdown, no explanation, no preamble.
...
```

The prompt instructs Claude to return a **JSON object** (not an array) with this shape:

```json
{
  "project": "...",
  "business_unit": "...",
  "date": "...",
  "day_of_week": "...",
  "weather": "...",
  "frontline_staff": [
    {
      "job_task": "...",
      "title": "...",
      "employee_name": "...",
      "ein": "...",
      "scheduled_start": "...",
      "scheduled_end": "...",
      "scheduled_hours": "...",
      "actual_start": "...",
      "lunch_out": "...",
      "lunch_in": "...",
      "actual_end": "...",
      "actual_hours": "...",
      "signature": "...",
      "absent": true/false/null,
      "schedule_changed": true/false/null,
      "confidence": 0.95
    }
  ],
  "management_staff": [ ... ],
  "summary": {
    "attendees": 12,
    "absent_count": 1
  }
}
```

`confidence` is Claude's self-assessed read quality per staff row (0.0–1.0). Lower values mean the handwriting was difficult. Rows below 0.8 are flagged yellow in Excel.

### What happens to Claude's response

1. Response text is stripped of whitespace
2. If Claude accidentally wrapped the JSON in markdown fences (` ```json `) despite the prompt, they are stripped off
3. `json.loads()` parses the text into a Python dict
4. Token counts (`input_tokens`, `output_tokens`) are read from `response.usage`

### Retry logic

If anything fails (JSON parse error, rate limit, network error), the page is retried:
- Up to **3 attempts**
- On rate limit: waits `5s × attempt number` before retrying
- On other errors: waits 5s flat
- If all 3 attempts fail: page is marked `_status: "failed"` and processing continues with the next page

---

## Step 3c — Save to Supabase

**Function:** `_save_to_db()`

The JSON object Claude returned is **flattened** — instead of storing one row per page, each staff member becomes its own database row.

For a page with 8 frontline staff and 2 management staff → **10 rows inserted**.

Each row contains:
- Page-level fields (project, business_unit, date, day_of_week, weather) — repeated on every row from that page
- Staff-level fields (employee_name, ein, times, absent, confidence, etc.)
- `staff_type` — `"Frontline"` or `"Management"`
- `model_used` — which Claude model processed this page
- `extraction_status` — `"success"` or `"failed"`
- `raw_json` — the **entire** original JSON Claude returned for that page, stored as JSONB for auditing

If a page failed, one row is inserted with `extraction_status = "failed"` and all staff fields as NULL.

---

## Step 3d — Per-Source Excel

**Function:** `_build_per_source_excel()`

After all pages of a single PDF are processed, one Excel file is written to `output/excel/<filename>_extracted.xlsx`.

It contains:
- **"All Records" sheet** — every staff row from every page of this file, flat table, color-coded
- **One sheet per page** — e.g. `"Ceiling_2_DEC_p1"` — shows page metadata at the top (project, date, etc.) then staff rows, then attendee/absent summary at the bottom
- **"Summary" sheet** — model used, page count, success/fail counts, token usage, actual cost, color legend

---

## Step 3e — Archive

After a PDF is fully processed and saved, it is moved from `input/` to `input/processed/`. This prevents the same file from being picked up and double-processed on the next run.

---

## Step 4 — Master Excel

**Function:** `_build_master_excel()`

After all selected PDFs are processed, one combined Excel file is written to `output/excel/timesheet_output.xlsx`.

Contains:
- **"All Records" sheet** — every staff row from every file in this batch run
- **"Summary" sheet** — batch-level stats, token totals, combined cost across all files

---

## Step 5 — Backups

**JSON:** `output/json/extraction_<timestamp>.json`
The full list of page-level result dicts (as Claude returned them, with metadata added). One file per run. Useful if you need to re-process or re-import without calling the API again.

**CSV:** `output/csv/extraction_<timestamp>.csv`
Flat staff-level rows — same data as Supabase but in a CSV. One file per run.

---

## Step 6 — Cost Summary

At the end of the run, logged to console and `logs/extract.log`:

```
Batch complete | 48 pages | 2 file(s)
API usage: 76800 in + 28800 out tokens = $1.6620  (model: claude-sonnet-4-20250514)
```

---

## Token counting and cost

Tokens are counted by the Claude API and returned in every response. The script accumulates them across all pages. Cost is calculated as:

```
cost = (input_tokens / 1,000,000 × input_rate)
     + (output_tokens / 1,000,000 × output_rate)
```

Current rates (per million tokens):

| Model | Input | Output |
|-------|-------|--------|
| claude-opus-4 | $15.00 | $75.00 |
| claude-sonnet-4 | $3.00 | $15.00 |
| claude-haiku-4 | $0.80 | $4.00 |
| claude-haiku-3 | $0.25 | $1.25 |

The **pre-flight estimate** uses fixed averages (1,600 input + 600 output tokens per page). The **actual cost** uses real token counts from the API.
