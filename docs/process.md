# How the Pipeline Works

This tool is an end-to-end ETL (Extract → Transform → Load) pipeline that turns physical paper timesheets into structured billing data.

---

## The Big Picture

```
Physical Timesheets (PDF scans)
        │
        ▼
  [ Step 1: Extract ]  ←  you are here
        │
        ▼
  JSON / CSV / Excel (local)  →  Supabase PostgreSQL
        │
        ▼
  [ Step 2: Clean & Transform ]  (coming soon)
        │
        ▼
  [ Step 3: Load into Client Report ]  (coming soon)
```

---

## Step 1 — Extract

**Goal:** Turn scanned PDF timesheets into structured staff rows saved locally and in Supabase.

### Execution order

```
Phase 1 — File Selection  (no API calls)
│
│   Menu displayed → you pick files → estimated cost shown → confirm y/n
│
Phase 2 — Per-file loop  (one file fully completed before the next starts)
│
│   For each PDF:
│   ├─ API calls: every page sent to Claude one by one (results held in memory)
│   ├─ Per-file JSON saved  →  output/json/<filename>_<timestamp>.json    ← checkpoint
│   ├─ Per-file Excel saved →  output/excel/<filename>_extracted.xlsx
│   └─ PDF moved            →  input/processed/                           ← happens here, not at end
│
Phase 3 — After ALL files are done
│
│   ├─ Master JSON saved    →  output/json/extraction_<timestamp>.json   (all files combined)
│   ├─ CSV saved            →  output/csv/extraction_<timestamp>.csv     (all files combined)
│   ├─ Master Excel saved   →  output/excel/timesheet_output.xlsx        (all files combined)
│   └─ DB export            →  Supabase timesheet_entries                ← always last
```

### Why this order?
Local files (JSON, CSV, Excel) are always written before the DB export. If the DB export fails for any reason — schema mismatch, network error, credentials — your extracted data is already safely saved. You can re-import without making any API calls:

```bash
python src/extract.py --from-json "output/json/extraction_<timestamp>.json"
```

### What Claude receives per page

Each page is sent to Claude as a **native PDF document** (`type: "document"`, `media_type: "application/pdf"`), not a rendered image. This lets Claude use its built-in PDF understanding to correctly handle mixed content — tables, handwriting, barcodes, graphics — on the same page.

Claude returns one JSON object per page:

**Top-level fields (one per page)**
| Field | Description |
|-------|-------------|
| `project` | Project or job code |
| `business_unit` | Department or business unit |
| `date` | Calendar date (as written) |
| `day_of_week` | Day name |
| `weather` | Weather noted on sheet, if any |

**Per staff row** (in `frontline_staff` and `management_staff` arrays)
| Field | Description |
|-------|-------------|
| `job_task` | Task or job code |
| `title` | Job title |
| `employee_name` | Employee name |
| `ein` | Employee ID number |
| `scheduled_start/end/hours` | Scheduled shift times |
| `actual_start/end/hours` | Actual clock-in/out times |
| `lunch_out / lunch_in` | Lunch break times |
| `absent` | true / false / null |
| `schedule_changed` | true / false / null |
| `signature` | Signature noted, if any |
| `confidence` | 0.0–1.0 — Claude's self-assessed read quality per row |

**Summary** (one per page)
| Field | Description |
|-------|-------------|
| `attendees` | Total headcount |
| `absent_count` | Total absences |

If Claude hits a rate limit or returns unparseable JSON, the page is retried up to 3 times with backoff before being marked `failed`.

### Outputs per run

| Output | Location | Timing |
|--------|----------|--------|
| Per-file JSON | `output/json/<file>_<ts>.json` | After each file's API calls |
| Per-file Excel | `output/excel/<file>_extracted.xlsx` | After each file's API calls |
| Master JSON | `output/json/extraction_<ts>.json` | After all files |
| CSV | `output/csv/extraction_<ts>.csv` | After all files |
| Master Excel | `output/excel/timesheet_output.xlsx` | After all files |
| Supabase DB | `timesheet_entries` table | Last — after all local files saved |

### Supabase — one row per staff member

Each row in `timesheet_entries` represents one staff member from one page of one PDF:
- `source_file`, `page_number`, `extracted_at` — audit trail
- `extraction_status` — `success` or `failed`
- `model_used` — which Claude model processed this page
- `staff_type` — `Frontline` or `Management`
- `confidence` — Claude's read confidence for this row
- `raw_json` — full page JSON from Claude (JSONB, queryable)

### Excel — color coding

| Row color | Meaning |
|-----------|---------|
| Yellow | Confidence < 80% — review recommended |
| Red | Page failed — no data extracted |
| Alternating white/blue | Normal rows |

### Cost tracking

- **Before run:** estimated cost shown based on page count × average tokens
- **Per page:** tokens in/out and page cost logged to console and `logs/extract.log`
- **End of run:** total tokens and total USD cost printed

---

## Step 2 — Clean & Transform *(planned)*

Raw extracted text will be cleaned and standardised using Python Pandas:
- Normalise date and time formats
- Resolve inconsistent name spellings
- Calculate derived fields (e.g. total daily hours)
- Flag anomalies (missing punches, overlapping times)

---

## Step 3 — Load into Client Report *(planned)*

Cleaned data will be written into client-specific Excel report templates, matching the layout and formatting they expect for billing.

---

## Design Principles

- **Accuracy first** — Claude's native PDF document API is used. It handles barcodes, mixed graphics, and handwriting correctly without rendering to image first.
- **Local before cloud** — JSON and CSV are always written before the DB export. A DB failure never means lost data.
- **Confidence-aware** — Every staff row carries a `confidence` score. Low-confidence rows are flagged yellow in Excel and queryable in Supabase.
- **Nothing lost** — `raw_json` in Supabase stores the full Claude response per page for auditing. JSON backups allow DB re-import at any time at no API cost.
- **Non-destructive** — Source PDFs are moved to `input/processed/` one at a time as each file completes, never deleted.
- **Cost-transparent** — Estimated cost shown before processing. Actual token usage and USD cost logged at end of every run.
- **Auditable** — Every row tagged with source file, page number, extraction timestamp, and model used.
