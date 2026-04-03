# Project Structure

```
Billing Automation/
│
├── input/                          # Drop source PDF timesheets here before running
│   └── processed/                  # PDFs are automatically moved here after extraction
│
├── output/
│   ├── json/
│   │   └── extraction_<ts>.json    # Full extraction dump, timestamped (backup)
│   └── csv/
│       └── extraction_<ts>.csv     # Flat CSV of all extracted rows, timestamped (backup)
│
├── src/
│   └── extract.py                  # Step 1 — PDF extraction via Claude Vision → Supabase
│
├── scripts/                        # Reserved for future utility / helper scripts
│
├── logs/
│   └── extract.log                 # Execution logs (appended on each run)
│
├── docs/
│   ├── structure.md                # This file — directory layout and file roles
│   ├── how-to-run.md               # Setup and execution instructions
│   └── process.md                  # How the pipeline works (human-readable)
│
├── .venv/                          # Python virtual environment (not committed)
├── .env                            # Local environment variables (not committed)
├── .env.example                    # Template showing required env vars
├── requirements.txt                # Python dependencies
└── readme.md                       # Project overview
```

## Key Files

| File | Purpose |
|------|---------|
| `src/extract.py` | Step 1 — reads PDFs from `input/`, sends each page to Claude, writes rows to Supabase + backup files |
| `output/json/extraction_<ts>.json` | JSON array of every row from a single run (manual review backup) |
| `output/csv/extraction_<ts>.csv` | Same data as a flat CSV, easy to open in Excel |
| `logs/extract.log` | Timestamped log of every run — useful for debugging and auditing |
| `.env` | Must contain `ANTHROPIC_API_KEY` and `DATABASE_URL` (see `.env.example`) |

## Primary Data Store — Supabase

All extracted rows land in the `timesheet_entries` table in Supabase PostgreSQL.
The table is created automatically on first run if it does not exist.

| Column | Type | Description |
|--------|------|-------------|
| `id` | SERIAL | Auto-incrementing primary key |
| `source_file` | TEXT | Name of the source PDF |
| `page_number` | INTEGER | Page within that PDF |
| `extracted_at` | TIMESTAMPTZ | UTC timestamp of extraction |
| `project` | TEXT | Project / job code |
| `business_unit` | TEXT | Department or business unit |
| `date` | TEXT | Calendar date of entry |
| `day_of_week` | TEXT | Day name (Mon, Tue, …) |
| `eid` | TEXT | Employee ID |
| `name` | TEXT | Employee name |
| `title` | TEXT | Job title |
| `start_time` | TEXT | Clock-in time |
| `lunch_out` | TEXT | Lunch break start |
| `lunch_in` | TEXT | Lunch break end |
| `end_time` | TEXT | Clock-out time |
| `hours` | NUMERIC | Total hours worked |
| `absent` | TEXT | Absence indicator |
| `notes` | TEXT | Additional notes |
| `raw_json` | JSONB | Full Claude response for that row (audit trail) |
