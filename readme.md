# Timesheet Batch Extractor

Processes 1,000+ timesheet images using Claude Vision API.
Outputs a structured Excel file with all records, confidence flags, and an optional PostgreSQL export.

---

## Setup

### 1. Install dependencies
```bash
pip install anthropic openpyxl
```

For database export, also install:
```bash
pip install psycopg2-binary sqlalchemy pandas
```

### 2. Set your API key
```bash
# Mac/Linux
export ANTHROPIC_API_KEY="sk-ant-..."

# Windows
set ANTHROPIC_API_KEY=sk-ant-...
```
Get your key at: https://console.anthropic.com/

---

## Usage

### Basic — images folder → Excel
```bash
python extractor.py ./my_timesheets/
```

### Custom output filename
```bash
python extractor.py ./my_timesheets/ --output march_2026.xlsx
```

### With PostgreSQL export
```bash
python extractor.py ./my_timesheets/ \
  --output march_2026.xlsx \
  --db "postgresql://user:password@localhost:5432/timesheets"
```

### Slow down API calls (if hitting rate limits)
```bash
python extractor.py ./my_timesheets/ --delay 1.5
```

---

## Supported Image Formats
`.jpg` `.jpeg` `.png` `.webp` `.gif`

---

## Output Files

| File | Description |
|---|---|
| `timesheet_output.xlsx` | All extracted records with 3 sheets |
| `results.json` | Raw JSON backup (updated after each image) |
| `extraction.log` | Full run log with errors and warnings |

### Excel Sheets
- **All Records** — flat table of every employee row across all sheets
- **Summary** — totals: processed, succeeded, failed, low confidence
- **Failed & Review** — files that failed + rows flagged for human review (confidence < 80%)

---

## Confidence Flags

Claude returns a `confidence` score (0.0–1.0) per employee row:
- **≥ 0.8** → marked OK, inserted normally
- **< 0.8** → highlighted yellow, flagged in "Failed & Review" sheet

This handles handwritten fields that are hard to read.

---

## Database Schema (PostgreSQL)

The script auto-creates a `timesheet_staging` table on first run:

```sql
CREATE TABLE timesheet_staging (
    id              SERIAL PRIMARY KEY,
    source_file     VARCHAR(255),
    extracted_at    TIMESTAMP,
    project         VARCHAR(255),
    business_unit   VARCHAR(100),
    work_date       VARCHAR(50),
    day_of_week     VARCHAR(20),
    staff_type      VARCHAR(20),    -- 'frontline' or 'management'
    job_task        VARCHAR(10),
    title           VARCHAR(100),
    employee_name   VARCHAR(150),
    ein             VARCHAR(30),
    scheduled_start VARCHAR(20),
    scheduled_end   VARCHAR(20),
    scheduled_hours VARCHAR(10),
    actual_start    VARCHAR(20),
    lunch_out       VARCHAR(20),
    lunch_in        VARCHAR(20),
    actual_end      VARCHAR(20),
    actual_hours    VARCHAR(10),
    absent          BOOLEAN,
    schedule_changed BOOLEAN,
    confidence      NUMERIC(3,2),
    reviewed        BOOLEAN DEFAULT FALSE  -- for human review queue
);
```

After reviewing flagged rows, promote to production:
```sql
-- Promote clean rows to production table
INSERT INTO timesheet_production
SELECT * FROM timesheet_staging
WHERE confidence >= 0.8 AND reviewed = FALSE;

-- Mark reviewed
UPDATE timesheet_staging SET reviewed = TRUE
WHERE confidence >= 0.8;
```

---

## Cost Estimate

| Volume | Est. Cost (Claude Sonnet) |
|---|---|
| 100 sheets | ~$1–3 |
| 1,000 sheets | ~$10–30 |
| 5,000 sheets | ~$50–150 |

Use `--delay 1.0` or higher for large batches to avoid rate limits.

---

## Crash Recovery

The script saves `results.json` after **every single image**. If it crashes mid-run:

1. Check `results.json` for already-processed files
2. Move processed images to a separate folder
3. Re-run on the remaining images
4. Merge the two JSON files if needed

---

## Customizing for Different Form Layouts

Edit the `EXTRACTION_PROMPT` variable in `extractor.py` to match your specific form fields. The prompt uses plain English — no ML training required.