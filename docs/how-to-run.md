# How to Run

## Prerequisites

- Python 3.10 or later
- An Anthropic API key ([console.anthropic.com](https://console.anthropic.com))
- A Supabase project with the connection string ready

---

## First-Time Setup

**1. Create and activate the virtual environment, then install dependencies**
```bash
python -m venv .venv

# Windows
.venv\Scripts\activate

# Mac / Linux
source .venv/bin/activate

pip install -r requirements.txt
```

**2. Configure environment variables**

Copy the example env file and fill in both keys:
```bash
cp .env.example .env
```

Open `.env` and set:
```
ANTHROPIC_API_KEY=your_anthropic_api_key_here
DATABASE_URL=postgresql://postgres.YOUR_PROJECT_REF:[YOUR-PASSWORD]@aws-0-us-west-2.pooler.supabase.com:5432/postgres
```

The `DATABASE_URL` is found in:
**Supabase Dashboard → Your Project → Connect → Connection string → URI**

---

## Running Step 1 — Extraction

**1. Drop PDF files into the `input/` folder.**

The script processes all `.pdf` files found there. You can drop in up to ~20 files at a time.

**2. Run the script from the project root:**
```bash
python src/extract.py
```

**3. Check the outputs:**

| Output | Location |
|--------|----------|
| Database | Supabase → table `timesheet_entries` |
| JSON backup | `output/json/extraction_<timestamp>.json` |
| CSV backup | `output/csv/extraction_<timestamp>.csv` |
| Run log | `logs/extract.log` |

Processed PDFs are automatically moved to `input/processed/` so they are not re-processed on the next run.

---

## Switching Models

Open [src/extract.py](../src/extract.py) and change the `MODEL` constant near the top:

```python
# Available options:
# "claude-opus-4-6"            ← highest accuracy (default)
# "claude-sonnet-4-6"          ← faster, lower cost
# "claude-haiku-4-5-20251001"  ← fastest, lowest cost
MODEL = "claude-opus-4-6"
```

---

## Querying the Database

From Supabase Dashboard → Table Editor, or using Python:

```python
import os
import pandas as pd
import psycopg2
from dotenv import load_dotenv

load_dotenv()
conn = psycopg2.connect(os.environ["DATABASE_URL"])
df = pd.read_sql("SELECT * FROM timesheet_entries", conn)
print(df.head())
conn.close()
```

---

## Troubleshooting

| Issue | Fix |
|-------|-----|
| `ANTHROPIC_API_KEY not set` | Make sure `.env` exists and contains your key |
| `DATABASE_URL is not set` | Add your Supabase connection string to `.env` |
| `connection refused` / auth error | Double-check the password in `DATABASE_URL` |
| `No PDF files found` | Confirm PDFs are in `input/` (not a subfolder) |
| Empty rows extracted | Check `logs/extract.log` for warnings; the page may be a cover page or low-quality scan |
| Low accuracy | Switch to `claude-opus-4-6` and ensure scans are at least 150 DPI |
