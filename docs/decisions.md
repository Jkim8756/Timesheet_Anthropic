# Technical Decisions & Change Log

A record of significant design choices, mistakes, and what was learned.

---

## 2026-04-01 — Extraction approach: PyMuPDF → pypdf + PDF document API

### What changed
Replaced `PyMuPDF` (`fitz`) with `pypdf`, and changed how pages are sent to Claude.

| | Before | After |
|---|---|---|
| **Library** | `PyMuPDF` (fitz) | `pypdf` |
| **PDF → Claude** | Rendered each page to PNG at 150 DPI, sent as `type: "image"` | Extracts each page as a single-page PDF binary, sent as `type: "document"` with `media_type: "application/pdf"` |
| **Claude content type** | `"type": "image"` | `"type": "document"` |

### Why it failed with images
The timesheets contain barcodes and dense graphics. When Claude received pages as rendered PNG images it transcribed the barcode content as a long binary-looking string instead of reading the timesheet data. Switching to `type: "document"` lets Claude use its native PDF understanding, which handles mixed content correctly and returned clean structured JSON.

### What else changed at the same time
- **Prompt restructured** — returns a JSON *object* with `frontline_staff` and `management_staff` arrays instead of a flat JSON array. Adds `confidence` (0–1 per staff row), `weather`, `schedule_changed`, `signature` fields.
- **Excel output added** — per-source Excel workbook + master workbook with formatted sheets become the primary human-readable output. Color-coded rows (yellow = low confidence, red = failed).
- **Interactive file selection** — menu shown before any API calls; displays page counts and estimated cost, requires confirmation before spending credits.
- **DB schema updated** — one row per staff member (not per page). New columns: `staff_type`, `model_used`, `confidence`, `schedule_changed`, `signature`.
- **Retry logic added** — automatic retries with backoff on rate limits or JSON parse failures (up to 3 attempts).
- **PyMuPDF removed** from `requirements.txt` and uninstalled from `.venv`.

### Lesson
Always use `type: "document"` for PDF input to Claude. Rendering to image first is slower, uses more tokens (larger images), and loses PDF structure that Claude can otherwise exploit. Only fall back to image rendering if the source is a non-PDF image format (JPEG, PNG scan).

---
