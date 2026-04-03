#!/usr/bin/env python3
"""
Diagnostic script — inspect what Claude actually sees.

Usage:
    python scripts/debug_pdf.py "input/processed/Ceiling 2 DEC.pdf"

What it does:
    1. Renders the first 3 pages of the PDF and saves them as PNGs in debug/images/
    2. Sends page 1 to Claude and saves the full raw response to debug/response_page1.txt
    3. Prints a summary so you can see what's happening
"""

import base64
import json
import os
import sys
from pathlib import Path

import anthropic
import fitz  # PyMuPDF
from dotenv import load_dotenv

load_dotenv()

BASE_DIR  = Path(__file__).resolve().parent.parent
DEBUG_DIR = BASE_DIR / "debug"
IMG_DIR   = DEBUG_DIR / "images"
IMG_DIR.mkdir(parents=True, exist_ok=True)

MODEL = "claude-opus-4-6"

_EXTRACTION_PROMPT = """\
You are an expert timesheet data extractor.

Examine this timesheet image and extract EVERY row that represents a time entry.
Return a JSON array where each element is one row. Use these exact field names:

  project, business_unit, date, day_of_week, eid, name, title,
  start_time, lunch_out, lunch_in, end_time, hours, absent, notes

Rules:
- Use null for any cell that is blank or unreadable.
- Preserve the original text exactly (do not normalise dates or times).
- If the sheet contains columns not listed above, include them as extra keys.
- If the page contains no data rows (e.g. a cover page), return an empty array [].
- Return ONLY the JSON array — no markdown fences, no commentary.
"""


def main() -> None:
    if len(sys.argv) < 2:
        print("Usage: python scripts/debug_pdf.py <path_to_pdf>")
        sys.exit(1)

    pdf_path = Path(sys.argv[1])
    if not pdf_path.exists():
        print(f"File not found: {pdf_path}")
        sys.exit(1)

    print(f"Opening: {pdf_path.name}")
    doc = fitz.open(str(pdf_path))
    total_pages = len(doc)
    print(f"Total pages: {total_pages}")

    pages_to_check = min(3, total_pages)
    images: list[str] = []

    for page_num in range(pages_to_check):
        page = doc[page_num]
        pix = page.get_pixmap(dpi=150)
        out_path = IMG_DIR / f"page_{page_num + 1}.png"
        pix.save(str(out_path))
        b64 = base64.standard_b64encode(pix.tobytes("png")).decode()
        images.append(b64)
        size_kb = len(b64) * 3 // 4 // 1024
        print(f"  Page {page_num + 1}: {pix.width}x{pix.height} px, ~{size_kb} KB  → saved to {out_path.relative_to(BASE_DIR)}")

    doc.close()

    # Send page 1 to Claude and capture the full raw response
    print(f"\nSending page 1 to Claude ({MODEL})...")
    client = anthropic.Anthropic()
    response = client.messages.create(
        model=MODEL,
        max_tokens=4096,
        messages=[
            {
                "role": "user",
                "content": [
                    {
                        "type": "image",
                        "source": {
                            "type": "base64",
                            "media_type": "image/png",
                            "data": images[0],
                        },
                    },
                    {"type": "text", "text": _EXTRACTION_PROMPT},
                ],
            }
        ],
    )

    raw_text = response.content[0].text if response.content else ""
    stop    = response.stop_reason
    in_tok  = response.usage.input_tokens
    out_tok = response.usage.output_tokens

    print(f"  stop_reason  : {stop}")
    print(f"  input_tokens : {in_tok}")
    print(f"  output_tokens: {out_tok}")
    print(f"  response_len : {len(raw_text)} chars")
    print(f"  first 500 chars:\n{raw_text[:500]}")

    # Save full response
    resp_path = DEBUG_DIR / "response_page1.txt"
    resp_path.write_text(raw_text, encoding="utf-8")
    print(f"\nFull response saved to {resp_path.relative_to(BASE_DIR)}")

    # Try to parse as JSON
    trimmed = raw_text.strip()
    if trimmed.startswith("```"):
        trimmed = trimmed.split("\n", 1)[-1].rsplit("```", 1)[0].strip()
    try:
        rows = json.loads(trimmed)
        print(f"\nJSON parsed OK — {len(rows)} row(s) extracted from page 1")
        if rows:
            print("First row:", json.dumps(rows[0], indent=2))
    except Exception as exc:
        print(f"\nJSON parse failed: {exc}")
        print("Open debug/images/page_1.png to see what Claude received.")


if __name__ == "__main__":
    main()
