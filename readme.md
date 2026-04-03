Billing Automation Tool 


An End-to-End ETL pipeline to extract text from a physical timesheet, transform as per clients' request for billing. 

1. Extract text: 
    - OCR/IDP/AI models: Claude, Google Document AI, AWS Textract, etc 
    - multiple pages of PDF files, around 30 pages per file, up to 20 files. 
    - compare cost, accuracy of each models, Accuracy is top priority. 
    - output should be stored in SQL for the mainstream and json and csv as backup for manual review. 
        -Columns should be but not limited to : {Project, Business Unit, Date, Day of Week, EID, Name, Title, Start time, Lunch Out, Lunch In, End time, Hours, Absent}. Having more data is better than not having enough. 

2. Data cleaning:
    Clean and transform data in Python Pandas.


3. Ingest cleaned data into client report (Mostly excel). 



Billing Automation/
├── input/              ← drop PDFs here before running
│   └── processed/      ← PDFs auto-moved here after extraction
├── output/
│   ├── db/             ← timesheets.db (Supabase, primary store)
│   ├── json/           ← extraction_<timestamp>.json (backup)
│   └── csv/            ← extraction_<timestamp>.csv (backup)
├── src/
│   └── extract.py      ← Step 1 extraction script
├── logs/
│   └── extract.log
├── .env.example
├── requirements.txt
└── readme.md
