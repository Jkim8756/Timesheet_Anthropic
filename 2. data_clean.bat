@echo off
cd /d "%~dp0"
.venv\Scripts\python.exe scripts\clean.py --search-dir data\2.output --output-dir data\3.archive
pause
