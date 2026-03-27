@echo off
cd /d "%~dp0"
.venv\Scripts\python.exe scripts\extractor_anthropic.py data\1.input --output-dir data\2.output
pause
