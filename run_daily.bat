@echo off
cd /d "C:\Yotta Lab\Yotta-Analytics"
call .venv\Scripts\activate.bat
echo [%date% %time%] Avvio sync giornaliero...
python sync.py --days 2
python sync_notes.py --days 2
python analyze_tickets.py --days 2
echo [%date% %time%] Sync completato.
