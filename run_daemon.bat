@echo off
echo ========================================================
echo KHOI DONG HE THONG THU THAP DU LIEU THOI TIET SAN BAY
echo (METAR: moi 10 phut ^| Open-Meteo: moi 60 phut ^| Himawari-9: moi 10 phut)
echo ========================================================
.venv\Scripts\python.exe -m src.ingestion.run_ingest --loop --include-himawari
pause
